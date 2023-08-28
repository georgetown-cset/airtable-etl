"""
Generates a dag that copies data from Airtable into BigQuery for each config in the airtable_to_bq_config dir
in the Airflow dag bucket on GCS.
"""

import json
import os
from datetime import datetime

from airflow import DAG
from airflow.contrib.operators.bigquery_to_bigquery import BigQueryToBigQueryOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from dataloader.airflow_utils.defaults import (
    DEV_DATA_BUCKET,
    GCP_ZONE,
    PROJECT_ID,
    get_default_args,
    get_post_success,
)

from airtable_scripts.utils import airtable_to_gcs_airflow

DATASET = "airtable_to_bq"
STAGING_DATASET = f"staging_{DATASET}"
CONFIG_PATH = os.path.join(f"{os.environ.get('DAGS_FOLDER')}", f"{DATASET}_config")
PARENT_CONFIG = "config.json"


def update_staging(dag: DAG, start_task, config: dict):
    bucket = DEV_DATA_BUCKET
    name = config["name"]
    sql_dir = f"sql/{DATASET}/{config.get('parent_name', name)}"
    schema_dir = f"schemas/{DATASET}/{config.get('parent_name', name)}"
    tmp_dir = f"{DATASET}/{name if 'parent_name' not in config else config['parent_name']+'_'+name}/tmp"
    with dag:
        clear_tmp_dir = GCSDeleteObjectsOperator(
            task_id=f"clear_tmp_dir_{name}", bucket_name=bucket, prefix=tmp_dir
        )

        pull_from_airtable = PythonOperator(
            task_id=f"pull_{name}_from_airtable",
            op_kwargs={
                "table_name": config["airtable_table"],
                "base_id": config["airtable_base"],
                "bucket_name": bucket,
                "output_prefix": f"{tmp_dir}/data",
                "column_map": config.get("column_map"),
            },
            python_callable=airtable_to_gcs_airflow,
        )

        date = datetime.now().strftime("%Y%m%d")
        raw_table = f"{name}_raw"
        new_table = f"{name}_new_{date}"
        merged_table = f"{name}_merged"
        gcs_to_bq = GCSToBigQueryOperator(
            task_id=f"gcs_to_bq_{name}",
            bucket=bucket,
            source_objects=[f"{tmp_dir}/data*"],
            schema_object=f"{schema_dir}/{config['schema_name']}.json",
            destination_project_dataset_table=f"{STAGING_DATASET}.{raw_table}",
            source_format="NEWLINE_DELIMITED_JSON",
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_TRUNCATE",
        )

        # in the next two steps,
        # rather than trying to filter data in the airtable query, we load the entire contents of the airtable
        # table into BQ, then filter to the rows we want to update in the production table
        merge_data = BigQueryInsertJobOperator(
            task_id=f"merge_data_{name}",
            configuration={
                "query": {
                    "query": "{% include '"
                    + f"{sql_dir}/{config['merge_query']}.sql"
                    + "' %}",
                    "useLegacySql": False,
                    "destinationTable": {
                        "projectId": PROJECT_ID,
                        "datasetId": STAGING_DATASET,
                        "tableId": merged_table,
                    },
                    "allowLargeResults": True,
                    "createDisposition": "CREATE_IF_NEEDED",
                    "writeDisposition": "WRITE_TRUNCATE",
                }
            },
            params={
                "staging_dataset": STAGING_DATASET,
                "production_dataset": config["production_dataset"],
                "staging_table_name": new_table
                if config.get("new_query")
                else raw_table,
                "production_table_name": config["production_table"],
            },
        )

        if config.get("new_query"):
            # If we have a new query, we identify all the new rows, save them, and then merge with existing data.
            # Otherwise, we identify and merge all as part of the above merge_data step, and don't bother
            # creating a table with only the new rows.
            save_new_rows = BigQueryInsertJobOperator(
                task_id=f"save_new_rows_{name}",
                configuration={
                    "query": {
                        "query": "{% include '"
                        + f"{sql_dir}/{config['new_query']}.sql"
                        + "' %}",
                        "useLegacySql": False,
                        "destinationTable": {
                            "projectId": PROJECT_ID,
                            "datasetId": STAGING_DATASET,
                            "tableId": new_table,
                        },
                        "allowLargeResults": True,
                        "createDisposition": "CREATE_IF_NEEDED",
                        "writeDisposition": "WRITE_TRUNCATE",
                    }
                },
                params={
                    "staging_dataset": STAGING_DATASET,
                    "production_dataset": config["production_dataset"],
                    "staging_table_name": raw_table,
                    "production_table_name": config["production_table"],
                },
            )
            gcs_to_bq >> save_new_rows >> merge_data
        else:
            gcs_to_bq >> merge_data

        start_task >> clear_tmp_dir >> pull_from_airtable >> gcs_to_bq

        config["date"] = date
        config["merged_table"] = merged_table
        return merge_data


def update_production(dag: DAG, start_task, end_task, config: dict) -> None:
    STAGING_DATASET = f"staging_{DATASET}"
    name = config["name"]
    with dag:
        prod_update = BigQueryToBigQueryOperator(
            task_id=f"prod_update_{name}",
            source_project_dataset_tables=[
                f"{STAGING_DATASET}.{config['merged_table']}"
            ],
            destination_project_dataset_table=f"{config['production_dataset']}.{config['production_table']}",
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_TRUNCATE",
        )

        backup = BigQueryToBigQueryOperator(
            task_id=f"backup_{name}",
            source_project_dataset_tables=[
                f"{config['production_dataset']}.{config['production_table']}"
            ],
            destination_project_dataset_table=f"{config['production_dataset']}_backups.{config['production_table']}_{config['date']}",
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_TRUNCATE",
        )

        start_task >> prod_update >> backup >> end_task


def create_dag(dagname: str, config: dict, parent_dir: str = None) -> DAG:
    """
    Generates a dag that will update BigQuery from airtable
    :param dagname: Name of the dag to create
    :param config: Pipeline configuration
    :param parent_dir: If specified, will look in this dir for specific configs to merge with the
    (presumed shared/general in this case) `config`
    :return: Dag that runs an import from airtable to bq
    """
    default_args = get_default_args()
    default_args.pop("on_failure_callback")

    dag = DAG(
        dagname,
        default_args=default_args,
        description=f"Airtable data export for {dagname}",
        schedule_interval=config["schedule_interval"],
        catchup=False,
    )
    with dag:
        start = DummyOperator(task_id="start")
        msg_success = get_post_success(
            f"Ingested new data from Airtable for {dagname}", dag
        )
        curr_task = start

        if parent_dir:
            child_configs = []
            for child_config_fi in os.listdir(parent_dir):
                if child_config_fi != PARENT_CONFIG:
                    with open(os.path.join(parent_dir, child_config_fi)) as f:
                        child_config = json.loads(f.read())
                        child_config.update(config)
                        child_configs.append(child_config)
                        child_config["name"] = child_config.get(
                            "name", child_config_fi.replace(".json", "")
                        )
                        curr_task = update_staging(dag, curr_task, child_config)
            for child_config in child_configs:
                update_production(dag, curr_task, msg_success, child_config)
        else:
            curr_task = update_staging(dag, start, config)
            update_production(dag, curr_task, msg_success, config)

    return dag


for config_fi in os.listdir(CONFIG_PATH):
    config_fi_path = os.path.join(CONFIG_PATH, config_fi)
    if os.path.isdir(config_fi_path):
        # in this case, we'll have a directory of configs, one named `PARENT_CONFIG`, and the rest named
        # whatever makes sense. We'll merge the shared configuration in `PARENT_CONFIG` with the specific
        # configuration in the other configs as we run the pipeline for each config
        parent_config = os.path.join(config_fi_path, PARENT_CONFIG)
        if not os.path.exists(parent_config):
            continue
        with open(parent_config) as f:
            config = json.loads(f.read())
        config["parent_name"] = config_fi
        # we'll use the parent directory to name the dag
        dagname = f"{DATASET}_{config_fi}"
    else:
        # in this case, a single top-level configuration file contains all the info we need, and the
        # pipeline will only import data for one table
        with open(config_fi_path) as f:
            config = json.loads(f.read())
        dagname = f"{DATASET}_{config['name']}"
        parent_config = None
    parent_path = config_fi_path if parent_config else None
    globals()[dagname] = create_dag(dagname, config, parent_path)
