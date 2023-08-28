"""
Generates a dag that copies data from BigQuery into Airtable for each config in the bq_to_airtable_config dir
in the Airflow dag bucket on GCS.
"""

import json
import os

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import (
    BigQueryToGCSOperator,
)
from dataloader.airflow_utils.defaults import (
    DEV_DATA_BUCKET,
    GCP_ZONE,
    PROJECT_ID,
    get_default_args,
    get_post_success,
)

from airtable_scripts.utils import gcs_to_airtable_airflow

DATASET = "bq_to_airtable"
STAGING_DATASET = f"staging_{DATASET}"
CONFIG_PATH = os.path.join(f"{os.environ.get('DAGS_FOLDER')}", f"{DATASET}_config")
PARENT_CONFIG = "config.json"


def update_airtable(dag: DAG, start_task, end_task, config: dict):
    bucket = DEV_DATA_BUCKET
    name = config["name"]
    sql_dir = f"sql/{DATASET}/{config.get('parent_name', name)}"
    tmp_dir = f"{DATASET}/{name if 'parent_name' not in config else config['parent_name']+'_'+name}/tmp"
    with dag:
        clear_tmp_dir = GCSDeleteObjectsOperator(
            task_id=f"clear_tmp_dir_{name}", bucket_name=bucket, prefix=tmp_dir
        )

        bq_table = config["input_data"]
        get_input_data = BigQueryInsertJobOperator(
            task_id=f"get_input_data_for_{name}",
            configuration={
                "query": {
                    "query": "{% include '" + f"{sql_dir}/{bq_table}.sql" + "' %}",
                    "useLegacySql": False,
                    "destinationTable": {
                        "projectId": PROJECT_ID,
                        "datasetId": STAGING_DATASET,
                        "tableId": bq_table,
                    },
                    "allowLargeResults": True,
                    "createDisposition": "CREATE_IF_NEEDED",
                    "writeDisposition": "WRITE_TRUNCATE",
                }
            },
        )

        export_to_gcs = BigQueryToGCSOperator(
            task_id=f"export_{name}_to_gcs",
            source_project_dataset_table=f"{STAGING_DATASET}.{bq_table}",
            destination_cloud_storage_uris=f"gs://{bucket}/{tmp_dir}/{bq_table}/data*.jsonl",
            export_format="NEWLINE_DELIMITED_JSON",
        )

        add_to_airtable = PythonOperator(
            task_id=f"add_{name}_to_airtable",
            op_kwargs={
                "bucket_name": bucket,
                "input_prefix": f"{tmp_dir}/{bq_table}/data",
                "table_name": config["airtable_table"],
                "base_id": config["airtable_base"],
                "column_map": config.get("column_map"),
            },
            python_callable=gcs_to_airtable_airflow,
        )

        (start_task >> clear_tmp_dir >> get_input_data >> export_to_gcs >> end_task)
        return add_to_airtable


def create_dag(dagname: str, config: dict, parent_dir: str) -> DAG:
    """
    Generates a dag that will update airtable from BigQuery
    :param dagname: Name of the dag to create
    :param config: Pipeline configuration
    :param parent_dir: If specified, will look in this dir for specific configs to merge with the
    (presumed shared/general in this case) `config`
    :return: Dag that runs an import from bq to airtable
    """
    default_args = get_default_args()
    default_args.pop("on_failure_callback")

    dag = DAG(
        dagname,
        default_args=default_args,
        description=f"Airtable data ingest for {dagname}",
        schedule_interval=config["schedule_interval"],
        catchup=False,
    )
    with dag:
        start = DummyOperator(task_id="start")
        wait_for_export = DummyOperator(task_id="wait_for_export")

        msg_success = get_post_success(
            f"Exported new data to Airtable for {dagname}", dag
        )

        prev_task = wait_for_export
        if parent_dir:
            for child_config_fi in os.listdir(parent_dir):
                if child_config_fi != PARENT_CONFIG:
                    with open(os.path.join(parent_dir, child_config_fi)) as f:
                        child_config = json.loads(f.read())
                        child_config.update(config)
                        child_config["name"] = child_config.get(
                            "name", child_config_fi.replace(".json", "")
                        )
                        add_to_airtable = update_airtable(
                            dag, start, wait_for_export, child_config
                        )
                        prev_task >> add_to_airtable
                        prev_task = add_to_airtable
        else:
            add_to_airtable = update_airtable(dag, start, wait_for_export, config)
            prev_task >> add_to_airtable
            prev_task = add_to_airtable

        prev_task >> msg_success

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
