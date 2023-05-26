"""
Generates a dag that copies data from Airtable into BigQuery for each config in the airtable_to_bq_config dir
in the Airflow dag bucket on GCS.
"""

import json
import os
from datetime import datetime

from airflow import DAG
from airflow.contrib.operators.bigquery_to_bigquery import BigQueryToBigQueryOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.operators.cloud_sql import (
    CloudSQLImportInstanceOperator,
)
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.providers.google.cloud.operators.kubernetes_engine import (
    GKEStartPodOperator,
)
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import (
    BigQueryToGCSOperator,
)
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


def create_dag(dagname: str, config: dict) -> DAG:
    """
    Generates a dag that will update BigQuery from airtable
    :param dagname: Name of the dag to create
    :param config: Pipeline configuration
    :return: Dag that runs a scraper
    """
    bucket = DEV_DATA_BUCKET
    staging_dataset = f"staging_{DATASET}"
    sql_dir = f"sql/{DATASET}/{config['name']}"
    schema_dir = f"schemas/{DATASET}/{config['name']}"
    tmp_dir = f"{DATASET}/{config['name']}/tmp"

    default_args = get_default_args()
    default_args.pop("on_failure_callback")

    dag = DAG(
        dagname,
        default_args=default_args,
        description=f"Airtable data export for {config['name']}",
        schedule_interval=config["schedule_interval"],
        catchup=False,
    )
    with dag:
        clear_tmp_dir = GCSDeleteObjectsOperator(
            task_id="clear_tmp_dir", bucket_name=bucket, prefix=tmp_dir
        )

        pull_from_airtable = PythonOperator(
            task_id="pull_from_airtable",
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
        raw_table = f"{config['name']}_raw"
        new_table = f"{config['name']}_new_{date}"
        merged_table = f"{config['name']}_merged"
        gcs_to_bq = GCSToBigQueryOperator(
            task_id="gcs_to_bq",
            bucket=bucket,
            source_objects=[f"{tmp_dir}/data*"],
            schema_object=f"{schema_dir}/{config['schema_name']}.json",
            destination_project_dataset_table=f"{staging_dataset}.{raw_table}",
            source_format="NEWLINE_DELIMITED_JSON",
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_TRUNCATE",
        )

        # in the next two steps,
        # rather than trying to filter data in the airtable query, we load the entire contents of the airtable
        # table into BQ, then filter to the rows we want to update in the production table
        merge_data = BigQueryInsertJobOperator(
            task_id="merge_data",
            configuration={
                "query": {
                    "query": "{% include '"
                    + f"{sql_dir}/{config['merge_query']}.sql"
                    + "' %}",
                    "useLegacySql": False,
                    "destinationTable": {
                        "projectId": PROJECT_ID,
                        "datasetId": staging_dataset,
                        "tableId": merged_table,
                    },
                    "allowLargeResults": True,
                    "createDisposition": "CREATE_IF_NEEDED",
                    "writeDisposition": "WRITE_TRUNCATE",
                }
            },
            params={
                "staging_dataset": staging_dataset,
                "production_dataset": config["production_dataset"],
                "staging_table_name": new_table if config["new_query"] else raw_table,
                "production_table_name": config["production_table"],
            },
        )

        if config.get("new_query"):
            # If we have a new query, we identify all the new rows, save them, and then merge with existing data.
            # Otherwise, we identify and merge all as part of the above merge_data step, and don't bother
            # creating a table with only the new rows.
            save_new_rows = BigQueryInsertJobOperator(
                task_id="save_new_rows",
                configuration={
                    "query": {
                        "query": "{% include '"
                        + f"{sql_dir}/{config['new_query']}.sql"
                        + "' %}",
                        "useLegacySql": False,
                        "destinationTable": {
                            "projectId": PROJECT_ID,
                            "datasetId": staging_dataset,
                            "tableId": new_table,
                        },
                        "allowLargeResults": True,
                        "createDisposition": "CREATE_IF_NEEDED",
                        "writeDisposition": "WRITE_TRUNCATE",
                    }
                },
                params={
                    "staging_dataset": staging_dataset,
                    "production_dataset": config["production_dataset"],
                    "staging_table_name": raw_table,
                    "production_table_name": config["production_table"],
                },
            )
            gcs_to_bq >> save_new_rows >> merge_data
        else:
            gcs_to_bq >> merge_data

        prod_update = BigQueryToBigQueryOperator(
            task_id="prod_update",
            source_project_dataset_tables=[f"{staging_dataset}.{merged_table}"],
            destination_project_dataset_table=f"{config['production_dataset']}.{config['production_table']}",
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_TRUNCATE",
        )

        backup = BigQueryToBigQueryOperator(
            task_id="backup",
            source_project_dataset_tables=[
                f"{config['production_dataset']}.{config['production_table']}"
            ],
            destination_project_dataset_table=f"{config['production_dataset']}_backups.{config['production_table']}_{date}",
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_TRUNCATE",
        )

        msg_success = get_post_success(
            f"Ingested new data from Airtable for {config['name']}", dag
        )

        (clear_tmp_dir >> pull_from_airtable >> gcs_to_bq)
        (merge_data >> prod_update >> backup >> msg_success)

    return dag


config_path = os.path.join(f"{os.environ.get('DAGS_FOLDER')}", f"{DATASET}_config")
for config_fi in os.listdir(config_path):
    with open(os.path.join(config_path, config_fi)) as f:
        config = json.loads(f.read())
    dagname = f"{DATASET}_{config['name']}"
    globals()[dagname] = create_dag(dagname, config)
