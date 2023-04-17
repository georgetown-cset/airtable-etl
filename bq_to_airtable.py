"""
Generates a dag that copies data from BigQuery into Airtable for each config in the bq_to_airtable_config dir
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

from airtable_scripts.utils import gcs_to_airtable_airflow


def create_dag(dagname: str, config: dict) -> DAG:
    """
    Generates a dag that will update airtable from BigQuery
    :param dagname: Name of the dag to create
    :param config: Pipeline configuration
    :return: Dag that runs a scraper
    """
    bucket = DEV_DATA_BUCKET
    staging_dataset = "staging_airtable_to_bq"
    sql_dir = "sql/airtable_to_bq/"
    tmp_dir = f"airtable_to_bq/{config['name']}/tmp"

    default_args = get_default_args()
    default_args.pop("on_failure_callback")

    dag = DAG(
        dagname,
        default_args=default_args,
        description=f"Airtable data ingest for {config['name']}",
        schedule_interval=config["schedule_interval"],
        catchup=False,
    )
    with dag:
        clear_tmp_dir = GCSDeleteObjectsOperator(
            task_id="clear_tmp_dir", bucket_name=bucket, prefix=tmp_dir
        )

        bq_table = config["input_data"]
        get_input_data = BigQueryInsertJobOperator(
            task_id="get_input_data",
            configuration={
                "query": {
                    "query": "{% include '" + f"{sql_dir}/{bq_table}.sql" + "' %}",
                    "useLegacySql": False,
                    "destinationTable": {
                        "projectId": PROJECT_ID,
                        "datasetId": staging_dataset,
                        "tableId": bq_table,
                    },
                    "allowLargeResults": True,
                    "createDisposition": "CREATE_IF_NEEDED",
                    "writeDisposition": "WRITE_TRUNCATE",
                }
            },
        )

        export_to_gcs = BigQueryToGCSOperator(
            task_id="export_to_gcs",
            source_project_dataset_table=f"{staging_dataset}.{bq_table}",
            destination_cloud_storage_uris=f"gs://{bucket}/{tmp_dir}/{bq_table}/data*.jsonl",
            export_format="NEWLINE_DELIMITED_JSON",
        )

        add_to_airtable = PythonOperator(
            task_id="add_to_airtable",
            op_kwargs={
                "bucket_name": bucket,
                "input_prefix": f"{tmp_dir}/{bq_table}/data",
                "table_name": config["airtable_table"],
                "base_id": config["airtable_base"],
            },
            python_callable=gcs_to_airtable_airflow,
        )

        msg_success = get_post_success(
            f"Exported new data to Airtable for {config['name']}", dag
        )

        (
            clear_tmp_dir
            >> get_input_data
            >> export_to_gcs
            >> add_to_airtable
            >> msg_success
        )

    return dag


config_path = os.path.join(f"{os.environ.get('DAGS_FOLDER')}", "bq_to_airtable_config")
for config_fi in os.listdir(config_path):
    with open(os.path.join(config_path, config_fi)) as f:
        config = json.loads(f.read())
    dagname = f"bq_to_airtable_{config['name']}"
    globals()[dagname] = create_dag(dagname, config)
