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
from airtable_scripts.utils import jsonl_dir_to_airtable


def create_dag(config: dict) -> DAG:
    """
    Generates a dag that will run a scraper
    :param config: scraper configuration
    :return: dag that runs a scraper
    """
    dagname = f"bq_to_airtable_{config['name']}"
    bucket = DEV_DATA_BUCKET
    production_dataset = config["production_dataset"]
    staging_dataset = f"staging_{production_dataset}"
    sql_dir = f"sql/{production_dataset}"
    tmp_dir = f"{production_dataset}/tmp"

    default_args = get_default_args()
    default_args.pop("on_failure_callback")

    dag = DAG(
        dagname,
        default_args=default_args,
        description=f"scraper for {config['name']}",
        schedule_interval=config["schedule_interval"],
        catchup=False,
        user_defined_macros={
            "staging_dataset": staging_dataset,
            "production_dataset": production_dataset,
        },
    )
    with dag:
        # clear tmp dir
        clear_tmp_dir = GCSDeleteObjectsOperator(
            task_id="clear_tmp_dir", bucket_name=bucket, prefix=tmp_dir
        )

        get_relevant_data = BigQueryInsertJobOperator(
            task_id="get_relevant_data",
            configuration={
                "query": {
                    "query": "{% include '" + f"{sql_dir}/{config['relevant_data']}.sql" + "' %}",
                    "useLegacySql": False,
                    "destinationTable": {
                        "projectId": project_id,
                        "datasetId": staging_dataset,
                        "tableId": config["relevant_data"]
                    },
                    "allowLargeResults": True,
                    "createDisposition": "CREATE_IF_NEEDED",
                    "writeDisposition": "WRITE_TRUNCATE"
                }
            },
        )

        export_to_gcs = BigQueryToGCSOperator(
            task_id="export_to_gcs",
            source_project_dataset_table=f"{staging_dataset}.{config['relevant_data']}",
            destination_cloud_storage_uris=f"gs://{bucket}/{tmp_dir}/{config['relevant_data']}/data*.jsonl",
            print_header=False,
        )

        add_to_airtable = PythonOperator(
            task_id="add_to_airtable",
            op_kwargs={
                "input_dir": f"{os.environ.get('DAGS_FOLDER')}/schemas/{gcs_folder}/{table}.json",
                "table_name": f"{production_dataset}.{table}"
            },
            python_callable=jsonl_dir_to_airtable
        )

        # post success to slack
        msg_success = get_post_success(f"Exported new data to Airtable for {}", dag)

        clear_tmp_dir >> get_relevant_data >> export_to_gcs >> add_to_airtable >> msg_success

    return dag


config_path = os.path.join(f"{os.environ.get('DAGS_FOLDER')}", "bq_to_airtable")
for config_fi in os.listdir(config_path):
    with open(os.path.join(config_path, config_fi)) as f:
        config = json.loads(f.read())
    globals()[dagname] = create_dag(config)
