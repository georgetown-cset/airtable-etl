"""
Utilities for interacting with airtable
"""

import json
import os
import tempfile

import requests
from airflow.hooks.base_hook import BaseHook
from google.cloud import storage
from more_itertools import batched


def jsonl_dir_to_json_iter(jsonl_dir: str) -> iter:
    """
    Reads data from directory of jsonl files and returns it as a generator
    :param jsonl_dir: Directory containing jsonl files
    :return: Generator of dicts corresponding to each record in `jsonl_dir`
    """
    for fi in os.listdir(jsonl_dir):
        with open(os.path.join(jsonl_dir, fi)) as f:
            for line in f:
                yield json.loads(line)


def jsonl_dir_to_batches(jsonl_dir: str, batch_size=10) -> iter:
    """
    Batches the data within `jsonl_dir` into chunks of `batch_size`
    :param jsonl_dir: Directory containing input data
    :param batch_size: Max number of records each batch should contain
    :return: Iterable of batches
    """
    jsons = jsonl_dir_to_json_iter(jsonl_dir)
    return batched(jsons, batch_size)


def insert_into_airtable(base_id: str, table_name: str, data: list, token: str) -> None:
    """
    Inserts a list of data into airtable
    :param base_id: Airtable base id
    :param table_name: Airtable table name
    :param data: List of data (as dicts) to insert
    :param token: Airtable access token
    :return: None
    """
    headers = {"Authorization": f"Bearer {token}"}
    reformatted_data = {"records": [{"fields": elt} for elt in data]}
    result = requests.post(
        f"https://api.airtable.com/v0/{base_id}/{table_name}",
        json=reformatted_data,
        headers=headers,
    )
    if result.status_code != 200:
        print(result.text)
        print(result.content)
        raise ValueError(f"Unexpected status code: {result.status_code}")


def jsonl_dir_to_airtable(
    bucket_name: str, input_prefix: str, table_name: str, base_id: str, token: str
) -> None:
    """
    Ingests a JSONL data export of one or more files starting with `input_prefix` within `bucket` into Airtable
    :param bucket_name: GCS bucket where input data is stored
    :param input_prefix: GCS prefix of input data
    :param table_name: Airtable table name
    :param base_id: Airtable base id
    :param token: Airtable access token
    :return: None
    """
    gcs_client = storage.Client()
    bucket = gcs_client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=input_prefix)
    with tempfile.TemporaryDirectory() as tmpdir:
        for blob in blobs:
            file_name = os.path.join(tmpdir, blob.name.split("/")[-1])
            blob.download_to_filename(file_name)
        batches = jsonl_dir_to_batches(tmpdir)
        for batch in batches:
            insert_into_airtable(base_id, table_name, batch, token)


def jsonl_dir_to_airtable_airflow(
    bucket_name: str, input_prefix: str, table_name: str, base_id: str
) -> None:
    """
    Calls `jsonl_dir_to_airtable` from airflow, where we can grab the API key from the airtable connection
    :param bucket_name: GCS bucket where input data is stored
    :param input_prefix: GCS prefix of input data
    :param table_name: Airtable table name
    :param base_id: Airtable base id
    :return: None
    """
    connection = BaseHook.get_connection("airtable")
    token = connection.password
    jsonl_dir_to_airtable(bucket_name, input_prefix, table_name, base_id, token)


if __name__ == "__main__":
    # to be used only for testing purposes
    token = os.environ.get("AIRTABLE_TOKEN")

    jsonl_dir_to_airtable(
        "jtm23",
        "airtable_tests/data",
        "Table 1",
        "appvnA46jraScMMth",  # the "Airflow testing" base
        token,
    )
