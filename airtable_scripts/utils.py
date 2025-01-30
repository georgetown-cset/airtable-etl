"""
Utilities for interacting with airtable
"""

import json
import os
import tempfile

import requests
from dataloader.airflow_utils.utils import get_connection_info
from google.cloud import storage
from more_itertools import batched

# Used to indicate which Airtable columns should be excluded from the results in BigQuery
EXCLUDE = "EXCLUDE"


def jsonl_dir_to_json_iter(
    jsonl_dir: str, column_map: dict, integer_cols: list
) -> iter:
    """
    Reads data from directory of jsonl files and returns it as a generator
    :param jsonl_dir: Directory containing jsonl files
    :param column_map: A mapping from BQ column names to Airtable column names. If null, BQ
    column names will be used in Airtable
    :param integer_cols: Columns with values that should be converted to integer (see also:
    https://issuetracker.google.com/issues/35905373?pli=1)
    :return: Generator of dicts corresponding to each record in `jsonl_dir`
    """
    for fi in os.listdir(jsonl_dir):
        with open(os.path.join(jsonl_dir, fi)) as f:
            for line in f:
                row = json.loads(line)
                if column_map:
                    row = {column_map.get(k, k): v for k, v in row.items()}
                for col in integer_cols:
                    row[col] = int(row[col])
                yield row


def jsonl_dir_to_batches(
    jsonl_dir: str, column_map: dict, integer_cols: list, batch_size=10
) -> iter:
    """
    Batches the data within `jsonl_dir` into chunks of `batch_size`
    :param jsonl_dir: Directory containing input data
    :param column_map: A mapping from BQ column names to Airtable column names. If null, BQ
    column names will be used in Airtable
    :param integer_cols: Columns with values that should be converted to integer (see also:
    https://issuetracker.google.com/issues/35905373?pli=1)
    :param batch_size: Max number of records each batch should contain
    :return: Iterable of batches
    """
    jsons = jsonl_dir_to_json_iter(jsonl_dir, column_map, integer_cols)
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
        raise ValueError(f"Unexpected status code: {result.status_code}")


def gcs_to_airtable(
    bucket_name: str,
    input_prefix: str,
    table_name: str,
    base_id: str,
    token: str,
    column_map: dict,
    integer_cols: list,
) -> None:
    """
    Ingests a JSONL data export of one or more files starting with `input_prefix` within `bucket` into Airtable
    :param bucket_name: GCS bucket where input data is stored
    :param input_prefix: GCS prefix of input data
    :param table_name: Airtable table name
    :param base_id: Airtable base id
    :param token: Airtable access token
    :param column_map: A mapping from BQ column names to Airtable column names. If null, BQ
    column names will be used in Airtable
    :param integer_cols: Columns with values that should be converted to integer (see also:
    https://issuetracker.google.com/issues/35905373?pli=1)
    :return: None
    """
    gcs_client = storage.Client()
    bucket = gcs_client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=input_prefix)
    with tempfile.TemporaryDirectory() as tmpdir:
        for blob in blobs:
            file_name = os.path.join(tmpdir, blob.name.split("/")[-1])
            blob.download_to_filename(file_name)
        batches = jsonl_dir_to_batches(tmpdir, column_map, integer_cols)
        for batch in batches:
            insert_into_airtable(base_id, table_name, batch, token)


def gcs_to_airtable_airflow(
    bucket_name: str,
    input_prefix: str,
    table_name: str,
    base_id: str,
    column_map: dict,
    integer_cols: list,
) -> None:
    """
    Calls `gcs_to_airtable` from airflow, where we can grab the API key from the airtable connection
    :param bucket_name: GCS bucket where input data is stored
    :param input_prefix: GCS prefix of input data
    :param table_name: Airtable table name
    :param base_id: Airtable base id
    :param column_map: A mapping from BQ column names to Airtable column names. If null, BQ
    column names will be used in Airtable
    :param integer_cols: Columns with values that should be converted to integer (see also:
    https://issuetracker.google.com/issues/35905373?pli=1)
    :return: None
    """
    connection = get_connection_info("ETO_scout_airtable")
    token = connection["password"]
    gcs_to_airtable(
        bucket_name, input_prefix, table_name, base_id, token, column_map, integer_cols
    )


def get_airtable_iter(
    table_name: str, base_id: str, token: str, include_airtable_id: bool = False
) -> iter:
    """
    Retrieves data from an airtable table
    :param table_name: Airtable table name we are retrieving data from
    :param base_id: Airtable base
    :param token: Airtable access token
    :param include_airtable_id: If true, the airtable id of each row will be included in the results
    :return: Iterable of rows from `table_name`
    """
    headers = {"Authorization": f"Bearer {token}"}
    offset = None
    while True:
        query = f"/?offset={offset}" if offset else ""
        result = requests.get(
            f"https://api.airtable.com/v0/{base_id}/{table_name}{query}",
            headers=headers,
        )
        result.raise_for_status()
        data = result.json()
        for row in data["records"]:
            content = row["fields"]
            if include_airtable_id:
                content["airtable_id"] = row["id"]
            yield content
        # An offset will be provided if there is an additional page of data to retrieve
        if not data.get("offset"):
            break
        offset = data["offset"]


def airtable_to_gcs(
    table_name: str,
    base_id: str,
    bucket_name: str,
    output_prefix: str,
    token: str,
    column_map: dict,
    include_airtable_id: bool = False,
) -> None:
    """
    Retrieves data from airtable and writes it to GCS
    :param table_name: Airtable table name we are retrieving data from
    :param base_id: Airtable base
    :param bucket_name: GCS bucket where output data should go
    :param output_prefix: GCS prefix where output data should go within `bucket`
    :param token: Airtable access token
    :param column_map: A mapping from Airtable column names to BigQuery column names. If null, BigQuery
    column names will be used in Airtable
    :param include_airtable_id: If true, the airtable id of each row will be included in the results
    :return: None
    """
    gcs_client = storage.Client()
    bucket = gcs_client.get_bucket(bucket_name)
    blob = bucket.blob(output_prefix.strip("/") + "/data.jsonl")
    data = get_airtable_iter(table_name, base_id, token, include_airtable_id)
    with blob.open("w") as f:
        for row in data:
            if column_map:
                row = {
                    column_map.get(k, k): v
                    for k, v in row.items()
                    if column_map.get(k) != EXCLUDE
                }
            f.write(json.dumps(row) + "\n")


def airtable_to_gcs_airflow(
    table_name: str,
    base_id: str,
    bucket_name: str,
    output_prefix: str,
    column_map: dict,
    include_airtable_id: bool = False,
) -> None:
    """
    Calls `airtable_to_gcs` from airflow, where we can grab the API key from the airtable connection
    :param table_name: Airtable table name we are retrieving data from
    :param base_id: Airtable base
    :param bucket_name: GCS bucket where output data should go
    :param output_prefix: GCS prefix where output data should go within `bucket`
    :param column_map: A mapping from Airtable column names to BigQuery column names. If null, BigQuery
    column names will be used in Airtable
    :param include_airtable_id: If true, the airtable id of each row will be included in the results
    :return: None
    """
    connection = get_connection_info("ETO_scout_airtable")
    token = connection["password"]
    airtable_to_gcs(
        table_name,
        base_id,
        bucket_name,
        output_prefix,
        token,
        column_map,
        include_airtable_id,
    )


if __name__ == "__main__":
    # This block is only for local testing purposes
    token = os.environ.get("AIRTABLE_TOKEN")

    gcs_to_airtable(
        "jtm23",
        "airtable_tests/data",
        "Table 1",
        "appvnA46jraScMMth",  # the "Airflow testing" base
        token,
        {"foo": "Foo"},
    )
    airtable_to_gcs(
        "Table 1",
        "appvnA46jraScMMth",  # the "Airflow testing" base
        "jtm23",
        "airtable_tests/output",
        token,
        {"Foo": "foo", "bar": "mapped_bar", "baz": EXCLUDE},
    )
