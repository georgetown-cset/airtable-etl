import json
import os
import requests
import tempfile
from google.cloud import storage
from more_itertools import batched


def jsonl_dir_to_json_iter(jsonl_dir: str) -> iter:
    """

    :param jsonl_dir:
    :return:
    """
    for fi in os.listdir(jsonl_dir):
        with open(os.path.join(jsonl_dir, fi)) as f:
            for line in f:
                yield json.loads(line)


def jsonl_dir_to_batches(jsonl_dir: str, batch_size=10) -> iter:
    """

    :param file_path:
    :param batch_size:
    :return:
    """
    jsons = jsonl_dir_to_json_iter(jsonl_dir)
    return batched(jsons, batch_size)


def insert_into_airtable(base_id: str, table_name: str, data: list, token: str) -> None:
    headers = {"Authorization": f"Bearer {token}"}
    reformatted_data = {"records": [{"fields": elt} for elt in data]}
    result = requests.post(f"https://api.airtable.com/v0/{base_id}/{table_name}", json=reformatted_data, headers=headers)
    if result.status_code != 200:
        print(result.text)
        print(result.content)
        raise ValueError(f"Unexpected status code: {result.status_code}")


def jsonl_dir_to_airtable(bucket_name: str, input_prefix: str, table_name: str, base_id: str, token: str) -> None:
    """

    :param bucket_name:
    :param input_prefix:
    :param table_name:
    :param base_id:
    :param token:
    :return:
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


def jsonl_dir_to_airtable_airflow(bucket_name: str, input_prefix: str, table_name: str, base_id: str, token: str) -> None:
    """

    :param bucket_name:
    :param input_prefix:
    :param table_name:
    :param base_id:
    :param token:
    :return:
    """
    # todo - retrieve token from airflow
    pass


if __name__ == "__main__":
    # to be used only for testing purposes
    token = os.environ.get("AIRTABLE_TOKEN")

    jsonl_dir_to_airtable(
        "jtm23",
        "airtable_tests/data",
        "Table 1",
        "appvnA46jraScMMth",  # the "Airflow testing" base
        token
    )


