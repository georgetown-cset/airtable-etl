import itertools
import json
import requests
import tempfile
from google.cloud import storage


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
    return itertools.batched(jsons, batch_size)


def insert_into_airtable(base_id: str, table_name: str, data: list, token: str) -> None:
    headers = {"Authorization": f"Bearer {token}"}
    result = requests.post(f"https://api.airtable.com/v0/{base_id}/{table_name}", json=data, headers=headers)
    if result.status_code != 200:
        raise ValueError(f"Unexpected status code: {reslt.status_code}")


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
            file_name = os.path.join(tmpdir.name, blob.name.split("/"))
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
    parser = argparse.ArgumentParser()
    parser.add_argument("token")
    args = parser.parse_args()

    jsonl_dir_to_airtable_airflow(
        "airtable-airtable-test",
        "input-data/data",
        "test",
        "appvnA46jraScMMth", # the "Airflow testing" base
    )


