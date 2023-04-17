gsutil cp bq_to_airtable.py gs://us-east1-dev2023-cc1-b088c7e1-bucket/dags/
gsutil rm -r gs://us-east1-dev2023-cc1-b088c7e1-bucket/dags/airtable_scripts
gsutil -m cp -r airtable_scripts gs://us-east1-dev2023-cc1-b088c7e1-bucket/dags/
