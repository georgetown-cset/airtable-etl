gsutil rm -r gs://us-east1-production2023-cc1-01d75926-bucket/dags/airtable_scripts
gsutil -m cp -r airtable_scripts gs://us-east1-production2023-cc1-01d75926-bucket/dags/
gsutil cp bq_to_airtable.py gs://us-east1-production2023-cc1-01d75926-bucket/dags/
gsutil cp airtable_to_bq.py gs://us-east1-production2023-cc1-01d75926-bucket/dags/
