gsutil rm -r gs://us-east1-dev2023-cc1-b088c7e1-bucket/dags/airtable_scripts
gsutil -m cp -r airtable_scripts gs://us-east1-dev2023-cc1-b088c7e1-bucket/dags/
gsutil cp bq_to_airtable.py gs://us-east1-dev2023-cc1-b088c7e1-bucket/dags/
gsutil cp airtable_to_bq.py gs://us-east1-dev2023-cc1-b088c7e1-bucket/dags/
gsutil cp -r examples/multi_airtable_to_bq_test gs://us-east1-dev2023-cc1-b088c7e1-bucket/dags/airtable_to_bq_config/
gsutil cp examples/single_airtable_to_bq_test.json gs://us-east1-dev2023-cc1-b088c7e1-bucket/dags/airtable_to_bq_config/
gsutil cp examples/single_bq_to_airtable_test.json gs://us-east1-dev2023-cc1-b088c7e1-bucket/dags/bq_to_airtable_config/
gsutil cp -r examples/multi_bq_to_airtable_test gs://us-east1-dev2023-cc1-b088c7e1-bucket/dags/bq_to_airtable_config/
gsutil cp examples/sql/* gs://us-east1-dev2023-cc1-b088c7e1-bucket/dags/sql/airtable_to_bq/multi_airtable_to_bq_test/
gsutil cp examples/sql/* gs://us-east1-dev2023-cc1-b088c7e1-bucket/dags/sql/bq_to_airtable/multi_bq_to_airtable_test/
gsutil cp examples/sql/base2_table1_input.sql gs://us-east1-dev2023-cc1-b088c7e1-bucket/dags/sql/bq_to_airtable/single_bq_to_airtable_test/
gsutil cp examples/sql/default_merge.sql gs://us-east1-dev2023-cc1-b088c7e1-bucket/dags/sql/airtable_to_bq/single_airtable_to_bq_test/
gsutil cp examples/schemas/* gs://airflow-data-exchange-development/schemas/airtable_to_bq/multi_airtable_to_bq_test/
gsutil cp examples/schemas/default.json gs://airflow-data-exchange-development/schemas/airtable_to_bq/single_airtable_to_bq_test/
