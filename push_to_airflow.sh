gcloud storage rm -r gs://us-east1-production-cc2-202-b42a7a54-bucket/dags/airtable_scripts
gcloud storage cp -r airtable_scripts gs://us-east1-production-cc2-202-b42a7a54-bucket/dags/
gcloud storage cp bq_to_airtable.py gs://us-east1-production-cc2-202-b42a7a54-bucket/dags/
gcloud storage cp airtable_to_bq.py gs://us-east1-production-cc2-202-b42a7a54-bucket/dags/
gcloud storage cp -r examples/multi_airtable_to_bq_test gs://us-east1-production-cc2-202-b42a7a54-bucket/dags/airtable_to_bq_config/
gcloud storage cp examples/single_airtable_to_bq_test.json gs://us-east1-production-cc2-202-b42a7a54-bucket/dags/airtable_to_bq_config/
gcloud storage cp examples/single_bq_to_airtable_test.json gs://us-east1-production-cc2-202-b42a7a54-bucket/dags/bq_to_airtable_config/
gcloud storage cp -r examples/multi_bq_to_airtable_test gs://us-east1-production-cc2-202-b42a7a54-bucket/dags/bq_to_airtable_config/
gcloud storage cp examples/sql/* gs://us-east1-production-cc2-202-b42a7a54-bucket/dags/sql/airtable_to_bq/multi_airtable_to_bq_test/
gcloud storage cp examples/sql/* gs://us-east1-production-cc2-202-b42a7a54-bucket/dags/sql/bq_to_airtable/multi_bq_to_airtable_test/
gcloud storage cp examples/sql/base2_table1_input.sql gs://us-east1-production-cc2-202-b42a7a54-bucket/dags/sql/bq_to_airtable/single_bq_to_airtable_test/
gcloud storage cp examples/sql/default_merge.sql gs://us-east1-production-cc2-202-b42a7a54-bucket/dags/sql/airtable_to_bq/single_airtable_to_bq_test/
gcloud storage cp examples/schemas/* gs://airflow-data-exchange-development/schemas/airtable_to_bq/multi_airtable_to_bq_test/
gcloud storage cp examples/schemas/default.json gs://airflow-data-exchange-development/schemas/airtable_to_bq/single_airtable_to_bq_test/
