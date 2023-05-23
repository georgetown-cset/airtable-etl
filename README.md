# Airtable ETL

This repository contains DAG generators for:

* Putting data that meets certain criteria into an existing Airtable base and table
* Putting Airtable data that meets certain criteria into BigQuery

If your use case fits one of these templates, you can simply add a new configuration json (see below) and
the appropriate DAG generator will create a pipeline for you. Otherwise, hopefully these pipelines are a
useful starting point for further development.

## Adding a new BQ -> Airtable pipeline

To create a new BQ to Airtable pipeline, create a json with the following fields:

* `name` - Name of your pipeline (matching `[A-Za-z0-9_]+`)
* `schedule_interval` - Cron-style string indicating when your pipeline should run. Specify `null` if you will trigger
your pipeline another way.
* `input_data` - Name of query (no file extension) that can be found in `gs://<your dag dir>/dags/sql/bq_to_airtable/<name>/`.
This query should be used to retrieve the data you want to put in Airtable
* `airtable_table` - Name of the Airtable table that should receive the data
* `airtable_base` - Airtable Base ID containing `airtable_table`. To find the Base ID, open the table you want to
add data to in Airtable, then look at the url. The Base ID appears directly after the domain name, i.e. https://airtable.com/ **appvnA46jraScMMth** /tblfDL6s8f3LWb0C2/viwBnWFt0W8D5UcuB?blocks=hide
* [Optional] `column_map` - A dict mapping BigQuery column names to the column names you want to appear in Airtable

Put your JSON in `gs://<your dag dir>/dags/bq_to_airtable_config/` and `bq_to_airtable.py` will generate a DAG from it.

## Adding a new Airtable -> BQ pipeline

To create a new Airtable to BQ pipeline, create a json with the following fields:

* `name` - Name of your pipeline (matching `[A-Za-z0-9_]+`)
* `schedule_interval` - Cron-style string indicating when your pipeline should run. Specify `null` if you will trigger
your pipeline another way.
* `airtable_table` - Name of the Airtable table that you want to retrieve data from
* `airtable_base` - Airtable Base ID containing `airtable_table`. To find the Base ID, open the table you want to
add data to in Airtable, then look at the url. The Base ID appears directly after the domain name, i.e. https://airtable.com/ **appvnA46jraScMMth** /tblfDL6s8f3LWb0C2/viwBnWFt0W8D5UcuB?blocks=hide
* `schema_name` - Name of schema (no file extension) that can be found in `gs://airflow-data-exchange(development)?/schemas/airtable_to_bq/<name>/`
* `filter_query` - Name of query (no file extension) that can be found in `gs://<your dag dir>/dags/sql/airtable_to_bq/<name>/`.
This query will be used to generate the updated production table from the contents of the full Airtable table and the
current production table.
* `production_dataset` - BQ dataset containing production table
* `production_table` - Production BQ table we are updating within `production_dataset`
* [Optional] `column_map` - A dict mapping Airtable column names to the column names you want to appear in BigQuery

Put your JSON in `gs://<your dag dir>/dags/airtable_to_bq_config/` and `airtable_to_bq.py` will generate a DAG from it.
