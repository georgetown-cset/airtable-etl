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
* [Optional] `column_map` - A dict mapping BigQuery column names to the column names you want to appear in Airtable. Mapping does not have to cover all columns; unspecified columns will use BigQuery names

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
* [Optional] `new_query` - Name of query (no file extension) that can be found in `gs://<your dag dir>/dags/sql/airtable_to_bq/<name>/`.
This query will be used to filter only the new rows from the contents of the full Airtable table and save them.
* `merge_query` - Name of query (no file extension) that can be found in `gs://<your dag dir>/dags/sql/airtable_to_bq/<name>/`.
This query will be used to generate the updated production table from the contents of the full Airtable table and the
current production table.
* `production_dataset` - BQ dataset containing production table
* `production_table` - Production BQ table we are updating within `production_dataset`
* [Optional] `column_map` - A dict mapping Airtable column names to the column names you want to appear in BigQuery. Mapping does not have to cover all columns; unspecified columns will use Airtable names. To exclude columns from BigQuery, map them to the string "EXCLUDE"

Put your JSON in `gs://<your dag dir>/dags/airtable_to_bq_config/` and `airtable_to_bq.py` will generate a DAG from it.

## Adding multiple imports/exports per pipeline

You can now specify both single or multiple imports/exports per pipeline. As you can see in the `examples` directory,
single exports should be specified in a single config file within
`gs://<your dag dir>/dags/{airtable_to_bq or bq_to_airtable, as appropriate}_config/`. Multiple exports can be defined
with a directory under `gs://<your dag dir>/dags/`. The pipeline will be named after the directory name. Shared
configuration should go in a `config.json`, while table-specific configuration should go in individual config files.
* Make sure the `name` field inside the table-specific config is unique and not a duplicate name of any tables inside the `staging_airtable_to_bq` dataset in BigQuery.

## Updating a single Airtable -> BQ pipeline to include multiple Airtable tables
To update a single Airtable -> BQ pipeline in `gs://<your dag dir>/dags/airtable_to_bq_config/{current_pipeline}.json`, make a folder in `gs://<your dag dir>/dags/airtable_to_bq_config/`
* The name of the folder will be the same as the `name` field in `{current_pipeline}.json`
* `config.json` - Create this file inside the new folder. Move the `schedule_interval` and `production_dataset` from `{current_pipeline}.json` to this file
* Move `{current_pipeline}.json` inside the folder
* `Table specific config files`: Create a new config file `{new_table}.json` for tables inside this folder
    * Make sure the `name` field inside this config is unique and not a duplicate name of any tables inside the `staging_airtable_to_bq` dataset in BigQuery.

SQL files:
* Create a folder (if it doesn't exist) with the same name as the previously created folder inside `gs://<your dag dir>/dags/sql/airtable_to_bq/`
* Move the pipeline's existing SQL file inside this folder
* Create the merge query `{new_table}_merge.sql` for the new table inside this folder (and optional new query)

Schema files:
* Create a folder (if it doesn't exist) with the same name as the previously created folder inside `gs://airflow-data-exchange/schemas/airtable_to_bq/`
* Move the pipeline's existing schema file inside this folder
* Create the schema file `{new_table}.json` for the new table inside this folder

## Adding a column to an Airtable table that goes to BQ
1. Add the column name to the `merge query`
    * If the merge query references the prod table, manually add the column to the prod table in BQ
2. Add the column name to `column_map` inside the table's json config
3. Add the column name to the table's `schema file`

## Airtable credentials

We're using a [Personal Access Token](https://airtable.com/developers/web/guides/personal-access-tokens) to authenticate with Airtable.
This type of token is tied to a particular user (currently jd1881), so deleting that user from our Airtable organization will invalidate the credentials.
To use a new token, create it in Airtable, then update in [Secret Manager](https://cloud.google.com/secret-manager/docs/overview) the `ETO_scout_airtable` secret, and any others as necessary.
