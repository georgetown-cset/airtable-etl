SELECT * FROM {{ params.staging_dataset }}.{{ params.staging_table_name }} WHERE foo IS NOT NULL
