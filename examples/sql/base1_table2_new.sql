SELECT *
FROM {{ params.staging_dataset }}.{{ params.staging_table_name }}
WHERE length(some_notes) > 20
