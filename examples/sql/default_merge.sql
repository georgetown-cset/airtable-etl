SELECT
  Name,
  some_notes,
  current_date() AS import_date
FROM {{ params.staging_dataset }}.{{ params.staging_table_name }}
UNION ALL
SELECT
  Name,
  some_notes,
  import_date
FROM {{ params.production_dataset }}.{{ params.production_table_name }}
