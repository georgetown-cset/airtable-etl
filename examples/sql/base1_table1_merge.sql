SELECT
  foo,
  bar,
  baz
FROM {{ params.staging_dataset }}.{{ params.staging_table_name }}
UNION ALL
SELECT
  foo,
  bar,
  baz
FROM
  {{ params.production_dataset }}.{{ params.production_table_name }}
WHERE foo NOT IN (SELECT foo FROM {{ params.staging_dataset }}.{{ params.staging_table_name }})
