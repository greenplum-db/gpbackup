SELECT
  table_schema,
  table_name, 
  big.cnt_rows(table_name)
FROM 
  information_schema.tables
WHERE
  table_schema = 'big'
  AND table_type='BASE TABLE'
ORDER BY
  table_schema,
  table_name
  
