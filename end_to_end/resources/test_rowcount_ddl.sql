CREATE OR REPLACE FUNCTION 
    cnt_rows()
  RETURNS TABLE(table_schema text, table_name text, seg_id int, row_count bigint)
  LANGUAGE plpgsql AS
$func$
DECLARE
   schemaname text;
   tablename text;
BEGIN
    FOR schemaname, tablename IN
        SELECT 
            tb.table_schema,
            tb.table_name
        FROM   
            information_schema.tables tb
        WHERE  
            tb.table_schema not in ('pg_catalog', 'gp_toolkit', 'information_schema', 'pg_toast', 'pg_aoseg')
            AND tb.table_type='BASE TABLE'
        ORDER BY
            tb.table_schema,
            tb.table_name
    LOOP
        RETURN QUERY EXECUTE
            format('SELECT cast(%L as text) as table_schema, cast(%L as text) as table_name, gp_segment_id as seg_id, count(1) as row_count FROM %I.%I group by gp_segment_id order by gp_segment_id;',
                schemaname, tablename, schemaname, tablename);
    END LOOP;
END
$func$;
