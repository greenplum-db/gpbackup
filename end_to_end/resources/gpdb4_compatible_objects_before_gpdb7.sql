CREATE PROCEDURAL LANGUAGE plpythonu;


CREATE TABLE part_with_ext (
    id integer,
    year integer,
    qtr integer,
    day integer,
    region text
) DISTRIBUTED BY (id) PARTITION BY RANGE(year)
          (
          PARTITION yr_1 START (2010) END (2011) EVERY (1) WITH (tablename='sales_1_prt_yr_1', appendonly=false ),
          PARTITION yr_2 START (2011) END (2012) EVERY (1) WITH (tablename='sales_1_prt_yr_2', appendonly=false ),
          PARTITION yr_3 START (2012) END (2013) EVERY (1) WITH (tablename='sales_1_prt_yr_3', appendonly=false ),
          PARTITION yr_4 START (2013) END (2014) EVERY (1) WITH (tablename='sales_1_prt_yr_4', appendonly=false )
          );
-- FIXME: Parallel metadata restore does not correctly assign the EXCHANGE PARTITION statements to the same cohort as the parent table, which causes the
--        EXCHANGE PARTITION to fail for certain tests.
-- Error encountered when executing statement: ALTER TABLE public.part_with_ext EXCHANGE PARTITION yr_1 WITH TABLE public.sales_1_prt_yr_1_ext_part_ WITHOUT VALIDATION;
-- Error was: ERROR: relation "public.part_with_ext" does not exist (SQLSTATE 42P01)
-- This was masked previously because the
-- ALTER TABLE part_with_ext EXCHANGE PARTITION yr_1 WITH TABLE sales_1_prt_yr_1_external_partition__ WITHOUT VALIDATION;
-- DROP TABLE sales_1_prt_yr_1_external_partition__;


CREATE TRIGGER sync_trigger_table1
    AFTER INSERT OR DELETE OR UPDATE ON trigger_table1
    FOR EACH STATEMENT
    EXECUTE PROCEDURE "RI_FKey_check_ins"();
