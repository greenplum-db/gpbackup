CREATE PROCEDURAL LANGUAGE plpython3u;


CREATE TABLE part_with_ext (
    id integer,
    year integer,
    qtr integer,
    day integer,
    region text
) DISTRIBUTED BY (id) PARTITION BY RANGE(year)
          (
          PARTITION yr_2 START (2011) END (2012) EVERY (1) WITH (tablename='sales_1_prt_yr_2', appendonly=false ),
          PARTITION yr_3 START (2012) END (2013) EVERY (1) WITH (tablename='sales_1_prt_yr_3', appendonly=false ),
          PARTITION yr_4 START (2013) END (2014) EVERY (1) WITH (tablename='sales_1_prt_yr_4', appendonly=false )
          );
-- TODO: Fix dependency issues with external leaf partitions for GPDB 7
--ALTER TABLE part_with_ext ATTACH PARTITION sales_1_prt_yr_1_external_partition__ FOR VALUES FROM ('2010') TO ('2011');


CREATE TRIGGER sync_trigger_table1
    AFTER INSERT ON trigger_table1
    FOR EACH ROW
    EXECUTE PROCEDURE "RI_FKey_check_ins"();
