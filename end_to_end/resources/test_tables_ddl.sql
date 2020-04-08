--
-- Greenplum Database database dump
--

SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;

SET default_with_oids = false;

--
--

CREATE SCHEMA schema2;



SET search_path = public, pg_catalog;

SET default_tablespace = '';

--
--

CREATE SEQUENCE myseq1 START 100;

CREATE SEQUENCE myseq2 START 100;

CREATE VIEW myview2 AS SELECT '1';

--
--

CREATE TABLE foo (
    i integer DEFAULT nextval('myseq1') NOT NULL
) DISTRIBUTED BY (i);

CREATE VIEW myview1 AS SELECT * from foo;

--
--

CREATE TABLE holds (
    i integer
) DISTRIBUTED BY (i);

--
--


CREATE TABLE sales (
    id integer,
    date date,
    amt numeric(10,2)
) DISTRIBUTED BY (id) PARTITION BY RANGE(date)
          (
          PARTITION jan17 START ('2017-01-01'::date) END ('2017-02-01'::date) WITH (tablename='sales_1_prt_jan17', appendonly=false ),
          PARTITION feb17 START ('2017-02-01'::date) END ('2017-03-01'::date) WITH (tablename='sales_1_prt_feb17', appendonly=false ),
          PARTITION mar17 START ('2017-03-01'::date) END ('2017-04-01'::date) WITH (tablename='sales_1_prt_mar17', appendonly=false ),
          PARTITION apr17 START ('2017-04-01'::date) END ('2017-05-01'::date) WITH (tablename='sales_1_prt_apr17', appendonly=false ),
          PARTITION may17 START ('2017-05-01'::date) END ('2017-06-01'::date) WITH (tablename='sales_1_prt_may17', appendonly=false ),
          PARTITION jun17 START ('2017-06-01'::date) END ('2017-07-01'::date) WITH (tablename='sales_1_prt_jun17', appendonly=false ),
          PARTITION jul17 START ('2017-07-01'::date) END ('2017-08-01'::date) WITH (tablename='sales_1_prt_jul17', appendonly=false ),
          PARTITION aug17 START ('2017-08-01'::date) END ('2017-09-01'::date) WITH (tablename='sales_1_prt_aug17', appendonly=false ),
          PARTITION sep17 START ('2017-09-01'::date) END ('2017-10-01'::date) WITH (tablename='sales_1_prt_sep17', appendonly=false ),
          PARTITION oct17 START ('2017-10-01'::date) END ('2017-11-01'::date) WITH (tablename='sales_1_prt_oct17', appendonly=false ),
          PARTITION nov17 START ('2017-11-01'::date) END ('2017-12-01'::date) WITH (tablename='sales_1_prt_nov17', appendonly=false ),
          PARTITION dec17 START ('2017-12-01'::date) END ('2018-01-01'::date) WITH (tablename='sales_1_prt_dec17', appendonly=false )
          );


SET search_path = schema2, pg_catalog;

--
--

CREATE TABLE foo2 (
    i integer
) DISTRIBUTED BY (i);



--
--



CREATE TABLE foo3 (
    i integer
) DISTRIBUTED BY (i);

--
--

CREATE TABLE returns (
    id integer,
    date date,
    amt numeric(10,2)
) DISTRIBUTED BY (id) PARTITION BY RANGE(date)
          (
          PARTITION jan17 START ('2017-01-01'::date) END ('2017-02-01'::date) WITH (tablename='returns_1_prt_jan17', appendonly=false ),
          PARTITION feb17 START ('2017-02-01'::date) END ('2017-03-01'::date) WITH (tablename='returns_1_prt_feb17', appendonly=false ),
          PARTITION mar17 START ('2017-03-01'::date) END ('2017-04-01'::date) WITH (tablename='returns_1_prt_mar17', appendonly=false ),
          PARTITION apr17 START ('2017-04-01'::date) END ('2017-05-01'::date) WITH (tablename='returns_1_prt_apr17', appendonly=false ),
          PARTITION may17 START ('2017-05-01'::date) END ('2017-06-01'::date) WITH (tablename='returns_1_prt_may17', appendonly=false ),
          PARTITION jun17 START ('2017-06-01'::date) END ('2017-07-01'::date) WITH (tablename='returns_1_prt_jun17', appendonly=false ),
          PARTITION jul17 START ('2017-07-01'::date) END ('2017-08-01'::date) WITH (tablename='returns_1_prt_jul17', appendonly=false ),
          PARTITION aug17 START ('2017-08-01'::date) END ('2017-09-01'::date) WITH (tablename='returns_1_prt_aug17', appendonly=false ),
          PARTITION sep17 START ('2017-09-01'::date) END ('2017-10-01'::date) WITH (tablename='returns_1_prt_sep17', appendonly=false ),
          PARTITION oct17 START ('2017-10-01'::date) END ('2017-11-01'::date) WITH (tablename='returns_1_prt_oct17', appendonly=false ),
          PARTITION nov17 START ('2017-11-01'::date) END ('2017-12-01'::date) WITH (tablename='returns_1_prt_nov17', appendonly=false ),
          PARTITION dec17 START ('2017-12-01'::date) END ('2018-01-01'::date) WITH (tablename='returns_1_prt_dec17', appendonly=false )
          );

CREATE TABLE ao1 (
    i integer
) WITH (appendonly=true);

CREATE TABLE ao2 (
    i integer
) WITH (appendonly=true, orientation=column);

--
--

REVOKE ALL ON SCHEMA public FROM PUBLIC;
GRANT ALL ON SCHEMA public TO PUBLIC;

CREATE TABLE public."FOObar" (i int);
insert into public."FOObar" values (1);

--
-- Greenplum Database database dump complete
--

