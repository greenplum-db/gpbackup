--
-- Greenplum Database database dump
--

SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;

SET default_with_oids = false;


SET search_path = public, pg_catalog;

SET default_tablespace = '';

INSERT INTO foo SELECT generate_series(1,40000);

--
--


INSERT INTO holds SELECT generate_series(1,50000);

--
--

COPY sales (id, date, amt) FROM stdin;
1	2017-01-01	20.00
3	2017-03-01	20.00
25	2017-05-01	20.00
2	2017-02-01	20.00
4	2017-04-01	20.00
8	2017-08-01	20.00
15	2017-05-01	20.00
5	2017-05-01	20.00
9	2017-09-01	20.00
7	2017-07-01	20.00
11	2017-11-01	20.00
18	2017-08-01	20.00
12	2017-12-01	20.00
\.


SET search_path = schema2, pg_catalog;

--
--

INSERT INTO foo3 SELECT generate_series(201,300);

INSERT INTO ao1 SELECT generate_series(1,1000);

INSERT INTO ao2 SELECT generate_series(1,1000);

--
--

COPY returns (id, date, amt) FROM stdin;
2	2017-02-01	20.00
7	2017-07-01	20.00
25	2017-05-01	20.00
15	2017-05-01	20.00
11	2017-11-01	20.00
12	2017-12-01	20.00
\.


--
-- Greenplum Database database dump complete
--

