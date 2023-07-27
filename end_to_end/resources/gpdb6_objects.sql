SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;

SET search_path = public, pg_catalog;

SET default_with_oids = false;


CREATE TYPE employee_type AS (
	name text,
	salary numeric
);


CREATE FUNCTION add(integer, integer) RETURNS integer
    LANGUAGE sql WINDOW CONTAINS SQL
    AS $_$SELECT $1 + $2$_$;


CREATE FUNCTION mleast(VARIADIC arr numeric[]) RETURNS numeric
    LANGUAGE sql CONTAINS SQL
    AS $_$
    SELECT min($1[i]) FROM generate_subscripts($1, 1) g(i);
$_$;


CREATE FOREIGN DATA WRAPPER fdw;

CREATE SERVER sc FOREIGN DATA WRAPPER fdw;

CREATE USER MAPPING FOR testrole
        SERVER sc
        OPTIONS (user 'foreign_user', password 'password');


CREATE FOREIGN TABLE ft1 (
    c1 integer OPTIONS (param1 'val1') NOT NULL,
    c2 text OPTIONS (param2 'val2', param3 'val3')
) SERVER sc OPTIONS (delimiter ',', quote '"');

CREATE OR REPLACE FUNCTION do_nothing()
  RETURNS event_trigger
 LANGUAGE plpgsql
  AS $$
BEGIN END;
$$;

CREATE EVENT TRIGGER nothing_ddl ON ddl_command_start
   WHEN TAG IN ('ALTER SERVER')
   EXECUTE PROCEDURE do_nothing();

CREATE COLLATION public.some_coll (lc_collate = 'POSIX', lc_ctype = 'POSIX');

CREATE TYPE public.textrange AS RANGE (
	SUBTYPE_OPCLASS = public.range_class,
	SUBTYPE = pg_catalog.text,
	COLLATION = public.some_coll
);

CREATE TYPE public.colors AS ENUM (
    'red',
    'green',
    'blue'
);
--CREATE TABLE public.legacy_enum (
--    color public.colors
--) DISTRIBUTED BY (color cdbhash_enum_ops);

CREATE TABLE aa (
    a integer NOT NULL,
    b integer NOT NULL,
    c integer
) DISTRIBUTED BY (a, b);
ALTER TABLE ONLY aa REPLICA IDENTITY FULL;

CREATE TABLE t (a int, b text);
SECURITY LABEL ON TABLE t IS 'classified';

CREATE OR REPLACE FUNCTION test_function()
  RETURNS TRIGGER
 LANGUAGE plpgsql
  AS $$
BEGIN END;
$$;

CREATE CONSTRAINT TRIGGER ensure_user_role_exists
AFTER INSERT OR UPDATE ON aa
FOR EACH ROW
EXECUTE PROCEDURE test_function();
