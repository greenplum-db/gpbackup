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

CREATE FOREIGN TABLE ft1 (
    c1 integer OPTIONS (param1 'val1') NOT NULL,
    c2 text OPTIONS (param2 'val2', param3 'val3')
) SERVER sc OPTIONS (delimiter ',', quote '"');

CREATE OR REPLACE FUNCTION abort_any_command()
  RETURNS event_trigger
 LANGUAGE plpgsql
  AS $$
BEGIN
  RAISE EXCEPTION 'command % is disabled', tg_tag;
END;
$$;

CREATE EVENT TRIGGER abort_ddl ON ddl_command_start
   EXECUTE PROCEDURE abort_any_command();
