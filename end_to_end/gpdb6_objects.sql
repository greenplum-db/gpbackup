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
