SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;

SET default_with_oids = false;
SET search_path = public, pg_catalog;


CREATE TYPE enum_type AS ENUM (
    '750582',
    '750583',
    '750584'
);


CREATE FUNCTION plusone(x character varying) RETURNS character varying
    AS $$
BEGIN
    RETURN x || 'a';
END;
$$
    LANGUAGE plpgsql NO SQL
    SET standard_conforming_strings TO 'on'
    SET client_min_messages TO 'notice'
    SET search_path TO public;


CREATE FUNCTION return_enum_as_array(anyenum, anyelement, anyelement) RETURNS TABLE(ae anyenum, aa anyarray)
    AS $_$
SELECT $1, array[$2, $3]
$_$
    LANGUAGE sql STABLE CONTAINS SQL;



SET default_tablespace = '';


CREATE CAST (text AS integer) WITH FUNCTION public.casttoint(text) AS ASSIGNMENT;


COMMENT ON CAST (text AS integer) IS 'sample cast';



CREATE TEXT SEARCH DICTIONARY testdictionary (
    TEMPLATE = pg_catalog.snowball,
    language = 'russian', stopwords = 'russian' );


CREATE TEXT SEARCH CONFIGURATION testconfiguration (
    PARSER = pg_catalog."default" );


CREATE TEXT SEARCH TEMPLATE testtemplate (
    LEXIZE = dsimple_lexize );

CREATE TEXT SEARCH DICTIONARY testdictionary2 (
    TEMPLATE = public.testtemplate);


CREATE TEXT SEARCH PARSER testparser (
    START = prsd_start,
    GETTOKEN = prsd_nexttoken,
    END = prsd_end,
    LEXTYPES = prsd_lextype );

CREATE TEXT SEARCH CONFIGURATION testconfiguration2 (
    PARSER = public.testparser );

CREATE VIEW ts_config_view AS SELECT * FROM ts_debug('public.testconfiguration2', '
PostgreSQL, the highly scalable, SQL compliant, open source
object-relational database management system, is now undergoing
beta testing of the nextversion of our software');

CREATE VIEW ts_dict_view AS SELECT ts_lexize('public.testdictionary2', 'hello world');

CREATE OPERATOR FAMILY test_fam USING hash;

CREATE OPERATOR CLASS test_op_class
    FOR TYPE _int4 USING hash FAMILY test_fam AS
    STORAGE _int4;
