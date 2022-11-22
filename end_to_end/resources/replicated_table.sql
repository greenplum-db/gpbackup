DROP SCHEMA IF EXISTS schemaone CASCADE;
CREATE SCHEMA schemaone;
CREATE TABLE schemaone.test_table (a int, b int, c int) DISTRIBUTED REPLICATED;
INSERT INTO schemaone.test_table SELECT i,i,0 FROM generate_series(1,100) I;
