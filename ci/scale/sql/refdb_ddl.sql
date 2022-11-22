DROP SCHEMA IF EXISTS prod CASCADE;
CREATE SCHEMA prod;

CREATE TABLE IF NOT EXISTS prod.test_names (
    test_id INTEGER,
    test_name TEXT,

    CONSTRAINT unique_test_names UNIQUE (test_id, test_name)
);

INSERT INTO prod.test_names(test_id, test_name)
VALUES
    (1, 'gpb_single_data_file_copy_q8'),
    (2, 'gpr_single_data_file_copy_q8'),
    (3, 'gpb_scale_multi_data_file'),
    (4, 'gpr_scale_multi_data_file'),
    (5, 'gpb_scale_multi_data_file_zstd'),
    (6, 'gpr_scale_multi_data_file_zstd'),
    (7, 'gpb_scale_single_data_file'),
    (8, 'gpr_scale_single_data_file'),
    (9, 'gpb_scale_single_data_file_zstd'),
    (10, 'gpr_scale_single_data_file_zstd'),
    (11, 'gpb_scale_metadata'),
    (12, 'gpr_scale_metadata'),
    (13, 'gpb_distr_snap_edit_data'),
    (14, 'gpr_distr_snap_edit_data'),
    (15, 'gpb_distr_snap_high_conc'),
    (16, 'gpr_distr_snap_high_conc')
;

CREATE TABLE IF NOT EXISTS prod.test_stats (
    test_id integer,
    test_name text,
    test_runs_included integer,
    test_runtime_avg bigint,
    test_runtime_var bigint,
    test_limit_report bigint,
    test_limit_fail bigint,

    CONSTRAINT unique_test_stats UNIQUE (test_id)
);

CREATE TABLE IF NOT EXISTS prod.test_runs (
    test_id integer,
    run_timestamp text,
    test_runtime bigint,
    gpbackup_version text,
    gpdb_version text,
    was_reported boolean,
    was_failed boolean
);

CREATE OR REPLACE function
    prod.summarize_runs(_testname text)
RETURNS
    VOID
AS 
$$
BEGIN
    DELETE FROM
        prod.test_stats ts
    WHERE 
        ts.test_id in (
            SELECT DISTINCT
                tn.test_id 
            FROM 
                prod.test_names tn
            WHERE
                tn.test_name = _testname
        )
    ;

    CREATE TEMPORARY TABLE temp_stats AS
    SELECT
        tn.test_id,
        tn.test_name,
        count(tr.test_runtime) AS test_runs_included,
        avg(tr.test_runtime) AS test_runtime_avg,
        variance(tr.test_runtime) AS test_runtime_var
    FROM
        prod.test_names tn
        LEFT JOIN prod.test_runs tr
            ON tn.test_id = tr.test_id
    WHERE
        tr.was_failed = false
        AND tr.was_reported = false
        AND tn.test_name = _testname
        AND tr.test_runtime > 0
    GROUP BY
        tn.test_id,
        tn.test_name
    ORDER BY
        tn.test_id,
        tn.test_name
    ;

    INSERT INTO prod.test_stats
    SELECT
        tmp.test_id,
        tmp.test_name,
        tmp.test_runs_included,
        tmp.test_runtime_avg,
        tmp.test_runtime_var,
        tmp.test_runtime_avg * 1.1 AS test_limit_report,
        tmp.test_runtime_avg * 1.2 AS test_limit_fail
    FROM
        temp_stats tmp
    ;

END;
$$ 
LANGUAGE 
    plpgsql;
