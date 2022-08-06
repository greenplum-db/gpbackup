#!/usr/bin/python2

import os
import sys
from datetime import datetime
from pygresql import pg

## Constants for interacting with refdb
RESULTS_LOG_FILE = os.environ.get('RESULTS_LOG_FILE')
RESULTS_DATABASE_HOST = os.environ.get('RESULTS_DATABASE_HOST')
RESULTS_DATABASE_USER = os.environ.get('RESULTS_DATABASE_USER')
RESULTS_DATABASE_NAME = os.environ.get('RESULTS_DATABASE_NAME')
RESULTS_DATABASE_PASSWORD = os.environ.get('RESULTS_DATABASE_PASSWORD')
GPDB_VERSION = os.environ.get('GPDB_VERSION').replace("gpstart version ", "")
GPB_VERSION = os.environ.get('GPB_VERSION').replace("gpbackup version ", "")


def parse_runtime(logline):
    runtime = int(float(logline.replace("TEST RUNTIME: ", "").replace("\n", "").replace("\r", "")))
    return runtime

def parse_log():
    """Extract required info from log file"""
    log_dict = {}
    with open(RESULTS_LOG_FILE, 'r') as fp:
        log_lines = fp.readlines()

    for line in log_lines:
        if line.startswith('TEST RUNTIME'):
            runtime = parse_runtime(line)
            log_dict['test_runtime'] = runtime
    return log_dict

def get_test_id(test_name):
    conn = pg.connect(
        dbname=RESULTS_DATABASE_NAME,
        host=RESULTS_DATABASE_HOST,
        user=RESULTS_DATABASE_USER,
        passwd=RESULTS_DATABASE_PASSWORD
    )

    select_string = """
        SELECT
            tn.test_id
        FROM 
            prod.test_names tn
        WHERE
            tn.test_name = '{}'
        ;
    """.format(test_name)

    query = conn.query(select_string)
    result = query.getresult()
    result = result[0] # unpack list of single tuple
    conn.close()

    test_id = result[0]

    return test_id


def get_stats(test_name):
    """Retrieve stats for given test name from provided postgres database"""
    stats_dict = {}
    conn = pg.connect(
        dbname=RESULTS_DATABASE_NAME,
        host=RESULTS_DATABASE_HOST,
        user=RESULTS_DATABASE_USER,
        passwd=RESULTS_DATABASE_PASSWORD
    )

    select_string = """
        SELECT
            tn.test_id,
            tn.test_name,
            ts.test_runs_included,
            ts.test_runtime_avg,
            ts.test_runtime_var,
            ts.test_limit_report,
            ts.test_limit_fail
        FROM 
            prod.test_names tn
            LEFT JOIN prod.test_stats ts
                ON ts.test_id = tn.test_id
        WHERE
            tn.test_name = '{}'
        ;
    """.format(test_name)

    query = conn.query(select_string)
    result = query.getresult()
    conn.close()

    if result:
        result = result[0] # unpack list of single tuple
        stats_dict['test_id'] = result[0]
        stats_dict['test_name'] = result[1]
        stats_dict['test_runs_included'] = result[2] or 0
        stats_dict['test_runtime_avg'] = result[3] or 0
        stats_dict['test_runtime_var'] = result[4] or 0
        stats_dict['test_limit_report'] = result[5] or 99999
        stats_dict['test_limit_fail'] = result[6] or 99999

    return stats_dict

def analyze_stats(run_stats, summary_stats, test_name):
    """
    Possible result values are: "pass" "fail" "report"
    """
    run_stats['was_failed'] = "false"
    run_stats['was_reported'] = "false"

    if (
        summary_stats.get('test_runs_included', 0) >= 10
        and run_stats.get('test_runtime') >= summary_stats.get('test_limit_fail')
        ):
        run_stats['was_failed'] = "true"

    if run_stats.get('was_failed') == "true":
        result_string = "Failed"
    else:
        result_string = "Passed"

    report_string = """
    ############################################################
    Test Name: {test_name}
    Runtime: {run_time}
    Compared against: {summary_stats}
    Comparison result: {comp_result}
    ############################################################
    """.format(
        test_name = test_name,
        run_time = run_stats.get('test_runtime'),
        summary_stats = summary_stats,
        comp_result = result_string
    )
    print report_string
    return

def store_stats(run_stats, test_id, test_name):
    now_ts = datetime.now()
    now_ts_str = datetime.strftime(now_ts, "%Y-%m-%d-%H-%M-%S")
    conn = pg.connect(
        dbname=RESULTS_DATABASE_NAME,
        host=RESULTS_DATABASE_HOST,
        user=RESULTS_DATABASE_USER,
        passwd=RESULTS_DATABASE_PASSWORD
    )

    insert_qry_string = """
    INSERT INTO prod.test_runs(test_id, run_timestamp, test_runtime, gpbackup_version, gpdb_version, was_reported, was_failed)
    VALUES
        ({id}, '{ts}', {runtime}, '{gpbver}', '{gpdbver}', {reported}, {failed})
    ;
    """.format(
        id=test_id, 
        ts=now_ts_str,
        gpbver=GPB_VERSION,
        gpdbver=GPDB_VERSION,
        runtime=run_stats.get('test_runtime', 0),
        reported=run_stats.get('was_reported', False),
        failed=run_stats.get('was_failed', True)
    )

    summ_func_qry_string = """
    SELECT prod.summarize_runs('{}');
    """.format(test_name)

    conn.query(insert_qry_string)
    conn.query(summ_func_qry_string)
    # conn.commit()
    conn.close()
    return

def main():
    try:
        TEST_NAME = sys.argv[1]
        run_stats = parse_log()
        test_id = get_test_id(TEST_NAME)
        summary_stats = get_stats(TEST_NAME)
        analyze_stats(run_stats, summary_stats, TEST_NAME)
        store_stats(run_stats, test_id, TEST_NAME)
        return
    except Exception as e:
        print "Python script errored: {}".format(e)
        return

if __name__ == "__main__":
    main()
