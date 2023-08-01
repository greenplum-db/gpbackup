#!/usr/bin/python3

import os
from time import sleep
import psycopg2
from slack_sdk.webhook import WebhookClient

## Constants for managing reporting
RESULTS_DATABASE_HOST = os.environ.get('RESULTS_DATABASE_HOST')
RESULTS_DATABASE_USER = os.environ.get('RESULTS_DATABASE_USER')
RESULTS_DATABASE_NAME = os.environ.get('RESULTS_DATABASE_NAME')
RESULTS_DATABASE_PASSWORD = os.environ.get('RESULTS_DATABASE_PASSWORD')
SLACK_WEBHOOK_URL = os.environ.get('SLACK_WEBHOOK_URL')


def get_unreported_failures():
    conn = psycopg2.connect(
        host=RESULTS_DATABASE_HOST, 
        user=RESULTS_DATABASE_USER, 
        dbname=RESULTS_DATABASE_NAME, 
        password=RESULTS_DATABASE_PASSWORD
    )
    cur = conn.cursor()

    select_string = """
        SELECT
            tr.test_id,
            tn.test_name,
            tr.run_timestamp,
            tr.test_runtime,
            tr.gpbackup_version,
            tr.gpdb_version,
            tr.was_reported
        FROM 
            prod.test_names tn
            left join prod.test_runs tr
                on tn.test_id = tr.test_id
        WHERE
            tr.was_failed = true
            and tr.was_reported = false
        ;
    """

    cur.execute(select_string)
    unreported_failures = cur.fetchall()
    conn.commit()
    cur.close()
    conn.close()
    return unreported_failures

def update_unreported_failures():
    conn = psycopg2.connect(
        host=RESULTS_DATABASE_HOST, 
        user=RESULTS_DATABASE_USER, 
        dbname=RESULTS_DATABASE_NAME, 
        password=RESULTS_DATABASE_PASSWORD
    )
    cur = conn.cursor()

    update_string = """
    UPDATE prod.test_runs
    SET
        was_reported = true
    WHERE
        was_failed = true
        and was_reported = false
    """

    cur.execute(update_string)
    conn.commit()
    cur.close()
    conn.close()
    return

def send_slack_notification(unreported_failures):
    webhook = WebhookClient(SLACK_WEBHOOK_URL)
    webhook.send(
        text=f"""
        gpbackup/gprestore Scale perf regressions for the following test runs:
        {unreported_failures}
        """
    )

def main():
    try:
        print("Examining runtime database to report test failures")
        unreported_failures = get_unreported_failures()
        if unreported_failures:
            print("Reporting test failures")
            send_slack_notification(unreported_failures)
            update_unreported_failures()
        else:
            print("No test failures found to report")

    except Exception as e:
        print("Python script errored: {}".format(e))
        return

if __name__ == "__main__":
    main()
