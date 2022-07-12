#!/bin/bash
set -x
set -euo pipefail

yum install -y postgresql-devel python-dev
pip3 install psycopg2-binary
pip3 install slack-sdk

python3 gpbackup/ci/scripts/scale_test_slack_notify.py
