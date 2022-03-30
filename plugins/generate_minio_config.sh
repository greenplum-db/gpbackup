#!/bin/bash

cat <<MINIO_CONFIG > /tmp/minio_config.yaml
executablepath: $(which gpbackup_s3_plugin)
options:
  endpoint: http://localhost:9000/
  aws_access_key_id: minioadmin
  aws_secret_access_key: minioadmin
  bucket: gpbackup-s3-test
  folder: test/backup
MINIO_CONFIG
