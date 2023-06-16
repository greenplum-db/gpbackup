#!/bin/bash

set -ex

. gpbackup/ci/scripts/setup-perf.bash

cat <<SCRIPT > /tmp/run_perf.bash
#!/bin/bash

set -e
source env.sh

TIMEFORMAT=%R

function print_header() {
    header="### \$1 ###"
    len=\$(echo \$header | awk '{print length}')
    printf "%0.s#" \$(seq 1 \$len) && echo
    echo -e "\$header"
    printf "%0.s#" \$(seq 1 \$len) && echo
}

function print_time_exec() {
  echo \$1
  time eval \$1
}

cat << CONFIG > \${HOME}/s3_config.yaml
executablepath: \${GPHOME}/bin/gpbackup_s3_plugin
options:
  region: ${REGION}
  aws_access_key_id: ${AWS_ACCESS_KEY_ID}
  aws_secret_access_key: ${AWS_SECRET_ACCESS_KEY}
  bucket: ${BUCKET}
  folder: test/backup
CONFIG

# ----------------------------------------------
# Run S3 Plugin scale tests
# ----------------------------------------------
pushd \${GOPATH}/src/github.com/greenplum-db/gpbackup/plugins
print_time_exec "./plugin_test_scale.sh \${GPHOME}/bin/gpbackup_s3_plugin ~/s3_config.yaml"
popd

# ----------------------------------------------
# Run S3 Plugin RESTORE_DIRECTORY and BACKUP_DIRECTORY serial
# ----------------------------------------------
mkdir -p /data/gpdata/stage1 /data/gpdata/stage2
pushd /data/gpdata/stage1
# Copy data from S3 to local using restore_directory
print_header "RESTORE_DIRECTORY (SERIAL) with ${SCALE_FACTOR} GB of data"
print_time_exec "\${GPHOME}/bin/gpbackup_s3_plugin restore_directory \
    ~/s3_config.yaml benchmark/tpch/lineitem/${SCALE_FACTOR}/lineitem_data"
echo
mkdir -p tmp/\$timestamp && mv benchmark tmp/\$timestamp
# Copy data from local to S3 using backup_directory
print_header "BACKUP_DIRECTORY (SERIAL) with ${SCALE_FACTOR} GB of data"
print_time_exec "\${GPHOME}/bin/gpbackup_s3_plugin backup_directory \
    ~/s3_config.yaml tmp/\$timestamp/benchmark/tpch/lineitem"
echo
rm -rf ~/tpch_data/tmp/\$timestamp

# ----------------------------------------------
# Run S3 Plugin RESTORE_DIRECTORY and BACKUP_DIRECTORY parallel
# ----------------------------------------------
popd && pushd /data/gpdata/stage2
print_header "RESTORE_DIRECTORY (PARALLEL=5) with ${SCALE_FACTOR} GB of data"
# Copy data from S3 to local using restore_directory_parallel
print_time_exec "\${GPHOME}/bin/gpbackup_s3_plugin restore_directory_parallel \
    ~/s3_config.yaml benchmark/tpch/lineitem/${SCALE_FACTOR}/lineitem_data"
echo
mkdir -p tmp/\$timestamp && mv benchmark tmp/\$timestamp
# Copy data from local to S3 using backup_directory_parallel
print_header "BACKUP_DIRECTORY (PARALLEL=5) with ${SCALE_FACTOR} GB of data"
print_time_exec "\${GPHOME}/bin/gpbackup_s3_plugin backup_directory_parallel \
    ~/s3_config.yaml tmp/\$timestamp/benchmark/tpch/lineitem"
echo
rm -rf ~/tpch_data/tmp/\$timestamp
popd

SCRIPT

chmod +x /tmp/run_perf.bash
scp /tmp/run_perf.bash cdw:
ssh -t cdw "/home/gpadmin/run_perf.bash"
