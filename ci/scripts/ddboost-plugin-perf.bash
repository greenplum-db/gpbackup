#!/bin/bash

set -ex

. gpbackup/ci/scripts/setup-perf.bash

cat <<SCRIPT > /tmp/run_perf.bash
#!/bin/bash

set -e
source env.sh

# important: whitespace of yaml below is critical, do not change it
cat << CONFIG > \${HOME}/ddboost_config.yaml
executablepath: \${GPHOME}/bin/gpbackup_ddboost_plugin
options:
  hostname: ${DD_SOURCE_HOST}
  username: ${DD_USER}
  storage_unit: GPDB
  directory: gpbackup_tests${GPDB_VERSION}
  replication: "off"
  replication_streams: 10
  pgport: 5432
  password: ${DD_PW}
  remote_hostname: ${DD_DEST_HOST}
  remote_username: ${DD_USER}
  remote_storage_unit: GPDB
  remote_directory: gpbackup_tests${GPDB_VERSION}
  remote_password: ${DD_ENCRYPTED_PW}
  remote_password_encryption: "on"
  gpbackup_ddboost_plugin: 66706c6c6e677a6965796f68343365303133336f6c73366b316868326764
CONFIG

# important: whitespace of yaml below is critical, do not change it
cat << CONFIG > \${HOME}/ddboost_config_replication.yaml
executablepath: \${GPHOME}/bin/gpbackup_ddboost_plugin
options:
  hostname: ${DD_SOURCE_HOST}
  username: ${DD_USER}
  storage_unit: GPDB
  directory: gpbackup_tests${GPDB_VERSION}
  replication: "on"
  pgport: 5432
  password: ${DD_PW}
  remote_hostname: ${DD_DEST_HOST}
  remote_username: ${DD_USER}
  remote_storage_unit: GPDB
  remote_directory: gpbackup_tests${GPDB_VERSION}
  remote_password: ${DD_ENCRYPTED_PW}
  remote_password_encryption: "on"
  gpbackup_ddboost_plugin: 66706c6c6e677a6965796f68343365303133336f6c73366b316868326764
CONFIG

pushd \${GOPATH}/src/github.com/greenplum-db/gpbackup/plugins
./plugin_test_scale.sh \${GPHOME}/bin/gpbackup_ddboost_plugin \${HOME}/ddboost_config.yaml
./plugin_test_scale.sh \${GPHOME}/bin/gpbackup_ddboost_plugin \${HOME}/ddboost_config_replication.yaml
popd

SCRIPT

chmod +x /tmp/run_perf.bash
scp /tmp/run_perf.bash cdw:
ssh -t cdw "/home/gpadmin/run_perf.bash"
ssh -t cdw "source env.sh && dropdb tpchdb"
