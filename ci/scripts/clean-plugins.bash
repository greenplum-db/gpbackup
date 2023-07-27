#!/bin/bash

# We deliberately do not set -e here, because these commands error if the 
# directory happens to have already been deleted, which we do not want.
set -x

# Add locale for locale tests
localedef -c -i de_DE -f UTF-8 de_DE
echo LANG=\"de_DE\" >> /etc/locale.conf
source /etc/locale.conf

## old versions of ld have a bug that our CGO libs exercise. update binutils to avoid it
OLDLDVERSION=$(ld --version | grep "2.25");
if [[ ${OS} == "RHEL6" ]]; then
    ## have to use vault to update because centos6 is EOL
    sudo yum install -y centos-release-scl
    echo "https://vault.centos.org/6.10/os/x86_64/" > /var/cache/yum/x86_64/6/C6.10-base/mirrorlist.txt
    echo "http://vault.centos.org/6.10/extras/x86_64/" > /var/cache/yum/x86_64/6/C6.10-extras/mirrorlist.txt
    echo "http://vault.centos.org/6.10/updates/x86_64/" > /var/cache/yum/x86_64/6/C6.10-updates/mirrorlist.txt
    mkdir /var/cache/yum/x86_64/6/centos-sclo-rh/
    echo "http://vault.centos.org/6.10/sclo/x86_64/rh" > /var/cache/yum/x86_64/6/centos-sclo-rh/mirrorlist.txt
    mkdir /var/cache/yum/x86_64/6/centos-sclo-sclo/
    echo "http://vault.centos.org/6.10/sclo/x86_64/sclo" > /var/cache/yum/x86_64/6/centos-sclo-sclo/mirrorlist.txt
    sudo yum install -y devtoolset-7-binutils*
    \cp /opt/rh/devtoolset-7/root/usr/bin/ld /usr/bin/ld
elif [[ $OLDLDVERSION != "" ]]; then
    yum install -y binutils
fi

mkdir /tmp/untarred
tar -xzf gppkgs/gpbackup-gppkgs.tar.gz -C /tmp/untarred

if [[ ! -f bin_gpdb/bin_gpdb.tar.gz ]] ; then
  mv bin_gpdb/*.tar.gz bin_gpdb/bin_gpdb.tar.gz
fi
source gpdb_src/concourse/scripts/common.bash
time install_gpdb
time ./gpdb_src/concourse/scripts/setup_gpadmin_user.bash
time NUM_PRIMARY_MIRROR_PAIRS=${LOCAL_CLUSTER_SIZE} make_cluster

# generate configs for the CI storage used in our tests
cat << CONFIG > /tmp/ddboost_config_local.yaml
executablepath: \${GPHOME}/bin/gpbackup_ddboost_plugin
options:
  hostname: ${DD_SOURCE_HOST}
  username: ${DD_USER}
  storage_unit: GPDB
  directory: gpbackup_tests6
  pgport: 6000
  password: ${DD_ENCRYPTED_PW}
  password_encryption: "on"
  gpbackup_ddboost_plugin: 66706c6c6e677a6965796f68343365303133336f6c73366b316868326764
CONFIG

cat << CONFIG > /tmp/ddboost_config_remote.yaml
executablepath: \${GPHOME}/bin/gpbackup_ddboost_plugin
options:
  hostname: ${DD_DEST_HOST}
  username: ${DD_USER}
  storage_unit: GPDB
  directory: gpbackup_tests6
  pgport: 6000
  password: ${DD_ENCRYPTED_PW}
  password_encryption: "on"
  gpbackup_ddboost_plugin: 66706c6c6e677a6965796f68343365303133336f6c73366b316868326764
CONFIG

  cat << CONFIG > /tmp/s3_config.yaml
executablepath: \${GPHOME}/bin/gpbackup_s3_plugin
options:
  region: ${REGION}
  aws_access_key_id: ${AWS_ACCESS_KEY_ID}
  aws_secret_access_key: ${AWS_SECRET_ACCESS_KEY}
  bucket: ${BUCKET}
  folder: test/backup
  backup_multipart_chunksize: 100MB
  restore_multipart_chunksize: 100MB
CONFIG

cat <<SCRIPT > /tmp/run_clean.bash
#!/bin/bash

set -x

# use "temp build dir" of parent shell
export GOPATH=\${HOME}/go
export PATH=/usr/local/go/bin:\$PATH:\${GOPATH}/bin:/opt/rh/devtoolset-7/root/usr/bin/
if [[ -f /opt/gcc_env.sh ]]; then
    source /opt/gcc_env.sh
fi
mkdir -p \${GOPATH}/bin \${GOPATH}/src/github.com/greenplum-db
cp -R $(pwd)/gpbackup \${GOPATH}/src/github.com/greenplum-db/

source /usr/local/greenplum-db-devel/greenplum_path.sh
source $(pwd)/gpdb_src/gpAux/gpdemo/gpdemo-env.sh

mkdir -p "\${GPHOME}/postgresql"

# Install gpbackup_tools
out=\$(psql postgres -c "select version();")
GPDB_VERSION=\$(echo \$out | sed -n 's/.*Greenplum Database \([0-9]\).*/\1/p')
gppkg -i /tmp/untarred/gpbackup_tools*gp\${GPDB_VERSION}*${OS}*.gppkg

# Install pgcrypto so ddboost plugin will work
psql -d postgres -c "CREATE EXTENSION IF NOT EXISTS pgcrypto"

# Invoke delete directory on parent directories we use for our tests
\${GPHOME}/bin/gpbackup_ddboost_plugin delete_directory /tmp/ddboost_config_local.yaml gpbackup_tests5
\${GPHOME}/bin/gpbackup_ddboost_plugin delete_directory /tmp/ddboost_config_remote.yaml gpbackup_tests5
\${GPHOME}/bin/gpbackup_ddboost_plugin delete_directory /tmp/ddboost_config_local.yaml gpbackup_tests6
\${GPHOME}/bin/gpbackup_ddboost_plugin delete_directory /tmp/ddboost_config_remote.yaml gpbackup_tests6
# TODO -- add deletes for gpdb7 when we add plugin tests for that
\${GPHOME}/bin/gpbackup_s3_plugin delete_directory /tmp/s3_config.yaml test/backup

echo "Contents of plugin storage directories deleted"
SCRIPT

chmod +x /tmp/run_clean.bash
su - gpadmin "/tmp/run_clean.bash"

