#!/bin/bash

set -ex

mkdir /tmp/untarred
tar -xzf gppkgs/gpbackup-gppkgs.tar.gz -C /tmp/untarred
if [[ -d gp-pkg ]] ; then
    mkdir /tmp/gppkgv2
    tar -xzf gp-pkg/gppkg* -C /tmp/gppkgv2
fi

# this will dump a folder /tmp/icw-migr-backup with our saved gpbackup results
tar -xzf migration-backup/migration-backup.tar.gz -C /tmp

if [[ ! -f bin_gpdb/bin_gpdb.tar.gz ]] ; then
  mv bin_gpdb/*.tar.gz bin_gpdb/bin_gpdb.tar.gz
fi
source gpdb_src/concourse/scripts/common.bash
time install_gpdb
time ./gpdb_src/concourse/scripts/setup_gpadmin_user.bash
time make_cluster

cat <<SCRIPT > /tmp/icw_restore.bash
#!/bin/bash

set -ex

# use "temp build dir" of parent shell
export GOPATH=\${HOME}/go
export PATH=/usr/local/go/bin:\$PATH:\${GOPATH}/bin:/opt/rh/devtoolset-7/root/usr/bin/
mkdir -p \${GOPATH}/bin \${GOPATH}/src/github.com/greenplum-db
cp -R $(pwd)/gpbackup \${GOPATH}/src/github.com/greenplum-db/

# Install dependencies before sourcing greenplum path. Using the GPDB curl is causing issues.
pushd \${GOPATH}/src/github.com/greenplum-db/gpbackup
  make depend
popd

source /usr/local/greenplum-db-devel/greenplum_path.sh
source $(pwd)/gpdb_src/gpAux/gpdemo/gpdemo-env.sh

mkdir -p "\${GPHOME}/postgresql"

# Install gpbackup gppkg
out=\$(psql postgres -c "select version();")
GPDB_VERSION=\$(echo \$out | sed -n 's/.*Greenplum Database \([0-9]\).*/\1/p')

if [[ -f /tmp/gppkgv2/gppkg ]] ; then
    /tmp/gppkgv2/gppkg install -a /tmp/untarred/gpbackup*gp\${GPDB_VERSION}*${OS}*.gppkg
else
    gppkg -i /tmp/untarred/gpbackup*gp\${GPDB_VERSION}*${OS}*.gppkg
fi

# extract timestamp from saved folder, use it to run a restore
TS=\$(ls /tmp/icw-migr-backup/backups/*)
gprestore --timestamp=\$TS --backup-dir=/tmp/icw-migr-backup --create-db --with-globals --on-error-continue | tee /tmp/gpbackup_test.log

# We expect some errors, so we have to use on-error-continue, but we want to parse for unexpected
# errors so that we will still fail this test when appropriate.  We chain greps here to allow us to
# easily add or remove expected errors in the future.

# TODO: see if we can extract this to a standalone script, so this can be kept general for migration testing
# Expected Errors:
##  Resource group option names were changed in GPDB7.  We expect this to throw an error.

cat /home/gpadmin/gpAdminLogs/gprestore_* | grep -E "ERROR" | grep -Ev "Encountered [0-9]" \
    | grep -Ev "ALTER RESOURCE GROUP" \
    | grep -Ev "LOG ERRORS" \
    | grep -Ev "RAISE EXCEPTION" \
    | tee /tmp/error_list.log

if [[ -s /tmp/error_list.log ]] ; then
    echo "Unexpected errors found in gprestore output"
    exit 1
fi

SCRIPT

chmod +x /tmp/icw_restore.bash
su - gpadmin "/tmp/icw_restore.bash"

