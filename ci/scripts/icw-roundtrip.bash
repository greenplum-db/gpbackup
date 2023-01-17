#!/bin/bash

set -ex

# setup cluster and install gpbackup tools using gppkg
ccp_src/scripts/setup_ssh_to_cluster.sh
out=$(ssh -t cdw 'source env.sh && psql postgres -c "select version();"')
GPDB_VERSION=$(echo ${out} | sed -n 's/.*Greenplum Database \([0-9]\).*/\1/p')
mkdir -p /tmp/untarred
tar -xzf gppkgs/gpbackup-gppkgs.tar.gz -C /tmp/untarred
scp /tmp/untarred/gpbackup_tools*gp${GPDB_VERSION}*${OS}*.gppkg cdw:/home/gpadmin
scp ./icw_dump/dump.sql.xz cdw:/home/gpadmin

pushd ./diffdb_src
    go build
    scp ./diffdb cdw:/home/gpadmin/
popd

cat <<SCRIPT > /tmp/run_tests.bash
#!/bin/bash

source env.sh

# Double the vmem protect limit default on the master segment to
# prevent query cancels on large table creations
gpconfig -c gp_vmem_protect_limit -v 16384 --masteronly
gpstop -air

# only install if not installed already
is_installed_output=\$(source env.sh; gppkg -q gpbackup*gp*.gppkg)
set +e
echo \$is_installed_output | grep 'is installed'
if [ \$? -ne 0 ] ; then
  set -e
  gppkg -i gpbackup*gp*.gppkg
fi

# run dump into database
echo "## Loading dumpfile ##"
unxz < /home/gpadmin/dump.sql.xz | PGOPTIONS='--client-min-messages=warning' psql -q -f - postgres

# server bug. can't safely use enums as distribution key
# https://github.com/greenplum-db/gpdb/issues/14198
psql -d regression -c "DROP TYPE IF EXISTS gpdist_legacy_opclasses.colors CASCADE;"
psql -d regression -c "DROP TABLE IF EXISTS gpdist_legacy_opclasses.legacy_enum CASCADE;"

echo "## Performing backup of regression database ## "
gpbackup --dbname regression --backup-dir /home/gpadmin/data | tee /tmp/gpbackup_test.log
timestamp=\$(head -10 /tmp/gpbackup_test.log | grep "Backup Timestamp " | grep -Eo "[[:digit:]]{14}")
gpbackup_manager display-report \$timestamp

# restore database
echo "## Performing restore of regression database ## "
time gprestore --timestamp "\$timestamp" --backup-dir /home/gpadmin/data --create-db --redirect-db restoreregression

./diffdb --basedb=regression --testdb=restoreregression &> ./dbdiff.log
grep "matches database" ./dbdiff.log
if [ \$? -ne 0 ] ; then
    echo "ERROR: ICW round-trip restore did not match"
    cat ./dbdiff.log
    exit 1
fi
echo "ICW round-trip restore was successful"

SCRIPT

chmod +x /tmp/run_tests.bash
scp /tmp/run_tests.bash cdw:/home/gpadmin/run_tests.bash
ssh -t cdw "/home/gpadmin/run_tests.bash"
