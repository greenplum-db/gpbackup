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

if [[ -d gp-pkg ]] ; then
  mkdir /tmp/gppkgv2
  tar -xzf gp-pkg/gppkg* -C /tmp/gppkgv2

  # install gppkgv2 onto all segments
  while read -r host; do
    ssh -n "$host" "mkdir -p /home/gpadmin/.local/bin"
    scp /tmp/gppkgv2/gppkg "$host":/home/gpadmin/.local/bin
  done <cluster_env_files/hostfile_all
fi

cat <<SCRIPT > /tmp/run_tests.bash
#!/bin/bash

source env.sh

# Double the vmem protect limit default on the master segment to
# prevent query cancels on large table creations
gpconfig -c gp_vmem_protect_limit -v 16384 --masteronly
gpstop -air

# only install if not installed already
if [[ -f /home/gpadmin/.local/bin/gppkg ]] ; then
  # gppkg v2 is installed here
  source env.sh; gppkg query greenplum_backup_restore
  if [ \$? -ne 0 ] ; then
    gppkg install -a gpbackup*gp*.gppkg
  fi
else
  is_installed_output=\$(source env.sh; gppkg -q gpbackup*gp*.gppkg)
  echo \$is_installed_output | grep 'is installed'
  if [ \$? -ne 0 ] ; then
    gppkg -i gpbackup*gp*.gppkg
  fi
fi

# run dump into database
echo "## Loading dumpfile ##"
unxz < /home/gpadmin/dump.sql.xz | PGOPTIONS='--client-min-messages=warning' psql -q -f - postgres


# gpbackup bug. there is a ticket open to resolve
psql -d regression -c "DROP TABLE IF EXISTS public.equal_operator_not_in_search_path_table CASCADE;"
psql -d regression -c "DROP TABLE IF EXISTS public.equal_operator_not_in_search_path_table_multi_key CASCADE;"


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
