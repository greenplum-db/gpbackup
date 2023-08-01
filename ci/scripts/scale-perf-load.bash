#!/bin/bash

set -ex

# setup cluster and load necessary tools and data to it
ccp_src/scripts/setup_ssh_to_cluster.sh
out=$(ssh -t cdw 'source env.sh && psql postgres -c "select version();"')
GPDB_VERSION=$(echo ${out} | sed -n 's/.*Greenplum Database \([0-9]\).*/\1/p')
mkdir -p /tmp/untarred
tar -xzf gppkgs/gpbackup-gppkgs.tar.gz -C /tmp/untarred
scp /tmp/untarred/gpbackup_tools*gp${GPDB_VERSION}*${OS}*.gppkg cdw:/home/gpadmin
scp gpbackup/ci/scripts/analyze_run.py cdw:/home/gpadmin/analyze_run.py
scp gpbackup/ci/scale/sql/scaletestdb_bigschema_ddl.sql cdw:/home/gpadmin/scaletestdb_bigschema_ddl.sql
scp gpbackup/ci/scale/sql/scaletestdb_wideschema_ddl.sql cdw:/home/gpadmin/scaletestdb_wideschema_ddl.sql
scp gpbackup/ci/scale/sql/etl_job.sql cdw:/home/gpadmin/etl_job.sql
scp gpbackup/ci/scale/sql/pull_rowcount.sql cdw:/home/gpadmin/pull_rowcount.sql
scp gpbackup/ci/scale/sql/valid_metadata.sql cdw:/home/gpadmin/valid_metadata.sql
scp -r gpbackup/ci/scale/gpload_yaml cdw:/home/gpadmin/gpload_yaml

set +x
printf "%s" "${GOOGLE_CREDENTIALS}" > "/tmp/keyfile.json"
set -x

scp /tmp/keyfile.json cdw:/home/gpadmin/keyfile.json && rm -f /tmp/keyfile.json

cat <<SCRIPT > /tmp/load_data.bash
#!/bin/bash
source env.sh

# set GCS credentials and mount gcs bucket with gcsfuse
export GOOGLE_APPLICATION_CREDENTIALS=/home/gpadmin/keyfile.json
gcloud auth activate-service-account --key-file=/home/gpadmin/keyfile.json
rm -rf /home/gpadmin/bucket && mkdir /home/gpadmin/bucket
gcsfuse --implicit-dirs dp-gpbackup-scale-test-data /home/gpadmin/bucket

# Double the vmem protect limit default on the coordinator segment to
# prevent query cancels on large table creations (e.g. scale_db1.sql)
gpconfig -c gp_vmem_protect_limit -v 16384 --masteronly
gpconfig -c client_min_messages -v error
gpstop -air

# only install if not installed already
is_installed_output=\$(source env.sh; gppkg -q gpbackup*gp*.gppkg)
set +e
echo \$is_installed_output | grep 'is installed'
if [ \$? -ne 0 ] ; then
  set -e
  gppkg -i gpbackup*gp*.gppkg
fi
set -e

### Data scale tests ###
echo "## Loading data into database for scale tests ##"
createdb scaletestdb
psql -d scaletestdb -q -f scaletestdb_bigschema_ddl.sql
gpload -f /home/gpadmin/gpload_yaml/customer.yml
gpload -f /home/gpadmin/gpload_yaml/lineitem.yml
gpload -f /home/gpadmin/gpload_yaml/orders.yml
gpload -f /home/gpadmin/gpload_yaml/orders_2.yml
gpload -f /home/gpadmin/gpload_yaml/orders_3.yml
gpload -f /home/gpadmin/gpload_yaml/nation.yml
gpload -f /home/gpadmin/gpload_yaml/part.yml
gpload -f /home/gpadmin/gpload_yaml/partsupp.yml
gpload -f /home/gpadmin/gpload_yaml/region.yml
gpload -f /home/gpadmin/gpload_yaml/supplier.yml

# clean out credentials after data is loaded
rm -f /home/gpadmin/keyfile.json

# unmount bucket before exiting
fusermount -u /home/gpadmin/bucket

SCRIPT

# tar up files for this cluster so that later jobs can connect to it
tar -czf cluster-metadata/cluster-metadata.tar.gz cluster_env_files/

chmod +x /tmp/load_data.bash
scp /tmp/load_data.bash cdw:/home/gpadmin/load_data.bash
ssh -t cdw "/home/gpadmin/load_data.bash"

