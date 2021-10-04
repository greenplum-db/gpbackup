#!/bin/bash

set -ex

# setup cluster and install gpbackup tools using gppkg
ccp_src/scripts/setup_ssh_to_cluster.sh
out=$(ssh -t mdw 'source env.sh && psql postgres -c "select version();"')
GPDB_VERSION=$(echo ${out} | sed -n 's/.*Greenplum Database \([0-9]\).*/\1/p')
mkdir -p /tmp/untarred
tar -xzf gppkgs/gpbackup-gppkgs.tar.gz -C /tmp/untarred
scp /tmp/untarred/gpbackup_tools*gp${GPDB_VERSION}*${OS}*.gppkg mdw:/home/gpadmin

cat <<SCRIPT > /tmp/run_tests.bash
#!/bin/bash

source env.sh

# Double the vmem protect limit default on the master segment to
# prevent query cancels on large table creations (e.g. scale_db1.sql)
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
set -e

### Data scale tests ###
log_file=/tmp/gpbackup.log
echo "## Populating database for data scale test ##"
createdb datascaledb
for j in {1..5000}
do
  psql -d datascaledb -q -c "CREATE TABLE tbl_1k_\$j(i int) DISTRIBUTED BY (i);"
  psql -d datascaledb -q -c "INSERT INTO tbl_1k_\$j SELECT generate_series(1,1000)"
done
for j in {1..100}
do
  psql -d datascaledb -q -c "CREATE TABLE tbl_1M_\$j(i int) DISTRIBUTED BY(i);"
  psql -d datascaledb -q -c "INSERT INTO tbl_1M_\$j SELECT generate_series(1,1000000)"
done
psql -d datascaledb -q -c "CREATE TABLE tbl_1B(i int) DISTRIBUTED BY(i);"
for j in {1..1000}
do
  psql -d datascaledb -q -c "INSERT INTO tbl_1B SELECT generate_series(1,1000000)"
done

echo "## Performing backup for data scale test ##"
### Multiple data file test ###
time gpbackup --dbname datascaledb --backup-dir /data/gpdata/ | tee "\$log_file"
timestamp=\$(head -10 "\$log_file" | grep "Backup Timestamp " | grep -Eo "[[:digit:]]{14}")
dropdb datascaledb
echo "## Performing restore for data scale test ##"
time gprestore --timestamp "\$timestamp" --backup-dir /data/gpdata/ --create-db --jobs=4 --quiet
rm "\$log_file"

echo "## Performing backup for data scale test with zstd ##"
### Multiple data file test with zstd ###
time gpbackup --dbname datascaledb --backup-dir /data/gpdata/ --compression-type zstd | tee "\$log_file"
timestamp=\$(head -10 "\$log_file" | grep "Backup Timestamp " | grep -Eo "[[:digit:]]{14}")
dropdb datascaledb
echo "## Performing restore for data scale test with zstd ##"
time gprestore --timestamp "\$timestamp" --backup-dir /data/gpdata/ --create-db --jobs=4 --quiet
rm "\$log_file"

echo "## Performing single-data-file backup for data scale test ##"
### Single data file test ###
time gpbackup --dbname datascaledb --backup-dir /data/gpdata/ --single-data-file | tee "\$log_file"
timestamp=\$(head -10 "\$log_file" | grep "Backup Timestamp " | grep -Eo "[[:digit:]]{14}")
dropdb datascaledb
echo "## Performing single-data-file restore for data scale test ##"
time gprestore --timestamp "\$timestamp" --backup-dir /data/gpdata/  --create-db --quiet
rm "\$log_file"

echo "## Performing single-data-file backup for data scale test with zstd ##"
### Single data file test with zstd ###
time gpbackup --dbname datascaledb --backup-dir /data/gpdata/ --single-data-file --compression-type zstd | tee "\$log_file"
timestamp=\$(head -10 "\$log_file" | grep "Backup Timestamp " | grep -Eo "[[:digit:]]{14}")
dropdb datascaledb
echo "## Performing single-data-file restore for data scale test with zstd ##"
time gprestore --timestamp "\$timestamp" --backup-dir /data/gpdata/  --create-db --quiet
dropdb datascaledb
rm "\$log_file"

### Metadata scale test ###
echo "## Populating database for metadata scale test ##"
tar -xvf scale_db1.tgz
createdb metadatascaledb -T template0

psql -f scale_db1.sql -d metadatascaledb -v client_min_messages=error -q

echo "## Performing pg_dump with metadata-only ##"
time pg_dump -s metadatascaledb > /data/gpdata/pg_dump.sql
echo "## Performing gpbackup with metadata-only ##"
time gpbackup --dbname metadatascaledb --backup-dir /data/gpdata/ --metadata-only --verbose | tee "\$log_file"

timestamp=\$(head -10 "\$log_file" | grep "Backup Timestamp " | grep -Eo "[[:digit:]]{14}")
echo "## Performing gprestore with metadata-only ##"
time gprestore --timestamp "\$timestamp" --backup-dir /data/gpdata/ --redirect-db=metadatascaledb_res --jobs=4 --create-db

SCRIPT

chmod +x /tmp/run_tests.bash
scp /tmp/run_tests.bash mdw:/home/gpadmin/run_tests.bash
scp -r scale_schema/scale_db1.tgz mdw:/home/gpadmin/
ssh -t mdw "/home/gpadmin/run_tests.bash"
