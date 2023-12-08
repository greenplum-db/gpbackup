#!/bin/bash
set -ex

mkdir /tmp/untarred
tar -xzf gppkgs/gpbackup-gppkgs.tar.gz -C /tmp/untarred
if [[ -d gp-pkg ]] ; then
    mkdir /tmp/gppkgv2
    tar -xzf gp-pkg/gppkg* -C /tmp/gppkgv2
fi

if [[ ! -f bin_gpdb/bin_gpdb.tar.gz ]] ; then
  mv bin_gpdb/*.tar.gz bin_gpdb/bin_gpdb.tar.gz
fi
source gpdb_src/concourse/scripts/common.bash
time install_gpdb
time ./gpdb_src/concourse/scripts/setup_gpadmin_user.bash
time make_cluster

# unxz the dump to a findable location
xz -dc icw_dump/dump.sql.xz > /tmp/dump.sql

cat <<SCRIPT > /tmp/backup_icw.bash
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

# run the ICW dump into the cluster
createdb regression
PGOPTIONS='--client-min-messages=warning' psql -d regression -q -f /tmp/dump.sql

# run cleanups needed for migration backup, and tar cleaned backup into a tempdir
# TODO: move these into a standalone script to abstract version-specific cleanups so we can re-use
# this for migration testing other to/from GPDB versions
psql -d regression -c "DROP TABLE IF EXISTS gpdist_legacy_opclasses.legacy_enum CASCADE;"
psql -d regression -c "DROP EXTENSION IF EXISTS plpythonu CASCADE;"
psql -d regression -c "DROP TABLE IF EXISTS bfv_partition.t26002_t1 CASCADE;"
psql -d regression -c "DROP TABLE IF EXISTS dpe_malp.malp CASCADE;"
psql -d regression -c "DROP TABLE IF EXISTS partition_pruning.pt_complex CASCADE;"
psql -d regression -c "DROP TABLE IF EXISTS public.mpp18162a CASCADE;"
psql -d regression -c "DROP TABLE IF EXISTS public.mpp18162b CASCADE;"
psql -d regression -c "DROP TABLE IF EXISTS public.mpp18162c CASCADE;"
psql -d regression -c "DROP TABLE IF EXISTS public.mpp18162d CASCADE;"
psql -d regression -c "DROP TABLE IF EXISTS public.mpp18162e CASCADE;"
psql -d regression -c "DROP TABLE IF EXISTS public.mpp18162f CASCADE;"
psql -d regression -c "DROP TABLE IF EXISTS public.mpp18179 CASCADE;"
psql -d regression -c "DROP TABLE IF EXISTS public.mpp5878 CASCADE;"
psql -d regression -c "DROP TABLE IF EXISTS public.mpp5878a CASCADE;"
psql -d regression -c "DROP TABLE IF EXISTS public.equal_operator_not_in_search_path_table_multi_key CASCADE;"
psql -d regression -c "ALTER TABLE gpdist_legacy_opclasses.all_legacy_types drop column abstime_col;"
psql -d regression -c "ALTER TABLE gpdist_legacy_opclasses.all_legacy_types drop column tinterval_col;"
psql -d regression -c "DROP TABLE IF EXISTS public.aocs_unknown CASCADE;"
psql -d regression -c "DROP TABLE IF EXISTS public.test_issue_12936 CASCADE;"
psql -d regression -c "DROP MATERIALIZED VIEW IF EXISTS public.mv_unspecified_types;"
psql -d regression -c "DROP TABLE IF EXISTS public.mpp5992 CASCADE;"
psql -d regression -c "DROP TABLE IF EXISTS public.pt_ao_tab_rng CASCADE;"
psql -d regression -c "DROP TABLE IF EXISTS public.pt_co_tab_rng CASCADE;"
psql -d regression -c "DROP OPERATOR IF EXISTS public.=> (bigint, NONE) CASCADE;"
psql -d regression -c "DROP VIEW IF EXISTS mpp7164.partagain CASCADE;"
psql -d regression -c "DROP VIEW IF EXISTS mpp7164.partlist CASCADE;"
psql -d regression -c "DROP VIEW IF EXISTS mpp7164.partrank CASCADE;"
psql -d regression -c "DROP VIEW IF EXISTS public.redundantly_named_part;"
psql -d regression -c "DROP FUNCTION IF EXISTS index_constraint_naming_partition.partition_tables() CASCADE;"
psql -d regression -c "DROP TRIGGER IF EXISTS after_ins_stmt_trig on public.main_table;"
psql -d regression -c "DROP TRIGGER IF EXISTS after_upd_b_stmt_trig on public.main_table;"
psql -d regression -c "DROP TRIGGER IF EXISTS after_upd_stmt_trig on public.main_table;"
psql -d regression -c "DROP TRIGGER IF EXISTS before_ins_stmt_trig on public.main_table;"
psql -d regression -c "DROP TRIGGER IF EXISTS before_upd_a_stmt_trig on public.main_table;"
psql -d regression -c "DROP TRIGGER IF EXISTS foo_as_trigger on test_expand_table.table_with_update_trigger;"
psql -d regression -c "DROP TRIGGER IF EXISTS foo_bs_trigger on test_expand_table.table_with_update_trigger;"

mkdir /tmp/icw-migr-backup
gpbackup --dbname regression --backup-dir /tmp/icw-migr-backup --single-backup-dir
echo "Backup for migration testing completed"

SCRIPT

chmod +x /tmp/backup_icw.bash
su - gpadmin "/tmp/backup_icw.bash"

# move artifacts for Concourse put
tar -czf migration-backup.tar.gz -C /tmp icw-migr-backup
mv migration-backup.tar.gz migration-artifacts

