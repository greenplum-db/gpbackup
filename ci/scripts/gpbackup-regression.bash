#!/bin/bash

set -ex

# assume greenplum is fresh and has only system databases
ccp_src/scripts/setup_ssh_to_cluster.sh
ssh -t mdw 'mkdir -p /home/gpadmin/sqldump'
scp sqldump/* mdw:/home/gpadmin/sqldump/
ssh -t mdw 'xz -d /home/gpadmin/sqldump/dump.sql.xz'

# load data from sql, backup, and export the backup artifact
GENERATE_SCRIPT=gpbackup/ci/scripts/generate_backup_artifact.bash
scp ${GENERATE_SCRIPT} mdw:/home/gpadmin/generate_backup_artifact.bash
ssh -t mdw "bash  /home/gpadmin/generate_backup_artifact.bash"

scp mdw:/tmp/regression_dump.sql.xz  artifacts/

# combine gpbackup's separate tarballs for master and segments
ssh -t sdw1 "pushd /tmp ; tar czvf backup_artifact.tar.gz backup_artifact ; popd"
mkdir /tmp/gpbackup_allsegments
scp mdw:/tmp/backup_artifact.tar.gz  /tmp/gpbackup_allsegments/gpbackup_mdw.tar.gz
scp sdw1:/tmp/backup_artifact.tar.gz /tmp/gpbackup_allsegments/gpbackup_sdw1.tar.gz

tar czvf artifacts/gpbackup_all.tar.gz -C /tmp/ gpbackup_allsegments