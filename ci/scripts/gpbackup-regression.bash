#!/bin/bash

set -ex

# assume greenplum is fresh and has only system databases
ccp_src/scripts/setup_ssh_to_cluster.sh
ssh -t cdw 'mkdir -p /home/gpadmin/sqldump'
scp sqldump/* cdw:/home/gpadmin/sqldump/
ssh -t cdw 'xz -d /home/gpadmin/sqldump/dump.sql.xz'

# load data from sql, backup, and export the backup artifact
GENERATE_SCRIPT=gpbackup/ci/scripts/generate_backup_artifact.bash
scp ${GENERATE_SCRIPT} cdw:/home/gpadmin/generate_backup_artifact.bash
ssh -t cdw "bash  /home/gpadmin/generate_backup_artifact.bash"

scp cdw:/tmp/regression_dump.sql.xz artifacts/

# combine gpbackup's separate tarballs for coordinator and segments
ssh -t sdw1 "pushd /tmp ; tar czvf backup_artifact.tar.gz backup_artifact ; popd"
mkdir /tmp/gpbackup_allsegments
scp cdw:/tmp/backup_artifact.tar.gz  /tmp/gpbackup_allsegments/gpbackup_cdw.tar.gz
scp sdw1:/tmp/backup_artifact.tar.gz /tmp/gpbackup_allsegments/gpbackup_sdw1.tar.gz

tar czvf artifacts/gpbackup_all.tar.gz -C /tmp/ gpbackup_allsegments
