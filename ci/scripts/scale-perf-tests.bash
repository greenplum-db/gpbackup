#!/bin/bash

set -ex

# retrieve cluster set up by previous job, and set up SSH to it
tar -xvf "cluster-metadata/cluster-metadata.tar.gz"
ccp_src/scripts/setup_ssh_to_cluster.sh

cat <<SCRIPT > /tmp/run_tests.bash
#!/bin/bash

source env.sh
# set format for logging
export TIMEFORMAT="TEST RUNTIME: %E"
export RESULTS_LOG_FILE=${RESULTS_LOG_FILE}

# set parameters for reference time DB
export RESULTS_DATABASE_HOST=${RESULTS_DATABASE_HOST}
export RESULTS_DATABASE_USER=${RESULTS_DATABASE_USER}
export RESULTS_DATABASE_NAME=${RESULTS_DATABASE_NAME}
export RESULTS_DATABASE_PASSWORD=${RESULTS_DATABASE_PASSWORD}

# capture installed versions for later storage in run stats
gpstart --version > /home/gpadmin/gpversion.txt
gpbackup --version > /home/gpadmin/gpbversion.txt
export GPDB_VERSION=\$(cat /home/gpadmin/gpversion.txt)
export GPB_VERSION=\$(cat /home/gpadmin/gpbversion.txt)

echo "## Capturing row counts for comparison ##"
psql -d scaletestdb -f /home/gpadmin/pull_rowcount.sql -o /home/gpadmin/rowcounts_orig.txt

#####################################################################
##################################################################### 
echo "## Performing single-data-file, --no-compression, --copy-queue-size 8 backup test ##"
# BACKUP
rm -f $RESULTS_LOG_FILE
(time gpbackup --dbname scaletestdb --include-schema big --backup-dir /data/gpdata/ --single-data-file --no-compression --copy-queue-size 8) > $RESULTS_LOG_FILE 2>&1
timestamp=\$(head -10 "\$RESULTS_LOG_FILE" | grep "Backup Timestamp " | grep -Eo "[[:digit:]]{14}")
echo "gpb_single_data_file_copy_q8 timestamp backed up: \$timestamp"

# conduct runtime analysis
python /home/gpadmin/analyze_run.py gpb_single_data_file_copy_q8
#####################################################################

#####################################################################
echo "## Performing single-data-file, --no-compression, --copy-queue-size 8 restore test ##"
# RESTORE
rm -f $RESULTS_LOG_FILE
(time gprestore --timestamp "\$timestamp" --include-schema big --backup-dir /data/gpdata/ --create-db --redirect-db copyqueuerestore8 --copy-queue-size 8) > $RESULTS_LOG_FILE 2>&1
echo "gpr_single_data_file_copy_q8 timestamp restored: \$timestamp"

# compare round-trip row counts
psql -d copyqueuerestore8 -f /home/gpadmin/pull_rowcount.sql -o /home/gpadmin/rowcounts_gpr_single_data_file_copy_q8.txt
ROWCOUNTS_DIFF=\$(diff -w /home/gpadmin/rowcounts_orig.txt /home/gpadmin/rowcounts_gpr_single_data_file_copy_q8.txt)
if [ "\$ROWCOUNTS_DIFF" != "" ] 
then
  echo "Failed result from gpr_single_data_file_copy_q8 -- mismatched row counts.  Exiting early with failure code."
  exit 1
fi

# conduct runtime analysis
python /home/gpadmin/analyze_run.py gpr_single_data_file_copy_q8

# clean out redirected database before proceeding further
yes y | gpbackup_manager delete-backup "\$timestamp"
dropdb copyqueuerestore8
#####################################################################
#####################################################################

#####################################################################
##################################################################### 
echo "## Performing backup for data scale test ##"
# BACKUP
rm -f $RESULTS_LOG_FILE
(time gpbackup --dbname scaletestdb --include-schema big --backup-dir /data/gpdata/) > $RESULTS_LOG_FILE 2>&1
timestamp=\$(head -10 "\$RESULTS_LOG_FILE" | grep "Backup Timestamp " | grep -Eo "[[:digit:]]{14}")
echo "gpb_scale_multi_data_file timestamp backed up: \$timestamp"

# conduct runtime analysis
python /home/gpadmin/analyze_run.py gpb_scale_multi_data_file
#####################################################################

#####################################################################
echo "## Performing restore for data scale test ##"
# RESTORE
rm -f $RESULTS_LOG_FILE
(time gprestore --timestamp "\$timestamp" --include-schema big --backup-dir /data/gpdata/ --create-db --redirect-db scalemultifile --jobs=4) > $RESULTS_LOG_FILE 2>&1
echo "gpr_scale_multi_data_file timestamp restored: \$timestamp"

# compare round-trip row counts
psql -d scalemultifile -f /home/gpadmin/pull_rowcount.sql -o /home/gpadmin/rowcounts_gpr_scale_multi_data_file.txt
ROWCOUNTS_DIFF=\$(diff -w /home/gpadmin/rowcounts_orig.txt /home/gpadmin/rowcounts_gpr_scale_multi_data_file.txt)
if [ "\$ROWCOUNTS_DIFF" != "" ] 
then
  echo "Failed result from gpr_scale_multi_data_file -- mismatched row counts.  Exiting early with failure code."
  exit 1
fi

# conduct runtime analysis
python /home/gpadmin/analyze_run.py gpr_scale_multi_data_file

# clean out redirected database before proceeding further
yes y | gpbackup_manager delete-backup "\$timestamp"
dropdb scalemultifile
#####################################################################
#####################################################################

#####################################################################
##################################################################### 
echo "## Performing backup for data scale test with zstd ##"
# BACKUP
rm -f $RESULTS_LOG_FILE
(time gpbackup --dbname scaletestdb --include-schema big --backup-dir /data/gpdata/ --compression-type zstd) > $RESULTS_LOG_FILE 2>&1
timestamp=\$(head -10 "\$RESULTS_LOG_FILE" | grep "Backup Timestamp " | grep -Eo "[[:digit:]]{14}")
echo "gpb_scale_multi_data_file_zstd timestamp backed up: \$timestamp"

# conduct runtime analysis
python /home/gpadmin/analyze_run.py gpb_scale_multi_data_file_zstd
#####################################################################

#####################################################################
echo "## Performing restore for data scale test with zstd ##"
# RESTORE
rm -f $RESULTS_LOG_FILE
(time gprestore --timestamp "\$timestamp" --include-schema big --backup-dir /data/gpdata/ --create-db --redirect-db scalemultifilezstd --jobs=4) > $RESULTS_LOG_FILE 2>&1
echo "gpr_scale_multi_data_file_zstd timestamp restored: \$timestamp"

# compare round-trip row counts
psql -d scalemultifilezstd -f /home/gpadmin/pull_rowcount.sql -o /home/gpadmin/rowcounts_gpr_scale_multi_data_file_zstd.txt
ROWCOUNTS_DIFF=\$(diff -w /home/gpadmin/rowcounts_orig.txt /home/gpadmin/rowcounts_gpr_scale_multi_data_file_zstd.txt)
if [ "\$ROWCOUNTS_DIFF" != "" ] 
then
  echo "Failed result from gpr_scale_multi_data_file_zstd -- mismatched row counts.  Exiting early with failure code."
  exit 1
fi

# conduct runtime analysis
python /home/gpadmin/analyze_run.py gpr_scale_multi_data_file_zstd

# clean out redirected database before proceeding further
yes y | gpbackup_manager delete-backup "\$timestamp"
dropdb scalemultifilezstd
#####################################################################
#####################################################################

#####################################################################
##################################################################### 
echo "## Performing single-data-file backup for data scale test ##"
# BACKUP
rm -f $RESULTS_LOG_FILE
(time gpbackup --dbname scaletestdb --include-schema big --backup-dir /data/gpdata/ --single-data-file) > $RESULTS_LOG_FILE 2>&1
timestamp=\$(head -10 "\$RESULTS_LOG_FILE" | grep "Backup Timestamp " | grep -Eo "[[:digit:]]{14}")
echo "gpb_scale_single_data_file timestamp backed up: \$timestamp"

# conduct runtime analysis
python /home/gpadmin/analyze_run.py gpb_scale_single_data_file
#####################################################################

#####################################################################
echo "## Performing single-data-file restore for data scale test ##"
# RESTORE
rm -f $RESULTS_LOG_FILE
(time gprestore --timestamp "\$timestamp" --include-schema big --backup-dir /data/gpdata/ --create-db --redirect-db scalesinglefile) > $RESULTS_LOG_FILE 2>&1
echo "gpr_scale_single_data_file timestamp restored: \$timestamp"

# compare round-trip row counts
psql -d scalesinglefile -f /home/gpadmin/pull_rowcount.sql -o /home/gpadmin/rowcounts_gpr_scale_single_data_file.txt
ROWCOUNTS_DIFF=\$(diff -w /home/gpadmin/rowcounts_orig.txt /home/gpadmin/rowcounts_gpr_scale_single_data_file.txt)
if [ "\$ROWCOUNTS_DIFF" != "" ] 
then
  echo "Failed result from gpr_scale_single_data_file -- mismatched row counts.  Exiting early with failure code."
  exit 1
fi

# conduct runtime analysis
python /home/gpadmin/analyze_run.py gpr_scale_single_data_file

# clean out redirected database before proceeding further
yes y | gpbackup_manager delete-backup "\$timestamp"
dropdb scalesinglefile
#####################################################################
#####################################################################

#####################################################################
##################################################################### 
echo "## Performing single-data-file backup for data scale test with zstd ##"
# BACKUP
rm -f $RESULTS_LOG_FILE
(time gpbackup --dbname scaletestdb --include-schema big --backup-dir /data/gpdata/ --single-data-file --compression-type zstd) > $RESULTS_LOG_FILE 2>&1
timestamp=\$(head -10 "\$RESULTS_LOG_FILE" | grep "Backup Timestamp " | grep -Eo "[[:digit:]]{14}")
echo "gpb_scale_single_data_file_zstd timestamp backed up: \$timestamp"

# conduct runtime analysis
python /home/gpadmin/analyze_run.py gpb_scale_single_data_file_zstd
#####################################################################

#####################################################################
echo "## Performing single-data-file restore for data scale test with zstd ##"
# RESTORE
rm -f $RESULTS_LOG_FILE
(time gprestore --timestamp "\$timestamp" --include-schema big --backup-dir /data/gpdata/ --create-db --redirect-db scalesinglefilezstd) > $RESULTS_LOG_FILE 2>&1
echo "gpr_scale_single_data_file_zstd timestamp restored: \$timestamp"

# compare round-trip row counts
psql -d scalesinglefilezstd -f /home/gpadmin/pull_rowcount.sql -o /home/gpadmin/rowcounts_gpr_scale_single_data_file_zstd.txt
ROWCOUNTS_DIFF=\$(diff -w /home/gpadmin/rowcounts_orig.txt /home/gpadmin/rowcounts_gpr_scale_single_data_file_zstd.txt)
if [ "\$ROWCOUNTS_DIFF" != "" ] 
then
  echo "Failed result from gpr_scale_single_data_file_zstd -- mismatched row counts.  Exiting early with failure code."
  exit 1
fi

# conduct runtime analysis
python /home/gpadmin/analyze_run.py gpr_scale_single_data_file_zstd

# clean out redirected database before proceeding further
yes y | gpbackup_manager delete-backup "\$timestamp"
dropdb scalesinglefilezstd
#####################################################################
#####################################################################

#####################################################################
##################################################################### 
# TEST GPBACKUP UNDER VARIOUS PRESSURES
#####################################################################
##################################################################### 

#####################################################################
#####################################################################
echo "## Performing backup with moderate number of jobs while database is being edited ##"
# BACKUP
rm -f $RESULTS_LOG_FILE
echo "RESULTS_LOG_FILE: \$RESULTS_LOG_FILE"
(time gpbackup --dbname scaletestdb --include-schema big --backup-dir /data/gpdata --jobs=16 ) > \$RESULTS_LOG_FILE 2>&1 &
echo "Backup initiated in the background."
# check log for lock acquisition before proceeding
set +e # turn off exit on error so grep doesn't halt the whole script
TIMEOUT_COUNTER=0
while true
do
    sleep 1
    LOCKSGREP=\$(grep "Locks acquired: .* 100\.00\%" \$RESULTS_LOG_FILE)
    if [ "\$LOCKSGREP" != "" ]; then
        echo "All locks acquired.  Proceeding with ETL job."
        break
    fi

    if ((\$TIMEOUT_COUNTER > 100)); then
        echo "Test timed out waiting for lock acquisition"
        exit 1
    fi
    echo "\$TIMEOUT_COUNTER"
    ((TIMEOUT_COUNTER=\$TIMEOUT_COUNTER+1))
done

# begin ETL job
psql -d scaletestdb -f /home/gpadmin/etl_job.sql > /dev/null

# check log for backup completion before proceeding
TIMEOUT_COUNTER=0
while true
do
    sleep 1
    COMPGREP=\$(grep "Backup completed successfully" \$RESULTS_LOG_FILE)
    if [ "\$COMPGREP" != "" ]; then
        break
    fi

    if ((\$TIMEOUT_COUNTER > 10000)); then
        echo "Test timed out waiting for backup completion"
        exit 1
    fi
    ((TIMEOUT_COUNTER=\$TIMEOUT_COUNTER+1))
done
set -e

timestamp=\$(head -10 "\$RESULTS_LOG_FILE" | grep "Backup Timestamp " | grep -Eo "[[:digit:]]{14}")
echo "gpb_distr_snap_edit_data timestamp backed up: \$timestamp"

# conduct runtime analysis
python /home/gpadmin/analyze_run.py gpb_distr_snap_edit_data
#####################################################################

#####################################################################
echo "## Performing restore with moderate number of jobs on backup done while database is edited ##"
# RESTORE
rm -f $RESULTS_LOG_FILE
dropdb scaletestdb
(time gprestore --timestamp "\$timestamp" --include-schema big --backup-dir /data/gpdata --create-db --redirect-db newscaletestdb --jobs=16) > \$RESULTS_LOG_FILE 2>&1
echo "gpr_distr_snap_edit_data timestamp restored: \$timestamp"

# compare round-trip row counts
psql -d newscaletestdb -f /home/gpadmin/pull_rowcount.sql -o /home/gpadmin/rowcounts_gpr_distr_snap_edit_data.txt
ROWCOUNTS_DIFF=\$(diff -w /home/gpadmin/rowcounts_orig.txt /home/gpadmin/rowcounts_gpr_distr_snap_edit_data.txt)
if [ "\$ROWCOUNTS_DIFF" != "" ] 
then
  echo "Failed result from gpr_distr_snap_edit_data -- mismatched row counts.  Exiting early with failure code."
  exit 1
fi

# conduct runtime analysis
python /home/gpadmin/analyze_run.py gpr_distr_snap_edit_data

# clean out redirected database before proceeding further
yes y | gpbackup_manager delete-backup "\$timestamp"
#####################################################################
#####################################################################

#####################################################################
##################################################################### 
echo "## Performing backup with high number of jobs on cluster with high-concurrency load ##"
# BACKUP
rm -f $RESULTS_LOG_FILE
(time gpbackup --dbname newscaletestdb --include-schema big --backup-dir /data/gpdata --jobs=32 ) > \$RESULTS_LOG_FILE 2>&1 &
# check log for lock acquisition before proceeding
set +e set +e # turn off exit on error so grep doesn't halt the whole script
TIMEOUT_COUNTER=0
while true
do
    sleep 1
    LOCKSGREP=\$(grep "Locks acquired: .* 100\.00\%" \$RESULTS_LOG_FILE)
    if [ "\$LOCKSGREP" != "" ]; then
        echo "All locks acquired.  Proceeding with data load"
        break
    fi

    if ((\$TIMEOUT_COUNTER > 100)); then
        echo "Test timed out waiting for lock acquisition"
        exit 1
    fi
    ((TIMEOUT_COUNTER=\$TIMEOUT_COUNTER+1))
done

# load data into a separate database to apply high concurrent load to cluster
createdb scaletestdb
psql -d scaletestdb -q -f scaletestdb_bigschema_ddl.sql
gpload -f /home/gpadmin/gpload_yaml/lineitem.yml
gpload -f /home/gpadmin/gpload_yaml/orders_3.yml

# check log for backup completion before proceeding
TIMEOUT_COUNTER=0
while true
do
    sleep 1
    COMPGREP=\$(grep "Backup completed successfully" \$RESULTS_LOG_FILE)
    if [ "\$COMPGREP" != "" ]; then
        break
    fi

    if ((\$TIMEOUT_COUNTER > 10000)); then
        echo "Test timed out waiting for backup completion"
        exit 1
    fi
    ((TIMEOUT_COUNTER=\$TIMEOUT_COUNTER+1))
done
set -e

timestamp=\$(head -10 "\$RESULTS_LOG_FILE" | grep "Backup Timestamp " | grep -Eo "[[:digit:]]{14}")
echo "gpb_distr_snap_high_conc timestamp backed up: \$timestamp"

# conduct runtime analysis
python /home/gpadmin/analyze_run.py gpb_distr_snap_high_conc
#####################################################################

#####################################################################
echo "## Performing restore with high number of jobs on backup done while cluster had high-concurrency load ##"
# RESTORE
rm -f $RESULTS_LOG_FILE
dropdb scaletestdb
(time gprestore --timestamp "\$timestamp" --include-schema big --backup-dir /data/gpdata --create-db --redirect-db scaletestdb --jobs=32) > \$RESULTS_LOG_FILE 2>&1
echo "gpr_distr_snap_high_conc timestamp restored: \$timestamp"

# compare round-trip row counts
psql -d scaletestdb -f /home/gpadmin/pull_rowcount.sql -o /home/gpadmin/rowcounts_gpr_distr_snap_high_conc.txt
ROWCOUNTS_DIFF=\$(diff -w /home/gpadmin/rowcounts_orig.txt /home/gpadmin/rowcounts_gpr_distr_snap_high_conc.txt)
if [ "\$ROWCOUNTS_DIFF" != "" ] 
then
  echo "Failed result from gpb_distr_snap_high_conc -- mismatched row counts.  Exiting early with failure code."
  exit 1
fi

# conduct runtime analysis
python /home/gpadmin/analyze_run.py gpr_distr_snap_high_conc

# clean out redirected database before proceeding further
yes y | gpbackup_manager delete-backup "\$timestamp"
dropdb newscaletestdb
#####################################################################
#####################################################################

#####################################################################
##################################################################### 
# METADATA-ONLY FROM HERE ON
echo "## Loading wide schema for metadata tests"
psql -d scaletestdb -q -f scaletestdb_wideschema_ddl.sql
#####################################################################
##################################################################### 

#####################################################################
##################################################################### 
echo "## Performing first backup with metadata-only ##"
# BACKUP
rm -f $RESULTS_LOG_FILE
(time gpbackup --dbname scaletestdb --include-schema wide --backup-dir /data/gpdata/ --metadata-only --verbose) > $RESULTS_LOG_FILE 2>&1
timestamp=\$(head -10 "\$RESULTS_LOG_FILE" | grep "Backup Timestamp " | grep -Eo "[[:digit:]]{14}")
echo "gpb_scale_metadata timestamp backed up: \$timestamp"
test_metadata=\$(find /data/gpdata/ -name *\$timestamp*_metadata.sql)

METADATA_DIFF=\$(diff -w /home/gpadmin/valid_metadata.sql \$test_metadata)
echo "got past metadata diff"
if [ "\$METADATA_DIFF" != "" ] 
then
  echo "Failed result from gpb_scale_metadata -- mismatched metadata output.  Exiting early with failure code."
  exit 1
fi

# conduct runtime analysis
python /home/gpadmin/analyze_run.py gpb_scale_metadata
#####################################################################

#####################################################################
echo "## Performing restore on metadata-only ##"
# RESTORE
rm -f $RESULTS_LOG_FILE
dropdb scaletestdb
(time gprestore --timestamp "\$timestamp" --include-schema wide --backup-dir /data/gpdata/ --create-db --redirect-db scaletestdb) > $RESULTS_LOG_FILE 2>&1
echo "gpr_scale_metadata timestamp restored: \$timestamp"

echo "## Performing second backup with metadata-only ##"
rm -f $RESULTS_LOG_FILE
(time gpbackup --dbname scaletestdb --include-schema wide --backup-dir /data/gpdata/ --metadata-only --verbose) > $RESULTS_LOG_FILE 2>&1
timestamp=\$(head -10 "\$RESULTS_LOG_FILE" | grep "Backup Timestamp " | grep -Eo "[[:digit:]]{14}")
test_metadata=\$(find /data/gpdata/ -name *\$timestamp*_metadata.sql)

METADATA_DIFF=\$(diff -w /home/gpadmin/valid_metadata.sql \$test_metadata)
if [ "\$METADATA_DIFF" != "" ] 
then
  echo "Failed result from gpr_scale_metadata -- mismatched metadata output.  Exiting early with failure code."
  exit 1
fi

# conduct runtime analysis
python /home/gpadmin/analyze_run.py gpr_scale_metadata

# clean out redirected database before proceeding further
yes y | gpbackup_manager delete-backup "\$timestamp"
#####################################################################
#####################################################################

SCRIPT

chmod +x /tmp/run_tests.bash
scp /tmp/run_tests.bash cdw:/home/gpadmin/run_tests.bash
ssh -t cdw "/home/gpadmin/run_tests.bash"
