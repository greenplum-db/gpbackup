#!/bin/bash
set -e
set -o pipefail

plugin=$1
plugin_config=$2
secondary_plugin_config=$3
SUPPORTED_API_VERSION="0.3.0"

# ----------------------------------------------
# Test suite setup
# This will put small amounts of data in the
# plugin destination location
# ----------------------------------------------
if [ $# -lt 2 ] || [ $# -gt 3 ]
  then
    echo "Usage: plugin_test_bench.sh [path_to_executable] [plugin_config] [optional_config_for_secondary_destination]"
    exit 1
fi

time_second=$(date +"%s")
testfile="/tmp/testseg/backups/2018010101/20180101010101/testfile_$time_second.txt"
testdata="/tmp/testseg/backups/2018010101/20180101010101/testdata_$time_second.txt"
test_no_data="/tmp/testseg/backups/2018010101/20180101010101/test_no_data_$time_second.txt"
testdir=`dirname $testfile`

text="this is some text"
data=`LC_ALL=C tr -dc 'A-Za-z0-9' </dev/urandom | head -c 1000 ; echo`
mkdir -p $testdir
echo $text > $testfile

echo "# ----------------------------------------------"
echo "# Starting gpbackup plugin tests"
echo "# ----------------------------------------------"

# ----------------------------------------------
# Check API version
# ----------------------------------------------

echo "[RUNNING] plugin_api_version"
output=`$plugin plugin_api_version`
if [ "$output" != "$SUPPORTED_API_VERSION" ]; then
  echo "Plugin API version does not match supported version $SUPPORTED_API_VERSION"
  exit 1
fi
echo "[PASSED] plugin_api_version"

# ----------------------------------------------
# Setup and Backup/Restore file functions
# ----------------------------------------------

echo "[RUNNING] setup_plugin_for_backup on master"
$plugin setup_plugin_for_backup $plugin_config $testdir master \"-1\"
echo "[RUNNING] setup_plugin_for_backup on segment_host"
$plugin setup_plugin_for_backup $plugin_config $testdir segment_host
echo "[RUNNING] setup_plugin_for_backup on segment 0"
$plugin setup_plugin_for_backup $plugin_config $testdir segment \"0\"

echo "[RUNNING] backup_file"
$plugin backup_file $plugin_config $testfile
# plugins should leave copies of the files locally when they run backup_file
test -f $testfile

echo "[RUNNING] setup_plugin_for_restore on master"
$plugin setup_plugin_for_restore $plugin_config $testdir master \"-1\"
echo "[RUNNING] setup_plugin_for_restore on segment_host"
$plugin setup_plugin_for_restore $plugin_config $testdir segment_host
echo "[RUNNING] setup_plugin_for_restore on segment 0"
$plugin setup_plugin_for_restore $plugin_config $testdir segment \"0\"

echo "[RUNNING] restore_file"
rm $testfile
$plugin restore_file $plugin_config $testfile
output=`cat $testfile`
if [ "$output" != "$text" ]; then
  echo "Failed to backup and restore file using plugin"
  exit 1
fi
if [ -n "$secondary_plugin_config" ]; then
  rm $testfile
  echo "[RUNNING] restore_file (from secondary destination)"
  $plugin restore_file $secondary_plugin_config $testfile
  output=`cat $testfile`
  if [ "$output" != "$text" ]; then
    echo "Failed to backup and restore file using plugin from secondary destination"
    exit 1
  fi
fi
echo "[PASSED] setup_plugin_for_backup"
echo "[PASSED] backup_file"
echo "[PASSED] setup_plugin_for_restore"
echo "[PASSED] restore_file"

# ----------------------------------------------
# Backup/Restore data functions
# ----------------------------------------------

echo "[RUNNING] backup_data"
echo $data | $plugin backup_data $plugin_config $testdata
echo "[RUNNING] restore_data"
output=`$plugin restore_data $plugin_config $testdata`

if [ "$output" != "$data" ]; then
  echo "Failed to backup and restore data using plugin"
  exit 1
fi

if [ -n "$secondary_plugin_config" ]; then
  echo "[RUNNING] restore_data (from secondary destination)"
  output=`$plugin restore_data $secondary_plugin_config $testdata`

  if [ "$output" != "$data" ]; then
    echo "Failed to backup and restore data using plugin"
    exit 1
  fi
fi
echo "[PASSED] backup_data"
echo "[PASSED] restore_data"


echo "[RUNNING] backup_data with no data"
echo -n "" | $plugin backup_data $plugin_config $test_no_data
echo "[RUNNING] restore_data with no data"
output=`$plugin restore_data $plugin_config $test_no_data`

if [ "$output" != "" ]; then
  echo "Failed to backup and restore data using plugin"
  exit 1
fi

if [ -n "$secondary_plugin_config" ]; then
  echo "[RUNNING] restore_data with no data (from secondary destination)"
  output=`$plugin restore_data $secondary_plugin_config $test_no_data`

  if [ "$output" != "" ]; then
    echo "Failed to backup and restore data using plugin"
    exit 1
  fi
fi
echo "[PASSED] backup_data with no data"
echo "[PASSED] restore_data with no data"

# ----------------------------------------------
# Delete backup directory function
# ----------------------------------------------

echo "[RUNNING] delete_dir"
echo $data | $plugin backup_data $plugin_config $testdata

# ensures the dir exists prior to deletion
$plugin restore_data $plugin_config $testdata > /dev/null

$plugin delete_dir $plugin_config 20180101010101

set +e
output=$($plugin restore_data $plugin_config $testdata 2>/dev/null)
retval=$(echo $?)
if [ "${output}" = "${data}"  ] || [ "$retval" = "0" ] ; then
  echo "Failed to delete backup using plugin"
  exit 1
fi
set -e
echo "[PASSED] delete_dir"

# ----------------------------------------------
# Cleanup functions
# ----------------------------------------------

echo "[RUNNING] cleanup_plugin_for_backup on master"
$plugin cleanup_plugin_for_backup $plugin_config $testdir master \"-1\"
echo "[RUNNING] cleanup_plugin_for_backup on segment_host"
$plugin cleanup_plugin_for_backup $plugin_config $testdir segment_host
echo "[RUNNING] cleanup_plugin_for_backup on segment 0"
$plugin cleanup_plugin_for_backup $plugin_config $testdir segment \"0\"
echo "[PASSED] cleanup_plugin_for_backup"

echo "[RUNNING] cleanup_plugin_for_restore on master"
$plugin cleanup_plugin_for_restore $plugin_config $testdir master \"-1\"
echo "[RUNNING] cleanup_plugin_for_restore on segment_host"
$plugin cleanup_plugin_for_restore $plugin_config $testdir segment_host
echo "[RUNNING] cleanup_plugin_for_restore on segment 0"
$plugin cleanup_plugin_for_restore $plugin_config $testdir segment \"0\"
echo "[PASSED] cleanup_plugin_for_restore"


# ----------------------------------------------
# Run test gpbackup and gprestore with plugin
# ----------------------------------------------

#gpbackup --dbname $test_db --plugin-config $plugin_config $further_options > $log_file


test_backup_and_restore_with_plugin() {
    flags=$1
    test_db=plugin_test_db
    log_file=/tmp/plugin_test_log_file

    psql -d postgres -qc "DROP DATABASE IF EXISTS $test_db" 2>/dev/null
    createdb $test_db
    psql -d $test_db -qc "CREATE TABLE test_table(i int) DISTRIBUTED RANDOMLY; INSERT INTO test_table select generate_series(1,50000)"

    echo "[RUNNING] gpbackup with test database (using ${flags})"
    gpbackup --dbname $test_db --plugin-config $plugin_config $flags > $log_file
    if [ ! $? -eq 0 ]; then
        echo "gpbackup failed. Check gpbackup log file in ~/gpAdminLogs for details."
        exit 1
    fi
    timestamp=`head -4 $log_file | grep "Backup Timestamp " | grep -Eo "[[:digit:]]{14}"`
    dropdb $test_db

    echo "[RUNNING] gprestore with test database"
    gprestore --timestamp $timestamp --plugin-config $plugin_config --create-db --quiet
    if [ ! $? -eq 0 ]; then
        echo "gprestore failed. Check gprestore log file in ~/gpAdminLogs for details."
        exit 1
    fi
    num_rows=`psql -d $test_db -tc "SELECT count(*) FROM test_table" | xargs`
    if [ "$num_rows" != "50000" ]; then
        echo "Expected to restore 50000 rows, got $num_rows"
        exit 1
    fi

    if [ -n "$secondary_plugin_config" ]; then
        dropdb $test_db
        echo "[RUNNING] gprestore with test database from secondary destination"
        gprestore --timestamp $timestamp --plugin-config $secondary_plugin_config --create-db --quiet
        if [ ! $? -eq 0 ]; then
            echo "gprestore from secondary destination failed. Check gprestore log file in ~/gpAdminLogs for details."
            exit 1
        fi
        num_rows=`psql -d $test_db -tc "SELECT count(*) FROM test_table" | xargs`
        if [ "$num_rows" != "50000" ]; then
          echo "Expected to restore 50000 rows, got $num_rows"
          exit 1
        fi
    fi
    echo "[PASSED] gpbackup and gprestore (using ${flags})"
}

test_backup_and_restore_with_plugin "--no-compression --single-data-file"
test_backup_and_restore_with_plugin "--no-compression"

# ----------------------------------------------
# Cleanup test artifacts
# ----------------------------------------------
dropdb $test_db
rm $log_file
rm -r /tmp/testseg
rm -f $testfile

echo "# ----------------------------------------------"
echo "# Finished gpbackup plugin tests"
echo "# ----------------------------------------------"
