#!/usr/bin/env bash

set -ex
source env.sh

mkdir /tmp/backup_artifact # parent dir for all backups

echo "##### Loading sqldump into DB #####"
psql -d postgres -f /home/gpadmin/sqldump/dump.sql >/dev/null

echo "##### Running pg_dump on regression DB #####"
pg_dump regression -f /tmp/regression_dump.sql --schema-only
pushd /tmp
    xz -z regression_dump.sql
popd

# Although the current restore/diff testing only uses the regression
# database, we may in the future choose to diff other databases created
# in the "src/test/regress" directory of GPDB.
#
# Because the test suite of src/test/regress includes a database
# with a special char ("funny copy""db'with\\quotes"),
# iterating through the databases is the easiest way to reference
# a list of DBs, using an index rather than db name.
# For the regression database, that number is 17, as of March 2019,
# and that's the index number expected by the sibling "gprestore" script

REGRESSION_DB_INDEX=17

# iterate through all the databases to issue a warning the index changes
psql -t postgres -c "SELECT datname FROM pg_database WHERE datistemplate = false;" > /tmp/db_names

while read -r dbname ; do
    # skip all but regression
    [[ "regression" != "${dbname}" ]] && continue

    db_index=${REGRESSION_DB_INDEX}
    dir="/tmp/backup_artifact/${db_index}"
    mkdir "${dir}"

    echo "##### Backing up database: ${dbname} #####"
    gpbackup --dbname "${dbname}" --backup-dir "${dir}" --metadata-only
done < /tmp/db_names

# create tarball of all backups by backing up parent dir
pushd /tmp
    tar -czvf backup_artifact.tar.gz backup_artifact
popd
