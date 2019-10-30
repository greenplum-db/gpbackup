#!/usr/bin/env bash

set -ex
source env.sh

# gprestore regression database and use pg_dump to then dump the result
pushd /tmp
    db_index=17 # 17 is the regression database
    find ./backup_artifact/${db_index} -name "*_metadata.sql" | sed -nr 's#.*gpseg-[0-9]/backups/.*/([0-9]{14})/.*#\1#p' > /tmp/${db_index}_timestamp
    read timestamp < /tmp/${db_index}_timestamp
    echo "${db_index} timestamp: ${timestamp}"

    echo "##### Restore backup artifact for ${db_index} DB #####"
    set +e
      gprestore --create-db --timestamp ${timestamp} --backup-dir /tmp/backup_artifact/${db_index} --with-globals --on-error-continue
    set -e

    echo "##### pg_dump #####"
    set +e
      pg_dump regression -f /tmp/post_regression_dump.sql --schema-only
    set -e
    xz -z /tmp/post_regression_dump.sql
popd
