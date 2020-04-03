#!/bin/bash

set -ex

ccp_src/scripts/setup_ssh_to_cluster.sh
ssh -t centos@mdw "sudo yum install -y s3fs-fuse"
ssh -t mdw "mkdir -p /tmp/s3 && \
    echo ${AWS_ACCESS_KEY_ID}:${AWS_SECRET_ACCESS_KEY} > \${HOME}/.passwd-s3fs && \
    chmod 600 \${HOME}/.passwd-s3fs && \
    s3fs gpbackup-s3-plugin-test /tmp/s3 -o passwd_file=\${HOME}/.passwd-s3fs && \
    ln -s /tmp/s3 ~/tpch_data && \
    ls -l ~/tpch_data/benchmark/tpch/lineitem/${SCALE_FACTOR}"

cat << EOF > lineitem.ddl
DROP TABLE IF EXISTS lineitem;
CREATE TABLE lineitem (
    l_orderkey       INTEGER NOT NULL,
    l_partkey        INTEGER NOT NULL,
    l_suppkey        INTEGER NOT NULL,
    l_linenumber     INTEGER NOT NULL,
    l_quantity       DECIMAL(15,2) NOT NULL,
    l_extendedprice  DECIMAL(15,2) NOT NULL,
    l_discount       DECIMAL(15,2) NOT NULL,
    l_tax            DECIMAL(15,2) NOT NULL,
    l_returnflag     CHAR(1) NOT NULL,
    l_linestatus     CHAR(1) NOT NULL,
    l_shipdate       DATE NOT NULL,
    l_commitdate     DATE NOT NULL,
    l_receiptdate    DATE NOT NULL,
    l_shipinstruct   CHAR(25) NOT NULL,
    l_shipmode       CHAR(10) NOT NULL,
    l_comment        VARCHAR(44) NOT NULL
)
DISTRIBUTED BY (l_orderkey);
EOF

cat << EOF > gpload.yml
---
VERSION: 1.0.0.1
DATABASE: tpchdb
USER: gpadmin
HOST: localhost
PORT: ${PG_PORT}
GPLOAD:
   INPUT:
    - SOURCE:
         FILE:
           - /home/gpadmin/tpch_data/benchmark/tpch/lineitem/${SCALE_FACTOR}/lineitem.tbl
    - FORMAT: text
    - DELIMITER: '|'
    - HEADER: false
   OUTPUT:
    - TABLE: lineitem
    - MODE: insert
    - UPDATE_CONDITION: 'boolean_condition'
   PRELOAD:
    - TRUNCATE: true
    - REUSE_TABLES: false
EOF

cat <<SCRIPT > /tmp/run_perf.bash
#!/bin/bash

set -ex
source env.sh

createdb tpchdb
createdb restoredb
psql -d tpchdb -a -f lineitem.ddl

# ----------------------------------------------------------------------
# Load lineitem data using gpload
# ----------------------------------------------------------------------
time gpload -f gpload.yml
time psql -d tpchdb -c "CREATE TABLE lineitem_1 AS SELECT * FROM lineitem DISTRIBUTED BY (l_orderkey)"
time psql -d tpchdb -c "CREATE TABLE lineitem_2 AS SELECT * FROM lineitem DISTRIBUTED BY (l_orderkey)"
time psql -d tpchdb -c "CREATE TABLE lineitem_3 AS SELECT * FROM lineitem DISTRIBUTED BY (l_orderkey)"
time psql -d tpchdb -c "CREATE TABLE lineitem_4 AS SELECT * FROM lineitem DISTRIBUTED BY (l_orderkey)"
time psql -d tpchdb -c "CREATE TABLE lineitem_5 AS SELECT * FROM lineitem DISTRIBUTED BY (l_orderkey)"

# ----------------------------------------------------------------------
# Run gpbackup followed by gprestore
# ----------------------------------------------------------------------
log_file=/tmp/gpbackup.log
time gpbackup --dbname tpchdb --plugin-config ~/s3_config.yaml | tee "\$log_file"
timestamp=\$(head -5 "\$log_file" | grep "Backup Timestamp " | grep -Eo "[[:digit:]]{14}")
time gprestore --redirect-db restoredb --timestamp "\$timestamp" --plugin-config ~/s3_config.yaml

# ----------------------------------------------------------------------
# Run restore_directory followed by backup_directory
# ----------------------------------------------------------------------
mkdir -p /data/gpdata/stage1 /data/gpdata/stage2
pushd /data/gpdata/stage1
# Copy data from S3 to local using restore_directory
time \${GPHOME}/bin/gpbackup_s3_plugin restore_directory \
    ~/s3_config.yaml benchmark/tpch/lineitem/${SCALE_FACTOR}/lineitem_data
ls -l benchmark/tpch/lineitem/${SCALE_FACTOR}/lineitem_data

mkdir tmp && mv benchmark tmp
# Copy data from local to S3 using backup_directory
time \${GPHOME}/bin/gpbackup_s3_plugin backup_directory \
    ~/s3_config.yaml tmp/benchmark/tpch/lineitem
ls -l ~/tpch_data/tmp/benchmark/tpch/lineitem/${SCALE_FACTOR}/lineitem_data
rm -rf ~/tpch_data/tmp

# ----------------------------------------------------------------------
# Run restore_directory_parallel followed by backup_directory_parallel
# ----------------------------------------------------------------------
popd && pushd /data/gpdata/stage2
# Copy data from S3 to local using restore_directory_parallel
time \${GPHOME}/bin/gpbackup_s3_plugin restore_directory_parallel \
    ~/s3_config.yaml benchmark/tpch/lineitem/${SCALE_FACTOR}/lineitem_data
ls -l benchmark/tpch/lineitem/${SCALE_FACTOR}/lineitem_data

mkdir tmp && mv benchmark tmp
# Copy data from local to S3 using backup_directory_parallel
time \${GPHOME}/bin/gpbackup_s3_plugin backup_directory_parallel \
    ~/s3_config.yaml tmp/benchmark/tpch/lineitem
ls -l ~/tpch_data/tmp/benchmark/tpch/lineitem/${SCALE_FACTOR}/lineitem_data
rm -rf ~/tpch_data/tmp
popd

SCRIPT

chmod +x /tmp/run_perf.bash
scp lineitem.ddl gpload.yml /tmp/run_perf.bash mdw:
ssh -t mdw "/home/gpadmin/run_perf.bash"
