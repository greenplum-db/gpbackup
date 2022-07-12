# Scale Testing
The structure of the scale tests uses TPC-Hd ata that was manually generated and stored into a gcs bucket.  These data are loaded onto a concourse VM, then backed up and restored using various configurations of `gbackup` and `gprestore` with correctness and runtime tests after each operation.

## Data Generation
The data used were generated locally using [TPC-H](https://github.com/edespino/TPC-H), configured with `GEN_DATA_SCALE="100"`.

One **important note** is that to generate valid flat files for use in loading the below code block must be added to `TPC-H/00_compile_tpch/dbgen/config.h` and re-compilation must be included in the configuration with `RUN_COMPILE_TPCH="true"`.
```c
#ifndef EOL_HANDLING
#define EOL_HANDLING
#endif
```

## Data Loading
This data is loaded onto the testing cluster using `gpload`, in the format indicated in `scaletestdb_bigschema_ddl.sql`.  To help keep storage on gcs buckets down 

# Tests
* The tests below are currently run as part of the pipeline.  These are treated as backup/restore pairs.  The backup from each gpb_* test is restored using its paired gpr_* test.  
* The row counts of each restore test are compared against the expected row counts for the loaded data.  Any mismatch in any table fails the whole Concourse job.
    * For metadata-only tests, row-counts cannot be compared.  Instead a manually-created and -validated metadata file is included in the repo.  The backed-up metadata files are compared against this to ensure the round-trip is made correctly.  To include `gprestore` in this loop `gpbackup` is run twice, once on the originally loaded schema and again on the restored schema, and the outputs of both backups are checked.
* For each test, the `time` builtin is used to capture the runtime of the operation. The runtime of each test is checked against a rolling average (stats for each test, both individual and summary, are kept in the Reference Database described below).  If the runtime is past a given threshold, a Slack notification is sent to the Data Protection team to investigate the cause of the performance regression.

## Tests Currently Included
* gpb_single_data_file_copy_q8
    * `gpbackup --dbname scaletestdb --include-schema big --backup-dir /data/gpdata/ --single-data-file --no-compression --copy-queue-size 8`
* gpr_single_data_file_copy_q8
    * `gprestore --timestamp "\$timestamp" --include-schema big --backup-dir /data/gpdata/ --create-db --redirect-db copyqueuerestore8 --copy-queue-size 8`
* gpb_scale_multi_data_file
    * `gpbackup --dbname scaletestdb --include-schema big --backup-dir /data/gpdata/`
* gpr_scale_multi_data_file
    * `gprestore --timestamp "\$timestamp" --include-schema big --backup-dir /data/gpdata/ --create-db --redirect-db scalemultifile --jobs=4`
* gpb_scale_multi_data_file_zstd
    * `gpbackup --dbname scaletestdb --include-schema big --backup-dir /data/gpdata/ --compression-type zstd`
* gpr_scale_multi_data_file_zstd
    * `gprestore --timestamp "\$timestamp" --include-schema big --backup-dir /data/gpdata/ --create-db --redirect-db scalemultifilezstd --jobs=4`
* gpb_scale_single_data_file
    * `gpbackup --dbname scaletestdb --include-schema big --backup-dir /data/gpdata/ --single-data-file`
* gpr_scale_single_data_file
    * `gprestore --timestamp "\$timestamp" --include-schema big --backup-dir /data/gpdata/ --create-db --redirect-db scalesinglefile`
* gpb_scale_single_data_file_zstd
    * `gpbackup --dbname scaletestdb --include-schema big --backup-dir /data/gpdata/ --single-data-file --compression-type zstd`
* gpr_scale_single_data_file_zstd
    * `gprestore --timestamp "\$timestamp" --include-schema big --backup-dir /data/gpdata/ --create-db --redirect-db scalesinglefilezstd`
* gpb_scale_metadata
    * `gpbackup --dbname scaletestdb --include-schema wide --backup-dir /data/gpdata/ --metadata-only --verbose`
* gpr_scale_metadata
    * `gprestore --timestamp "\$timestamp" --include-schema wide --backup-dir /data/gpdata/ --create-db --redirect-db scaletestdb`

# Creating Reference Database
The Reference Database for all test run information is kept in a [Google Cloud SQL](https://console.cloud.google.com/sql/) instance.  To configure this instance, the following steps are necessary.
1. Create an instance
2. Choose PostgreSQL
3. Choose the following configuration options
    * Version: 9.6 (but newer versions should work)
    * Production Configuration
    * Zonal Availability: Multiple Zones
    * Machine Type: Lightweight -- 1 vCPU, 3.75 GB 
    * Storage: HDD -- 100GB
    * Connections:
        * Public IP: Enabled
            * For development and debugging, individual workstation IPs must be added to the allowlist after starting up the database.
        * Private IP: Enabled
            * To make this database reachable by our Concourse instances, it was attached to `bosh-network`
    * Automatic Backups: Enabled
