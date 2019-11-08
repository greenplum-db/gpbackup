#!/bin/bash

set -ex

# Need to rename GPDB6 Release Candidate binary for common script usage
mv bin_gpdb/*.tar.gz bin_gpdb/bin_gpdb.tar.gz

# Set up container environment and install/start up Greenplum
source gpdb_src/concourse/scripts/common.bash
time install_gpdb
time ./gpdb_src/concourse/scripts/setup_gpadmin_user.bash
time make_cluster

# Unpack gpbackup/gprestore
pushd github_release_components
	tar xvzf gpbackup-*.tar.gz
	tar xvzf bin_gpbackup.tar.gz -C /usr/local/greenplum-db-devel
popd

# Unpack GPDB 4.3 backup to be consumed by gprestore
tar xzvf gpdb43_backup/gpdb43_regression.tar.gz -C /
chown -R gpadmin.gpadmin /tmp/gpdb43_regression/

# Generate gprestore wrapper script for gpadmin user to execute
cat > /home/gpadmin/run_gprestore.sh <<-EOF
	#!/bin/bash

	# Source Greenplum
	source /usr/local/greenplum-db-devel/greenplum_path.sh
	export PGPORT=15432

	## Run gprestore
	timestamp=$(ls /tmp/gpdb43_regression/demoDataDir-1/*/*)
	gprestore --timestamp \$timestamp --backup-dir /tmp/gpdb43_regression --create-db --with-globals --on-error-continue
EOF

# Run gprestore to GPDB 6 cluster
chmod +x /home/gpadmin/run_gprestore.sh
su - gpadmin /home/gpadmin/run_gprestore.sh

