#!/bin/bash -l

set -eox pipefail

source gpdb_src/concourse/scripts/common.bash
install_and_configure_gpdb
make -C gpdb_src/src/test/regress/
make -C gpdb_src/contrib/dummy_seclabel/ install
gpdb_src/concourse/scripts/setup_gpadmin_user.bash
make_cluster

su - gpadmin -c "
source /usr/local/greenplum-db-devel/greenplum_path.sh;
source ~/gpdb_src/gpAux/gpdemo/gpdemo-env.sh;
gpconfig -c shared_preload_libraries -v dummy_seclabel;
gpstop -ar;
wget https://golang.org/dl/go1.14.15.linux-amd64.tar.gz;
tar -C ~/ -xzf go1.14.15.linux-amd64.tar.gz;
PATH=$PATH:~/go/bin GOPATH=~/go make depend build install test end_to_end -C go/src/github.com/greenplum-db/gpbackup/"