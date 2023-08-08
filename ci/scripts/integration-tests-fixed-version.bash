#!/bin/bash

set -ex

if [[ ! -f bin_gpdb/bin_gpdb.tar.gz ]] ; then
  mv bin_gpdb/*.tar.gz bin_gpdb/bin_gpdb.tar.gz
fi
source gpdb_src/concourse/scripts/common.bash
time install_gpdb
time ./gpdb_src/concourse/scripts/setup_gpadmin_user.bash
time make_cluster

CWDIR=$(pwd)
GOPATH=/home/gpadmin/go

mkdir -p ${GOPATH}/bin ${GOPATH}/src/github.com/greenplum-db
cp -R $(pwd)/gpbackup ${GOPATH}/src/github.com/greenplum-db
tar -zxf gpbackup_1.12.1_dependencies/*.tar.gz -C ${GOPATH}/src/github.com/greenplum-db/gpbackup
chown -R gpadmin:gpadmin ${GOPATH}

cat<<SCRIPT > /home/gpadmin/.bashrc
export GOPATH=${GOPATH}
export PATH=/usr/local/go/bin:$PATH:${GOPATH}/bin
source /usr/local/greenplum-db-devel/greenplum_path.sh
source ${CWDIR}/gpdb_src/gpAux/gpdemo/gpdemo-env.sh
SCRIPT

cat <<SCRIPT > /tmp/run_tests.bash
set -ex

#!/bin/bash

cd \${GOPATH}/src/github.com/greenplum-db/gpbackup

git checkout ${GPBACKUP_VERSION}

# NOTE: There was a change to constraint handling in GPDB5 that caused an update
# to our test suite. Rather than revv the version of gpbackup that we are packaging
# with gpdb5, we've decided to simply cherry-pick the commit prior to running tests.
git checkout c149e8b7b671e931ca892f22c8cdef906512d591

tar -zxf ${CWDIR}/gpbackup_1.12.1_dependencies/*.tar.gz
go install github.com/onsi/ginkgo/ginkgo@v1

# Disable go modules before building
export GO111MODULE=off
make build integration

make end_to_end
SCRIPT

chmod +x /tmp/run_tests.bash
su - gpadmin "/tmp/run_tests.bash"
