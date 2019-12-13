#!/bin/bash

set -ex

ccp_src/scripts/setup_ssh_to_cluster.sh

cat <<SCRIPT > /tmp/run_tests.bash
set -ex

source env.sh

cd \${GOPATH}/src/github.com/greenplum-db/gpbackup

git checkout ${GPBACKUP_VERSION}

# NOTE: There was a change to constraint handling in GPDB5 that caused an update
# to our test suite. Rather than revv the version of gpbackup that we are packaging
# with gpdb5, we've decided to simply cherry-pick the commit prior to running tests.
git cherry-pick c149e8b7b671e931ca892f22c8cdef906512d591

tar -zxf ~/gpbackup_1.12.1_dependencies.tar.gz

make depend build integration

# NOTE: This is a temporary hotfix intended to skip these tests when running on CCP cluster
#       because the backup artifact that these tests are using only works on local clusters.
sed -i 's|\tIt\(.*\)\(--on-error-continue\)|\tPIt\1\2|' end_to_end/end_to_end_suite_test.go

make end_to_end
SCRIPT

chmod +x /tmp/run_tests.bash
scp /tmp/run_tests.bash mdw:/home/gpadmin/run_tests.bash

rsync -a gpbackup_1.12.1_dependencies/gpbackup_1.12.1_dependencies.tar.gz mdw:/home/gpadmin

ssh -t mdw "bash /home/gpadmin/run_tests.bash"
