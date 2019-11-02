#!/bin/bash

set -ex

ccp_src/scripts/setup_ssh_to_cluster.sh

cat <<SCRIPT > /tmp/run_tests.bash
set -ex
source env.sh

cd \$GOPATH/src/github.com/greenplum-db/gpbackup

git checkout ${GPBACKUP_VERSION}

# NOTE: There was a change to constraint handling in GPDB5 that caused an update
# to our test suite. Rather than revv the version of gpbackup that we are packaging
# with gpdb5, we've decided to simply cherry-pick the commit prior to running tests.
git cherry-pick c149e8b7b671e931ca892f22c8cdef906512d591

make build

# Possibly need to revert the dependencies back to ${GPBACKUP_VERSION} dependencies
make depend
make integration

# NOTE: This is a temporary hotfix intended to skip this test when running on CCP cluster because the backup artifact that this test is using only works on local clusters.
sed -i 's|\tIt(\`gprestore continues when encountering errors during data load with --single-data-file and --on-error-continue\`, func() {|\tPIt(\`gprestore continues when encountering errors during data load with --single-data-file and --on-error-continue\`, func() {|g' end_to_end/end_to_end_suite_test.go
sed -i 's|\tIt(\`ensure gprestore on corrupt backup with --on-error-continue logs error tables\`, func() {|\tPIt(\`ensure gprestore on corrupt backup with --on-error-continue logs error tables\`, func() {|g' end_to_end/end_to_end_suite_test.go
sed -i 's|\tIt(\`ensure successful gprestore with --on-error-continue does not log error tables\`, func() {|\tPIt(\`ensure successful gprestore with --on-error-continue does not log error tables\`, func() {|g' end_to_end/end_to_end_suite_test.go

make end_to_end
SCRIPT

chmod +x /tmp/run_tests.bash
scp /tmp/run_tests.bash mdw:/home/gpadmin/run_tests.bash
ssh -t mdw "bash /home/gpadmin/run_tests.bash"