#!/bin/bash

set -ex

ccp_src/scripts/setup_ssh_to_cluster.sh
ssh -t ubuntu@mdw "sudo wget https://storage.googleapis.com/golang/go1.12.7.linux-amd64.tar.gz && sudo tar -C /usr/local -xzf go1.12.7.linux-amd64.tar.gz"
ssh -t ubuntu@mdw "sudo mkdir /home/gpadmin/go && sudo chown gpadmin:gpadmin -R /home/gpadmin/go"
rsync -a gpbackup-dependencies mdw:/home/gpadmin
ssh -t mdw "mkdir -p /home/gpadmin/go/src/github.com/greenplum-db"
scp -r -q go/src/github.com/greenplum-db/gpbackup mdw:/home/gpadmin/go/src/github.com/greenplum-db/gpbackup

# Install gpbackup binaries using gppkg
cat << ENV_SCRIPT > /tmp/env.sh
  # export GOPATH=/home/gpadmin/go
  source /usr/local/greenplum-db-devel/greenplum_path.sh
  export PGPORT=5432
  export MASTER_DATA_DIRECTORY=/data/gpdata/master/gpseg-1
  # export PATH=\$GOPATH/bin:/usr/local/go/bin:\$PATH
ENV_SCRIPT
chmod +x /tmp/env.sh
scp /tmp/env.sh mdw:/home/gpadmin/env.sh

out=`ssh -t mdw 'source env.sh && psql postgres -c "select version();"'`
GPDB_VERSION=`echo $out | sed -n 's/.*Greenplum Database \([0-9]\).*/\1/p'`
mkdir /tmp/untarred
tar -xzf gppkgs/gpbackup-gppkgs.tar.gz -C /tmp/untarred
scp /tmp/untarred/gpbackup_tools*gp${GPDB_VERSION}*ubuntu*.gppkg mdw:/home/gpadmin


scp dummy_seclabel/dummy_seclabel*.so mdw:/usr/local/greenplum-db-devel/lib/postgresql/dummy_seclabel.so
scp dummy_seclabel/dummy_seclabel*.so sdw1:/usr/local/greenplum-db-devel/lib/postgresql/dummy_seclabel.so

ssh -t mdw "source env.sh; gppkg -i gpbackup_tools*ubuntu*.gppkg"

cat <<SCRIPT > /tmp/run_tests.bash
  set -ex
  export GOPATH=/home/gpadmin/go
  export PGPORT=5432
  export MASTER_DATA_DIRECTORY=/data/gpdata/master/gpseg-1
  export PATH=\$GOPATH/bin:/usr/local/go/bin:\$PATH

  tar -zxf gpbackup-dependencies/dependencies.tar.gz -C \$GOPATH/src/github.com

  cd \$GOPATH/src/github.com/greenplum-db/gpbackup
  make depend # Needed to install ginkgo
  # Source greenplum_path.sh after "make depend" to avoid certificate issues.
  source /usr/local/greenplum-db-devel/greenplum_path.sh

  gpconfig -c shared_preload_libraries -v dummy_seclabel
  gpstop -ra
  gpconfig -s shared_preload_libraries | grep dummy_seclabel

  # NOTE: This is a temporary hotfix intended to skip this test when running on CCP cluster because the backup artifact that this test is using only works on local clusters.
  sed -i 's|\tIt(\`gprestore continues when encountering errors during data load with --single-data-file and --on-error-continue\`, func() {|\tPIt(\`gprestore continues when encountering errors during data load with --single-data-file and --on-error-continue\`, func() {|g' end_to_end/end_to_end_suite_test.go
  sed -i 's|\tIt(\`ensure gprestore on corrupt backup with --on-error-continue logs error tables\`, func() {|\tPIt(\`ensure gprestore on corrupt backup with --on-error-continue logs error tables\`, func() {|g' end_to_end/end_to_end_suite_test.go
    sed -i 's|\tIt(\`ensure successful gprestore with --on-error-continue does not log error tables\`, func() {|\tPIt(\`ensure successful gprestore with --on-error-continue does not log error tables\`, func() {|g' end_to_end/end_to_end_suite_test.go
  make end_to_end_without_install
SCRIPT

chmod +x /tmp/run_tests.bash
scp /tmp/run_tests.bash mdw:/home/gpadmin/run_tests.bash
ssh -t mdw "bash /home/gpadmin/run_tests.bash"
