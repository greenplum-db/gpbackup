#!/bin/bash

set -ex

ccp_src/scripts/setup_ssh_to_cluster.sh
out=`ssh -t mdw 'source env.sh && psql postgres -c "select version();"'`
GPDB_VERSION=`echo ${out} | sed -n 's/.*Greenplum Database \([0-9]\).*/\1/p'`
mkdir /tmp/untarred
tar -xzf gppkgs/gpbackup-gppkgs.tar.gz -C /tmp/untarred
scp /tmp/untarred/gpbackup_tools*gp${GPDB_VERSION}*RHEL*.gppkg mdw:/home/gpadmin

# place correct tarballs in gpbackup dir for consumption
if [[ -f "bin_gpbackup_1.0.0_and_1.7.1/gpbackup_bins_1.0.0_and_1.7.1.tar.gz" ]] && \
   [[ "${GPBACKUP_VERSION}" != "" ]] ; then
  tar -xzf bin_gpbackup_1.0.0_and_1.7.1/gpbackup_bins_1.0.0_and_1.7.1.tar.gz -C /tmp/
  scp -r /tmp/${GPBACKUP_VERSION} mdw:/tmp
fi

cat <<SCRIPT > /tmp/run_tests.bash
  set -ex
  source env.sh

  # only install if not installed already
  is_installed_output=\$(source env.sh; gppkg -q gpbackup*gp*.gppkg)
  set +e
  echo \$is_installed_output | grep 'is installed'
  if [ \$? -ne 0 ] ; then
    set -e
    gppkg -i gpbackup*gp*.gppkg
  fi
  set -e
  cd \$GOPATH/src/github.com/greenplum-db/gpbackup
  export OLD_BACKUP_VERSION="${GPBACKUP_VERSION}"

  make unit
  make integration

  # NOTE: This is a temporary hotfix intended to skip this test when running on CCP cluster because the backup artifact that this test is using only works on local clusters.
  sed -i 's|\tIt(\`gprestore continues when encountering errors during data load with --single-data-file and --on-error-continue\`, func() {|\tPIt(\`gprestore continues when encountering errors during data load with --single-data-file and --on-error-continue\`, func() {|g' end_to_end/end_to_end_suite_test.go
  sed -i 's|\tIt(\`ensure gprestore on corrupt backup with --on-error-continue logs error tables\`, func() {|\tPIt(\`ensure gprestore on corrupt backup with --on-error-continue logs error tables\`, func() {|g' end_to_end/end_to_end_suite_test.go
  sed -i 's|\tIt(\`ensure successful gprestore with --on-error-continue does not log error tables\`, func() {|\tPIt(\`ensure successful gprestore with --on-error-continue does not log error tables\`, func() {|g' end_to_end/end_to_end_suite_test.go

  if [ -z "\$OLD_BACKUP_VERSION" ] ; then
    make end_to_end_without_install
  else
    make install_helper helper_path=/tmp/\${OLD_BACKUP_VERSION}/gpbackup_helper
    ginkgo -r -randomizeSuites -slowSpecThreshold=10 -noisySkippings=false -randomizeAllSpecs end_to_end -- --custom_backup_dir "/tmp" 2>&1
  fi
SCRIPT

chmod +x /tmp/run_tests.bash
scp /tmp/run_tests.bash mdw:/home/gpadmin/run_tests.bash
ssh -t mdw "bash /home/gpadmin/run_tests.bash"
