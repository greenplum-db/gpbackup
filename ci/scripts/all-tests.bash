#!/bin/bash

set -ex

# setup cluster and install gpbackup tools using gppkg
ccp_src/scripts/setup_ssh_to_cluster.sh
out=$(ssh -t mdw 'source env.sh && psql postgres -c "select version();"')
TEST_GPDB_VERSION=$(echo ${out} | sed -n 's/.*Greenplum Database \([0-9].[0-9]\+.[0-9]\+\).*/\1/p')
GPDB_VERSION=$(echo ${TEST_GPDB_VERSION} | head -c 1)
mkdir -p /tmp/untarred
tar -xzf gppkgs/gpbackup-gppkgs.tar.gz -C /tmp/untarred
scp /tmp/untarred/gpbackup_tools*gp${GPDB_VERSION}*RHEL*.gppkg mdw:/home/gpadmin
ssh -t mdw "source env.sh; gppkg -q gpbackup*gp*.gppkg | grep 'is installed' || gppkg -i gpbackup_tools*.gppkg"

# place correct tarballs in gpbackup dir for consumption
if [[ -f "bin_gpbackup_1.0.0_and_1.7.1/gpbackup_bins_1.0.0_and_1.7.1.tar.gz" ]] && \
   [[ "${GPBACKUP_VERSION}" != "" ]] ; then
  tar -xzf bin_gpbackup_1.0.0_and_1.7.1/gpbackup_bins_1.0.0_and_1.7.1.tar.gz -C /tmp/
  scp -r /tmp/${GPBACKUP_VERSION} mdw:/tmp
fi

cat <<SCRIPT > /tmp/run_tests.bash
  #!/bin/bash

  set -ex
  source env.sh
  if [[ -f /opt/gcc_env.sh ]]; then
      source /opt/gcc_env.sh
  fi
  cd \${GOPATH}/src/github.com/greenplum-db/gpbackup
  export OLD_BACKUP_VERSION="${GPBACKUP_VERSION}"

  # Set the GPDB version to use for the unit tests
  export TEST_GPDB_VERSION=${TEST_GPDB_VERSION}

  make unit integration

  # NOTE: This is a temporary hotfix intended to skip these tests when running on CCP cluster
  #       because the backup artifact that these tests are using only works on local clusters.
  sed -i 's|\tIt\(.*\)\(--on-error-continue\)|\tPIt\1\2|' end_to_end/end_to_end_suite_test.go

  if [ -z "\${OLD_BACKUP_VERSION}" ] ; then
    make end_to_end
  else
    make install helper_path=/tmp/\${OLD_BACKUP_VERSION}/gpbackup_helper
    ginkgo -r -randomizeSuites -slowSpecThreshold=10 -noisySkippings=false -randomizeAllSpecs end_to_end -- --custom_backup_dir "/tmp" 2>&1
  fi
SCRIPT

chmod +x /tmp/run_tests.bash
scp /tmp/run_tests.bash mdw:/home/gpadmin/run_tests.bash
ssh -t mdw "/home/gpadmin/run_tests.bash"
