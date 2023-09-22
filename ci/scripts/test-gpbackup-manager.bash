#!/bin/bash

set -ex

# setup cluster and install gpbackup tools using gppkg
ccp_src/scripts/setup_ssh_to_cluster.sh
out=$(ssh -t cdw 'source env.sh && psql postgres -c "select version();"')
TEST_GPDB_VERSION=$(echo ${out} | sed -n 's/.*Greenplum Database \([0-9].[0-9]\+.[0-9]\+\).*/\1/p')
GPDB_VERSION=$(echo ${TEST_GPDB_VERSION} | head -c 1)
mkdir -p /tmp/untarred
tar -xzf gppkgs/gpbackup-gppkgs.tar.gz -C /tmp/untarred
scp /tmp/untarred/gpbackup_tools*gp${GPDB_VERSION}*${OS}*.gppkg cdw:/home/gpadmin
ssh -t cdw "source env.sh; gppkg -q gpbackup*gp*.gppkg | grep 'is installed' || gppkg -i gpbackup_tools*.gppkg"

ssh cdw "mkdir -p /home/gpadmin/go/src/github.com/pivotal/gp-backup-manager"
pushd gpbackup_manager_src
  tar -cvf concourse_sucks.tar .
  scp concourse_sucks.tar cdw:/tmp
popd


cat <<SCRIPT > /tmp/run_tests.bash
  #!/bin/bash

  set -ex
  source env.sh

  tar -xf /tmp/concourse_sucks.tar -C /home/gpadmin/go/src/github.com/pivotal/gp-backup-manager
  cd \${GOPATH}/src/github.com/pivotal/gp-backup-manager

  export OLD_BACKUP_VERSION="${GPBACKUP_VERSION}"

  # install pgcrypto; works for GPDB 5.22+ and 6+
  psql -d postgres -c "CREATE EXTENSION IF NOT EXISTS pgcrypto"

  make unit integration

  if [ -z "\${OLD_BACKUP_VERSION}" ] ; then
    # Print the version to help distinguish between the "old version" and "current version" tests
    gpbackup --version
    gpssh -f /home/gpadmin/segment_host_list "source /usr/local/greenplum-db-devel/greenplum_path.sh; gpbackup_helper --version"

    make end_to_end
  else
    # Print the version to help distinguish between the "old version" and "current version" tests
    gpbackup --version
    gpssh -f /home/gpadmin/segment_host_list "source /usr/local/greenplum-db-devel/greenplum_path.sh; gpbackup_helper --version"

    # TODO: It might be nice to make this a tarfile, like with the backwards compatibility job, but this works fine as-is
    pushd \${GOPATH}/src/github.com/greenplum-db/gpbackup
      git checkout \${OLD_BACKUP_VERSION}
      make build

      cp \${GOPATH}/bin/gpbackup \${GPHOME}/bin/.
      cp \${GOPATH}/bin/gpbackup_helper \${GPHOME}/bin/.
      gpscp -f /home/gpadmin/segment_host_list \${GOPATH}/bin/gpbackup =:\${GPHOME}/bin/.
      gpscp -f /home/gpadmin/segment_host_list \${GOPATH}/bin/gpbackup_helper =:\${GPHOME}/bin/.
    popd

    make end_to_end
  fi

SCRIPT

chmod +x /tmp/run_tests.bash
scp /tmp/run_tests.bash cdw:/home/gpadmin/run_tests.bash
ssh -t cdw "/home/gpadmin/run_tests.bash"
