#!/bin/bash

set -ex

# setup cluster and install gpbackup tools using gppkg
ccp_src/scripts/setup_ssh_to_cluster.sh

GO_VERSION=1.13.4
GPHOME=/usr/local/greenplum-db-devel

ssh -t ${default_ami_user}@mdw " \
    sudo wget https://storage.googleapis.com/golang/go${GO_VERSION}.linux-amd64.tar.gz && \
    sudo tar -C /usr/local -xzf go${GO_VERSION}.linux-amd64.tar.gz && \
    sudo mkdir -p /home/gpadmin/go/src/github.com/greenplum-db && \
    sudo chown gpadmin:gpadmin -R /home/gpadmin"

scp -r -q gpbackup mdw:/home/gpadmin/go/src/github.com/greenplum-db/gpbackup

if test -f dummy_seclabel/dummy_seclabel*.so; then
  scp dummy_seclabel/dummy_seclabel*.so mdw:${GPHOME}/lib/postgresql/dummy_seclabel.so
  scp dummy_seclabel/dummy_seclabel*.so sdw1:${GPHOME}/lib/postgresql/dummy_seclabel.so
fi

# Install gpbackup binaries using gppkg
cat << ENV_SCRIPT > /tmp/env.sh
  export GOPATH=/home/gpadmin/go
  source ${GPHOME}/greenplum_path.sh
  export PGPORT=5432
  export MASTER_DATA_DIRECTORY=/data/gpdata/master/gpseg-1
  export PATH=\${GOPATH}/bin:/usr/local/go/bin:\${PATH}
ENV_SCRIPT
chmod +x /tmp/env.sh
scp /tmp/env.sh mdw:/home/gpadmin/env.sh

out=$(ssh -t mdw 'source env.sh && psql postgres -c "select version();"')
GPDB_VERSION=$(echo ${out} | sed -n 's/.*Greenplum Database \([0-9]\).*/\1/p')
mkdir -p /tmp/untarred
tar -xzf gppkgs/gpbackup-gppkgs.tar.gz -C /tmp/untarred
scp /tmp/untarred/gpbackup_tools*gp${GPDB_VERSION}*${OS}*.gppkg mdw:/home/gpadmin
ssh -t mdw "source env.sh; gppkg -i gpbackup_tools*.gppkg"

cat <<SCRIPT > /tmp/run_tests.bash
  #!/bin/bash

  set -ex
  source env.sh

  if test -f ${GPHOME}/lib/postgresql/dummy_seclabel.so; then
    gpconfig -c shared_preload_libraries -v dummy_seclabel
    gpstop -ar
    gpconfig -s shared_preload_libraries | grep dummy_seclabel
  fi

  cd \${GOPATH}/src/github.com/greenplum-db/gpbackup
  make depend # Needed to install ginkgo

  # NOTE: This is a temporary hotfix intended to skip these tests when running on CCP cluster
  #       because the backup artifact that these tests are using only works on local clusters.
  sed -i 's|\tIt\(.*\)\(--on-error-continue\)|\tPIt\1\2|' end_to_end/end_to_end_suite_test.go
  make end_to_end
SCRIPT

chmod +x /tmp/run_tests.bash
scp /tmp/run_tests.bash mdw:/home/gpadmin/run_tests.bash
ssh -t mdw "/home/gpadmin/run_tests.bash"
