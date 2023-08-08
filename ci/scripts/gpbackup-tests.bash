#!/bin/bash

set -ex

# setup cluster and install gpbackup tools using gppkg
ccp_src/scripts/setup_ssh_to_cluster.sh

GPHOME=/usr/local/greenplum-db-devel

ssh -t ${default_ami_user}@cdw " \
    sudo mkdir -p /home/gpadmin/go/src/github.com/greenplum-db && \
    sudo chown gpadmin:gpadmin -R /home/gpadmin"

scp -r -q gpbackup cdw:/home/gpadmin/go/src/github.com/greenplum-db/gpbackup

if test -f dummy_seclabel/dummy_seclabel*.so; then
  scp dummy_seclabel/dummy_seclabel*.so cdw:${GPHOME}/lib/postgresql/dummy_seclabel.so
  scp dummy_seclabel/dummy_seclabel*.so sdw1:${GPHOME}/lib/postgresql/dummy_seclabel.so
fi

# Install gpbackup binaries using gppkg
cat << ENV_SCRIPT > /tmp/env.sh
  export GOPATH=/home/gpadmin/go
  source ${GPHOME}/greenplum_path.sh
  export PGPORT=5432
  export COORDINATOR_DATA_DIRECTORY=/data/gpdata/coordinator/gpseg-1
  export MASTER_DATA_DIRECTORY=/data/gpdata/coordinator/gpseg-1
  export PATH=\${GOPATH}/bin:/usr/local/go/bin:\${PATH}
ENV_SCRIPT
chmod +x /tmp/env.sh
scp /tmp/env.sh cdw:/home/gpadmin/env.sh

out=$(ssh -t cdw 'source env.sh && psql postgres -c "select version();"')
GPDB_VERSION=$(echo ${out} | sed -n 's/.*Greenplum Database \([0-9]\).*/\1/p')
mkdir -p /tmp/untarred
tar -xzf gppkgs/gpbackup-gppkgs.tar.gz -C /tmp/untarred
scp /tmp/untarred/gpbackup_tools*gp${GPDB_VERSION}*${OS}*.gppkg cdw:/home/gpadmin
ssh -t cdw "source env.sh; gppkg -i gpbackup_tools*.gppkg"

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

  make end_to_end
SCRIPT

chmod +x /tmp/run_tests.bash
scp /tmp/run_tests.bash cdw:/home/gpadmin/run_tests.bash
ssh -t cdw "/home/gpadmin/run_tests.bash"
