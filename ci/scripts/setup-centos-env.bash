#!/bin/bash

set -ex

ccp_src/scripts/setup_ssh_to_cluster.sh

GO_VERSION=1.17.6
GPHOME=/usr/local/greenplum-db-devel

ssh -t ${default_ami_user}@mdw " \
    sudo yum -y install git && \
    sudo wget https://storage.googleapis.com/golang/go${GO_VERSION}.linux-amd64.tar.gz && \
    sudo tar -C /usr/local -xzf go${GO_VERSION}.linux-amd64.tar.gz && \
    sudo mkdir -p /home/gpadmin/go/src/github.com/greenplum-db && \
    sudo chown gpadmin:gpadmin -R /home/gpadmin && \
    sudo yum -y install zstd"

ssh -t ${default_ami_user}@sdw1 "sudo yum -y install zstd"

scp -r -q gpbackup mdw:/home/gpadmin/go/src/github.com/greenplum-db/gpbackup

if test -f dummy_seclabel/dummy_seclabel*.so; then
  scp dummy_seclabel/dummy_seclabel*.so mdw:${GPHOME}/lib/postgresql/dummy_seclabel.so
  scp dummy_seclabel/dummy_seclabel*.so sdw1:${GPHOME}/lib/postgresql/dummy_seclabel.so
fi

cat <<SCRIPT > /tmp/setup_env.bash
#!/bin/bash

set -ex
    cat << ENV_SCRIPT > env.sh
    export GOPATH=/home/gpadmin/go
    source ${GPHOME}/greenplum_path.sh
    export PGPORT=5432
    export COORDINATOR_DATA_DIRECTORY=/data/gpdata/coordinator/gpseg-1
    export MASTER_DATA_DIRECTORY=/data/gpdata/master/gpseg-1
    export PATH=\\\${GOPATH}/bin:/usr/local/go/bin:\\\${PATH}
    if [[ -f /opt/gcc_env.sh ]]; then
        source /opt/gcc_env.sh
    fi
ENV_SCRIPT

export GOPATH=/home/gpadmin/go
chown gpadmin:gpadmin -R \${GOPATH}
chmod +x env.sh
source env.sh
gpconfig --skipvalidation -c fsync -v off
if test -f ${GPHOME}/lib/postgresql/dummy_seclabel.so; then
    gpconfig -c shared_preload_libraries -v dummy_seclabel
fi
gpstop -ar
SCRIPT

chmod +x /tmp/setup_env.bash
scp /tmp/setup_env.bash mdw:/home/gpadmin/setup_env.bash
ssh -t mdw "/home/gpadmin/setup_env.bash"
