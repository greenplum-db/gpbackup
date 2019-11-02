#!/bin/bash

set -ex

ccp_src/scripts/setup_ssh_to_cluster.sh

cat <<SCRIPT > /tmp/setup_env.bash
set -ex
    cat << ENV_SCRIPT > env.sh
    export GOPATH=/home/gpadmin/go
    source /usr/local/greenplum-db-devel/greenplum_path.sh
    export PGPORT=5432
    export MASTER_DATA_DIRECTORY=/data/gpdata/master/gpseg-1
    export PATH=\\\$GOPATH/bin:/usr/local/go/bin:\\\$PATH
ENV_SCRIPT

export GOPATH=/home/gpadmin/go
chown gpadmin:gpadmin -R \$GOPATH
chmod +x env.sh
source env.sh
gpconfig --skipvalidation -c fsync -v off
if test -f /usr/local/greenplum-db-devel/lib/postgresql/dummy_seclabel.so; then
    gpconfig -c shared_preload_libraries -v dummy_seclabel
fi
gpstop -ar

tar -zxf gpbackup-dependencies/dependencies.tar.gz -C \$GOPATH/src/github.com
pushd \$GOPATH/src/github.com/greenplum-db/gpbackup
    make depend
popd
SCRIPT

chmod +x /tmp/setup_env.bash

if test -f dummy_seclabel/dummy_seclabel*.so; then
    scp dummy_seclabel/dummy_seclabel*.so mdw:/usr/local/greenplum-db-devel/lib/postgresql/dummy_seclabel.so
    scp dummy_seclabel/dummy_seclabel*.so sdw1:/usr/local/greenplum-db-devel/lib/postgresql/dummy_seclabel.so
fi

ssh -t ${default_ami_user}@mdw "sudo yum -y install git && wget https://storage.googleapis.com/golang/go1.12.7.linux-amd64.tar.gz && tar -xzf go1.12.7.linux-amd64.tar.gz && sudo mv go /usr/local"
ssh -t mdw "mkdir -p /home/gpadmin/go/src/github.com/greenplum-db"
rsync -a gpbackup-dependencies mdw:/home/gpadmin
scp /tmp/setup_env.bash mdw:/home/gpadmin/setup_env.bash
scp -r -q gpbackup mdw:/home/gpadmin/go/src/github.com/greenplum-db/gpbackup
ssh -t mdw "bash /home/gpadmin/setup_env.bash"
