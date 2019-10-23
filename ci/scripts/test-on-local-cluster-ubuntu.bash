#!/bin/bash

set -ex
if [[ ! -f bin_gpdb/bin_gpdb.tar.gz ]] ; then
  mv bin_gpdb/*.tar.gz bin_gpdb/bin_gpdb.tar.gz
fi

DEBIAN_FRONTEND=noninteractive apt-get update && apt-get -yq install curl
wget https://storage.googleapis.com/golang/go1.12.7.linux-amd64.tar.gz && tar -xzf go1.12.7.linux-amd64.tar.gz && mv go /usr/local

source gpdb_src/concourse/scripts/common.bash
time install_gpdb
time ./gpdb_src/concourse/scripts/setup_gpadmin_user.bash
pushd gpdb_src/gpAux/gpdemo
  time su gpadmin -c "source /usr/local/greenplum-db-devel/greenplum_path.sh; make create-demo-cluster"
popd
cp -r gppkgs /home/gpadmin

cat <<SCRIPT > /tmp/run_tests.bash
#!/bin/bash
set -ex
cd ~

# use "temp build dir" of parent shell
export GOPATH=\$HOME/go
export PATH=/usr/local/go/bin:\$PATH:\$GOPATH/bin
mkdir -p \$GOPATH/bin \$GOPATH/src/github.com/greenplum-db

cp -R $(pwd)/gpbackup \$GOPATH/src/github.com/greenplum-db/
tar -zxf $(pwd)/gpbackup-dependencies/dependencies.tar.gz -C \$GOPATH/src/github.com

# Install dependencies before sourcing greenplum path. Using the GPDB curl is causing issues.
pushd \$GOPATH/src/github.com/greenplum-db/gpbackup
  make depend
popd

source /usr/local/greenplum-db-devel/greenplum_path.sh
source $(pwd)/gpdb_src/gpAux/gpdemo/gpdemo-env.sh
if [ ${REQUIRES_DUMMY_SEC} ]; then
  # dummy security label: copy library from bucket to correct location
  mkdir -p "\$GPHOME/postgresql"
  install -m 755 -T $(pwd)/dummy_seclabel/dummy_seclabel*.so "\$GPHOME/lib/postgresql/dummy_seclabel.so"
  gpconfig -c shared_preload_libraries -v dummy_seclabel
  gpstop -ra
  gpconfig -s shared_preload_libraries | grep dummy_seclabel
fi

# Install gpbackup gppkg
mkdir /tmp/untarred
tar -xzf gppkgs/gpbackup-gppkgs.tar.gz -C /tmp/untarred
gppkg -i /tmp/untarred/gpbackup*-ubuntu-*.gppkg

# Test gpbackup
pushd \$GOPATH/src/github.com/greenplum-db/gpbackup
  make integration
  make end_to_end_without_install
popd
SCRIPT

chmod +x /tmp/run_tests.bash
su - gpadmin bash -c /tmp/run_tests.bash
