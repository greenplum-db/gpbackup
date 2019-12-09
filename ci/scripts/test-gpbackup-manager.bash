#!/bin/bash

set -ex

if [[ ! -f bin_gpdb/bin_gpdb.tar.gz ]] ; then
  mv bin_gpdb/*.tar.gz bin_gpdb/bin_gpdb.tar.gz
fi

source gpdb_src/concourse/scripts/common.bash
time install_gpdb
time ./gpdb_src/concourse/scripts/setup_gpadmin_user.bash
time make_cluster

# copy gpbackup-manager into the GOPATH used by user "gpadmin"
export GOPATH=/home/gpadmin/go
mkdir -p ${GOPATH}/src/github.com/pivotal
cp -R gp-backup-manager ${GOPATH}/src/github.com/pivotal/

chown -R gpadmin ${GOPATH}

cat <<SCRIPT > /tmp/run_tests.bash
#!/bin/bash

set -ex

source /usr/local/greenplum-db-devel/greenplum_path.sh

# use "temp build dir" of parent shell
source $(pwd)/gpdb_src/gpAux/gpdemo/gpdemo-env.sh
export GOPATH=\$HOME/go
mkdir -p \${GOPATH}/bin \${GOPATH}/src
export PATH=/usr/local/go/bin:\$PATH:\${GOPATH}/bin

# Install gppkgs
mkdir /tmp/untarred
tar -xzf gppkgs/gpbackup-gppkgs.tar.gz -C /tmp/untarred
out=\$(psql postgres -c "select version();")
GPDB_VERSION=\$(echo \$out | sed -n 's/.*Greenplum Database \([0-9]\).*/\1/p')
gppkg -i /tmp/untarred/gpbackup*gp\${GPDB_VERSION}*RHEL*.gppkg

# install pgcrypto; works for GPDB 5.22+ and 6+
psql -d postgres -c "create extension pgcrypto"

# Test gpbackup manager
pushd \${GOPATH}/src/github.com/pivotal/gp-backup-manager
  make unit integration end_to_end
popd

SCRIPT

cp -r gppkgs /home/gpadmin
chmod +x /tmp/run_tests.bash
su - gpadmin "/tmp/run_tests.bash"
