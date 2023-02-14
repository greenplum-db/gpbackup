#!/bin/bash

set -ex

GO_VERSION=1.17.6

# If go is not installed or it's not the expected version, install the expected version
if ! command -v go &> /dev/null || ! $(go version | grep -q ${GO_VERSION}); then
  wget https://storage.googleapis.com/golang/go${GO_VERSION}.linux-amd64.tar.gz
  rm -rf /usr/local/go && tar -xzf go${GO_VERSION}.linux-amd64.tar.gz -C /usr/local
fi

# Add locale for locale tests
localedef -c -i de_DE -f UTF-8 de_DE
echo LANG=\"de_DE\" >> /etc/locale.conf
source /etc/locale.conf

## old versions of ld have a bug that our CGO libs exercise. update binutils to avoid it
set +e
OLDLDVERSION=$(ld --version | grep "2.25");
set -e
if [[ ${OS} == "RHEL6" ]]; then
    ## have to use vault to update because centos6 is EOL
    sudo yum install -y centos-release-scl
    echo "https://vault.centos.org/6.10/os/x86_64/" > /var/cache/yum/x86_64/6/C6.10-base/mirrorlist.txt
    echo "http://vault.centos.org/6.10/extras/x86_64/" > /var/cache/yum/x86_64/6/C6.10-extras/mirrorlist.txt
    echo "http://vault.centos.org/6.10/updates/x86_64/" > /var/cache/yum/x86_64/6/C6.10-updates/mirrorlist.txt
    mkdir /var/cache/yum/x86_64/6/centos-sclo-rh/
    echo "http://vault.centos.org/6.10/sclo/x86_64/rh" > /var/cache/yum/x86_64/6/centos-sclo-rh/mirrorlist.txt
    mkdir /var/cache/yum/x86_64/6/centos-sclo-sclo/
    echo "http://vault.centos.org/6.10/sclo/x86_64/sclo" > /var/cache/yum/x86_64/6/centos-sclo-sclo/mirrorlist.txt
    sudo yum install -y devtoolset-7-binutils*
    \cp /opt/rh/devtoolset-7/root/usr/bin/ld /usr/bin/ld
elif [[ $OLDLDVERSION != "" ]]; then
    yum install -y binutils
fi

mkdir /tmp/untarred
tar -xzf gppkgs/gpbackup-gppkgs.tar.gz -C /tmp/untarred

if [[ ! -f bin_gpdb/bin_gpdb.tar.gz ]] ; then
  mv bin_gpdb/*.tar.gz bin_gpdb/bin_gpdb.tar.gz
fi
source gpdb_src/concourse/scripts/common.bash
time install_gpdb
time ./gpdb_src/concourse/scripts/setup_gpadmin_user.bash
time NUM_PRIMARY_MIRROR_PAIRS=${LOCAL_CLUSTER_SIZE} make_cluster

cat <<SCRIPT > /tmp/run_tests.bash
#!/bin/bash

set -ex

# use "temp build dir" of parent shell
export GOPATH=\${HOME}/go
export PATH=/usr/local/go/bin:\$PATH:\${GOPATH}/bin:/opt/rh/devtoolset-7/root/usr/bin/
if [[ -f /opt/gcc_env.sh ]]; then
    source /opt/gcc_env.sh
fi
mkdir -p \${GOPATH}/bin \${GOPATH}/src/github.com/greenplum-db
cp -R $(pwd)/gpbackup \${GOPATH}/src/github.com/greenplum-db/

# Install dependencies before sourcing greenplum path. Using the GPDB curl is causing issues.
pushd \${GOPATH}/src/github.com/greenplum-db/gpbackup
  make depend
popd

source /usr/local/greenplum-db-devel/greenplum_path.sh
source $(pwd)/gpdb_src/gpAux/gpdemo/gpdemo-env.sh

mkdir -p "\${GPHOME}/postgresql"

if [ ${REQUIRES_DUMMY_SEC} ]; then
  # dummy security label: copy library from bucket to correct location
  install -m 755 -T $(pwd)/dummy_seclabel/dummy_seclabel*.so "\${GPHOME}/lib/postgresql/dummy_seclabel.so"
  gpconfig -c shared_preload_libraries -v dummy_seclabel
  gpstop -ra
  gpconfig -s shared_preload_libraries | grep dummy_seclabel
fi

# Install gpbackup gppkg
out=\$(psql postgres -c "select version();")
GPDB_VERSION=\$(echo \$out | sed -n 's/.*Greenplum Database \([0-9]\).*/\1/p')
gppkg -i /tmp/untarred/gpbackup*gp\${GPDB_VERSION}*${OS}*.gppkg

# Get the GPDB version to use for the unit tests
export TEST_GPDB_VERSION=\$(echo \$out | sed -n 's/.*Greenplum Database \([0-9].[0-9]\+.[0-9]\+\).*/\1/p')

# Test gpbackup
pushd \${GOPATH}/src/github.com/greenplum-db/gpbackup
  make unit integration end_to_end
popd
SCRIPT

chmod +x /tmp/run_tests.bash
su - gpadmin "/tmp/run_tests.bash"

