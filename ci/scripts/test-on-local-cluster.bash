#!/bin/bash

set -ex

# Add locale for locale tests
localedef -c -i de_DE -f UTF-8 de_DE
echo LANG=\"de_DE\" >> /etc/locale.conf
source /etc/locale.conf

## old versions of ld have a bug that our CGO libs exercise. update binutils to avoid it
set +e
OLDLDVERSION=$(ld --version | grep "2.25");
set -e

mkdir /tmp/untarred
tar -xzf gppkgs/gpbackup-gppkgs.tar.gz -C /tmp/untarred
if [[ -d gp-pkg ]] ; then
    mkdir /tmp/gppkgv2
    tar -xzf gp-pkg/gppkg* -C /tmp/gppkgv2
fi

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

    editConfig() {
    	echo "editing \$1"
    	sed -i "s/^shared_preload_libraries=.*//g" \$1 && echo "shared_preload_libraries='dummy_seclabel'" >> \$1
    }

    export -f editConfig
    find $(pwd)/gpdb_src -name postgresql.conf -exec bash -c 'editConfig "\$0"' {} \;
    gpstop -ra
fi

# Install gpbackup gppkg
out=\$(psql postgres -c "select version();")
GPDB_VERSION=\$(echo \$out | sed -n 's/.*Greenplum Database \([0-9]\).*/\1/p')

if [[ -f /tmp/gppkgv2/gppkg ]] ; then
    /tmp/gppkgv2/gppkg install -a /tmp/untarred/gpbackup*gp\${GPDB_VERSION}*${OS}*.gppkg
else
    gppkg -i /tmp/untarred/gpbackup*gp\${GPDB_VERSION}*${OS}*.gppkg
fi

# Get the GPDB version to use for the unit tests
export TEST_GPDB_VERSION=\$(echo \$out | sed -n 's/.*Greenplum Database \([0-9].[0-9]\+.[0-9]\+\).*/\1/p')

# Test gpbackup
pushd \${GOPATH}/src/github.com/greenplum-db/gpbackup
  make unit integration end_to_end
popd
SCRIPT

chmod +x /tmp/run_tests.bash
su - gpadmin "/tmp/run_tests.bash"

