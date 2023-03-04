#!/bin/bash

set -ex

export GOPATH=$(pwd)/go
export PATH=$PATH:${GOPATH}/bin

if [[ ${OS} == "RHEL6" ]]; then
    ## pre-installed version of ld has a bug that our CGO libs exercise. update binutils to avoid it
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
fi

if [[ -f /opt/gcc_env.sh ]]; then
    source /opt/gcc_env.sh
fi

# Build gpbackup
pushd ${GOPATH}/src/github.com/greenplum-db/gpbackup
  make depend build unit
  version=$(git describe --tags | perl -pe 's/(.*)-([0-9]*)-(g[0-9a-f]*)/\1+dev.\2.\3/')
popd
echo ${version} > go_components/gpbackup_version

if [[ "gpbackup version ${version}" != "$(${GOPATH}/bin/gpbackup --version)" ]]; then
  echo "unexpected difference in version recorded for gpbackup: expected ${version} to be same as:"
  ${GOPATH}/bin/gpbackup --version
  exit 1
fi

# Build s3 plugin
pushd ${GOPATH}/src/github.com/greenplum-db/gpbackup-s3-plugin
  make depend build unit
  s3_plugin_version=$(git describe --tags | perl -pe 's/(.*)-([0-9]*)-(g[0-9a-f]*)/\1+dev.\2.\3/')
popd
echo ${s3_plugin_version} > go_components/s3_plugin_version

if [[ "gpbackup_s3_plugin version ${s3_plugin_version}" != "$(${GOPATH}/bin/gpbackup_s3_plugin --version)" ]]; then
  echo "unexpected difference in version recorded for gpbackup_s3_plugin: expected ${s3_plugin_version} to be same as:"
  ${GOPATH}/bin/gpbackup_s3_plugin --version
  exit 1
fi

# Build gpbackup-manager
pushd ${GOPATH}/src/github.com/pivotal/gp-backup-manager
  make depend build unit
  gpbackup_manager_version=$(git describe --tags | perl -pe 's/(.*)-([0-9]*)-(g[0-9a-f]*)/\1+dev.\2.\3/')
popd
echo ${gpbackup_manager_version} > go_components/gpbackup_manager_version

# gpbackup_manager puts newline in front of version line
output=$(${GOPATH}/bin/gpbackup_manager --version | grep gpbackup)
if [[ "gpbackup_manager version ${gpbackup_manager_version}" != "$output" ]]; then
  echo "unexpected difference in version recorded for gpbackup_manager: expected 'gpbackup_manager version ${gpbackup_manager_version}' to be same as: '$output'"
  exit 1
fi

cp ${GOPATH}/bin/gpbackup go_components/
cp ${GOPATH}/bin/gpbackup_helper go_components/
cp ${GOPATH}/bin/gprestore go_components/
cp ${GOPATH}/bin/gpbackup_s3_plugin go_components/
cp ${GOPATH}/bin/gpbackup_manager go_components/
cd go_components && tar cfz go_components.tar.gz *
