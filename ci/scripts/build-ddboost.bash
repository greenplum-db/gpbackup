#!/bin/bash

set -ex

# Install gpdb binaries (required because ddboost has a dependency on libpq-fe.h)
mkdir -p /usr/local/greenplum-db-devel
tar -xzf bin_gpdb/*.tar.gz -C /usr/local/greenplum-db-devel
source /usr/local/greenplum-db-devel/greenplum_path.sh

# build ddboost plugin
pushd gpbackup_ddboost_plugin
    source /opt/gcc_env.sh || true
    make build
    ddboost_plugin_version=$(git describe --tags | perl -pe 's/(.*)-([0-9]*)-(g[0-9a-f]*)/\1+dev.\2.\3/')
popd

echo ${ddboost_plugin_version} > ddboost_components/ddboost_plugin_version
cp gpbackup_ddboost_plugin/gpbackup_ddboost_plugin ddboost_components/
cp gpbackup_ddboost_plugin/DDBoostSDK/lib/release/linux/64/libDDBoost.so ddboost_components/
