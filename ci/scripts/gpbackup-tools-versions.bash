#!/bin/bash

set -ex

# get versions for gpbackup, s3_plugin and gpbackup_manager
pushd gpbackup-go-components-rhel8
      tar -xzf go_components.tar.gz
      GPBACKUP_VERSION=$(cat gpbackup_version)
      cp *_version ../gpbackup-tools-versions/
popd
echo ${GPBACKUP_VERSION} > gpbackup-tools-versions/pkg_version

# get version for ddboost_plugin
pushd gpbackup_ddboost_plugin
      DDBOOST_PLUGIN_VERSION=$(git describe --tags | perl -pe 's/(.*)-([0-9]*)-(g[0-9a-f]*)/\1+dev.\2.\3/')
popd
echo ${DDBOOST_PLUGIN_VERSION} > gpbackup-tools-versions/ddboost_plugin_version
