#!/bin/bash

set -ex

# get versions for gpbackup, s3_plugin and gpbackup_manager
pushd gpbackup-go-components
      tar -xzf go_components.tar.gz
      GPBACKUP_VERSION=`cat gpbackup_version`
      cp *_version ../gpbackup-tools-versions/
popd

# get version for ddboost_plugin
pushd gpbackup_ddboost_plugin
      DDBOOST_PLUGIN_VERSION=`git describe --tags | perl -pe 's/(.*)-([0-9]*)-(g[0-9a-f]*)/\1+dev.\2.\3/'`
popd
touch gpbackup-tools-versions/ddboost_plugin_version
echo ${DDBOOST_PLUGIN_VERSION} > gpbackup-tools-versions/ddboost_plugin_version

# get version for future .rpm and .deb files as well as .gppkg files
pushd pivnet_release_cache
  PRV_TILE_RELEASE_VERSION="v-${GPBACKUP_VERSION}*"
  if [ -f $PRV_TILE_RELEASE_VERSION ]; then
    # increment the counter if the expected release version has been used before
    COUNT=$(echo $PRV_TILE_RELEASE_VERSION | sed -n "s/v-${GPBACKUP_VERSION}-\([0-9]*\).*/\1/p")
    COUNT=$(($COUNT+1))
  else
    # reset the version count
    COUNT=1
  fi
  # NEXT_GPBACKUP_TOOLS_VERSION is the tile release version with the `-` changed to a `+`
  # because the `-` is reserved in RPM SPEC to denote `%{version}-%{release}` and the `_` is invalid symbol for .deb package name
  NEXT_GPBACKUP_TOOLS_VERSION=${GPBACKUP_VERSION}+${COUNT}
popd

touch gpbackup-tools-versions/pkg_version
echo ${NEXT_GPBACKUP_TOOLS_VERSION} > gpbackup-tools-versions/pkg_version


