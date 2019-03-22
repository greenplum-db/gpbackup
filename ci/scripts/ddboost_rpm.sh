#!/bin/sh
set -ex

# USAGE: ./ddboost_rpm.sh [ddboost version] [source targz file]
# Example: ./ddboost_rpm.sh 1.8.0 mybinaries.tar.gz
if [ "$#" -ne 2 ]; then
    echo "./ddboost_rpm.sh [ddboost plugin version] [source targz file]"
fi

DDBOOST_PLUGIN_VERSION=$1
SOURCE_TARGZ=$2

DDBOOST_DIR=$(dirname $0)/../..

# Create rpm directory structure
RPMROOT=/tmp/ddboost_rpm
rm -rf ${RPMROOT}
mkdir -p ${RPMROOT}/{BUILD,RPMS,SOURCES,SPECS,SRPMS}

# Move source targz to SOURCES
cp ${SOURCE_TARGZ} ${RPMROOT}/SOURCES/.
cp ${DDBOOST_DIR}/gppkg/ddboost.spec.in ${RPMROOT}/SPECS/ddboost.spec

rpmbuild -bb ${RPMROOT}/SPECS/ddboost.spec --define "%_topdir ${RPMROOT}" --define "debug_package %{nil}" --define "ddboost_plugin_version $DDBOOST_PLUGIN_VERSION"

echo "Successfully built ddboost RPM"
