#!/bin/sh
set -ex

# USAGE: ./s3_rpm.sh [s3 version] [source targz file]
# Example: ./s3_rpm.sh 1.8.0 mybinaries.tar.gz
if [ "$#" -ne 2 ]; then
    echo "./s3_rpm.sh [s3 plugin version] [source targz file]"
fi

S3_PLUGIN_VERSION=$1
SOURCE_TARGZ=$2

S3_DIR=$(dirname $0)/../..

# Create rpm directory structure
RPMROOT=/tmp/s3_rpm
rm -rf ${RPMROOT}
mkdir -p ${RPMROOT}/{BUILD,RPMS,SOURCES,SPECS,SRPMS}

# Move source targz to SOURCES
cp ${SOURCE_TARGZ} ${RPMROOT}/SOURCES/.
cp ${S3_DIR}/gppkg/s3.spec.in ${RPMROOT}/SPECS/s3.spec

rpmbuild -bb ${RPMROOT}/SPECS/s3.spec --define "%_topdir ${RPMROOT}" --define "debug_package %{nil}" --define "s3_plugin_version $S3_PLUGIN_VERSION"

echo "Successfully built s3 RPM"
