#!/bin/sh
set -ex

# USAGE: ./gpbackup_rpm.sh [gpbackup version] [source targz file]
# Example: ./gpbackup_rpm.sh 1.8.0 mybinaries.tar.gz
if [ "$#" -ne 2 ]; then
    echo "./gpbackup_rpm.sh [gpbackup version] [source targz file]"
fi

GPBACKUP_VERSION=$1
SOURCE_TARGZ=$2

GPBACKUP_DIR=$(dirname $0)/../..

# Create rpm directory structure
RPMROOT=/tmp/gpbackup_rpm
rm -rf ${RPMROOT}
mkdir -p ${RPMROOT}/{BUILD,RPMS,SOURCES,SPECS,SRPMS}

# Move source targz to SOURCES
cp ${SOURCE_TARGZ} ${RPMROOT}/SOURCES/.
cp ${GPBACKUP_DIR}/gppkg/gpbackup.spec.in ${RPMROOT}/SPECS/gpbackup.spec

rpmbuild -bb ${RPMROOT}/SPECS/gpbackup.spec --define "%_topdir ${RPMROOT}" --define "debug_package %{nil}" --define "gpbackup_version $GPBACKUP_VERSION"

echo "Successfully built RPM"
