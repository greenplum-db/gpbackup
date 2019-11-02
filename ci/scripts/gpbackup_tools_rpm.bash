#!/bin/bash

set -ex

# USAGE: ./gpbackup_tools_rpm.bash [rpm version] [source targz file] [OS]
# Example: ./gpbackup_tools_rpm.bash 1.8.0 mybinaries.tar.gz
if [[ "$#" -ne 2 ]]; then
    echo "./gpbackup_tools_rpm.bash [rpm version] [source targz file] [OS]"
fi

GPBACKUP_TOOLS_VERSION=$1
SOURCE_TARGZ=$2
OS=$3

GPBACKUP_DIR=$(dirname $0)/../..

# Create rpm directory structure
RPMROOT=/tmp/gpbackup_tools_rpm
rm -rf ${RPMROOT}
mkdir -p ${RPMROOT}/{BUILD,RPMS,SOURCES,SPECS,SRPMS}

# Move source targz to SOURCES
cp ${SOURCE_TARGZ} ${RPMROOT}/SOURCES/.
cp ${GPBACKUP_DIR}/gppkg/gpbackup_tools.spec.in ${RPMROOT}/SPECS/gpbackup_tools.spec

rpmbuild -bb ${RPMROOT}/SPECS/gpbackup_tools.spec --define "%_topdir ${RPMROOT}" --define "debug_package %{nil}" --define "rpm_version ${GPBACKUP_TOOLS_VERSION}" --define "operating_system ${OS}"

echo "Successfully built RPM"
