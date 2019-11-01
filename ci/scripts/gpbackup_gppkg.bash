#!/bin/bash

set -ex

# USAGE: ./gpbackup_gppkg.bash [gpbackup version] [gpdb major version] [os]
# Example: ./gpbackup_gppkg.bash 1.8.0 6 rhel6
if [[ "$#" -ne 3 ]]; then
    echo "./gpbackup_gppkg.bash [gpbackup version] [gpdb major version] [os]"
fi

export GPBACKUP_VERSION=$1
export GPDB_MAJOR_VERSION=$2
export OS=$3

GPBACKUP_DIR=$(dirname $0)/../..
RPMROOT=/tmp/gpbackup_tools_rpm

GPPKG_DEST_DIR=gpbackup_gppkg
GPBACKUP_GPPKG=gpbackup_tools-${GPBACKUP_VERSION}-gp${GPDB_MAJOR_VERSION}-${OS}-x86_64.gppkg

# Create gppkg directory structure
GPPKG_SOURCE_DIR=/tmp/gpbackup_gppkg
rm -rf ${GPPKG_SOURCE_DIR}
mkdir -p ${GPPKG_SOURCE_DIR}/deps

# Interpolate version values to create gppkg yaml file
envsubst < ${GPBACKUP_DIR}/gppkg/gppkg_spec.yml.in > ${GPPKG_SOURCE_DIR}/gppkg_spec.yml
cat ${GPPKG_SOURCE_DIR}/gppkg_spec.yml

cp ${RPMROOT}/RPMS/x86_64/*${OS}*.rpm ${GPPKG_SOURCE_DIR}
gppkg --build ${GPPKG_SOURCE_DIR}
echo "Successfully built gppkg"

if [[ ${GPDB_MAJOR_VERSION} == '6' ]] && [[ ${OS} == "rhel6" ]]; then
    echo "Testing installation of gpbackup using gppkg"

    GPBIN=${GPHOME}/bin
    rm -f ${GPBIN}/gpbackup ${GPBIN}/gprestore ${GPBIN}/gpbackup_helper

    gppkg -i ${GPBACKUP_GPPKG}
    if [[ ! -f ${GPBIN}/gpbackup ]] || [[ ! -f ${GPBIN}/gprestore ]] || [[ ! -f ${GPBIN}/gpbackup_helper ]]; then
        echo "Failed to install gpbackup using gppkg!"
        exit 1
    fi

    gppkg -r gpbackup
    if [[ -f ${GPBIN}/gpbackup ]] || [[ -f ${GPBIN}/gprestore ]] || [[ -f ${GPBIN}/gpbackup_helper ]]; then
        echo "Failed to remove gpbackup using gppkg!"
        exit 1
    fi
    echo "gpbackup_gppkg installation test passed"
fi

echo "Moving ${GPBACKUP_GPPKG} to ${GPPKG_DEST_DIR}"
mkdir -p ${GPPKG_DEST_DIR}
mv ${GPBACKUP_GPPKG} ${GPPKG_DEST_DIR}/.
