#!/bin/sh
set -ex

# USAGE: ./gpbackup_gppkg.sh [gpbackup version] [gpdb major version] [os]
# Example: ./gpbackup_gppkg.sh 1.8.0 6 rhel6
if [ "$#" -ne 3 ]; then
    echo "./gpbackup_gppkg.sh [gpbackup version] [gpdb major version] [os]"
fi

GPBACKUP_VERSION=$1
GPDB_MAJOR_VERSION=$2
OS=$3

GPBACKUP_DIR=$(dirname $0)/../..
RPMROOT=/tmp/gpbackup_rpm

GPPKG_DEST_DIR=gpbackup_gppkg
GPBACKUP_GPPKG=gpbackup-${GPBACKUP_VERSION}-gp${GPDB_MAJOR_VERSION}-${OS}-x86_64.gppkg

# Create gppkg directory structure
GPPKG_SOURCE_DIR=/tmp/gpbackup_gppkg
rm -rf ${GPPKG_SOURCE_DIR}
mkdir -p ${GPPKG_SOURCE_DIR}/deps

# Interpolate version values to create gppkg yaml file
rm -f temp.yml
( echo "cat <<EOF >${GPPKG_SOURCE_DIR}/gppkg_spec.yml";   cat ${GPBACKUP_DIR}/gppkg/gppkg_spec.yml.in;   echo "EOF"; ) >temp.yml
. ./temp.yml
rm -f temp.yml

cp ${RPMROOT}/RPMS/x86_64/*.rpm ${GPPKG_SOURCE_DIR}
gppkg --build ${GPPKG_SOURCE_DIR}
echo "Successfully built gppkg"

if [ ${GPDB_MAJOR_VERSION} == '5' ] && [ ${OS} == "rhel6" ]; then
    echo "Testing installation of gpbackup using gppkg"
    GPBIN=${GPHOME}/bin
    rm -f ${GPBIN}/gpbackup
    rm -f ${GPBIN}/gprestore
    rm -f ${GPBIN}/gpbackup_helper

    gppkg -i ${GPBACKUP_GPPKG}
    if [ ! -f ${GPBIN}/gpbackup ] || [ ! -f ${GPBIN}/gprestore ] || [ ! -f ${GPBIN}/gpbackup_helper ]; then
        echo "Failed to install gpbackup using gppkg!"
        exit 1
    fi

    gppkg -r gpbackup
    if [ -f ${GPBIN}/gpbackup ] || [ -f ${GPBIN}/gprestore ] || [ -f ${GPBIN}/gpbackup_helper ]; then
        echo "Failed to remove gpbackup using gppkg!"
        exit 1
    fi
    echo "gpbackup_gppkg installation test passed"
fi

echo "Moving ${GPBACKUP_GPPKG} to ${GPPKG_DEST_DIR}"
mkdir -p ${GPPKG_DEST_DIR}
mv ${GPBACKUP_GPPKG} ${GPPKG_DEST_DIR}/.
