#!/bin/sh
set -ex

# USAGE: ./s3_gppkg.sh [s3 version] [gpdb major version] [os]
# Example: ./s3_gppkg.sh 1.8.0 6 rhel6
if [ "$#" -ne 3 ]; then
    echo "./s3_gppkg.sh [s3 version] [gpdb major version] [os]"
fi

S3_PLUGIN_VERSION=$1
GPDB_MAJOR_VERSION=$2
OS=$3

S3_DIR=$(dirname $0)/../..
RPMROOT=/tmp/s3_rpm

GPPKG_DEST_DIR=s3_gppkg
S3_GPPKG=s3-${S3_PLUGIN_VERSION}-gp${GPDB_MAJOR_VERSION}-${OS}-x86_64.gppkg

# Create gppkg directory structure
GPPKG_SOURCE_DIR=/tmp/s3_gppkg
rm -rf ${GPPKG_SOURCE_DIR}
mkdir -p ${GPPKG_SOURCE_DIR}/deps

# Interpolate version values to create gppkg yaml file
rm -f temp.yml
( echo "cat <<EOF >${GPPKG_SOURCE_DIR}/gppkg_spec.yml";   cat ${S3_DIR}/gppkg/s3_gppkg_spec.yml.in;   echo "EOF"; ) >temp.yml
. ./temp.yml
rm -f temp.yml

cp ${RPMROOT}/RPMS/x86_64/*.rpm ${GPPKG_SOURCE_DIR}
gppkg --build ${GPPKG_SOURCE_DIR}
echo "Successfully built gppkg"

if [ ${GPDB_MAJOR_VERSION} == '6' ] && [ ${OS} == "rhel6" ]; then
    echo "Testing installation of s3 using gppkg"
    GPBIN=${GPHOME}/bin
    rm -f ${GPBIN}/gpbackup_s3_plugin

    gppkg -i ${S3_GPPKG}
    if [ ! -f ${GPBIN}/gpbackup_s3_plugin ]; then
        echo "Failed to install s3 using gppkg!"
        exit 1
    fi

    gppkg -r s3
    if [ -f ${GPBIN}/gpbackup_s3_plugin ]; then
        echo "Failed to remove s3 using gppkg!"
        exit 1
    fi
    echo "s3_gppkg installation test passed"
fi

echo "Moving ${S3_GPPKG} to ${GPPKG_DEST_DIR}"
mkdir -p ${GPPKG_DEST_DIR}
mv ${S3_GPPKG} ${GPPKG_DEST_DIR}/.
