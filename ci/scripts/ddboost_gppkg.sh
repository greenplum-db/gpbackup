#!/bin/sh
set -ex

# USAGE: ./ddboost_gppkg.sh [ddboost version] [gpdb major version] [os]
# Example: ./ddboost_gppkg.sh 1.8.0 6 rhel6
if [ "$#" -ne 3 ]; then
    echo "./ddboost_gppkg.sh [ddboost version] [gpdb major version] [os]"
fi

DDBOOST_PLUGIN_VERSION=$1
GPDB_MAJOR_VERSION=$2
OS=$3

DDBOOST_DIR=$(dirname $0)/../..
RPMROOT=/tmp/ddboost_rpm

GPPKG_DEST_DIR=ddboost_gppkg
DDBOOST_GPPKG=ddboost-${DDBOOST_PLUGIN_VERSION}-gp${GPDB_MAJOR_VERSION}-${OS}-x86_64.gppkg

# Create gppkg directory structure
GPPKG_SOURCE_DIR=/tmp/ddboost_gppkg
rm -rf ${GPPKG_SOURCE_DIR}
mkdir -p ${GPPKG_SOURCE_DIR}/deps

# Interpolate version values to create gppkg yaml file
rm -f temp.yml
( echo "cat <<EOF >${GPPKG_SOURCE_DIR}/gppkg_spec.yml";   cat ${DDBOOST_DIR}/gppkg/ddboost_gppkg_spec.yml.in;   echo "EOF"; ) >temp.yml
. ./temp.yml
rm -f temp.yml

cp ${RPMROOT}/RPMS/x86_64/*.rpm ${GPPKG_SOURCE_DIR}
gppkg --build ${GPPKG_SOURCE_DIR}
echo "Successfully built gppkg"

if [ ${GPDB_MAJOR_VERSION} == '6' ] && [ ${OS} == "rhel6" ]; then
    echo "Testing installation of ddboost using gppkg"
    GPBIN=${GPHOME}/bin
    rm -f ${GPBIN}/gpbackup_ddboost_plugin

    gppkg -i ${DDBOOST_GPPKG}
    if [ ! -f ${GPBIN}/gpbackup_ddboost_plugin ]; then
        echo "Failed to install ddboost using gppkg!"
        exit 1
    fi

    gppkg -r ddboost
    if [ -f ${GPBIN}/gpbackup_ddboost_plugin ]; then
        echo "Failed to remove ddboost using gppkg!"
        exit 1
    fi
    echo "ddboost_gppkg installation test passed"
fi

echo "Moving ${DDBOOST_GPPKG} to ${GPPKG_DEST_DIR}"
mkdir -p ${GPPKG_DEST_DIR}
mv ${DDBOOST_GPPKG} ${GPPKG_DEST_DIR}/.
