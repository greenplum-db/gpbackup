#!/bin/bash

set -ex

GPBACKUP_TOOLS_VERSION=`cat gpbackup-tools-versions/pkg_version`
ARCH=amd64
GPDB_MAJOR_VERSION="6"

############# Creates .deb installation files for Ubuntu ##############

echo "Building deb installer for gpbackup version: ${GPBACKUP_TOOLS_VERSION} gpdb version: ${GPDB_MAJOR_VERSION} platform: ${OS}"

DEB_NAME=gpbackup_tools-${GPBACKUP_TOOLS_VERSION}-gp${GPDB_MAJOR_VERSION}-${OS}-amd64.deb
PACKAGE_NAME=${DEB_NAME%.*}

mkdir -p deb_build_dir
pushd deb_build_dir
    mkdir -p ${PACKAGE_NAME}/DEBIAN
    # control file
    cat <<EOF >${PACKAGE_NAME}/DEBIAN/control
Package: GreenplumBackupTools
Priority: extra
Maintainer: gpdb-dp@pivotal.io
Architecture: ${ARCH}
Version: ${GPBACKUP_TOOLS_VERSION}
Provides: gpbackup_tools
Description: gpbackup and gprestore are Go utilities for performing Greenplum Database backups.
Homepage: https://github.com/greenplum-db/gpbackup
EOF
    tar -xzf ../gpbackup_tar/bin_gpbackup.tar.gz -C ${PACKAGE_NAME}
    dpkg-deb --build ${PACKAGE_NAME}
popd

############# Creates gppkg from .deb ##############

# Install gpdb binaries
mv bin_gpdb/{*.tar.gz,bin_gpdb.tar.gz}
mkdir -p /usr/local/greenplum-db-devel
tar -xzf bin_gpdb/bin_gpdb.tar.gz -C /usr/local/greenplum-db-devel
source /usr/local/greenplum-db-devel/greenplum_path.sh

# spec file
cat <<EOF >"gppkg_spec.yml"
Pkgname: gpbackup_tools
Architecture: ${ARCH}
OS: ${OS}
Version: ${GPBACKUP_TOOLS_VERSION}-gp${GPDB_MAJOR_VERSION}
GPDBVersion: ${GPDB_MAJOR_VERSION}
Description: gpbackup and gprestore are Go utilities for performing Greenplum Database backups.
EOF

mkdir -p gppkg
cp gppkg_spec.yml deb_build_dir/${DEB_NAME} gppkg/
gppkg --build gppkg/
echo "Successfully built gppkg"

########### Prepare to publish output ###########

mv ${PACKAGE_NAME}.gppkg gppkgs/
