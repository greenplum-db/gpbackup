#!/bin/bash

set -ex

########################################################################################
# Create native package installation files
function build_rpm_rhel() {
    export ARCH=x86_64
    GPDB_VER=( "5" "6" )
    RPMROOT=/tmp/gpbackup_tools_rpm
    mkdir -p ${RPMROOT}/{BUILD,RPMS,SOURCES,SPECS,SRPMS}

    # Move source targz to SOURCES
    cp gpbackup_tar/bin_gpbackup.tar.gz ${RPMROOT}/SOURCES/.
    cp gpbackup/gppkg/gpbackup_tools.spec.in ${RPMROOT}/SPECS/gpbackup_tools.spec

    sudo yum -y install rpm-build
    rpmbuild -bb ${RPMROOT}/SPECS/gpbackup_tools.spec \
         --define "%_topdir ${RPMROOT}" \
         --define "debug_package %{nil}" \
         --define "rpm_version ${GPBACKUP_TOOLS_VERSION}" \
         --define "operating_system ${OS}" \
         --define "_build_id_links none"

    PKG_FILES=${RPMROOT}/RPMS/x86_64/*${OS}*.rpm
}

function build_deb_ubuntu() {
    export ARCH=amd64
    GPDB_VER=( "6" )
    DEB_NAME=gpbackup_tools-${GPBACKUP_TOOLS_VERSION}-${OS}-amd64.deb
    PACKAGE_NAME=${DEB_NAME%.*}

    # gettext-base package is required to run envsubst command
    apt update && apt-get install -y gettext-base
    mkdir -p deb_build_dir
    pushd deb_build_dir
        mkdir -p ${PACKAGE_NAME}/DEBIAN
        # control file
        envsubst < ../gpbackup/gppkg/gpbackup_control.in > ${PACKAGE_NAME}/DEBIAN/control
        tar -xzf ../gpbackup_tar/bin_gpbackup.tar.gz -C ${PACKAGE_NAME}
        dpkg-deb --build ${PACKAGE_NAME}
    popd

    PKG_FILES=deb_build_dir/${DEB_NAME}
}
########################################################################################

########################################################################################
export GPBACKUP_TOOLS_VERSION=$(cat gpbackup-tools-versions/pkg_version)
echo "Building gppkg v1 installer for gpbackup version: ${GPBACKUP_TOOLS_VERSION} platform: ${OS}"

if [[ ${OS} == "RHEL6" || ${OS} == "RHEL7" ||  ${OS} == "RHEL8" ]]; then
    build_rpm_rhel
elif [[ ${OS} == "ubuntu" ]]; then
    build_deb_ubuntu
fi


# Install gpdb binaries
if [[ ! -f bin_gpdb/bin_gpdb.tar.gz ]]; then
  mv bin_gpdb/{*.tar.gz,bin_gpdb.tar.gz}
fi
mkdir -p /usr/local/greenplum-db-devel
tar -xzf bin_gpdb/bin_gpdb.tar.gz -C /usr/local/greenplum-db-devel

# Setup gpadmin user
gpdb_src/concourse/scripts/setup_gpadmin_user.bash
source /usr/local/greenplum-db-devel/greenplum_path.sh

# Create gppkg_v1 from native package
for i in ${GPDB_VER[@]}; do
    # spec file
    export GPDB_MAJOR_VERSION=${i}
    envsubst < gpbackup/gppkg/gppkg_spec.yml.in > gppkg_spec.yml
    cat gppkg_spec.yml

    mkdir -p gppkg
    cp gppkg_spec.yml ${PKG_FILES} gppkg/
    gppkg --build gppkg/
done
echo "Successfully built gppkg v1 for GPDB5 and GPDB6"
########################################################################################

########################################################################################
# gpdb7+ uses gppkg_v2, which does not require an rpm or deb package.  Instead just placed compiled
# binaries and config metadata into a folder for building
if [[ ${OS} == "RHEL8" ]]; then
    export ARCH=x86_64
    export GPBACKUP_TOOLS_VERSION=$(cat gpbackup-tools-versions/pkg_version)
    echo "Building gppkg v2 installer for gpbackup version: ${GPBACKUP_TOOLS_VERSION} platform: ${OS}"

    mkdir /tmp/gppkgv2
    tar -xzf gp-pkg/gppkg* -C /tmp/gppkgv2

    BUILDROOT=/tmp/gpbackup_tools_build
    mkdir -p ${BUILDROOT}/bin

    cp gpbackup_tar/bin_gpbackup.tar.gz ${BUILDROOT}/
    tar -xzvf ${BUILDROOT}/bin_gpbackup.tar.gz -C ${BUILDROOT}/bin

    # Create gppkg v2 from compiled binaries
    # spec file
    export GPDB_MAJOR_VERSION=7
    envsubst < gpbackup/gppkg/gppkg_v2_spec.yml.in > gppkg_spec.yml
    cat gppkg_spec.yml

    cp gppkg_spec.yml ${BUILDROOT}
    /tmp/gppkgv2/gppkg build -c $BUILDROOT/gppkg_spec.yml -i $BUILDROOT/bin

    # gppkg v2 defaults to appending .tar.gz to the gppkg file
    for file in *gppkg.tar.gz; do
        if [ -f "$file" ]; then
            new_name="${file%.gppkg.tar.gz}"
            mv "$file" "${new_name}-RHEL8-x86_64.gppkg"
        fi
    done

    # other build scripts expect the file to be named gpbackup_tools*, even though the package name
    # is greenplum_backup_restore*
    set +e
    rename greenplum_backup_restore gpbackup_tools *.gppkg
    set -e
    echo "Successfully built gppkg v2 for GPDB7"
fi
########################################################################################

########################################################################################
# Prepare to publish output
chown gpadmin:gpadmin *.gppkg
ls -l *.gppkg
mv *.gppkg gppkgs/
########################################################################################
