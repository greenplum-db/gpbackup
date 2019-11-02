#!/bin/bash

set -ex

mkdir workspace/files-to-upload

# Add binary tarball to pivnet upload, excluding version strings
cp github_release_components_rc/*.gz /tmp/
pushd /tmp
  mkdir untarred
  tar xzf *.gz -C untarred
popd
cp /tmp/untarred/bin_gpbackup.tar.gz workspace/files-to-upload/

cp gpbackup/ci/pivnet_release/metadata.yml workspace/
tar xzf gppkgs/gpbackup-gppkgs.tar.gz -C workspace/files-to-upload/
GPBACKUP_VERSION=$(cat workspace/files-to-upload/gpbackup_version)

# the same gpbackup version will exist in the tile version string
# if this release only includes updates to components outside of
# gpbackup itself (e.g. plugins, manager)
pushd pivnet_release_cache
  PRV_TILE_VERSION=$(echo v-*)
  CURR_TILE_VERSION="v-${GPBACKUP_VERSION}*"
  if [[ -f ${CURR_TILE_VERSION} ]]; then
    COUNT=$(echo ${CURR_TILE_VERSION} | sed -n "s/v-${GPBACKUP_VERSION}-\([0-9]*\).*/\1/p")
    COUNT=$(($COUNT+1))
    # If same gpbackup version, then not a major/minor release
    RELEASE_TYPE="Maintenance"
  else
    # reset the version count
    COUNT=1
    # detect the release type
    PRV_MAJOR=$(echo ${PRV_TILE_VERSION:2} |cut -d. -f1)
    PRV_MINOR=$(echo ${PRV_TILE_VERSION:2} |cut -d. -f2)
    PRV_PATCH=$(echo ${PRV_TILE_VERSION:2} |cut -d. -f3 | sed -r "s/([0-9]+).*/\1/")
    CURR_MAJOR=$(echo ${GPBACKUP_VERSION} |cut -d. -f1)
    CURR_MINOR=$(echo ${GPBACKUP_VERSION} |cut -d. -f2)
    CURR_PATCH=$(echo ${GPBACKUP_VERSION} |cut -d. -f3)
    if [[ "$PRV_MAJOR" != "$CURR_MAJOR" ]] ; then
      RELEASE_TYPE="Major"
    elif [[ "$PRV_MINOR" != "$CURR_MINOR" ]] ; then
      RELEASE_TYPE="Minor"
    elif [[ "$PRV_PATCH" != "$CURR_PATCH" ]] ; then
      RELEASE_TYPE="Maintenance"
    else
      echo "Unable to determine release type."
      exit 1
    fi
  fi

  TILE_RELEASE_VERSION=${GPBACKUP_VERSION}-${COUNT}
  touch ../workspace/v-${TILE_RELEASE_VERSION}
popd

if [[ ! -f gpbackup-release-license/open_source_license_pivotal-gpdb-backup-${GPBACKUP_VERSION}-*.txt ]] ; then
  echo "License file for gpbackup version ${GPBACKUP_VERSION} does not exist in resource.\n Ensure the OSL is properly uploaded to the GCS bucket prior to pushing to pivnet." 1>&2
  exit 1
fi
cp gpbackup-release-license/open_source_license_pivotal-gpdb-backup-${GPBACKUP_VERSION}-*.txt workspace/files-to-upload/

# NOTE: We must use the Pivnet Release Version (including the counter) because we cannot upload files with the same name in different tile releases
DDBOOST_PLUGIN_VERSION=$(cat workspace/files-to-upload/ddboost_plugin_version)
sed -i "s/<DDBOOST_PLUGIN_VERSION>/${DDBOOST_PLUGIN_VERSION}/g" workspace/metadata.yml
S3_PLUGIN_VERSION=$(cat workspace/files-to-upload/s3_plugin_version)
sed -i "s/<S3_PLUGIN_VERSION>/${S3_PLUGIN_VERSION}/g" workspace/metadata.yml
BMAN_VERSION=$(cat workspace/files-to-upload/gpbackup_manager_version)
sed -i "s/<BMAN_VERSION>/${BMAN_VERSION}/g" workspace/metadata.yml
sed -i "s/<TILE_RELEASE_VERSION>/${TILE_RELEASE_VERSION}/g" workspace/metadata.yml
sed -i "s/<GPBAR_VERSION>/${GPBACKUP_VERSION}/g" workspace/metadata.yml
OSL_FILENAME=$(basename -- gpbackup-release-license/open_source_license_pivotal-gpdb-backup-${GPBACKUP_VERSION}-*.txt)
sed -i "s/<OSL_FILENAME>/${OSL_FILENAME}/g" workspace/metadata.yml
sed -i "s/<RELEASE_TYPE>/${RELEASE_TYPE} Release/g" workspace/metadata.yml

# The URL adjusts based on major/minor version
REL_NOTES_VERSION=$(echo ${GPBACKUP_VERSION//./-} | cut -d- -f1,2)
sed -i "s/<REL_NOTES_VERSION>/${REL_NOTES_VERSION}/g" workspace/metadata.yml

cat workspace/metadata.yml
pushd workspace/files-to-upload
  # rename files to match the name inside metadata.yml
  for filename in *.gppkg; do
    newFilename=$(sed -e "s/gpbackup_tools-${GPBACKUP_VERSION}+[0-9]*/pivotal_greenplum_backup_restore-${TILE_RELEASE_VERSION}/" -e "s/RHEL/rhel/" -e "s/SLES/sles/" <<< "$filename")
    mv "$filename" "$newFilename"
  done
  # rename binary tarball
  mv "bin_gpbackup.tar.gz" "pivotal_greenplum_backup_restore-${TILE_RELEASE_VERSION}.tar.gz"
popd

echo ${TILE_RELEASE_VERSION} > workspace/tile_release_version
rm workspace/files-to-upload/*_version
# We must remove unnecessary GP7 artifacts from the pivnet upload directory
rm workspace/files-to-upload/*-gp7-*
ls -l workspace/files-to-upload
