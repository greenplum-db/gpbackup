#!/bin/bash

set -ex

export TANZUNET_REFRESH_TOKEN=${TANZUNET_REFRESH_TOKEN}
mkdir workspace/files-to-upload

##############################
# Add binary tarballs to pivnet upload, excluding version strings
mkdir /tmp/rhel6
mkdir /tmp/rhel7
mkdir /tmp/rhel8
mkdir /tmp/rhel9
mkdir /tmp/ubuntu

cp release_components_rhel6/*.gz /tmp/rhel6
cp release_components_rhel7/*.gz /tmp/rhel7
cp release_components_rhel8/*.gz /tmp/rhel8
cp release_components_rhel9/*.gz /tmp/rhel9
cp release_components_ubuntu/*.gz /tmp/ubuntu

pushd /tmp
    mkdir untarred_rhel6
    mkdir untarred_rhel7
    mkdir untarred_rhel8
    mkdir untarred_rhel9
    mkdir untarred_ubuntu

    tar xzf rhel6/*.gz -C untarred_rhel6
    tar xzf rhel7/*.gz -C untarred_rhel7
    tar xzf rhel8/*.gz -C untarred_rhel8
    tar xzf rhel9/*.gz -C untarred_rhel9
    tar xzf ubuntu/*.gz -C untarred_ubuntu
popd

cp /tmp/untarred_rhel6/bin_gpbackup.tar.gz workspace/files-to-upload/bin_gpbackup-rhel6.tar.gz
cp /tmp/untarred_rhel7/bin_gpbackup.tar.gz workspace/files-to-upload/bin_gpbackup-rhel7.tar.gz
cp /tmp/untarred_rhel8/bin_gpbackup.tar.gz workspace/files-to-upload/bin_gpbackup-rhel8.tar.gz
cp /tmp/untarred_rhel9/bin_gpbackup.tar.gz workspace/files-to-upload/bin_gpbackup-rhel9.tar.gz
cp /tmp/untarred_ubuntu/bin_gpbackup.tar.gz workspace/files-to-upload/bin_gpbackup-ubuntu.tar.gz
##############################

##############################
cp gpbackup/ci/pivnet_release/metadata.yml workspace/
tar xzf gppkgs/gpbackup-gppkgs.tar.gz -C workspace/files-to-upload/
GPBACKUP_VERSION=$(cat workspace/files-to-upload/gpbackup_version)

# the same gpbackup version will exist in the tile version string
# if this release only includes updates to components outside of
# gpbackup itself (e.g. plugins, manager)
pushd pivnet_release_cache
  PRV_TILE_VERSION=$(echo v-* | tr '+' '-' | cut -d- -f1-2)
  CURR_TILE_VERSION=$(echo "v-${GPBACKUP_VERSION}" | tr '+' '-' | cut -d- -f1-2)
  if [[ -f ${CURR_TILE_VERSION} ]]; then
      echo "Release already exists on Pivnet"
      exit 1
  else
    # detect the release type
    PRV_MAJOR=$(echo ${PRV_TILE_VERSION:2} | cut -d. -f1)
    PRV_MINOR=$(echo ${PRV_TILE_VERSION:2} | cut -d. -f2)
    PRV_PATCH=$(echo ${PRV_TILE_VERSION:2} | cut -d. -f3 | sed -r "s/([0-9]+).*/\1/")
    CURR_MAJOR=$(echo ${CURR_TILE_VERSION:2} | cut -d. -f1)
    CURR_MINOR=$(echo ${CURR_TILE_VERSION:2} | cut -d. -f2)
    CURR_PATCH=$(echo ${CURR_TILE_VERSION:2} | cut -d. -f3 | sed -r "s/([0-9]+).*/\1/")
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

  TILE_RELEASE_VERSION=${CURR_TILE_VERSION:2}
  touch ../workspace/v-${TILE_RELEASE_VERSION}
popd

if test ! -f gpbackup-release-license/open_source_license_VMware_Greenplum_Backup_and_Restore_${CURR_MAJOR}.${CURR_MINOR}.*.txt ; then
  echo "License file for gpbackup version ${CURR_MAJOR}.${CURR_MINOR}.* does not exist in resource.\n Ensure the OSL is properly uploaded to the GCS bucket prior to pushing to pivnet." 1>&2
  exit 1
fi
cp gpbackup-release-license/open_source_license_VMware_Greenplum_Backup_and_Restore_*.txt workspace/files-to-upload/

# NOTE: We must use the Pivnet Release Version because we cannot upload files with the same name in different tile releases
DDBOOST_PLUGIN_VERSION=$(cat workspace/files-to-upload/ddboost_plugin_version)
sed -i "s/<DDBOOST_PLUGIN_VERSION>/${DDBOOST_PLUGIN_VERSION}/g" workspace/metadata.yml
S3_PLUGIN_VERSION=$(cat workspace/files-to-upload/s3_plugin_version)
sed -i "s/<S3_PLUGIN_VERSION>/${S3_PLUGIN_VERSION}/g" workspace/metadata.yml
BMAN_VERSION=$(cat workspace/files-to-upload/gpbackup_manager_version)
sed -i "s/<BMAN_VERSION>/${BMAN_VERSION}/g" workspace/metadata.yml
sed -i "s/<TILE_RELEASE_VERSION>/${TILE_RELEASE_VERSION}/g" workspace/metadata.yml
sed -i "s/<GPBAR_VERSION>/${TILE_RELEASE_VERSION}/g" workspace/metadata.yml
OSL_FILENAME=$(basename -- gpbackup-release-license/open_source_license_VMware_Greenplum_Backup_and_Restore_*.txt)
sed -i "s/<OSL_FILENAME>/${OSL_FILENAME}/g" workspace/metadata.yml
sed -i "s/<RELEASE_TYPE>/${RELEASE_TYPE} Release/g" workspace/metadata.yml

# Calculate end of support date (last day of the month 18 months from now)
future_date_18_month=$(date -d "+19 month" +%Y-%m-01)
END_OF_SUPPORT_DATE=$(date -d "$future_date_18_month - 1 day" +%Y-%m-%d)
sed -i "s/<END_OF_SUPPORT_DATE>/${END_OF_SUPPORT_DATE}/g" workspace/metadata.yml

# The URL adjusts based on major/minor version
REL_NOTES_VERSION=$(echo ${TILE_RELEASE_VERSION} | cut -d. -f1,2)
sed -i "s/<REL_NOTES_VERSION>/${REL_NOTES_VERSION}/g" workspace/metadata.yml

cat workspace/metadata.yml
pushd workspace/files-to-upload
  # rename files to match the name inside metadata.yml
  for filename in *.gppkg; do
    newFilename=$(sed -e "s/gpbackup_tools-[+0-9a-z.]*/greenplum_backup_restore-${TILE_RELEASE_VERSION}/" -e "s/RHEL/rhel/" -e "s/SLES/sles/" <<< "$filename")
    if ! [ "$filename" == "$newFilename" ]; then
      mv "$filename" "$newFilename"
    fi
  done

  # rename binary tarballs, to match the name inside metadata.yml
  for filename in *.tar.gz; do
      newFilename="${filename/bin_gpbackup/greenplum_backup_restore-${TILE_RELEASE_VERSION}}"
      if ! [ "$filename" == "$newFilename" ]; then
          mv "$filename" "$newFilename"
      fi
  done
popd

echo ${TILE_RELEASE_VERSION} > workspace/tile_release_version
rm workspace/files-to-upload/*_version
ls -l workspace/files-to-upload

push_to_tanzunet() {
	set -ex
	chmod a+x tanzunet_client/gp-tanzunet-client
	tanzunet_client/gp-tanzunet-client upload -g "${TILE_RELEASE_VERSION}"\
                                       -m "workspace/metadata.yml" -s "workspace/files-to-upload"\
                                       --tanzunet-endpoint "https://network.tanzu.vmware.com" --tanzunet-slug "greenplum-backup-restore"\
                                       --debug
}

if [ "${BASH_SOURCE[0]}" == "${0}" ]; then
	push_to_tanzunet
fi
