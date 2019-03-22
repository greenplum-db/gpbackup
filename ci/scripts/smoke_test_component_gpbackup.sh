#!/bin/bash
set -ex

package_version=`cat gpbackup_version`
if [[ `gpbackup --version` != "gpbackup version $package_version" ]]
then
  exit 1
fi
if [[ `gprestore --version` != "gprestore version $package_version" ]]
then
  exit 1
fi
if [[ `gpbackup_helper --version` != "gpbackup_helper version $package_version" ]]
then
  exit 1
fi
gpbackup_s3_plugin --version
gpbackup_ddboost_plugin --version

