#!/bin/bash

set -ex

pushd gpbackup-go-components
  tar -xzf go_components.tar.gz
popd

pushd gpbackup_tar
  # Create install script
  printf "#!/bin/sh\nset -x\ntar -xzvf bin_gpbackup.tar.gz -C \$GPHOME" > install_gpdb_component
  chmod +x install_gpdb_component

  cp ../gpbackup-go-components/gpbackup_manager_version .
  cp ../gpbackup-go-components/s3_plugin_version .
  cp ../gpbackup-go-components/gpbackup_version .
  mv ../gpbackup-go-components/gpbackup_version version
  cp ../ddboost_components/ddboost_plugin_version .

  mkdir -p bin
  cp ../gpbackup-go-components/gpbackup bin/
  cp ../gpbackup-go-components/gpbackup_helper bin/
  cp ../gpbackup-go-components/gprestore bin/
  cp ../gpbackup-go-components/gpbackup_s3_plugin bin/
  cp ../gpbackup-go-components/gpbackup_manager bin/
  cp ../ddboost_components/gpbackup_ddboost_plugin bin/

  mkdir -p lib
  cp ../ddboost_components/libDDBoost.so lib/

  tar -czvf bin_gpbackup.tar.gz bin/ lib/

  version=`cat version`
  tar -czvf "gpbackup-${version}.tar.gz" bin_gpbackup.tar.gz install_gpdb_component gpbackup_version version s3_plugin_version ddboost_plugin_version gpbackup_manager_version
popd
