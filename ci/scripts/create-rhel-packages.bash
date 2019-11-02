#!/bin/bash

set -ex

GPBACKUP_TOOLS_VERSION=`cat gpbackup-tools-versions/pkg_version`

############# Creates .rpm and gppkg  ##############
sudo yum -y install rpm-build

# Install gpdb binaries
if [[ ! -f bin_gpdb/bin_gpdb.tar.gz ]]; then
  mv bin_gpdb/{*.tar.gz,bin_gpdb.tar.gz}
fi
mkdir -p /usr/local/greenplum-db-devel
tar -xzf bin_gpdb/bin_gpdb.tar.gz -C /usr/local/greenplum-db-devel

# Setup gpadmin user
gpdb_src/concourse/scripts/setup_gpadmin_user.bash centos

cat <<EOF > gpadmin_cmds.sh
  #!/bin/sh
  set -ex

  OS=\$1
  # gpdb4 gppkgs must have 'orca' in its version because of the version validation done on the name
  GPDB_VER=( "4.3orca" "5" "6" "7")

  # Create RPM before sourcing greenplum path
  ./gpbackup/ci/scripts/gpbackup_tools_rpm.bash ${GPBACKUP_TOOLS_VERSION} gpbackup_tar/bin_gpbackup.tar.gz \$OS

  source /usr/local/greenplum-db-devel/greenplum_path.sh
  for i in "\${GPDB_VER[@]}"; do
    ./gpbackup/ci/scripts/gpbackup_gppkg.bash ${GPBACKUP_TOOLS_VERSION} \$i \$OS
  done
EOF

chown gpadmin:gpadmin .
chmod +x gpadmin_cmds.sh

su gpadmin -c "./gpadmin_cmds.sh $OS"

########### Prepare to publish output ###########

mv gpbackup_gppkg/* gppkgs/

