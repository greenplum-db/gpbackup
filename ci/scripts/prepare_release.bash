#!/bin/bash

set -ex

# untar components, and then nested tarball of binaries
tar xvzf release_components_rhel7/*.gz -C components_untarred_rhel7
tar xvzf release_components_rhel8/*.gz -C components_untarred_rhel8
tar xvzf release_components_rhel9/*.gz -C components_untarred_rhel9
tar xvzf release_components_ubuntu/*.gz -C components_untarred_ubuntu

tar xvzf components_untarred_rhel7/bin_gpbackup.tar.gz -C components_untarred_rhel7
tar xvzf components_untarred_rhel8/bin_gpbackup.tar.gz -C components_untarred_rhel8
tar xvzf components_untarred_rhel9/bin_gpbackup.tar.gz -C components_untarred_rhel9
tar xvzf components_untarred_ubuntu/bin_gpbackup.tar.gz -C components_untarred_ubuntu

# make directories for unpacking and for final delivery
mkdir components_untarred_rhel7/components
mkdir components_untarred_rhel8/components
mkdir components_untarred_rhel9/components
mkdir components_untarred_ubuntu/components

mkdir components_untarred_rhel7/output
mkdir components_untarred_rhel8/output
mkdir components_untarred_rhel9/output
mkdir components_untarred_ubuntu/output

## Move binaries to final destination and tar up for publishing
# RHEL7
cp components_untarred_rhel7/bin/gpbackup components_untarred_rhel7/components
cp components_untarred_rhel7/bin/gprestore components_untarred_rhel7/components
cp components_untarred_rhel7/bin/gpbackup_helper components_untarred_rhel7/components
tar -czvf components_untarred_rhel7/output/gpbackup_binaries_rhel7.tar.gz -C components_untarred_rhel7/components .

# RHEL8
cp components_untarred_rhel8/bin/gpbackup components_untarred_rhel8/components
cp components_untarred_rhel8/bin/gprestore components_untarred_rhel8/components
cp components_untarred_rhel8/bin/gpbackup_helper components_untarred_rhel8/components
tar -czvf components_untarred_rhel8/output/gpbackup_binaries_rhel8.tar.gz -C components_untarred_rhel8/components .

# RHEL9
cp components_untarred_rhel9/bin/gpbackup components_untarred_rhel9/components
cp components_untarred_rhel9/bin/gprestore components_untarred_rhel9/components
cp components_untarred_rhel9/bin/gpbackup_helper components_untarred_rhel9/components
tar -czvf components_untarred_rhel9/output/gpbackup_binaries_rhel9.tar.gz -C components_untarred_rhel9/components .

# UBUNTU
cp components_untarred_ubuntu/bin/gpbackup components_untarred_ubuntu/components
cp components_untarred_ubuntu/bin/gprestore components_untarred_ubuntu/components
cp components_untarred_ubuntu/bin/gpbackup_helper components_untarred_ubuntu/components
tar -czvf components_untarred_ubuntu/output/gpbackup_binaries_ubuntu.tar.gz -C components_untarred_ubuntu/components .
