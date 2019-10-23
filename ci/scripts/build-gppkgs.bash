#!/bin/bash
set -ex

if [[ $OS == "RHEL" ]]; then
    gpbackup/ci/scripts/create-rhel-packages.bash
elif [[ $OS == "SLES" ]]; then
    gpbackup/ci/scripts/create-rhel-packages.bash
elif [[ $OS == "ubuntu" ]]; then
    gpbackup/ci/scripts/create-ubuntu-packages.bash
else
    exit 1
fi
