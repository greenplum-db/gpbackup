#!/bin/bash

set -ex

cp gpbackup-tools-versions/* gppkgs/
mv rhel6-gppkg/* gppkgs/
mv rhel7-gppkg/* gppkgs/
mv rhel8-gppkg/* gppkgs/
mv rhel9-gppkg/* gppkgs/
if [[ -d ubuntu-gppkg ]]; then
    mv ubuntu-gppkg/* gppkgs/
fi

pushd gppkgs
    tar cvzf gpbackup-gppkgs.tar.gz *
popd
