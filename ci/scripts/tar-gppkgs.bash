#!/bin/bash

set -ex

cp gpbackup-tools-versions/* gppkgs/
mv rhel-gppkg/* gppkgs/
if [[ -d sles-gppkg ]]; then
    mv sles-gppkg/* gppkgs/
fi
if [[ -d ubuntu-gppkg ]]; then
    mv ubuntu-gppkg/* gppkgs/
fi

pushd gppkgs
    tar cvzf gpbackup-gppkgs.tar.gz *
popd
