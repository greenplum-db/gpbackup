#!/bin/bash

set -ex

mv rhel-gppkg/* gppkgs/
mv sles-gppkg/* gppkgs/
mv ubuntu-gppkg/* gppkgs/

pushd gppkgs
    tar cvzf gpbackup-gppkgs.tar.gz *
popd


