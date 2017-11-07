#!/bin/bash

set -ex

GPBACKUP_SRC=${GOPATH}/src/github.com/greenplum-db/gpbackup
pushd ${GPBACKUP_SRC}

GIT_VER=$(git describe --tags | awk -F "-" '{$2+=0; print $1 "." $2}')
DEV_VER=$(git diff | wc -l | awk '{if($1!=0) {print "+dev"}}')

make depend

sed -i '.bak' "s/^GIT_VERSION := .*/GIT_VERSION=${GIT_VER}/" Makefile
sed -i '' "s/^DEV_VERSION := .*/DEV_VERSION=${DEV_VER}/" Makefile

cp ./conan/conanfile.py $GOPATH
popd
pushd $GOPATH
sed -i '' "s/version = .*/version = '${GIT_VER}${DEV_VER}'/" conanfile.py

CONAN_VER=gpbackup/${GIT_VER}${DEV_VER}@gpdb/devel
CONAN_REPO_NAME=zzz_gpdb_oss
conan remove -f ${CONAN_VER}
conan export ${CONAN_VER}
conan install ${CONAN_VER} --build=missing
conan remote add ${CONAN_REPO_NAME} https://api.bintray.com/conan/greenplum-db/gpdb-oss
conan user -r ${CONAN_REPO_NAME} -p ${BINTRAY_TOKEN} ${BINTRAY_USER}
conan upload -r ${CONAN_REPO_NAME} ${CONAN_VER}
conan remote remove ${CONAN_REPO_NAME}
mv ${GPBACKUP_SRC}/Makefile.bak ${GPBACKUP_SRC}/Makefile
