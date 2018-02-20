#!/bin/bash

set -ex

GO_DIR=$(echo $GOPATH | cut -d : -f 1)
GPBACKUP_SRC=$GO_DIR/src/github.com/greenplum-db/gpbackup
pushd ${GPBACKUP_SRC}

GIT_VER=$(git describe --tags | perl -pe 's/(.*)-([0-9]*)-(g[0-9a-f]*)/\1+dev.\2.\3/')

make depend

sed -i '.bak' "s/^GIT_VERSION := .*/GIT_VERSION=${GIT_VER}/" Makefile

cp ./conan/conanfile.py $GO_DIR
popd
pushd $GO_DIR
sed -i '' "s/version = .*/version = '${GIT_VER}'/" conanfile.py

CONAN_VER=gpbackup/${GIT_VER}@gpdb/devel
CONAN_REPO_NAME=zzz_gpdb_oss
conan remove -f ${CONAN_VER}
conan export ${GPBACKUP_SRC}/conan ${CONAN_VER}
conan install ${CONAN_VER} --build=missing
conan remote add ${CONAN_REPO_NAME} https://api.bintray.com/conan/greenplum-db/gpdb-oss
conan user -r ${CONAN_REPO_NAME} -p ${BINTRAY_TOKEN} ${BINTRAY_USER}
conan upload -r ${CONAN_REPO_NAME} ${CONAN_VER}
conan remote remove ${CONAN_REPO_NAME}
mv ${GPBACKUP_SRC}/Makefile.bak ${GPBACKUP_SRC}/Makefile
