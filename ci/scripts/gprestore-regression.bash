#!/bin/bash

set -ex

ccp_src/scripts/setup_ssh_to_cluster.sh
cp -r backup_artifact_resource/* /tmp
cp -r regression_dump/* /tmp

pushd /tmp
  tar -zxf gpbackup_all.tar.gz
  scp gpbackup_allsegments/gpbackup_cdw.tar.gz cdw:/tmp
  scp gpbackup_allsegments/gpbackup_sdw1.tar.gz sdw1:/tmp
popd
ssh -t sdw1 'pushd /tmp ; tar -xzf gpbackup_sdw1.tar.gz ; popd'
ssh -t cdw 'pushd /tmp ; tar -xzf gpbackup_cdw.tar.gz ; popd'

# restore the backedup data to a new cluster and generate a pg_dump.
# do not fail here because might be possible for gpbackup to fail but still produce the same dump diff
scp gpbackup/ci/scripts/gprestore_and_dump.bash cdw:/home/gpadmin/gprestore_and_dump.bash
set +e
  ssh -t cdw "bash /home/gpadmin/gprestore_and_dump.bash"
set -e

scp cdw:/tmp/post_regression_dump.sql.xz /tmp/

# Compare sqldump resource and the pg_dump that was newly generated

xz -d /tmp/regression_dump.sql.xz
xz -d /tmp/post_regression_dump.sql.xz
set +e
  diff -u /tmp/regression_dump.sql /tmp/post_regression_dump.sql > /tmp/diff.txt
set -e

# Because there are known, trivial differences between pg_dump and
# gpbackup, this diff will be non-zero, as described in
# README_regression.md.
# Therefore, we compare this diff with a frozen version,
# expecting no significant changes. First, however, we need to cut off
# headers/footers which may contain timestamps
FROZEN_DIFF=gpbackup/ci/regression/diff.txt
tail -n+4 /tmp/diff.txt | grep -v "@@ .* @@" > /tmp/diff_no_header.txt
tail -n+4 ${FROZEN_DIFF}  | grep -v "@@ .* @@" > /tmp/existing_diff_no_header.txt

# here is any real difference:
set +e
  diff /tmp/diff_no_header.txt /tmp/existing_diff_no_header.txt
  result=$?
set -e

if [[ ${result} -ne 0 ]] ; then
  echo "#####################################"
  echo "beginning of differences:"
  cat /tmp/diff.txt | head -200
  echo "\n...\n"
  echo "#####################################"
  exit 1
fi
