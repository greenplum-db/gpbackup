#!/bin/bash

set -ex

# setup cluster and install gpbackup tools using gppkg
ccp_src/scripts/setup_ssh_to_cluster.sh
out=$(ssh -t cdw 'source env.sh && psql postgres -c "select version();"')
GPDB_VERSION=$(echo ${out} | sed -n 's/.*Greenplum Database \([0-9]\).*/\1/p')
mkdir -p /tmp/untarred
tar -xzf gppkgs/gpbackup-gppkgs.tar.gz -C /tmp/untarred
scp /tmp/untarred/gpbackup_tools*gp${GPDB_VERSION}*${OS}*.gppkg cdw:/home/gpadmin
ssh -t cdw "source env.sh; gppkg -i gpbackup_tools*.gppkg"

cat <<SCRIPT > /tmp/setup_minio.bash
# Install Minio
wget https://dl.min.io/server/minio/release/linux-amd64/minio
sudo mv minio /usr/local/bin/
sudo chmod 777 /usr/local/bin/minio
sudo chown rocky:rocky /usr/local/bin/minio

# Setup Minio server
mkdir -p /tmp/minio_root
mkdir -p /tmp/.minio/logs
touch /tmp/.minio/logs/minio_log
chmod 777 /tmp/minio_root
chmod 777 /tmp/.minio/logs
chown -R rocky:rocky /tmp/minio_root
chown -R rocky:rocky /tmp/.minio
nohup minio server --console-address ":9001" /tmp/minio_root > /tmp/.minio/logs/minio_log 2>&1 &

curl https://dl.min.io/client/mc/release/linux-amd64/mc \
  --create-dirs \
  -o \$HOME/minio-binaries/mc

chmod +x \$HOME/minio-binaries/mc
export PATH=\$PATH:\$HOME/minio-binaries/
which mc

#create test bucket
mc mb /tmp/minio_root/minio-test-bucket

SCRIPT

chmod +x /tmp/setup_minio.bash
scp /tmp/setup_minio.bash rocky@cdw:/home/rocky/setup_minio.bash
ssh -t rocky@cdw "/home/rocky/setup_minio.bash"

ssh -t rocky@cdw "ps aux | grep minio"

cat <<SCRIPT > /tmp/minio_tests.bash
  #!/bin/bash

  set -ex
  source env.sh

  # config to test against s3-compliant storage minio
  cat << MINIO_CONFIG > \${HOME}/minio_config.yaml
executablepath: \${GPHOME}/bin/gpbackup_s3_plugin
options:
  endpoint: http://\$(hostname -I | awk '{print \$1}'):9000/
  aws_access_key_id: minioadmin
  aws_secret_access_key: minioadmin
  bucket: minio-test-bucket
  folder: minio-folder/test-cluster
  backup_max_concurrent_requests: 2
  backup_multipart_chunksize: 5MB
  restore_max_concurrent_requests: 2
  restore_multipart_chunksize: 5MB
MINIO_CONFIG

  pushd ~/go/src/github.com/greenplum-db/gpbackup/plugins
    ./plugin_test.sh \${GPHOME}/bin/gpbackup_s3_plugin \${HOME}/minio_config.yaml
  popd
SCRIPT

chmod +x /tmp/minio_tests.bash
scp /tmp/minio_tests.bash cdw:/home/gpadmin/minio_tests.bash
ssh -t cdw "/home/gpadmin/minio_tests.bash"

cat <<SCRIPT > /tmp/s3_tests.bash
  #!/bin/bash

  set -ex
  source env.sh

  cat << S3_CONFIG > \${HOME}/s3_config.yaml
executablepath: \${GPHOME}/bin/gpbackup_s3_plugin
options:
  region: ${REGION}
  aws_access_key_id: ${AWS_ACCESS_KEY_ID}
  aws_secret_access_key: ${AWS_SECRET_ACCESS_KEY}
  bucket: ${BUCKET}
  folder: test/backup
  backup_multipart_chunksize: 100MB
  restore_multipart_chunksize: 100MB
S3_CONFIG

  pushd ~/go/src/github.com/greenplum-db/gpbackup/plugins
    ./plugin_test.sh \${GPHOME}/bin/gpbackup_s3_plugin \${HOME}/s3_config.yaml
  popd
SCRIPT

chmod +x /tmp/s3_tests.bash
scp /tmp/s3_tests.bash cdw:/home/gpadmin/s3_tests.bash
ssh -t cdw "/home/gpadmin/s3_tests.bash"
