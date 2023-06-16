#!/bin/bash

set -ex

ccp_src/scripts/setup_ssh_to_cluster.sh

# Install ddboost dependencies
scp -r gpbackup_ddboost_plugin cdw:/home/gpadmin/gpbackup_ddboost_plugin
ssh -t ${default_ami_user}@cdw "sudo yum install -y autoconf automake libtool"

if test -f pgcrypto43/pgcrypto*; then
  scp -r pgcrypto43/pgcrypto*.gppkg cdw:.
  ssh -t gpadmin@cdw "source env.sh; \
    gppkg -i pgcrypto*.gppkg && \
    psql -d postgres -f \${GPHOME}/share/postgresql/contrib/pgcrypto.sql"
else
  ssh -t gpadmin@cdw "source env.sh; \
    psql -d postgres -c 'CREATE EXTENSION pgcrypto;'"
fi

DDBOOSTFS_RPM=DDBoostFS-1.1.0.1-565598.rhel.x86_64.rpm
cat > /tmp/script.sh << SCRIPT
  #!/bin/bash

  set -ex

  cd /tmp
  sudo yum -y install ${DDBOOSTFS_RPM} expect

  expect << EOD
spawn /opt/emc/boostfs/bin/boostfs lockbox set -d ${DD_SOURCE_HOST} -s gpdb_boostfs -u ${DD_USER}
expect "password"
send "${DD_PW}\n"
expect "password"
send "${DD_PW}\n"
expect eof
EOD

  sudo mkdir /data/gpdata/dd_dir
  sudo chown -R gpadmin:gpadmin /data/gpdata/dd_dir
  sudo /opt/emc/boostfs/bin/boostfs mount /data/gpdata/dd_dir -d ${DD_SOURCE_HOST} -s gpdb_boostfs -o allow-others=true
SCRIPT
chmod +x /tmp/script.sh

hostnames=$(cat ./cluster_env_files/etc_hostfile | awk '{print $2}')
for host in ${hostnames}; do
  echo "Installing boostfs on $host"
  scp /tmp/script.sh boostfs_installer/${DDBOOSTFS_RPM} ${default_ami_user}@${host}:/tmp
  ssh ${default_ami_user}@${host} "/tmp/script.sh"
done
