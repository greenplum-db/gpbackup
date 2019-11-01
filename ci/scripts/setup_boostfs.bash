#!/bin/bash

cat > /tmp/script.sh << SCRIPT
  #!/bin/bash

  set -ex

  cd /tmp
  sudo yum -y install DDBoostFS-1.1.0.1-565598.rhel.x86_64.rpm
  sudo yum -y install expect

  expect << EOD
spawn /opt/emc/boostfs/bin/boostfs lockbox set -d ${DD_IP} -s gpdb_boostfs -u ${DD_USER}
expect "password"
send "${DD_PASSWORD}\n"
expect "password"
send "${DD_PASSWORD}\n"
expect eof
EOD

  sudo mkdir /data/gpdata/dd_dir
  sudo /opt/emc/boostfs/bin/boostfs mount /data/gpdata/dd_dir -d ${DD_IP} -s gpdb_boostfs -o allow-others=true
SCRIPT
chmod +x /tmp/script.sh

hostnames=`cat ./cluster_env_files/etc_hostfile | awk '{print $2}'`
for host in ${hostnames}; do
  echo "Installing boostfs on $host"
  scp /tmp/script.sh centos@${host}:/tmp
  scp boostfs_installer/DDBoostFS-1.1.0.1-565598.rhel.x86_64.rpm centos@${host}:/tmp
  ssh centos@${host} "/tmp/script.sh"
done
