  #!/bin/bash
set -e

setup_plugin_for_backup(){
  echo "setup_plugin_for_backup $1 $2 $3" >> /tmp/plugin_out.txt
  mkdir -p /tmp/plugin_dest
}

setup_plugin_for_restore(){
  echo "setup_plugin_for_restore $1 $2 $3" >> /tmp/plugin_out.txt
}

cleanup_plugin_for_backup(){
  echo "cleanup_plugin_for_backup $1 $2 $3" >> /tmp/plugin_out.txt
}

cleanup_plugin_for_restore(){
  echo "cleanup_plugin_for_restore $1 $2 $3" >> /tmp/plugin_out.txt
}

restore_file() {
  echo "restore_file $1 $2" >> /tmp/plugin_out.txt
  filename=`basename "$2"`
	cat /tmp/plugin_dest/$filename > $2
}

backup_file() {
  echo "backup_file $1 $2" >> /tmp/plugin_out.txt
  filename=`basename "$2"`
	cat $2 > /tmp/plugin_dest/$filename
}

backup_data() {
  echo "backup_data $1 $2" >> /tmp/plugin_out.txt
  filename=`basename "$2"`
	cat - > /tmp/plugin_dest/$filename
}

restore_data() {
  echo "restore_data $1 $2" >> /tmp/plugin_out.txt
  filename=`basename "$2"`
	cat /tmp/plugin_dest/$filename
}

plugin_api_version(){
  echo "0.2.0"
  echo "0.2.0" >> /tmp/plugin_out.txt
}

"$@"
