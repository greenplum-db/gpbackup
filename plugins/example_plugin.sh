#!/bin/bash
set -e

setup_plugin_for_backup(){
  mkdir -p /tmp/plugin_dest
}

setup_plugin_for_restore(){
  :
}

cleanup_plugin_for_backup(){
  :
}

cleanup_plugin_for_restore(){
  :
}

restore_file() {
  filename=`basename "$2"`
	cat /tmp/plugin_dest/$filename > $2
}

backup_file() {
  filename=`basename "$2"`
	cat $2 > /tmp/plugin_dest/$filename
}

backup_data() {
  filename=`basename "$2"`
	cat - > /tmp/plugin_dest/$filename
}

restore_data() {
  filename=`basename "$2"`
	cat /tmp/plugin_dest/$filename
}

plugin_api_version(){
  echo "0.2.0"
}

"$@"
