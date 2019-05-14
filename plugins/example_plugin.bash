#!/bin/bash
set -e

setup_plugin_for_backup(){
  echo "setup_plugin_for_backup $1 $2 $3 $4" >> /tmp/plugin_out.txt
  if [ "$3" = "master" ]
    then echo "setup_plugin_for_backup was called for scope = master" >> /tmp/plugin_out.txt
  elif [ "$3" = "segment_host" ]
    then echo "setup_plugin_for_backup was called for scope = segment_host" >> /tmp/plugin_out.txt
  elif [ "$3" = "segment" ]
    then echo "setup_plugin_for_backup was called for scope = segment" >> /tmp/plugin_out.txt
  fi
  timestamp_dir=`basename "$2"`
  mkdir -p /tmp/plugin_dest/$timestamp_dir
}

setup_plugin_for_restore(){
  echo "setup_plugin_for_restore $1 $2 $3 $4" >> /tmp/plugin_out.txt
  if [ "$3" = "master" ]
    then echo "setup_plugin_for_restore was called for scope = master" >> /tmp/plugin_out.txt
  elif [ "$3" = "segment_host" ]
    then echo "setup_plugin_for_restore was called for scope = segment_host" >> /tmp/plugin_out.txt
  elif [ "$3" = "segment" ]
    then echo "setup_plugin_for_restore was called for scope = segment" >> /tmp/plugin_out.txt
  fi
}

cleanup_plugin_for_backup(){
  echo "cleanup_plugin_for_backup $1 $2 $3 $4" >> /tmp/plugin_out.txt
  if [ "$3" = "master" ]
    then echo "cleanup_plugin_for_backup was called for scope = master" >> /tmp/plugin_out.txt
  elif [ "$3" = "segment_host" ]
    then echo "cleanup_plugin_for_backup was called for scope = segment_host" >> /tmp/plugin_out.txt
  elif [ "$3" = "segment" ]
    then echo "cleanup_plugin_for_backup was called for scope = segment" >> /tmp/plugin_out.txt
  fi
}

cleanup_plugin_for_restore(){
  echo "cleanup_plugin_for_restore $1 $2 $3 $4" >> /tmp/plugin_out.txt
  if [ "$3" = "master" ]
    then echo "cleanup_plugin_for_restore was called for scope = master" >> /tmp/plugin_out.txt
  elif [ "$3" = "segment_host" ]
    then echo "cleanup_plugin_for_restore was called for scope = segment_host" >> /tmp/plugin_out.txt
  elif [ "$3" = "segment" ]
    then echo "cleanup_plugin_for_restore was called for scope = segment" >> /tmp/plugin_out.txt
  fi
}

restore_file() {
  echo "restore_file $1 $2" >> /tmp/plugin_out.txt
  filename=`basename "$2"`
  timestamp_dir=`basename $(dirname "$2")`
	cat /tmp/plugin_dest/$timestamp_dir/$filename > $2
}

backup_file() {
  echo "backup_file $1 $2" >> /tmp/plugin_out.txt
  filename=`basename "$2"`
  timestamp_dir=`basename $(dirname "$2")`
	cat $2 > /tmp/plugin_dest/$timestamp_dir/$filename
}

backup_data() {
  echo "backup_data $1 $2" >> /tmp/plugin_out.txt
  filename=`basename "$2"`
  timestamp_dir=`basename $(dirname "$2")`
	cat - > /tmp/plugin_dest/$timestamp_dir/$filename
}

restore_data() {
  echo "restore_data $1 $2" >> /tmp/plugin_out.txt
  filename=`basename "$2"`
  timestamp_dir=`basename $(dirname "$2")`
	cat /tmp/plugin_dest/$timestamp_dir/$filename
}

delete_backup() {
  echo "delete_backup $1 $2" >> /tmp/plugin_out.txt
  rm -rf /tmp/plugin_dest/$2

}

plugin_api_version(){
  echo "0.4.0"
  echo "0.4.0" >> /tmp/plugin_out.txt
}

"$@"
