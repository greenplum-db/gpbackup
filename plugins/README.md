# Plugins in gpbackup

gpbackup and gprestore support backing up to and restoring from remote storage locations (e.g.: s3) using a plugin architecture

## Using plugins
The plugin executable must exist on all segments at the same path.

Backing up with a plugin:
```
gpbackup ... --plugin-config <Absolute path to config file>
```

Restoring with a plugin:
```
gprestore ... --plugin-config <Absolute path to config file>
```
The backup you are restoring must have been taken with the same plugin.

## Plugin configuration file format
The plugin configuration must be specified in a yaml file. This yaml file is only required to exist on the coordinator host, and is automatically copied to segment hosts.

The _executablepath_ is a required parameter and must point to the absolute path of the executable on each host. Additional parameters may be specified under the _options_ key as required by the specific plugin. Refer to the documentation for the plugin you are using for additional required paramters. The _options_ section will include "pgport" for one of the segments on a given host, in case the plugin requires usage of a postgres function. Upon a restore, the _options_ section may also contain "backup_plugin_version" if the information is available from historical records.  With this historical version, a newer plugin could possibly support backwards compatibility toward backups created with older versions of plugins.

```
executablepath: <Absolute path to plugin executable>
options:
  my_first_option: <value1>
  my_second_option: <value2>
  pgport: 5432
  backup_plugin_version: 1.3
  <Additional options for the specific plugin>
```

## Available plugins
[gpbackup_s3_plugin](https://github.com/greenplum-db/gpbackup-s3-plugin): Allows users to back up their Greenplum Database to Amazon S3.

## Developing plugins

Plugins can be written in any language as long as they can be called as an executable and adhere to the gpbackup plugin API.

gpbackup and gprestore will call the plugin executable in the format
```
[plugin_executable_name] [command] arg1 arg2
```

If an error occurs during plugin execution, plugins should write an error message to stderr and return a non-zero error code.



## Commands

The current version of our utility calls all commands listed below. Errors will occur if any of them are not defined. If your plugin does not require the functionality of one of these commands, leave the implementation empty.

See [Release Notes](#Release_Notes) for command modification history.

[setup_plugin_for_backup](#setup_plugin_for_backup)

[setup_plugin_for_restore](#setup_plugin_for_restore)

[cleanup_plugin_for_backup](#cleanup_plugin_for_backup)

[cleanup_plugin_for_restore](#cleanup_plugin_for_restore)

[backup_file](#backup_file)

[restore_file](#restore_file)

[backup_data](#backup_data)

[restore_data](#restore_data)

[plugin_api_version](#plugin_api_version)

[delete_backup](#delete_backup)

[--version](#--version)

## Command Arguments

These arguments are passed to the plugin by gpbackup/gprestore.

[config_path](#config_path): Absolute path to the config yaml file

[local_backup_directory](#local_backup_directory): The path to the directory where gpbackup would place backup files on the coordinator host if not using a plugin. Our plugins reference this path to recreate a similar directory structure on the destination system. gprestore will read files from this location so the plugin will need to create the directory during setup if it does not already exist.

[scope](#scope): The scope at which this plugin's setup/cleanup hook is invoked. Values for this parameter are "coordinator", "segment_host" and "segment" (with "master" being a supported synonym for "coordinator" for backwards compatibility). Each such hook is invoked at each of these scopes. For eg. If we have a cluster with a coordinator on 1 coordinator host and 2 segment hosts each with 4 segments, each of these hooks will be executed in the following manner: There will be 1 invocation
of each method with the parameter "coordinator", offering a chance to perform some setup/cleanup to be done *once* per cluster. Creation/Deletion of a remote directory is a perfect candidate here. Furthermore, there will be 1 invocation for each of these commands for each of the segment hosts, offering a chance to establish/teardown connectivity to a remote storage provider such as S3 for instance. Finally, there will be 1 invocation for each of these commands for each of the segments.

Note: "segment_host" and "segment" are both provided as a single physical segment host may house multiple segment processes in Greenplum. There maybe some setup or cleanup required at the segment host level as compared to each segment process.

[contentID](#contentID): The contentID corresponding to the scope. This is passed in only for the "coordinator" and "segment" scopes.

[filepath](#filepath): The local path to a file written by gpbackup and/or read by gprestore.

[data_filekey](#data_filekey): The path where a data file would be written on local disk if not using a plugin. The plugin should use the filename specified in this argument when storing the streamed data on the remote system because the same path will be used as a key to the restore_data command to retrieve the data.

[timestamp](#timestamp): The timestamp key for a particular backup.

## Command API

### [setup_plugin_for_backup](#setup_plugin_for_backup)

Steps necessary to initialize plugin before backup begins. E.g. Creating remote directories, validating connectivity, disk checks, etc.

**Usage within gpbackup:**

Called at the start of the backup process on the coordinator and each segment host.

**Arguments:**

[config_path](#config_path)

[local_backup_directory](#local_backup_directory)

[scope](#scope)

[contentID](#contentID)

**Stdout:** None

**Example:**
```
test_plugin setup_plugin_for_backup /home/test_plugin_config.yaml /data_dir-1/backups/20180101/20180101010101 coordinator -1
test_plugin setup_plugin_for_backup /home/test_plugin_config.yaml /data_dir0/backups/20180101/20180101010101 segment_host
test_plugin setup_plugin_for_backup /home/test_plugin_config.yaml /data_dir0/backups/20180101/20180101010101 segment 0
test_plugin setup_plugin_for_backup /home/test_plugin_config.yaml /data_dir1/backups/20180101/20180101010101 segment 1
```

### [setup_plugin_for_restore](#setup_plugin_for_restore)

Steps necessary to initialize plugin before restore begins. E.g. validating connectivity

**Usage within gprestore:**

Called at the start of the restore process on the coordinator and each segment host.

**Arguments:**

[config_path](#config_path)

[local_backup_directory](#local_backup_directory)

[scope](#scope)

[contentID](#contentID)

**Stdout:** None

**Example:**
```
test_plugin setup_plugin_for_restore /home/test_plugin_config.yaml /data_dir-1/backups/20180101/20180101010101 coordinator -1
test_plugin setup_plugin_for_restore /home/test_plugin_config.yaml /data_dir0/backups/20180101/20180101010101 segment_host
test_plugin setup_plugin_for_restore /home/test_plugin_config.yaml /data_dir0/backups/20180101/20180101010101 segment 0
test_plugin setup_plugin_for_restore /home/test_plugin_config.yaml /data_dir1/backups/20180101/20180101010101 segment 1
```

### [cleanup_plugin_for_backup](#cleanup_plugin_for_backup)

Steps necessary to tear down plugin once backup is complete. E.g. Disconnecting from backup service, removing temporary files created during backup, etc.

**Usage within gpbackup:**

Called during the backup teardown phase on the coordinator and each segment host. This will execute even if backup fails early due to an error.

**Arguments:**

[config_path](#config_path)

[local_backup_directory](#local_backup_directory)

[scope](#scope)

[contentID](#contentID)

**Stdout:** None

**Example:**
```
test_plugin cleanup_plugin_for_backup /home/test_plugin_config.yaml /data_dir-1/backups/20180101/20180101010101 coordinator -1
test_plugin cleanup_plugin_for_backup /home/test_plugin_config.yaml /data_dir0/backups/20180101/20180101010101 segment_host
test_plugin cleanup_plugin_for_backup /home/test_plugin_config.yaml /data_dir0/backups/20180101/20180101010101 segment 0
test_plugin cleanup_plugin_for_backup /home/test_plugin_config.yaml /data_dir1/backups/20180101/20180101010101 segment 1
```

### [cleanup_plugin_for_restore](#cleanup_plugin_for_restore)

Steps necessary to tear down plugin once restore is complete. E.g. Disconnecting from backup service, removing files created during restore, etc.

**Usage within gprestore:**

Called during the restore teardown phase on the coordinator and each segment host. This will execute even if restore fails early due to an error.

**Arguments:**

[config_path](#config_path)

[local_backup_directory](#local_backup_directory)

[scope](#scope)

[contentID](#contentID)

**Stdout:** None

**Example:**
```
test_plugin cleanup_plugin_for_restore /home/test_plugin_config.yaml /data_dir-1/backups/20180101/20180101010101 coordinator -1
test_plugin cleanup_plugin_for_restore /home/test_plugin_config.yaml /data_dir0/backups/20180101/20180101010101 segment_host
test_plugin cleanup_plugin_for_restore /home/test_plugin_config.yaml /data_dir0/backups/20180101/20180101010101 segment 0
test_plugin cleanup_plugin_for_restore /home/test_plugin_config.yaml /data_dir1/backups/20180101/20180101010101 segment 1
```

### [backup_file](#backup_file)

Given the path to a file gpbackup has created on local disk, this command should copy the file to the remote system. The original file should be left behind.

**Usage within gpbackup:**

Called once for each file created by gpbackup after the files have been written to the backup directories on local disk. Some files exist on the coordinator and others exist on the segments.

**Arguments:**

[config_path](#config_path)

[filepath_to_back_up](#filepath)

**Stdout:** None

**Example:**
```
test_plugin backup_file /home/test_plugin_config.yaml /data_dir/backups/20180101/20180101010101/gpbackup_20180101010101_metadata.sql
```

### [restore_file](#restore_file)

Given the path to a file gprestore will read on local disk, this command should recover this file from the remote system and place it at the specified path.

**Usage within gprestore:**

Called once for each file created by gpbackup to restore them to local disk so gprestore can read them. Some files will be restored to the coordinator and others to the segments.

**Arguments:**

[config_path](#config_path)

[filepath_to_restore](#filepath)

**Stdout:** None

**Example:**
```
test_plugin restore_file /home/test_plugin_config.yaml /data_dir/backups/20180101/20180101010101/gpbackup_20180101010101_metadata.sql
```

### [backup_data](#backup_data)

This command should read a potentially large stream of data from stdin and process/write this data to a remote system. The destination file should keep the same name as the provided argument for easier restore.

**Usage within gpbackup:**

Called by the gpbackup_helper agent process to stream all table data for a segment from the postgres process' stdout to the plugin's stdin. This is a single continuous stream per segment, and can be either compressed or uncompressed depending on flags provided to gpbackup.

**Arguments:**

[config_path](#config_path)

[data_filekey](#data_filekey)

**Stdout:** None

**Stdin** Expecting stream of data

**Example:**
```
COPY "<large amount of data>" | test_plugin backup_data /home/test_plugin_config.yaml /data_dir/backups/20180101/20180101010101/gpbackup_0_20180101010101
```

### [restore_data](#restore_data)

This command should read a potentially large data file specified by the filepath argument from the remote filesystem and process/write the contents to stdout. The data file in the restore system should have the same name as the filepath argument.

**Usage within gprestore:**

Called by the gpbackup_helper agent process to stream all table data for a segment from the remote system to be processed by the agent. If the backup_data command modified the data format (compression or otherwise), restore_data should perform the reverse operation before sending the data to gprestore.

**Arguments:**

[config_path](#config_path)

[data_filekey](#data_filekey)

**Stdout:** Stream of data from the remote source

**Example:**
```
test_plugin restore_data /home/test_plugin_config.yaml /data_dir/backups/20180101/20180101010101/gpbackup_0_20180101010101 > COPY ...
```
### [plugin_api_version](#plugin_api_version)

This command should echo the gpbackup plugin api version to stdout.

**Usage within gpbackup and gprestore:**

Called to verify the plugin is using a version of the gpbackup plugin API that is compatible with the given version of gpbackup and gprestore.

**Arguments:**

None

**Stdout:** X.Y.Z

**Example:**
```
test_plugin plugin_api_version
```

### [delete_backup](#delete_backup)

This command should delete the directory specified by the given backup timestamp on the remote system.

**Arguments:**

[config_path](#config_path)

[timestamp](#timestamp)

**Stdout:** None

**Example:**
```
test_plugin delete_backup /home/test_plugin_config.yaml 20180108130802
```

### [--version](#--version)

This command should display the version of the plugin itself (not the api version).

**Arguments:** None

**Stdout:**
[plugin_name] version [git_version]

_e.g.:_ gpbackup_s3_plugin version 1.1.0+dev.2.g16b18a1

**Example:**
```
test_plugin --version
```


## Plugin flow within gpbackup and gprestore
### Backup Plugin Flow
![Backup Plugin Flow](https://github.com/greenplum-db/gpbackup/wiki/backup_plugin_flow.png)

### Restore Plugin Flow
![Restore Plugin Flow](https://github.com/greenplum-db/gpbackup/wiki/restore_plugin_flow.png)

## Custom yaml file
Parameters specific to a plugin can be specified through the plugin configuration yaml file. The _executablepath_ key is required and used by gpbackup and gprestore. Additional arguments should be specified under the _options_ keyword. A path to this file is passed as the first argument to every API command. Options and valid arguments should be documented by the plugin.

Example yaml file for s3:
```
executablepath: <full path to gpbackup_s3_plugin>
options:
  region: us-west-2
  aws_access_key_id: ...
  aws_secret_access_key: ...
  bucket: my_bucket_name
  folder: greenplum_backups
```

## Verification using the gpbackup plugin API test bench

We provide tests to ensure your plugin will work with gpbackup and gprestore. If the tests succesfully run your plugin, you can be confident that your plugin will work with the utilities. The tests are located [here](https://github.com/greenplum-db/gpbackup/blob/coordinator/plugins/plugin_test.sh).

Run the test bench script using:

```
plugin_test.sh [path_to_executable] [plugin_config] [optional_config_for_secondary_destination]
```

This will individually test each command and run a backup and restore using your plugin. This suite will upload small amounts of data to your destination system (<1MB total)

If the `[optional_config_for_secondary_destination]` is provided, the test bench will also restore from this secondary destination.


## [Release Notes](#Release_Notes)

### Version 0.4.0
 - [delete_backup](#delete_backup) command added

### Version 0.2.0 - 0.3.0
 - Added [scope](#scope) and [contentID](#contentID) arguments to setup and cleanup functions for more control over execution location.

### Version 0.1.0
 - Initial commands added.
