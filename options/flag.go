package options

/*
 * This file contains functions and structs relating to flag parsing.
 */

import (
	"regexp"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
)

const (
	BACKUP_DIR            = "backup-dir"
	COMPRESSION_TYPE      = "compression-type"
	COMPRESSION_LEVEL     = "compression-level"
	DATA_ONLY             = "data-only"
	DBNAME                = "dbname"
	DEBUG                 = "debug"
	EXCLUDE_RELATION      = "exclude-table"
	EXCLUDE_RELATION_FILE = "exclude-table-file"
	EXCLUDE_SCHEMA        = "exclude-schema"
	EXCLUDE_SCHEMA_FILE   = "exclude-schema-file"
	FROM_TIMESTAMP        = "from-timestamp"
	INCLUDE_RELATION      = "include-table"
	INCLUDE_RELATION_FILE = "include-table-file"
	INCLUDE_SCHEMA        = "include-schema"
	INCLUDE_SCHEMA_FILE   = "include-schema-file"
	INCREMENTAL           = "incremental"
	JOBS                  = "jobs"
	LEAF_PARTITION_DATA   = "leaf-partition-data"
	METADATA_ONLY         = "metadata-only"
	NO_COMPRESSION        = "no-compression"
	PLUGIN_CONFIG         = "plugin-config"
	QUIET                 = "quiet"
	SINGLE_DATA_FILE      = "single-data-file"
	COPY_QUEUE_SIZE       = "copy-queue-size"
	VERBOSE               = "verbose"
	WITH_STATS            = "with-stats"
	CREATE_DB             = "create-db"
	ON_ERROR_CONTINUE     = "on-error-continue"
	REDIRECT_DB           = "redirect-db"
	RUN_ANALYZE           = "run-analyze"
	TIMESTAMP             = "timestamp"
	WITH_GLOBALS          = "with-globals"
	REDIRECT_SCHEMA       = "redirect-schema"
	TRUNCATE_TABLE        = "truncate-table"
	WITHOUT_GLOBALS       = "without-globals"
	RESIZE_CLUSTER        = "resize-cluster"
)

func SetBackupFlagDefaults(flagSet *pflag.FlagSet) {
	flagSet.String(BACKUP_DIR, "", "The absolute path of the directory to which all backup files will be written")
	flagSet.String(COMPRESSION_TYPE, "gzip", "Type of compression to use during data backup. Valid values are 'gzip', 'zstd'")
	flagSet.Int(COMPRESSION_LEVEL, 1, "Level of compression to use during data backup. Range of valid values depends on compression type")
	flagSet.Bool(DATA_ONLY, false, "Only back up data, do not back up metadata")
	flagSet.String(DBNAME, "", "The database to be backed up")
	flagSet.Bool(DEBUG, false, "Print verbose and debug log messages")
	flagSet.StringArray(EXCLUDE_SCHEMA, []string{}, "Back up all metadata except objects in the specified schema(s). --exclude-schema can be specified multiple times.")
	flagSet.String(EXCLUDE_SCHEMA_FILE, "", "A file containing a list of schemas to be excluded from the backup")
	flagSet.StringArray(EXCLUDE_RELATION, []string{}, "Back up all metadata except the specified table(s). --exclude-table can be specified multiple times.")
	flagSet.String(EXCLUDE_RELATION_FILE, "", "A file containing a list of fully-qualified tables to be excluded from the backup")
	flagSet.String(FROM_TIMESTAMP, "", "A timestamp to use to base the current incremental backup off")
	flagSet.Bool("help", false, "Help for gpbackup")
	flagSet.StringArray(INCLUDE_SCHEMA, []string{}, "Back up only the specified schema(s). --include-schema can be specified multiple times.")
	flagSet.String(INCLUDE_SCHEMA_FILE, "", "A file containing a list of schema(s) to be included in the backup")
	flagSet.StringArray(INCLUDE_RELATION, []string{}, "Back up only the specified table(s). --include-table can be specified multiple times.")
	flagSet.String(INCLUDE_RELATION_FILE, "", "A file containing a list of fully-qualified tables to be included in the backup")
	flagSet.Bool(INCREMENTAL, false, "Only back up data for AO tables that have been modified since the last backup")
	flagSet.Int(JOBS, 1, "The number of parallel connections to use when backing up data")
	flagSet.Bool(LEAF_PARTITION_DATA, false, "For partition tables, create one data file per leaf partition instead of one data file for the whole table")
	flagSet.Bool(METADATA_ONLY, false, "Only back up metadata, do not back up data")
	flagSet.Bool(NO_COMPRESSION, false, "Skip compression of data files")
	flagSet.String(PLUGIN_CONFIG, "", "The configuration file to use for a plugin")
	flagSet.Bool("version", false, "Print version number and exit")
	flagSet.Bool(QUIET, false, "Suppress non-warning, non-error log messages")
	flagSet.Bool(SINGLE_DATA_FILE, false, "Back up all data to a single file instead of one per table")
	flagSet.Int(COPY_QUEUE_SIZE, 1, "number of COPY commands gpbackup should enqueue when backing up using the --single-data-file option")
	flagSet.Bool(VERBOSE, false, "Print verbose log messages")
	flagSet.Bool(WITH_STATS, false, "Back up query plan statistics")
	flagSet.Bool(WITHOUT_GLOBALS, false, "Skip backup of global metadata")
}

func SetRestoreFlagDefaults(flagSet *pflag.FlagSet) {
	flagSet.String(BACKUP_DIR, "", "The absolute path of the directory in which the backup files to be restored are located")
	flagSet.Bool(CREATE_DB, false, "Create the database before metadata restore")
	flagSet.Bool(DATA_ONLY, false, "Only restore data, do not restore metadata")
	flagSet.Bool(DEBUG, false, "Print verbose and debug log messages")
	flagSet.StringArray(EXCLUDE_SCHEMA, []string{}, "Restore all metadata except objects in the specified schema(s). --exclude-schema can be specified multiple times.")
	flagSet.String(EXCLUDE_SCHEMA_FILE, "", "A file containing a list of schemas that will not be restored")
	flagSet.StringArray(EXCLUDE_RELATION, []string{}, "Restore all metadata except the specified relation(s). --exclude-table can be specified multiple times.")
	flagSet.String(EXCLUDE_RELATION_FILE, "", "A file containing a list of fully-qualified relation(s) that will not be restored")
	flagSet.Bool("help", false, "Help for gprestore")
	flagSet.StringArray(INCLUDE_SCHEMA, []string{}, "Restore only the specified schema(s). --include-schema can be specified multiple times.")
	flagSet.String(INCLUDE_SCHEMA_FILE, "", "A file containing a list of schemas that will be restored")
	flagSet.StringArray(INCLUDE_RELATION, []string{}, "Restore only the specified relation(s). --include-table can be specified multiple times.")
	flagSet.String(INCLUDE_RELATION_FILE, "", "A file containing a list of fully-qualified relation(s) that will be restored")
	flagSet.Bool(INCREMENTAL, false, "BETA FEATURE: Only restore data for all heap tables and only AO tables that have been modified since the last backup")
	flagSet.Bool(METADATA_ONLY, false, "Only restore metadata, do not restore data")
	flagSet.Int(JOBS, 1, "Number of parallel connections to use when restoring table data and post-data")
	flagSet.Bool(ON_ERROR_CONTINUE, false, "Log errors and continue restore, instead of exiting on first error")
	flagSet.String(PLUGIN_CONFIG, "", "The configuration file to use for a plugin")
	flagSet.Bool("version", false, "Print version number and exit")
	flagSet.Bool(QUIET, false, "Suppress non-warning, non-error log messages")
	flagSet.String(REDIRECT_DB, "", "Restore to the specified database instead of the database that was backed up")
	flagSet.String(REDIRECT_SCHEMA, "", "Restore to the specified schema instead of the schema that was backed up")
	flagSet.Int(COPY_QUEUE_SIZE, 1, "Number of COPY commands gprestore should enqueue when restoring a backup taken using the --single-data-file option")
	flagSet.Bool(WITH_GLOBALS, false, "Restore global metadata")
	flagSet.String(TIMESTAMP, "", "The timestamp to be restored, in the format YYYYMMDDHHMMSS")
	flagSet.Bool(TRUNCATE_TABLE, false, "Removes data of the tables getting restored")
	flagSet.Bool(VERBOSE, false, "Print verbose log messages")
	flagSet.Bool(WITH_STATS, false, "Restore query plan statistics")
	flagSet.Bool(LEAF_PARTITION_DATA, false, "For partition tables, create one data file per leaf partition instead of one data file for the whole table")
	flagSet.Bool(RUN_ANALYZE, false, "Run ANALYZE on restored tables")
	flagSet.Bool(RESIZE_CLUSTER, false, "Restore a backup taken on a cluster with more or fewer segments than the cluster to which it will be restored")
	_ = flagSet.MarkHidden(LEAF_PARTITION_DATA)
}

/*
 * Functions for validating whether flags are set and in what combination
 */

// At most one of the flags passed to this function may be set
func CheckExclusiveFlags(flags *pflag.FlagSet, flagNames ...string) {
	numSet := 0
	for _, name := range flagNames {
		if flags.Changed(name) {
			numSet++
		}
	}
	if numSet > 1 {
		gplog.Fatal(errors.Errorf("The following flags may not be specified together: %s", strings.Join(flagNames, ", ")), "")
	}
}

/*
 * Functions for validating flag values
 */

/*
 * Convert arguments that contain a single dash to double dashes for backward
 * compatibility.
 */
func HandleSingleDashes(args []string) []string {
	r, _ := regexp.Compile(`^-(\w{2,})`)
	var newArgs []string
	for _, arg := range args {
		newArg := r.ReplaceAllString(arg, "--$1")
		newArgs = append(newArgs, newArg)
	}
	return newArgs
}

func MustGetFlagString(cmdFlags *pflag.FlagSet, flagName string) string {
	value, err := cmdFlags.GetString(flagName)
	gplog.FatalOnError(err)
	return value
}

func MustGetFlagInt(cmdFlags *pflag.FlagSet, flagName string) int {
	value, err := cmdFlags.GetInt(flagName)
	gplog.FatalOnError(err)
	return value
}

func MustGetFlagBool(cmdFlags *pflag.FlagSet, flagName string) bool {
	value, err := cmdFlags.GetBool(flagName)
	gplog.FatalOnError(err)
	return value
}

func MustGetFlagStringSlice(cmdFlags *pflag.FlagSet, flagName string) []string {
	value, err := cmdFlags.GetStringSlice(flagName)
	gplog.FatalOnError(err)
	return value
}

func MustGetFlagStringArray(cmdFlags *pflag.FlagSet, flagName string) []string {
	value, err := cmdFlags.GetStringArray(flagName)
	gplog.FatalOnError(err)
	return value
}
