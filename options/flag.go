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
	VERBOSE               = "verbose"
	WITH_STATS            = "with-stats"
	CREATE_DB             = "create-db"
	ON_ERROR_CONTINUE     = "on-error-continue"
	REDIRECT_DB           = "redirect-db"
	TIMESTAMP             = "timestamp"
	WITH_GLOBALS          = "with-globals"
	REDIRECT_SCHEMA       = "redirect-schema"
)

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
