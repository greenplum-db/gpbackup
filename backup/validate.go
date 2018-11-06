package backup

import (
	"fmt"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpbackup/backup_filepath"
	"github.com/greenplum-db/gpbackup/utils"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
)

/*
 * This file contains functions related to validating user input.
 */

func validateFilterLists() {
	ValidateFilterSchemas(connectionPool, MustGetFlagStringSlice(utils.INCLUDE_SCHEMA), false)
	ValidateFilterSchemas(connectionPool, MustGetFlagStringSlice(utils.EXCLUDE_SCHEMA), true)
	ValidateFilterTables(connectionPool, MustGetFlagStringSlice(utils.INCLUDE_RELATION), false)
	ValidateFilterTables(connectionPool, MustGetFlagStringSlice(utils.EXCLUDE_RELATION), true)
}

func ValidateFilterSchemas(connectionPool *dbconn.DBConn, schemaList []string, excludeSet bool) {
	if len(schemaList) == 0 {
		return
	}
	quotedSchemasStr := utils.SliceToQuotedString(schemaList)
	query := fmt.Sprintf("SELECT nspname AS string FROM pg_namespace WHERE nspname IN (%s)", quotedSchemasStr)
	resultSchemas := dbconn.MustSelectStringSlice(connectionPool, query)
	if len(resultSchemas) < len(schemaList) {
		schemaSet := utils.NewSet(resultSchemas)
		for _, schema := range schemaList {
			if !schemaSet.MatchesFilter(schema) {
				if excludeSet {
					gplog.Warn(`Excluded schema %s does not exist`, schema)
				} else {
					gplog.Fatal(nil, "Schema %s does not exist", schema)
				}
			}
		}
	}
}

func ValidateFilterTables(connectionPool *dbconn.DBConn, tableList []string, excludeSet bool) {
	if len(tableList) == 0 {
		return
	}
	utils.ValidateFQNs(tableList)
	quotedTablesStr := utils.SliceToQuotedString(tableList)
	query := fmt.Sprintf(`
SELECT
	c.oid,
	n.nspname || '.' || c.relname AS name
FROM pg_namespace n
JOIN pg_class c ON n.oid = c.relnamespace
WHERE quote_ident(n.nspname) || '.' || quote_ident(c.relname) IN (%s)`, quotedTablesStr)
	resultTables := make([]struct {
		Oid  uint32
		Name string
	}, 0)
	err := connectionPool.Select(&resultTables, query)
	gplog.FatalOnError(err)
	tableMap := make(map[string]uint32)
	for _, table := range resultTables {
		tableMap[table.Name] = table.Oid
	}

	partTableMap := GetPartitionTableMap(connectionPool)
	for _, table := range tableList {
		tableOid := tableMap[table]
		if tableOid == 0 {
			if excludeSet {
				gplog.Warn("Excluded table %s does not exist", table)
			} else {
				gplog.Fatal(nil, "Table %s does not exist", table)
			}
		}
		if partTableMap[tableOid].Level == "i" {
			gplog.Fatal(nil, "Cannot filter on %s, as it is an intermediate partition table.  Only parent partition tables and leaf partition tables may be specified.", table)
		}
	}
}

func ValidateFlagCombinations(flags *pflag.FlagSet) {
	utils.CheckExclusiveFlags(flags, utils.DEBUG, utils.QUIET, utils.VERBOSE)
	utils.CheckExclusiveFlags(flags, utils.DATA_ONLY, utils.METADATA_ONLY, utils.INCREMENTAL)
	utils.CheckExclusiveFlags(flags, utils.INCLUDE_SCHEMA, utils.INCLUDE_RELATION, utils.INCLUDE_RELATION_FILE)
	utils.CheckExclusiveFlags(flags, utils.EXCLUDE_SCHEMA, utils.INCLUDE_SCHEMA)
	utils.CheckExclusiveFlags(flags, utils.EXCLUDE_SCHEMA, utils.EXCLUDE_RELATION, utils.INCLUDE_RELATION, utils.EXCLUDE_RELATION_FILE, utils.INCLUDE_RELATION_FILE)
	utils.CheckExclusiveFlags(flags, utils.EXCLUDE_RELATION, utils.EXCLUDE_RELATION_FILE, utils.LEAF_PARTITION_DATA)
	utils.CheckExclusiveFlags(flags, utils.JOBS, utils.METADATA_ONLY, utils.SINGLE_DATA_FILE)
	utils.CheckExclusiveFlags(flags, utils.METADATA_ONLY, utils.LEAF_PARTITION_DATA)
	utils.CheckExclusiveFlags(flags, utils.NO_COMPRESSION, utils.COMPRESSION_LEVEL)
	utils.CheckExclusiveFlags(flags, utils.PLUGIN_CONFIG, utils.BACKUP_DIR)
	if MustGetFlagString(utils.FROM_TIMESTAMP) != "" && !MustGetFlagBool(utils.INCREMENTAL) {
		gplog.Fatal(errors.Errorf("--from-timestamp must be specified with --incremental"), "")
	}
	if MustGetFlagBool(utils.INCREMENTAL) && !MustGetFlagBool(utils.LEAF_PARTITION_DATA) {
		gplog.Fatal(errors.Errorf("--leaf-partition-data must be specified with --incremental"), "")
	}
}

func ValidateFlagValues() {
	err := utils.ValidateFullPath(MustGetFlagString(utils.BACKUP_DIR))
	gplog.FatalOnError(err)
	err = utils.ValidateFullPath(MustGetFlagString(utils.PLUGIN_CONFIG))
	gplog.FatalOnError(err)
	ValidateCompressionLevel(MustGetFlagInt(utils.COMPRESSION_LEVEL))
	if MustGetFlagString(utils.FROM_TIMESTAMP) != "" && !utils.IsValidTimestamp(MustGetFlagString(utils.FROM_TIMESTAMP)) {
		gplog.Fatal(errors.Errorf("Timestamp %s is invalid.  Timestamps must be in the format YYYYMMDDHHMMSS.",
			MustGetFlagString(utils.FROM_TIMESTAMP)), "")
	}
}

func ValidateCompressionLevel(compressionLevel int) {
	if compressionLevel < 1 || compressionLevel > 9 {
		gplog.Fatal(errors.Errorf("Compression level must be between 1 and 9"), "")
	}
}

func ValidateFromTimestamp(fromTimestamp string) {
	fromTimestampFPInfo := backup_filepath.NewFilePathInfo(globalCluster, globalFPInfo.UserSpecifiedBackupDir,
		fromTimestamp, globalFPInfo.UserSpecifiedSegPrefix)
	if MustGetFlagString(utils.PLUGIN_CONFIG) != "" {
		// The config file needs to be downloaded from the remote system into the local filesystem
		pluginConfig.MustRestoreFile(fromTimestampFPInfo.GetConfigFilePath())
	}
	fromBackupConfig := utils.ReadConfigFile(fromTimestampFPInfo.GetConfigFilePath())

	if !MatchesIncrementalFlags(fromBackupConfig, &backupReport.BackupConfig) {
		gplog.Fatal(errors.Errorf("The flags of the backup with timestamp = %s does not match "+
			"that of the current one. Please refer to the report to view the flags supplied for the"+
			"previous backup.", fromTimestampFPInfo.Timestamp), "")
	}
}
