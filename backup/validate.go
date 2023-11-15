package backup

import (
	"fmt"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpbackup/filepath"
	"github.com/greenplum-db/gpbackup/history"
	"github.com/greenplum-db/gpbackup/options"
	"github.com/greenplum-db/gpbackup/utils"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
)

/*
 * This file contains functions related to validating user input.
 */

/* This function populates the global table filter lists, so must be run before those are
 * referenced
 */
func ValidateAndProcessFilterLists(opts *options.Options) {
	gplog.Verbose("Validating Tables and Schemas exist in Database")

	// pre-create these so we can save the processed versions of our filters
	IncludedRelationFqns = make([]options.Relation, 0)
	ExcludedRelationFqns = make([]options.Relation, 0)

	ValidateTablesExist(connectionPool, opts.GetIncludedTables(), false)
	ValidateTablesExist(connectionPool, opts.GetExcludedTables(), true)
	ValidateSchemasExist(connectionPool, opts.GetIncludedSchemas(), false)
	ValidateSchemasExist(connectionPool, opts.GetExcludedSchemas(), true)
}

func ValidateSchemasExist(connectionPool *dbconn.DBConn, schemaList []string, excludeSet bool) {
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

func ValidateTablesExist(conn *dbconn.DBConn, tableList []string, excludeSet bool) {
	if len(tableList) == 0 {
		return
	}

	quotedIncludeRelations, err := options.QuoteTableNames(connectionPool, tableList)
	gplog.FatalOnError(err)

	quotedTablesStr := utils.SliceToQuotedString(quotedIncludeRelations)
	query := fmt.Sprintf(`
	SELECT
        n.oid as schemaoid,
		c.oid,
        n.nspname as schemaname,
        c.relname as tablename,
		quote_ident(n.nspname) || '.' || quote_ident(c.relname) AS name
	FROM pg_namespace n
	JOIN pg_class c ON n.oid = c.relnamespace
	WHERE quote_ident(n.nspname) || '.' || quote_ident(c.relname) IN (%s)`, quotedTablesStr)
	resultTables := make([]struct {
		SchemaOid  uint32
		Oid        uint32
		SchemaName string
		TableName  string
		Name       string
	}, 0)
	err = conn.Select(&resultTables, query)
	gplog.FatalOnError(err, fmt.Sprintf("Query was: %s", query))
	tableMap := make(map[string]options.Relation)
	for _, table := range resultTables {
		currFqn := options.Relation{
			SchemaOid: table.SchemaOid,
			Oid:       table.Oid,
			Schema:    table.SchemaName,
			Name:      table.TableName,
		}
		tableMap[table.Name] = currFqn
	}

	partTableMap := GetPartitionTableMap(conn)
	for idx, table := range quotedIncludeRelations {
		tableRel, ok := tableMap[table]

		/* Because users have some flexibility in what they choose to quote, also check for matches
		 * using table names that are not run through quote_ident.
		 */
		if !ok {
			unquotedTable := tableList[idx]
			unQuotedTableRel, unquotedOk := tableMap[unquotedTable]
			if unquotedOk {
				tableRel = unQuotedTableRel
				table = unquotedTable
				ok = true
			}
		}

		if !ok {
			if excludeSet {
				gplog.Warn("Excluded table %s does not exist", table)
			} else {
				gplog.Fatal(nil, "Table %s does not exist", table)
			}
		}
		if partTableMap[tableRel.Oid].Level == "i" {
			gplog.Fatal(nil, "Cannot filter on %s, as it is an intermediate partition table.  Only parent partition tables and leaf partition tables may be specified.", table)
		}
		if partTableMap[tableRel.Oid].Level == "l" && !MustGetFlagBool(options.LEAF_PARTITION_DATA) {
			gplog.Fatal(nil, "--leaf-partition-data flag must be specified to filter on %s, as it is a leaf partition table.", table)
		}

		// store the table oids for later use, to avoid needing to re-process
		if ok {
			if excludeSet {
				AddExcludedRelationFqn(tableRel)
			} else {
				AddIncludedRelationFqn(tableRel)
			}
		}
	}
}

func validateFlagCombinations(flags *pflag.FlagSet) {
	// TODO: remove these three checks once the deprecated flags are removed
	options.CheckExclusiveFlags(flags, options.JOBS, options.METADATA_ONLY, options.SINGLE_DATA_FILE)
	options.CheckExclusiveFlags(flags, options.DATA_ONLY, options.METADATA_ONLY, options.INCREMENTAL)
	options.CheckExclusiveFlags(flags, options.METADATA_ONLY, options.LEAF_PARTITION_DATA)

	// Do not allow mixing --sections with the legacy flags
	options.CheckExclusiveFlags(flags, options.SECTIONS, options.METADATA_ONLY)
	options.CheckExclusiveFlags(flags, options.SECTIONS, options.DATA_ONLY)
	options.CheckExclusiveFlags(flags, options.SECTIONS, options.WITHOUT_GLOBALS)
	options.CheckExclusiveFlags(flags, options.SECTIONS, options.WITH_STATS)

	options.CheckExclusiveFlags(flags, options.DEBUG, options.QUIET, options.VERBOSE)
	options.CheckExclusiveFlags(flags, options.INCLUDE_SCHEMA, options.INCLUDE_SCHEMA_FILE, options.INCLUDE_RELATION, options.INCLUDE_RELATION_FILE)
	options.CheckExclusiveFlags(flags, options.EXCLUDE_SCHEMA, options.EXCLUDE_SCHEMA_FILE, options.INCLUDE_SCHEMA, options.INCLUDE_SCHEMA_FILE)
	options.CheckExclusiveFlags(flags, options.EXCLUDE_SCHEMA, options.EXCLUDE_SCHEMA_FILE, options.EXCLUDE_RELATION, options.INCLUDE_RELATION, options.EXCLUDE_RELATION_FILE, options.INCLUDE_RELATION_FILE)
	options.CheckExclusiveFlags(flags, options.NO_COMPRESSION, options.COMPRESSION_TYPE)
	options.CheckExclusiveFlags(flags, options.NO_COMPRESSION, options.COMPRESSION_LEVEL)
	options.CheckExclusiveFlags(flags, options.PLUGIN_CONFIG, options.BACKUP_DIR)
	if FlagChanged(options.COPY_QUEUE_SIZE) && !MustGetFlagBool(options.SINGLE_DATA_FILE) {
		gplog.Fatal(errors.Errorf("--copy-queue-size must be specified with --single-data-file"), "")
	}
	if MustGetFlagString(options.FROM_TIMESTAMP) != "" && !MustGetFlagBool(options.INCREMENTAL) {
		gplog.Fatal(errors.Errorf("--from-timestamp must be specified with --incremental"), "")
	}
	if MustGetFlagBool(options.INCREMENTAL) && !MustGetFlagBool(options.LEAF_PARTITION_DATA) {
		gplog.Fatal(errors.Errorf("--leaf-partition-data must be specified with --incremental"), "")
	}
	if MustGetFlagBool(options.NO_INHERITS) && !(FlagChanged(options.INCLUDE_RELATION) || FlagChanged(options.INCLUDE_RELATION_FILE)) {
		gplog.Fatal(errors.Errorf("--no-inherits must be specified with either --include-table or --include-table-file"), "")
	}
	if FlagChanged(options.SINGLE_BACKUP_DIR) && !FlagChanged(options.BACKUP_DIR) {
		gplog.Fatal(errors.Errorf("--single-backup-dir must be specified with --backup-dir"), "")
	}
}

func validateFlagValues() {
	err := utils.ValidateFullPath(MustGetFlagString(options.BACKUP_DIR))
	gplog.FatalOnError(err)
	err = utils.ValidateFullPath(MustGetFlagString(options.PLUGIN_CONFIG))
	gplog.FatalOnError(err)
	err = utils.ValidateCompressionTypeAndLevel(MustGetFlagString(options.COMPRESSION_TYPE), MustGetFlagInt(options.COMPRESSION_LEVEL))
	gplog.FatalOnError(err)
	if MustGetFlagString(options.FROM_TIMESTAMP) != "" && !filepath.IsValidTimestamp(MustGetFlagString(options.FROM_TIMESTAMP)) {
		gplog.Fatal(errors.Errorf("Timestamp %s is invalid.  Timestamps must be in the format YYYYMMDDHHMMSS.",
			MustGetFlagString(options.FROM_TIMESTAMP)), "")
	}
	if FlagChanged(options.COPY_QUEUE_SIZE) && MustGetFlagInt(options.COPY_QUEUE_SIZE) < 2 {
		gplog.Fatal(errors.Errorf("--copy-queue-size %d is invalid. Must be at least 2",
			MustGetFlagInt(options.COPY_QUEUE_SIZE)), "")
	}
}

func validateFromTimestamp(fromTimestamp string) {
	fromTimestampFPInfo := filepath.NewFilePathInfo(globalCluster, globalFPInfo.UserSpecifiedBackupDir,
		fromTimestamp, globalFPInfo.UserSpecifiedSegPrefix, globalFPInfo.SingleBackupDir)

	if MustGetFlagString(options.PLUGIN_CONFIG) != "" {
		// The config file needs to be downloaded from the remote system into the local filesystem
		pluginConfig.MustRestoreFile(fromTimestampFPInfo.GetConfigFilePath())
	}
	fromBackupConfig := history.ReadConfigFile(fromTimestampFPInfo.GetConfigFilePath())

	if !matchesIncrementalFlags(fromBackupConfig, &backupReport.BackupConfig) {
		gplog.Fatal(errors.Errorf("The flags of the backup with timestamp = %s does not match "+
			"that of the current one. Please refer to the report to view the flags supplied for the "+
			"previous backup.", fromTimestampFPInfo.Timestamp), "")
	}
}

// Validate some flag combinations here, since they're harder to check in DoValidation
// before the sections are parsed
func ValidateBackupSections(flags *pflag.FlagSet) {
	if options.MustGetFlagBool(flags, options.SINGLE_DATA_FILE) && !BackupSections.Data {
		gplog.Fatal(errors.Errorf("Cannot use --single-data-file without data section"), "")
	}
	if options.MustGetFlagBool(flags, options.LEAF_PARTITION_DATA) && !BackupSections.Data {
		gplog.Fatal(errors.Errorf("Cannot use --leaf-partition-data without data section"), "")
	}
	if options.MustGetFlagBool(flags, options.INCREMENTAL) && (!BackupSections.Data || !BackupSections.Predata || !BackupSections.Postdata) {
		gplog.Fatal(errors.Errorf("Cannot use --incremental without predata, data, and postdata sections in a backup"), "")
	}

}
