package restore

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpbackup/history"
	"github.com/greenplum-db/gpbackup/options"
	"github.com/greenplum-db/gpbackup/toc"
	"github.com/greenplum-db/gpbackup/utils"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
)

/*
 * This file contains functions related to validating user input.
 */

func validateFilterListsInBackupSet() {
	ValidateIncludeSchemasInBackupSet(opts.IncludedSchemas)
	ValidateExcludeSchemasInBackupSet(opts.ExcludedSchemas)
	ValidateIncludeRelationsInBackupSet(opts.IncludedRelations)
	ValidateExcludeRelationsInBackupSet(opts.ExcludedRelations)
}

func ValidateIncludeSchemasInBackupSet(schemaList []string) {
	if keys := getFilterSchemasInBackupSet(schemaList); len(keys) != 0 {
		gplog.Fatal(errors.Errorf("Could not find the following schema(s) in the backup set: %s", strings.Join(keys, ", ")), "")
	}
}

func ValidateExcludeSchemasInBackupSet(schemaList []string) {
	if keys := getFilterSchemasInBackupSet(schemaList); len(keys) != 0 {
		gplog.Warn("Could not find the following excluded schema(s) in the backup set: %s", strings.Join(keys, ", "))
	}
}

/* This only checks the globalTOC, but will still succesfully validate tables
 * in incremental backups since incremental backups will always take backups of
 * the metadata (--incremental and --data-only backup flags are not compatible)
 */
func getFilterSchemasInBackupSet(schemaList []string) []string {
	if len(schemaList) == 0 {
		return []string{}
	}
	schemaMap := make(map[string]bool, len(schemaList))
	for _, schema := range schemaList {
		schemaMap[schema] = true
	}
	if RestoreSections.Predata {
		for _, entry := range globalTOC.PredataEntries {
			if _, ok := schemaMap[entry.Schema]; ok {
				delete(schemaMap, entry.Schema)
			}
			if len(schemaMap) == 0 {
				return []string{}
			}
		}
	} else {
		for _, entry := range globalTOC.DataEntries {
			if _, ok := schemaMap[entry.Schema]; ok {
				delete(schemaMap, entry.Schema)
			}
			if len(schemaMap) == 0 {
				return []string{}
			}
		}
	}

	keys := make([]string, len(schemaMap))
	i := 0
	for k := range schemaMap {
		keys[i] = k
		i++
	}
	return keys
}

func GenerateRestoreRelationList(opts options.Options) []string {
	includeRelations := opts.IncludedRelations
	if len(includeRelations) > 0 {
		return includeRelations
	}

	relationList := make([]string, 0)
	includedSchemaSet := utils.NewIncludeSet(opts.IncludedSchemas)
	excludedSchemaSet := utils.NewExcludeSet(opts.ExcludedSchemas)
	excludedRelationsSet := utils.NewExcludeSet(opts.ExcludedRelations)

	if len(globalTOC.DataEntries) == 0 {
		return []string{}
	}
	for _, entry := range globalTOC.DataEntries {
		fqn := utils.MakeFQN(entry.Schema, entry.Name)

		if includedSchemaSet.MatchesFilter(entry.Schema) &&
			excludedSchemaSet.MatchesFilter(entry.Schema) &&
			excludedRelationsSet.MatchesFilter(fqn) {
			relationList = append(relationList, fqn)
		}
	}
	return relationList
}
func ValidateRelationsInRestoreDatabase(connectionPool *dbconn.DBConn, relationList []string) {
	if len(relationList) == 0 {
		return
	}
	quotedTablesStr := utils.SliceToQuotedString(relationList)
	query := fmt.Sprintf(`
SELECT
	quote_ident(n.nspname) || '.' || quote_ident(c.relname) AS string
FROM pg_namespace n
JOIN pg_class c ON n.oid = c.relnamespace
WHERE quote_ident(n.nspname) || '.' || quote_ident(c.relname) IN (%s)`, quotedTablesStr)
	relationsInDB := dbconn.MustSelectStringSlice(connectionPool, query)

	/*
	 * When not restoring predata, we check that the relations we are planning to restore
	 * are already defined in the database so we have somewhere to put the data.
	 *
	 * Otherwise we check that the relations we are planning to restore
	 * are not already in the database so we don't get duplicate data.
	 */
	var errMsg string
	if !RestoreSections.Predata {
		gplog.Info("\n\n -- IN DB %s\nLIST %s", relationsInDB, relationList)
		if len(relationsInDB) < len(relationList) {
			dbRelationsSet := utils.NewSet(relationsInDB)
			for _, restoreRelation := range relationList {
				matches := dbRelationsSet.MatchesFilter(restoreRelation)
				if !matches {
					errMsg = fmt.Sprintf("Relation %s must exist for data-only restore", restoreRelation)
				}
			}
		}
	} else if len(relationsInDB) > 0 {
		errMsg = fmt.Sprintf("Relation %s already exists", relationsInDB[0])
	}
	if errMsg != "" {
		gplog.Fatal(nil, errMsg)
	}
}

func ValidateRedirectSchema(connectionPool *dbconn.DBConn, redirectSchema string) {
	query := fmt.Sprintf(`SELECT quote_ident(nspname) AS name FROM pg_namespace n WHERE n.nspname = '%s'`, redirectSchema)
	schemaInDB := dbconn.MustSelectStringSlice(connectionPool, query)

	if len(schemaInDB) == 0 {
		gplog.Fatal(nil, fmt.Sprintf("Schema %s to redirect into does not exist", redirectSchema))
	}
}

func ValidateIncludeRelationsInBackupSet(schemaList []string) {
	if keys := getFilterRelationsInBackupSet(schemaList); len(keys) != 0 {
		gplog.Fatal(errors.Errorf("Could not find the following relation(s) in the backup set: %s", strings.Join(keys, ", ")), "")
	}
}

func ValidateExcludeRelationsInBackupSet(schemaList []string) {
	if keys := getFilterRelationsInBackupSet(schemaList); len(keys) != 0 {
		gplog.Warn("Could not find the following excluded relation(s) in the backup set: %s", strings.Join(keys, ", "))
	}
}

func getFilterRelationsInBackupSet(relationList []string) []string {
	if len(relationList) == 0 {
		return []string{}
	}
	relationMap := make(map[string]bool, len(relationList))
	for _, relation := range relationList {
		relationMap[relation] = true
	}
	for _, entry := range globalTOC.PredataEntries {
		if entry.ObjectType != toc.OBJ_TABLE && entry.ObjectType != toc.OBJ_SEQUENCE && entry.ObjectType != toc.OBJ_VIEW && entry.ObjectType != toc.OBJ_MATERIALIZED_VIEW {
			continue
		}
		fqn := utils.MakeFQN(entry.Schema, entry.Name)
		if _, ok := relationMap[fqn]; ok {
			delete(relationMap, fqn)
		}
		if len(relationMap) == 0 {
			return []string{}
		}
	}

	dataEntries := make([]string, 0)
	for _, restorePlanEntry := range backupConfig.RestorePlan {
		dataEntries = append(dataEntries, restorePlanEntry.TableFQNs...)
	}
	for _, fqn := range dataEntries {
		if _, ok := relationMap[fqn]; ok {
			delete(relationMap, fqn)
		}
		if len(relationMap) == 0 {
			return []string{}
		}
	}

	keys := make([]string, len(relationMap))
	i := 0
	for k := range relationMap {
		keys[i] = k
		i++
	}
	return keys
}

func ValidateDatabaseExistence(unquotedDBName string, createDatabase bool, isFiltered bool) {
	qry := fmt.Sprintf(`
SELECT CASE
	WHEN EXISTS (SELECT 1 FROM pg_database WHERE datname='%s') THEN 'true'
	ELSE 'false'
END AS string;`, utils.EscapeSingleQuotes(unquotedDBName))
	databaseExists, err := strconv.ParseBool(dbconn.MustSelectString(connectionPool, qry))
	gplog.FatalOnError(err)
	if !databaseExists {
		if isFiltered {
			gplog.Fatal(errors.Errorf(`Database "%s" must be created manually to restore table-filtered or data-only backups.`, unquotedDBName), "")
		} else if !createDatabase {
			gplog.Fatal(errors.Errorf(`Database "%s" does not exist. Use the --create-db flag to create "%s" as part of the restore process.`, unquotedDBName, unquotedDBName), "")
		}
	} else if createDatabase {
		gplog.Fatal(errors.Errorf(`Database "%s" already exists. Run gprestore again without --create-db flag.`, unquotedDBName), "")
	}
}

func ValidateBackupFlagCombinations() {
	if (backupConfig.IncludeTableFiltered || !backupConfig.Globals) && RestoreSections.Globals {
		gplog.Warn("Global metadata is not backed up in table-filtered or data-only backups.")
	}

	if backupConfig.SingleDataFile && MustGetFlagInt(options.JOBS) != 1 {
		gplog.Fatal(errors.Errorf("Cannot use jobs flag when restoring backups with a single data file per segment."), "")
	}
	if !backupConfig.SingleDataFile && FlagChanged(options.COPY_QUEUE_SIZE) {
		gplog.Fatal(errors.Errorf("The --copy-queue-size flag can only be used if the backup was taken with --single-data-file"), "")
	}
	validateBackupFlagPluginCombinations()
}

func validateBackupFlagPluginCombinations() {
	if backupConfig.Plugin != "" && MustGetFlagString(options.PLUGIN_CONFIG) == "" {
		gplog.Fatal(errors.Errorf("Backup was taken with plugin %s. The --plugin-config flag must be used to restore.", backupConfig.Plugin), "")
	} else if backupConfig.Plugin == "" && MustGetFlagString(options.PLUGIN_CONFIG) != "" {
		gplog.Fatal(errors.Errorf("The --plugin-config flag cannot be used to restore a backup taken without a plugin."), "")
	}
}

func ValidateFlagCombinations(flags *pflag.FlagSet) {
	// TODO: remove these four checks once the deprecated flags are removed
	options.CheckExclusiveFlags(flags, options.DATA_ONLY, options.WITH_GLOBALS)
	options.CheckExclusiveFlags(flags, options.DATA_ONLY, options.CREATE_DB)
	options.CheckExclusiveFlags(flags, options.METADATA_ONLY, options.DATA_ONLY)
	options.CheckExclusiveFlags(flags, options.TRUNCATE_TABLE, options.METADATA_ONLY, options.INCREMENTAL)

	// Do not allow mixing --sections with the legacy flags
	options.CheckExclusiveFlags(flags, options.SECTIONS, options.METADATA_ONLY)
	options.CheckExclusiveFlags(flags, options.SECTIONS, options.DATA_ONLY)
	options.CheckExclusiveFlags(flags, options.SECTIONS, options.WITH_STATS)
	options.CheckExclusiveFlags(flags, options.SECTIONS, options.WITH_GLOBALS)

	options.CheckExclusiveFlags(flags, options.DEBUG, options.QUIET, options.VERBOSE)
	options.CheckExclusiveFlags(flags, options.INCLUDE_SCHEMA, options.INCLUDE_RELATION, options.INCLUDE_RELATION_FILE)
	options.CheckExclusiveFlags(flags, options.EXCLUDE_SCHEMA, options.INCLUDE_SCHEMA)
	options.CheckExclusiveFlags(flags, options.EXCLUDE_SCHEMA, options.EXCLUDE_RELATION, options.INCLUDE_RELATION, options.EXCLUDE_RELATION_FILE, options.INCLUDE_RELATION_FILE)
	options.CheckExclusiveFlags(flags, options.PLUGIN_CONFIG, options.BACKUP_DIR)
	options.CheckExclusiveFlags(flags, options.TRUNCATE_TABLE, options.INCREMENTAL)
	options.CheckExclusiveFlags(flags, options.TRUNCATE_TABLE, options.REDIRECT_SCHEMA)

	if flags.Changed(options.REDIRECT_SCHEMA) {
		// Redirect schema not compatible with any exclude flags
		if flags.Changed(options.EXCLUDE_SCHEMA) || flags.Changed(options.EXCLUDE_SCHEMA_FILE) ||
			flags.Changed(options.EXCLUDE_RELATION) || flags.Changed(options.EXCLUDE_RELATION_FILE) {
			gplog.Fatal(errors.Errorf("Cannot use --redirect-schema with exclude flags"), "")
		}
		// Redirect schema requires an include flag
		if !(flags.Changed(options.INCLUDE_RELATION) || flags.Changed(options.INCLUDE_RELATION_FILE) ||
			flags.Changed(options.INCLUDE_SCHEMA) || flags.Changed(options.INCLUDE_SCHEMA_FILE)) {
			gplog.Fatal(errors.Errorf("Cannot use --redirect-schema without --include-table, --include-table-file, --include-schema, or --include-schema-file"), "")
		}
	}
	if !flags.Changed(options.TIMESTAMP) && !flags.Changed(options.BACKUP_DIR) {
		gplog.Fatal(errors.Errorf("Must provide --backup-dir if --timestamp is not provided"), "")
	}

	// TODO: remove/edit these checks once the deprecated flags are removed
	sections := options.MustGetFlagStringSlice(flags, options.SECTIONS)
	options.CheckExclusiveFlags(flags, options.RUN_ANALYZE, options.WITH_STATS)
	if flags.Changed(options.RUN_ANALYZE) && slices.Contains(sections, "statistics") {
		gplog.Fatal(errors.Errorf("Cannot use --run-analyse if --sections=statistics is provided"), "")
	}

	if flags.Changed(options.TRUNCATE_TABLE) &&
		!(flags.Changed(options.INCLUDE_RELATION) || flags.Changed(options.INCLUDE_RELATION_FILE)) &&
		!flags.Changed(options.DATA_ONLY) {
		gplog.Fatal(errors.Errorf("Cannot use --truncate-table without --include-table or --include-table-file and without --data-only"), "")
	}
	if flags.Changed(options.INCREMENTAL) &&
		(!flags.Changed(options.DATA_ONLY) &&
			(slices.Contains(sections, "globals") || slices.Contains(sections, "predata") ||
				slices.Contains(sections, "postdata"))) {
		gplog.Fatal(errors.Errorf("Cannot use --incremental with globals, predata, or postdata sections"), "")
	}
}

func ValidateSafeToResizeCluster() {
	// If SegmentCount is 0, the backup was taken before the SegmentCount parameter was added, in which case we won't
	// allow a restore to a different-size cluster.  Any backups that do have a SegmentCount will have that checked
	// when attempting a normal restore, so that the user doesn't accidentally restore a different-size backup without
	// using the --resize-cluster flag.
	origSize, destSize, resizeCluster := GetResizeClusterInfo()

	if resizeCluster {
		if origSize == 0 {
			timestamp := MustGetFlagString(options.TIMESTAMP)
			gplog.Fatal(errors.Errorf("Segment count for backup with timestamp %s is unknown, cannot restore using --resize-cluster flag.", timestamp), "")
		} else if origSize == destSize {
			cmdFlags.Set(options.RESIZE_CLUSTER, "false")
			gplog.Warn("Backup segment count matches restore segment count; the --resize-cluster flag is not needed.  Proceeding with a normal restore.")
		} else {
			gplog.Info("Resize restore specified, will restore a backup set from a %d-segment cluster to a %d-segment cluster", origSize, destSize)
		}
	} else {
		if origSize != 0 && origSize != destSize {
			gplog.Fatal(errors.New(fmt.Sprintf("Cannot restore a backup taken on a cluster with %d segments to a cluster with %d segments unless the --resize-cluster flag is used.", origSize, destSize)), "")
		}
	}
}

// Various section-related validation that's hard to do in DoValidation before sections are parsed
func ValidateRestoreSections(flags *pflag.FlagSet, backupConfig *history.BackupConfig) {
	if (backupConfig.Sections == options.Sections{}) {
		gplog.Fatal(errors.Errorf("Must specify at least one section to restore"), "")
	}

	/*
	 * Check for sections set to true in RestoreSections that are not included in the backup
	 * set and handle them appropriately.
	 *
	 * 1) If no section-related flags were passed (so the values in RestoreSections were the
	 *    flag defaults), set them to match the sections found in the backup set; we want to
	 *    include whichever of the default sections were actually backed up.
	 *
	 * While we're here, also exclude metadata sections if --incremental was passed, since
	 * the default for an incremental restore is to restore only data regardless of which
	 * sections were included in the backup.
	 */
	useDefaults := !(flags.Changed(options.SECTIONS) ||
		flags.Changed(options.METADATA_ONLY) || flags.Changed(options.DATA_ONLY) ||
		flags.Changed(options.WITH_STATS) || flags.Changed(options.WITH_GLOBALS))
	if useDefaults {
		incremental := options.MustGetFlagBool(flags, options.INCREMENTAL)
		RestoreSections.Globals = RestoreSections.Globals && backupConfig.Globals && !incremental
		RestoreSections.Predata = RestoreSections.Predata && backupConfig.Predata && !incremental
		RestoreSections.Data = RestoreSections.Data && backupConfig.Data
		RestoreSections.Postdata = RestoreSections.Postdata && backupConfig.Postdata && !incremental
	}

	/*
	 * 2) If section-related flags were indeed passed, validate that each of the indicated
	 *    sections is available in the backup set, and if not, fail; we don't want to just
	 *    go ahead and restore what we have if the user asked for something else.
	 *
	 * InitializeBackupConfig already handles translating pre-section variables in old backups
	 * into the current format, so we only need to check the sections in backupConfig.Sections.
	 */
	errorMsg := "Cannot restore the %s section, as it is not present in this backup"
	if RestoreSections.Globals && !backupConfig.Globals {
		gplog.Fatal(errors.Errorf(errorMsg, "globals"), "")
	}
	if RestoreSections.Predata && !backupConfig.Predata {
		gplog.Fatal(errors.Errorf(errorMsg, "predata"), "")
	}
	if RestoreSections.Data && !backupConfig.Data {
		gplog.Fatal(errors.Errorf(errorMsg, "data"), "")
	}
	if RestoreSections.Postdata && !backupConfig.Postdata {
		gplog.Fatal(errors.Errorf(errorMsg, "postdata"), "")
	}
	if RestoreSections.Statistics && !backupConfig.Statistics {
		gplog.Fatal(errors.Errorf(errorMsg, "statistics"), "")
	}

	if options.MustGetFlagBool(flags, options.TRUNCATE_TABLE) && !RestoreSections.Data {
		gplog.Fatal(errors.Errorf("Cannot use --truncate-table without data section"), "")
	}
	if options.MustGetFlagBool(flags, options.CREATE_DB) && !RestoreSections.Predata {
		gplog.Fatal(errors.Errorf("Cannot use --create-db without predata section"), "")
	}
	if options.MustGetFlagBool(flags, options.INCREMENTAL) && (RestoreSections.Predata || RestoreSections.Postdata || RestoreSections.Globals || RestoreSections.Statistics) {
		gplog.Fatal(errors.Errorf("Cannot use --incremental with non-data sections in a restore"), "")
	}
}
