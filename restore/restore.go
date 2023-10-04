package restore

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"runtime/debug"
	"strings"
	"sync"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/greenplum-db/gpbackup/filepath"
	"github.com/greenplum-db/gpbackup/history"
	"github.com/greenplum-db/gpbackup/options"
	"github.com/greenplum-db/gpbackup/report"
	"github.com/greenplum-db/gpbackup/toc"
	"github.com/greenplum-db/gpbackup/utils"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

// This function handles setup that can be done before parsing flags.
func DoInit(cmd *cobra.Command) {
	CleanupGroup = &sync.WaitGroup{}
	CleanupGroup.Add(1)
	gplog.InitializeLogging("gprestore", "")
	SetCmdFlags(cmd.Flags())
	utils.InitializeSignalHandler(DoCleanup, "restore process", &wasTerminated)
}

/*
* This function handles argument parsing and validation, e.g. checking that a passed filename exists.
* It should only validate; initialization with any sort of side effects should go in DoInit or DoSetup.
 */
func DoValidation(cmd *cobra.Command) {
	ValidateFlagCombinations(cmd.Flags())
	err := utils.ValidateFullPath(MustGetFlagString(options.BACKUP_DIR))
	gplog.FatalOnError(err)
	err = utils.ValidateFullPath(MustGetFlagString(options.PLUGIN_CONFIG))
	gplog.FatalOnError(err)
	providedTimestamp := MustGetFlagString(options.TIMESTAMP)
	if providedTimestamp != "" && !filepath.IsValidTimestamp(providedTimestamp) {
		gplog.Fatal(errors.Errorf("Timestamp %s is invalid.  Timestamps must be in the format YYYYMMDDHHMMSS.", providedTimestamp), "")
	}
}

// This function handles setup that must be done after parsing flags.
func DoSetup() {
	SetLoggerVerbosity()
	gplog.Verbose("Restore Command: %s", os.Args)

	utils.CheckGpexpandRunning(utils.RestorePreventedByGpexpandMessage)
	restoreStartTime = history.CurrentTimestamp()

	CreateConnectionPool("postgres")
	segConfig := cluster.MustGetSegmentConfiguration(connectionPool)
	globalCluster = cluster.NewCluster(segConfig)

	var err error
	backupTimestamp := MustGetFlagString(options.TIMESTAMP)
	if backupTimestamp == "" {
		backupDir := MustGetFlagString(options.BACKUP_DIR)
		if backupDir == "" {
			backupDir = globalCluster.GetDirForContent(-1)
		}
		backupTimestamp, err = filepath.GetTimestampFromBackupDirectory(backupDir)
		gplog.FatalOnError(err)
	}
	gplog.Info("Restore Key = %s", backupTimestamp)

	var segPrefix string
	opts, err = options.NewOptions(cmdFlags)
	gplog.FatalOnError(err)

	err = opts.QuoteIncludeRelations(connectionPool)
	gplog.FatalOnError(err)

	err = opts.QuoteExcludeRelations(connectionPool)
	gplog.FatalOnError(err)

	segPrefix, err = filepath.ParseSegPrefix(MustGetFlagString(options.BACKUP_DIR))
	gplog.FatalOnError(err)
	globalFPInfo = filepath.NewFilePathInfo(globalCluster, MustGetFlagString(options.BACKUP_DIR), backupTimestamp, segPrefix)
	if reportDir := MustGetFlagString(options.REPORT_DIR); reportDir != "" {
		globalFPInfo.UserSpecifiedReportDir = reportDir
		err := operating.System.MkdirAll(globalFPInfo.GetReportDirectoryPath(), 0775)
		gplog.FatalOnError(err)
	}

	// Get restore metadata from plugin
	if MustGetFlagString(options.PLUGIN_CONFIG) != "" {
		RecoverMetadataFilesUsingPlugin()
	} else {
		InitializeBackupConfig()
	}

	ValidateSafeToResizeCluster()

	gplog.Info("gpbackup version = %s", backupConfig.BackupVersion)
	gplog.Info("gprestore version = %s", GetVersion())
	gplog.Info("Greenplum Database Version = %s", connectionPool.Version.VersionString)

	BackupConfigurationValidation()
	metadataFilename := globalFPInfo.GetMetadataFilePath()
	if !backupConfig.DataOnly {
		gplog.Verbose("Metadata will be restored from %s", metadataFilename)
	}
	unquotedRestoreDatabase := utils.UnquoteIdent(backupConfig.DatabaseName)
	if MustGetFlagString(options.REDIRECT_DB) != "" {
		unquotedRestoreDatabase = MustGetFlagString(options.REDIRECT_DB)
	}
	ValidateDatabaseExistence(unquotedRestoreDatabase, MustGetFlagBool(options.CREATE_DB), backupConfig.IncludeTableFiltered || backupConfig.DataOnly)
	if MustGetFlagBool(options.WITH_GLOBALS) {
		restoreGlobal(metadataFilename)
	} else if MustGetFlagBool(options.CREATE_DB) {
		createDatabase(metadataFilename)
	}
	if connectionPool != nil {
		connectionPool.Close()
	}
	InitializeConnectionPool(backupTimestamp, restoreStartTime, unquotedRestoreDatabase)

	/*
	 * We don't need to validate anything if we're creating the database; we
	 * should not error out for validation reasons once the restore database exists.
	 * For on-error-continue, we will see the same errors later when we try to run SQL,
	 * but since they will not stop the restore, it is not necessary to log them twice.
	 */
	if !MustGetFlagBool(options.CREATE_DB) && !MustGetFlagBool(options.ON_ERROR_CONTINUE) && !MustGetFlagBool(options.INCREMENTAL) {
		relationsToRestore := GenerateRestoreRelationList(*opts)
		if opts.RedirectSchema != "" {
			fqns, err := options.SeparateSchemaAndTable(relationsToRestore)
			gplog.FatalOnError(err)
			redirectRelationsToRestore := make([]string, 0)
			for _, fqn := range fqns {
				redirectRelationsToRestore = append(redirectRelationsToRestore, utils.MakeFQN(opts.RedirectSchema, fqn.Name))
			}
			relationsToRestore = redirectRelationsToRestore
		}
		ValidateRelationsInRestoreDatabase(connectionPool, relationsToRestore)
	}

	if opts.RedirectSchema != "" {
		ValidateRedirectSchema(connectionPool, opts.RedirectSchema)
	}
}

func DoRestore() {
	var filteredDataEntries map[string][]toc.CoordinatorDataEntry
	metadataFilename := globalFPInfo.GetMetadataFilePath()
	isDataOnly := backupConfig.DataOnly || MustGetFlagBool(options.DATA_ONLY)
	isMetadataOnly := backupConfig.MetadataOnly || MustGetFlagBool(options.METADATA_ONLY)
	isIncremental := MustGetFlagBool(options.INCREMENTAL)

	if isIncremental {
		verifyIncrementalState()
	}

	if !isDataOnly && !isIncremental {
		restorePredata(metadataFilename)
	} else if isDataOnly {
		// The sequence setval commands need to be run during data only restores since
		// they are arguably the data of the sequence relations and can affect user tables
		// containing columns that reference those sequence relations.
		restoreSequenceValues(metadataFilename)
	}

	totalTablesRestored := 0
	if !isMetadataOnly {
		if MustGetFlagString(options.PLUGIN_CONFIG) == "" {
			VerifyBackupFileCountOnSegments()
		}
		totalTablesRestored, filteredDataEntries = restoreData()
	}

	if !isDataOnly && !isIncremental {
		restorePostdata(metadataFilename)
	}

	if MustGetFlagBool(options.WITH_STATS) && backupConfig.WithStatistics {
		restoreStatistics()
	} else if MustGetFlagBool(options.RUN_ANALYZE) && totalTablesRestored > 0 {
		runAnalyze(filteredDataEntries)
	}
}

func createDatabase(metadataFilename string) {
	objectTypes := []string{toc.OBJ_SESSION_GUC, toc.OBJ_DATABASE_GUC, toc.OBJ_DATABASE, toc.OBJ_DATABASE_METADATA}
	dbName := backupConfig.DatabaseName
	gplog.Info("Creating database")
	statements := GetRestoreMetadataStatements("global", metadataFilename, objectTypes, []string{})
	if MustGetFlagString(options.REDIRECT_DB) != "" {
		quotedDBName := utils.QuoteIdent(connectionPool, MustGetFlagString(options.REDIRECT_DB))
		dbName = quotedDBName
		statements = toc.SubstituteRedirectDatabaseInStatements(statements, backupConfig.DatabaseName, quotedDBName)
	}
	numErrors := ExecuteRestoreMetadataStatements(statements, "", nil, utils.PB_NONE, false)

	if numErrors > 0 {
		gplog.Info("Database creation completed with failures for: %s", dbName)
	} else {
		gplog.Info("Database creation complete for: %s", dbName)
	}
}

func restoreGlobal(metadataFilename string) {
	objectTypes := []string{toc.OBJ_SESSION_GUC, toc.OBJ_DATABASE_GUC, toc.OBJ_DATABASE_METADATA,
		toc.OBJ_RESOURCE_QUEUE, toc.OBJ_RESOURCE_GROUP, toc.OBJ_ROLE, toc.OBJ_ROLE_GUC, toc.OBJ_ROLE_GRANT, toc.OBJ_TABLESPACE}
	if MustGetFlagBool(options.CREATE_DB) {
		objectTypes = append(objectTypes, toc.OBJ_DATABASE)
	}
	gplog.Info("Restoring global metadata")
	statements := GetRestoreMetadataStatements("global", metadataFilename, objectTypes, []string{})
	if MustGetFlagString(options.REDIRECT_DB) != "" {
		quotedDBName := utils.QuoteIdent(connectionPool, MustGetFlagString(options.REDIRECT_DB))
		statements = toc.SubstituteRedirectDatabaseInStatements(statements, backupConfig.DatabaseName, quotedDBName)
	}
	statements = toc.RemoveActiveRole(connectionPool.User, statements)
	numErrors := ExecuteRestoreMetadataStatements(statements, "Global objects", nil, utils.PB_VERBOSE, false)

	if numErrors > 0 {
		gplog.Info("Global database metadata restore completed with failures")
	} else {
		gplog.Info("Global database metadata restore complete")
	}
}

func verifyIncrementalState() {
	lastRestorePlanEntry := backupConfig.RestorePlan[len(backupConfig.RestorePlan)-1]
	tableFQNsToRestore := lastRestorePlanEntry.TableFQNs

	existingSchemas, err := GetExistingSchemas()
	gplog.FatalOnError(err)
	existingTableFQNs, err := GetExistingTableFQNs()
	gplog.FatalOnError(err)

	existingSchemasMap := make(map[string]Empty)
	for _, schema := range existingSchemas {
		existingSchemasMap[schema] = Empty{}
	}
	existingTablesMap := make(map[string]Empty)
	for _, table := range existingTableFQNs {
		existingTablesMap[table] = Empty{}
	}

	var schemasToCreate []string
	var tableFQNsToCreate []string
	var schemasExcludedByUserInput []string
	var tablesExcludedByUserInput []string
	for _, table := range tableFQNsToRestore {
		schemaName := strings.Split(table, ".")[0]
		if utils.SchemaIsExcludedByUser(opts.IncludedSchemas, opts.ExcludedSchemas, schemaName) {
			if !utils.Exists(schemasExcludedByUserInput, schemaName) {
				schemasExcludedByUserInput = append(schemasExcludedByUserInput, schemaName)
			}
			tablesExcludedByUserInput = append(tablesExcludedByUserInput, table)
			continue
		}

		if _, exists := existingTablesMap[table]; !exists {
			if utils.RelationIsExcludedByUser(opts.IncludedRelations, opts.ExcludedRelations, table) {
				tablesExcludedByUserInput = append(tablesExcludedByUserInput, table)
			} else {
				_, schemaExists := existingSchemasMap[schemaName]
				preFilteredToCreate := utils.Exists(schemasToCreate, schemaName)
				if !schemaExists && !preFilteredToCreate {
					schemasToCreate = append(schemasToCreate, schemaName)
				}
				tableFQNsToCreate = append(tableFQNsToCreate, table)
			}
		}
	}

	var missing []string
	if len(schemasToCreate) > 0 && !MustGetFlagBool(options.ON_ERROR_CONTINUE) {
		missing = schemasToCreate
	}
	if len(tableFQNsToCreate) > 0 && !MustGetFlagBool(options.ON_ERROR_CONTINUE) {
		missing = append(missing, tableFQNsToCreate...)
	}
	if missing != nil {
		err = errors.Errorf("Following objects are missing from the target database: %v", missing)
		gplog.FatalOnError(err)
	}
}

func restorePredata(metadataFilename string) {
	if wasTerminated {
		return
	}
	var numErrors int32
	gplog.Info("Restoring pre-data metadata")
	// if not incremental restore - assume database is empty and just filter based on user input
	filters := NewFilters(opts.IncludedSchemas, opts.ExcludedSchemas, opts.IncludedRelations, opts.ExcludedRelations)
	var schemaStatements []toc.StatementWithType
	if opts.RedirectSchema == "" {
		schemaStatements = GetRestoreMetadataStatementsFiltered("predata", metadataFilename, []string{toc.OBJ_SCHEMA}, []string{}, filters)
	}
	statements := GetRestoreMetadataStatementsFiltered("predata", metadataFilename, []string{}, []string{toc.OBJ_SCHEMA}, filters)

	editStatementsRedirectSchema(statements, opts.RedirectSchema)
	progressBar := utils.NewProgressBar(len(schemaStatements)+len(statements), "Pre-data objects restored: ", utils.PB_VERBOSE)
	progressBar.Start()

	RestoreSchemas(schemaStatements, progressBar)
	executeInParallel := connectionPool.NumConns > 2 && !MustGetFlagBool(options.ON_ERROR_CONTINUE)
	if executeInParallel {
		// Batch statements by tier to allow more aggressive parallelization by cohort downstream.
		first, tiered, last := BatchPredataStatements(statements)
		gplog.Debug("Restoring predata metadata tier: 0")

		// Wrap each of the statement batches in a commit to avoid the overhead of 2-phase commits.
		// This dramatically reduces runtime of large metadata restores.
		connectionPool.MustBegin(0)
		numErrors = ExecuteRestoreMetadataStatements(first, "Pre-data objects", progressBar, utils.PB_VERBOSE, false)
		connectionPool.MustCommit(0)

		var t uint32 = 1
		var size uint32 = uint32(len(tiered))
		for t <= size {
			gplog.Debug("Restoring predata metadata tier: %d", t)

			// Begin transactions for each connec worker connection.  In parallel restores we reserve conn 0 for administration and monitoring.
			txMutex.Lock()
			for connNum := 1; connNum < connectionPool.NumConns; connNum++ {
				connectionPool.MustExec(fmt.Sprintf("SET application_name TO 'gprestore_%d_%s'", connNum, MustGetFlagString(options.TIMESTAMP)), connNum)
				connectionPool.MustBegin(connNum)
			}
			txMutex.Unlock()

			numErrors += ExecuteRestoreMetadataStatements(tiered[t], "Pre-data objects", progressBar, utils.PB_VERBOSE, true)
			t++
			// Connections are individually committed/rolled back as they finish to avoid lock
			// contention causing hangs. Ideally there will be no such contention, but this is done
			// defensively, to guard against unexpected metadata distributions.
		}
		gplog.Debug("Restoring predata metadata tier: %d", len(tiered)+1)
		connectionPool.MustBegin(0)
		numErrors += ExecuteRestoreMetadataStatements(last, "Pre-data objects", progressBar, utils.PB_VERBOSE, false)
		connectionPool.MustCommit(0)
	} else {
		if !MustGetFlagBool(options.ON_ERROR_CONTINUE) {
			// If on-error-continue is indicated, errors will kill the whole transaction and ruin
			// the behavior of the flag. So, we don't do transactions in that case, and just take
			// the perf hit.
			connectionPool.MustBegin(0)
		}
		numErrors = ExecuteRestoreMetadataStatements(statements, "Pre-data objects", progressBar, utils.PB_VERBOSE, false)
		if !MustGetFlagBool(options.ON_ERROR_CONTINUE) {
			connectionPool.Commit(0)
		}
	}

	progressBar.Finish()
	if wasTerminated {
		gplog.Info("Pre-data metadata restore incomplete")
	} else if numErrors > 0 {
		gplog.Info("Pre-data metadata restore completed with failures")
	} else {
		gplog.Info("Pre-data metadata restore complete")
	}
}

func restoreSequenceValues(metadataFilename string) {
	if wasTerminated {
		return
	}
	gplog.Info("Restoring sequence values")

	// if not incremental restore - assume database is empty and just filter based on user input
	filters := NewFilters(opts.IncludedSchemas, opts.ExcludedSchemas, opts.IncludedRelations, opts.ExcludedRelations)

	// Extract out the setval calls for each SEQUENCE object
	var sequenceValueStatements []toc.StatementWithType
	statements := GetRestoreMetadataStatementsFiltered("predata", metadataFilename, []string{toc.OBJ_SEQUENCE}, []string{}, filters)
	re := regexp.MustCompile(`SELECT pg_catalog.setval\(.*`)
	for _, statement := range statements {
		matches := re.FindStringSubmatch(statement.Statement)
		if len(matches) == 1 {
			statement.Statement = matches[0]
			sequenceValueStatements = append(sequenceValueStatements, statement)
		}
	}

	numErrors := int32(0)
	if len(sequenceValueStatements) == 0 {
		gplog.Verbose("No sequence values to restore")
	} else {
		progressBar := utils.NewProgressBar(len(sequenceValueStatements), "Sequence values restored: ", utils.PB_VERBOSE)
		progressBar.Start()
		numErrors = ExecuteRestoreMetadataStatements(sequenceValueStatements, "Sequence values", progressBar, utils.PB_VERBOSE, true)
		progressBar.Finish()
	}

	if wasTerminated {
		gplog.Info("Sequence values restore incomplete")
	} else if numErrors > 0 {
		gplog.Info("Sequence values restore completed with failures")
	} else {
		gplog.Info("Sequence values restore complete")
	}
}

func editStatementsRedirectSchema(statements []toc.StatementWithType, redirectSchema string) {
	if redirectSchema == "" {
		return
	}

	schemaMatch := `(?:".+?"|[^.]+?)` // matches either an unquoted schema with no dots or a quoted schema containing dots
	// This expression matches a GRANT or REVOKE statement on any object and captures the old schema name
	permissionsRE := regexp.MustCompile(fmt.Sprintf(`(?m)(^(?:REVOKE|GRANT) .+ ON .+?) (%s)((\..+)? (?:FROM|TO) .+)`, schemaMatch))
	// This expression matches an ATTACH PARTITION statement and captures both the parent and child schema names
	attachRE := regexp.MustCompile(fmt.Sprintf(`(ALTER TABLE(?: ONLY)?) (%[1]s)(\..+ ATTACH PARTITION) (%[1]s)(\..+)`, schemaMatch))
	for i := range statements {
		oldSchema := fmt.Sprintf("%s.", statements[i].Schema)
		newSchema := fmt.Sprintf("%s.", redirectSchema)
		statement := statements[i].Statement
		// Schemas themselves are a special case, since they lack the trailing dot.
		statement = strings.Replace(statement, fmt.Sprintf("CREATE SCHEMA %s", statements[i].Schema), fmt.Sprintf("CREATE SCHEMA %s", redirectSchema), 1)

		statements[i].Schema = redirectSchema
		if statements[i].ObjectType == toc.OBJ_SCHEMA {
			statements[i].Name = redirectSchema
		}
		replaced := false

		// Permission statements come in multi-line blocks, so ensure we replace all instances of the existing schema
		if strings.Contains(statement, "GRANT") || strings.Contains(statement, "REVOKE") {
			statement = permissionsRE.ReplaceAllString(statement, fmt.Sprintf("$1 %s$3", redirectSchema))
			replaced = true
		}

		// ALTER TABLE schema.root ATTACH PARTITION schema.leaf needs two schema replacements
		if connectionPool.Version.AtLeast("7") && statements[i].ObjectType == toc.OBJ_TABLE && statements[i].ReferenceObject != "" && strings.Contains(statement, "ATTACH") {
			statement = attachRE.ReplaceAllString(statement, fmt.Sprintf("$1 %[1]s$3 %[1]s$5", redirectSchema))
			replaced = true
		}

		// Only do a general replace if we haven't replaced anything yet, to avoid e.g. hitting a schema in a VIEW definition
		if !replaced {
			statement = strings.Replace(statement, oldSchema, newSchema, 1)
		}
		statements[i].Statement = statement

		// only postdata will have a reference object
		if statements[i].ReferenceObject != "" {
			statements[i].ReferenceObject = strings.Replace(statements[i].ReferenceObject, oldSchema, newSchema, 1)
		}
	}
}

func restoreData() (int, map[string][]toc.CoordinatorDataEntry) {
	if wasTerminated {
		return -1, nil
	}
	restorePlan := backupConfig.RestorePlan
	restorePlanEntries := make([]history.RestorePlanEntry, 0)
	if MustGetFlagBool(options.INCREMENTAL) {
		restorePlanEntries = append(restorePlanEntries,
			restorePlan[len(backupConfig.RestorePlan)-1])
	} else {
		for _, restorePlanEntry := range restorePlan {
			restorePlanEntries = append(restorePlanEntries, restorePlanEntry)
		}
	}

	totalTables := 0
	filteredDataEntries := make(map[string][]toc.CoordinatorDataEntry)
	for _, entry := range restorePlanEntries {
		fpInfo := GetBackupFPInfoForTimestamp(entry.Timestamp)
		tocfile := toc.NewTOC(fpInfo.GetTOCFilePath())
		restorePlanTableFQNs := entry.TableFQNs
		filteredDataEntriesForTimestamp := tocfile.GetDataEntriesMatching(opts.IncludedSchemas,
			opts.ExcludedSchemas, opts.IncludedRelations, opts.ExcludedRelations, restorePlanTableFQNs)
		filteredDataEntries[entry.Timestamp] = filteredDataEntriesForTimestamp
		totalTables += len(filteredDataEntriesForTimestamp)
	}
	numTuplesBars := 0
	if connectionPool.Version.AtLeast("7") {
		numTuplesBars = connectionPool.NumConns - 1
	}
	dataProgressBar := utils.NewMultiProgressBar(totalTables, "Tables restored: ", numTuplesBars, MustGetFlagBool(options.VERBOSE))
	defer dataProgressBar.Finish()
	err := dataProgressBar.Start()
	gplog.FatalOnError(err)

	gucStatements := setGUCsForConnection(nil, 0)
	numErrors := int32(0)
	for timestamp, entries := range filteredDataEntries {
		gplog.Verbose("Restoring data for %d tables from backup with timestamp: %s", len(entries), timestamp)
		numErrors = restoreDataFromTimestamp(GetBackupFPInfoForTimestamp(timestamp), entries, gucStatements, dataProgressBar)
	}

	dataProgressBar.Finish()
	if wasTerminated {
		gplog.Info("Data restore incomplete")
	} else if numErrors > 0 {
		gplog.Info("Data restore completed with failures")
	} else {
		gplog.Info("Data restore complete")
	}

	return totalTables, filteredDataEntries
}

func restorePostdata(metadataFilename string) {
	if wasTerminated {
		return
	}
	gplog.Info("Restoring post-data metadata")

	filters := NewFilters(opts.IncludedSchemas, opts.ExcludedSchemas, opts.IncludedRelations, opts.ExcludedRelations)

	statements := GetRestoreMetadataStatementsFiltered("postdata", metadataFilename, []string{}, []string{}, filters)
	editStatementsRedirectSchema(statements, opts.RedirectSchema)
	firstBatch, secondBatch, thirdBatch := BatchPostdataStatements(statements)
	progressBar := utils.NewProgressBar(len(statements), "Post-data objects restored: ", utils.PB_VERBOSE)
	progressBar.Start()

	numErrors := ExecuteRestoreMetadataStatements(firstBatch, "", progressBar, utils.PB_VERBOSE, connectionPool.NumConns > 2)
	numErrors += ExecuteRestoreMetadataStatements(secondBatch, "", progressBar, utils.PB_VERBOSE, connectionPool.NumConns > 2)
	numErrors += ExecuteRestoreMetadataStatements(thirdBatch, "", progressBar, utils.PB_VERBOSE, connectionPool.NumConns > 2)
	progressBar.Finish()

	if wasTerminated {
		gplog.Info("Post-data metadata restore incomplete")
	} else if numErrors > 0 {
		gplog.Info("Post-data metadata restore completed with failures")
	} else {
		gplog.Info("Post-data metadata restore complete")
	}
}

func restoreStatistics() {
	if wasTerminated {
		return
	}
	statisticsFilename := globalFPInfo.GetStatisticsFilePath()
	gplog.Info("Restoring query planner statistics from %s", statisticsFilename)

	filters := NewFilters(opts.IncludedSchemas, opts.ExcludedSchemas, opts.IncludedRelations, opts.ExcludedRelations)

	statements := GetRestoreMetadataStatementsFiltered("statistics", statisticsFilename, []string{}, []string{}, filters)
	editStatementsRedirectSchema(statements, opts.RedirectSchema)
	numErrors := ExecuteRestoreMetadataStatements(statements, "Table statistics", nil, utils.PB_VERBOSE, false)

	if numErrors > 0 {
		gplog.Info("Query planner statistics restore completed with failures")
	} else {
		gplog.Info("Query planner statistics restore complete")
	}
}

func runAnalyze(filteredDataEntries map[string][]toc.CoordinatorDataEntry) {
	if wasTerminated {
		return
	}
	gplog.Info("Running ANALYZE on restored tables")

	var analyzeStatements []toc.StatementWithType
	for _, dataEntries := range filteredDataEntries {
		for _, entry := range dataEntries {
			tableSchema := entry.Schema
			if opts.RedirectSchema != "" {
				tableSchema = opts.RedirectSchema
			}
			tableFQN := utils.MakeFQN(tableSchema, entry.Name)
			analyzeCommand := fmt.Sprintf("ANALYZE %s", tableFQN)

			newAnalyzeStatement := toc.StatementWithType{
				Schema:    tableSchema,
				Name:      entry.Name,
				Statement: analyzeCommand,
				Tier:      []uint32{0, 0},
			}
			analyzeStatements = append(analyzeStatements, newAnalyzeStatement)
		}
	}

	// Only GPDB 5+ has leaf partition stats merged up to the root
	// automatically. Against GPDB 4.3, we must extract the root partitions
	// from the leaf partition info and run ANALYZE ROOTPARTITION on the root
	// partitions. These particular ANALYZE ROOTPARTITION statements should run
	// last so add them to the end of the analyzeStatements list.
	if connectionPool.Version.Is("4") {
		// Create root partition set
		partitionRootSet := map[string]toc.StatementWithType{}
		for _, dataEntries := range filteredDataEntries {
			for _, entry := range dataEntries {
				if entry.PartitionRoot != "" {
					tableSchema := entry.Schema
					if opts.RedirectSchema != "" {
						tableSchema = opts.RedirectSchema
					}
					rootFQN := utils.MakeFQN(tableSchema, entry.PartitionRoot)
					analyzeCommand := fmt.Sprintf("ANALYZE ROOTPARTITION %s", rootFQN)
					rootStatement := toc.StatementWithType{
						Schema:    tableSchema,
						Name:      entry.PartitionRoot,
						Statement: analyzeCommand,
						Tier:      []uint32{0, 0},
					}

					// use analyze command as map key, since struct isn't a valid key but statement
					// encodes everything in it anyway
					if _, ok := partitionRootSet[analyzeCommand]; !ok {
						partitionRootSet[analyzeCommand] = rootStatement
					}
				}
			}
		}

		for _, rootAnalyzeStatement := range partitionRootSet {
			analyzeStatements = append(analyzeStatements, rootAnalyzeStatement)
		}
	}

	progressBar := utils.NewProgressBar(len(analyzeStatements), "Tables analyzed: ", utils.PB_VERBOSE)
	progressBar.Start()
	numErrors := ExecuteStatements(analyzeStatements, progressBar, connectionPool.NumConns > 2)
	progressBar.Finish()

	if wasTerminated {
		gplog.Info("ANALYZE on restored tables incomplete")
	} else if numErrors > 0 {
		gplog.Info("ANALYZE on restored tables completed with failures")
	} else {
		gplog.Info("ANALYZE on restored tables complete")
	}
}

func DoTeardown() {
	restoreFailed := false
	defer func() {
		DoCleanup(restoreFailed)

		errorCode := gplog.GetErrorCode()
		if errorCode == 0 {
			gplog.Info("Restore completed successfully")
		}
		os.Exit(errorCode)

	}()

	errStr := ""
	if err := recover(); err != nil {
		// Check if gplog.Fatal did not cause the panic
		if gplog.GetErrorCode() != 2 {
			gplog.Error(fmt.Sprintf("%v: %s", err, debug.Stack()))
			gplog.SetErrorCode(2)
		} else {
			errStr = fmt.Sprintf("%+v", err)
		}
		restoreFailed = true
	}
	if wasTerminated {
		/*
		 * Don't print an error if the restore was canceled, as the signal handler
		 * will take care of cleanup and return codes.  Just wait until the signal
		 * handler's DoCleanup completes so the main goroutine doesn't exit while
		 * cleanup is still in progress.
		 */
		CleanupGroup.Wait()
		restoreFailed = true
		return
	}
	if errStr != "" {
		fmt.Println(errStr)
	}
	errMsg := report.ParseErrorMessage(errStr)

	if globalFPInfo.Timestamp != "" {
		_, statErr := os.Stat(globalFPInfo.GetDirForContent(-1))
		if statErr != nil { // Even if this isn't os.IsNotExist, don't try to write a report file in case of further errors
			return
		}
		reportFilename := globalFPInfo.GetRestoreReportFilePath(restoreStartTime)
		origSize, destSize, _ := GetResizeClusterInfo()
		report.WriteRestoreReportFile(reportFilename, globalFPInfo.Timestamp, restoreStartTime, connectionPool, version, origSize, destSize, errMsg)
		report.EmailReport(globalCluster, globalFPInfo.Timestamp, reportFilename, "gprestore", !restoreFailed, backupConfig.DatabaseName)
		if pluginConfig != nil {
			pluginConfig.CleanupPluginForRestore(globalCluster, globalFPInfo)
			pluginConfig.DeletePluginConfigWhenEncrypting(globalCluster)
		}
		if len(errorTablesMetadata) > 0 {
			// tables with metadata errors
			writeErrorTables(true)
		}
		if len(errorTablesData) > 0 {
			// tables with data errors
			writeErrorTables(false)
		}
	}
}

func writeErrorTables(isMetadata bool) {
	var errorTables *map[string]Empty
	var errorFilename string

	if isMetadata == true {
		errorFilename = globalFPInfo.GetErrorTablesMetadataFilePath(restoreStartTime)
		errorTables = &errorTablesMetadata
		gplog.Verbose("Logging error tables during metadata restore in %s", errorFilename)
	} else {
		errorFilename = globalFPInfo.GetErrorTablesDataFilePath(restoreStartTime)
		errorTables = &errorTablesData
		gplog.Verbose("Logging error tables during data restore in %s", errorFilename)
	}

	errorFile, err := os.OpenFile(errorFilename, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	// We don't Fatal here, or in the other error checks below, as we don't want the restore
	// to be considered a failure if only this file errors out; there may be legitimate
	// reasons for it to fail, such as the files being restored from a read-only filesystem.
	if err != nil {
		gplog.Warn("Unable to open error table file %s, skipping error report creation", errorFilename)
		return
	}
	errorWriter := bufio.NewWriter(errorFile)
	start := true
	for table := range *errorTables {
		if start == false {
			_, _ = errorWriter.WriteString("\n")
		} else {
			start = false
		}
		_, _ = errorWriter.WriteString(table)
	}
	err = errorWriter.Flush()
	err = errorFile.Close()
	if err != nil {
		gplog.Warn("Could not close error tables file: %v", err)
		return
	}
	err = os.Chmod(errorFilename, 0444)
	if err != nil {
		gplog.Warn("Could not modify permissions of error tables file: %v", err)
	}
}

func DoCleanup(restoreFailed bool) {
	defer func() {
		if err := recover(); err != nil {
			gplog.Warn("Encountered error during cleanup: %+v", err)
		}
		gplog.Verbose("Cleanup complete")
		CleanupGroup.Done()
	}()

	gplog.Verbose("Beginning cleanup")
	if backupConfig != nil && backupConfig.SingleDataFile {
		fpInfoList := GetBackupFPInfoListFromRestorePlan()
		for _, fpInfo := range fpInfoList {
			// Copy sessions must be terminated before cleaning up gpbackup_helper processes to avoid a potential deadlock
			// If the terminate query is sent via a connection with an active COPY command, and the COPY's pipe is cleaned up, the COPY query will hang.
			// This results in the DoCleanup function passed to the signal handler to never return, blocking the os.Exit call
			if wasTerminated { // These should all end on their own in a successful restore
				utils.TerminateHangingCopySessions(connectionPool, fpInfo, fmt.Sprintf("gprestore_%s_%s", fpInfo.Timestamp, restoreStartTime))
			}
			if restoreFailed {
				utils.CleanUpSegmentHelperProcesses(globalCluster, fpInfo, "restore")
			}
			utils.CleanUpHelperFilesOnAllHosts(globalCluster, fpInfo)
		}
	}

	if connectionPool != nil {
		connectionPool.Close()
	}
}
