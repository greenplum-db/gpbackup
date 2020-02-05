package restore

import (
	"bufio"
	"fmt"
	"os"
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
	"github.com/spf13/pflag"
)

/*
 * We define and initialize flags separately to avoid import conflicts in tests.
 * The flag variables, and setter functions for them, are in global_variables.go.
 */
func initializeFlags(cmd *cobra.Command) {
	SetFlagDefaults(cmd.Flags())

	_ = cmd.MarkFlagRequired(options.TIMESTAMP)

	cmdFlags = cmd.Flags()
}
func SetFlagDefaults(flagSet *pflag.FlagSet) {
	flagSet.String(options.BACKUP_DIR, "", "The absolute path of the directory in which the backup files to be restored are located")
	flagSet.Bool(options.CREATE_DB, false, "Create the database before metadata restore")
	flagSet.Bool(options.DATA_ONLY, false, "Only restore data, do not restore metadata")
	flagSet.Bool(options.DEBUG, false, "Print verbose and debug log messages")
	flagSet.StringArray(options.EXCLUDE_SCHEMA, []string{}, "Restore all metadata except objects in the specified schema(s). --exclude-schema can be specified multiple times.")
	flagSet.String(options.EXCLUDE_SCHEMA_FILE, "", "A file containing a list of schemas that will not be restored")
	flagSet.StringArray(options.EXCLUDE_RELATION, []string{}, "Restore all metadata except the specified relation(s). --exclude-table can be specified multiple times.")
	flagSet.String(options.EXCLUDE_RELATION_FILE, "", "A file containing a list of fully-qualified relation(s) that will not be restored")
	flagSet.Bool("help", false, "Help for gprestore")
	flagSet.StringArray(options.INCLUDE_SCHEMA, []string{}, "Restore only the specified schema(s). --include-schema can be specified multiple times.")
	flagSet.String(options.INCLUDE_SCHEMA_FILE, "", "A file containing a list of schemas that will be restored")
	flagSet.StringArray(options.INCLUDE_RELATION, []string{}, "Restore only the specified relation(s). --include-table can be specified multiple times.")
	flagSet.String(options.INCLUDE_RELATION_FILE, "", "A file containing a list of fully-qualified relation(s) that will be restored")
	flagSet.Bool(options.INCREMENTAL, false, "Only restore data for all heap tables and only AO tables that have been modified since the last backup")
	flagSet.Bool(options.METADATA_ONLY, false, "Only restore metadata, do not restore data")
	flagSet.Int(options.JOBS, 1, "Number of parallel connections to use when restoring table data and post-data")
	flagSet.Bool(options.ON_ERROR_CONTINUE, false, "Log errors and continue restore, instead of exiting on first error")
	flagSet.String(options.PLUGIN_CONFIG, "", "The configuration file to use for a plugin")
	flagSet.Bool("version", false, "Print version number and exit")
	flagSet.Bool(options.QUIET, false, "Suppress non-warning, non-error log messages")
	flagSet.String(options.REDIRECT_DB, "", "Restore to the specified database instead of the database that was backed up")
	flagSet.Bool(options.WITH_GLOBALS, false, "Restore global metadata")
	flagSet.String(options.TIMESTAMP, "", "The timestamp to be restored, in the format YYYYMMDDHHMMSS")
	flagSet.Bool(options.VERBOSE, false, "Print verbose log messages")
	flagSet.Bool(options.WITH_STATS, false, "Restore query plan statistics")
	flagSet.Bool(options.LEAF_PARTITION_DATA, false, "For partition tables, create one data file per leaf partition instead of one data file for the whole table")
	_ = flagSet.MarkHidden(options.LEAF_PARTITION_DATA)
}

// This function handles setup that can be done before parsing flags.
func DoInit(cmd *cobra.Command) {
	CleanupGroup = &sync.WaitGroup{}
	CleanupGroup.Add(1)
	gplog.InitializeLogging("gprestore", "")
	initializeFlags(cmd)
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
	if !filepath.IsValidTimestamp(MustGetFlagString(options.TIMESTAMP)) {
		gplog.Fatal(errors.Errorf("Timestamp %s is invalid.  Timestamps must be in the format YYYYMMDDHHMMSS.", MustGetFlagString(options.TIMESTAMP)), "")
	}
}

// This function handles setup that must be done after parsing flags.
func DoSetup() {
	SetLoggerVerbosity()
	gplog.Verbose("Restore Command: %s", os.Args)

	utils.CheckGpexpandRunning(utils.RestorePreventedByGpexpandMessage)
	restoreStartTime = history.CurrentTimestamp()
	gplog.Info("Restore Key = %s", MustGetFlagString(options.TIMESTAMP))

	CreateConnectionPool("postgres")

	_, err := options.NewOptions(cmdFlags)
	gplog.FatalOnError(err)

	segConfig := cluster.MustGetSegmentConfiguration(connectionPool)
	globalCluster = cluster.NewCluster(segConfig)
	segPrefix := filepath.ParseSegPrefix(MustGetFlagString(options.BACKUP_DIR), MustGetFlagString(options.TIMESTAMP))
	globalFPInfo = filepath.NewFilePathInfo(globalCluster, MustGetFlagString(options.BACKUP_DIR), MustGetFlagString(options.TIMESTAMP), segPrefix)

	// Get restore metadata from plugin
	if MustGetFlagString(options.PLUGIN_CONFIG) != "" {
		RecoverMetadataFilesUsingPlugin()
	} else {
		InitializeBackupConfig()
	}

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
	InitializeConnectionPool(unquotedRestoreDatabase)

	/*
	 * We don't need to validate anything if we're creating the database; we
	 * should not error out for validation reasons once the restore database exists.
	 * For on-error-continue, we will see the same errors later when we try to run SQL,
	 * but since they will not stop the restore, it is not necessary to log them twice.
	 */
	if !MustGetFlagBool(options.CREATE_DB) && !MustGetFlagBool(options.ON_ERROR_CONTINUE) && !MustGetFlagBool(options.INCREMENTAL) {
		relationsToRestore := GenerateRestoreRelationList()
		ValidateRelationsInRestoreDatabase(connectionPool, relationsToRestore)
	}
}

func DoRestore() {
	metadataFilename := globalFPInfo.GetMetadataFilePath()
	isDataOnly := backupConfig.DataOnly || MustGetFlagBool(options.DATA_ONLY)
	isMetadataOnly := backupConfig.MetadataOnly || MustGetFlagBool(options.METADATA_ONLY)

	if !isDataOnly {
		restorePredata(metadataFilename)
	}

	if !isMetadataOnly {
		if MustGetFlagString(options.PLUGIN_CONFIG) == "" {
			backupFileCount := 2 // 1 for the actual data file, 1 for the segment TOC file
			if !backupConfig.SingleDataFile {
				backupFileCount = len(globalTOC.DataEntries)
			}
			VerifyBackupFileCountOnSegments(backupFileCount)
		}
		restoreData()
	}

	if !isDataOnly {
		restorePostdata(metadataFilename)
	}

	if MustGetFlagBool(options.WITH_STATS) && backupConfig.WithStatistics {
		restoreStatistics()
	}
}

func createDatabase(metadataFilename string) {
	objectTypes := []string{"SESSION GUCS", "DATABASE GUC", "DATABASE", "DATABASE METADATA"}
	dbName := backupConfig.DatabaseName
	gplog.Info("Creating database")
	statements := GetRestoreMetadataStatements("global", metadataFilename, objectTypes, []string{})
	if MustGetFlagString(options.REDIRECT_DB) != "" {
		quotedDBName := utils.QuoteIdent(connectionPool, MustGetFlagString(options.REDIRECT_DB))
		dbName = quotedDBName
		statements = toc.SubstituteRedirectDatabaseInStatements(statements, backupConfig.DatabaseName, quotedDBName)
	}
	ExecuteRestoreMetadataStatements(statements, "", nil, utils.PB_NONE, false)
	gplog.Info("Database creation complete for: %s", dbName)
}

func restoreGlobal(metadataFilename string) {
	objectTypes := []string{"SESSION GUCS", "DATABASE GUC", "DATABASE METADATA", "RESOURCE QUEUE", "RESOURCE GROUP", "ROLE", "ROLE GUCS", "ROLE GRANT", "TABLESPACE"}
	if MustGetFlagBool(options.CREATE_DB) {
		objectTypes = append(objectTypes, "DATABASE")
	}
	gplog.Info("Restoring global metadata")
	statements := GetRestoreMetadataStatements("global", metadataFilename, objectTypes, []string{})
	if MustGetFlagString(options.REDIRECT_DB) != "" {
		quotedDBName := utils.QuoteIdent(connectionPool, MustGetFlagString(options.REDIRECT_DB))
		statements = toc.SubstituteRedirectDatabaseInStatements(statements, backupConfig.DatabaseName, quotedDBName)
	}
	statements = toc.RemoveActiveRole(connectionPool.User, statements)
	ExecuteRestoreMetadataStatements(statements, "Global objects", nil, utils.PB_VERBOSE, false)
	gplog.Info("Global database metadata restore complete")
}

func restorePredata(metadataFilename string) {
	if wasTerminated {
		return
	}
	gplog.Info("Restoring pre-data metadata")

	var inSchemas, exSchemas, inRelations, exRelations []string
	inSchemasUserInput := MustGetFlagStringArray(options.INCLUDE_SCHEMA)
	exSchemasUserInput := MustGetFlagStringArray(options.EXCLUDE_SCHEMA)
	inRelationsUserInput := MustGetFlagStringArray(options.INCLUDE_RELATION)
	exRelationsUserInput := MustGetFlagStringArray(options.EXCLUDE_RELATION)

	if MustGetFlagBool(options.INCREMENTAL) {
		lastRestorePlanEntry := backupConfig.RestorePlan[len(backupConfig.RestorePlan)-1]
		tableFQNsToRestore := lastRestorePlanEntry.TableFQNs

		existingSchemas, err := GetExistingSchemas()
		gplog.FatalOnError(err)
		existingTableFQNs, err := GetExistingTableFQNs()
		gplog.FatalOnError(err)

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
			if utils.SchemaIsExcludedByUser(inSchemasUserInput, exSchemasUserInput, schemaName) {
				if !utils.Exists(schemasExcludedByUserInput, schemaName) {
					schemasExcludedByUserInput = append(schemasExcludedByUserInput, schemaName)
				}
				tablesExcludedByUserInput = append(tablesExcludedByUserInput, table)
				continue
			}

			if _, exists := existingTablesMap[table]; !exists {
				if utils.RelationIsExcludedByUser(inRelationsUserInput, exRelationsUserInput, table) {
					tablesExcludedByUserInput = append(tablesExcludedByUserInput, table)
				} else {
					if !utils.Exists(schemasToCreate, schemaName) {
						schemasToCreate = append(schemasToCreate, schemaName)
					}
					tableFQNsToCreate = append(tableFQNsToCreate, table)
				}
			}
		}

		if len(schemasToCreate) == 0 { // no new schemas
			exSchemas = append(existingSchemas, schemasExcludedByUserInput...)
		} else {
			inSchemas = schemasToCreate
		}

		if len(tableFQNsToCreate) == 0 { // no new tables
			exRelations = append(existingTableFQNs, tablesExcludedByUserInput...)
		} else {
			inRelations = tableFQNsToCreate
		}
	} else { // if not incremental restore - assume database is empty and just filter based on user input
		inSchemas = inSchemasUserInput
		exSchemas = exSchemasUserInput
		inRelations = inRelationsUserInput
		exRelations = exRelationsUserInput
	}
	
	filters := NewFilters(inSchemas, exSchemas, inRelations, exRelations)
	schemaStatements := GetRestoreMetadataStatementsFiltered("predata", metadataFilename, []string{"SCHEMA"}, []string{}, filters)
	statements := GetRestoreMetadataStatementsFiltered("predata", metadataFilename, []string{}, []string{"SCHEMA"}, filters)

	progressBar := utils.NewProgressBar(len(schemaStatements)+len(statements), "Pre-data objects restored: ", utils.PB_VERBOSE)
	progressBar.Start()

	RestoreSchemas(schemaStatements, progressBar)
	ExecuteRestoreMetadataStatements(statements, "Pre-data objects", progressBar, utils.PB_VERBOSE, false)

	progressBar.Finish()
	if wasTerminated {
		gplog.Info("Pre-data metadata restore incomplete")
	} else {
		gplog.Info("Pre-data metadata restore complete")
	}
}

func restoreData() {
	if wasTerminated {
		return
	}
	restorePlan := backupConfig.RestorePlan
	restorePlanEntries := make([]history.RestorePlanEntry, 0)
	if MustGetFlagBool(options.INCREMENTAL) {
		restorePlanEntries = append(restorePlanEntries, restorePlan[len(backupConfig.RestorePlan)-1])

	} else {
		for _, restorePlanEntry := range restorePlan {
			restorePlanEntries = append(restorePlanEntries, restorePlanEntry)
		}
	}

	totalTables := 0
	filteredDataEntries := make(map[string][]toc.MasterDataEntry)
	for _, entry := range restorePlanEntries {
		fpInfo := GetBackupFPInfoForTimestamp(entry.Timestamp)
		tocfile := toc.NewTOC(fpInfo.GetTOCFilePath())
		restorePlanTableFQNs := entry.TableFQNs
		filteredDataEntriesForTimestamp := tocfile.GetDataEntriesMatching(MustGetFlagStringArray(options.INCLUDE_SCHEMA),
			MustGetFlagStringArray(options.EXCLUDE_SCHEMA), options.MustGetFlagStringArray(cmdFlags, options.INCLUDE_RELATION),
			MustGetFlagStringArray(options.EXCLUDE_RELATION), restorePlanTableFQNs)
		filteredDataEntries[entry.Timestamp] = filteredDataEntriesForTimestamp
		totalTables += len(filteredDataEntriesForTimestamp)
	}
	dataProgressBar := utils.NewProgressBar(totalTables, "Tables restored: ", utils.PB_INFO)
	dataProgressBar.Start()

	gucStatements := setGUCsForConnection(nil, 0)
	for timestamp, entries := range filteredDataEntries {
		gplog.Verbose("Restoring data from backup with timestamp: %s", timestamp)
		if MustGetFlagBool(options.INCREMENTAL) {
			_ = TruncateTablesBeforeRestore(entries)
		}
		restoreDataFromTimestamp(GetBackupFPInfoForTimestamp(timestamp), entries, gucStatements, dataProgressBar)
	}

	dataProgressBar.Finish()
	if wasTerminated {
		gplog.Info("Data restore incomplete")
	} else {
		gplog.Info("Data restore complete")
	}
}

func restorePostdata(metadataFilename string) {
	if wasTerminated {
		return
	}
	gplog.Info("Restoring post-data metadata")

	inSchemas := MustGetFlagStringArray(options.INCLUDE_SCHEMA)
	exSchemas := MustGetFlagStringArray(options.EXCLUDE_SCHEMA)
	inRelations := MustGetFlagStringArray(options.INCLUDE_RELATION)
	exRelations := MustGetFlagStringArray(options.EXCLUDE_RELATION)
	filters := NewFilters(inSchemas, exSchemas, inRelations, exRelations)

	statements := GetRestoreMetadataStatementsFiltered("postdata", metadataFilename, []string{}, []string{}, filters)
	firstBatch, secondBatch := BatchPostdataStatements(statements)
	progressBar := utils.NewProgressBar(len(statements), "Post-data objects restored: ", utils.PB_VERBOSE)
	progressBar.Start()
	ExecuteRestoreMetadataStatements(firstBatch, "", progressBar, utils.PB_VERBOSE, connectionPool.NumConns > 1)
	ExecuteRestoreMetadataStatements(secondBatch, "", progressBar, utils.PB_VERBOSE, connectionPool.NumConns > 1)
	progressBar.Finish()
	if wasTerminated {
		gplog.Info("Post-data metadata restore incomplete")
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

	inSchemas := MustGetFlagStringArray(options.INCLUDE_SCHEMA)
	exSchemas := MustGetFlagStringArray(options.EXCLUDE_SCHEMA)
	inRelations := MustGetFlagStringArray(options.INCLUDE_RELATION)
	exRelations := MustGetFlagStringArray(options.EXCLUDE_RELATION)
	filters := NewFilters(inSchemas, exSchemas, inRelations, exRelations)

	statements := GetRestoreMetadataStatementsFiltered("statistics", statisticsFilename, []string{}, []string{}, filters)
	ExecuteRestoreMetadataStatements(statements, "Table statistics", nil, utils.PB_VERBOSE, false)
	gplog.Info("Query planner statistics restore complete")
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
			errStr = fmt.Sprintf("%v", err)
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
		report.WriteRestoreReportFile(reportFilename, globalFPInfo.Timestamp, restoreStartTime, connectionPool, version, errMsg)
		report.EmailReport(globalCluster, globalFPInfo.Timestamp, reportFilename, "gprestore")
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
	gplog.FatalOnError(err)
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
	gplog.FatalOnError(err)
	err = operating.System.Chmod(errorFilename, 0444)
	gplog.FatalOnError(err)
}

func DoCleanup(restoreFailed bool) {
	defer func() {
		if err := recover(); err != nil {
			gplog.Warn("Encountered error during cleanup: %v", err)
		}
		gplog.Verbose("Cleanup complete")
		CleanupGroup.Done()
	}()

	gplog.Verbose("Beginning cleanup")
	if backupConfig != nil && backupConfig.SingleDataFile {
		fpInfoList := GetBackupFPInfoListFromRestorePlan()
		for _, fpInfo := range fpInfoList {
			if restoreFailed {
				utils.CleanUpSegmentHelperProcesses(globalCluster, fpInfo, "restore")
			}
			utils.CleanUpHelperFilesOnAllHosts(globalCluster, fpInfo)
			if wasTerminated { // These should all end on their own in a successful restore
				utils.TerminateHangingCopySessions(connectionPool, fpInfo, "gprestore")
			}
		}
	}

	if connectionPool != nil {
		connectionPool.Close()
	}
}
