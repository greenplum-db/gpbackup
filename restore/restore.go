package restore

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpbackup/utils"
	"github.com/spf13/cobra"
	"gopkg.in/cheggaaa/pb.v1"

	"github.com/pkg/errors"
	"github.com/spf13/pflag"
)

/*
 * We define and initialize flags separately to avoid import conflicts in tests.
 * The flag variables, and setter functions for them, are in global_variables.go.
 */
func initializeFlags(cmd *cobra.Command) {
	SetFlagDefaults(cmd.Flags())

	_ = cmd.MarkFlagRequired(utils.TIMESTAMP)

	cmdFlags = cmd.Flags()
}
func SetFlagDefaults(flagSet *pflag.FlagSet) {
	flagSet.String(utils.BACKUP_DIR, "", "The absolute path of the directory in which the backup files to be restored are located")
	flagSet.Bool(utils.CREATE_DB, false, "Create the database before metadata restore")
	flagSet.Bool(utils.DATA_ONLY, false, "Only restore data, do not restore metadata")
	flagSet.Bool(utils.DEBUG, false, "Print verbose and debug log messages")
	flagSet.StringSlice(utils.EXCLUDE_SCHEMA, []string{}, "Restore all metadata except objects in the specified schema(s). --exclude-schema can be specified multiple times.")
	flagSet.StringSlice(utils.EXCLUDE_RELATION, []string{}, "Restore all metadata except the specified relation(s). --exclude-table can be specified multiple times.")
	flagSet.String(utils.EXCLUDE_RELATION_FILE, "", "A file containing a list of fully-qualified relation(s) that will not be restored")
	flagSet.Bool("help", false, "Help for gprestore")
	flagSet.StringSlice(utils.INCLUDE_SCHEMA, []string{}, "Restore only the specified schema(s). --include-schema can be specified multiple times.")
	flagSet.StringSlice(utils.INCLUDE_RELATION, []string{}, "Restore only the specified relation(s). --include-table can be specified multiple times.")
	flagSet.String(utils.INCLUDE_RELATION_FILE, "", "A file containing a list of fully-qualified relation(s) that will be restored")
	flagSet.Bool(utils.METADATA_ONLY, false, "Only restore metadata, do not restore data")
	flagSet.Int(utils.JOBS, 1, "Number of parallel connections to use when restoring table data and post-data")
	flagSet.Bool(utils.ON_ERROR_CONTINUE, false, "Log errors and continue restore, instead of exiting on first error")
	flagSet.String(utils.PLUGIN_CONFIG, "", "The configuration file to use for a plugin")
	flagSet.Bool("version", false, "Print version number and exit")
	flagSet.Bool(utils.QUIET, false, "Suppress non-warning, non-error log messages")
	flagSet.String(utils.REDIRECT_DB, "", "Restore to the specified database instead of the database that was backed up")
	flagSet.Bool(utils.WITH_GLOBALS, false, "Restore global metadata")
	flagSet.String(utils.TIMESTAMP, "", "The timestamp to be restored, in the format YYYYMMDDHHMMSS")
	flagSet.Bool(utils.VERBOSE, false, "Print verbose log messages")
	flagSet.Bool(utils.WITH_STATS, false, "Restore query plan statistics")
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
	utils.ValidateFullPath(MustGetFlagString(utils.BACKUP_DIR))
	utils.ValidateFullPath(MustGetFlagString(utils.PLUGIN_CONFIG))
	if !utils.IsValidTimestamp(MustGetFlagString(utils.TIMESTAMP)) {
		gplog.Fatal(errors.Errorf("Timestamp %s is invalid.  Timestamps must be in the format YYYYMMDDHHMMSS.", MustGetFlagString(utils.TIMESTAMP)), "")
	}
}

// This function handles setup that must be done after parsing flags.
func DoSetup() {
	SetLoggerVerbosity()
	restoreStartTime = utils.CurrentTimestamp()
	gplog.Info("Restore Key = %s", MustGetFlagString(utils.TIMESTAMP))

	InitializeConnection("postgres")
	segConfig := cluster.MustGetSegmentConfiguration(connectionPool)
	globalCluster = cluster.NewCluster(segConfig)
	segPrefix := utils.ParseSegPrefix(MustGetFlagString(utils.BACKUP_DIR), MustGetFlagString(utils.TIMESTAMP))
	globalFPInfo = utils.NewFilePathInfo(globalCluster, MustGetFlagString(utils.BACKUP_DIR), MustGetFlagString(utils.TIMESTAMP), segPrefix)

	// Get restore metadata from plugin
	if MustGetFlagString(utils.PLUGIN_CONFIG) != "" {
		RecoverMetadataFilesUsingPlugin()
	} else {
		InitializeBackupConfig()
	}

	BackupConfigurationValidation()
	metadataFilename := globalFPInfo.GetMetadataFilePath()
	if !backupConfig.DataOnly {
		gplog.Verbose("Metadata will be restored from %s", metadataFilename)
	}
	restoreDatabase := utils.UnquoteIdent(backupConfig.DatabaseName)
	if MustGetFlagString(utils.REDIRECT_DB) != "" {
		restoreDatabase = MustGetFlagString(utils.REDIRECT_DB)
	}
	ValidateDatabaseExistence(restoreDatabase, MustGetFlagBool(utils.CREATE_DB), backupConfig.IncludeTableFiltered || backupConfig.DataOnly)
	if MustGetFlagBool(utils.CREATE_DB) {
		createDatabase(metadataFilename)
	}
	InitializeConnection(restoreDatabase)

	if MustGetFlagBool(utils.WITH_GLOBALS) {
		restoreGlobal(metadataFilename)
	}

	/*
	 * We don't need to validate anything if we're creating the database; we
	 * should not error out for validation reasons once the restore database exists.
	 * For on-error-continue, we will see the same errors later when we try to run SQL,
	 * but since they will not stop the restore, it is not necessary to log them twice.
	 */
	if !MustGetFlagBool(utils.CREATE_DB) && !MustGetFlagBool(utils.ON_ERROR_CONTINUE) {
		ValidateRelationsInRestoreDatabase(connectionPool, MustGetFlagStringSlice(utils.INCLUDE_RELATION))
	}
}

func DoRestore() {
	gucStatements := setGUCsForConnection(nil, 0)
	metadataFilename := globalFPInfo.GetMetadataFilePath()
	isDataOnly := backupConfig.DataOnly || MustGetFlagBool(utils.DATA_ONLY)
	isMetadataOnly := backupConfig.MetadataOnly || MustGetFlagBool(utils.METADATA_ONLY)
	if !isDataOnly {
		restorePredata(metadataFilename)
	}

	if !isMetadataOnly {
		if MustGetFlagString(utils.PLUGIN_CONFIG) == "" {
			backupFileCount := 2 // 1 for the actual data file, 1 for the segment TOC file
			if !backupConfig.SingleDataFile {
				backupFileCount = len(globalTOC.DataEntries)
			}
			VerifyBackupFileCountOnSegments(backupFileCount)
		}
		restoreData(GetBackupFPInfoListFromRestorePlan(), gucStatements)
	}

	if !isDataOnly {
		restorePostdata(metadataFilename)
	}

	if MustGetFlagBool(utils.WITH_STATS) && backupConfig.WithStatistics {
		restoreStatistics()
	}
}

func createDatabase(metadataFilename string) {
	objectTypes := []string{"SESSION GUCS", "DATABASE GUC", "DATABASE", "DATABASE METADATA"}
	gplog.Info("Creating database")
	statements := GetRestoreMetadataStatements("global", metadataFilename, objectTypes, []string{}, false, false)
	if MustGetFlagString(utils.REDIRECT_DB) != "" {
		redirectDBName := utils.QuoteIdent(connectionPool, MustGetFlagString(utils.REDIRECT_DB))
		statements = utils.SubstituteRedirectDatabaseInStatements(statements, backupConfig.DatabaseName, redirectDBName)
	}
	ExecuteRestoreMetadataStatements(statements, "", nil, utils.PB_NONE, false)
	gplog.Info("Database creation complete")
}

func restoreGlobal(metadataFilename string) {
	objectTypes := []string{"SESSION GUCS", "DATABASE GUC", "DATABASE METADATA", "RESOURCE QUEUE", "RESOURCE GROUP", "ROLE", "ROLE GRANT", "TABLESPACE"}
	gplog.Info("Restoring global metadata")
	statements := GetRestoreMetadataStatements("global", metadataFilename, objectTypes, []string{}, false, false)
	if MustGetFlagString(utils.REDIRECT_DB) != "" {
		redirectDBName := utils.QuoteIdent(connectionPool, MustGetFlagString(utils.REDIRECT_DB))
		statements = utils.SubstituteRedirectDatabaseInStatements(statements, backupConfig.DatabaseName, redirectDBName)
	}
	statements = utils.RemoveActiveRole(connectionPool.User, statements)
	ExecuteRestoreMetadataStatements(statements, "Global objects", nil, utils.PB_VERBOSE, false)
	gplog.Info("Global database metadata restore complete")
}

func restorePredata(metadataFilename string) {
	if wasTerminated {
		return
	}
	gplog.Info("Restoring pre-data metadata")

	schemaStatements := GetRestoreMetadataStatements("predata", metadataFilename, []string{"SCHEMA"}, []string{}, true, false)
	statements := GetRestoreMetadataStatements("predata", metadataFilename, []string{}, []string{"SCHEMA"}, true, true)

	progressBar := utils.NewProgressBar(len(schemaStatements)+len(statements), "Pre-data objects restored: ", utils.PB_VERBOSE)
	progressBar.Start()

	restoreSchemas(schemaStatements, progressBar)
	ExecuteRestoreMetadataStatements(statements, "Pre-data objects", progressBar, utils.PB_VERBOSE, false)

	progressBar.Finish()
	if wasTerminated {
		gplog.Info("Pre-data metadata restore complete")
	} else {
		gplog.Info("Pre-data metadata restore incomplete")
	}
}

func restoreData(fpInfoList []utils.FilePathInfo, gucStatements []utils.StatementWithType) {
	if wasTerminated {
		return
	}
	latestRestorePlan := backupConfig.RestorePlan

	totalTables := 0
	filteredDataEntries := make([][]utils.MasterDataEntry, 0)
	for i, fpInfo := range fpInfoList {
		tocFilename := fpInfo.GetTOCFilePath()
		toc := utils.NewTOC(tocFilename)
		restorePlanTableFQNs := latestRestorePlan[i].TableFQNs
		filteredDataEntriesForTimestamp := toc.GetDataEntriesMatching(MustGetFlagStringSlice(utils.INCLUDE_SCHEMA),
			MustGetFlagStringSlice(utils.EXCLUDE_SCHEMA), MustGetFlagStringSlice(utils.INCLUDE_RELATION),
			MustGetFlagStringSlice(utils.EXCLUDE_RELATION), restorePlanTableFQNs)
		filteredDataEntries = append(filteredDataEntries, filteredDataEntriesForTimestamp)

		totalTables += len(filteredDataEntriesForTimestamp)
	}
	dataProgressBar := utils.NewProgressBar(totalTables, "Tables restored: ", utils.PB_INFO)
	dataProgressBar.Start()

	for i, fpInfo := range fpInfoList {
		gplog.Verbose("Restoring data from backup with timestamp: %s", fpInfo.Timestamp)
		restoreDataFromTimestamp(fpInfo, filteredDataEntries[i], gucStatements, dataProgressBar)
	}

	dataProgressBar.Finish()
	if wasTerminated {
		gplog.Info("Data restore incomplete")
	} else {
		gplog.Info("Data restore complete")
	}
}

func restoreDataFromTimestamp(fpInfo utils.FilePathInfo, dataEntries []utils.MasterDataEntry,
	gucStatements []utils.StatementWithType, dataProgressBar utils.ProgressBar) {
	if len(dataEntries) == 0 {
		gplog.Verbose("No data to restore for timestamp = %s", fpInfo.Timestamp)
		return
	}

	if backupConfig.SingleDataFile {
		gplog.Verbose("Initializing pipes and gpbackup_helper on segments for single data file restore")
		utils.VerifyHelperVersionOnSegments(version, globalCluster)
		filteredOids := make([]string, len(dataEntries))
		for i, entry := range dataEntries {
			filteredOids[i] = fmt.Sprintf("%d", entry.Oid)
		}
		utils.WriteOidListToSegments(filteredOids, globalCluster, fpInfo)
		firstOid := fmt.Sprintf("%d", dataEntries[0].Oid)
		utils.CreateFirstSegmentPipeOnAllHosts(firstOid, globalCluster, fpInfo)
		if wasTerminated {
			return
		}
		utils.StartAgent(globalCluster, fpInfo, "--restore-agent", MustGetFlagString(utils.PLUGIN_CONFIG), "")
	}
	/*
	 * We break when an interrupt is received and rely on
	 * TerminateHangingCopySessions to kill any COPY
	 * statements in progress if they don't finish on their own.
	 */
	var tableNum uint32 = 1
	tasks := make(chan utils.MasterDataEntry, len(dataEntries))
	var workerPool sync.WaitGroup
	var fatalErr error
	var numErrors int32
	for i := 0; i < connectionPool.NumConns; i++ {
		workerPool.Add(1)
		go func(whichConn int) {
			defer workerPool.Done()
			setGUCsForConnection(gucStatements, whichConn)
			for entry := range tasks {
				if wasTerminated || fatalErr != nil {
					dataProgressBar.(*pb.ProgressBar).NotPrint = true
					return
				}
				err := restoreSingleTableData(&fpInfo, entry, tableNum, len(dataEntries), whichConn)
				if err != nil {
					if MustGetFlagBool(utils.ON_ERROR_CONTINUE) {
						gplog.Verbose(err.Error())
						atomic.AddInt32(&numErrors, 1)
					} else {
						fatalErr = err
					}
				}
				atomic.AddUint32(&tableNum, 1)
				dataProgressBar.Increment()
			}
		}(i)
	}
	for _, entry := range dataEntries {
		tasks <- entry
	}
	close(tasks)
	workerPool.Wait()
	if fatalErr != nil {
		gplog.Fatal(fatalErr, "")
	} else if numErrors > 0 {
		gplog.Error("Encountered %d errors during table data restore; see log file %s for a list of table errors.", numErrors, gplog.GetLogFilePath())
	}
	err := CheckAgentErrorsOnSegments()
	if err != nil {
		errMsg := "Error restoring data for one or more tables"
		if MustGetFlagBool(utils.ON_ERROR_CONTINUE) {
			gplog.Error("%s: %v", errMsg, err)
		} else {
			gplog.Fatal(err, errMsg)
		}
	}
}

func restorePostdata(metadataFilename string) {
	if wasTerminated {
		return
	}
	gplog.Info("Restoring post-data metadata")
	statements := GetRestoreMetadataStatements("postdata", metadataFilename, []string{}, []string{}, true, true)
	firstBatch, secondBatch := BatchPostdataStatements(statements)
	progressBar := utils.NewProgressBar(len(statements), "Post-data objects restored: ", utils.PB_VERBOSE)
	progressBar.Start()
	ExecuteRestoreMetadataStatements(firstBatch, "", progressBar, utils.PB_VERBOSE, connectionPool.NumConns > 1)
	ExecuteRestoreMetadataStatements(secondBatch, "", progressBar, utils.PB_VERBOSE, connectionPool.NumConns > 1)
	progressBar.Finish()
	if wasTerminated {
		gplog.Info("Post-data metadata restore complete")
	} else {
		gplog.Info("Post-data metadata restore incomplete")
	}
}

func restoreStatistics() {
	if wasTerminated {
		return
	}
	statisticsFilename := globalFPInfo.GetStatisticsFilePath()
	gplog.Info("Restoring query planner statistics from %s", statisticsFilename)
	statements := GetRestoreMetadataStatements("statistics", statisticsFilename, []string{}, []string{}, true, false)
	ExecuteRestoreMetadataStatements(statements, "Table statistics", nil, utils.PB_VERBOSE, false)
	gplog.Info("Query planner statistics restore complete")
}

func DoTeardown() {
	defer func() {
		DoCleanup()

		errorCode := gplog.GetErrorCode()
		if errorCode == 0 {
			gplog.Info("Restore completed successfully")
		}
		os.Exit(errorCode)

	}()

	errStr := ""
	if err := recover(); err != nil {
		errStr = fmt.Sprintf("%v", err)
	}
	if wasTerminated {
		/*
		 * Don't print an error if the restore was canceled, as the signal handler
		 * will take care of cleanup and return codes.  Just wait until the signal
		 * handler's DoCleanup completes so the main goroutine doesn't exit while
		 * cleanup is still in progress.
		 */
		CleanupGroup.Wait()
		return
	}
	if errStr != "" {
		fmt.Println(errStr)
	}
	errMsg := utils.ParseErrorMessage(errStr)

	if globalFPInfo.Timestamp != "" {
		_, statErr := os.Stat(globalFPInfo.GetDirForContent(-1))
		if statErr != nil { // Even if this isn't os.IsNotExist, don't try to write a report file in case of further errors
			return
		}

		reportFilename := globalFPInfo.GetRestoreReportFilePath(restoreStartTime)
		utils.WriteRestoreReportFile(reportFilename, globalFPInfo.Timestamp, restoreStartTime, connectionPool, version, errMsg)
		utils.EmailReport(globalCluster, globalFPInfo.Timestamp, reportFilename, "gprestore")
		if pluginConfig != nil {
			pluginConfig.CleanupPluginForRestore(globalCluster, globalFPInfo)
		}
	}
}

func DoCleanup() {
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
			utils.CleanUpSegmentHelperProcesses(globalCluster, fpInfo, "restore")
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

func GetVersion() string {
	return version
}
