package restore

import (
	"flag"
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpbackup/utils"
	pb "gopkg.in/cheggaaa/pb.v1"

	"github.com/pkg/errors"
)

/*
 * We define and initialize flags separately to avoid import conflicts in tests.
 * The flag variables, and setter functions for them, are in global_variables.go.
 */
func initializeFlags() {
	backupDir = flag.String("backup-dir", "", "The absolute path of the directory in which the backup files to be restored are located")
	createDB = flag.Bool("create-db", false, "Create the database before metadata restore")
	debug = flag.Bool("debug", false, "Print verbose and debug log messages")
	flag.Var(&excludeSchemas, "exclude-schema", "Restore all metadata except objects in the specified schema(s). --exclude-schema can be specified multiple times.")
	flag.Var(&excludeTables, "exclude-table", "Restore all metadata except the specified table(s). --exclude-table can be specified multiple times.")
	excludeTableFile = flag.String("exclude-table-file", "", "A file containing a list of fully-qualified tables that will not be restored")
	flag.Var(&includeSchemas, "include-schema", "Restore only the specified schema(s). --include-schema can be specified multiple times.")
	flag.Var(&includeTables, "include-table", "Restore only the specified table(s). --include-table can be specified multiple times.")
	includeTableFile = flag.String("include-table-file", "", "A file containing a list of fully-qualified tables that will be restored")
	numJobs = flag.Int("jobs", 1, "Number of parallel connections to use when restoring table data and post-data")
	onErrorContinue = flag.Bool("on-error-continue", false, "Log errors and continue restore, instead of exiting on first error")
	pluginConfigFile = flag.String("plugin-config", "", "The configuration file to use for a plugin")
	printVersion = flag.Bool("version", false, "Print version number and exit")
	quiet = flag.Bool("quiet", false, "Suppress non-warning, non-error log messages")
	redirect = flag.String("redirect-db", "", "Restore to the specified database instead of the database that was backed up")
	restoreGlobals = flag.Bool("with-globals", false, "Restore global metadata")
	timestamp = flag.String("timestamp", "", "The timestamp to be restored, in the format YYYYMMDDHHMMSS")
	verbose = flag.Bool("verbose", false, "Print verbose log messages")
	withStats = flag.Bool("with-stats", false, "Restore query plan statistics")
}

// This function handles setup that can be done before parsing flags.
func DoInit() {
	CleanupGroup = &sync.WaitGroup{}
	CleanupGroup.Add(1)
	gplog.InitializeLogging("gprestore", "")
	initializeFlags()
	utils.InitializeSignalHandler(DoCleanup, "restore process", &wasTerminated)
}

/*
* This function handles argument parsing and validation, e.g. checking that a passed filename exists.
* It should only validate; initialization with any sort of side effects should go in DoInit or DoSetup.
 */
func DoValidation() {
	if len(os.Args) == 1 {
		flag.PrintDefaults()
		os.Exit(0)
	}
	flag.Parse()
	if *printVersion {
		fmt.Printf("gprestore %s\n", version)
		os.Exit(0)
	}
	ValidateFlagCombinations()
	utils.ValidateFullPath(*backupDir)
	utils.ValidateFullPath(*pluginConfigFile)
	if !utils.IsValidTimestamp(*timestamp) {
		gplog.Fatal(errors.Errorf("Timestamp %s is invalid.  Timestamps must be in the format YYYYMMDDHHMMSS.", *timestamp), "")
	}
}

// This function handles setup that must be done after parsing flags.
func DoSetup() {
	SetLoggerVerbosity()
	restoreStartTime = utils.CurrentTimestamp()
	gplog.Info("Restore Key = %s", *timestamp)

	InitializeConnection("postgres")
	segConfig := cluster.GetSegmentConfiguration(connection)
	globalCluster = cluster.NewCluster(segConfig)
	segPrefix := utils.ParseSegPrefix(*backupDir)
	globalFPInfo = utils.NewFilePathInfo(globalCluster.SegDirMap, *backupDir, *timestamp, segPrefix)

	// Get restore metadata from plugin
	if *pluginConfigFile != "" {
		RecoverMetadataFilesUsingPlugin()
	} else {
		InitializeBackupConfig()
	}

	BackupConfigurationValidation()
	metadataFilename := globalFPInfo.GetMetadataFilePath()
	if !backupConfig.DataOnly {
		gplog.Verbose("Metadata will be restored from %s", metadataFilename)
	}
	restoreDatabase := backupConfig.DatabaseName
	if *redirect != "" {
		restoreDatabase = *redirect
	}
	ValidateDatabaseExistence(restoreDatabase, *createDB, backupConfig.IncludeTableFiltered || backupConfig.DataOnly)
	if *createDB {
		createDatabase(metadataFilename)
	}
	InitializeConnection(restoreDatabase)

	if *restoreGlobals {
		restoreGlobal(metadataFilename)
	}

	/*
	 * We don't need to validate anything if we're creating the database; we
	 * should not error out for validation reasons once the restore database exists.
	 */
	if !*createDB {
		ValidateFilterTablesInRestoreDatabase(connection, includeTables)
	}
}

func DoRestore() {
	gucStatements := setGUCsForConnection(nil, 0)
	metadataFilename := globalFPInfo.GetMetadataFilePath()
	if !backupConfig.DataOnly {
		restorePredata(metadataFilename)
	}

	if !backupConfig.MetadataOnly {
		if *pluginConfigFile == "" {
			backupFileCount := 2 // 1 for the actual data file, 1 for the segment TOC file
			if !backupConfig.SingleDataFile {
				backupFileCount = len(globalTOC.DataEntries)
			}
			VerifyBackupFileCountOnSegments(backupFileCount)
		}
		restoreData(gucStatements)
	}

	if !backupConfig.DataOnly {
		restorePostdata(metadataFilename)
	}

	if *withStats && backupConfig.WithStatistics {
		restoreStatistics()
	}
}

func createDatabase(metadataFilename string) {
	objectTypes := []string{"SESSION GUCS", "DATABASE GUC", "DATABASE", "DATABASE METADATA"}
	gplog.Info("Creating database")
	statements := GetRestoreMetadataStatements("global", metadataFilename, objectTypes, []string{}, false, false)
	if *redirect != "" {
		statements = utils.SubstituteRedirectDatabaseInStatements(statements, backupConfig.DatabaseName, *redirect)
	}
	ExecuteRestoreMetadataStatements(statements, "", nil, utils.PB_NONE, false)
	gplog.Info("Database creation complete")
}

func restoreGlobal(metadataFilename string) {
	objectTypes := []string{"SESSION GUCS", "DATABASE GUC", "DATABASE METADATA", "RESOURCE QUEUE", "RESOURCE GROUP", "ROLE", "ROLE GRANT", "TABLESPACE"}
	gplog.Info("Restoring global metadata")
	statements := GetRestoreMetadataStatements("global", metadataFilename, objectTypes, []string{}, false, false)
	if *redirect != "" {
		statements = utils.SubstituteRedirectDatabaseInStatements(statements, backupConfig.DatabaseName, *redirect)
	}
	statements = utils.RemoveActiveRole(connection.User, statements)
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
	gplog.Info("Pre-data metadata restore complete")
}

func restoreData(gucStatements []utils.StatementWithType) {
	if wasTerminated {
		return
	}
	gplog.Info("Restoring data")
	filteredMasterDataEntries := globalTOC.GetDataEntriesMatching(includeSchemas, excludeSchemas, includeTables, excludeTables)
	if backupConfig.SingleDataFile {
		gplog.Verbose("Initializing pipes and gpbackup_helper on segments for single data file restore")
		VerifyHelperVersionOnSegments(version)
		filteredOids := make([]string, len(filteredMasterDataEntries))
		for i, entry := range filteredMasterDataEntries {
			filteredOids[i] = fmt.Sprintf("%d", entry.Oid)
		}
		WriteOidListToSegments(filteredOids)
		firstOid := filteredMasterDataEntries[0].Oid
		CreateSegmentPipesOnAllHostsForRestore(firstOid)
		WriteToSegmentPipes()
	}

	totalTables := len(filteredMasterDataEntries)
	dataProgressBar := utils.NewProgressBar(totalTables, "Tables restored: ", utils.PB_INFO)
	dataProgressBar.Start()

	/*
	 * In both the serial and parallel cases, we break when an interrupt is
	 * received and rely on TerminateHangingCopySessions to kill any COPY
	 * statements in progress if they don't finish on their own.
	 */
	if connection.NumConns == 1 {
		for i, entry := range filteredMasterDataEntries {
			if wasTerminated {
				dataProgressBar.(*pb.ProgressBar).NotPrint = true
				return
			}
			restoreSingleTableData(entry, uint32(i)+1, totalTables, 0)
			dataProgressBar.Increment()
		}
	} else {
		var tableNum uint32 = 1
		tasks := make(chan utils.MasterDataEntry, totalTables)
		var workerPool sync.WaitGroup
		for i := 0; i < connection.NumConns; i++ {
			workerPool.Add(1)
			go func(whichConn int) {
				setGUCsForConnection(gucStatements, whichConn)
				for entry := range tasks {
					if wasTerminated {
						break
					}
					restoreSingleTableData(entry, tableNum, totalTables, whichConn)
					atomic.AddUint32(&tableNum, 1)
					dataProgressBar.Increment()
				}
				workerPool.Done()
			}(i)
		}
		for _, entry := range filteredMasterDataEntries {
			tasks <- entry
		}
		close(tasks)
		workerPool.Wait()
	}
	dataProgressBar.Finish()
	err := CheckAgentErrorsOnSegments()
	if err != nil {
		errMsg := "Error restoring data for one or more tables"
		if *onErrorContinue {
			gplog.Error("%s: %v", errMsg, err)
		} else {
			gplog.Fatal(err, errMsg)
		}
	}
	gplog.Info("Data restore complete")
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
	ExecuteRestoreMetadataStatements(firstBatch, "", progressBar, utils.PB_VERBOSE, connection.NumConns > 1)
	ExecuteRestoreMetadataStatements(secondBatch, "", progressBar, utils.PB_VERBOSE, connection.NumConns > 1)
	progressBar.Finish()
	gplog.Info("Post-data metadata restore complete")
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
	errorCode := gplog.GetErrorCode()

	if globalFPInfo.Timestamp != "" {
		reportFilename := globalFPInfo.GetRestoreReportFilePath(restoreStartTime)
		utils.WriteRestoreReportFile(reportFilename, globalFPInfo.Timestamp, restoreStartTime, connection, version, errMsg)
		utils.EmailReport(globalCluster, globalFPInfo.Timestamp, reportFilename, "gprestore")
		if pluginConfig != nil {
			pluginConfig.CleanupPluginForRestoreOnAllHosts(globalCluster, pluginConfig.ConfigPath, globalFPInfo.GetDirForContent(-1))
		}
	}

	DoCleanup()

	os.Exit(errorCode)
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
		CleanUpSegmentHelperProcesses()
		CleanUpHelperFilesOnAllHosts()
		if wasTerminated { // These should all end on their own in a successful restore
			utils.TerminateHangingCopySessions(connection, globalFPInfo, "gprestore")
		}
	}
	if connection != nil {
		connection.Close()
	}
}
