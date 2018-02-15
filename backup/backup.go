package backup

import (
	"flag"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpbackup/utils"
)

/*
 * We define and initialize flags separately to avoid import conflicts in tests.
 * The flag variables, and setter functions for them, are in global_variables.go.
 */
func initializeFlags() {
	backupDir = flag.String("backup-dir", "", "The absolute path of the directory to which all backup files will be written")
	compressionLevel = flag.Int("compression-level", 0, "Level of compression to use during data backup. Valid values are between 1 and 9.")
	dataOnly = flag.Bool("data-only", false, "Only back up data, do not back up metadata")
	dbname = flag.String("dbname", "", "The database to be backed up")
	debug = flag.Bool("debug", false, "Print verbose and debug log messages")
	flag.Var(&excludeSchemas, "exclude-schema", "Back up all schemas except the specified schema(s). --exclude-schema can be specified multiple times.")
	flag.Var(&excludeTables, "exclude-table", "Back up all tables except the specified table(s). --exclude-table can be specified multiple times.")
	excludeTableFile = flag.String("exclude-table-file", "", "A file containing a list of fully-qualified tables to be excluded from the backup")
	flag.Var(&includeSchemas, "include-schema", "Back up only the specified schema(s). --include-schema can be specified multiple times.")
	flag.Var(&includeTables, "include-table", "Back up only the specified table(s). --include-table can be specified multiple times.")
	includeTableFile = flag.String("include-table-file", "", "A file containing a list of fully-qualified tables to be included in the backup")
	leafPartitionData = flag.Bool("leaf-partition-data", false, "For partition tables, create one data file per leaf partition instead of one data file for the whole table")
	metadataOnly = flag.Bool("metadata-only", false, "Only back up metadata, do not back up data")
	noCompression = flag.Bool("no-compression", false, "Disable compression of data files")
	printVersion = flag.Bool("version", false, "Print version number and exit")
	quiet = flag.Bool("quiet", false, "Suppress non-warning, non-error log messages")
	singleDataFile = flag.Bool("single-data-file", false, "Back up all data to a single file instead of one per table")
	verbose = flag.Bool("verbose", false, "Print verbose log messages")
	withStats = flag.Bool("with-stats", false, "Back up query plan statistics")
}

// This function handles setup that can be done before parsing flags.
func DoInit() {
	CleanupGroup = &sync.WaitGroup{}
	CleanupGroup.Add(1)
	gplog.InitializeLogging("gpbackup", "")
	initializeFlags()
	utils.InitializeSignalHandler(DoCleanup, "backup process", &wasTerminated)
}

func DoFlagValidation() {
	if len(os.Args) == 1 {
		flag.PrintDefaults()
		os.Exit(0)
	}
	flag.Parse()
	if *printVersion {
		fmt.Printf("gpbackup %s\n", version)
		os.Exit(0)
	}
	ValidateFlagCombinations()
	ValidateFlagValues()
}

// This function handles setup that must be done after parsing flags.
func DoSetup() {
	SetLoggerVerbosity()
	timestamp := utils.CurrentTimestamp()
	utils.CreateBackupLockFile(timestamp)
	gplog.Info("Starting backup of database %s", *dbname)
	InitializeConnection()

	InitializeFilterLists()
	InitializeBackupReport()
	validateFilterLists()

	segConfig := cluster.GetSegmentConfiguration(connection)
	segPrefix := utils.GetSegPrefix(connection)
	globalCluster = cluster.NewCluster(segConfig)
	globalFPInfo = utils.NewFilePathInfo(globalCluster.SegDirMap, *backupDir, timestamp, segPrefix)
	CreateBackupDirectoriesOnAllHosts()
	globalTOC = &utils.TOC{}
	globalTOC.InitializeEntryMap()
}

func DoBackup() {
	LogBackupInfo()

	objectCounts = make(map[string]int, 0)

	metadataTables, dataTables, tableDefs := RetrieveAndProcessTables()
	CheckTablesContainData(dataTables, tableDefs)
	metadataFilename := globalFPInfo.GetMetadataFilePath()
	gplog.Info("Metadata will be written to %s", metadataFilename)
	metadataFile := utils.NewFileWithByteCountFromFile(metadataFilename)
	defer metadataFile.Close()

	BackupSessionGUCs(metadataFile)
	if !*dataOnly {
		if len(includeTables) > 0 {
			backupTablePredata(metadataFile, metadataTables, tableDefs)
		} else {
			backupGlobal(metadataFile)
			backupPredata(metadataFile, metadataTables, tableDefs)
		}
		backupPostdata(metadataFile)
	}

	if !backupReport.MetadataOnly {
		backupData(dataTables, tableDefs)
	}

	if *withStats {
		backupStatistics(metadataTables)
	}

	globalTOC.WriteToFileAndMakeReadOnly(globalFPInfo.GetTOCFilePath())
	connection.MustCommit()
}

func backupGlobal(metadataFile *utils.FileWithByteCount) {
	gplog.Info("Writing global database metadata")

	BackupTablespaces(metadataFile)
	BackupCreateDatabase(metadataFile)
	BackupDatabaseGUCs(metadataFile)

	if len(includeSchemas) == 0 {
		BackupResourceQueues(metadataFile)
		if connection.Version.AtLeast("5") {
			BackupResourceGroups(metadataFile)
		}
		BackupRoles(metadataFile)
		BackupRoleGrants(metadataFile)
	}
	gplog.Info("Global database metadata backup complete")
}

func backupPredata(metadataFile *utils.FileWithByteCount, tables []Relation, tableDefs map[uint32]TableDefinition) {
	gplog.Info("Writing pre-data metadata")

	BackupSchemas(metadataFile)
	if len(includeSchemas) == 0 && connection.Version.AtLeast("5") {
		BackupExtensions(metadataFile)
	}

	procLangs := GetProceduralLanguages(connection)
	langFuncs, otherFuncs, functionMetadata := RetrieveFunctions(procLangs)
	types, typeMetadata, funcInfoMap := RetrieveTypes()

	if len(includeSchemas) == 0 {
		BackupProceduralLanguages(metadataFile, procLangs, langFuncs, functionMetadata, funcInfoMap)
	}

	BackupShellTypes(metadataFile, types)
	if connection.Version.AtLeast("5") {
		BackupEnumTypes(metadataFile, typeMetadata)
	}

	relationMetadata := GetMetadataForObjectType(connection, TYPE_RELATION)
	sequences := GetAllSequences(connection)
	BackupCreateSequences(metadataFile, sequences, relationMetadata)

	constraints, conMetadata := RetrieveConstraints()

	BackupFunctionsAndTypesAndTables(metadataFile, otherFuncs, types, tables, functionMetadata, typeMetadata, relationMetadata, tableDefs, constraints)
	BackupAlterSequences(metadataFile, sequences)

	if len(includeSchemas) == 0 {
		BackupProtocols(metadataFile, funcInfoMap)
		if connection.Version.AtLeast("6") {
			BackupForeignDataWrappers(metadataFile, funcInfoMap)
			BackupForeignServers(metadataFile)
			BackupUserMappings(metadataFile)
		}
	}

	if connection.Version.AtLeast("5") {
		BackupTSParsers(metadataFile)
		BackupTSTemplates(metadataFile)
		BackupTSDictionaries(metadataFile)
		BackupTSConfigurations(metadataFile)
	}

	BackupOperators(metadataFile)
	if connection.Version.AtLeast("5") {
		BackupOperatorFamilies(metadataFile)
	}
	BackupOperatorClasses(metadataFile)

	BackupConversions(metadataFile)
	BackupAggregates(metadataFile, funcInfoMap)
	BackupCasts(metadataFile)
	BackupViews(metadataFile, relationMetadata)
	BackupConstraints(metadataFile, constraints, conMetadata)
	gplog.Info("Pre-data metadata backup complete")
}

func backupTablePredata(metadataFile *utils.FileWithByteCount, tables []Relation, tableDefs map[uint32]TableDefinition) {
	gplog.Info("Writing table metadata")

	relationMetadata := GetMetadataForObjectType(connection, TYPE_RELATION)

	constraints, conMetadata := RetrieveConstraints(tables...)

	BackupTables(metadataFile, tables, relationMetadata, tableDefs, constraints)
	BackupConstraints(metadataFile, constraints, conMetadata)
	gplog.Info("Table metadata backup complete")
}

func backupData(tables []Relation, tableDefs map[uint32]TableDefinition) {
	gplog.Info("Writing data to file")
	rowsCopiedMap := BackupData(tables, tableDefs)
	AddTableDataEntriesToTOC(tables, tableDefs, rowsCopiedMap)
	if *singleDataFile {
		MoveSegmentTOCsAndMakeReadOnly()
	}
	gplog.Info("Data backup complete")
}

func backupPostdata(metadataFile *utils.FileWithByteCount) {
	gplog.Info("Writing post-data metadata")

	BackupIndexes(metadataFile)
	BackupRules(metadataFile)
	BackupTriggers(metadataFile)
	gplog.Info("Post-data metadata backup complete")
}

func backupStatistics(tables []Relation) {
	statisticsFilename := globalFPInfo.GetStatisticsFilePath()
	gplog.Info("Writing query planner statistics to %s", statisticsFilename)
	statisticsFile := utils.NewFileWithByteCountFromFile(statisticsFilename)
	defer statisticsFile.Close()
	BackupStatistics(statisticsFile, tables)
	gplog.Info("Query planner statistics backup complete")
}

func DoTeardown() {
	errStr := ""
	if err := recover(); err != nil {
		errStr = fmt.Sprintf("%v", err)
	}
	if wasTerminated {
		/*
		 * Don't print an error or create a report file if the backup was canceled,
		 * as the signal handler will take care of cleanup and return codes.  Just
		 * wait until the signal handler's DoCleanup completes so the main goroutine
		 * doesn't exit while cleanup is still in progress.
		 */
		CleanupGroup.Wait()
		return
	}
	if errStr != "" {
		fmt.Println(errStr)
	}
	errMsg := utils.ParseErrorMessage(errStr)
	errorCode := gplog.GetErrorCode()

	/*
	 * Only create a report file if we fail after the cluster is initialized
	 * and a backup directory exists in which to create the report file.
	 */
	if globalFPInfo.Timestamp != "" {
		_, statErr := os.Stat(globalFPInfo.GetDirForContent(-1))
		if statErr != nil { // Even if this isn't os.IsNotExist, don't try to write a report file in case of further errors
			os.Exit(errorCode)
		}
		reportFilename := globalFPInfo.GetBackupReportFilePath()
		configFilename := globalFPInfo.GetConfigFilePath()

		time.Sleep(time.Second) // We sleep for 1 second to ensure multiple backups do not start within the same second.
		timestampLockFile := fmt.Sprintf("/tmp/%s.lck", globalFPInfo.Timestamp)
		err := os.Remove(timestampLockFile)
		if err != nil {
			gplog.Warn("Failed to remove lock file %s.", timestampLockFile)
		}

		backupReport.ConstructBackupParamsString()
		backupReport.WriteConfigFile(configFilename)
		backupReport.WriteBackupReportFile(reportFilename, globalFPInfo.Timestamp, objectCounts, errMsg)
		utils.EmailReport(globalCluster, globalFPInfo.Timestamp, reportFilename, "gpbackup")
	}

	DoCleanup()

	if errorCode == 0 {
		gplog.Info("Backup completed successfully")
	}
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
	if *singleDataFile {
		CleanUpSegmentPipesOnAllHosts()
		CleanUpSegmentTailProcesses()
		if wasTerminated {
			// It is possible for the COPY command to become orphaned if an agent process is killed
			utils.TerminateHangingCopySessions(connection, globalFPInfo, "gpbackup")
		}
	}
	if globalFPInfo.Timestamp != "" {
		timestampLockFile := fmt.Sprintf("/tmp/%s.lck", globalFPInfo.Timestamp)
		os.Remove(timestampLockFile) // Don't check error, as DoTeardown may have removed it already
	}
	if connection != nil {
		connection.Close()
	}
}
