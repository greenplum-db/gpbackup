package backup

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/greenplum-db/gpbackup/utils"
)

/*
 * We define and initialize flags separately to avoid import conflicts in tests.
 * The flag variables, and setter functions for them, are in global_variables.go.
 */
func initializeFlags() {
	backupDir = flag.String("backupdir", "", "The absolute path of the directory to which all backup files will be written")
	compressionLevel = flag.Int("compression-level", 0, "Level of compression to use during data backup. Valid values are between 1 and 9.")
	dataOnly = flag.Bool("data-only", false, "Only back up data, do not back up metadata")
	dbname = flag.String("dbname", "", "The database to be backed up")
	debug = flag.Bool("debug", false, "Print verbose and debug log messages")
	flag.Var(&excludeSchemas, "exclude-schema", "Do not back up only the specified schema(s). --exclude-schema can be specified multiple times.")
	excludeTableFile = flag.String("exclude-table-file", "", "A file containing a list of fully-qualified tables to be excluded from the backup")
	flag.Var(&includeSchemas, "include-schema", "Back up only the specified schema(s). --include-schema can be specified multiple times.")
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
	SetLogger(utils.InitializeLogging("gpbackup", ""))
	initializeFlags()
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
	logger.Info("Starting backup of database %s", *dbname)
	InitializeConnection()

	InitializeFilterLists()
	InitializeBackupReport()
	validateFilterLists()

	segConfig := utils.GetSegmentConfiguration(connection)
	segPrefix := utils.GetSegPrefix(connection)
	globalCluster = utils.NewCluster(segConfig, *backupDir, timestamp, segPrefix)
	globalCluster.CreateBackupDirectoriesOnAllHosts()
	globalTOC = &utils.TOC{}
	globalTOC.InitializeEntryMap()
}

func DoBackup() {
	LogBackupInfo()

	objectCounts = make(map[string]int, 0)

	isTableFiltered := len(includeTables) > 0 || len(excludeTables) > 0
	metadataTables, dataTables, tableDefs := RetrieveAndProcessTables()
	metadataFilename := globalCluster.GetMetadataFilePath()
	logger.Info("Metadata will be written to %s", metadataFilename)
	metadataFile := utils.NewFileWithByteCountFromFile(metadataFilename)
	defer metadataFile.Close()
	if !*dataOnly {
		if isTableFiltered {
			backupTablePredata(metadataFile, metadataTables, tableDefs)
		} else {
			backupGlobal(metadataFile)
			backupPredata(metadataFile, metadataTables, tableDefs)
			backupPostdata(metadataFile)
		}
	} else {
		BackupSessionGUCs(metadataFile)
	}

	if !*metadataOnly {
		backupData(dataTables, tableDefs)
	}

	if *withStats {
		backupStatistics(metadataTables)
	}

	globalTOC.WriteToFileAndMakeReadOnly(globalCluster.GetTOCFilePath())
	connection.Commit()
}

func backupGlobal(metadataFile *utils.FileWithByteCount) {
	logger.Info("Writing global database metadata")

	BackupSessionGUCs(metadataFile)
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
	logger.Info("Global database metadata backup complete")
}

func backupPredata(metadataFile *utils.FileWithByteCount, tables []Relation, tableDefs map[uint32]TableDefinition) {
	logger.Info("Writing pre-data metadata")

	BackupSessionGUCs(metadataFile)
	BackupSchemas(metadataFile)

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
	logger.Info("Pre-data metadata backup complete")
}

func backupTablePredata(metadataFile *utils.FileWithByteCount, tables []Relation, tableDefs map[uint32]TableDefinition) {
	logger.Info("Writing table metadata")

	BackupSessionGUCs(metadataFile)

	relationMetadata := GetMetadataForObjectType(connection, TYPE_RELATION)

	constraints, conMetadata := RetrieveConstraints(tables...)

	BackupTables(metadataFile, tables, relationMetadata, tableDefs, constraints)
	BackupConstraints(metadataFile, constraints, conMetadata)
	logger.Info("Table metadata backup complete")
}

func backupData(tables []Relation, tableDefs map[uint32]TableDefinition) {
	logger.Info("Writing data to file")
	BackupData(tables, tableDefs)
	AddTableDataEntriesToTOC(tables, tableDefs)
	if *singleDataFile {
		globalCluster.MoveSegmentTOCsAndMakeReadOnly()
	}
	logger.Info("Data backup complete")
}

func backupPostdata(metadataFile *utils.FileWithByteCount) {
	logger.Info("Writing post-data metadata")

	BackupSessionGUCs(metadataFile)
	BackupIndexes(metadataFile)
	BackupRules(metadataFile)
	BackupTriggers(metadataFile)
	logger.Info("Post-data metadata backup complete")
}

func backupStatistics(tables []Relation) {
	statisticsFilename := globalCluster.GetStatisticsFilePath()
	logger.Info("Writing query planner statistics to %s", statisticsFilename)
	statisticsFile := utils.NewFileWithByteCountFromFile(statisticsFilename)
	defer statisticsFile.Close()
	BackupStatistics(statisticsFile, tables)
	logger.Info("Query planner statistics backup complete")
}

func DoTeardown() {
	errStr := ""
	if err := recover(); err != nil {
		errStr = fmt.Sprintf("%v", err)
		fmt.Println(err)
	}
	errMsg, exitCode := utils.ParseErrorMessage(errStr)
	if connection != nil {
		connection.Close()
	}

	/*
	 * Only create a report file if we fail after the cluster is initialized
	 * and a backup directory exists in which to create the report file.
	 */
	if globalCluster.Timestamp != "" {
		_, statErr := os.Stat(globalCluster.GetDirForContent(-1))
		if statErr != nil { // Even if this isn't os.IsNotExist, don't try to write a report file in case of further errors
			os.Exit(exitCode)
		}
		reportFilename := globalCluster.GetReportFilePath()
		configFilename := globalCluster.GetConfigFilePath()
		backupReport.WriteReportFile(reportFilename, globalCluster.Timestamp, objectCounts, errMsg)
		backupReport.WriteConfigFile(configFilename)
		utils.EmailReport(globalCluster)
		// We sleep for 1 second to ensure multiple backups do not start within the same second.
		time.Sleep(1000 * time.Millisecond)
		timestampLockFile := fmt.Sprintf("/tmp/%s.lck", globalCluster.Timestamp)
		err := os.Remove(timestampLockFile)
		if err != nil {
			logger.Warn("Failed to remove lock file %s.", timestampLockFile)
		}
	}

	if exitCode == 0 {
		logger.Info("Backup completed successfully")
	}
	os.Exit(exitCode)
}
