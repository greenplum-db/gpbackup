package backup

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpbackup/utils"
	"github.com/spf13/cobra"
)

/*
 * We define and initialize flags separately to avoid import conflicts in tests.
 * The flag variables, and setter functions for them, are in global_variables.go.
 */
func initializeFlags(cmd *cobra.Command) {
	backupDir = cmd.Flags().String("backup-dir", "", "The absolute path of the directory to which all backup files will be written")
	compressionLevel = cmd.Flags().Int("compression-level", 0, "Level of compression to use during data backup. Valid values are between 1 and 9.")
	dataOnly = cmd.Flags().Bool("data-only", false, "Only back up data, do not back up metadata")
	dbname = cmd.Flags().String("dbname", "", "The database to be backed up")
	debug = cmd.Flags().Bool("debug", false, "Print verbose and debug log messages")
	excludeSchemas = cmd.Flags().StringSlice("exclude-schema", []string{}, "Back up all metadata except objects in the specified schema(s). --exclude-schema can be specified multiple times.")
	excludeRelations = cmd.Flags().StringSlice("exclude-table", []string{}, "Back up all metadata except the specified table(s). --exclude-table can be specified multiple times.")
	excludeRelationFile = cmd.Flags().String("exclude-table-file", "", "A file containing a list of fully-qualified tables to be excluded from the backup")
	cmd.Flags().Bool("help", false, "Help for gpbackup")
	includeSchemas = cmd.Flags().StringSlice("include-schema", []string{}, "Back up only the specified schema(s). --include-schema can be specified multiple times.")
	includeRelations = cmd.Flags().StringSlice("include-table", []string{}, "Back up only the specified table(s). --include-table can be specified multiple times.")
	includeRelationFile = cmd.Flags().String("include-table-file", "", "A file containing a list of fully-qualified tables to be included in the backup")
	numJobs = cmd.Flags().Int("jobs", 1, "The number of parallel connections to use when backing up data")
	leafPartitionData = cmd.Flags().Bool("leaf-partition-data", false, "For partition tables, create one data file per leaf partition instead of one data file for the whole table")
	metadataOnly = cmd.Flags().Bool("metadata-only", false, "Only back up metadata, do not back up data")
	noCompression = cmd.Flags().Bool("no-compression", false, "Disable compression of data files")
	pluginConfigFile = cmd.Flags().String("plugin-config", "", "The configuration file to use for a plugin")
	cmd.Flags().Bool("version", false, "Print version number and exit")
	quiet = cmd.Flags().Bool("quiet", false, "Suppress non-warning, non-error log messages")
	singleDataFile = cmd.Flags().Bool("single-data-file", false, "Back up all data to a single file instead of one per table")
	verbose = cmd.Flags().Bool("verbose", false, "Print verbose log messages")
	withStats = cmd.Flags().Bool("with-stats", false, "Back up query plan statistics")

	_ = cmd.MarkFlagRequired("dbname")
}

// This function handles setup that can be done before parsing flags.
func DoInit(cmd *cobra.Command) {
	CleanupGroup = &sync.WaitGroup{}
	CleanupGroup.Add(1)
	gplog.InitializeLogging("gpbackup", "")
	initializeFlags(cmd)
	utils.InitializeSignalHandler(DoCleanup, "backup process", &wasTerminated)
}

func DoFlagValidation(cmd *cobra.Command) {
	ValidateFlagCombinations(cmd.Flags())
	ValidateFlagValues()
}

// This function handles setup that must be done after parsing flags.
func DoSetup() {
	SetLoggerVerbosity()
	timestamp := utils.CurrentTimestamp()
	utils.CreateBackupLockFile(timestamp)

	gplog.Info("Starting backup of database %s", *dbname)
	InitializeConnectionPool()

	InitializeFilterLists()
	InitializeBackupReport()
	validateFilterLists()

	segConfig := cluster.MustGetSegmentConfiguration(connectionPool)
	globalCluster = cluster.NewCluster(segConfig)
	segPrefix := utils.GetSegPrefix(connectionPool)
	globalFPInfo = utils.NewFilePathInfo(globalCluster, *backupDir, timestamp, segPrefix)
	CreateBackupDirectoriesOnAllHosts()
	globalTOC = &utils.TOC{}
	globalTOC.InitializeMetadataEntryMap()

	if *pluginConfigFile != "" {
		pluginConfig = utils.ReadPluginConfig(*pluginConfigFile)
		pluginConfig.CheckPluginExistsOnAllHosts(globalCluster)
		pluginConfig.CopyPluginConfigToAllHosts(globalCluster, *pluginConfigFile)
		pluginConfig.SetupPluginForBackup(globalCluster, pluginConfig.ConfigPath, globalFPInfo.GetDirForContent(-1))
		backupReport.Plugin = pluginConfig.ExecutablePath
	}
}

func DoBackup() {
	LogBackupInfo()

	objectCounts = make(map[string]int, 0)

	metadataTables, dataTables, tableDefs := RetrieveAndProcessTables()
	CheckTablesContainData(dataTables, tableDefs)
	metadataFilename := globalFPInfo.GetMetadataFilePath()
	gplog.Info("Metadata will be written to %s", metadataFilename)
	metadataFile := utils.NewFileWithByteCountFromFile(metadataFilename)

	BackupSessionGUCs(metadataFile)
	if !*dataOnly {
		if len(*includeRelations) > 0 {
			backupRelationPredata(metadataFile, metadataTables, tableDefs)
		} else {
			backupGlobal(metadataFile)
			backupPredata(metadataFile, metadataTables, tableDefs)
		}
		backupPostdata(metadataFile)
		BackupIncrementalMetadata()
	}

	/*
	 * We check this in the backup report rather than the flag because we
	 * perform a metadata only backup if the database contains no tables
	 * or only external tables
	 */
	if !backupReport.MetadataOnly {
		backupData(dataTables, tableDefs)
	}

	if *withStats {
		backupStatistics(metadataTables)
	}

	globalTOC.WriteToFileAndMakeReadOnly(globalFPInfo.GetTOCFilePath())
	for connNum := 0; connNum < connectionPool.NumConns; connNum++ {
		connectionPool.MustCommit(connNum)
	}
	metadataFile.Close()
	if *pluginConfigFile != "" {
		pluginConfig.BackupFile(metadataFilename)
		pluginConfig.BackupFile(globalFPInfo.GetTOCFilePath())
		if *withStats {
			pluginConfig.BackupFile(globalFPInfo.GetStatisticsFilePath())
		}
	}
}

func backupGlobal(metadataFile *utils.FileWithByteCount) {
	gplog.Info("Writing global database metadata")

	BackupTablespaces(metadataFile)
	BackupCreateDatabase(metadataFile)
	BackupDatabaseGUCs(metadataFile)

	if len(*includeSchemas) == 0 {
		BackupResourceQueues(metadataFile)
		if connectionPool.Version.AtLeast("5") {
			BackupResourceGroups(metadataFile)
		}
		BackupRoles(metadataFile)
		BackupRoleGrants(metadataFile)
	}
	if wasTerminated {
		gplog.Info("Global database metadata backup incomplete")
	} else {
		gplog.Info("Global database metadata backup complete")
	}
}

func backupPredata(metadataFile *utils.FileWithByteCount, tables []Relation, tableDefs map[uint32]TableDefinition) {
	if wasTerminated {
		return
	}
	gplog.Info("Writing pre-data metadata")

	BackupSchemas(metadataFile)
	if len(*includeSchemas) == 0 && connectionPool.Version.AtLeast("5") {
		BackupExtensions(metadataFile)
	}

	if connectionPool.Version.AtLeast("6") {
		BackupCollations(metadataFile)
	}
	procLangs := GetProceduralLanguages(connectionPool)
	langFuncs, otherFuncs, functionMetadata := RetrieveFunctions(procLangs)
	types, typeMetadata, funcInfoMap := RetrieveTypes()

	if len(*includeSchemas) == 0 {
		BackupProceduralLanguages(metadataFile, procLangs, langFuncs, functionMetadata, funcInfoMap)
	}

	BackupShellTypes(metadataFile, types)
	if connectionPool.Version.AtLeast("5") {
		BackupEnumTypes(metadataFile, typeMetadata)
	}

	relationMetadata := GetMetadataForObjectType(connectionPool, TYPE_RELATION)
	sequences, sequenceOwnerColumns := RetrieveSequences()
	BackupCreateSequences(metadataFile, sequences, relationMetadata)

	constraints, conMetadata := RetrieveConstraints()
	protocols, protoMetadata := RetrieveAndProcessProtocols(funcInfoMap)

	BackupDependentObjects(metadataFile, otherFuncs, types, tables, protocols, functionMetadata, typeMetadata, relationMetadata, protoMetadata, tableDefs, constraints)
	PrintAlterSequenceStatements(metadataFile, globalTOC, sequences, sequenceOwnerColumns)

	if len(*includeSchemas) == 0 {
		if connectionPool.Version.AtLeast("6") {
			BackupForeignDataWrappers(metadataFile, funcInfoMap)
			BackupForeignServers(metadataFile)
			BackupUserMappings(metadataFile)
		}
	}

	if connectionPool.Version.AtLeast("5") {
		BackupTSParsers(metadataFile)
		BackupTSTemplates(metadataFile)
		BackupTSDictionaries(metadataFile)
		BackupTSConfigurations(metadataFile)
	}

	BackupOperators(metadataFile)
	if connectionPool.Version.AtLeast("5") {
		BackupOperatorFamilies(metadataFile)
	}
	BackupOperatorClasses(metadataFile)

	BackupConversions(metadataFile)
	BackupAggregates(metadataFile, funcInfoMap)
	BackupCasts(metadataFile)
	BackupViews(metadataFile, relationMetadata)
	BackupConstraints(metadataFile, constraints, conMetadata)
	if wasTerminated {
		gplog.Info("Pre-data metadata backup incomplete")
	} else {
		gplog.Info("Pre-data metadata backup complete")
	}
}

func backupRelationPredata(metadataFile *utils.FileWithByteCount, tables []Relation, tableDefs map[uint32]TableDefinition) {
	if wasTerminated {
		return
	}
	gplog.Info("Writing table metadata")

	relationMetadata := GetMetadataForObjectType(connectionPool, TYPE_RELATION)

	sequences, sequenceOwnerColumns := RetrieveSequences()
	BackupCreateSequences(metadataFile, sequences, relationMetadata)

	constraints, conMetadata := RetrieveConstraints(tables...)

	BackupTables(metadataFile, tables, relationMetadata, tableDefs, constraints)
	PrintAlterSequenceStatements(metadataFile, globalTOC, sequences, sequenceOwnerColumns)

	BackupViews(metadataFile, relationMetadata)

	BackupConstraints(metadataFile, constraints, conMetadata)
	gplog.Info("Table metadata backup complete")
}

func backupData(tables []Relation, tableDefs map[uint32]TableDefinition) {
	if *singleDataFile {
		gplog.Verbose("Initializing pipes and gpbackup_helper on segments for single data file backup")
		utils.VerifyHelperVersionOnSegments(version, globalCluster)
		oidList := make([]string, 0, len(tables))
		for _, table := range tables {
			if !tableDefs[table.Oid].IsExternal {
				oidList = append(oidList, fmt.Sprintf("%d", table.Oid))
			}
		}
		utils.WriteOidListToSegments(oidList, globalCluster, globalFPInfo)
		utils.CreateFirstSegmentPipeOnAllHosts(oidList[0], globalCluster, globalFPInfo)
		compressStr := fmt.Sprintf(" --compression-level %d", *compressionLevel)
		if !*noCompression && *compressionLevel == 0 {
			compressStr = " --compression-level 1"
		}
		utils.StartAgent(globalCluster, globalFPInfo, "--backup-agent", *pluginConfigFile, compressStr)
	}
	gplog.Info("Writing data to file")
	rowsCopiedMaps := BackupDataForAllTables(tables, tableDefs)
	AddTableDataEntriesToTOC(tables, tableDefs, rowsCopiedMaps)
	if *singleDataFile && *pluginConfigFile != "" {
		pluginConfig.BackupSegmentTOCs(globalCluster, globalFPInfo)
	}
	if wasTerminated {
		gplog.Info("Data backup incomplete")
	} else {
		gplog.Info("Data backup complete")
	}
}

func backupPostdata(metadataFile *utils.FileWithByteCount) {
	if wasTerminated {
		return
	}
	gplog.Info("Writing post-data metadata")

	BackupIndexes(metadataFile)
	BackupRules(metadataFile)
	BackupTriggers(metadataFile)
	if wasTerminated {
		gplog.Info("Post-data metadata backup incomplete")
	} else {
		gplog.Info("Post-data metadata backup complete")
	}
}

func backupStatistics(tables []Relation) {
	if wasTerminated {
		return
	}
	statisticsFilename := globalFPInfo.GetStatisticsFilePath()
	gplog.Info("Writing query planner statistics to %s", statisticsFilename)
	statisticsFile := utils.NewFileWithByteCountFromFile(statisticsFilename)
	defer statisticsFile.Close()
	BackupStatistics(statisticsFile, tables)
	if wasTerminated {
		gplog.Info("Query planner statistics backup incomplete")
	} else {
		gplog.Info("Query planner statistics backup complete")
	}
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

	/*
	 * Only create a report file if we fail after the cluster is initialized
	 * and a backup directory exists in which to create the report file.
	 */
	if globalFPInfo.Timestamp != "" {
		_, statErr := os.Stat(globalFPInfo.GetDirForContent(-1))
		if statErr != nil { // Even if this isn't os.IsNotExist, don't try to write a report file in case of further errors
			os.Exit(gplog.GetErrorCode())
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
		if pluginConfig != nil {
			pluginConfig.BackupFile(configFilename, true)
			pluginConfig.BackupFile(reportFilename, true)
			pluginConfig.CleanupPluginForBackup(globalCluster, pluginConfig.ConfigPath, globalFPInfo.GetDirForContent(-1))
		}
	}

	DoCleanup()

	errorCode := gplog.GetErrorCode()
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
	if globalFPInfo.Timestamp != "" {
		if *singleDataFile {
			utils.CleanUpHelperFilesOnAllHosts(globalCluster, globalFPInfo)
			if wasTerminated {
				utils.CleanUpSegmentHelperProcesses(globalCluster, globalFPInfo, "backup")
				// It is possible for the COPY command to become orphaned if an agent process is killed
				utils.TerminateHangingCopySessions(connectionPool, globalFPInfo, "gpbackup")
			}
		}
		timestampLockFile := fmt.Sprintf("/tmp/%s.lck", globalFPInfo.Timestamp)
		_ = os.Remove(timestampLockFile) // Don't check error, as DoTeardown may have removed it already
	}
	if connectionPool != nil {
		connectionPool.Close()
	}
}

func GetVersion() string {
	return version
}
