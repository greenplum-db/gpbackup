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
	"github.com/spf13/pflag"
)

/*
 * We define and initialize flags separately to avoid import conflicts in tests.
 * The flag variables, and setter functions for them, are in global_variables.go.
 */

func initializeFlags(cmd *cobra.Command) {
	SetFlagDefaults(cmd.Flags())

	_ = cmd.MarkFlagRequired(utils.DBNAME)

	cmdFlags = cmd.Flags()
}

func SetFlagDefaults(flagSet *pflag.FlagSet) {
	flagSet.String(utils.BACKUP_DIR, "", "The absolute path of the directory to which all backup files will be written")
	flagSet.Int(utils.COMPRESSION_LEVEL, 1, "Level of compression to use during data backup. Valid values are between 1 and 9.")
	flagSet.Bool(utils.DATA_ONLY, false, "Only back up data, do not back up metadata")
	flagSet.String(utils.DBNAME, "", "The database to be backed up")
	flagSet.Bool(utils.DEBUG, false, "Print verbose and debug log messages")
	flagSet.StringSlice(utils.EXCLUDE_SCHEMA, []string{}, "Back up all metadata except objects in the specified schema(s). --exclude-schema can be specified multiple times.")
	flagSet.StringSlice(utils.EXCLUDE_RELATION, []string{}, "Back up all metadata except the specified table(s). --exclude-table can be specified multiple times.")
	flagSet.String(utils.EXCLUDE_RELATION_FILE, "", "A file containing a list of fully-qualified tables to be excluded from the backup")
	flagSet.String(utils.FROM_TIMESTAMP, "", "A timestamp to use to base the current incremental backup off")
	flagSet.Bool("help", false, "Help for gpbackup")
	flagSet.StringSlice(utils.INCLUDE_SCHEMA, []string{}, "Back up only the specified schema(s). --include-schema can be specified multiple times.")
	flagSet.StringSlice(utils.INCLUDE_RELATION, []string{}, "Back up only the specified table(s). --include-table can be specified multiple times.")
	flagSet.String(utils.INCLUDE_RELATION_FILE, "", "A file containing a list of fully-qualified tables to be included in the backup")
	flagSet.Bool(utils.INCREMENTAL, false, "Only back up data for AO tables that have been modified since the last backup")
	flagSet.Int(utils.JOBS, 1, "The number of parallel connections to use when backing up data")
	flagSet.Bool(utils.LEAF_PARTITION_DATA, false, "For partition tables, create one data file per leaf partition instead of one data file for the whole table")
	flagSet.Bool(utils.METADATA_ONLY, false, "Only back up metadata, do not back up data")
	flagSet.Bool(utils.NO_COMPRESSION, false, "Disable compression of data files")
	flagSet.String(utils.PLUGIN_CONFIG, "", "The configuration file to use for a plugin")
	flagSet.Bool("version", false, "Print version number and exit")
	flagSet.Bool(utils.QUIET, false, "Suppress non-warning, non-error log messages")
	flagSet.Bool(utils.SINGLE_DATA_FILE, false, "Back up all data to a single file instead of one per table")
	flagSet.Bool(utils.VERBOSE, false, "Print verbose log messages")
	flagSet.Bool(utils.WITH_STATS, false, "Back up query plan statistics")
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
	CreateBackupLockFile(timestamp)

	gplog.Info("Starting backup of database %s", MustGetFlagString(utils.DBNAME))
	InitializeConnectionPool()

	InitializeFilterLists()
	validateFilterLists()

	segConfig := cluster.MustGetSegmentConfiguration(connectionPool)
	globalCluster = cluster.NewCluster(segConfig)
	segPrefix := utils.GetSegPrefix(connectionPool)
	globalFPInfo = utils.NewFilePathInfo(globalCluster, MustGetFlagString(utils.BACKUP_DIR), timestamp, segPrefix)
	CreateBackupDirectoriesOnAllHosts()
	globalTOC = &utils.TOC{}
	globalTOC.InitializeMetadataEntryMap()
	utils.InitializePipeThroughParameters(!MustGetFlagBool(utils.NO_COMPRESSION), MustGetFlagInt(utils.COMPRESSION_LEVEL))

	pluginConfigFlag := MustGetFlagString(utils.PLUGIN_CONFIG)

	if pluginConfigFlag != "" {
		pluginConfig = utils.ReadPluginConfig(pluginConfigFlag)
	}

	InitializeBackupReport()

	if pluginConfigFlag != "" {
		pluginConfig.CheckPluginExistsOnAllHosts(globalCluster)

		pluginConfig.CopyPluginConfigToAllHosts(globalCluster, pluginConfigFlag)
		pluginConfig.SetupPluginForBackup(globalCluster, globalFPInfo)
	}
}

func DoBackup() {
	LogBackupInfo()

	targetBackupTimestamp := ""
	var targetBackupFPInfo utils.FilePathInfo
	if MustGetFlagBool(utils.INCREMENTAL) {
		targetBackupTimestamp = GetTargetBackupTimestamp()
		targetBackupFPInfo = utils.NewFilePathInfo(globalCluster, globalFPInfo.UserSpecifiedBackupDir,
			targetBackupTimestamp, globalFPInfo.UserSpecifiedSegPrefix)

		if MustGetFlagString(utils.PLUGIN_CONFIG) != "" {
			// These files need to be downloaded from the remote system into the local filesystem
			pluginConfig.MustRestoreFile(targetBackupFPInfo.GetConfigFilePath())
			pluginConfig.MustRestoreFile(targetBackupFPInfo.GetTOCFilePath())
		}
	}

	objectCounts = make(map[string]int, 0)

	gplog.Info("Gathering table state information")
	metadataTables, dataTables, tableDefs := RetrieveAndProcessTables()
	if !(MustGetFlagBool(utils.METADATA_ONLY) || MustGetFlagBool(utils.DATA_ONLY)) {
		BackupIncrementalMetadata()
	}
	CheckTablesContainData(dataTables, tableDefs)
	metadataFilename := globalFPInfo.GetMetadataFilePath()
	gplog.Info("Metadata will be written to %s", metadataFilename)
	metadataFile := utils.NewFileWithByteCountFromFile(metadataFilename)

	BackupSessionGUCs(metadataFile)
	if !MustGetFlagBool(utils.DATA_ONLY) {
		if len(MustGetFlagStringSlice(utils.INCLUDE_RELATION)) > 0 {
			backupRelationPredata(metadataFile, metadataTables, tableDefs)
		} else {
			backupGlobal(metadataFile)
			backupPredata(metadataFile, metadataTables, tableDefs)
		}
		backupPostdata(metadataFile)
	}

	/*
	 * We check this in the backup report rather than the flag because we
	 * perform a metadata only backup if the database contains no tables
	 * or only external tables
	 */
	if !backupReport.MetadataOnly {
		backupSetTables := dataTables

		targetBackupRestorePlan := make([]utils.RestorePlanEntry, 0)
		if targetBackupTimestamp != "" {
			gplog.Info("Basing incremental backup off of backup with timestamp = %s", targetBackupTimestamp)

			targetBackupTOC := utils.NewTOC(targetBackupFPInfo.GetTOCFilePath())
			targetBackupRestorePlan = utils.ReadConfigFile(targetBackupFPInfo.GetConfigFilePath()).RestorePlan
			backupSetTables = FilterTablesForIncremental(targetBackupTOC, globalTOC, dataTables)
		}

		backupReport.RestorePlan = PopulateRestorePlan(backupSetTables, targetBackupRestorePlan, dataTables)

		backupData(backupSetTables, tableDefs)
	}

	if MustGetFlagBool(utils.WITH_STATS) {
		backupStatistics(metadataTables)
	}

	globalTOC.WriteToFileAndMakeReadOnly(globalFPInfo.GetTOCFilePath())
	for connNum := 0; connNum < connectionPool.NumConns; connNum++ {
		connectionPool.MustCommit(connNum)
	}
	metadataFile.Close()
	if MustGetFlagString(utils.PLUGIN_CONFIG) != "" {
		pluginConfig.MustBackupFile(metadataFilename)
		pluginConfig.MustBackupFile(globalFPInfo.GetTOCFilePath())
		if MustGetFlagBool(utils.WITH_STATS) {
			pluginConfig.MustBackupFile(globalFPInfo.GetStatisticsFilePath())
		}
	}

	utils.WriteBackupHistory(globalFPInfo.GetBackupHistoryFilePath(), &backupReport.BackupConfig)
}

func backupGlobal(metadataFile *utils.FileWithByteCount) {
	gplog.Info("Writing global database metadata")

	BackupTablespaces(metadataFile)
	BackupCreateDatabase(metadataFile)
	BackupDatabaseGUCs(metadataFile)

	if len(MustGetFlagStringSlice(utils.INCLUDE_SCHEMA)) == 0 {
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
	if len(MustGetFlagStringSlice(utils.INCLUDE_SCHEMA)) == 0 && connectionPool.Version.AtLeast("5") {
		BackupExtensions(metadataFile)
	}

	if connectionPool.Version.AtLeast("6") {
		BackupCollations(metadataFile)
	}
	procLangs := GetProceduralLanguages(connectionPool)
	langFuncs, otherFuncs, functionMetadata := RetrieveFunctions(procLangs)
	types, typeMetadata, funcInfoMap := RetrieveTypes()

	if len(MustGetFlagStringSlice(utils.INCLUDE_SCHEMA)) == 0 {
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

	if len(MustGetFlagStringSlice(utils.INCLUDE_SCHEMA)) == 0 {
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
	if MustGetFlagBool(utils.SINGLE_DATA_FILE) {
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
		compressStr := fmt.Sprintf(" --compression-level %d", MustGetFlagInt(utils.COMPRESSION_LEVEL))
		if MustGetFlagBool(utils.NO_COMPRESSION) {
			compressStr = " --compression-level 0"
		}
		utils.StartAgent(globalCluster, globalFPInfo, "--backup-agent",
			MustGetFlagString(utils.PLUGIN_CONFIG), compressStr)
	}
	gplog.Info("Writing data to file")
	rowsCopiedMaps := BackupDataForAllTables(tables, tableDefs)
	AddTableDataEntriesToTOC(tables, tableDefs, rowsCopiedMaps)
	if MustGetFlagBool(utils.SINGLE_DATA_FILE) && MustGetFlagString(utils.PLUGIN_CONFIG) != "" {
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
	defer func() {
		DoCleanup()

		errorCode := gplog.GetErrorCode()
		if errorCode == 0 {
			gplog.Info("Backup completed successfully")
		}
		os.Exit(errorCode)
	}()

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
			return
		}
		reportFilename := globalFPInfo.GetBackupReportFilePath()
		configFilename := globalFPInfo.GetConfigFilePath()

		time.Sleep(time.Second) // We sleep for 1 second to ensure multiple backups do not start within the same second.

		if backupReport != nil {
			backupReport.ConstructBackupParamsString()
			backupReport.WriteConfigFile(configFilename)
			backupReport.WriteBackupReportFile(reportFilename, globalFPInfo.Timestamp, objectCounts, errMsg)
			utils.EmailReport(globalCluster, globalFPInfo.Timestamp, reportFilename, "gpbackup")
			if pluginConfig != nil {
				err := pluginConfig.BackupFile(configFilename)
				if err != nil {
					gplog.Error(fmt.Sprintf("%v", err))
					return
				}
				err = pluginConfig.BackupFile(reportFilename)
				if err != nil {
					gplog.Error(fmt.Sprintf("%v", err))
					return
				}
			}
		}
		if pluginConfig != nil {
			pluginConfig.CleanupPluginForBackup(globalCluster, globalFPInfo)
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
	if globalFPInfo.Timestamp != "" {
		if MustGetFlagBool(utils.SINGLE_DATA_FILE) {
			utils.CleanUpHelperFilesOnAllHosts(globalCluster, globalFPInfo)
			if wasTerminated {
				utils.CleanUpSegmentHelperProcesses(globalCluster, globalFPInfo, "backup")
				// It is possible for the COPY command to become orphaned if an agent process is killed
				utils.TerminateHangingCopySessions(connectionPool, globalFPInfo, "gpbackup")
			}
		}
	}
	err := backupLockFile.Unlock()
	if err != nil {
		gplog.Warn("Failed to remove lock file %s.", backupLockFile)
	}
	if connectionPool != nil {
		connectionPool.Close()
	}
}

func GetVersion() string {
	return version
}
