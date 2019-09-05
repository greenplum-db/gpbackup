package backup

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/greenplum-db/gpbackup/options"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/greenplum-db/gpbackup/backup_filepath"
	"github.com/greenplum-db/gpbackup/backup_history"
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
	flagSet.StringArray(utils.INCLUDE_RELATION, []string{}, "Back up only the specified table(s). --include-table can be specified multiple times.")
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
	objectCounts = make(map[string]int)
}

func DoFlagValidation(cmd *cobra.Command) {
	ValidateFlagCombinations(cmd.Flags())
	ValidateFlagValues()
}

// This function handles setup that must be done after parsing flags.
func DoSetup() {
	SetLoggerVerbosity()
	utils.CheckGpexpandRunning(utils.BackupPreventedByGpexpandMessage)
	timestamp := backup_history.CurrentTimestamp()
	CreateBackupLockFile(timestamp)
	InitializeConnectionPool()

	gplog.Info("Starting backup of database %s", MustGetFlagString(utils.DBNAME))
	opts, err := options.NewOptions(cmdFlags)
	gplog.FatalOnError(err)

	DBValidate(connectionPool, opts.GetIncludedTables(), false)

	// todo remove these when EXCLUDE_RELATION* flags are handled by options object
	InitializeFilterLists()
	validateFilterLists()

	err = opts.ExpandIncludesForPartitions(connectionPool, cmdFlags)
	gplog.FatalOnError(err)

	segConfig := cluster.MustGetSegmentConfiguration(connectionPool)
	globalCluster = cluster.NewCluster(segConfig)
	segPrefix := backup_filepath.GetSegPrefix(connectionPool)
	globalFPInfo = backup_filepath.NewFilePathInfo(globalCluster, MustGetFlagString(utils.BACKUP_DIR), timestamp, segPrefix)
	CreateBackupDirectoriesOnAllHosts()
	globalTOC = &utils.TOC{}
	globalTOC.InitializeMetadataEntryMap()
	utils.InitializePipeThroughParameters(!MustGetFlagBool(utils.NO_COMPRESSION), MustGetFlagInt(utils.COMPRESSION_LEVEL))

	pluginConfigFlag := MustGetFlagString(utils.PLUGIN_CONFIG)

	if pluginConfigFlag != "" {
		pluginConfig, err = utils.ReadPluginConfig(pluginConfigFlag)
		configFilename := filepath.Base(pluginConfig.ConfigPath)
		configDirname := filepath.Dir(pluginConfig.ConfigPath)
		pluginConfig.ConfigPath = filepath.Join(configDirname, timestamp+"_"+configFilename)
		_ = cmdFlags.Set(utils.PLUGIN_CONFIG, pluginConfig.ConfigPath)
		gplog.Info("Plugin config path: %s", pluginConfig.ConfigPath)
		gplog.FatalOnError(err)
	}

	InitializeBackupReport(*opts)

	if pluginConfigFlag != "" {
		backupReport.PluginVersion = pluginConfig.CheckPluginExistsOnAllHosts(globalCluster)
		pluginConfig.CopyPluginConfigToAllHosts(globalCluster)
		pluginConfig.SetupPluginForBackup(globalCluster, globalFPInfo)
	}
}

func DoBackup() {
	gplog.Info("Backup Timestamp = %s", globalFPInfo.Timestamp)
	gplog.Info("Backup Database = %s", connectionPool.DBName)
	gplog.Verbose("Backup Parameters: {%s}", strings.ReplaceAll(backupReport.BackupParamsString, "\n", ", "))

	pluginConfigFlag := MustGetFlagString(utils.PLUGIN_CONFIG)
	targetBackupTimestamp := ""
	var targetBackupFPInfo backup_filepath.FilePathInfo
	if MustGetFlagBool(utils.INCREMENTAL) {
		targetBackupTimestamp = GetTargetBackupTimestamp()
		targetBackupFPInfo = backup_filepath.NewFilePathInfo(globalCluster, globalFPInfo.UserSpecifiedBackupDir,
			targetBackupTimestamp, globalFPInfo.UserSpecifiedSegPrefix)

		if pluginConfigFlag != "" {
			// These files need to be downloaded from the remote system into the local filesystem
			pluginConfig.MustRestoreFile(targetBackupFPInfo.GetConfigFilePath())
			pluginConfig.MustRestoreFile(targetBackupFPInfo.GetTOCFilePath())
			pluginConfig.MustRestoreFile(targetBackupFPInfo.GetPluginConfigPath())
		}
	}

	gplog.Info("Gathering table state information")
	metadataTables, dataTables := RetrieveAndProcessTables()
	if !(MustGetFlagBool(utils.METADATA_ONLY) || MustGetFlagBool(utils.DATA_ONLY)) {
		BackupIncrementalMetadata()
	}
	CheckTablesContainData(dataTables)
	metadataFilename := globalFPInfo.GetMetadataFilePath()
	gplog.Info("Metadata will be written to %s", metadataFilename)
	metadataFile := utils.NewFileWithByteCountFromFile(metadataFilename)

	BackupSessionGUCs(metadataFile)
	if !MustGetFlagBool(utils.DATA_ONLY) {
		tableOnlyBackup := true
		if len(MustGetFlagStringArray(utils.INCLUDE_RELATION)) == 0 {
			tableOnlyBackup = false
			backupGlobal(metadataFile)
		}
		backupPredata(metadataFile, metadataTables, tableOnlyBackup)
		backupPostdata(metadataFile)
	}

	/*
	 * We check this in the backup report rather than the flag because we
	 * perform a metadata only backup if the database contains no tables
	 * or only external tables
	 */
	if !backupReport.MetadataOnly {
		backupSetTables := dataTables

		targetBackupRestorePlan := make([]backup_history.RestorePlanEntry, 0)
		if targetBackupTimestamp != "" {
			gplog.Info("Basing incremental backup off of backup with timestamp = %s", targetBackupTimestamp)

			targetBackupTOC := utils.NewTOC(targetBackupFPInfo.GetTOCFilePath())
			targetBackupRestorePlan = backup_history.ReadConfigFile(targetBackupFPInfo.GetConfigFilePath()).RestorePlan
			backupSetTables = FilterTablesForIncremental(targetBackupTOC, globalTOC, dataTables)
		}

		backupReport.RestorePlan = PopulateRestorePlan(backupSetTables, targetBackupRestorePlan, dataTables)

		backupData(backupSetTables)
	}

	if MustGetFlagBool(utils.WITH_STATS) {
		backupStatistics(metadataTables)
	}

	globalTOC.WriteToFileAndMakeReadOnly(globalFPInfo.GetTOCFilePath())
	for connNum := 0; connNum < connectionPool.NumConns; connNum++ {
		connectionPool.MustCommit(connNum)
	}
	metadataFile.Close()
	if pluginConfigFlag != "" {
		pluginConfig.MustBackupFile(metadataFilename)
		pluginConfig.MustBackupFile(globalFPInfo.GetTOCFilePath())
		if MustGetFlagBool(utils.WITH_STATS) {
			pluginConfig.MustBackupFile(globalFPInfo.GetStatisticsFilePath())
		}
		_ = utils.CopyFile(pluginConfigFlag, globalFPInfo.GetPluginConfigPath())
		pluginConfig.MustBackupFile(globalFPInfo.GetPluginConfigPath())
	}

	err := backup_history.WriteBackupHistory(globalFPInfo.GetBackupHistoryFilePath(), &backupReport.BackupConfig)
	gplog.FatalOnError(err)
}

func backupGlobal(metadataFile *utils.FileWithByteCount) {
	gplog.Info("Writing global database metadata")

	BackupResourceQueues(metadataFile)
	if connectionPool.Version.AtLeast("5") {
		BackupResourceGroups(metadataFile)
	}
	BackupRoles(metadataFile)
	BackupRoleGrants(metadataFile)
	BackupTablespaces(metadataFile)
	BackupCreateDatabase(metadataFile)
	BackupDatabaseGUCs(metadataFile)
	BackupRoleGUCs(metadataFile)

	if wasTerminated {
		gplog.Info("Global database metadata backup incomplete")
	} else {
		gplog.Info("Global database metadata backup complete")
	}
}

func backupPredata(metadataFile *utils.FileWithByteCount, tables []Table, tableOnly bool) {
	if wasTerminated {
		return
	}
	gplog.Info("Writing pre-data metadata")

	sortables := make([]Sortable, 0)
	metadataMap := make(MetadataMap)
	sortables = append(sortables, convertToSortableSlice(tables)...)
	relationMetadata := GetMetadataForObjectType(connectionPool, TYPE_RELATION)
	addToMetadataMap(relationMetadata, metadataMap)

	var protocols []ExternalProtocol
	funcInfoMap := GetFunctionOidToInfoMap(connectionPool)

	if !tableOnly {
		BackupSchemas(metadataFile)
		if len(MustGetFlagStringSlice(utils.INCLUDE_SCHEMA)) == 0 && connectionPool.Version.AtLeast("5") {
			BackupExtensions(metadataFile)
		}

		if connectionPool.Version.AtLeast("6") {
			BackupCollations(metadataFile)
		}

		procLangs := GetProceduralLanguages(connectionPool)
		langFuncs, functionMetadata := RetrieveFunctions(&sortables, metadataMap, procLangs)

		if len(MustGetFlagStringSlice(utils.INCLUDE_SCHEMA)) == 0 {
			BackupProceduralLanguages(metadataFile, procLangs, langFuncs, functionMetadata, funcInfoMap)
		}
		RetrieveAndBackupTypes(metadataFile, &sortables, metadataMap)

		if len(MustGetFlagStringSlice(utils.INCLUDE_SCHEMA)) == 0 &&
			connectionPool.Version.AtLeast("6") {
			RetrieveForeignDataWrappers(&sortables, metadataMap)
			RetrieveForeignServers(&sortables, metadataMap)
			RetrieveUserMappings(&sortables)
		}

		protocols = RetrieveProtocols(&sortables, metadataMap)

		if connectionPool.Version.AtLeast("5") {
			RetrieveTSParsers(&sortables, metadataMap)
			RetrieveTSConfigurations(&sortables, metadataMap)
			RetrieveTSTemplates(&sortables, metadataMap)
			RetrieveTSDictionaries(&sortables, metadataMap)

			BackupOperatorFamilies(metadataFile)
		}

		RetrieveOperators(&sortables, metadataMap)
		RetrieveOperatorClasses(&sortables, metadataMap)
		RetrieveAggregates(&sortables, metadataMap)
		RetrieveCasts(&sortables, metadataMap)
	}

	RetrieveViews(&sortables)
	sequences, sequenceOwnerColumns := RetrieveSequences()
	BackupCreateSequences(metadataFile, sequences, relationMetadata)
	constraints, conMetadata := RetrieveConstraints()

	BackupDependentObjects(metadataFile, tables, protocols, metadataMap, constraints, sortables, funcInfoMap, tableOnly)

	PrintAlterSequenceStatements(metadataFile, globalTOC, sequences, sequenceOwnerColumns)

	BackupConversions(metadataFile)
	BackupConstraints(metadataFile, constraints, conMetadata)
	if wasTerminated {
		gplog.Info("Pre-data metadata backup incomplete")
	} else {
		gplog.Info("Pre-data metadata backup complete")
	}
}

func backupData(tables []Table) {
	if len(tables) == 0 {
		// No incremental data changes to backup
		gplog.Info("No tables to backup")
		gplog.Info("Data backup complete")
		return
	}

	if MustGetFlagBool(utils.SINGLE_DATA_FILE) {
		gplog.Verbose("Initializing pipes and gpbackup_helper on segments for single data file backup")
		utils.VerifyHelperVersionOnSegments(version, globalCluster)
		oidList := make([]string, 0, len(tables))
		for _, table := range tables {
			if !table.SkipDataBackup() {
				oidList = append(oidList, fmt.Sprintf("%d", table.Oid))
			}
		}
		utils.WriteOidListToSegments(oidList, globalCluster, globalFPInfo)
		utils.CreateFirstSegmentPipeOnAllHosts(oidList[0], globalCluster, globalFPInfo)
		compressStr := fmt.Sprintf(" --compression-level %d", MustGetFlagInt(utils.COMPRESSION_LEVEL))
		if MustGetFlagBool(utils.NO_COMPRESSION) {
			compressStr = " --compression-level 0"
		}
		// Do not pass through the --on-error-continue flag because it does not apply to gpbackup
		utils.StartGpbackupHelpers(globalCluster, globalFPInfo, "--backup-agent",
			MustGetFlagString(utils.PLUGIN_CONFIG), compressStr, false)
	}
	gplog.Info("Writing data to file")
	rowsCopiedMaps := BackupDataForAllTables(tables)
	AddTableDataEntriesToTOC(tables, rowsCopiedMaps)
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
	if connectionPool.Version.AtLeast("6") {
		BackupDefaultPrivileges(metadataFile)
		if len(MustGetFlagStringSlice(utils.INCLUDE_SCHEMA)) == 0 {
			BackupEventTriggers(metadataFile)
		}
	}
	if wasTerminated {
		gplog.Info("Post-data metadata backup incomplete")
	} else {
		gplog.Info("Post-data metadata backup complete")
	}
}

func backupStatistics(tables []Table) {
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
	backupFailed := false
	defer func() {
		DoCleanup(backupFailed)

		errorCode := gplog.GetErrorCode()
		if errorCode == 0 {
			gplog.Info("Backup completed successfully")
		}
		os.Exit(errorCode)
	}()

	errStr := ""
	if err := recover(); err != nil {
		// gplog's Fatal will cause a panic with error code 2
		if gplog.GetErrorCode() != 2 {
			gplog.Error(fmt.Sprintf("%v: %s", err, debug.Stack()))
			gplog.SetErrorCode(2)
		} else {
			errStr = fmt.Sprintf("%v", err)
		}
		backupFailed = true
	}
	if wasTerminated {
		/*
		 * Don't print an error or create a report file if the backup was canceled,
		 * as the signal handler will take care of cleanup and return codes.  Just
		 * wait until the signal handler's DoCleanup completes so the main goroutine
		 * doesn't exit while cleanup is still in progress.
		 */
		CleanupGroup.Wait()
		backupFailed = true
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
			backup_history.WriteConfigFile(&backupReport.BackupConfig, configFilename)
			endtime, _ := time.ParseInLocation("20060102150405", backupReport.BackupConfig.EndTime, operating.System.Local)
			backupReport.WriteBackupReportFile(reportFilename, globalFPInfo.Timestamp, endtime, objectCounts, errMsg)
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
			pluginConfig.DeletePluginConfigWhenEncrypting(globalCluster)
		}
	}
}

func DoCleanup(backupFailed bool) {
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
			if backupFailed {
				// Cleanup only if terminated or fataled
				utils.CleanUpSegmentHelperProcesses(globalCluster, globalFPInfo, "backup")
			}
			if wasTerminated {
				// It is possible for the COPY command to become orphaned if an agent process is killed
				utils.TerminateHangingCopySessions(connectionPool, globalFPInfo, "gpbackup")
			}
			utils.CleanUpHelperFilesOnAllHosts(globalCluster, globalFPInfo)
		}
	}
	err := backupLockFile.Unlock()
	if err != nil && backupLockFile != "" {
		gplog.Warn("Failed to remove lock file %s.", backupLockFile)
	}
	if connectionPool != nil {
		connectionPool.Close()
	}
}

func GetVersion() string {
	return version
}
