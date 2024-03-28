package backup

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	path "path/filepath"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/greenplum-db/gpbackup/filepath"
	"github.com/greenplum-db/gpbackup/history"
	"github.com/greenplum-db/gpbackup/options"
	"github.com/greenplum-db/gpbackup/report"
	"github.com/greenplum-db/gpbackup/toc"
	"github.com/greenplum-db/gpbackup/utils"
	"github.com/spf13/cobra"
)

// This function handles setup that can be done before parsing flags.
func DoInit(cmd *cobra.Command) {
	CleanupGroup = &sync.WaitGroup{}
	CleanupGroup.Add(1)
	gplog.InitializeLogging("gpbackup", "")
	SetCmdFlags(cmd.Flags())
	_ = cmd.MarkFlagRequired(options.DBNAME)
	utils.InitializeSignalHandler(DoCleanup, "backup process", &wasTerminated)
	objectCounts = make(map[string]int)
}

func DoFlagValidation(cmd *cobra.Command) {
	validateFlagCombinations(cmd.Flags())
	validateFlagValues()
}

// This function handles setup that must be done after parsing flags.
func DoSetup() {
	SetLoggerVerbosity()
	gplog.Verbose("Backup Command: %s", os.Args)
	gplog.Info("gpbackup version = %s", GetVersion())

	utils.CheckGpexpandRunning(utils.BackupPreventedByGpexpandMessage)
	timestamp := history.CurrentTimestamp()
	createBackupLockFile(timestamp)
	initializeConnectionPool(timestamp)
	gplog.Info("Greenplum Database Version = %s", connectionPool.Version.VersionString)

	gplog.Info("Starting backup of database %s", MustGetFlagString(options.DBNAME))
	opts, err := options.NewOptions(cmdFlags)
	gplog.FatalOnError(err)

	ValidateAndProcessFilterLists(opts)
	includeOids := GetOidsFromRelationList(IncludedRelationFqns)
	err = ExpandIncludesForPartitions(connectionPool, opts, includeOids, cmdFlags)
	gplog.FatalOnError(err)

	clusterConfigConn := dbconn.NewDBConnFromEnvironment(MustGetFlagString(options.DBNAME))
	clusterConfigConn.MustConnect(1)

	segConfig := cluster.MustGetSegmentConfiguration(clusterConfigConn)
	globalCluster = cluster.NewCluster(segConfig)
	segPrefix := ""
	if !MustGetFlagBool(options.SINGLE_BACKUP_DIR) {
		segPrefix = filepath.GetSegPrefix(clusterConfigConn)
	}
	clusterConfigConn.Close()

	globalFPInfo = filepath.NewFilePathInfo(globalCluster, MustGetFlagString(options.BACKUP_DIR), timestamp, segPrefix, MustGetFlagBool(options.SINGLE_BACKUP_DIR))
	if MustGetFlagBool(options.METADATA_ONLY) {
		_, err = globalCluster.ExecuteLocalCommand(fmt.Sprintf("mkdir -p %s", globalFPInfo.GetDirForContent(-1)))
		gplog.FatalOnError(err)
	} else {
		createBackupDirectoriesOnAllHosts()
	}
	globalTOC = &toc.TOC{}
	globalTOC.InitializeMetadataEntryMap()
	utils.InitializePipeThroughParameters(!MustGetFlagBool(options.NO_COMPRESSION), MustGetFlagString(options.COMPRESSION_TYPE), MustGetFlagInt(options.COMPRESSION_LEVEL))
	getQuotedRoleNames(connectionPool)

	pluginConfigFlag := MustGetFlagString(options.PLUGIN_CONFIG)

	if pluginConfigFlag != "" {
		pluginConfig, err = utils.ReadPluginConfig(pluginConfigFlag)
		gplog.FatalOnError(err)
		configFilename := path.Base(pluginConfig.ConfigPath)
		configDirname := path.Dir(pluginConfig.ConfigPath)
		pluginConfig.ConfigPath = path.Join(configDirname, timestamp+"_"+configFilename)
		_ = cmdFlags.Set(options.PLUGIN_CONFIG, pluginConfig.ConfigPath)
		gplog.Debug("Plugin config path: %s", pluginConfig.ConfigPath)
	}

	initializeBackupReport(*opts)

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

	pluginConfigFlag := MustGetFlagString(options.PLUGIN_CONFIG)
	targetBackupTimestamp := ""
	var targetBackupFPInfo filepath.FilePathInfo
	if MustGetFlagBool(options.INCREMENTAL) {
		targetBackupTimestamp = GetTargetBackupTimestamp()

		targetBackupFPInfo = filepath.NewFilePathInfo(globalCluster, globalFPInfo.UserSpecifiedBackupDir,
			targetBackupTimestamp, globalFPInfo.UserSpecifiedSegPrefix, globalFPInfo.SingleBackupDir)

		if pluginConfigFlag != "" {
			// These files need to be downloaded from the remote system into the local filesystem
			pluginConfig.MustRestoreFile(targetBackupFPInfo.GetConfigFilePath())
			pluginConfig.MustRestoreFile(targetBackupFPInfo.GetTOCFilePath())
			pluginConfig.MustRestoreFile(targetBackupFPInfo.GetPluginConfigPath())
		}
	}

	gplog.Info("Gathering table state information")
	metadataTables, dataTables := RetrieveAndProcessTables()
	dataTables, numExtOrForeignTables := GetBackupDataSet(dataTables)
	if len(dataTables) == 0 && !backupReport.MetadataOnly {
		gplog.Warn("No tables in backup set contain data. Performing metadata-only backup instead.")
		backupReport.MetadataOnly = true
	}
	// This must be a full backup with --leaf-parition-data to query for incremental metadata
	if !(MustGetFlagBool(options.METADATA_ONLY) || MustGetFlagBool(options.DATA_ONLY)) && MustGetFlagBool(options.LEAF_PARTITION_DATA) {
		backupIncrementalMetadata()
	} else {
		gplog.Verbose("Skipping query for incremental metadata.")
	}

	metadataFilename := globalFPInfo.GetMetadataFilePath()
	gplog.Info("Metadata will be written to %s", metadataFilename)
	metadataFile := utils.NewFileWithByteCountFromFile(metadataFilename)

	/*
	 * We check this in the backup report rather than the flag because we
	 * perform a metadata only backup if the database contains no tables
	 * or only external tables
	 */
	backupSetTables := dataTables
	if !backupReport.MetadataOnly {
		targetBackupRestorePlan := make([]history.RestorePlanEntry, 0)
		if targetBackupTimestamp != "" {
			gplog.Info("Basing incremental backup off of backup with timestamp = %s", targetBackupTimestamp)

			targetBackupTOC := toc.NewTOC(targetBackupFPInfo.GetTOCFilePath())
			targetBackupRestorePlan = history.ReadConfigFile(targetBackupFPInfo.GetConfigFilePath()).RestorePlan
			backupSetTables = FilterTablesForIncremental(targetBackupTOC, globalTOC, dataTables)
		}

		backupReport.RestorePlan = PopulateRestorePlan(backupSetTables, targetBackupRestorePlan, dataTables)
	}

	// As soon as all necessary data is available, capture the backup into history database
	if !MustGetFlagBool(options.NO_HISTORY) {
		historyDBName := globalFPInfo.GetBackupHistoryDatabasePath()
		historyDB, err := history.InitializeHistoryDatabase(historyDBName)
		if err != nil {
			gplog.FatalOnError(err)
		} else {
			err = history.StoreBackupHistory(historyDB, &backupReport.BackupConfig)
			historyDB.Close()
			gplog.FatalOnError(err)
		}
	}

	backupSessionGUC(metadataFile)
	if !MustGetFlagBool(options.DATA_ONLY) {
		isFullBackup := len(MustGetFlagStringArray(options.INCLUDE_RELATION)) == 0
		if isFullBackup && !MustGetFlagBool(options.WITHOUT_GLOBALS) {
			backupGlobals(metadataFile)
		}

		isFilteredBackup := !isFullBackup
		backupPredata(metadataFile, metadataTables, isFilteredBackup)
		backupPostdata(metadataFile)
	}

	if !backupReport.MetadataOnly {
		backupData(backupSetTables)
	}

	printDataBackupWarnings(numExtOrForeignTables)
	if MustGetFlagBool(options.WITH_STATS) {
		backupStatistics(metadataTables)
	}

	globalTOC.WriteToFileAndMakeReadOnly(globalFPInfo.GetTOCFilePath())
	for connNum := 0; connNum < connectionPool.NumConns; connNum++ {
		// COMMIT TRANSACTION
		// The transaction could have been rollbacked already
		// during COPY step due to deadlock handling.
		if connectionPool.Tx[connNum] != nil {
			connectionPool.MustCommit(connNum)
		}
	}
	metadataFile.Close()
	if pluginConfigFlag != "" {
		pluginConfig.MustBackupFile(metadataFilename)
		pluginConfig.MustBackupFile(globalFPInfo.GetTOCFilePath())
		if MustGetFlagBool(options.WITH_STATS) {
			pluginConfig.MustBackupFile(globalFPInfo.GetStatisticsFilePath())
		}
		_ = utils.CopyFile(pluginConfigFlag, globalFPInfo.GetPluginConfigPath())
		pluginConfig.MustBackupFile(globalFPInfo.GetPluginConfigPath())
	}
}

func backupGlobals(metadataFile *utils.FileWithByteCount) {
	gplog.Info("Writing global database metadata")

	backupResourceQueues(metadataFile)
	backupResourceGroups(metadataFile)
	backupRoles(metadataFile)
	backupRoleGrants(metadataFile)
	backupTablespaces(metadataFile)
	backupCreateDatabase(metadataFile)
	backupDatabaseGUCs(metadataFile)
	backupRoleGUCs(metadataFile)

	logCompletionMessage("Global database metadata backup")
}

func backupPredata(metadataFile *utils.FileWithByteCount, tables []Table, tableOnly bool) {
	if wasTerminated {
		return
	}
	gplog.Info("Writing pre-data metadata")

	var protocols []ExternalProtocol
	var functions []Function
	var funcInfoMap map[uint32]FunctionInfo
	objects := make([]Sortable, 0)
	metadataMap := make(MetadataMap)

	if !tableOnly {
		functions, funcInfoMap = retrieveFunctions(&objects, metadataMap)
	}
	objects = append(objects, convertToSortableSlice(tables)...)
	relationMetadata := GetMetadataForObjectType(connectionPool, TYPE_RELATION)
	addToMetadataMap(relationMetadata, metadataMap)

	if !tableOnly {
		protocols = retrieveProtocols(&objects, metadataMap)
		backupSchemas(metadataFile, createAlteredPartitionSchemaSet(tables))
		backupExtensions(metadataFile)
		backupCollations(metadataFile)
		retrieveAndBackupTypes(metadataFile, &objects, metadataMap)

		if len(MustGetFlagStringArray(options.INCLUDE_SCHEMA)) == 0 {
			backupProceduralLanguages(metadataFile, functions, funcInfoMap, metadataMap)
			retrieveTransforms(&objects)
			retrieveFDWObjects(&objects, metadataMap)
		}

		retrieveTSObjects(&objects, metadataMap)
		backupOperatorFamilies(metadataFile)
		retrieveOperatorObjects(&objects, metadataMap)
		retrieveAggregates(&objects, metadataMap)
		retrieveCasts(&objects, metadataMap)
		backupAccessMethods(metadataFile)
	}

	retrieveViews(&objects)
	sequences := retrieveAndBackupSequences(metadataFile, relationMetadata)
	domainConstraints, nonDomainConstraints, conMetadata := retrieveConstraints(&objects, metadataMap)

	viewsDependingOnConstraints := backupDependentObjects(metadataFile, tables, protocols, metadataMap, domainConstraints, objects, sequences, funcInfoMap, tableOnly)

	backupConversions(metadataFile)

	// These two are actually in postdata, but we print them here to avoid passing information around too much
	backupConstraints(metadataFile, nonDomainConstraints, conMetadata)
	backupViewsDependingOnConstraints(metadataFile, viewsDependingOnConstraints)

	logCompletionMessage("Pre-data metadata metadata backup")
}

func backupData(tables []Table) {
	if len(tables) == 0 {
		// No incremental data changes to backup
		gplog.Info("No tables to backup")
		gplog.Info("Data backup complete")
		return
	}
	if MustGetFlagBool(options.SINGLE_DATA_FILE) {
		gplog.Verbose("Initializing pipes and gpbackup_helper on segments for single data file backup")
		utils.VerifyHelperVersionOnSegments(version, globalCluster)
		oidList := make([]string, 0, len(tables))
		for _, table := range tables {
			oidList = append(oidList, fmt.Sprintf("%d", table.Oid))
		}
		utils.WriteOidListToSegments(oidList, globalCluster, globalFPInfo, "oid")
		compressStr := fmt.Sprintf(" --compression-level %d --compression-type %s", MustGetFlagInt(options.COMPRESSION_LEVEL), MustGetFlagString(options.COMPRESSION_TYPE))
		if MustGetFlagBool(options.NO_COMPRESSION) {
			compressStr = " --compression-level 0"
		}
		initialPipes := CreateInitialSegmentPipes(oidList, globalCluster, connectionPool, globalFPInfo)
		// Do not pass through the --on-error-continue flag or the resizeClusterMap because neither apply to gpbackup
		utils.StartGpbackupHelpers(globalCluster, globalFPInfo, "--backup-agent",
			MustGetFlagString(options.PLUGIN_CONFIG), compressStr, false, false, &wasTerminated, initialPipes, true, false, 0, 0)
	}
	gplog.Info("Writing data to file")
	rowsCopiedMaps := BackupDataForAllTables(tables)
	AddTableDataEntriesToTOC(tables, rowsCopiedMaps)
	if MustGetFlagBool(options.SINGLE_DATA_FILE) && MustGetFlagString(options.PLUGIN_CONFIG) != "" {
		pluginConfig.BackupSegmentTOCs(globalCluster, globalFPInfo)
	}
	logCompletionMessage("Data backup")
}

func backupPostdata(metadataFile *utils.FileWithByteCount) {
	if wasTerminated {
		return
	}
	gplog.Info("Writing post-data metadata")

	backupIndexes(metadataFile)
	backupRules(metadataFile)
	backupTriggers(metadataFile)
	if connectionPool.Version.AtLeast("6") {
		backupDefaultPrivileges(metadataFile)
		if len(MustGetFlagStringArray(options.INCLUDE_SCHEMA)) == 0 {
			backupEventTriggers(metadataFile)
		}
	}
	if connectionPool.Version.AtLeast("7") {
		backupRowLevelSecurityPolicies(metadataFile)
		backupExtendedStatistic(metadataFile)
	}

	logCompletionMessage("Post-data metadata backup")
}

func backupStatistics(tables []Table) {
	if wasTerminated {
		return
	}
	statisticsFilename := globalFPInfo.GetStatisticsFilePath()
	gplog.Info("Writing query planner statistics to %s", statisticsFilename)
	statisticsFile := utils.NewFileWithByteCountFromFile(statisticsFilename)
	defer statisticsFile.Close()
	backupTableStatistics(statisticsFile, tables)

	logCompletionMessage("Query planner statistics backup")
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
	errMsg := report.ParseErrorMessage(errStr)

	/*
	 * Only create a report file if we fail after the cluster is initialized
	 * and a backup directory exists in which to create the report file.
	 */
	if globalFPInfo.Timestamp != "" {
		_, statErr := os.Stat(globalFPInfo.GetDirForContent(-1))
		if statErr != nil { // Even if this isn't os.IsNotExist, don't try to write a report file in case of further errors
			return
		}
		historyFileLegacyName := globalFPInfo.GetBackupHistoryFilePath()
		reportFilename := globalFPInfo.GetBackupReportFilePath()
		configFilename := globalFPInfo.GetConfigFilePath()

		time.Sleep(time.Second) // We sleep for 1 second to ensure multiple backups do not start within the same second.

		// Check if legacy history file is still present, log warning if so. Only log if we're planning to use history db.
		var err error
		if _, err = os.Stat(historyFileLegacyName); err == nil && !MustGetFlagBool(options.NO_HISTORY) {
			gplog.Warn("Legacy gpbackup_history file %s is still present. Please run 'gpbackup_manager migrate-history' to add entries from that file to the history database.", historyFileLegacyName)
		}

		if backupReport != nil {
			if backupFailed {
				backupReport.BackupConfig.Status = history.BackupStatusFailed
			} else {
				backupReport.BackupConfig.Status = history.BackupStatusSucceed
			}
			backupReport.ConstructBackupParamsString()

			history.WriteConfigFile(&backupReport.BackupConfig, configFilename)
			// We always want to override the initial end time set by the call to StoreBackupHistory
			backupReport.BackupConfig.EndTime = history.CurrentTimestamp()
			endtime, _ := time.ParseInLocation("20060102150405", backupReport.BackupConfig.EndTime, operating.System.Local)
			backupReport.WriteBackupReportFile(reportFilename, globalFPInfo.Timestamp, endtime, objectCounts, errMsg)
			report.EmailReport(globalCluster, globalFPInfo.Timestamp, reportFilename, "gpbackup", !backupFailed, backupReport.BackupConfig.DatabaseName)
			if pluginConfig != nil {
				err = pluginConfig.BackupFile(configFilename)
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
	cleanupTimeout := 60 * time.Second

	defer func() {
		if err := recover(); err != nil {
			gplog.Warn("Encountered error during cleanup: %v", err)
		}
		if connectionPool != nil {
			connectionPool.Close()
		}
		gplog.Verbose("Cleanup complete")
		CleanupGroup.Done()
	}()

	gplog.Verbose("Beginning cleanup")
	if connectionPool != nil {
		cancelBlockedQueries(globalFPInfo.Timestamp)
	}
	if globalFPInfo.Timestamp != "" && MustGetFlagBool(options.SINGLE_DATA_FILE) {
		// Copy sessions must be terminated before cleaning up gpbackup_helper processes to avoid a potential deadlock
		// If the terminate query is sent via a connection with an active COPY command, and the COPY's pipe is cleaned up, the COPY query will hang.
		// This results in the DoCleanup function passed to the signal handler to never return, blocking the os.Exit call

		// All COPY commands should end on their own for a successful restore, however we cleanup any hanging COPY sessions here as a precaution
		utils.TerminateHangingCopySessions(globalFPInfo, fmt.Sprintf("gpbackup_%s", globalFPInfo.Timestamp), cleanupTimeout, 5 * time.Second)

		// Ensure we don't leave anything behind on the segments
		utils.CleanUpSegmentHelperProcesses(globalCluster, globalFPInfo, "backup", cleanupTimeout)
		utils.CleanUpHelperFilesOnAllHosts(globalCluster, globalFPInfo, cleanupTimeout)
		utils.CleanUpPipesOnAllHosts(globalCluster, globalFPInfo, cleanupTimeout)

		// Check gpbackup_helper errors here if backup was terminated
		if wasTerminated {
			err := utils.CheckAgentErrorsOnSegments(globalCluster, globalFPInfo)
			if err != nil {
				gplog.Error(err.Error())
			}
		}
	}

	// The gpbackup_history entry is written to the DB with an "In Progress" status and a preliminary EndTime value
	// very early on.  If we get to cleanup and the backup succeeded, mark it as a success, otherwise mark it as a
	// failure; in either case, update the end time to the actual value. Between our signal handler and recovering
	// panics, there should be no way for gpbackup to exit that leaves the entry in the initial status.

	if !MustGetFlagBool(options.NO_HISTORY) {
		var statusString string
		if backupFailed {
			statusString = history.BackupStatusFailed
		} else {
			statusString = history.BackupStatusSucceed
		}
		historyDBName := globalFPInfo.GetBackupHistoryDatabasePath()
		historyDB, err := history.InitializeHistoryDatabase(historyDBName)
		if err != nil {
			gplog.Error(fmt.Sprintf("Unable to update history database.  Error: %v", err))
		} else {
			_, err := historyDB.Exec(fmt.Sprintf("UPDATE backups SET status='%s', end_time='%s' WHERE timestamp='%s'", statusString, backupReport.BackupConfig.EndTime, globalFPInfo.Timestamp))
			historyDB.Close()
			if err != nil {
				gplog.Error(fmt.Sprintf("Unable to update history database.  Error: %v", err))
			}
		}
	}

	err := backupLockFile.Unlock()
	if err != nil && backupLockFile != "" {
		gplog.Warn("Failed to remove lock file %s.", backupLockFile)
	}
}

// Cancel blocked gpbackup queries waiting for locks.
func cancelBlockedQueries(timestamp string) {
	conn := dbconn.NewDBConnFromEnvironment(MustGetFlagString(options.DBNAME))
	conn.MustConnect(1)
	defer conn.Close()

	// Query for all blocked queries
	pids := make([]int64, 0)
	var findBlockedQuery string
	if conn.Version.Before("6") {
		findBlockedQuery = fmt.Sprintf("SELECT procpid from pg_stat_activity WHERE application_name='gpbackup_%s' AND waiting='t' AND waiting_reason='lock';", timestamp)
	}
	if conn.Version.Is("6") {
		findBlockedQuery = fmt.Sprintf("SELECT pid from pg_stat_activity WHERE application_name='gpbackup_%s' AND waiting='t' AND waiting_reason='lock';", timestamp)
	} else if conn.Version.AtLeast("7") {
		findBlockedQuery = fmt.Sprintf("SELECT pid from pg_stat_activity WHERE application_name='gpbackup_%s' AND wait_event_type='Lock';", timestamp)
	}
	err := conn.Select(&pids, findBlockedQuery)
	gplog.FatalOnError(err)

	if len(pids) == 0 {
		return
	}

	gplog.Info(fmt.Sprintf("Canceling %d blocked queries", len(pids)))
	// Cancel all gpbackup queries waiting for a lock
	for _, pid := range pids {
		conn.MustExec(fmt.Sprintf("SELECT pg_cancel_backend(%d)", pid))
	}

	// Wait for the cancel queries to finish
	tickerCheckCanceled := time.NewTicker(500 * time.Millisecond)
	var count string
	for {
		select {
		case <-tickerCheckCanceled.C:
			blockedQueryCount := fmt.Sprintf("SELECT count(*) from pg_stat_activity WHERE application_name='gpbackup_%s' AND waiting='t' AND  waiting_reason='lock';", timestamp)
			if conn.Version.AtLeast("7") {
				blockedQueryCount = fmt.Sprintf("SELECT count(*) from pg_stat_activity WHERE application_name='gpbackup_%s' AND wait_event_type='Lock';", timestamp)
			}
			count = dbconn.MustSelectString(conn, blockedQueryCount)
			if count == "0" {
				return
			}
		case <-time.After(20 * time.Second):
			tickerCheckCanceled.Stop()
			gplog.FatalOnError(errors.New("Timeout attempting to cancel blocked queries"))
		}
	}
}

func GetVersion() string {
	return version
}

func logCompletionMessage(msg string) {
	if wasTerminated {
		gplog.Info("%s incomplete", msg)
	} else {
		gplog.Info("%s complete", msg)
	}
}

func CreateInitialSegmentPipes(oidList []string, c *cluster.Cluster, connectionPool *dbconn.DBConn, fpInfo filepath.FilePathInfo) int {
	// Create min(connections, tables) segment pipes on each host
	var maxPipes int
	if connectionPool.NumConns < len(oidList) {
		maxPipes = connectionPool.NumConns
	} else {
		maxPipes = len(oidList)
	}
	for i := 0; i < maxPipes; i++ {
		utils.CreateSegmentPipeOnAllHosts(oidList[i], c, fpInfo)
	}
	return maxPipes
}

type TableLocks struct {
	Oid         uint32
	Database    string
	Relation    string
	Mode        string
	Application string
	Granted     string
	User        string
	Pid         string
}

func getTableLocks(table Table) []TableLocks {
	conn := dbconn.NewDBConnFromEnvironment(MustGetFlagString(options.DBNAME))
	conn.MustConnect(1)
	var query string
	defer conn.Close()
	if conn.Version.Before("6") {
		query = fmt.Sprintf(`
		SELECT c.oid as oid,
		coalesce(a.datname, '') as database,
		n.nspname || '.' || c.relname as relation,
		l.mode,
		l.GRANTED as granted,
		coalesce(a.application_name, '') as application,
		coalesce(a.usename, '') as user,
		a.procpid as pid
		FROM pg_stat_activity a
		JOIN pg_locks l ON l.pid = a.procpid
		JOIN pg_class c on c.oid = l.relation
		JOIN pg_namespace n on n.oid=c.relnamespace
		WHERE (a.datname = '%s' OR a.datname IS NULL)
		AND NOT a.procpid = pg_backend_pid()
		AND relation = '%s'::regclass
		AND mode = 'AccessExclusiveLock'
		ORDER BY a.query_start;
		`, conn.DBName, table.FQN())
	} else {
		query = fmt.Sprintf(`
		SELECT c.oid as oid,
		coalesce(a.datname, '') as database,
		n.nspname || '.' || c.relname relation,
		l.mode,
		l.GRANTED as granted,
		coalesce(a.application_name, '') as application,
		coalesce(a.usename, '') as user,
		a.pid
		FROM pg_stat_activity a
		JOIN pg_locks l ON l.pid = a.pid
		JOIN pg_class c on c.oid = l.relation
		JOIN pg_namespace n on n.oid=c.relnamespace
		WHERE (a.datname = '%s' OR a.datname IS NULL)
		AND NOT a.pid = pg_backend_pid()
		AND relation = '%s'::regclass
		AND mode = 'AccessExclusiveLock'
		ORDER BY a.query_start;
		`, conn.DBName, table.FQN())
	}

	locksResults := make([]TableLocks, 0)
	err := conn.Select(&locksResults, query)
	if err != nil {
		gplog.FatalOnError(err)
	}

	return locksResults
}

func logTableLocks(table Table, whichConn int) {
	locks := getTableLocks(table)
	jsonData, _ := json.Marshal(&locks)
	gplog.Warn("Locks held on table %s: %s", table.FQN(), jsonData)
}
