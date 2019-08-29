package restore

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gp-common-go-libs/iohelper"
	"github.com/greenplum-db/gpbackup/backup_filepath"
	"github.com/greenplum-db/gpbackup/backup_history"
	"github.com/greenplum-db/gpbackup/utils"
)

/*
 * This file contains wrapper functions that group together functions relating
 * to querying and restoring metadata, so that the logic for each object type
 * can all be in one place and restore.go can serve as a high-level look at the
 * overall restore flow.
 */

/*
 * Setup and validation wrapper functions
 */

func SetLoggerVerbosity() {
	if MustGetFlagBool(utils.QUIET) {
		gplog.SetVerbosity(gplog.LOGERROR)
	} else if MustGetFlagBool(utils.DEBUG) {
		gplog.SetVerbosity(gplog.LOGDEBUG)
	} else if MustGetFlagBool(utils.VERBOSE) {
		gplog.SetVerbosity(gplog.LOGVERBOSE)
	}
}

func InitializeConnectionPool(unquotedDBName string) {
	connectionPool = dbconn.NewDBConnFromEnvironment(unquotedDBName)
	connectionPool.MustConnect(MustGetFlagInt(utils.JOBS))
	utils.ValidateGPDBVersionCompatibility(connectionPool)
	setupQuery := `
SET application_name TO 'gprestore';
SET search_path TO pg_catalog;
SET gp_default_storage_options='';
SET statement_timeout = 0;
SET check_function_bodies = false;
SET client_min_messages = error;
SET standard_conforming_strings = on;
SET default_with_oids = off;
`
	if connectionPool.Version.Is("4") {
		setupQuery += "SET gp_strict_xml_parse = off;\n"
	}
	if connectionPool.Version.AtLeast("5") {
		setupQuery += "SET gp_ignore_error_table = on;\n"
	}
	if connectionPool.Version.Before("6") {
		setupQuery += "SET allow_system_table_mods = 'DML';\n"
	}
	if connectionPool.Version.AtLeast("6") {
		setupQuery += "SET allow_system_table_mods = true;\n"
		setupQuery += "SET lock_timeout = 0;\n"
		setupQuery += "SET default_transaction_read_only = off;\n"
	}
	setupQuery += SetMaxCsvLineLengthQuery(connectionPool)

	for i := 0; i < connectionPool.NumConns; i++ {
		connectionPool.MustExec(setupQuery, i)
	}
}

func SetMaxCsvLineLengthQuery(connectionPool *dbconn.DBConn) string {
	if connectionPool.Version.AtLeast("6") {
		return ""
	}

	var maxLineLength int
	if connectionPool.Version.Is("4") && connectionPool.Version.AtLeast("4.3.30") {
		maxLineLength = 1024 * 1024 * 1024 // 1GB
	} else if connectionPool.Version.Is("5") && connectionPool.Version.AtLeast("5.11.0") {
		maxLineLength = 1024 * 1024 * 1024
	} else {
		maxLineLength = 4 * 1024 * 1024 // 4MB
	}

	return fmt.Sprintf("SET gp_max_csv_line_length = %d;\n", maxLineLength)
}

func InitializeBackupConfig() {
	backupConfig = backup_history.ReadConfigFile(globalFPInfo.GetConfigFilePath())
	utils.InitializePipeThroughParameters(backupConfig.Compressed, 0)
	utils.EnsureBackupVersionCompatibility(backupConfig.BackupVersion, version)
	utils.EnsureDatabaseVersionCompatibility(backupConfig.DatabaseVersion, connectionPool.Version)
}

func InitializeFilterLists() {
	if MustGetFlagString(utils.INCLUDE_RELATION_FILE) != "" {
		includeRelations := strings.Join(iohelper.MustReadLinesFromFile(MustGetFlagString(utils.INCLUDE_RELATION_FILE)), ",")
		err := cmdFlags.Set(utils.INCLUDE_RELATION, includeRelations)
		gplog.FatalOnError(err)
	}
	if MustGetFlagString(utils.EXCLUDE_RELATION_FILE) != "" {
		excludeRelations := strings.Join(iohelper.MustReadLinesFromFile(MustGetFlagString(utils.EXCLUDE_RELATION_FILE)), ",")
		err := cmdFlags.Set(utils.EXCLUDE_RELATION, excludeRelations)
		gplog.FatalOnError(err)
	}
}

func BackupConfigurationValidation() {
	InitializeFilterLists()

	gplog.Verbose("Gathering information on backup directories")
	VerifyBackupDirectoriesExistOnAllHosts()

	VerifyMetadataFilePaths(MustGetFlagBool(utils.WITH_STATS))

	tocFilename := globalFPInfo.GetTOCFilePath()
	globalTOC = utils.NewTOC(tocFilename)
	globalTOC.InitializeMetadataEntryMap()

	// Legacy backups prior to the incremental feature would have no restoreplan yaml element
	if isLegacyBackup := backupConfig.RestorePlan == nil; isLegacyBackup {
		SetRestorePlanForLegacyBackup(globalTOC, globalFPInfo.Timestamp, backupConfig)
	}

	ValidateBackupFlagCombinations()

	validateFilterListsInBackupSet()
}

func SetRestorePlanForLegacyBackup(toc *utils.TOC, backupTimestamp string, backupConfig *backup_history.BackupConfig) {
	tableFQNs := make([]string, 0, len(toc.DataEntries))
	for _, entry := range toc.DataEntries {
		entryFQN := utils.MakeFQN(entry.Schema, entry.Name)
		tableFQNs = append(tableFQNs, entryFQN)
	}
	backupConfig.RestorePlan = []backup_history.RestorePlanEntry{
		{Timestamp: backupTimestamp, TableFQNs: tableFQNs},
	}
}

func RecoverMetadataFilesUsingPlugin() {
	var err error
	pluginConfig, err = utils.ReadPluginConfig(MustGetFlagString(utils.PLUGIN_CONFIG))
	gplog.FatalOnError(err)
	configFilename := filepath.Base(pluginConfig.ConfigPath)
	configDirname := filepath.Dir(pluginConfig.ConfigPath)
	pluginConfig.ConfigPath = filepath.Join(configDirname, backup_history.CurrentTimestamp()+"_"+configFilename)
	_ = cmdFlags.Set(utils.PLUGIN_CONFIG, pluginConfig.ConfigPath)
	gplog.Info("plugin config path: %s", pluginConfig.ConfigPath)

	pluginConfig.CheckPluginExistsOnAllHosts(globalCluster)

	timestamp := MustGetFlagString(utils.TIMESTAMP)
	historicalPluginVersion := FindHistoricalPluginVersion(timestamp)
	pluginConfig.SetBackupPluginVersion(timestamp, historicalPluginVersion)

	pluginConfig.CopyPluginConfigToAllHosts(globalCluster)
	pluginConfig.SetupPluginForRestore(globalCluster, globalFPInfo)

	metadataFiles := []string{globalFPInfo.GetConfigFilePath(), globalFPInfo.GetMetadataFilePath(),
		globalFPInfo.GetBackupReportFilePath()}
	if MustGetFlagBool(utils.WITH_STATS) {
		metadataFiles = append(metadataFiles, globalFPInfo.GetStatisticsFilePath())
	}
	for _, filename := range metadataFiles {
		pluginConfig.MustRestoreFile(filename)
	}

	InitializeBackupConfig()

	var fpInfoList []backup_filepath.FilePathInfo
	if backupConfig.MetadataOnly {
		fpInfoList = []backup_filepath.FilePathInfo{globalFPInfo}
	} else {
		fpInfoList = GetBackupFPInfoListFromRestorePlan()
	}

	for _, fpInfo := range fpInfoList {
		pluginConfig.MustRestoreFile(fpInfo.GetTOCFilePath())
		if backupConfig.SingleDataFile {
			pluginConfig.RestoreSegmentTOCs(globalCluster, fpInfo)
		}
	}
}

func FindHistoricalPluginVersion(timestamp string) string {
	// in order for plugins to implement backwards compatibility,
	// first, read history from master and provide the historical version
	// of the plugin that was used to create the original backup

	// adapted from incremental GetLatestMatchingBackupTimestamp
	var historicalPluginVersion string
	if iohelper.FileExistsAndIsReadable(globalFPInfo.GetBackupHistoryFilePath()) {
		history, err := backup_history.NewHistory(globalFPInfo.GetBackupHistoryFilePath())
		gplog.FatalOnError(err)
		foundBackupConfig := history.FindBackupConfig(timestamp)
		if foundBackupConfig != nil {
			historicalPluginVersion = foundBackupConfig.PluginVersion
		}
	}
	return historicalPluginVersion
}

/*
 * Metadata and/or data restore wrapper functions
 */

func GetRestoreMetadataStatements(section string, filename string, includeObjectTypes []string, excludeObjectTypes []string, filterSchemas bool, filterRelations bool) []utils.StatementWithType {
	metadataFile := iohelper.MustOpenFileForReading(filename)
	var statements []utils.StatementWithType
	var inSchemas, exSchemas, inRelations, exRelations []string
	if len(includeObjectTypes) > 0 || len(excludeObjectTypes) > 0 || filterSchemas || filterRelations {
		if filterSchemas {
			inSchemas = MustGetFlagStringSlice(utils.INCLUDE_SCHEMA)
			exSchemas = MustGetFlagStringSlice(utils.EXCLUDE_SCHEMA)
		}
		if filterRelations {
			inRelations = MustGetFlagStringSlice(utils.INCLUDE_RELATION)
			exRelations = MustGetFlagStringSlice(utils.EXCLUDE_RELATION)
			fpInfoList := GetBackupFPInfoListFromRestorePlan()
			for _, fpInfo := range fpInfoList {
				tocFilename := fpInfo.GetTOCFilePath()
				toc := utils.NewTOC(tocFilename)
				inRelations = append(inRelations, utils.GetIncludedPartitionRoots(toc.DataEntries, inRelations)...)
			}
		}
	}
	statements = globalTOC.GetSQLStatementForObjectTypes(section, metadataFile, includeObjectTypes, excludeObjectTypes, inSchemas, exSchemas, inRelations, exRelations)
	return statements
}

func ExecuteRestoreMetadataStatements(statements []utils.StatementWithType, objectsTitle string, progressBar utils.ProgressBar, showProgressBar int, executeInParallel bool) {
	if progressBar == nil {
		ExecuteStatementsAndCreateProgressBar(statements, objectsTitle, showProgressBar, executeInParallel)
	} else {
		ExecuteStatements(statements, progressBar, executeInParallel)
	}
}

func GetBackupFPInfoListFromRestorePlan() []backup_filepath.FilePathInfo {
	fpInfoList := make([]backup_filepath.FilePathInfo, 0)
	for _, entry := range backupConfig.RestorePlan {
		segPrefix := backup_filepath.ParseSegPrefix(MustGetFlagString(utils.BACKUP_DIR), entry.Timestamp)

		fpInfo := backup_filepath.NewFilePathInfo(globalCluster, MustGetFlagString(utils.BACKUP_DIR), entry.Timestamp, segPrefix)
		fpInfoList = append(fpInfoList, fpInfo)
	}

	return fpInfoList
}

/*
 * The first time this function is called, it retrieves the session GUCs from the
 * predata file and processes them appropriately, then it returns them so they
 * can be used in later calls without the file access and processing overhead.
 */
func setGUCsForConnection(gucStatements []utils.StatementWithType, whichConn int) []utils.StatementWithType {
	if gucStatements == nil {
		objectTypes := []string{"SESSION GUCS"}
		gucStatements = GetRestoreMetadataStatements("global", globalFPInfo.GetMetadataFilePath(), objectTypes, []string{}, false, false)
	}
	ExecuteStatementsAndCreateProgressBar(gucStatements, "", utils.PB_NONE, false, whichConn)
	return gucStatements
}

func RestoreSchemas(schemaStatements []utils.StatementWithType, progressBar utils.ProgressBar) {
	numErrors := 0
	for _, schema := range schemaStatements {
		_, err := connectionPool.Exec(schema.Statement, 0)
		if err != nil {
			if strings.Contains(err.Error(), "already exists") {
				gplog.Warn("Schema %s already exists", schema.Name)
			} else {
				errMsg := fmt.Sprintf("Error encountered while creating schema %s", schema.Name)
				if MustGetFlagBool(utils.ON_ERROR_CONTINUE) {
					gplog.Verbose(fmt.Sprintf("%s: %s", errMsg, err.Error()))
					numErrors++
				} else {
					gplog.Fatal(err, errMsg)
				}
			}
		}
		progressBar.Increment()
	}
	if numErrors > 0 {
		gplog.Error("Encountered %d errors during schema restore; see log file %s for a list of errors.", numErrors, gplog.GetLogFilePath())
	}
}
