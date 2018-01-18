package restore

import (
	"fmt"

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
	if *quiet {
		logger.SetVerbosity(utils.LOGERROR)
	} else if *debug {
		logger.SetVerbosity(utils.LOGDEBUG)
	} else if *verbose {
		logger.SetVerbosity(utils.LOGVERBOSE)
	}
}

func InitializeConnection(dbname string) {
	connection = utils.NewDBConn(dbname)
	connection.Connect(*numJobs)
	connection.MustExec("SET application_name TO 'gprestore'")
	connection.SetDatabaseVersion()
	connection.MustExec("SET search_path TO pg_catalog")
	connection.MustExec("SET gp_enable_segment_copy_checking TO false")
	connection.MustExec("SET gp_default_storage_options='';")
}

func InitializeBackupConfig() {
	backupConfig = utils.ReadConfigFile(globalCluster.GetConfigFilePath())
	utils.InitializeCompressionParameters(backupConfig.Compressed, 0)
	utils.EnsureBackupVersionCompatibility(backupConfig.BackupVersion, version)
	utils.EnsureDatabaseVersionCompatibility(backupConfig.DatabaseVersion, connection.Version)
}

func InitializeFilterLists() {
	if *includeTableFile != "" {
		includeTables = utils.ReadLinesFromFile(*includeTableFile)
	}
}

/*
 * This function is for any validation that requires a database connection but
 * does not specifically need to connect to the restore database.
 */
func DoPostgresValidation() {
	InitializeFilterLists()

	logger.Verbose("Gathering information on backup directories")
	segConfig := utils.GetSegmentConfiguration(connection)
	globalCluster = utils.NewCluster(segConfig, *backupDir, *timestamp, "")
	globalCluster.UserSpecifiedSegPrefix = utils.ParseSegPrefix(*backupDir)
	VerifyBackupDirectoriesExistOnAllHosts(globalCluster)

	InitializeBackupConfig()
	ValidateBackupFlagCombinations()
	VerifyMetadataFilePaths(globalCluster, backupConfig.DataOnly, *withStats)

	tocFilename := globalCluster.GetTOCFilePath()
	globalTOC = utils.NewTOC(tocFilename)
	globalTOC.InitializeEntryMap()

	validateFilterListsInBackupSet()
}

func ConnectToRestoreDatabase() {
	restoreDatabase := ""
	if *redirect != "" {
		restoreDatabase = *redirect
	} else {
		restoreDatabase = backupConfig.DatabaseName
	}
	InitializeConnection(restoreDatabase)
}

func DoRestoreDatabaseValidation() {
	validateFilterListsInRestoreDatabase()
}

/*
 * Metadata and/or data restore wrapper functions
 */

func GetRestoreMetadataStatements(section string, filename string, objectTypes []string, includeSchemas []string, includeTables []string) []utils.StatementWithType {
	metadataFile := utils.MustOpenFileForReading(filename)
	var statements []utils.StatementWithType
	if len(objectTypes) > 0 || len(includeSchemas) > 0 || len(includeTables) > 0 {
		statements = globalTOC.GetSQLStatementForObjectTypes(section, metadataFile, objectTypes, includeSchemas, includeTables)
	} else {
		statements = globalTOC.GetAllSQLStatements(section, metadataFile)
	}
	return statements
}

func ExecuteRestoreMetadataStatements(statements []utils.StatementWithType, objectsTitle string, progressBar utils.ProgressBar, showProgressBar int, executeInParallel bool) {
	var shouldExecute *utils.FilterSet
	if connection.Version.AtLeast("5") {
		shouldExecute = utils.NewExcludeSet([]string{"GPDB4 SESSION GUCS"})
	} else {
		shouldExecute = utils.NewEmptyIncludeSet()
	}
	if progressBar == nil {
		ExecuteStatementsAndCreateProgressBar(statements, objectsTitle, showProgressBar, shouldExecute, executeInParallel)
	} else {
		ExecuteStatements(statements, progressBar, showProgressBar, shouldExecute, executeInParallel)
	}
}

/*
 * The first time this function is called, it retrieves the session GUCs from the
 * predata file and processes them appropriately, then it returns them so they
 * can be used in later calls without the file access and processing overhead.
 */
func setGUCsForConnection(gucStatements []utils.StatementWithType, whichConn int) []utils.StatementWithType {
	if gucStatements == nil {
		objectTypes := []string{"SESSION GUCS"}
		if connection.Version.Before("5") {
			objectTypes = append(objectTypes, "GPDB4 SESSION GUCS")
		}
		gucStatements = GetRestoreMetadataStatements("global", globalCluster.GetMetadataFilePath(), objectTypes, []string{}, []string{})
		// We only need to set the following GUC for data restores, but it doesn't hurt if we set it for metadata restores as well.
		gucStatements = append(gucStatements, utils.StatementWithType{ObjectType: "SESSION GUCS", Statement: "SET gp_enable_segment_copy_checking TO false;"})
	}
	ExecuteStatementsAndCreateProgressBar(gucStatements, "", utils.PB_NONE, utils.NewEmptyIncludeSet(), false, whichConn)
	return gucStatements
}

func restoreSingleTableData(entry utils.MasterDataEntry, tableNum uint32, totalTables int, whichConn int) {
	name := utils.MakeFQN(entry.Schema, entry.Name)
	if logger.GetVerbosity() > utils.LOGINFO {
		// No progress bar at this log level, so we note table count here
		logger.Verbose("Reading data for table %s from file (table %d of %d)", name, tableNum, totalTables)
	} else {
		logger.Verbose("Reading data for table %s from file", name)
	}
	backupFile := ""
	if backupConfig.SingleDataFile {
		backupFile = fmt.Sprintf("%s_%d", globalCluster.GetSegmentPipePathForCopyCommand(), globalCluster.PID)
	} else {
		backupFile = globalCluster.GetTableBackupFilePathForCopyCommand(entry.Oid, backupConfig.SingleDataFile)
	}
	CopyTableIn(connection, name, entry.AttributeString, backupFile, backupConfig.SingleDataFile, whichConn, entry.Oid)
}
