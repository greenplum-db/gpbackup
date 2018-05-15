package restore

import (
	"fmt"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gp-common-go-libs/iohelper"
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
		gplog.SetVerbosity(gplog.LOGERROR)
	} else if *debug {
		gplog.SetVerbosity(gplog.LOGDEBUG)
	} else if *verbose {
		gplog.SetVerbosity(gplog.LOGVERBOSE)
	}
}

func InitializeConnection(dbname string) {
	connection = dbconn.NewDBConnFromEnvironment(dbname)
	connection.MustConnect(*numJobs)
	utils.SetDatabaseVersion(connection)
	setupQuery := `
SET application_name TO 'gprestore';
SET search_path TO pg_catalog;
SET gp_enable_segment_copy_checking TO false;
SET gp_default_storage_options='';
SET statement_timeout = 0;
SET check_function_bodies = false;
SET client_min_messages = error;
SET standard_conforming_strings = on;
SET default_with_oids = off;
`
	if connection.Version.Before("5") {
		setupQuery += "SET gp_strict_xml_parse = off;\n"
	}
	for i := 0; i < connection.NumConns; i++ {
		connection.MustExec(setupQuery, i)
	}
}

func InitializeBackupConfig() {
	backupConfig = utils.ReadConfigFile(globalFPInfo.GetConfigFilePath())
	utils.InitializeCompressionParameters(backupConfig.Compressed, 0)
	utils.EnsureBackupVersionCompatibility(backupConfig.BackupVersion, version)
	utils.EnsureDatabaseVersionCompatibility(backupConfig.DatabaseVersion, connection.Version)
}

func InitializeFilterLists() {
	if *excludeTableFile != "" {
		excludeTables = iohelper.MustReadLinesFromFile(*excludeTableFile)
	}
	if *includeTableFile != "" {
		includeTables = iohelper.MustReadLinesFromFile(*includeTableFile)
	}
}

func BackupConfigurationValidation() {
	InitializeFilterLists()

	gplog.Verbose("Gathering information on backup directories")
	VerifyBackupDirectoriesExistOnAllHosts()

	VerifyMetadataFilePaths(*withStats)

	tocFilename := globalFPInfo.GetTOCFilePath()
	globalTOC = utils.NewTOC(tocFilename)
	globalTOC.InitializeEntryMap()
	ValidateBackupFlagCombinations()

	validateFilterListsInBackupSet()
}

func RecoverMetadataFilesUsingPlugin() {
	pluginConfig := utils.ReadPluginConfig(*pluginConfigFile)
	pluginConfig.CheckPluginExistsOnAllHosts(globalCluster)
	pluginConfig.CopyPluginConfigToAllHosts(globalCluster, *pluginConfigFile)
	pluginConfig.SetupPluginForRestoreOnAllHosts(globalCluster, pluginConfig.ConfigPath, globalFPInfo.GetDirForContent(-1))
	pluginConfig.RestoreFile(globalFPInfo.GetConfigFilePath())

	InitializeBackupConfig()

	metadataFiles := []string{globalFPInfo.GetMetadataFilePath(), globalFPInfo.GetTOCFilePath(), globalFPInfo.GetBackupReportFilePath()}
	if *withStats {
		metadataFiles = append(metadataFiles, globalFPInfo.GetStatisticsFilePath())
	}
	for _, filename := range metadataFiles {
		pluginConfig.RestoreFile(filename)
	}
	if !backupConfig.MetadataOnly {
		pluginConfig.RestoreSegmentTOCs(globalCluster, globalFPInfo)
	}
}

/*
 * Metadata and/or data restore wrapper functions
 */

func GetRestoreMetadataStatements(section string, filename string, includeObjectTypes []string, excludeObjectTypes []string, filterSchemas bool, filterTables bool) []utils.StatementWithType {
	metadataFile := iohelper.MustOpenFileForReading(filename)
	var statements []utils.StatementWithType
	if len(includeObjectTypes) > 0 || len(excludeObjectTypes) > 0 || filterSchemas || filterTables {
		var inSchemas, exSchemas, inTables, exTables []string
		if filterSchemas {
			inSchemas = includeSchemas
			exSchemas = excludeSchemas
		}
		if filterTables {
			inTables = includeTables
			exTables = excludeTables
		}
		statements = globalTOC.GetSQLStatementForObjectTypes(section, metadataFile, includeObjectTypes, excludeObjectTypes, inSchemas, exSchemas, inTables, exTables)
	} else {
		statements = globalTOC.GetAllSQLStatements(section, metadataFile)
	}
	return statements
}

func ExecuteRestoreMetadataStatements(statements []utils.StatementWithType, objectsTitle string, progressBar utils.ProgressBar, showProgressBar int, executeInParallel bool) {
	if progressBar == nil {
		ExecuteStatementsAndCreateProgressBar(statements, objectsTitle, showProgressBar, executeInParallel)
	} else {
		ExecuteStatements(statements, progressBar, showProgressBar, executeInParallel)
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
		gucStatements = GetRestoreMetadataStatements("global", globalFPInfo.GetMetadataFilePath(), objectTypes, []string{}, false, false)
	}
	ExecuteStatementsAndCreateProgressBar(gucStatements, "", utils.PB_NONE, false, whichConn)
	return gucStatements
}

func restoreSchemas(schemaStatements []utils.StatementWithType, progressBar utils.ProgressBar) {
	for _, schema := range schemaStatements {
		_, err := connection.Exec(schema.Statement, 0)
		if err != nil {
			fmt.Println()
			if strings.Contains(err.Error(), "already exists") {
				gplog.Warn("Schema %s already exists", schema.Name)
			} else {
				gplog.Fatal(err, "Error encountered while creating schema %s: %s", schema.Name, err.Error())
			}
		}
		progressBar.Increment()
	}
}
