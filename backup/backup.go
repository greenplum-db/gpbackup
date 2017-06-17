package backup

import (
	"flag"
	"fmt"

	"github.com/greenplum-db/gpbackup/utils"
)

var (
	connection *utils.DBConn
	logger     *utils.Logger
)

var ( // Command-line flags
	dbname  = flag.String("dbname", "", "The database to be backed up")
	debug   = flag.Bool("debug", false, "Print verbose and debug log messages")
	dumpDir = flag.String("dumpdir", "", "The directory to which all dump files will be written")
	quiet   = flag.Bool("quiet", false, "Suppress non-warning, non-error log messages")
	verbose = flag.Bool("verbose", false, "Print verbose log messages")
)

// This function handles setup that can be done before parsing flags.
func DoInit() {
	SetLogger(utils.InitializeLogging("gpbackup", ""))
}

func SetLogger(log *utils.Logger) {
	logger = log
}

/*
* This function handles argument parsing and validation, e.g. checking that a passed filename exists.
* It should only validate; initialization with any sort of side effects should go in DoInit or DoSetup.
 */
func DoValidation() {
	flag.Parse()
	utils.CheckExclusiveFlags("debug", "quiet", "verbose")
}

// This function handles setup that must be done after parsing flags.
func DoSetup() {
	if *quiet {
		logger.SetVerbosity(utils.LOGERROR)
	} else if *debug {
		logger.SetVerbosity(utils.LOGDEBUG)
	} else if *verbose {
		logger.SetVerbosity(utils.LOGVERBOSE)
	}
	connection = utils.NewDBConn(*dbname)
	connection.Connect()
	connection.Exec("SET application_name TO 'gpbackup'")

	utils.SetDumpTimestamp("")

	if *dumpDir != "" {
		utils.BaseDumpDir = *dumpDir
	}
	logger.Verbose("Creating dump directories")
	segConfig := utils.GetSegmentConfiguration(connection)
	utils.SetupSegmentConfiguration(segConfig)
	utils.CreateDumpDirs()
}

func DoBackup() {
	logger.Info("Dump Key = %s", utils.DumpTimestamp)
	logger.Info("Dump Database = %s", utils.QuoteIdent(connection.DBName))
	logger.Info("Database Size = %s", connection.GetDBSize())

	masterDumpDir := utils.GetDirForContent(-1)

	globalFilename := fmt.Sprintf("%s/global.sql", masterDumpDir)
	predataFilename := fmt.Sprintf("%s/predata.sql", masterDumpDir)
	postdataFilename := fmt.Sprintf("%s/postdata.sql", masterDumpDir)

	connection.Begin()
	connection.Exec("SET search_path TO pg_catalog")

	tables := GetAllUserTables(connection)
	extTableMap := GetExternalTablesMap(connection)

	logger.Info("Writing global database metadata to %s", globalFilename)
	backupGlobal(globalFilename)
	logger.Info("Global database metadata dump complete")

	logger.Info("Writing pre-data metadata to %s", predataFilename)
	backupPredata(predataFilename, tables, extTableMap)
	logger.Info("Pre-data metadata dump complete")

	logger.Info("Writing data to file")
	backupData(tables, extTableMap)
	logger.Info("Data dump complete")

	logger.Info("Writing post-data metadata to %s", postdataFilename)
	backupPostdata(postdataFilename, tables, extTableMap)
	logger.Info("Post-data metadata dump complete")

	connection.Commit()
}

func backupGlobal(filename string) {
	globalFile := utils.MustOpenFile(filename)

	logger.Verbose("Writing session GUCs to global file")
	gucs := GetSessionGUCs(connection)
	PrintSessionGUCs(globalFile, gucs)

	logger.Verbose("Writing CREATE DATABASE statement to global file")
	PrintCreateDatabaseStatement(globalFile)

	logger.Verbose("Writing database GUCs to global file")
	databaseGucs := GetDatabaseGUCs(connection)
	PrintDatabaseGUCs(globalFile, databaseGucs, connection.DBName)

	logger.Verbose("Writing database comment to global file")
	databaseComment := GetDatabaseComment(connection)
	if databaseComment != "" {
		utils.MustPrintf(globalFile, "\nCOMMENT ON DATABASE %s IS '%s';\n", connection.DBName, databaseComment)
	}
}

func backupPredata(filename string, tables []utils.Relation, extTableMap map[string]bool) {
	predataFile := utils.MustOpenFile(filename)
	PrintConnectionString(predataFile, connection.DBName)

	logger.Verbose("Writing session GUCs to predata file")
	gucs := GetSessionGUCs(connection)
	PrintSessionGUCs(predataFile, gucs)

	logger.Verbose("Writing CREATE SCHEMA statements to predata file")
	schemas := GetAllUserSchemas(connection)
	PrintCreateSchemaStatements(predataFile, schemas)

	types := GetTypeDefinitions(connection)
	logger.Verbose("Writing CREATE TYPE statements for shell types to predata file")
	PrintShellTypeStatements(predataFile, types)

	funcInfoMap := GetFunctionOidToInfoMap(connection)
	logger.Verbose("Writing CREATE PROCEDURAL LANGUAGE statements to predata file")
	procLangs := GetProceduralLanguages(connection)
	PrintCreateLanguageStatements(predataFile, procLangs, funcInfoMap)

	logger.Verbose("Writing CREATE TYPE statements for composite and enum types to predata file")
	PrintCreateCompositeAndEnumTypeStatements(predataFile, types)

	logger.Verbose("Writing CREATE FUNCTION statements to predata file")
	funcDefs := GetFunctionDefinitions(connection)
	PrintCreateFunctionStatements(predataFile, funcDefs)

	logger.Verbose("Writing CREATE TYPE statements for base types to predata file")
	PrintCreateBaseTypeStatements(predataFile, types)

	logger.Verbose("Writing CREATE AGGREGATE statements to predata file")
	aggDefs := GetAggregateDefinitions(connection)
	PrintCreateAggregateStatements(predataFile, aggDefs, funcInfoMap)

	logger.Verbose("Writing CREATE CAST statements to predata file")
	castDefs := GetCastDefinitions(connection)
	PrintCreateCastStatements(predataFile, castDefs)

	logger.Verbose("Writing CREATE TABLE statements to predata file")
	for _, table := range tables {
		isExternal := extTableMap[table.ToString()]
		tableDef := ConstructDefinitionsForTable(connection, table, isExternal)
		PrintCreateTableStatement(predataFile, table, tableDef)
	}

	logger.Verbose("Writing ADD CONSTRAINT statements to predata file")
	allConstraints, allFkConstraints := ConstructConstraintsForAllTables(connection, tables)
	PrintConstraintStatements(predataFile, allConstraints, allFkConstraints)

	logger.Verbose("Writing CREATE SEQUENCE statements to predata file")
	sequenceDefs := GetAllSequenceDefinitions(connection)
	sequenceOwners := GetSequenceOwnerMap(connection)
	PrintCreateSequenceStatements(predataFile, sequenceDefs, sequenceOwners)

}

func backupData(tables []utils.Relation, extTableMap map[string]bool) {
	for _, table := range tables {
		isExternal := extTableMap[table.ToString()]
		if !isExternal {
			logger.Verbose("Writing data for table %s to file", table.ToString())
			dumpFile := GetTableDumpFilePath(table)
			CopyTableOut(connection, table, dumpFile)
		} else {
			logger.Warn("Skipping data dump of table %s because it is an external table.", table.ToString())
		}
	}
	logger.Verbose("Writing table map file to %s", GetTableMapFilePath())
	WriteTableMapFile(tables)
}

func backupPostdata(filename string, tables []utils.Relation, extTableMap map[string]bool) {
	postdataFile := utils.MustOpenFile(filename)
	PrintConnectionString(postdataFile, connection.DBName)

	logger.Verbose("Writing session GUCs to predata file")
	gucs := GetSessionGUCs(connection)
	PrintSessionGUCs(postdataFile, gucs)

	logger.Verbose("Writing CREATE INDEX statements to postdata file")
	indexes := GetIndexesForAllTables(connection, tables)
	PrintCreateIndexStatements(postdataFile, indexes)
}

func DoTeardown() {
	if r := recover(); r != nil {
		fmt.Println(r)
	}
	if connection != nil {
		connection.Close()
	}
	// TODO: Add logic for error codes based on whether we Abort()ed or not
}
