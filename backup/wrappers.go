package backup

import (
	"fmt"
	"strings"

	"github.com/greenplum-db/gpbackup/utils"
)

/*
 * This file contains wrapper functions that group together functions relating
 * to querying and printing metadata, so that the logic for each object type
 * can all be in one place and backup.go can serve as a high-level look at the
 * overall backup flow.
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

func InitializeConnection() {
	connection = utils.NewDBConn(*dbname)
	connection.Connect(1)
	connection.MustExec("SET application_name TO 'gpbackup'")
	connection.SetDatabaseVersion()
	InitializeMetadataParams(connection)
	connection.Begin()
	SetSessionGUCs()
}

func SetSessionGUCs() {
	// These GUCs ensure the dumps portability accross systems
	connection.MustExec("SET search_path TO pg_catalog")
	connection.MustExec("SET statement_timeout = 0")
	connection.MustExec("SET DATESTYLE = ISO")
	if connection.Version.AtLeast("5") {
		connection.MustExec("SET synchronize_seqscans TO off")
	}
	if connection.Version.AtLeast("6") {
		connection.MustExec("SET INTERVALSTYLE = POSTGRES")
	}
}

func InitializeBackupReport() {
	dbname := utils.SelectString(connection, fmt.Sprintf("select quote_ident(datname) AS string FROM pg_database where datname='%s'", connection.DBName))
	config := utils.BackupConfig{
		DatabaseName:    dbname,
		DatabaseVersion: connection.Version.VersionString,
		BackupVersion:   version,
	}
	dbSize := ""
	if !*metadataOnly {
		dbSize = connection.GetDBSize()
	}

	backupReport = &utils.Report{
		DatabaseSize: dbSize,
		BackupConfig: config,
	}
	utils.InitializeCompressionParameters(!*noCompression, *compressionLevel)
	isSchemaFiltered := len(includeSchemas) > 0 || len(excludeSchemas) > 0
	isTableFiltered := len(includeTables) > 0 || len(excludeTables) > 0
	backupReport.ConstructBackupParamsStringFromFlags(*dataOnly, *metadataOnly, isSchemaFiltered, isTableFiltered, *singleDataFile, *withStats)
}

func InitializeFilterLists() {
	if *excludeTableFile != "" {
		excludeTables = utils.ReadLinesFromFile(*excludeTableFile)
	}
	if *includeTableFile != "" {
		includeTables = utils.ReadLinesFromFile(*includeTableFile)
	}
}

/*
 * Metadata retrieval wrapper functions
 */

func RetrieveAndProcessTables() ([]Relation, []Relation, map[uint32]TableDefinition) {
	logger.Info("Gathering list of tables for backup")
	tables := GetAllUserTables(connection)
	LockTables(connection, tables)

	/*
	 * We expand the includeTables list to include parent and leaf partitions that may not have been
	 * specified by the user but are used in the backup for metadata or data.
	 */
	userPassedIncludeTables := includeTables
	if len(includeTables) > 0 {
		expandedIncludeTables := make([]string, 0)
		for _, table := range tables {
			expandedIncludeTables = append(expandedIncludeTables, table.FQN())
		}
		includeTables = expandedIncludeTables
	}
	tableDefs := ConstructDefinitionsForTables(connection, tables)
	metadataTables, dataTables := SplitTablesByPartitionType(tables, tableDefs, userPassedIncludeTables)
	objectCounts["Tables"] = len(metadataTables)

	return metadataTables, dataTables, tableDefs
}

func RetrieveFunctions(procLangs []ProceduralLanguage) ([]Function, []Function, MetadataMap) {
	logger.Verbose("Retrieving function information")
	functions := GetFunctionsAllVersions(connection)
	objectCounts["Functions"] = len(functions)
	functionMetadata := GetMetadataForObjectType(connection, TYPE_FUNCTION)
	functions = ConstructFunctionDependencies(connection, functions)
	langFuncs, otherFuncs := ExtractLanguageFunctions(functions, procLangs)
	return langFuncs, otherFuncs, functionMetadata
}

func RetrieveTypes() ([]Type, MetadataMap, map[uint32]FunctionInfo) {
	logger.Verbose("Retrieving type information")
	shells := GetShellTypes(connection)
	bases := GetBaseTypes(connection)
	funcInfoMap := GetFunctionOidToInfoMap(connection)
	if connection.Version.Before("5") {
		bases = ConstructBaseTypeDependencies4(connection, bases, funcInfoMap)
	} else {
		bases = ConstructBaseTypeDependencies5(connection, bases)
	}
	types := append(shells, bases...)
	composites := GetCompositeTypes(connection)
	composites = ConstructCompositeTypeDependencies(connection, composites)
	types = append(types, composites...)
	domains := GetDomainTypes(connection)
	domains = ConstructDomainDependencies(connection, domains)
	types = append(types, domains...)
	objectCounts["Types"] = len(types)
	typeMetadata := GetMetadataForObjectType(connection, TYPE_TYPE)
	return types, typeMetadata, funcInfoMap
}

func RetrieveConstraints(tables ...Relation) ([]Constraint, MetadataMap) {
	constraints := GetConstraints(connection, tables...)
	conMetadata := GetCommentsForObjectType(connection, TYPE_CONSTRAINT)
	return constraints, conMetadata
}

/*
 * Generic metadata wrapper functions
 */

func LogBackupInfo() {
	logger.Info("Backup Timestamp = %s", globalCluster.Timestamp)
	logger.Info("Backup Database = %s", connection.DBName)
	params := strings.Split(backupReport.BackupParamsString, "\n")
	for _, param := range params {
		logger.Verbose(param)
	}
}

func BackupSessionGUCs(metadataFile *utils.FileWithByteCount) {
	gucs := GetSessionGUCs(connection)
	PrintSessionGUCs(metadataFile, globalTOC, gucs)
}

/*
 * Global metadata wrapper functions
 */

func BackupTablespaces(metadataFile *utils.FileWithByteCount) {
	logger.Verbose("Writing CREATE TABLESPACE statements to metadata file")
	tablespaces := GetTablespaces(connection)
	objectCounts["Tablespaces"] = len(tablespaces)
	tablespaceMetadata := GetMetadataForObjectType(connection, TYPE_TABLESPACE)
	PrintCreateTablespaceStatements(metadataFile, globalTOC, tablespaces, tablespaceMetadata)
}

func BackupCreateDatabase(metadataFile *utils.FileWithByteCount) {
	logger.Verbose("Writing CREATE DATABASE statement to metadata file")
	db := GetDatabaseInfo(connection)
	dbMetadata := GetMetadataForObjectType(connection, TYPE_DATABASE)
	PrintCreateDatabaseStatement(metadataFile, globalTOC, db, dbMetadata)
}

func BackupDatabaseGUCs(metadataFile *utils.FileWithByteCount) {
	logger.Verbose("Writing database GUCs to metadata file")
	databaseGucs := GetDatabaseGUCs(connection)
	objectCounts["Database GUCs"] = len(databaseGucs)
	PrintDatabaseGUCs(metadataFile, globalTOC, databaseGucs, connection.DBName)
}

func BackupResourceQueues(metadataFile *utils.FileWithByteCount) {
	logger.Verbose("Writing CREATE RESOURCE QUEUE statements to metadata file")
	resQueues := GetResourceQueues(connection)
	objectCounts["Resource Queues"] = len(resQueues)
	resQueueMetadata := GetCommentsForObjectType(connection, TYPE_RESOURCEQUEUE)
	PrintCreateResourceQueueStatements(metadataFile, globalTOC, resQueues, resQueueMetadata)
}

func BackupResourceGroups(metadataFile *utils.FileWithByteCount) {
	logger.Verbose("Writing CREATE RESOURCE GROUP statements to metadata file")
	resGroups := GetResourceGroups(connection)
	objectCounts["Resource Groups"] = len(resGroups)
	resGroupMetadata := GetCommentsForObjectType(connection, TYPE_RESOURCEGROUP)
	PrintCreateResourceGroupStatements(metadataFile, globalTOC, resGroups, resGroupMetadata)
}

func BackupRoles(metadataFile *utils.FileWithByteCount) {
	logger.Verbose("Writing CREATE ROLE statements to metadata file")
	roles := GetRoles(connection)
	objectCounts["Roles"] = len(roles)
	roleMetadata := GetCommentsForObjectType(connection, TYPE_ROLE)
	PrintCreateRoleStatements(metadataFile, globalTOC, roles, roleMetadata)
}

func BackupRoleGrants(metadataFile *utils.FileWithByteCount) {
	logger.Verbose("Writing GRANT ROLE statements to metadata file")
	roleMembers := GetRoleMembers(connection)
	PrintRoleMembershipStatements(metadataFile, globalTOC, roleMembers)
}

/*
 * Predata wrapper functions
 */

func BackupSchemas(metadataFile *utils.FileWithByteCount) {
	logger.Verbose("Writing CREATE SCHEMA statements to metadata file")
	schemas := GetAllUserSchemas(connection)
	objectCounts["Schemas"] = len(schemas)
	schemaMetadata := GetMetadataForObjectType(connection, TYPE_SCHEMA)
	PrintCreateSchemaStatements(metadataFile, globalTOC, schemas, schemaMetadata)
}

func BackupProceduralLanguages(metadataFile *utils.FileWithByteCount, procLangs []ProceduralLanguage, langFuncs []Function, functionMetadata MetadataMap, funcInfoMap map[uint32]FunctionInfo) {
	logger.Verbose("Writing CREATE PROCEDURAL LANGUAGE statements to metadata file")
	objectCounts["Procedural Languages"] = len(procLangs)
	for _, langFunc := range langFuncs {
		PrintCreateFunctionStatement(metadataFile, globalTOC, langFunc, functionMetadata[langFunc.Oid])
	}
	procLangMetadata := GetMetadataForObjectType(connection, TYPE_PROCLANGUAGE)
	PrintCreateLanguageStatements(metadataFile, globalTOC, procLangs, funcInfoMap, procLangMetadata)
}

func BackupForeignDataWrappers(metadataFile *utils.FileWithByteCount, funcInfoMap map[uint32]FunctionInfo) {
	logger.Verbose("Writing CREATE FOREIGN DATA WRAPPER statements to metadata file")
	wrappers := GetForeignDataWrappers(connection)
	objectCounts["Foreign Data Wrappers"] = len(wrappers)
	fdwMetadata := GetMetadataForObjectType(connection, TYPE_FOREIGNDATAWRAPPER)
	PrintCreateForeignDataWrapperStatements(metadataFile, globalTOC, wrappers, funcInfoMap, fdwMetadata)
}

func BackupForeignServers(metadataFile *utils.FileWithByteCount) {
	logger.Verbose("Writing CREATE SERVER statements to metadata file")
	servers := GetForeignServers(connection)
	objectCounts["Foreign Servers"] = len(servers)
	serverMetadata := GetMetadataForObjectType(connection, TYPE_FOREIGNSERVER)
	PrintCreateServerStatements(metadataFile, globalTOC, servers, serverMetadata)
}

func BackupUserMappings(metadataFile *utils.FileWithByteCount) {
	logger.Verbose("Writing CREATE USER MAPPING statements to metadata file")
	mappings := GetUserMappings(connection)
	objectCounts["User Mappings"] = len(mappings)
	PrintCreateUserMappingStatements(metadataFile, globalTOC, mappings)
}

func BackupShellTypes(metadataFile *utils.FileWithByteCount, types []Type) {
	logger.Verbose("Writing CREATE TYPE statements for shell types to metadata file")
	PrintCreateShellTypeStatements(metadataFile, globalTOC, types)
}

func BackupEnumTypes(metadataFile *utils.FileWithByteCount, typeMetadata MetadataMap) {
	enums := GetEnumTypes(connection)
	logger.Verbose("Writing CREATE TYPE statements for enum types to metadata file")
	objectCounts["Types"] += len(enums)
	PrintCreateEnumTypeStatements(metadataFile, globalTOC, enums, typeMetadata)
}

func BackupCreateSequences(metadataFile *utils.FileWithByteCount, sequences []Sequence, relationMetadata MetadataMap) {
	logger.Verbose("Writing CREATE SEQUENCE statements to metadata file")
	objectCounts["Sequences"] = len(sequences)
	PrintCreateSequenceStatements(metadataFile, globalTOC, sequences, relationMetadata)
}

// This function is fairly unwieldy, but there's not really a good way to break it down
func BackupFunctionsAndTypesAndTables(metadataFile *utils.FileWithByteCount, otherFuncs []Function, types []Type, tables []Relation, functionMetadata MetadataMap, typeMetadata MetadataMap, relationMetadata MetadataMap, tableDefs map[uint32]TableDefinition, constraints []Constraint) {
	logger.Verbose("Writing CREATE FUNCTION statements to metadata file")
	logger.Verbose("Writing CREATE TYPE statements for base, composite, and domain types to metadata file")
	logger.Verbose("Writing CREATE TABLE statements to metadata file")
	tables = ConstructTableDependencies(connection, tables, tableDefs, false)
	sortedSlice := SortFunctionsAndTypesAndTablesInDependencyOrder(otherFuncs, types, tables)
	filteredMetadata := ConstructFunctionAndTypeAndTableMetadataMap(functionMetadata, typeMetadata, relationMetadata)
	PrintCreateDependentTypeAndFunctionAndTablesStatements(metadataFile, globalTOC, sortedSlice, filteredMetadata, tableDefs, constraints)
	extPartInfo, partInfoMap := GetExternalPartitionInfo(connection)
	if len(extPartInfo) > 0 {
		logger.Verbose("Writing EXCHANGE PARTITION statements to metadata file")
		PrintExchangeExternalPartitionStatements(metadataFile, globalTOC, extPartInfo, partInfoMap, tables)
	}
}

// This function should be used only with a table-only backup.  For an unfiltered backup, the above function is used.
func BackupTables(metadataFile *utils.FileWithByteCount, tables []Relation, relationMetadata MetadataMap, tableDefs map[uint32]TableDefinition, constraints []Constraint) {
	logger.Verbose("Writing CREATE TABLE statements to metadata file")
	tables = ConstructTableDependencies(connection, tables, tableDefs, true)
	sortable := make([]Sortable, 0)
	for _, table := range tables {
		sortable = append(sortable, table)
	}
	sortedSlice := TopologicalSort(sortable)
	PrintCreateDependentTypeAndFunctionAndTablesStatements(metadataFile, globalTOC, sortedSlice, relationMetadata, tableDefs, constraints)
	extPartInfo, partInfoMap := GetExternalPartitionInfo(connection)
	if len(extPartInfo) > 0 {
		logger.Verbose("Writing EXCHANGE PARTITION statements to metadata file")
		PrintExchangeExternalPartitionStatements(metadataFile, globalTOC, extPartInfo, partInfoMap, tables)
	}
}

func BackupAlterSequences(metadataFile *utils.FileWithByteCount, sequences []Sequence) {
	logger.Verbose("Writing ALTER SEQUENCE statements to metadata file")
	sequenceColumnOwners := GetSequenceColumnOwnerMap(connection)
	PrintAlterSequenceStatements(metadataFile, globalTOC, sequences, sequenceColumnOwners)
}

func BackupProtocols(metadataFile *utils.FileWithByteCount, funcInfoMap map[uint32]FunctionInfo) {
	logger.Verbose("Writing CREATE PROTOCOL statements to metadata file")
	protocols := GetExternalProtocols(connection)
	objectCounts["Protocols"] = len(protocols)
	protoMetadata := GetMetadataForObjectType(connection, TYPE_PROTOCOL)
	PrintCreateExternalProtocolStatements(metadataFile, globalTOC, protocols, funcInfoMap, protoMetadata)
}

func BackupTSParsers(metadataFile *utils.FileWithByteCount) {
	logger.Verbose("Writing CREATE TEXT SEARCH PARSER statements to metadata file")
	parsers := GetTextSearchParsers(connection)
	objectCounts["Text Search Parsers"] = len(parsers)
	parserMetadata := GetCommentsForObjectType(connection, TYPE_TSPARSER)
	PrintCreateTextSearchParserStatements(metadataFile, globalTOC, parsers, parserMetadata)
}

func BackupTSTemplates(metadataFile *utils.FileWithByteCount) {
	logger.Verbose("Writing CREATE TEXT SEARCH TEMPLATE statements to metadata file")
	templates := GetTextSearchTemplates(connection)
	objectCounts["Text Search Templates"] = len(templates)
	templateMetadata := GetCommentsForObjectType(connection, TYPE_TSTEMPLATE)
	PrintCreateTextSearchTemplateStatements(metadataFile, globalTOC, templates, templateMetadata)
}

func BackupTSDictionaries(metadataFile *utils.FileWithByteCount) {
	logger.Verbose("Writing CREATE TEXT SEARCH DICTIONARY statements to metadata file")
	dictionaries := GetTextSearchDictionaries(connection)
	objectCounts["Text Search Dictionaries"] = len(dictionaries)
	dictionaryMetadata := GetMetadataForObjectType(connection, TYPE_TSDICTIONARY)
	PrintCreateTextSearchDictionaryStatements(metadataFile, globalTOC, dictionaries, dictionaryMetadata)
}

func BackupTSConfigurations(metadataFile *utils.FileWithByteCount) {
	logger.Verbose("Writing CREATE TEXT SEARCH CONFIGURATION statements to metadata file")
	configurations := GetTextSearchConfigurations(connection)
	objectCounts["Text Search Configurations"] = len(configurations)
	configurationMetadata := GetMetadataForObjectType(connection, TYPE_TSCONFIGURATION)
	PrintCreateTextSearchConfigurationStatements(metadataFile, globalTOC, configurations, configurationMetadata)
}

func BackupConversions(metadataFile *utils.FileWithByteCount) {
	logger.Verbose("Writing CREATE CONVERSION statements to metadata file")
	conversions := GetConversions(connection)
	objectCounts["Conversions"] = len(conversions)
	convMetadata := GetMetadataForObjectType(connection, TYPE_CONVERSION)
	PrintCreateConversionStatements(metadataFile, globalTOC, conversions, convMetadata)
}

func BackupOperators(metadataFile *utils.FileWithByteCount) {
	logger.Verbose("Writing CREATE OPERATOR statements to metadata file")
	operators := GetOperators(connection)
	objectCounts["Operators"] = len(operators)
	operatorMetadata := GetMetadataForObjectType(connection, TYPE_OPERATOR)
	PrintCreateOperatorStatements(metadataFile, globalTOC, operators, operatorMetadata)
}

func BackupOperatorFamilies(metadataFile *utils.FileWithByteCount) {
	logger.Verbose("Writing CREATE OPERATOR FAMILY statements to metadata file")
	operatorFamilies := GetOperatorFamilies(connection)
	objectCounts["Operator Families"] = len(operatorFamilies)
	operatorFamilyMetadata := GetMetadataForObjectType(connection, TYPE_OPERATORFAMILY)
	PrintCreateOperatorFamilyStatements(metadataFile, globalTOC, operatorFamilies, operatorFamilyMetadata)
}

func BackupOperatorClasses(metadataFile *utils.FileWithByteCount) {
	logger.Verbose("Writing CREATE OPERATOR CLASS statements to metadata file")
	operatorClasses := GetOperatorClasses(connection)
	objectCounts["Operator Classes"] = len(operatorClasses)
	operatorClassMetadata := GetMetadataForObjectType(connection, TYPE_OPERATORCLASS)
	PrintCreateOperatorClassStatements(metadataFile, globalTOC, operatorClasses, operatorClassMetadata)
}

func BackupAggregates(metadataFile *utils.FileWithByteCount, funcInfoMap map[uint32]FunctionInfo) {
	logger.Verbose("Writing CREATE AGGREGATE statements to metadata file")
	aggregates := GetAggregates(connection)
	objectCounts["Aggregates"] = len(aggregates)
	aggMetadata := GetMetadataForObjectType(connection, TYPE_AGGREGATE)
	PrintCreateAggregateStatements(metadataFile, globalTOC, aggregates, funcInfoMap, aggMetadata)
}

func BackupCasts(metadataFile *utils.FileWithByteCount) {
	logger.Verbose("Writing CREATE CAST statements to metadata file")
	casts := GetCasts(connection)
	objectCounts["Casts"] = len(casts)
	castMetadata := GetCommentsForObjectType(connection, TYPE_CAST)
	PrintCreateCastStatements(metadataFile, globalTOC, casts, castMetadata)
}

func BackupViews(metadataFile *utils.FileWithByteCount, relationMetadata MetadataMap) {
	logger.Verbose("Writing CREATE VIEW statements to metadata file")
	views := GetViews(connection)
	objectCounts["Views"] = len(views)
	views = ConstructViewDependencies(connection, views)
	views = SortViews(views)
	PrintCreateViewStatements(metadataFile, globalTOC, views, relationMetadata)
}

func BackupConstraints(metadataFile *utils.FileWithByteCount, constraints []Constraint, conMetadata MetadataMap) {
	logger.Verbose("Writing ADD CONSTRAINT statements to metadata file")
	PrintConstraintStatements(metadataFile, globalTOC, constraints, conMetadata)
}

/*
 * Postdata wrapper functions
 */

func BackupIndexes(metadataFile *utils.FileWithByteCount) {
	logger.Verbose("Writing CREATE INDEX statements to metadata file")
	indexNameMap := ConstructImplicitIndexNames(connection)
	indexes := GetIndexes(connection, indexNameMap)
	objectCounts["Indexes"] = len(indexes)
	indexMetadata := GetCommentsForObjectType(connection, TYPE_INDEX)
	PrintCreateIndexStatements(metadataFile, globalTOC, indexes, indexMetadata)
}

func BackupRules(metadataFile *utils.FileWithByteCount) {
	logger.Verbose("Writing CREATE RULE statements to metadata file")
	rules := GetRules(connection)
	objectCounts["Rules"] = len(rules)
	ruleMetadata := GetCommentsForObjectType(connection, TYPE_RULE)
	PrintCreateRuleStatements(metadataFile, globalTOC, rules, ruleMetadata)
}

func BackupTriggers(metadataFile *utils.FileWithByteCount) {
	logger.Verbose("Writing CREATE TRIGGER statements to metadata file")
	triggers := GetTriggers(connection)
	objectCounts["Triggers"] = len(triggers)
	triggerMetadata := GetCommentsForObjectType(connection, TYPE_TRIGGER)
	PrintCreateTriggerStatements(metadataFile, globalTOC, triggers, triggerMetadata)
}

/*
 * Data wrapper functions
 */

func BackupData(tables []Relation, tableDefs map[uint32]TableDefinition) {
	if *singleDataFile {
		globalCluster.CreateSegmentPipesOnAllHostsForBackup()
		defer globalCluster.CleanUpSegmentPipesOnAllHosts()
		globalCluster.ReadFromSegmentPipes()
		defer globalCluster.CleanUpSegmentTailProcesses()
	}
	BackupDataForAllTables(tables, tableDefs)
}

func BackupStatistics(statisticsFile *utils.FileWithByteCount, tables []Relation) {
	attStats := GetAttributeStatistics(connection, tables)
	tupleStats := GetTupleStatistics(connection, tables)

	BackupSessionGUCs(statisticsFile)
	PrintStatisticsStatements(statisticsFile, globalTOC, tables, attStats, tupleStats)
}
