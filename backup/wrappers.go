package backup

import (
	"fmt"

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
	connection.Connect()
	_, err := connection.Exec("SET application_name TO 'gpbackup'")
	utils.CheckError(err)
	connection.SetDatabaseVersion()
	InitializeMetadataParams(connection)
	connection.Begin()
	_, err = connection.Exec("SET search_path TO pg_catalog")
	utils.CheckError(err)
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
	backupReport.SetBackupTypeFromFlags(*dataOnly, *metadataOnly, *noCompression, isSchemaFiltered, isTableFiltered, *singleDataFile, *withStats)
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
	extPartInfo, _ := GetExternalPartitionInfo(connection)
	for _, partInfo := range extPartInfo {
		tableDef := tableDefs[partInfo.ParentRelationOid]
		tableDef.IsExternal = true
		tableDefs[partInfo.ParentRelationOid] = tableDef
	}
	metadataTables, dataTables := SplitTablesByPartitionType(tables, tableDefs, userPassedIncludeTables)
	objectCounts["Tables"] = len(metadataTables)

	return metadataTables, dataTables, tableDefs
}

func RetrieveFunctions(objectCounts map[string]int, procLangs []ProceduralLanguage) ([]Function, []Function, MetadataMap) {
	logger.Verbose("Retrieving function information")
	functions := GetFunctions(connection)
	objectCounts["Functions"] = len(functions)
	functionMetadata := GetMetadataForObjectType(connection, TYPE_FUNCTION)
	functions = ConstructFunctionDependencies(connection, functions)
	langFuncs, otherFuncs := ExtractLanguageFunctions(functions, procLangs)
	return langFuncs, otherFuncs, functionMetadata
}

func RetrieveTypes(objectCounts map[string]int) ([]Type, MetadataMap, map[uint32]FunctionInfo) {
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
	logger.Info("Backup Type = %s", backupReport.BackupType)
}

func BackupSessionGUCs(postdataFile *utils.FileWithByteCount) {
	gucs := GetSessionGUCs(connection)
	PrintSessionGUCs(postdataFile, globalTOC, gucs)
}

/*
 * Global metadata wrapper functions
 */

func BackupTablespaces(globalFile *utils.FileWithByteCount, objectCounts map[string]int) {
	logger.Verbose("Writing CREATE TABLESPACE statements to global file")
	tablespaces := GetTablespaces(connection)
	objectCounts["Tablespaces"] = len(tablespaces)
	tablespaceMetadata := GetMetadataForObjectType(connection, TYPE_TABLESPACE)
	PrintCreateTablespaceStatements(globalFile, globalTOC, tablespaces, tablespaceMetadata)
}

func BackupCreateDatabase(globalFile *utils.FileWithByteCount) {
	logger.Verbose("Writing CREATE DATABASE statement to global file")
	db := GetDatabaseName(connection)
	dbMetadata := GetMetadataForObjectType(connection, TYPE_DATABASE)
	PrintCreateDatabaseStatement(globalFile, globalTOC, db, dbMetadata)
}

func BackupDatabaseGUCs(globalFile *utils.FileWithByteCount, objectCounts map[string]int) {
	logger.Verbose("Writing database GUCs to global file")
	databaseGucs := GetDatabaseGUCs(connection)
	objectCounts["Database GUCs"] = len(databaseGucs)
	PrintDatabaseGUCs(globalFile, globalTOC, databaseGucs, connection.DBName)
}

func BackupResourceQueues(globalFile *utils.FileWithByteCount, objectCounts map[string]int) {
	logger.Verbose("Writing CREATE RESOURCE QUEUE statements to global file")
	resQueues := GetResourceQueues(connection)
	objectCounts["Resource Queues"] = len(resQueues)
	resQueueMetadata := GetCommentsForObjectType(connection, TYPE_RESOURCEQUEUE)
	PrintCreateResourceQueueStatements(globalFile, globalTOC, resQueues, resQueueMetadata)
}

func BackupResourceGroups(globalFile *utils.FileWithByteCount, objectCounts map[string]int) {
	logger.Verbose("Writing CREATE RESOURCE GROUP statements to global file")
	resGroups := GetResourceGroups(connection)
	objectCounts["Resource Groups"] = len(resGroups)
	resGroupMetadata := GetCommentsForObjectType(connection, TYPE_RESOURCEGROUP)
	PrintCreateResourceGroupStatements(globalFile, globalTOC, resGroups, resGroupMetadata)
}

func BackupRoles(globalFile *utils.FileWithByteCount, objectCounts map[string]int) {
	logger.Verbose("Writing CREATE ROLE statements to global file")
	roles := GetRoles(connection)
	objectCounts["Roles"] = len(roles)
	roleMetadata := GetCommentsForObjectType(connection, TYPE_ROLE)
	PrintCreateRoleStatements(globalFile, globalTOC, roles, roleMetadata)
}

func BackupRoleGrants(globalFile *utils.FileWithByteCount) {
	logger.Verbose("Writing GRANT ROLE statements to global file")
	roleMembers := GetRoleMembers(connection)
	PrintRoleMembershipStatements(globalFile, globalTOC, roleMembers)
}

/*
 * Predata wrapper functions
 */

func BackupSchemas(predataFile *utils.FileWithByteCount, objectCounts map[string]int) {
	logger.Verbose("Writing CREATE SCHEMA statements to predata file")
	schemas := GetAllUserSchemas(connection)
	objectCounts["Schemas"] = len(schemas)
	schemaMetadata := GetMetadataForObjectType(connection, TYPE_SCHEMA)
	PrintCreateSchemaStatements(predataFile, globalTOC, schemas, schemaMetadata)
}

func BackupProceduralLanguages(predataFile *utils.FileWithByteCount, objectCounts map[string]int, procLangs []ProceduralLanguage, langFuncs []Function, functionMetadata MetadataMap, funcInfoMap map[uint32]FunctionInfo) {
	logger.Verbose("Writing CREATE PROCEDURAL LANGUAGE statements to predata file")
	objectCounts["Procedural Languages"] = len(procLangs)
	for _, langFunc := range langFuncs {
		PrintCreateFunctionStatement(predataFile, globalTOC, langFunc, functionMetadata[langFunc.Oid])
	}
	procLangMetadata := GetMetadataForObjectType(connection, TYPE_PROCLANGUAGE)
	PrintCreateLanguageStatements(predataFile, globalTOC, procLangs, funcInfoMap, procLangMetadata)
}

func BackupShellTypes(predataFile *utils.FileWithByteCount, types []Type) {
	logger.Verbose("Writing CREATE TYPE statements for shell types to predata file")
	PrintCreateShellTypeStatements(predataFile, globalTOC, types)
}

func BackupEnumTypes(predataFile *utils.FileWithByteCount, typeMetadata MetadataMap) {
	enums := GetEnumTypes(connection)
	logger.Verbose("Writing CREATE TYPE statements for enum types to predata file")
	objectCounts["Types"] += len(enums)
	PrintCreateEnumTypeStatements(predataFile, globalTOC, enums, typeMetadata)
}

func BackupCreateSequences(predataFile *utils.FileWithByteCount, objectCounts map[string]int, sequences []Sequence, relationMetadata MetadataMap) {
	logger.Verbose("Writing CREATE SEQUENCE statements to predata file")
	objectCounts["Sequences"] = len(sequences)
	PrintCreateSequenceStatements(predataFile, globalTOC, sequences, relationMetadata)
}

// This function is fairly unwieldy, but there's not really a good way to break it down
func BackupFunctionsAndTypesAndTables(predataFile *utils.FileWithByteCount, otherFuncs []Function, types []Type, tables []Relation, functionMetadata MetadataMap, typeMetadata MetadataMap, relationMetadata MetadataMap, tableDefs map[uint32]TableDefinition, constraints []Constraint) {
	logger.Verbose("Writing CREATE FUNCTION statements to predata file")
	logger.Verbose("Writing CREATE TYPE statements for base, composite, and domain types to predata file")
	logger.Verbose("Writing CREATE TABLE statements to predata file")
	tables = ConstructTableDependencies(connection, tables, tableDefs, false)
	sortedSlice := SortFunctionsAndTypesAndTablesInDependencyOrder(otherFuncs, types, tables)
	filteredMetadata := ConstructFunctionAndTypeAndTableMetadataMap(functionMetadata, typeMetadata, relationMetadata)
	PrintCreateDependentTypeAndFunctionAndTablesStatements(predataFile, globalTOC, sortedSlice, filteredMetadata, tableDefs, constraints)
	extPartInfo, partInfoMap := GetExternalPartitionInfo(connection)
	if len(extPartInfo) > 0 {
		logger.Verbose("Writing EXCHANGE PARTITION statements to predata file")
		PrintExchangeExternalPartitionStatements(predataFile, globalTOC, extPartInfo, partInfoMap, tables)
	}
}

// This function should be used only with a table-only backup.  For an unfiltered backup, the above function is used.
func BackupTables(predataFile *utils.FileWithByteCount, tables []Relation, relationMetadata MetadataMap, tableDefs map[uint32]TableDefinition, constraints []Constraint) {
	logger.Verbose("Writing CREATE TABLE statements to predata file")
	tables = ConstructTableDependencies(connection, tables, tableDefs, true)
	sortable := make([]Sortable, 0)
	for _, table := range tables {
		sortable = append(sortable, table)
	}
	sortedSlice := TopologicalSort(sortable)
	PrintCreateDependentTypeAndFunctionAndTablesStatements(predataFile, globalTOC, sortedSlice, relationMetadata, tableDefs, constraints)
	extPartInfo, partInfoMap := GetExternalPartitionInfo(connection)
	if len(extPartInfo) > 0 {
		logger.Verbose("Writing EXCHANGE PARTITION statements to predata file")
		PrintExchangeExternalPartitionStatements(predataFile, globalTOC, extPartInfo, partInfoMap, tables)
	}
}

func BackupAlterSequences(predataFile *utils.FileWithByteCount, sequences []Sequence) {
	logger.Verbose("Writing ALTER SEQUENCE statements to predata file")
	sequenceColumnOwners := GetSequenceColumnOwnerMap(connection)
	PrintAlterSequenceStatements(predataFile, globalTOC, sequences, sequenceColumnOwners)
}

func BackupProtocols(predataFile *utils.FileWithByteCount, objectCounts map[string]int, funcInfoMap map[uint32]FunctionInfo) {
	logger.Verbose("Writing CREATE PROTOCOL statements to predata file")
	protocols := GetExternalProtocols(connection)
	objectCounts["Protocols"] = len(protocols)
	protoMetadata := GetMetadataForObjectType(connection, TYPE_PROTOCOL)
	PrintCreateExternalProtocolStatements(predataFile, globalTOC, protocols, funcInfoMap, protoMetadata)
}

func BackupTSParsers(predataFile *utils.FileWithByteCount, objectCounts map[string]int) {
	logger.Verbose("Writing CREATE TEXT SEARCH PARSER statements to predata file")
	parsers := GetTextSearchParsers(connection)
	objectCounts["Text Search Parsers"] = len(parsers)
	parserMetadata := GetCommentsForObjectType(connection, TYPE_TSPARSER)
	PrintCreateTextSearchParserStatements(predataFile, globalTOC, parsers, parserMetadata)
}

func BackupTSTemplates(predataFile *utils.FileWithByteCount, objectCounts map[string]int) {
	logger.Verbose("Writing CREATE TEXT SEARCH TEMPLATE statements to predata file")
	templates := GetTextSearchTemplates(connection)
	objectCounts["Text Search Templates"] = len(templates)
	templateMetadata := GetCommentsForObjectType(connection, TYPE_TSTEMPLATE)
	PrintCreateTextSearchTemplateStatements(predataFile, globalTOC, templates, templateMetadata)
}

func BackupTSDictionaries(predataFile *utils.FileWithByteCount, objectCounts map[string]int) {
	logger.Verbose("Writing CREATE TEXT SEARCH DICTIONARY statements to predata file")
	dictionaries := GetTextSearchDictionaries(connection)
	objectCounts["Text Search Dictionaries"] = len(dictionaries)
	dictionaryMetadata := GetMetadataForObjectType(connection, TYPE_TSDICTIONARY)
	PrintCreateTextSearchDictionaryStatements(predataFile, globalTOC, dictionaries, dictionaryMetadata)
}

func BackupTSConfigurations(predataFile *utils.FileWithByteCount, objectCounts map[string]int) {
	logger.Verbose("Writing CREATE TEXT SEARCH CONFIGURATION statements to predata file")
	configurations := GetTextSearchConfigurations(connection)
	objectCounts["Text Search Configurations"] = len(configurations)
	configurationMetadata := GetMetadataForObjectType(connection, TYPE_TSCONFIGURATION)
	PrintCreateTextSearchConfigurationStatements(predataFile, globalTOC, configurations, configurationMetadata)
}

func BackupConversions(predataFile *utils.FileWithByteCount, objectCounts map[string]int) {
	logger.Verbose("Writing CREATE CONVERSION statements to predata file")
	conversions := GetConversions(connection)
	objectCounts["Conversions"] = len(conversions)
	convMetadata := GetMetadataForObjectType(connection, TYPE_CONVERSION)
	PrintCreateConversionStatements(predataFile, globalTOC, conversions, convMetadata)
}

func BackupOperators(predataFile *utils.FileWithByteCount, objectCounts map[string]int) {
	logger.Verbose("Writing CREATE OPERATOR statements to predata file")
	operators := GetOperators(connection)
	objectCounts["Operators"] = len(operators)
	operatorMetadata := GetMetadataForObjectType(connection, TYPE_OPERATOR)
	PrintCreateOperatorStatements(predataFile, globalTOC, operators, operatorMetadata)
}

func BackupOperatorFamilies(predataFile *utils.FileWithByteCount, objectCounts map[string]int) {
	logger.Verbose("Writing CREATE OPERATOR FAMILY statements to predata file")
	operatorFamilies := GetOperatorFamilies(connection)
	objectCounts["Operator Families"] = len(operatorFamilies)
	operatorFamilyMetadata := GetMetadataForObjectType(connection, TYPE_OPERATORFAMILY)
	PrintCreateOperatorFamilyStatements(predataFile, globalTOC, operatorFamilies, operatorFamilyMetadata)
}

func BackupOperatorClasses(predataFile *utils.FileWithByteCount, objectCounts map[string]int) {
	logger.Verbose("Writing CREATE OPERATOR CLASS statements to predata file")
	operatorClasses := GetOperatorClasses(connection)
	objectCounts["Operator Classes"] = len(operatorClasses)
	operatorClassMetadata := GetMetadataForObjectType(connection, TYPE_OPERATORCLASS)
	PrintCreateOperatorClassStatements(predataFile, globalTOC, operatorClasses, operatorClassMetadata)
}

func BackupAggregates(predataFile *utils.FileWithByteCount, objectCounts map[string]int, funcInfoMap map[uint32]FunctionInfo) {
	logger.Verbose("Writing CREATE AGGREGATE statements to predata file")
	aggregates := GetAggregates(connection)
	objectCounts["Aggregates"] = len(aggregates)
	aggMetadata := GetMetadataForObjectType(connection, TYPE_AGGREGATE)
	PrintCreateAggregateStatements(predataFile, globalTOC, aggregates, funcInfoMap, aggMetadata)
}

func BackupCasts(predataFile *utils.FileWithByteCount, objectCounts map[string]int) {
	logger.Verbose("Writing CREATE CAST statements to predata file")
	casts := GetCasts(connection)
	objectCounts["Casts"] = len(casts)
	castMetadata := GetCommentsForObjectType(connection, TYPE_CAST)
	PrintCreateCastStatements(predataFile, globalTOC, casts, castMetadata)
}

func BackupViews(predataFile *utils.FileWithByteCount, objectCounts map[string]int, relationMetadata MetadataMap) {
	logger.Verbose("Writing CREATE VIEW statements to predata file")
	views := GetViews(connection)
	objectCounts["Views"] = len(views)
	views = ConstructViewDependencies(connection, views)
	views = SortViews(views)
	PrintCreateViewStatements(predataFile, globalTOC, views, relationMetadata)
}

func BackupConstraints(predataFile *utils.FileWithByteCount, constraints []Constraint, conMetadata MetadataMap) {
	logger.Verbose("Writing ADD CONSTRAINT statements to predata file")
	PrintConstraintStatements(predataFile, globalTOC, constraints, conMetadata)
}

/*
 * Postdata wrapper functions
 */

func BackupIndexes(postdataFile *utils.FileWithByteCount, objectCounts map[string]int) {
	logger.Verbose("Writing CREATE INDEX statements to postdata file")
	indexNameMap := ConstructImplicitIndexNames(connection)
	indexes := GetIndexes(connection, indexNameMap)
	objectCounts["Indexes"] = len(indexes)
	indexMetadata := GetCommentsForObjectType(connection, TYPE_INDEX)
	PrintCreateIndexStatements(postdataFile, globalTOC, indexes, indexMetadata)
}

func BackupRules(postdataFile *utils.FileWithByteCount, objectCounts map[string]int) {
	logger.Verbose("Writing CREATE RULE statements to postdata file")
	rules := GetRules(connection)
	objectCounts["Rules"] = len(rules)
	ruleMetadata := GetCommentsForObjectType(connection, TYPE_RULE)
	PrintCreateRuleStatements(postdataFile, globalTOC, rules, ruleMetadata)
}

func BackupTriggers(postdataFile *utils.FileWithByteCount, objectCounts map[string]int) {
	logger.Verbose("Writing CREATE TRIGGER statements to postdata file")
	triggers := GetTriggers(connection)
	objectCounts["Triggers"] = len(triggers)
	triggerMetadata := GetCommentsForObjectType(connection, TYPE_TRIGGER)
	PrintCreateTriggerStatements(postdataFile, globalTOC, triggers, triggerMetadata)
}

/*
 * Data wrapper functions
 */

func BackupData(tables []Relation, tableDefs map[uint32]TableDefinition) {
	numExtTables := 0
	numExtParts := 0
	numRegTables := 1
	totalExtTables := 0
	for _, table := range tables {
		if tableDefs[table.Oid].IsExternal {
			totalExtTables++
		}
	}
	totalRegTables := len(tables) - totalExtTables
	dataProgressBar := utils.NewProgressBar(totalRegTables, "Tables backed up: ", logger.GetVerbosity() == utils.LOGINFO)
	dataProgressBar.Start()

	for _, table := range tables {
		tableDef := tableDefs[table.Oid]
		isExternal := tableDef.IsExternal
		// A parent partition table has IsExternal set if it has any external partitions
		hasExternalPartitions := isExternal && tableDef.PartitionType == "p"
		if !isExternal {
			if logger.GetVerbosity() > utils.LOGINFO {
				// No progress bar at this log level, so we note table count here
				logger.Verbose("Writing data for table %s to file (table %d of %d)", table.ToString(), numRegTables, totalRegTables)
			} else {
				logger.Verbose("Writing data for table %s to file", table.ToString())
			}
			backupFile := globalCluster.GetTableBackupFilePathForCopyCommand(table.Oid, *singleDataFile)
			CopyTableOut(connection, table, backupFile)
			numRegTables++
			dataProgressBar.Increment()
		} else if hasExternalPartitions {
			logger.Verbose("Skipping data backup of table %s because it has one or more external partitions.", table.ToString())
			numExtParts++
		} else if *leafPartitionData || tableDef.PartitionType != "l" {
			logger.Verbose("Skipping data backup of table %s because it is an external table.", table.ToString())
			numExtTables++
		}
	}
	dataProgressBar.Finish()
	if numExtTables > 0 {
		s := ""
		if numExtTables > 1 {
			s = "s"
		}
		logger.Warn("Skipped data backup of %d external table%s.", numExtTables, s)
	}
	if numExtParts > 0 {
		s := ""
		if numExtParts > 1 {
			s = "s"
		}
		logger.Warn("Skipped data backup of %d partition table%s containing one or more external partitions.", numExtParts, s)
		logger.Warn("Set the --leaf-partition-data flag to back up data in those tables.")
	}
	if numExtTables > 0 || numExtParts > 0 {
		logger.Warn("See %s for a complete list of skipped tables.", logger.GetLogFilePath())
	}
}

func BackupStatistics(statisticsFile *utils.FileWithByteCount, tables []Relation) {
	attStats := GetAttributeStatistics(connection, tables)
	tupleStats := GetTupleStatistics(connection, tables)

	BackupSessionGUCs(statisticsFile)
	PrintStatisticsStatements(statisticsFile, globalTOC, tables, attStats, tupleStats)
}
