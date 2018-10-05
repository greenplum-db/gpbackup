package backup

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gp-common-go-libs/iohelper"
	"github.com/greenplum-db/gpbackup/utils"
	"github.com/nightlyone/lockfile"
	"github.com/pkg/errors"
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
	if MustGetFlagBool(utils.QUIET) {
		gplog.SetVerbosity(gplog.LOGERROR)
	} else if MustGetFlagBool(utils.DEBUG) {
		gplog.SetVerbosity(gplog.LOGDEBUG)
	} else if MustGetFlagBool(utils.VERBOSE) {
		gplog.SetVerbosity(gplog.LOGVERBOSE)
	}
}

func InitializeConnectionPool() {
	connectionPool = dbconn.NewDBConnFromEnvironment(MustGetFlagString(utils.DBNAME))
	connectionPool.MustConnect(MustGetFlagInt(utils.JOBS))
	utils.ValidateGPDBVersionCompatibility(connectionPool)
	InitializeMetadataParams(connectionPool)
	for connNum := 0; connNum < connectionPool.NumConns; connNum++ {
		connectionPool.MustExec("SET application_name TO 'gpbackup'", connNum)
		connectionPool.MustBegin(connNum)
		SetSessionGUCs(connNum)
	}
}

func SetSessionGUCs(connNum int) {
	// These GUCs ensure the dumps portability accross systems
	connectionPool.MustExec("SET search_path TO pg_catalog", connNum)
	connectionPool.MustExec("SET statement_timeout = 0", connNum)
	connectionPool.MustExec("SET DATESTYLE = ISO", connNum)
	if connectionPool.Version.AtLeast("5") {
		connectionPool.MustExec("SET synchronize_seqscans TO off", connNum)
	}
	if connectionPool.Version.AtLeast("6") {
		connectionPool.MustExec("SET INTERVALSTYLE = POSTGRES", connNum)
		connectionPool.MustExec("SET lock_timeout = 0", connNum)
	}
}

func InitializeBackupReport() {
	escapedDBName := dbconn.MustSelectString(connectionPool, fmt.Sprintf("select quote_ident(datname) AS string FROM pg_database where datname='%s'", utils.EscapeSingleQuotes(connectionPool.DBName)))
	plugin := ""
	if pluginConfig != nil {
		plugin = pluginConfig.ExecutablePath
	}
	config := utils.NewBackupConfig(escapedDBName, connectionPool.Version.VersionString, version,
		plugin, globalFPInfo.Timestamp, cmdFlags)

	isFilteredBackup := config.IncludeTableFiltered || config.IncludeSchemaFiltered ||
		config.ExcludeTableFiltered || config.ExcludeSchemaFiltered
	dbSize := ""
	if !MustGetFlagBool(utils.METADATA_ONLY) && !isFilteredBackup {
		gplog.Verbose("Getting database size")
		//Potentially expensive query
		dbSize = GetDBSize(connectionPool)
	}

	backupReport = &utils.Report{
		DatabaseSize: dbSize,
		BackupConfig: *config,
	}
	backupReport.ConstructBackupParamsString()
}

func InitializeFilterLists() {
	if MustGetFlagString(utils.EXCLUDE_RELATION_FILE) != "" {
		excludeRelations := iohelper.MustReadLinesFromFile(MustGetFlagString(utils.EXCLUDE_RELATION_FILE))
		err := cmdFlags.Set(utils.EXCLUDE_RELATION, strings.Join(excludeRelations, ","))
		gplog.FatalOnError(err)
	}
	if MustGetFlagString(utils.INCLUDE_RELATION_FILE) != "" {
		includeRelations := iohelper.MustReadLinesFromFile(MustGetFlagString(utils.INCLUDE_RELATION_FILE))
		err := cmdFlags.Set(utils.INCLUDE_RELATION, strings.Join(includeRelations, ","))
		gplog.FatalOnError(err)
	}
}

func CreateBackupLockFile(timestamp string) {
	var err error
	timestampLockFile := fmt.Sprintf("/tmp/%s.lck", timestamp)
	backupLockFile, err = lockfile.New(timestampLockFile)
	gplog.FatalOnError(err)
	err = backupLockFile.TryLock()
	if err != nil {
		gplog.Fatal(errors.Errorf("A backup with timestamp %s is already in progress. Wait 1 second and try the backup again.", timestamp), "")
	}
}

func CreateBackupDirectoriesOnAllHosts() {
	remoteOutput := globalCluster.GenerateAndExecuteCommand("Creating backup directories", func(contentID int) string {
		return fmt.Sprintf("mkdir -p %s", globalFPInfo.GetDirForContent(contentID))
	}, cluster.ON_SEGMENTS_AND_MASTER)
	globalCluster.CheckClusterError(remoteOutput, "Unable to create backup directories", func(contentID int) string {
		return fmt.Sprintf("Unable to create backup directory %s", globalFPInfo.GetDirForContent(contentID))
	})
}

/*
 * Metadata retrieval wrapper functions
 */

func RetrieveAndProcessTables() ([]Relation, []Relation, map[uint32]TableDefinition) {
	tables := GetAllUserTables(connectionPool)
	LockTables(connectionPool, tables)

	/*
	 * We expand the includeRelations list to include parent and leaf partitions that may not have been
	 * specified by the user but are used in the backup for metadata or data.
	 */
	userPassedIncludeRelations := MustGetFlagStringSlice(utils.INCLUDE_RELATION)
	ExpandIncludeRelations(tables)

	if connectionPool.Version.AtLeast("6") {
		tables = append(tables, GetForeignTableRelations(connectionPool)...)
	}

	tableDefs := ConstructDefinitionsForTables(connectionPool, tables)
	metadataTables, dataTables := SplitTablesByPartitionType(tables, tableDefs, userPassedIncludeRelations)
	objectCounts["Tables"] = len(metadataTables)

	return metadataTables, dataTables, tableDefs
}

func RetrieveFunctions(sortables *[]Sortable, metadataMap MetadataMap, procLangs []ProceduralLanguage) ([]Function, MetadataMap) {
	gplog.Verbose("Retrieving function information")
	functions := GetFunctionsAllVersions(connectionPool)
	objectCounts["Functions"] = len(functions)
	functionMetadata := GetMetadataForObjectType(connectionPool, TYPE_FUNCTION)
	langFuncs, otherFuncs := ExtractLanguageFunctions(functions, procLangs)

	*sortables = append(*sortables, convertToSortableSlice(otherFuncs)...)
	addToMetadataMap(functionMetadata, metadataMap)

	return langFuncs, functionMetadata
}

func RetrieveTypes(sortables *[]Sortable, metadataMap MetadataMap) ([]Type, MetadataMap) {
	gplog.Verbose("Retrieving type information")
	shells := GetShellTypes(connectionPool)
	bases := GetBaseTypes(connectionPool)
	types := append(shells, bases...)
	composites := GetCompositeTypes(connectionPool)
	types = append(types, composites...)
	domains := GetDomainTypes(connectionPool)
	types = append(types, domains...)
	objectCounts["Types"] = len(types)
	typeMetadata := GetMetadataForObjectType(connectionPool, TYPE_TYPE)

	*sortables = append(*sortables, convertToSortableSlice(types)...)
	addToMetadataMap(typeMetadata, metadataMap)

	return types, typeMetadata
}

func RetrieveConstraints(tables ...Relation) ([]Constraint, MetadataMap) {
	constraints := GetConstraints(connectionPool, tables...)
	conMetadata := GetCommentsForObjectType(connectionPool, TYPE_CONSTRAINT)
	return constraints, conMetadata
}

func RetrieveSequences() ([]Sequence, map[string]string) {
	sequenceOwnerTables, sequenceOwnerColumns := GetSequenceColumnOwnerMap(connectionPool)
	sequences := GetAllSequences(connectionPool, sequenceOwnerTables)
	return sequences, sequenceOwnerColumns
}

func RetrieveProtocols(sortables *[]Sortable, metadataMap MetadataMap) []ExternalProtocol {
	protocols := GetExternalProtocols(connectionPool)
	objectCounts["Protocols"] = len(protocols)
	protoMetadata := GetMetadataForObjectType(connectionPool, TYPE_PROTOCOL)

	*sortables = append(*sortables, convertToSortableSlice(protocols)...)
	addToMetadataMap(protoMetadata, metadataMap)

	return protocols
}

func RetrieveViews(sortables *[]Sortable) {
	views := GetViews(connectionPool)
	objectCounts["Views"] = len(views)

	*sortables = append(*sortables, convertToSortableSlice(views)...)
}

func RetrieveTSParsers(sortables *[]Sortable, metadataMap MetadataMap) {
	parsers := GetTextSearchParsers(connectionPool)
	objectCounts["Text Search Parsers"] = len(parsers)
	parserMetadata := GetCommentsForObjectType(connectionPool, TYPE_TSPARSER)

	*sortables = append(*sortables, convertToSortableSlice(parsers)...)
	addToMetadataMap(parserMetadata, metadataMap)
}

func RetrieveTSTemplates(sortables *[]Sortable, metadataMap MetadataMap) {
	gplog.Verbose("Retrieving TEXT SEARCH TEMPLATE information")
	templates := GetTextSearchTemplates(connectionPool)
	objectCounts["Text Search Templates"] = len(templates)
	templateMetadata := GetCommentsForObjectType(connectionPool, TYPE_TSTEMPLATE)

	*sortables = append(*sortables, convertToSortableSlice(templates)...)
	addToMetadataMap(templateMetadata, metadataMap)
}

func RetrieveTSDictionaries(sortables *[]Sortable, metadataMap MetadataMap) {
	gplog.Verbose("Retrieving TEXT SEARCH DICTIONARY information")
	dictionaries := GetTextSearchDictionaries(connectionPool)
	objectCounts["Text Search Dictionaries"] = len(dictionaries)
	dictionaryMetadata := GetMetadataForObjectType(connectionPool, TYPE_TSDICTIONARY)

	*sortables = append(*sortables, convertToSortableSlice(dictionaries)...)
	addToMetadataMap(dictionaryMetadata, metadataMap)
}

func RetrieveTSConfigurations(sortables *[]Sortable, metadataMap MetadataMap) {
	gplog.Verbose("Retrieving TEXT SEARCH CONFIGURATION information")
	configurations := GetTextSearchConfigurations(connectionPool)
	objectCounts["Text Search Configurations"] = len(configurations)
	configurationMetadata := GetMetadataForObjectType(connectionPool, TYPE_TSCONFIGURATION)

	*sortables = append(*sortables, convertToSortableSlice(configurations)...)
	addToMetadataMap(configurationMetadata, metadataMap)
}

func RetrieveOperators(sortables *[]Sortable, metadataMap MetadataMap) {
	gplog.Verbose("Retrieving OPERATOR information")
	operators := GetOperators(connectionPool)
	objectCounts["Operators"] = len(operators)
	operatorMetadata := GetMetadataForObjectType(connectionPool, TYPE_OPERATOR)

	*sortables = append(*sortables, convertToSortableSlice(operators)...)
	addToMetadataMap(operatorMetadata, metadataMap)
}

func RetrieveOperatorClasses(sortables *[]Sortable, metadataMap MetadataMap) {
	gplog.Verbose("Retrieving OPERATOR CLASS information")
	operatorClasses := GetOperatorClasses(connectionPool)
	objectCounts["Operator Classes"] = len(operatorClasses)
	operatorClassMetadata := GetMetadataForObjectType(connectionPool, TYPE_OPERATORCLASS)

	*sortables = append(*sortables, convertToSortableSlice(operatorClasses)...)
	addToMetadataMap(operatorClassMetadata, metadataMap)
}

func RetrieveAggregates(sortables *[]Sortable, metadataMap MetadataMap) {
	gplog.Verbose("Retrieving AGGREGATE information")
	aggregates := GetAggregates(connectionPool)
	objectCounts["Aggregates"] = len(aggregates)
	aggMetadata := GetMetadataForObjectType(connectionPool, TYPE_AGGREGATE)

	*sortables = append(*sortables, convertToSortableSlice(aggregates)...)
	addToMetadataMap(aggMetadata, metadataMap)
}

func RetrieveCasts(sortables *[]Sortable, metadataMap MetadataMap) {
	gplog.Verbose("Retrieving CAST information")
	casts := GetCasts(connectionPool)
	objectCounts["Casts"] = len(casts)
	castMetadata := GetCommentsForObjectType(connectionPool, TYPE_CAST)

	*sortables = append(*sortables, convertToSortableSlice(casts)...)
	addToMetadataMap(castMetadata, metadataMap)
}

/*
 * Generic metadata wrapper functions
 */

func LogBackupInfo() {
	gplog.Info("Backup Timestamp = %s", globalFPInfo.Timestamp)
	gplog.Info("Backup Database = %s", connectionPool.DBName)
	params := strings.Split(backupReport.BackupParamsString, "\n")
	for _, param := range params {
		gplog.Verbose(param)
	}
}

func BackupSessionGUCs(metadataFile *utils.FileWithByteCount) {
	gucs := GetSessionGUCs(connectionPool)
	PrintSessionGUCs(metadataFile, globalTOC, gucs)
}

/*
 * Global metadata wrapper functions
 */

func BackupTablespaces(metadataFile *utils.FileWithByteCount) {
	gplog.Verbose("Writing CREATE TABLESPACE statements to metadata file")
	tablespaces := GetTablespaces(connectionPool)
	objectCounts["Tablespaces"] = len(tablespaces)
	tablespaceMetadata := GetMetadataForObjectType(connectionPool, TYPE_TABLESPACE)
	PrintCreateTablespaceStatements(metadataFile, globalTOC, tablespaces, tablespaceMetadata)
}

func BackupCreateDatabase(metadataFile *utils.FileWithByteCount) {
	gplog.Verbose("Writing CREATE DATABASE statement to metadata file")
	db := GetDatabaseInfo(connectionPool)
	dbMetadata := GetMetadataForObjectType(connectionPool, TYPE_DATABASE)
	PrintCreateDatabaseStatement(metadataFile, globalTOC, db, dbMetadata)
}

func BackupDatabaseGUCs(metadataFile *utils.FileWithByteCount) {
	gplog.Verbose("Writing database GUCs to metadata file")
	databaseGucs := GetDatabaseGUCs(connectionPool)
	objectCounts["Database GUCs"] = len(databaseGucs)
	PrintDatabaseGUCs(metadataFile, globalTOC, databaseGucs, connectionPool.DBName)
}

func BackupResourceQueues(metadataFile *utils.FileWithByteCount) {
	gplog.Verbose("Writing CREATE RESOURCE QUEUE statements to metadata file")
	resQueues := GetResourceQueues(connectionPool)
	objectCounts["Resource Queues"] = len(resQueues)
	resQueueMetadata := GetCommentsForObjectType(connectionPool, TYPE_RESOURCEQUEUE)
	PrintCreateResourceQueueStatements(metadataFile, globalTOC, resQueues, resQueueMetadata)
}

func BackupResourceGroups(metadataFile *utils.FileWithByteCount) {
	gplog.Verbose("Writing CREATE RESOURCE GROUP statements to metadata file")
	resGroups := GetResourceGroups(connectionPool)
	objectCounts["Resource Groups"] = len(resGroups)
	resGroupMetadata := GetCommentsForObjectType(connectionPool, TYPE_RESOURCEGROUP)
	PrintResetResourceGroupStatements(metadataFile, globalTOC)
	PrintCreateResourceGroupStatements(metadataFile, globalTOC, resGroups, resGroupMetadata)
}

func BackupRoles(metadataFile *utils.FileWithByteCount) {
	gplog.Verbose("Writing CREATE ROLE statements to metadata file")
	roles := GetRoles(connectionPool)
	objectCounts["Roles"] = len(roles)
	roleGUCs := GetRoleGUCs(connectionPool)
	roleMetadata := GetCommentsForObjectType(connectionPool, TYPE_ROLE)
	PrintCreateRoleStatements(metadataFile, globalTOC, roles, roleGUCs, roleMetadata)
}

func BackupRoleGrants(metadataFile *utils.FileWithByteCount) {
	gplog.Verbose("Writing GRANT ROLE statements to metadata file")
	roleMembers := GetRoleMembers(connectionPool)
	PrintRoleMembershipStatements(metadataFile, globalTOC, roleMembers)
}

/*
 * Predata wrapper functions
 */

func BackupSchemas(metadataFile *utils.FileWithByteCount) {
	gplog.Verbose("Writing CREATE SCHEMA statements to metadata file")
	schemas := GetAllUserSchemas(connectionPool)
	objectCounts["Schemas"] = len(schemas)
	schemaMetadata := GetMetadataForObjectType(connectionPool, TYPE_SCHEMA)
	PrintCreateSchemaStatements(metadataFile, globalTOC, schemas, schemaMetadata)
}

func BackupProceduralLanguages(metadataFile *utils.FileWithByteCount, procLangs []ProceduralLanguage, langFuncs []Function, functionMetadata MetadataMap, funcInfoMap map[uint32]FunctionInfo) {
	gplog.Verbose("Writing CREATE PROCEDURAL LANGUAGE statements to metadata file")
	objectCounts["Procedural Languages"] = len(procLangs)
	for _, langFunc := range langFuncs {
		PrintCreateFunctionStatement(metadataFile, globalTOC, langFunc, functionMetadata[langFunc.GetUniqueID()])
	}
	procLangMetadata := GetMetadataForObjectType(connectionPool, TYPE_PROCLANGUAGE)
	PrintCreateLanguageStatements(metadataFile, globalTOC, procLangs, funcInfoMap, procLangMetadata)
}

func BackupForeignDataWrappers(metadataFile *utils.FileWithByteCount, funcInfoMap map[uint32]FunctionInfo) {
	gplog.Verbose("Writing CREATE FOREIGN DATA WRAPPER statements to metadata file")
	wrappers := GetForeignDataWrappers(connectionPool)
	objectCounts["Foreign Data Wrappers"] = len(wrappers)
	fdwMetadata := GetMetadataForObjectType(connectionPool, TYPE_FOREIGNDATAWRAPPER)
	PrintCreateForeignDataWrapperStatements(metadataFile, globalTOC, wrappers, funcInfoMap, fdwMetadata)
}

func BackupForeignServers(metadataFile *utils.FileWithByteCount) {
	gplog.Verbose("Writing CREATE SERVER statements to metadata file")
	servers := GetForeignServers(connectionPool)
	objectCounts["Foreign Servers"] = len(servers)
	serverMetadata := GetMetadataForObjectType(connectionPool, TYPE_FOREIGNSERVER)
	PrintCreateServerStatements(metadataFile, globalTOC, servers, serverMetadata)
}

func BackupUserMappings(metadataFile *utils.FileWithByteCount) {
	gplog.Verbose("Writing CREATE USER MAPPING statements to metadata file")
	mappings := GetUserMappings(connectionPool)
	objectCounts["User Mappings"] = len(mappings)
	PrintCreateUserMappingStatements(metadataFile, globalTOC, mappings)
}

func BackupShellTypes(metadataFile *utils.FileWithByteCount, types []Type) {
	gplog.Verbose("Writing CREATE TYPE statements for shell types to metadata file")
	PrintCreateShellTypeStatements(metadataFile, globalTOC, types)
}

func BackupEnumTypes(metadataFile *utils.FileWithByteCount, typeMetadata MetadataMap) {
	enums := GetEnumTypes(connectionPool)
	gplog.Verbose("Writing CREATE TYPE statements for enum types to metadata file")
	objectCounts["Types"] += len(enums)
	PrintCreateEnumTypeStatements(metadataFile, globalTOC, enums, typeMetadata)
}

func BackupCreateSequences(metadataFile *utils.FileWithByteCount, sequences []Sequence, relationMetadata MetadataMap) {
	gplog.Verbose("Writing CREATE SEQUENCE statements to metadata file")
	objectCounts["Sequences"] = len(sequences)
	PrintCreateSequenceStatements(metadataFile, globalTOC, sequences, relationMetadata)
}

func createBackupSet(objSlice []Sortable) (backupSet map[UniqueID]bool) {
	backupSet = make(map[UniqueID]bool, 0)
	for _, obj := range objSlice {
		backupSet[obj.GetUniqueID()] = true
	}

	return backupSet
}

func convertToSortableSlice(objSlice interface{}) []Sortable {
	sortableSlice := make([]Sortable, 0)
	s := reflect.ValueOf(objSlice)

	ret := make([]interface{}, s.Len())

	for i := 0; i < s.Len(); i++ {
		ret[i] = s.Index(i).Interface()
	}

	for _, obj := range ret {
		newObj := obj.(Sortable)

		// TODO: when types are broken out into separate structs don't pass enums and pseuo types in here.
		typeObj, ok := obj.(Type)
		if ok && (typeObj.Type == "e" || typeObj.Type == "p") {
			continue
		}

		sortableSlice = append(sortableSlice, newObj)
	}

	return sortableSlice
}

func addToMetadataMap(newMetadata MetadataMap, metadataMap MetadataMap) {
	for k, v := range newMetadata {
		metadataMap[k] = v
	}
}

// This function is fairly unwieldy, but there's not really a good way to break it down
func BackupDependentObjects(metadataFile *utils.FileWithByteCount, tables []Relation,
	protocols []ExternalProtocol, filteredMetadata MetadataMap, tableDefs map[uint32]TableDefinition,
	constraints []Constraint, sortables []Sortable, funcInfoMap map[uint32]FunctionInfo,
	tableOnly bool) {

	gplog.Verbose("Writing CREATE statements for dependent objects to metadata file")

	backupSet := createBackupSet(sortables)
	relevantDeps := GetDependencies(connectionPool, backupSet)
	if connectionPool.Version.Is("4") && !tableOnly {
		AddProtocolDependenciesForGPDB4(relevantDeps, tables, tableDefs, protocols)
	}
	sortedSlice := TopologicalSort(sortables, relevantDeps)

	PrintDependentObjectStatements(metadataFile, globalTOC, sortedSlice, filteredMetadata, tableDefs, constraints, funcInfoMap)
	extPartInfo, partInfoMap := GetExternalPartitionInfo(connectionPool)
	if len(extPartInfo) > 0 {
		gplog.Verbose("Writing EXCHANGE PARTITION statements to metadata file")
		PrintExchangeExternalPartitionStatements(metadataFile, globalTOC, extPartInfo, partInfoMap, tables)
	}
}

func BackupConversions(metadataFile *utils.FileWithByteCount) {
	gplog.Verbose("Writing CREATE CONVERSION statements to metadata file")
	conversions := GetConversions(connectionPool)
	objectCounts["Conversions"] = len(conversions)
	convMetadata := GetMetadataForObjectType(connectionPool, TYPE_CONVERSION)
	PrintCreateConversionStatements(metadataFile, globalTOC, conversions, convMetadata)
}

func BackupOperatorFamilies(metadataFile *utils.FileWithByteCount) {
	gplog.Verbose("Writing CREATE OPERATOR FAMILY statements to metadata file")
	operatorFamilies := GetOperatorFamilies(connectionPool)
	objectCounts["Operator Families"] = len(operatorFamilies)
	operatorFamilyMetadata := GetMetadataForObjectType(connectionPool, TYPE_OPERATORFAMILY)
	PrintCreateOperatorFamilyStatements(metadataFile, globalTOC, operatorFamilies, operatorFamilyMetadata)
}

func BackupCollations(metadataFile *utils.FileWithByteCount) {
	gplog.Verbose("Writing CREATE COLLATION statements to metadata file")
	collations := GetCollations(connectionPool)
	objectCounts["Collations"] = len(collations)
	collationMetadata := GetMetadataForObjectType(connectionPool, TYPE_COLLATION)
	PrintCreateCollationStatements(metadataFile, globalTOC, collations, collationMetadata)
}

func BackupExtensions(metadataFile *utils.FileWithByteCount) {
	gplog.Verbose("Writing CREATE EXTENSION statements to metadata file")
	extensions := GetExtensions(connectionPool)
	objectCounts["Extensions"] = len(extensions)
	extensionMetadata := GetCommentsForObjectType(connectionPool, TYPE_EXTENSION)
	PrintCreateExtensionStatements(metadataFile, globalTOC, extensions, extensionMetadata)
}

func BackupConstraints(metadataFile *utils.FileWithByteCount, constraints []Constraint, conMetadata MetadataMap) {
	gplog.Verbose("Writing ADD CONSTRAINT statements to metadata file")
	objectCounts["Constraints"] = len(constraints)
	PrintConstraintStatements(metadataFile, globalTOC, constraints, conMetadata)
}

/*
 * Postdata wrapper functions
 */

func BackupIndexes(metadataFile *utils.FileWithByteCount) {
	gplog.Verbose("Writing CREATE INDEX statements to metadata file")
	indexes := GetIndexes(connectionPool)
	objectCounts["Indexes"] = len(indexes)
	indexMetadata := GetCommentsForObjectType(connectionPool, TYPE_INDEX)
	PrintCreateIndexStatements(metadataFile, globalTOC, indexes, indexMetadata)
}

func BackupRules(metadataFile *utils.FileWithByteCount) {
	gplog.Verbose("Writing CREATE RULE statements to metadata file")
	rules := GetRules(connectionPool)
	objectCounts["Rules"] = len(rules)
	ruleMetadata := GetCommentsForObjectType(connectionPool, TYPE_RULE)
	PrintCreateRuleStatements(metadataFile, globalTOC, rules, ruleMetadata)
}

func BackupTriggers(metadataFile *utils.FileWithByteCount) {
	gplog.Verbose("Writing CREATE TRIGGER statements to metadata file")
	triggers := GetTriggers(connectionPool)
	objectCounts["Triggers"] = len(triggers)
	triggerMetadata := GetCommentsForObjectType(connectionPool, TYPE_TRIGGER)
	PrintCreateTriggerStatements(metadataFile, globalTOC, triggers, triggerMetadata)
}

func BackupEventTriggers(metadataFile *utils.FileWithByteCount) {
	gplog.Verbose("Writing CREATE EVENT TRIGGER statements to metadata file")
	eventTriggers := GetEventTriggers(connectionPool)
	objectCounts["Event Triggers"] = len(eventTriggers)
	eventTriggerMetadata := GetMetadataForObjectType(connectionPool, TYPE_EVENTTRIGGER)
	PrintCreateEventTriggerStatements(metadataFile, globalTOC, eventTriggers, eventTriggerMetadata)
}

/*
 * Data wrapper functions
 */

func BackupStatistics(statisticsFile *utils.FileWithByteCount, tables []Relation) {
	attStats := GetAttributeStatistics(connectionPool, tables)
	tupleStats := GetTupleStatistics(connectionPool, tables)

	BackupSessionGUCs(statisticsFile)
	PrintStatisticsStatements(statisticsFile, globalTOC, tables, attStats, tupleStats)
}

func BackupIncrementalMetadata() {
	aoTableEntries := GetAOIncrementalMetadata(connectionPool)
	globalTOC.IncrementalMetadata.AO = aoTableEntries
}
