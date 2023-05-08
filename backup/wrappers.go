package backup

import (
	"fmt"
	"path"
	"reflect"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpbackup/history"
	"github.com/greenplum-db/gpbackup/options"
	"github.com/greenplum-db/gpbackup/report"
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
	if MustGetFlagBool(options.QUIET) {
		gplog.SetVerbosity(gplog.LOGERROR)
	} else if MustGetFlagBool(options.DEBUG) {
		gplog.SetVerbosity(gplog.LOGDEBUG)
	} else if MustGetFlagBool(options.VERBOSE) {
		gplog.SetVerbosity(gplog.LOGVERBOSE)
	}
}

func initializeConnectionPool(timestamp string) {
	var numConns int
	var err error
	connectionPool = dbconn.NewDBConnFromEnvironment(MustGetFlagString(options.DBNAME))
	/*
	 * The additional connection is used to hold the locks and process any deferred
	 * tables while not being assigned any tables up front. The benefit is that the
	 * connection holding the locks won't be overburdened by processing an assigned
	 * set of tables and any extra deferred tables, at the cost of making one
	 * additional connection to the database.
	 */
	switch true {
	case FlagChanged(options.COPY_QUEUE_SIZE):
		numConns = MustGetFlagInt(options.COPY_QUEUE_SIZE) + 1
	case FlagChanged(options.JOBS):
		numConns = MustGetFlagInt(options.JOBS) + 1
	default:
		numConns = 2
	}

	gplog.Verbose(fmt.Sprintf("Initializing %d database connections", numConns))
	connectionPool.MustConnect(numConns)

	utils.ValidateGPDBVersionCompatibility(connectionPool)
	InitializeMetadataParams(connectionPool)
	// Begin transactions, initialize the synchronized snapshot, and set session GUCs
	for connNum := 0; connNum < connectionPool.NumConns; connNum++ {
		connectionPool.MustExec(fmt.Sprintf("SET application_name TO 'gpbackup_%s'", timestamp), connNum)
		// Begins transaction in repeatable read
		connectionPool.MustBegin(connNum)
		if connectionPool.Version.AtLeast(SNAPSHOT_GPDB_MIN_VERSION) && backupSnapshot == "" {
			backupSnapshot, err = GetSynchronizedSnapshot(connectionPool)
			if err != nil {
				gplog.FatalOnError(err)
			}
		}

		SetSessionGUCs(connNum)
	}
}

func SetSessionGUCs(connNum int) {
	// These GUCs ensure the dumps portability accross systems
	connectionPool.MustExec("SET search_path TO pg_catalog", connNum)
	connectionPool.MustExec("SET statement_timeout = 0", connNum)
	connectionPool.MustExec("SET DATESTYLE = ISO", connNum)
	connectionPool.MustExec("SET standard_conforming_strings = 1", connNum) // Needed for 4.3, default on in 5+
	connectionPool.MustExec("SET enable_mergejoin TO off", connNum)

	// The fix to raise the max of extra_float_digits GUC is going out with
	// GPDB 4.3.33.1. This means if we set the GUC using 'SET
	// extra_float_digits=3', gpbackup would be broken with GPDB 4.3.33.0 since
	// our Semver package only allows up to 3 digits. To avoid any complicated
	// version diffs of setting this GUC, we use set_config() with a subquery
	// getting the max value of the GUC.
	connectionPool.MustExec("SELECT set_config('extra_float_digits', (SELECT max_val FROM pg_settings WHERE name = 'extra_float_digits'), false)", connNum)

	if connectionPool.Version.AtLeast("5") {
		connectionPool.MustExec("SET synchronize_seqscans TO off", connNum)
	}
	if connectionPool.Version.AtLeast("6") {
		connectionPool.MustExec("SET INTERVALSTYLE = POSTGRES", connNum)
		connectionPool.MustExec("SET lock_timeout = 0", connNum)
	}

	if connectionPool.Version.AtLeast("7") {
		// This is a GPDB7+ GUC that can terminate sessions with open transactions that have been idle for too long, so we disable it.
		connectionPool.MustExec("SET idle_in_transaction_session_timeout = 0", connNum)
	}
}

func NewBackupConfig(dbName string, dbVersion string, backupVersion string, plugin string, timestamp string, opts options.Options) *history.BackupConfig {
	backupConfig := history.BackupConfig{
		BackupDir:             MustGetFlagString(options.BACKUP_DIR),
		BackupVersion:         backupVersion,
		Compressed:            !MustGetFlagBool(options.NO_COMPRESSION),
		CompressionType:       MustGetFlagString(options.COMPRESSION_TYPE),
		DatabaseName:          dbName,
		DatabaseVersion:       dbVersion,
		DataOnly:              MustGetFlagBool(options.DATA_ONLY),
		ExcludeRelations:      MustGetFlagStringArray(options.EXCLUDE_RELATION),
		ExcludeSchemaFiltered: len(MustGetFlagStringArray(options.EXCLUDE_SCHEMA)) > 0,
		ExcludeSchemas:        MustGetFlagStringArray(options.EXCLUDE_SCHEMA),
		ExcludeTableFiltered:  len(MustGetFlagStringArray(options.EXCLUDE_RELATION)) > 0,
		IncludeRelations:      opts.GetOriginalIncludedTables(),
		IncludeSchemaFiltered: len(MustGetFlagStringArray(options.INCLUDE_SCHEMA)) > 0,
		IncludeSchemas:        MustGetFlagStringArray(options.INCLUDE_SCHEMA),
		IncludeTableFiltered:  len(opts.GetOriginalIncludedTables()) > 0,
		Incremental:           MustGetFlagBool(options.INCREMENTAL),
		LeafPartitionData:     MustGetFlagBool(options.LEAF_PARTITION_DATA),
		MetadataOnly:          MustGetFlagBool(options.METADATA_ONLY),
		Plugin:                plugin,
		SingleDataFile:        MustGetFlagBool(options.SINGLE_DATA_FILE),
		Timestamp:             timestamp,
		WithoutGlobals:        MustGetFlagBool(options.WITHOUT_GLOBALS),
		WithStatistics:        MustGetFlagBool(options.WITH_STATS),
		Status:                history.BackupStatusInProgress,
	}

	return &backupConfig
}

func initializeBackupReport(opts options.Options) {
	escapedDBName := dbconn.MustSelectString(connectionPool, fmt.Sprintf("select quote_ident(datname) AS string FROM pg_database where datname='%s'", utils.EscapeSingleQuotes(connectionPool.DBName)))
	plugin := ""
	if pluginConfig != nil {
		_, plugin = path.Split(pluginConfig.ExecutablePath)
	}
	config := NewBackupConfig(escapedDBName, connectionPool.Version.VersionString, version,
		plugin, globalFPInfo.Timestamp, opts)

	isFilteredBackup := config.IncludeTableFiltered || config.IncludeSchemaFiltered ||
		config.ExcludeTableFiltered || config.ExcludeSchemaFiltered
	dbSize := ""
	if !MustGetFlagBool(options.METADATA_ONLY) && !isFilteredBackup {
		gplog.Verbose("Getting database size")
		//Potentially expensive query
		dbSize = GetDBSize(connectionPool)
	}

	config.SegmentCount = len(globalCluster.ContentIDs) - 1

	backupReport = &report.Report{
		DatabaseSize: dbSize,
		BackupConfig: *config,
	}
	backupReport.ConstructBackupParamsString()
}

func createBackupLockFile(timestamp string) {
	var err error
	var timestampLockFile string
	metadataOnly := MustGetFlagBool(options.METADATA_ONLY)
	backupDir := MustGetFlagString(options.BACKUP_DIR)
	noHistory := MustGetFlagBool(options.NO_HISTORY)
	if metadataOnly && noHistory && backupDir != "" {
		timestampLockFile = fmt.Sprintf("%s/%s.lck", backupDir, timestamp)
	} else {
		timestampLockFile = fmt.Sprintf("/tmp/%s.lck", timestamp)
	}
	backupLockFile, err = lockfile.New(timestampLockFile)
	gplog.FatalOnError(err)
	err = backupLockFile.TryLock()
	if err != nil {
		gplog.Error(err.Error())
		gplog.Fatal(errors.Errorf("A backup with timestamp %s is already in progress. Wait 1 second and try the backup again.", timestamp), "")
	}
}

func createBackupDirectoriesOnAllHosts() {
	remoteOutput := globalCluster.GenerateAndExecuteCommand("Creating backup directories",
		cluster.ON_SEGMENTS|cluster.INCLUDE_COORDINATOR,
		func(contentID int) string {
			return fmt.Sprintf("mkdir -p %s", globalFPInfo.GetDirForContent(contentID))
		})
	globalCluster.CheckClusterError(remoteOutput, "Unable to create backup directories", func(contentID int) string {
		return fmt.Sprintf("Unable to create backup directory %s", globalFPInfo.GetDirForContent(contentID))
	})
}

/*
 * Metadata retrieval wrapper functions
 */

func RetrieveAndProcessTables() ([]Table, []Table) {
	quotedIncludeRelations, err := options.QuoteTableNames(connectionPool, MustGetFlagStringArray(options.INCLUDE_RELATION))
	gplog.FatalOnError(err)

	tableRelations := GetIncludedUserTableRelations(connectionPool, quotedIncludeRelations)
	LockTables(connectionPool, tableRelations)

	if connectionPool.Version.AtLeast("6") {
		tableRelations = append(tableRelations, GetForeignTableRelations(connectionPool)...)
	}

	tables := ConstructDefinitionsForTables(connectionPool, tableRelations)

	metadataTables, dataTables := SplitTablesByPartitionType(tables, quotedIncludeRelations)
	objectCounts["Tables"] = len(metadataTables)

	return metadataTables, dataTables
}

func retrieveFunctions(sortables *[]Sortable, metadataMap MetadataMap) ([]Function, map[uint32]FunctionInfo) {
	gplog.Verbose("Retrieving function information")
	functionMetadata := GetMetadataForObjectType(connectionPool, TYPE_FUNCTION)
	addToMetadataMap(functionMetadata, metadataMap)
	functions := GetFunctionsAllVersions(connectionPool)
	funcInfoMap := GetFunctionOidToInfoMap(connectionPool)
	objectCounts["Functions"] = len(functions)
	*sortables = append(*sortables, convertToSortableSlice(functions)...)

	return functions, funcInfoMap
}

func retrieveTransforms(sortables *[]Sortable) {
	if connectionPool.Version.Before("7") {
		return
	}
	gplog.Verbose("Retrieving transform information")
	transforms := GetTransforms(connectionPool)
	objectCounts["Transforms"] = len(transforms)
	*sortables = append(*sortables, convertToSortableSlice(transforms)...)
}

func retrieveAndBackupTypes(metadataFile *utils.FileWithByteCount, sortables *[]Sortable, metadataMap MetadataMap) {
	gplog.Verbose("Retrieving type information")
	shells := GetShellTypes(connectionPool)
	bases := GetBaseTypes(connectionPool)
	composites := GetCompositeTypes(connectionPool)
	domains := GetDomainTypes(connectionPool)
	rangeTypes := make([]RangeType, 0)
	if connectionPool.Version.AtLeast("6") {
		rangeTypes = GetRangeTypes(connectionPool)
	}
	typeMetadata := GetMetadataForObjectType(connectionPool, TYPE_TYPE)

	backupShellTypes(metadataFile, shells, bases, rangeTypes)
	if connectionPool.Version.AtLeast("5") {
		backupEnumTypes(metadataFile, typeMetadata)
	}

	objectCounts["Types"] += len(shells)
	objectCounts["Types"] += len(bases)
	objectCounts["Types"] += len(composites)
	objectCounts["Types"] += len(domains)
	objectCounts["Types"] += len(rangeTypes)
	*sortables = append(*sortables, convertToSortableSlice(bases)...)
	*sortables = append(*sortables, convertToSortableSlice(composites)...)
	*sortables = append(*sortables, convertToSortableSlice(domains)...)
	*sortables = append(*sortables, convertToSortableSlice(rangeTypes)...)
	addToMetadataMap(typeMetadata, metadataMap)
}

func retrieveConstraints(sortables *[]Sortable, metadataMap MetadataMap, tables ...Relation) []Constraint {
	gplog.Verbose("Retrieving constraints")
	constraints := GetConstraints(connectionPool, tables...)
	if len(constraints) > 0 && connectionPool.Version.AtLeast("7") {
		RenameExchangedPartitionConstraints(connectionPool, &constraints)
	}

	//split into domain constraints and all others, as they are handled differently downstream
	domainConstraints := make([]Constraint, 0)
	nonDomainConstraints := make([]Constraint, 0)
	for _, con := range constraints {
		if con.IsDomainConstraint {
			domainConstraints = append(domainConstraints, con)
		} else {
			nonDomainConstraints = append(nonDomainConstraints, con)
		}
	}

	objectCounts["Constraints"] = len(nonDomainConstraints)
	conMetadata := GetCommentsForObjectType(connectionPool, TYPE_CONSTRAINT)
	*sortables = append(*sortables, convertToSortableSlice(nonDomainConstraints)...)
	addToMetadataMap(conMetadata, metadataMap)
	return domainConstraints
}

func retrieveAndBackupSequences(metadataFile *utils.FileWithByteCount,
	relationMetadata MetadataMap) []Sequence {
	gplog.Verbose("Writing CREATE SEQUENCE statements to metadata file")
	sequences := GetAllSequences(connectionPool)
	objectCounts["Sequences"] = len(sequences)
	PrintCreateSequenceStatements(metadataFile, globalTOC, sequences, relationMetadata)
	return sequences
}

func retrieveProtocols(sortables *[]Sortable, metadataMap MetadataMap) []ExternalProtocol {
	gplog.Verbose("Retrieving protocols")
	protocols := GetExternalProtocols(connectionPool)
	objectCounts["Protocols"] = len(protocols)
	protoMetadata := GetMetadataForObjectType(connectionPool, TYPE_PROTOCOL)

	*sortables = append(*sortables, convertToSortableSlice(protocols)...)
	addToMetadataMap(protoMetadata, metadataMap)

	return protocols
}

func retrieveViews(sortables *[]Sortable) {
	gplog.Verbose("Retrieving views")
	views := GetAllViews(connectionPool)
	objectCounts["Views"] = len(views)

	*sortables = append(*sortables, convertToSortableSlice(views)...)
}

func retrieveTSObjects(sortables *[]Sortable, metadataMap MetadataMap) {
	if !connectionPool.Version.AtLeast("5") {
		return
	}
	gplog.Verbose("Retrieving Text Search Parsers")
	retrieveTSParsers(sortables, metadataMap)
	retrieveTSConfigurations(sortables, metadataMap)
	retrieveTSTemplates(sortables, metadataMap)
	retrieveTSDictionaries(sortables, metadataMap)
}

func retrieveTSParsers(sortables *[]Sortable, metadataMap MetadataMap) {
	gplog.Verbose("Retrieving Text Search Parsers")
	parsers := GetTextSearchParsers(connectionPool)
	objectCounts["Text Search Parsers"] = len(parsers)
	parserMetadata := GetCommentsForObjectType(connectionPool, TYPE_TSPARSER)

	*sortables = append(*sortables, convertToSortableSlice(parsers)...)
	addToMetadataMap(parserMetadata, metadataMap)
}

func retrieveTSTemplates(sortables *[]Sortable, metadataMap MetadataMap) {
	gplog.Verbose("Retrieving TEXT SEARCH TEMPLATE information")
	templates := GetTextSearchTemplates(connectionPool)
	objectCounts["Text Search Templates"] = len(templates)
	templateMetadata := GetCommentsForObjectType(connectionPool, TYPE_TSTEMPLATE)

	*sortables = append(*sortables, convertToSortableSlice(templates)...)
	addToMetadataMap(templateMetadata, metadataMap)
}

func retrieveTSDictionaries(sortables *[]Sortable, metadataMap MetadataMap) {
	gplog.Verbose("Retrieving TEXT SEARCH DICTIONARY information")
	dictionaries := GetTextSearchDictionaries(connectionPool)
	objectCounts["Text Search Dictionaries"] = len(dictionaries)
	dictionaryMetadata := GetMetadataForObjectType(connectionPool, TYPE_TSDICTIONARY)

	*sortables = append(*sortables, convertToSortableSlice(dictionaries)...)
	addToMetadataMap(dictionaryMetadata, metadataMap)
}

func retrieveTSConfigurations(sortables *[]Sortable, metadataMap MetadataMap) {
	gplog.Verbose("Retrieving TEXT SEARCH CONFIGURATION information")
	configurations := GetTextSearchConfigurations(connectionPool)
	objectCounts["Text Search Configurations"] = len(configurations)
	configurationMetadata := GetMetadataForObjectType(connectionPool, TYPE_TSCONFIGURATION)

	*sortables = append(*sortables, convertToSortableSlice(configurations)...)
	addToMetadataMap(configurationMetadata, metadataMap)
}

func retrieveOperatorObjects(sortables *[]Sortable, metadataMap MetadataMap) {
	retrieveOperators(sortables, metadataMap)
	retrieveOperatorClasses(sortables, metadataMap)
}

func retrieveOperators(sortables *[]Sortable, metadataMap MetadataMap) {
	gplog.Verbose("Retrieving OPERATOR information")
	operators := GetOperators(connectionPool)
	objectCounts["Operators"] = len(operators)
	operatorMetadata := GetMetadataForObjectType(connectionPool, TYPE_OPERATOR)

	*sortables = append(*sortables, convertToSortableSlice(operators)...)
	addToMetadataMap(operatorMetadata, metadataMap)
}

func retrieveOperatorClasses(sortables *[]Sortable, metadataMap MetadataMap) {
	gplog.Verbose("Retrieving OPERATOR CLASS information")
	operatorClasses := GetOperatorClasses(connectionPool)
	objectCounts["Operator Classes"] = len(operatorClasses)
	operatorClassMetadata := GetMetadataForObjectType(connectionPool, TYPE_OPERATORCLASS)

	*sortables = append(*sortables, convertToSortableSlice(operatorClasses)...)
	addToMetadataMap(operatorClassMetadata, metadataMap)
}

func retrieveAggregates(sortables *[]Sortable, metadataMap MetadataMap) {
	gplog.Verbose("Retrieving AGGREGATE information")
	aggregates := GetAggregates(connectionPool)
	objectCounts["Aggregates"] = len(aggregates)
	aggMetadata := GetMetadataForObjectType(connectionPool, TYPE_AGGREGATE)

	*sortables = append(*sortables, convertToSortableSlice(aggregates)...)
	addToMetadataMap(aggMetadata, metadataMap)
}

func retrieveCasts(sortables *[]Sortable, metadataMap MetadataMap) {
	gplog.Verbose("Retrieving CAST information")
	casts := GetCasts(connectionPool)
	objectCounts["Casts"] = len(casts)
	castMetadata := GetCommentsForObjectType(connectionPool, TYPE_CAST)

	*sortables = append(*sortables, convertToSortableSlice(casts)...)
	addToMetadataMap(castMetadata, metadataMap)
}

func retrieveFDWObjects(sortables *[]Sortable, metadataMap MetadataMap) {
	if !connectionPool.Version.AtLeast("6") {
		return
	}
	retrieveForeignDataWrappers(sortables, metadataMap)
	retrieveForeignServers(sortables, metadataMap)
	retrieveUserMappings(sortables)
}

func retrieveForeignDataWrappers(sortables *[]Sortable, metadataMap MetadataMap) {
	gplog.Verbose("Writing CREATE FOREIGN DATA WRAPPER statements to metadata file")
	wrappers := GetForeignDataWrappers(connectionPool)
	objectCounts["Foreign Data Wrappers"] = len(wrappers)
	fdwMetadata := GetMetadataForObjectType(connectionPool, TYPE_FOREIGNDATAWRAPPER)

	*sortables = append(*sortables, convertToSortableSlice(wrappers)...)
	addToMetadataMap(fdwMetadata, metadataMap)
}

func retrieveForeignServers(sortables *[]Sortable, metadataMap MetadataMap) {
	gplog.Verbose("Writing CREATE SERVER statements to metadata file")
	servers := GetForeignServers(connectionPool)
	objectCounts["Foreign Servers"] = len(servers)
	serverMetadata := GetMetadataForObjectType(connectionPool, TYPE_FOREIGNSERVER)

	*sortables = append(*sortables, convertToSortableSlice(servers)...)
	addToMetadataMap(serverMetadata, metadataMap)
}

func retrieveUserMappings(sortables *[]Sortable) {
	gplog.Verbose("Writing CREATE USER MAPPING statements to metadata file")
	mappings := GetUserMappings(connectionPool)
	objectCounts["User Mappings"] = len(mappings)
	// No comments, owners, or ACLs on UserMappings so no need to get metadata

	*sortables = append(*sortables, convertToSortableSlice(mappings)...)
}

func backupSessionGUC(metadataFile *utils.FileWithByteCount) {
	gplog.Verbose("Writing Session Configuration Parameters to metadata file")
	gucs := GetSessionGUCs(connectionPool)
	PrintSessionGUCs(metadataFile, globalTOC, gucs)
}

/*
 * Global metadata wrapper functions
 */

func backupTablespaces(metadataFile *utils.FileWithByteCount) {
	gplog.Verbose("Writing CREATE TABLESPACE statements to metadata file")
	tablespaces := GetTablespaces(connectionPool)
	objectCounts["Tablespaces"] = len(tablespaces)
	tablespaceMetadata := GetMetadataForObjectType(connectionPool, TYPE_TABLESPACE)
	PrintCreateTablespaceStatements(metadataFile, globalTOC, tablespaces, tablespaceMetadata)
}

func backupCreateDatabase(metadataFile *utils.FileWithByteCount) {
	gplog.Verbose("Writing CREATE DATABASE statement to metadata file")
	defaultDB := GetDefaultDatabaseEncodingInfo(connectionPool)
	db := GetDatabaseInfo(connectionPool)
	dbMetadata := GetMetadataForObjectType(connectionPool, TYPE_DATABASE)
	PrintCreateDatabaseStatement(metadataFile, globalTOC, defaultDB, db, dbMetadata)
}

func backupDatabaseGUCs(metadataFile *utils.FileWithByteCount) {
	gplog.Verbose("Writing Database Configuration Parameters to metadata file")
	databaseGucs := GetDatabaseGUCs(connectionPool)
	objectCounts["Database GUCs"] = len(databaseGucs)
	PrintDatabaseGUCs(metadataFile, globalTOC, databaseGucs, connectionPool.DBName)
}

func backupResourceQueues(metadataFile *utils.FileWithByteCount) {
	gplog.Verbose("Writing CREATE RESOURCE QUEUE statements to metadata file")
	resQueues := GetResourceQueues(connectionPool)
	objectCounts["Resource Queues"] = len(resQueues)
	resQueueMetadata := GetCommentsForObjectType(connectionPool, TYPE_RESOURCEQUEUE)
	PrintCreateResourceQueueStatements(metadataFile, globalTOC, resQueues, resQueueMetadata)
}

func backupResourceGroups(metadataFile *utils.FileWithByteCount) {
	if !connectionPool.Version.AtLeast("5") {
		return
	}
	gplog.Verbose("Writing CREATE RESOURCE GROUP statements to metadata file")
	if connectionPool.Version.Before("7") {
		resGroups := GetResourceGroups[ResourceGroupBefore7](connectionPool)
		objectCounts["Resource Groups"] = len(resGroups)
		resGroupMetadata := GetCommentsForObjectType(connectionPool, TYPE_RESOURCEGROUP)
		PrintResetResourceGroupStatements(metadataFile, globalTOC)
		PrintCreateResourceGroupStatementsBefore7(metadataFile, globalTOC, resGroups, resGroupMetadata)
	} else { // GPDB7+
		resGroups := GetResourceGroups[ResourceGroupAtLeast7](connectionPool)
		objectCounts["Resource Groups"] = len(resGroups)
		resGroupMetadata := GetCommentsForObjectType(connectionPool, TYPE_RESOURCEGROUP)
		PrintResetResourceGroupStatements(metadataFile, globalTOC)
		PrintCreateResourceGroupStatementsAtLeast7(metadataFile, globalTOC, resGroups, resGroupMetadata)
	}
}

func backupRoles(metadataFile *utils.FileWithByteCount) {
	gplog.Verbose("Writing CREATE ROLE statements to metadata file")
	roles := GetRoles(connectionPool)
	objectCounts["Roles"] = len(roles)
	roleMetadata := GetMetadataForObjectType(connectionPool, TYPE_ROLE)
	PrintCreateRoleStatements(metadataFile, globalTOC, roles, roleMetadata)
}

func backupRoleGUCs(metadataFile *utils.FileWithByteCount) {
	gplog.Verbose("Writing ROLE Configuration Parameter to meadata file")
	roleGUCs := GetRoleGUCs(connectionPool)
	PrintRoleGUCStatements(metadataFile, globalTOC, roleGUCs)
}

func backupRoleGrants(metadataFile *utils.FileWithByteCount) {
	gplog.Verbose("Writing GRANT ROLE statements to metadata file")
	roleMembers := GetRoleMembers(connectionPool)
	PrintRoleMembershipStatements(metadataFile, globalTOC, roleMembers)
}

/*
 * Predata wrapper functions
 */

func backupSchemas(metadataFile *utils.FileWithByteCount, partitionAlteredSchemas map[string]bool) {
	gplog.Verbose("Writing CREATE SCHEMA statements to metadata file")
	schemas := GetAllUserSchemas(connectionPool, partitionAlteredSchemas)
	objectCounts["Schemas"] = len(schemas)
	schemaMetadata := GetMetadataForObjectType(connectionPool, TYPE_SCHEMA)
	PrintCreateSchemaStatements(metadataFile, globalTOC, schemas, schemaMetadata)
}

func backupProceduralLanguages(metadataFile *utils.FileWithByteCount,
	functions []Function, funcInfoMap map[uint32]FunctionInfo, functionMetadata MetadataMap) {
	gplog.Verbose("Writing CREATE PROCEDURAL LANGUAGE statements to metadata file")
	procLangs := GetProceduralLanguages(connectionPool)
	objectCounts["Procedural Languages"] = len(procLangs)
	langFuncs, _ := ExtractLanguageFunctions(functions, procLangs)
	for _, langFunc := range langFuncs {
		PrintCreateFunctionStatement(metadataFile, globalTOC, langFunc, functionMetadata[langFunc.GetUniqueID()])
	}
	procLangMetadata := GetMetadataForObjectType(connectionPool, TYPE_PROCLANGUAGE)
	PrintCreateLanguageStatements(metadataFile, globalTOC, procLangs, funcInfoMap, procLangMetadata)
}

func backupShellTypes(metadataFile *utils.FileWithByteCount, shellTypes []ShellType, baseTypes []BaseType, rangeTypes []RangeType) {
	gplog.Verbose("Writing CREATE TYPE statements for shell types to metadata file")
	PrintCreateShellTypeStatements(metadataFile, globalTOC, shellTypes, baseTypes, rangeTypes)
}

func backupEnumTypes(metadataFile *utils.FileWithByteCount, typeMetadata MetadataMap) {
	gplog.Verbose("Writing CREATE TYPE statements for enum types to metadata file")
	enums := GetEnumTypes(connectionPool)
	objectCounts["Types"] += len(enums)
	PrintCreateEnumTypeStatements(metadataFile, globalTOC, enums, typeMetadata)
}

func backupAccessMethods(metadataFile *utils.FileWithByteCount) {
	if connectionPool.Version.Before("7") {
		return
	}
	gplog.Verbose("Writing CREATE ACCESS METHOD statements to metadata file")
	accessMethods := GetAccessMethods(connectionPool)
	objectCounts["Access Methods"] = len(accessMethods)
	accessMethodsMetadata := GetMetadataForObjectType(connectionPool, TYPE_ACCESS_METHOD)
	PrintAccessMethodStatements(metadataFile, globalTOC, accessMethods, accessMethodsMetadata)
}

func createBackupSet(objSlice []Sortable) (backupSet map[UniqueID]bool) {
	backupSet = make(map[UniqueID]bool)
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
func backupDependentObjects(metadataFile *utils.FileWithByteCount, tables []Table,
	protocols []ExternalProtocol, filteredMetadata MetadataMap, domainConstraints []Constraint,
	sortables []Sortable, sequences []Sequence, funcInfoMap map[uint32]FunctionInfo, tableOnly bool) {

	gplog.Verbose("Writing CREATE statements for dependent objects to metadata file")

	backupSet := createBackupSet(sortables)
	relevantDeps := GetDependencies(connectionPool, backupSet, tables)
	if connectionPool.Version.Is("4") && !tableOnly {
		AddProtocolDependenciesForGPDB4(relevantDeps, tables, protocols)
	}

	sortedSlice := TopologicalSort(sortables, relevantDeps)

	PrintDependentObjectStatements(metadataFile, globalTOC, sortedSlice, filteredMetadata, domainConstraints, funcInfoMap)
	PrintIdentityColumns(metadataFile, globalTOC, sequences)
	PrintAlterSequenceStatements(metadataFile, globalTOC, sequences)
	extPartInfo, partInfoMap := GetExternalPartitionInfo(connectionPool)
	if connectionPool.Version.Before("7") && len(extPartInfo) > 0 {
		gplog.Verbose("Writing EXCHANGE PARTITION statements to metadata file")
		PrintExchangeExternalPartitionStatements(metadataFile, globalTOC, extPartInfo, partInfoMap, tables)
	}
}

func backupConversions(metadataFile *utils.FileWithByteCount) {
	gplog.Verbose("Writing CREATE CONVERSION statements to metadata file")
	conversions := GetConversions(connectionPool)
	objectCounts["Conversions"] = len(conversions)
	convMetadata := GetMetadataForObjectType(connectionPool, TYPE_CONVERSION)
	PrintCreateConversionStatements(metadataFile, globalTOC, conversions, convMetadata)
}

func backupOperatorFamilies(metadataFile *utils.FileWithByteCount) {
	if !connectionPool.Version.AtLeast("5") {
		return
	}
	gplog.Verbose("Writing CREATE OPERATOR FAMILY statements to metadata file")
	operatorFamilies := GetOperatorFamilies(connectionPool)
	objectCounts["Operator Families"] = len(operatorFamilies)
	operatorFamilyMetadata := GetMetadataForObjectType(connectionPool, TYPE_OPERATORFAMILY)
	PrintCreateOperatorFamilyStatements(metadataFile, globalTOC, operatorFamilies, operatorFamilyMetadata)
}

func backupCollations(metadataFile *utils.FileWithByteCount) {
	if !connectionPool.Version.AtLeast("6") {
		return
	}
	gplog.Verbose("Writing CREATE COLLATION statements to metadata file")
	collations := GetCollations(connectionPool)
	objectCounts["Collations"] = len(collations)
	collationMetadata := GetMetadataForObjectType(connectionPool, TYPE_COLLATION)
	PrintCreateCollationStatements(metadataFile, globalTOC, collations, collationMetadata)
}

func backupExtensions(metadataFile *utils.FileWithByteCount) {
	if !(len(MustGetFlagStringArray(options.INCLUDE_SCHEMA)) == 0 &&
		connectionPool.Version.AtLeast("5")) {
		return
	}
	gplog.Verbose("Writing CREATE EXTENSION statements to metadata file")
	extensions := GetExtensions(connectionPool)
	objectCounts["Extensions"] = len(extensions)
	extensionMetadata := GetCommentsForObjectType(connectionPool, TYPE_EXTENSION)
	PrintCreateExtensionStatements(metadataFile, globalTOC, extensions, extensionMetadata)
}

/*
 * Postdata wrapper functions
 */

func backupIndexes(metadataFile *utils.FileWithByteCount) {
	gplog.Verbose("Writing CREATE INDEX statements to metadata file")
	indexes := GetIndexes(connectionPool)
	objectCounts["Indexes"] = len(indexes)
	if objectCounts["Indexes"] > 0 && connectionPool.Version.Is("6") {
		// This bug is not addressed in versions prior to GPDB6
		// New partition exchange syntax in GPDB7+ obviates the need for this renaming
		RenameExchangedPartitionIndexes(connectionPool, &indexes)
	}
	indexMetadata := GetCommentsForObjectType(connectionPool, TYPE_INDEX)
	PrintCreateIndexStatements(metadataFile, globalTOC, indexes, indexMetadata)
}

func backupRules(metadataFile *utils.FileWithByteCount) {
	gplog.Verbose("Writing CREATE RULE statements to metadata file")
	rules := GetRules(connectionPool)
	objectCounts["Rules"] = len(rules)
	ruleMetadata := GetCommentsForObjectType(connectionPool, TYPE_RULE)
	PrintCreateRuleStatements(metadataFile, globalTOC, rules, ruleMetadata)
}

func backupTriggers(metadataFile *utils.FileWithByteCount) {
	gplog.Verbose("Writing CREATE TRIGGER statements to metadata file")
	triggers := GetTriggers(connectionPool)
	objectCounts["Triggers"] = len(triggers)
	triggerMetadata := GetCommentsForObjectType(connectionPool, TYPE_TRIGGER)
	PrintCreateTriggerStatements(metadataFile, globalTOC, triggers, triggerMetadata)
}

func backupEventTriggers(metadataFile *utils.FileWithByteCount) {
	gplog.Verbose("Writing CREATE EVENT TRIGGER statements to metadata file")
	eventTriggers := GetEventTriggers(connectionPool)
	objectCounts["Event Triggers"] = len(eventTriggers)
	eventTriggerMetadata := GetMetadataForObjectType(connectionPool, TYPE_EVENTTRIGGER)
	PrintCreateEventTriggerStatements(metadataFile, globalTOC, eventTriggers, eventTriggerMetadata)
}

func backupRowLevelSecurityPolicies(metadataFile *utils.FileWithByteCount) {
	gplog.Verbose("Writing CREATE POLICY statements to metadata file")
	policies := GetPolicies(connectionPool)
	objectCounts["Policies"] = len(policies)
	ruleMetadata := GetCommentsForObjectType(connectionPool, TYPE_RULE)
	PrintCreatePolicyStatements(metadataFile, globalTOC, policies, ruleMetadata)
}

func backupDefaultPrivileges(metadataFile *utils.FileWithByteCount) {
	gplog.Verbose("Writing ALTER DEFAULT PRIVILEGES statements to metadata file")
	defaultPrivileges := GetDefaultPrivileges(connectionPool)
	objectCounts["DEFAULT PRIVILEGES"] = len(defaultPrivileges)
	PrintDefaultPrivilegesStatements(metadataFile, globalTOC, defaultPrivileges)
}

func backupExtendedStatistic(metadataFile *utils.FileWithByteCount) {
	gplog.Verbose("Writing CREATE STATISTICS statements to metadata file (for extended statistics)")
	statisticsExt := GetExtendedStatistics(connectionPool)
	objectCounts["STATISTICS EXT"] = len(statisticsExt)
	statisticExtMetadata := GetMetadataForObjectType(connectionPool, TYPE_STATISTIC_EXT)
	PrintCreateExtendedStatistics(metadataFile, globalTOC, statisticsExt, statisticExtMetadata)
}

/*
 * Data wrapper functions
 */

func backupTableStatistics(statisticsFile *utils.FileWithByteCount, tables []Table) {
	attStats := GetAttributeStatistics(connectionPool, tables)
	tupleStats := GetTupleStatistics(connectionPool, tables)

	backupSessionGUC(statisticsFile)
	PrintStatisticsStatements(statisticsFile, globalTOC, tables, attStats, tupleStats)
}

func backupIncrementalMetadata() {
	aoTableEntries := GetAOIncrementalMetadata(connectionPool)
	globalTOC.IncrementalMetadata.AO = aoTableEntries
}
