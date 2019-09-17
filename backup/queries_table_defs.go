package backup

/*
 * This file contains structs and functions related to executing specific
 * queries to gather metadata for the objects handled in predata_relations.go.
 */

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpbackup/utils"
)

type Table struct {
	Relation
	TableDefinition
}

func (t Table) SkipDataBackup() bool {
	def := t.TableDefinition
	return def.IsExternal || (def.ForeignDef != ForeignTableDefinition{})
}

func (t Table) GetMetadataEntry() (string, utils.MetadataEntry) {
	objectType := "TABLE"
	if (t.ForeignDef != ForeignTableDefinition{}) {
		objectType = "FOREIGN TABLE"
	}
	return "predata",
		utils.MetadataEntry{
			Schema:          t.Schema,
			Name:            t.Name,
			ObjectType:      objectType,
			ReferenceObject: "",
			StartByte:       0,
			EndByte:         0,
		}
}

type TableDefinition struct {
	DistPolicy         string
	PartDef            string
	PartTemplateDef    string
	StorageOpts        string
	TablespaceName     string
	ColumnDefs         []ColumnDefinition
	IsExternal         bool
	ExtTableDef        ExternalTableDefinition
	PartitionLevelInfo PartitionLevelInfo
	TableType          string
	IsUnlogged         bool
	ForeignDef         ForeignTableDefinition
	Inherits           []string
	ReplicaIdentity    string
}

/*
 * This function calls all the functions needed to gather the metadata for a
 * single table and assembles the metadata into ColumnDef and TableDef structs
 * for more convenient handling in the PrintCreateTableStatement() function.
 */
func ConstructDefinitionsForTables(connectionPool *dbconn.DBConn, tableRelations []Relation) []Table {
	tables := make([]Table, 0)

	gplog.Info("Gathering additional table metadata")
	gplog.Verbose("Retrieving column information")
	columnMetadata := GetPrivilegesForColumns(connectionPool)
	columnDefs := GetColumnDefinitions(connectionPool, columnMetadata)
	distributionPolicies := GetDistributionPolicies(connectionPool)
	gplog.Verbose("Retrieving partition information")
	partitionDefs := GetPartitionDefinitions(connectionPool)
	partTemplateDefs := GetPartitionTemplates(connectionPool)
	gplog.Verbose("Retrieving storage information")
	tableStorageOptions := GetTableStorageOptions(connectionPool)
	tablespaceNames := GetTablespaceNames(connectionPool)
	gplog.Verbose("Retrieving external table information")
	extTableDefs := GetExternalTableDefinitions(connectionPool)
	partTableMap := GetPartitionTableMap(connectionPool)
	tableTypeMap := GetTableType(connectionPool)
	unloggedTableMap := GetUnloggedTables(connectionPool)
	foreignTableDefs := GetForeignTableDefinitions(connectionPool)
	inheritanceMap := GetTableInheritance(connectionPool, tableRelations)
	replicaIdentityMap := GetTableReplicaIdentity(connectionPool)

	gplog.Verbose("Constructing table definition map")
	for _, tableRel := range tableRelations {
		oid := tableRel.Oid
		tableDef := TableDefinition{
			DistPolicy:         distributionPolicies[oid],
			PartDef:            partitionDefs[oid],
			PartTemplateDef:    partTemplateDefs[oid],
			StorageOpts:        tableStorageOptions[oid],
			TablespaceName:     tablespaceNames[oid],
			ColumnDefs:         columnDefs[oid],
			IsExternal:         (extTableDefs[oid].Oid != 0),
			ExtTableDef:        extTableDefs[oid],
			PartitionLevelInfo: partTableMap[oid],
			TableType:          tableTypeMap[oid],
			IsUnlogged:         unloggedTableMap[oid],
			ForeignDef:         foreignTableDefs[oid],
			Inherits:           inheritanceMap[oid],
			ReplicaIdentity:    replicaIdentityMap[oid],
		}
		if tableDef.Inherits == nil {
			tableDef.Inherits = []string{}
		}
		tables = append(tables, Table{tableRel, tableDef})
	}
	return tables
}

/*
 * This returns a map of all parent partition tables and leaf partition tables;
 * "p" indicates a parent table, "l" indicates a leaf table, and "i" indicates
 * an intermediate table.
 */

type PartitionLevelInfo struct {
	Oid      uint32
	Level    string
	RootName string
}

func GetPartitionTableMap(connectionPool *dbconn.DBConn) map[uint32]PartitionLevelInfo {
	query := `
SELECT
	pc.oid AS oid,
	'p' AS level,
	'' AS rootname
FROM pg_partition p
JOIN pg_class pc
	ON p.parrelid = pc.oid
UNION
SELECT
	r.parchildrelid AS oid,
	CASE WHEN p.parlevel = levels.pl THEN 'l' ELSE 'i' END AS level,
	quote_ident(cparent.relname) AS rootname
FROM pg_partition p
JOIN pg_partition_rule r
	ON p.oid = r.paroid
JOIN pg_class cparent
	ON cparent.oid = p.parrelid
JOIN (
	SELECT
		parrelid AS relid,
		max(parlevel) AS pl
	FROM pg_partition
	GROUP BY parrelid
) AS levels
	ON p.parrelid = levels.relid
WHERE r.parchildrelid != 0;
`
	results := make([]PartitionLevelInfo, 0)
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)

	resultMap := make(map[uint32]PartitionLevelInfo)
	for _, result := range results {
		resultMap[result.Oid] = result
	}

	return resultMap
}

type ColumnDefinition struct {
	Oid                   uint32 `db:"attrelid"`
	Num                   int    `db:"attnum"`
	Name                  string
	NotNull               bool `db:"attnotnull"`
	HasDefault            bool `db:"atthasdef"`
	Type                  string
	Encoding              string
	StatTarget            int `db:"attstattarget"`
	StorageType           string
	DefaultVal            string
	Comment               string
	Options               string
	FdwOptions            string
	Collation             string
	SecurityLabelProvider string
	SecurityLabel         string
	ACL                   []ACL
}

var storageTypeCodes = map[string]string{
	"e": "EXTERNAL",
	"m": "MAIN",
	"p": "PLAIN",
	"x": "EXTENDED",
}

func GetColumnDefinitions(connectionPool *dbconn.DBConn, columnMetadata map[uint32]map[string][]ACL) map[uint32][]ColumnDefinition {
	// This query is adapted from the getTableAttrs() function in pg_dump.c.
	// Optimize Get column definitions to avoid child partitions
	// Include child partitions that are also external tables
	gplog.Verbose("Getting column definitions")
	results := make([]ColumnDefinition, 0)
	selectClause := `
    SELECT
		a.attrelid,
		a.attnum,
		quote_ident(a.attname) AS name,
		a.attnotnull,
		a.atthasdef,
		pg_catalog.format_type(t.oid,a.atttypmod) AS type,
		COALESCE(pg_catalog.array_to_string(e.attoptions, ','), '') AS encoding,
		a.attstattarget,
		CASE WHEN a.attstorage != t.typstorage THEN a.attstorage ELSE '' END AS storagetype,
		COALESCE(pg_catalog.pg_get_expr(ad.adbin, ad.adrelid), '') AS defaultval,
		COALESCE(d.description, '') AS comment`
	fromClause := `
	FROM pg_catalog.pg_attribute a
		JOIN pg_class c ON a.attrelid = c.oid
		JOIN pg_namespace n ON c.relnamespace = n.oid
		LEFT JOIN pg_catalog.pg_attrdef ad ON a.attrelid = ad.adrelid AND a.attnum = ad.adnum
		LEFT JOIN pg_catalog.pg_type t ON a.atttypid = t.oid
		LEFT JOIN pg_catalog.pg_attribute_encoding e ON e.attrelid = a.attrelid AND e.attnum = a.attnum
		LEFT JOIN pg_description d ON d.objoid = a.attrelid AND d.classoid = 'pg_class'::regclass AND d.objsubid = a.attnum`
	whereClause := `
	WHERE ` + relationAndSchemaFilterClause() + `
		AND NOT EXISTS (SELECT 1 FROM (SELECT parchildrelid FROM pg_partition_rule EXCEPT SELECT reloid FROM pg_exttable) par WHERE par.parchildrelid = c.oid)
		AND c.reltype <> 0
		AND a.attnum > 0::pg_catalog.int2
		AND a.attisdropped = 'f'
	ORDER BY a.attrelid, a.attnum;`

	if connectionPool.Version.AtLeast("6") {
		selectClause += `,
		COALESCE(pg_catalog.array_to_string(a.attoptions, ','), '') AS options,
		COALESCE(array_to_string(ARRAY(SELECT option_name || ' ' || quote_literal(option_value) FROM pg_options_to_table(attfdwoptions) ORDER BY option_name), ', '), '') AS fdwoptions,
		CASE WHEN a.attcollation <> t.typcollation THEN quote_ident(cn.nspname) || '.' || quote_ident(coll.collname) ELSE '' END AS collation,
		COALESCE(sec.provider,'') AS securitylabelprovider,
		COALESCE(sec.label,'') AS securitylabel`
		fromClause += `
		LEFT JOIN pg_collation coll ON a.attcollation = coll.oid
		LEFT JOIN pg_namespace cn ON coll.collnamespace = cn.oid
		LEFT JOIN pg_seclabel sec ON sec.objoid = a.attrelid AND sec.classoid = 'pg_class'::regclass AND sec.objsubid = a.attnum`
	}

	query := fmt.Sprintf(`%s %s %s`, selectClause, fromClause, whereClause)
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)
	resultMap := make(map[uint32][]ColumnDefinition)
	for _, result := range results {
		result.StorageType = storageTypeCodes[result.StorageType]
		if connectionPool.Version.Before("6") {
			result.ACL = []ACL{}
		} else {
			result.ACL = columnMetadata[result.Oid][result.Name]
		}
		resultMap[result.Oid] = append(resultMap[result.Oid], result)
	}
	return resultMap
}

type ColumnPrivilegesQueryStruct struct {
	TableOid   uint32
	Name       string
	Privileges sql.NullString
	Kind       string
}

func GetPrivilegesForColumns(connectionPool *dbconn.DBConn) map[uint32]map[string][]ACL {
	gplog.Verbose("Getting column privileges")
	metadataMap := make(map[uint32]map[string][]ACL)
	if connectionPool.Version.Before("6") {
		return metadataMap
	}
	query := fmt.Sprintf(`
SELECT
    a.attrelid AS tableoid,
    quote_ident(a.attname) AS name,
	CASE
		WHEN a.attacl IS NULL OR array_upper(a.attacl, 1) = 0 THEN a.attacl[0]
		ELSE unnest(a.attacl)
	END AS privileges,
	CASE
		WHEN a.attacl IS NULL THEN 'Default'
		WHEN array_upper(a.attacl, 1) = 0 THEN 'Empty'
		ELSE ''
	END AS kind
FROM pg_attribute a
JOIN pg_class c ON a.attrelid = c.oid
JOIN pg_namespace n ON c.relnamespace = n.oid
WHERE %s
AND a.attnum > 0::pg_catalog.int2
AND a.attisdropped = 'f'
ORDER BY a.attrelid, a.attname;
`, relationAndSchemaFilterClause())

	results := make([]ColumnPrivilegesQueryStruct, 0)
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)
	metadataMap = ConstructColumnPrivilegesMap(results)

	return metadataMap
}

func GetDistributionPolicies(connectionPool *dbconn.DBConn) map[uint32]string {
	gplog.Verbose("Getting distribution policies")
	var query string
	if connectionPool.Version.Before("6") {
		// This query is adapted from the addDistributedBy() function in pg_dump.c.
		query = `
		SELECT
			p.localoid as oid,
			'DISTRIBUTED BY (' || string_agg(quote_ident(a.attname) , ', ' order by index) || ')' AS value	
		FROM
			(select localoid,
				unnest(attrnums) AS attnum,
				generate_series(1,array_upper(attrnums,1)) AS index
			FROM gp_distribution_policy WHERE attrnums is NOT NULL
			) p
		JOIN pg_attribute a ON (p.localoid,p.attnum) = (a.attrelid,a.attnum)
		GROUP BY localoid
		UNION ALL
		SELECT p.localoid as oid,
			'DISTRIBUTED RANDOMLY' AS value
		FROM gp_distribution_policy p WHERE attrnums is NULL;`
	} else {
		query = `
		SELECT
			localoid as oid,
			pg_catalog.pg_get_table_distributedby(localoid) as value
		FROM
			gp_distribution_policy;`
	}

	resultMap := selectAsOidToStringMap(connectionPool, query)

	return resultMap
}

func GetTableType(connectionPool *dbconn.DBConn) map[uint32]string {
	if connectionPool.Version.Before("6") {
		return map[uint32]string{}
	}
	query := `select oid, reloftype::pg_catalog.regtype AS value from pg_class WHERE reloftype != 0`
	return selectAsOidToStringMap(connectionPool, query)
}

func GetTableReplicaIdentity(connectionPool *dbconn.DBConn) map[uint32]string {
	if connectionPool.Version.Before("6") {
		return map[uint32]string{}
	}
	query := `select oid, relreplident AS value from pg_class`
	return selectAsOidToStringMap(connectionPool, query)
}

func GetPartitionDefinitions(connectionPool *dbconn.DBConn) map[uint32]string {
	gplog.Info("Getting partition defintions")
	query := fmt.Sprintf(`SELECT p.parrelid AS oid, pg_get_partition_def(p.parrelid, true, true) AS value FROM pg_partition p
	JOIN pg_class c ON p.parrelid = c.oid
	JOIN pg_namespace n ON c.relnamespace = n.oid
	WHERE %s`, relationAndSchemaFilterClause())
	return selectAsOidToStringMap(connectionPool, query)
}

func GetPartitionTemplates(connectionPool *dbconn.DBConn) map[uint32]string {
	gplog.Info("Getting partition templates")
	query := fmt.Sprintf(`SELECT p.parrelid AS oid, pg_get_partition_template_def(p.parrelid, true, true) AS value FROM pg_partition p
	JOIN pg_class c ON p.parrelid = c.oid
	JOIN pg_namespace n ON c.relnamespace = n.oid
	WHERE %s`, relationAndSchemaFilterClause())
	return selectAsOidToStringMap(connectionPool, query)
}

func GetTableStorageOptions(connectionPool *dbconn.DBConn) map[uint32]string {
	query := `SELECT oid, array_to_string(reloptions, ', ') AS value FROM pg_class WHERE reloptions IS NOT NULL;`
	return selectAsOidToStringMap(connectionPool, query)
}

func GetTablespaceNames(connectionPool *dbconn.DBConn) map[uint32]string {
	query := `SELECT c.oid, quote_ident(t.spcname) AS value FROM pg_class c JOIN pg_tablespace t ON t.oid = c.reltablespace`
	return selectAsOidToStringMap(connectionPool, query)
}

func GetUnloggedTables(connectionPool *dbconn.DBConn) map[uint32]bool {
	if connectionPool.Version.Before("6") {
		return map[uint32]bool{}
	}
	query := `SELECT oid FROM pg_class WHERE relpersistence = 'u'`
	var results []struct {
		Oid uint32
	}
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)
	resultMap := make(map[uint32]bool)
	for _, result := range results {
		resultMap[result.Oid] = true
	}
	return resultMap
}

type ForeignTableDefinition struct {
	Oid     uint32 `db:"ftrelid"`
	Options string `db:"ftoptions"`
	Server  string `db:"ftserver"`
}

func GetForeignTableDefinitions(connectionPool *dbconn.DBConn) map[uint32]ForeignTableDefinition {
	if connectionPool.Version.Before("6") {
		return map[uint32]ForeignTableDefinition{}
	}
	queryResults := make([]ForeignTableDefinition, 0)

	query := `SELECT ftrelid, fs.srvname as ftserver,
              pg_catalog.array_to_string(array
       (
                select pg_catalog.quote_ident(option_name) ||' ' || pg_catalog.quote_literal(option_value)
                FROM pg_catalog.pg_options_to_table(ftoptions)
                ORDER BY option_name), e',    ')
              AS ftoptions
FROM pg_foreign_table ft JOIN pg_foreign_server fs on ft.ftserver = fs.oid;
`
	err := connectionPool.Select(&queryResults, query)
	gplog.FatalOnError(err)

	resultMap := make(map[uint32]ForeignTableDefinition, len(queryResults))
	for _, foreignTableDef := range queryResults {
		resultMap[foreignTableDef.Oid] = foreignTableDef
	}

	return resultMap
}

type Dependency struct {
	Oid              uint32
	ReferencedObject string
}

func GetTableInheritance(connectionPool *dbconn.DBConn, tables []Relation) map[uint32][]string {
	tableFilterStr := ""
	if len(MustGetFlagStringArray(utils.INCLUDE_RELATION)) > 0 {
		tableOidList := make([]string, len(tables))
		for i, table := range tables {
			tableOidList[i] = fmt.Sprintf("%d", table.Oid)
		}
		// If we are filtering on tables, we only want to record dependencies on other tables in the list
		if len(tableOidList) > 0 {
			tableFilterStr = fmt.Sprintf("\nAND i.inhrelid IN (%s)", strings.Join(tableOidList, ","))
		}
	}

	query := fmt.Sprintf(`
SELECT
	i.inhrelid AS oid,
	quote_ident(n.nspname) || '.' || quote_ident(p.relname) AS referencedobject
FROM pg_inherits i
JOIN pg_class p ON i.inhparent = p.oid
JOIN pg_namespace n ON p.relnamespace = n.oid
WHERE %s%s
ORDER BY i.inhrelid, i.inhseqno`, ExtensionFilterClause("p"), tableFilterStr)

	results := make([]Dependency, 0)
	inheritanceMap := make(map[uint32][]string)
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)
	for _, dependency := range results {
		inheritanceMap[dependency.Oid] = append(inheritanceMap[dependency.Oid], dependency.ReferencedObject)
	}
	return inheritanceMap
}

func selectAsOidToStringMap(connectionPool *dbconn.DBConn, query string) map[uint32]string {
	var results []struct {
		Oid   uint32
		Value string
	}
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)
	resultMap := make(map[uint32]string)
	for _, result := range results {
		resultMap[result.Oid] = result.Value
	}
	return resultMap
}
