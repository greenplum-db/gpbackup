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

func relationAndSchemaFilterClause() string {
	filterClause := SchemaFilterClause("n")
	if len(MustGetFlagStringSlice(utils.EXCLUDE_RELATION)) > 0 {
		excludeOids := GetOidsFromRelationList(connectionPool, MustGetFlagStringSlice(utils.EXCLUDE_RELATION))
		filterClause += fmt.Sprintf("\nAND c.oid NOT IN (%s)", strings.Join(excludeOids, ", "))
	}
	if len(MustGetFlagStringSlice(utils.INCLUDE_RELATION)) > 0 {
		includeOids := GetOidsFromRelationList(connectionPool, MustGetFlagStringSlice(utils.INCLUDE_RELATION))
		filterClause += fmt.Sprintf("\nAND c.oid IN (%s)", strings.Join(includeOids, ", "))
	}
	return filterClause
}

func GetOidsFromRelationList(connection *dbconn.DBConn, relationNames []string) []string {
	relList := utils.SliceToQuotedString(relationNames)
	query := fmt.Sprintf(`
SELECT
	c.oid AS string
FROM pg_class c
JOIN pg_namespace n ON c.relnamespace = n.oid
WHERE quote_ident(n.nspname) || '.' || quote_ident(c.relname) IN (%s)`, relList)
	return dbconn.MustSelectStringSlice(connection, query)
}

func GetAllUserTables(connection *dbconn.DBConn) []Relation {
	if len(MustGetFlagStringSlice(utils.INCLUDE_RELATION)) > 0 {
		return GetUserTablesWithIncludeFiltering(connection)
	}
	return GetUserTables(connection)
}

type Relation struct {
	SchemaOid uint32
	Oid       uint32
	Schema    string
	Name      string
	Inherits  []string // Only used for printing INHERITS statement
}

func (r Relation) FQN() string {
	return utils.MakeFQN(r.Schema, r.Name)
}

func (r Relation) GetDepEntry() DepEntry {
	return DepEntry{Classid: 1259, Objid: r.Oid}
}

/*
 * This function also handles exclude table filtering since the way we do
 * it is currently much simpler than the include case.
 */
func GetUserTables(connection *dbconn.DBConn) []Relation {
	childPartitionFilter := ""
	if !MustGetFlagBool(utils.LEAF_PARTITION_DATA) {
		//Filter out non-external child partitions
		childPartitionFilter = `
	AND c.oid NOT IN (
		SELECT
			p.parchildrelid
		FROM pg_partition_rule p
		LEFT JOIN pg_exttable e
			ON p.parchildrelid = e.reloid
		WHERE e.reloid IS NULL)`
	}

	query := fmt.Sprintf(`
SELECT
	n.oid AS schemaoid,
	c.oid AS oid,
	quote_ident(n.nspname) AS schema,
	quote_ident(c.relname) AS name
FROM pg_class c
JOIN pg_namespace n
	ON c.relnamespace = n.oid
WHERE %s
%s
AND relkind = 'r'
AND %s
ORDER BY c.oid;`, relationAndSchemaFilterClause(), childPartitionFilter, ExtensionFilterClause("c"))

	results := make([]Relation, 0)
	err := connection.Select(&results, query)
	gplog.FatalOnError(err)

	return results
}

func GetUserTablesWithIncludeFiltering(connection *dbconn.DBConn) []Relation {
	includeOids := GetOidsFromRelationList(connection, MustGetFlagStringSlice(utils.INCLUDE_RELATION))
	oidStr := strings.Join(includeOids, ", ")
	childPartitionFilter := ""
	if MustGetFlagBool(utils.LEAF_PARTITION_DATA) {
		//Get all leaf partition tables whose parents are in the include list
		childPartitionFilter = fmt.Sprintf(`
	OR c.oid IN (
		SELECT
			r.parchildrelid
		FROM pg_partition p
		JOIN pg_partition_rule r ON p.oid = r.paroid
		WHERE p.paristemplate = false
		AND p.parrelid IN (%s))`, oidStr)
	} else {
		//Get only external partition tables whose parents are in the include list
		childPartitionFilter = fmt.Sprintf(`
	OR c.oid IN (
		SELECT
			r.parchildrelid
		FROM pg_partition p
		JOIN pg_partition_rule r ON p.oid = r.paroid
		JOIN pg_exttable e ON r.parchildrelid = e.reloid
		WHERE p.paristemplate = false
		AND e.reloid IS NOT NULL
		AND p.parrelid IN (%s))`, oidStr)
	}

	query := fmt.Sprintf(`
SELECT
	n.oid AS schemaoid,
	c.oid AS oid,
	quote_ident(n.nspname) AS schema,
	quote_ident(c.relname) AS name
FROM pg_class c
JOIN pg_namespace n
	ON c.relnamespace = n.oid
WHERE %s
AND (
	-- Get tables in the include list
	c.oid IN (%s)
	-- Get parent partition tables whose children are in the include list
	OR c.oid IN (
		SELECT
			p.parrelid
		FROM pg_partition p
		JOIN pg_partition_rule r ON p.oid = r.paroid
		WHERE p.paristemplate = false
		AND r.parchildrelid IN (%s)
	)
	-- Get external partition tables whose siblings are in the include list
	OR c.oid IN (
		SELECT
			r.parchildrelid
		FROM pg_partition_rule r
		JOIN pg_exttable e ON r.parchildrelid = e.reloid
		WHERE r.paroid IN (
			SELECT
				pr.paroid
			FROM pg_partition_rule pr
			WHERE pr.parchildrelid IN (%s)
		)
	)
	%s
)
AND (relkind = 'r')
AND %s
ORDER BY c.oid;`, SchemaFilterClause("n"), oidStr, oidStr, oidStr, childPartitionFilter, ExtensionFilterClause("c"))

	results := make([]Relation, 0)
	err := connection.Select(&results, query)
	gplog.FatalOnError(err)
	return results
}

func GetForeignTableRelations(connection *dbconn.DBConn) []Relation {
	query := fmt.Sprintf(`
SELECT
	n.oid AS schemaoid,
	c.oid AS oid,
	quote_ident(n.nspname) AS schema,
	quote_ident(c.relname) AS name
FROM pg_class c
JOIN pg_namespace n
	ON c.relnamespace = n.oid
WHERE %s
AND relkind = 'f'
AND %s
ORDER BY c.oid;`, relationAndSchemaFilterClause(), ExtensionFilterClause("c"))

	results := make([]Relation, 0)
	err := connection.Select(&results, query)
	gplog.FatalOnError(err)
	return results
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

func GetPartitionTableMap(connection *dbconn.DBConn) map[uint32]PartitionLevelInfo {
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
	err := connection.Select(&results, query)
	gplog.FatalOnError(err)

	resultMap := make(map[uint32]PartitionLevelInfo, 0)
	for _, result := range results {
		resultMap[result.Oid] = result
	}

	return resultMap
}

type ColumnDefinition struct {
	Oid         uint32 `db:"attrelid"`
	Num         int    `db:"attnum"`
	Name        string
	NotNull     bool `db:"attnotnull"`
	HasDefault  bool `db:"atthasdef"`
	Type        string
	Encoding    string
	StatTarget  int `db:"attstattarget"`
	StorageType string
	DefaultVal  string
	Comment     string
	ACL         []ACL
	Options     string
	FdwOptions  string
	Collation   string
}

var storageTypeCodes = map[string]string{
	"e": "EXTERNAL",
	"m": "MAIN",
	"p": "PLAIN",
	"x": "EXTENDED",
}

func GetColumnDefinitions(connection *dbconn.DBConn, columnMetadata map[uint32]map[string][]ACL) map[uint32][]ColumnDefinition {
	// This query is adapted from the getTableAttrs() function in pg_dump.c.
	results := make([]ColumnDefinition, 0)
	version4query := fmt.Sprintf(`
SELECT
	a.attrelid,
	a.attnum,
	quote_ident(a.attname) AS name,
	a.attnotnull,
	a.atthasdef,
	pg_catalog.format_type(t.oid,a.atttypmod) AS type,
	coalesce(pg_catalog.array_to_string(e.attoptions, ','), '') AS encoding,
	a.attstattarget,
	CASE WHEN a.attstorage != t.typstorage THEN a.attstorage ELSE '' END AS storagetype,
	coalesce(pg_catalog.pg_get_expr(ad.adbin, ad.adrelid), '') AS defaultval,
	coalesce(d.description,'') AS comment
FROM pg_catalog.pg_attribute a
JOIN pg_class c ON a.attrelid = c.oid
JOIN pg_namespace n ON c.relnamespace = n.oid
LEFT JOIN pg_catalog.pg_attrdef ad ON (a.attrelid = ad.adrelid AND a.attnum = ad.adnum)
LEFT JOIN pg_catalog.pg_type t ON a.atttypid = t.oid
LEFT JOIN pg_catalog.pg_attribute_encoding e ON e.attrelid = a.attrelid AND e.attnum = a.attnum
LEFT JOIN pg_description d ON (d.objoid = a.attrelid AND d.classoid = 'pg_class'::regclass AND d.objsubid = a.attnum)
WHERE %s
AND a.attnum > 0::pg_catalog.int2
AND a.attisdropped = 'f'
ORDER BY a.attrelid, a.attnum;`, relationAndSchemaFilterClause())

	masterQuery := fmt.Sprintf(`
SELECT
	a.attrelid,
	a.attnum,
	quote_ident(a.attname) AS name,
	a.attnotnull,
	a.atthasdef,
	pg_catalog.format_type(t.oid,a.atttypmod) AS type,
	coalesce(pg_catalog.array_to_string(e.attoptions, ','), '') AS encoding,
	a.attstattarget,
	CASE WHEN a.attstorage != t.typstorage THEN a.attstorage ELSE '' END AS storagetype,
	coalesce(pg_catalog.pg_get_expr(ad.adbin, ad.adrelid), '') AS defaultval,
	coalesce(pg_catalog.array_to_string(a.attoptions, ','), '') AS options,
	coalesce(array_to_string(ARRAY(SELECT option_name || ' ' || quote_literal(option_value) FROM pg_options_to_table(attfdwoptions) ORDER BY option_name), ', '), '') AS fdwoptions,
	CASE WHEN a.attcollation <> t.typcollation THEN quote_ident(cn.nspname) || '.' || quote_ident(coll.collname) ELSE '' END AS collation,
	coalesce(d.description,'') AS comment
FROM pg_catalog.pg_attribute a
JOIN pg_class c ON a.attrelid = c.oid
JOIN pg_namespace n ON c.relnamespace = n.oid
LEFT JOIN pg_catalog.pg_attrdef ad ON (a.attrelid = ad.adrelid AND a.attnum = ad.adnum)
LEFT JOIN pg_catalog.pg_type t ON a.atttypid = t.oid
LEFT JOIN pg_collation coll on (a.attcollation = coll.oid)
LEFT JOIN pg_namespace cn on (coll.collnamespace = cn.oid)
LEFT JOIN pg_catalog.pg_attribute_encoding e ON e.attrelid = a.attrelid AND e.attnum = a.attnum
LEFT JOIN pg_description d ON (d.objoid = a.attrelid AND d.classoid = 'pg_class'::regclass AND d.objsubid = a.attnum)
WHERE %s
AND a.attnum > 0::pg_catalog.int2
AND a.attisdropped = 'f'
ORDER BY a.attrelid, a.attnum;`, relationAndSchemaFilterClause())

	var err error
	if connection.Version.Before("6") {
		err = connection.Select(&results, version4query)
	} else {
		err = connection.Select(&results, masterQuery)
	}

	gplog.FatalOnError(err)
	resultMap := make(map[uint32][]ColumnDefinition, 0)
	for _, result := range results {
		result.StorageType = storageTypeCodes[result.StorageType]
		if connection.Version.Before("6") {
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

func GetPrivilegesForColumns(connection *dbconn.DBConn) map[uint32]map[string][]ACL {
	metadataMap := make(map[uint32]map[string][]ACL)
	if connection.Version.Before("6") {
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
	err := connection.Select(&results, query)
	gplog.FatalOnError(err)
	metadataMap = ConstructColumnPrivilegesMap(results)

	return metadataMap
}

type DistributionPolicy struct {
	Oid    uint32
	Policy string
}

func SelectAsOidToStringMap(connection *dbconn.DBConn, query string) map[uint32]string {
	var results []struct {
		Oid   uint32
		Value string
	}
	err := connection.Select(&results, query)
	gplog.FatalOnError(err)
	resultMap := make(map[uint32]string, 0)
	for _, result := range results {
		resultMap[result.Oid] = result.Value
	}
	return resultMap
}

func SelectAsOidToBoolMap(connection *dbconn.DBConn, query string) map[uint32]bool {
	var results []struct {
		Oid uint32
	}
	err := connection.Select(&results, query)
	gplog.FatalOnError(err)
	resultMap := make(map[uint32]bool, 0)
	for _, result := range results {
		resultMap[result.Oid] = true
	}
	return resultMap
}

func GetDistributionPolicies(connection *dbconn.DBConn) map[uint32]string {
	// This query is adapted from the addDistributedBy() function in pg_dump.c.
	var query string
	if connection.Version.Before("6") {
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
			p.localoid as oid,
			CASE WHEN p.policytype = 'r' THEN 'DISTRIBUTED REPLICATED'
				 WHEN count(p.attnum) = 0 THEN 'DISTRIBUTED RANDOMLY'
				 ELSE 'DISTRIBUTED BY (' || array_to_string(array_agg(quote_ident(a.attname) order by index), ', ') || ')'
			END AS value	
		FROM
			(
			 SELECT
			 	localoid,
			 	policytype,
				attnum,
				row_number() over () as index
			 FROM
			 (select localoid, policytype,
			  	CASE WHEN attrnums is NULL THEN NULL
			  	ELSE unnest(attrnums)
				END AS attnum
			  FROM gp_distribution_policy) x
			 ) as p
			LEFT JOIN
			pg_attribute a
			ON (p.localoid,p.attnum) = (a.attrelid,a.attnum)
			GROUP BY localoid, policytype;`
	}

	resultMap := SelectAsOidToStringMap(connection, query)

	return resultMap
}

func GetTableType(connection *dbconn.DBConn) map[uint32]string {
	if connection.Version.Before("6") {
		return map[uint32]string{}
	}
	query := `select oid, reloftype::pg_catalog.regtype AS value from pg_class WHERE reloftype != 0`
	return SelectAsOidToStringMap(connection, query)
}

func GetPartitionDefinitions(connection *dbconn.DBConn) map[uint32]string {
	query := fmt.Sprintf(`SELECT p.parrelid AS oid, pg_get_partition_def(p.parrelid, true, true) AS value FROM pg_partition p
	JOIN pg_class c ON p.parrelid = c.oid
	JOIN pg_namespace n ON c.relnamespace = n.oid
	WHERE %s`, relationAndSchemaFilterClause())
	return SelectAsOidToStringMap(connection, query)
}

func GetPartitionTemplates(connection *dbconn.DBConn) map[uint32]string {
	query := fmt.Sprintf(`SELECT p.parrelid AS oid, pg_get_partition_template_def(p.parrelid, true, true) AS value FROM pg_partition p
	JOIN pg_class c ON p.parrelid = c.oid
	JOIN pg_namespace n ON c.relnamespace = n.oid
	WHERE %s`, relationAndSchemaFilterClause())
	return SelectAsOidToStringMap(connection, query)
}

func GetTableStorageOptions(connection *dbconn.DBConn) map[uint32]string {
	query := `SELECT oid, array_to_string(reloptions, ', ') AS value FROM pg_class WHERE reloptions IS NOT NULL;`
	return SelectAsOidToStringMap(connection, query)
}

func GetTablespaceNames(connection *dbconn.DBConn) map[uint32]string {
	query := `SELECT c.oid, quote_ident(t.spcname) AS value FROM pg_class c JOIN pg_tablespace t ON t.oid = c.reltablespace`
	return SelectAsOidToStringMap(connection, query)
}

func GetUnloggedTables(connection *dbconn.DBConn) map[uint32]bool {
	if connection.Version.Before("6") {
		return map[uint32]bool{}
	}
	query := `SELECT oid FROM pg_class WHERE relpersistence = 'u'`
	return SelectAsOidToBoolMap(connection, query)
}

func GetForeignTableDefinitions(connection *dbconn.DBConn) map[uint32]ForeignTableDefinition {
	if connection.Version.Before("6") {
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
	err := connection.Select(&queryResults, query)
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

func ConstructTableDependencies(connection *dbconn.DBConn, tables []Relation, tableDefs map[uint32]TableDefinition, isTableFiltered bool) []Relation {
	var tableNameSet *utils.FilterSet
	var tableOidList []string
	if isTableFiltered {
		tableNameSet = utils.NewIncludeSet([]string{})
		tableOidList = make([]string, len(tables))
		for i, table := range tables {
			tableNameSet.Add(table.FQN())
			tableOidList[i] = fmt.Sprintf("%d", table.Oid)
		}
	}
	typeQuery := fmt.Sprintf(`
SELECT
	objid AS oid,
	quote_ident(n.nspname) || '.' || quote_ident(p.typname) AS referencedobject,
	'f' AS istable
FROM pg_depend d
JOIN pg_type p ON d.refobjid = p.oid
JOIN pg_namespace n ON p.typnamespace = n.oid
JOIN pg_class c ON d.objid = c.oid AND c.relkind = 'r'
WHERE %s
AND %s`, SchemaFilterClause("n"), ExtensionFilterClause("p"))
	protocolQuery := fmt.Sprintf(`
SELECT
	objid AS oid,
	quote_ident(ptc.ptcname) AS referencedobject,
	'f' AS istable
FROM pg_depend d
JOIN pg_extprotocol ptc ON d.refobjid = ptc.oid
AND %s`, ExtensionFilterClause("ptc"))
	tableQuery := fmt.Sprintf(`
SELECT
	objid AS oid,
	quote_ident(n.nspname) || '.' || quote_ident(p.relname) AS referencedobject,
	't' AS istable
FROM pg_depend d
JOIN pg_class p ON d.refobjid = p.oid AND p.relkind = 'r'
JOIN pg_namespace n ON p.relnamespace = n.oid
JOIN pg_class c ON d.objid = c.oid AND c.relkind = 'r'
AND %s`, ExtensionFilterClause("p"))

	query := ""
	// If we are filtering on tables, we only want to record dependencies on other tables in the list
	if isTableFiltered && len(tableOidList) > 0 {
		query = fmt.Sprintf("%s\nWHERE objid IN (%s);", tableQuery, strings.Join(tableOidList, ","))
	} else {
		query = fmt.Sprintf("%s\nUNION\n%s\nUNION\n%s;", typeQuery, protocolQuery, tableQuery)
	}
	results := make([]struct {
		Oid              uint32
		ReferencedObject string
		IsTable          bool
	}, 0)
	dependencyMap := make(map[uint32][]string, 0)
	inheritanceMap := make(map[uint32][]string, 0)
	err := connection.Select(&results, query)
	gplog.FatalOnError(err)
	for _, dependency := range results {
		if tableDefs[dependency.Oid].IsExternal && tableDefs[dependency.Oid].PartitionLevelInfo.Level == "l" {
			continue
		}
		if dependency.IsTable {
			inheritanceMap[dependency.Oid] = append(inheritanceMap[dependency.Oid], dependency.ReferencedObject)
		}
		if isTableFiltered && !tableNameSet.MatchesFilter(dependency.ReferencedObject) {
			continue
		}
		dependencyMap[dependency.Oid] = append(dependencyMap[dependency.Oid], dependency.ReferencedObject)
	}
	for i := 0; i < len(tables); i++ {
		tables[i].Inherits = inheritanceMap[tables[i].Oid]
	}

	return tables
}

func GetAllSequenceRelations(connection *dbconn.DBConn) []Relation {
	query := fmt.Sprintf(`SELECT
	n.oid AS schemaoid,
	c.oid AS oid,
	quote_ident(n.nspname) AS schema,
	quote_ident(c.relname) AS name
FROM pg_class c
LEFT JOIN pg_namespace n
	ON c.relnamespace = n.oid
WHERE relkind = 'S'
AND %s
AND %s
ORDER BY n.nspname, c.relname;`, relationAndSchemaFilterClause(), ExtensionFilterClause("c"))

	results := make([]Relation, 0)
	err := connection.Select(&results, query)
	gplog.FatalOnError(err)

	return results
}

type SequenceDefinition struct {
	Name        string `db:"sequence_name"`
	LastVal     int64  `db:"last_value"`
	StartVal    int64  `db:"start_value"`
	Increment   int64  `db:"increment_by"`
	MaxVal      int64  `db:"max_value"`
	MinVal      int64  `db:"min_value"`
	CacheVal    int64  `db:"cache_value"`
	LogCnt      int64  `db:"log_cnt"`
	IsCycled    bool   `db:"is_cycled"`
	IsCalled    bool   `db:"is_called"`
	OwningTable string
}

func GetSequenceDefinition(connection *dbconn.DBConn, seqName string) SequenceDefinition {
	query := fmt.Sprintf("SELECT * FROM %s", seqName)
	result := SequenceDefinition{}
	err := connection.Get(&result, query)
	gplog.FatalOnError(err)
	return result
}

func GetSequenceColumnOwnerMap(connection *dbconn.DBConn) (map[string]string, map[string]string) {
	query := fmt.Sprintf(`SELECT
	quote_ident(n.nspname) AS schema,
	quote_ident(s.relname) AS name,
	quote_ident(c.relname) AS tablename,
	quote_ident(a.attname) AS columnname
FROM pg_depend d
JOIN pg_attribute a
	ON a.attrelid = d.refobjid AND a.attnum = d.refobjsubid
JOIN pg_class s
	ON s.oid = d.objid
JOIN pg_class c
	ON c.oid = d.refobjid
JOIN pg_namespace n
	ON n.oid = s.relnamespace
WHERE s.relkind = 'S'
AND %s;`, relationAndSchemaFilterClause())

	results := make([]struct {
		Schema     string
		Name       string
		TableName  string
		ColumnName string
	}, 0)
	sequenceOwnerTables := make(map[string]string, 0)
	sequenceOwnerColumns := make(map[string]string, 0)
	err := connection.Select(&results, query)
	gplog.FatalOnError(err)
	for _, seqOwner := range results {
		seqFQN := utils.MakeFQN(seqOwner.Schema, seqOwner.Name)
		tableFQN := fmt.Sprintf("%s.%s", seqOwner.Schema, seqOwner.TableName)
		columnFQN := fmt.Sprintf("%s.%s.%s", seqOwner.Schema, seqOwner.TableName, seqOwner.ColumnName)
		sequenceOwnerTables[seqFQN] = tableFQN
		sequenceOwnerColumns[seqFQN] = columnFQN
	}
	return sequenceOwnerTables, sequenceOwnerColumns
}

type View struct {
	Oid         uint32
	Schema      string
	Name        string
	Options     string
	Definition  string
	DependsUpon []string
}

func (v View) GetDepEntry() DepEntry {
	return DepEntry{Classid: 0, Objid: 0}
}

func (v View) FQN() string {
	return utils.MakeFQN(v.Schema, v.Name)
}

func GetViews(connection *dbconn.DBConn) []View {
	results := make([]View, 0)
	optionsStr := ""
	if connection.Version.AtLeast("6") {
		optionsStr = "coalesce(' WITH (' || array_to_string(c.reloptions, ', ') || ')', '') AS options,"
	}
	query := fmt.Sprintf(`
SELECT
	c.oid,
	quote_ident(n.nspname) AS schema,
	quote_ident(c.relname) AS name,
	%s
	pg_get_viewdef(c.oid) AS definition
FROM pg_class c
LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = 'v'::"char"
AND %s
AND %s;`, optionsStr, relationAndSchemaFilterClause(), ExtensionFilterClause("c"))
	err := connection.Select(&results, query)
	gplog.FatalOnError(err)
	return results
}

func ConstructViewDependencies(connection *dbconn.DBConn, views []View) []View {
	query := fmt.Sprintf(`
SELECT DISTINCT
	v2.oid,
	quote_ident(n.nspname) || '.' || quote_ident(v1.relname) AS referencedobject
FROM pg_class v1
JOIN pg_depend d ON d.refobjid = v1.oid
JOIN pg_rewrite rw ON rw.oid = d.objid
JOIN pg_class v2 ON rw.ev_class = v2.oid
JOIN pg_namespace n ON v1.relnamespace = n.oid
WHERE d.classid = 'pg_rewrite'::regclass::oid
	AND v1.oid != v2.oid
	AND v1.relkind = 'v'
	AND %s
	AND %s
ORDER BY v2.oid, referencedobject;`, SchemaFilterClause("n"), ExtensionFilterClause("v1"))

	results := make([]Dependency, 0)
	dependencyMap := make(map[uint32][]string, 0)
	err := connection.Select(&results, query)
	gplog.FatalOnError(err)
	for _, dependency := range results {
		dependencyMap[dependency.Oid] = append(dependencyMap[dependency.Oid], dependency.ReferencedObject)
	}
	for i := 0; i < len(views); i++ {
		views[i].DependsUpon = dependencyMap[views[i].Oid]
	}
	return views
}

func LockTables(connection *dbconn.DBConn, tables []Relation) {
	gplog.Info("Acquiring ACCESS SHARE locks on tables")
	progressBar := utils.NewProgressBar(len(tables), "Locks acquired: ", utils.PB_VERBOSE)
	progressBar.Start()
	for _, table := range tables {
		connection.MustExec(fmt.Sprintf("LOCK TABLE %s IN ACCESS SHARE MODE", table.FQN()))
		progressBar.Increment()
	}
	progressBar.Finish()
}
