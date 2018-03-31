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

func tableAndSchemaFilterClause() string {
	filterClause := SchemaFilterClause("n")
	if len(excludeTables) > 0 {
		excludeOids := GetOidsFromTableList(connection, excludeTables)
		filterClause += fmt.Sprintf("\nAND c.oid NOT IN (%s)", strings.Join(excludeOids, ", "))
	}
	if len(includeTables) > 0 {
		includeOids := GetOidsFromTableList(connection, includeTables)
		filterClause += fmt.Sprintf("\nAND c.oid IN (%s)", strings.Join(includeOids, ", "))
	}
	return filterClause
}

func GetOidsFromTableList(connection *dbconn.DBConn, tableNames []string) []string {
	tableList := utils.SliceToQuotedString(tableNames)
	query := fmt.Sprintf(`
SELECT
	c.oid AS string
FROM pg_class c
JOIN pg_namespace n ON c.relnamespace = n.oid
WHERE quote_ident(n.nspname) || '.' || quote_ident(c.relname) IN (%s)`, tableList)
	return dbconn.MustSelectStringSlice(connection, query)
}

func GetAllUserTables(connection *dbconn.DBConn) []Relation {
	if len(includeTables) > 0 {
		return GetUserTablesWithIncludeFiltering(connection)
	}
	return GetUserTables(connection)
}

/*
 * This function also handles exclude table filtering since the way we do
 * it is currently much simpler than the include case.
 */
func GetUserTables(connection *dbconn.DBConn) []Relation {
	childPartitionFilter := ""
	if !*leafPartitionData {
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
AND c.oid NOT IN (select objid from pg_depend where deptype = 'e')
ORDER BY c.oid;`, tableAndSchemaFilterClause(), childPartitionFilter)

	results := make([]Relation, 0)
	err := connection.Select(&results, query)
	gplog.FatalOnError(err)
	return results
}

func GetUserTablesWithIncludeFiltering(connection *dbconn.DBConn) []Relation {
	includeOids := GetOidsFromTableList(connection, includeTables)
	oidStr := strings.Join(includeOids, ", ")
	childPartitionFilter := ""
	if *leafPartitionData {
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
AND relkind = 'r'
AND c.oid NOT IN (select objid from pg_depend where deptype = 'e')
ORDER BY c.oid;`, SchemaFilterClause("n"), oidStr, oidStr, oidStr, childPartitionFilter)

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
func GetPartitionTableMap(connection *dbconn.DBConn) map[uint32]string {
	query := `
SELECT
	pc.oid AS oid,
	'p' AS value
FROM pg_partition p
JOIN pg_class pc
	ON p.parrelid = pc.oid
UNION
SELECT
	cc.oid AS oid,
	CASE WHEN p.parlevel = levels.pl THEN 'l' ELSE 'i' END AS value
FROM pg_partition p
JOIN pg_partition_rule r
	ON p.oid = r.paroid
JOIN pg_class cc
	ON r.parchildrelid = cc.oid
JOIN (
	SELECT
		parrelid AS relid,
		max(parlevel) AS pl
	FROM pg_partition
	GROUP BY parrelid
) AS levels
	ON p.parrelid = levels.relid;
`
	return SelectAsOidToStringMap(connection, query)
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
}

var storageTypeCodes = map[string]string{
	"e": "EXTERNAL",
	"m": "MAIN",
	"p": "PLAIN",
	"x": "EXTENDED",
}

func GetColumnDefinitions(connection *dbconn.DBConn, columnMetadata map[uint32]map[string][]ACL) map[uint32][]ColumnDefinition {
	// This query is adapted from the getTableAttrs() function in pg_dump.c.
	optionsQuery := ""
	if connection.Version.AtLeast("6") {
		optionsQuery = "coalesce(pg_catalog.array_to_string(a.attoptions, ','), '') AS options,"
	}
	query := fmt.Sprintf(`
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
	%s
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
ORDER BY a.attrelid, a.attnum;`, optionsQuery, tableAndSchemaFilterClause())

	results := make([]ColumnDefinition, 0)
	err := connection.Select(&results, query)
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
`, tableAndSchemaFilterClause())

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

func GetDistributionPolicies(connection *dbconn.DBConn, tables []Relation) map[uint32]string {
	// This query is adapted from the addDistributedBy() function in pg_dump.c.
	query := `
SELECT
	a.attrelid AS oid,
	'(' || array_to_string(array_agg(quote_ident(a.attname)), ', ') || ')' AS value
FROM pg_attribute a
JOIN (
	SELECT
		unnest(attrnums) AS attnum,
		localoid
	FROM gp_distribution_policy
) p
ON (p.localoid,p.attnum) = (a.attrelid,a.attnum)
GROUP BY a.attrelid ORDER BY a.attrelid;`

	resultMap := SelectAsOidToStringMap(connection, query)
	for _, table := range tables {
		if resultMap[table.Oid] != "" {
			resultMap[table.Oid] = fmt.Sprintf("DISTRIBUTED BY %s", resultMap[table.Oid])
		} else {
			resultMap[table.Oid] = "DISTRIBUTED RANDOMLY"
		}
	}
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
	query := `SELECT parrelid AS oid, pg_get_partition_def(parrelid, true, true) AS value FROM pg_partition`
	return SelectAsOidToStringMap(connection, query)
}

func GetPartitionTemplates(connection *dbconn.DBConn) map[uint32]string {
	query := fmt.Sprintf("SELECT parrelid AS oid, pg_get_partition_template_def(parrelid, true, true) AS value FROM pg_partition")
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

type Dependency struct {
	Oid              uint32
	ReferencedObject string
}

func ConstructTableDependencies(connection *dbconn.DBConn, tables []Relation, tableDefs map[uint32]TableDefinition, isTableFiltered bool) []Relation {
	var tableNameSet *utils.FilterSet
	var tableOidList []string
	if isTableFiltered {
		tableNameSet = utils.NewEmptyIncludeSet()
		tableOidList = make([]string, len(tables))
		for i, table := range tables {
			tableNameSet.Add(table.ToString())
			tableOidList[i] = fmt.Sprintf("%d", table.Oid)
		}
	}
	nonTableQuery := fmt.Sprintf(`
SELECT
	objid AS oid,
	quote_ident(n.nspname) || '.' || quote_ident(p.typname) AS referencedobject,
	'f' AS istable
FROM pg_depend d
JOIN pg_type p ON d.refobjid = p.oid
JOIN pg_namespace n ON p.typnamespace = n.oid
JOIN pg_class c ON d.objid = c.oid AND c.relkind = 'r'
WHERE %s
AND p.oid NOT IN (select objid from pg_depend where deptype = 'e')`, SchemaFilterClause("n"))
	tableQuery := `
SELECT
	objid AS oid,
	quote_ident(n.nspname) || '.' || quote_ident(p.relname) AS referencedobject,
	't' AS istable
FROM pg_depend d
JOIN pg_class p ON d.refobjid = p.oid AND p.relkind = 'r'
JOIN pg_namespace n ON p.relnamespace = n.oid
JOIN pg_class c ON d.objid = c.oid AND c.relkind = 'r'
AND p.oid NOT IN (select objid from pg_depend where deptype = 'e')`

	query := ""
	// If we are filtering on tables, we only want to record dependencies on other tables in the list
	if isTableFiltered {
		query = fmt.Sprintf("%s\nWHERE objid IN (%s);", tableQuery, strings.Join(tableOidList, ","))
	} else {
		query = fmt.Sprintf("%s\nUNION\n%s;", nonTableQuery, tableQuery)
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
		if tableDefs[dependency.Oid].IsExternal && tableDefs[dependency.Oid].PartitionType == "l" {
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
		tables[i].DependsUpon = dependencyMap[tables[i].Oid]
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
AND c.oid NOT IN (select objid from pg_depend where deptype = 'e')
ORDER BY n.nspname, c.relname;`, SchemaFilterClause("n"))

	results := make([]Relation, 0)
	err := connection.Select(&results, query)
	gplog.FatalOnError(err)
	return results
}

type SequenceDefinition struct {
	Name      string `db:"sequence_name"`
	LastVal   int64  `db:"last_value"`
	StartVal  int64  `db:"start_value"`
	Increment int64  `db:"increment_by"`
	MaxVal    int64  `db:"max_value"`
	MinVal    int64  `db:"min_value"`
	CacheVal  int64  `db:"cache_value"`
	LogCnt    int64  `db:"log_cnt"`
	IsCycled  bool   `db:"is_cycled"`
	IsCalled  bool   `db:"is_called"`
}

func GetSequenceDefinition(connection *dbconn.DBConn, seqName string) SequenceDefinition {
	query := fmt.Sprintf("SELECT * FROM %s", seqName)
	result := SequenceDefinition{}
	err := connection.Get(&result, query)
	gplog.FatalOnError(err)
	return result
}

func GetSequenceColumnOwnerMap(connection *dbconn.DBConn) map[string]string {
	query := `SELECT
	quote_ident(n.nspname) AS schema,
	quote_ident(s.relname) AS name,
	quote_ident(t.relname) AS tablename,
	quote_ident(a.attname) AS columnname
FROM pg_depend d
JOIN pg_attribute a
	ON a.attrelid = d.refobjid AND a.attnum = d.refobjsubid
JOIN pg_class s
	ON s.oid = d.objid
JOIN pg_class t
	ON t.oid = d.refobjid
JOIN pg_namespace n
	ON n.oid = s.relnamespace
WHERE s.relkind = 'S';`

	results := make([]struct {
		Schema     string
		Name       string
		TableName  string
		ColumnName string
	}, 0)
	sequenceOwners := make(map[string]string, 0)
	err := connection.Select(&results, query)
	gplog.FatalOnError(err)
	for _, seqOwner := range results {
		seqFQN := utils.MakeFQN(seqOwner.Schema, seqOwner.Name)
		columnFQN := fmt.Sprintf("%s.%s.%s", seqOwner.Schema, seqOwner.TableName, seqOwner.ColumnName)
		sequenceOwners[seqFQN] = columnFQN
	}
	return sequenceOwners
}

type View struct {
	Oid         uint32
	Schema      string
	Name        string
	Definition  string
	DependsUpon []string
}

func (v View) ToString() string {
	return utils.MakeFQN(v.Schema, v.Name)
}

func GetViews(connection *dbconn.DBConn) []View {
	results := make([]View, 0)

	query := fmt.Sprintf(`
SELECT
	c.oid,
	quote_ident(n.nspname) AS schema,
	quote_ident(c.relname) AS name,
	pg_get_viewdef(c.oid) AS definition
FROM pg_class c
LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = 'v'::"char" AND %s
AND c.oid NOT IN (select objid from pg_depend where deptype = 'e');`, SchemaFilterClause("n"))
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
	AND v1.oid NOT IN (select objid from pg_depend where deptype = 'e')
ORDER BY v2.oid, referencedobject;`, SchemaFilterClause("n"))

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
		connection.MustExec(fmt.Sprintf("LOCK TABLE %s IN ACCESS SHARE MODE", table.ToString()))
		progressBar.Increment()
	}
	progressBar.Finish()
}
