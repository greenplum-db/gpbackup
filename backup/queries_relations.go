package backup

/*
 * This file contains structs and functions related to executing specific
 * queries to gather metadata for the objects handled in predata_relations.go.
 */

import (
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
		if len(excludeOids) > 0 {
			filterClause += fmt.Sprintf("\nAND c.oid NOT IN (%s)", strings.Join(excludeOids, ", "))
		}
	}
	if len(MustGetFlagStringSlice(utils.INCLUDE_RELATION)) > 0 {
		includeOids := GetOidsFromRelationList(connectionPool, MustGetFlagStringSlice(utils.INCLUDE_RELATION))
		filterClause += fmt.Sprintf("\nAND c.oid IN (%s)", strings.Join(includeOids, ", "))
	}
	return filterClause
}

func GetOidsFromRelationList(connectionPool *dbconn.DBConn, relationNames []string) []string {
	relList := utils.SliceToQuotedString(relationNames)
	query := fmt.Sprintf(`
SELECT
	c.oid AS string
FROM pg_class c
JOIN pg_namespace n ON c.relnamespace = n.oid
WHERE quote_ident(n.nspname) || '.' || quote_ident(c.relname) IN (%s)`, relList)
	return dbconn.MustSelectStringSlice(connectionPool, query)
}

func GetAllUserTableRelations(connectionPool *dbconn.DBConn) []Relation {
	if len(MustGetFlagStringSlice(utils.INCLUDE_RELATION)) > 0 {
		return GetUserTableRelationsWithIncludeFiltering(connectionPool)
	}
	return GetUserTableRelations(connectionPool)
}

type Relation struct {
	SchemaOid uint32
	Oid       uint32
	Schema    string
	Name      string
}

func (r Relation) FQN() string {
	return utils.MakeFQN(r.Schema, r.Name)
}

func (r Relation) GetUniqueID() UniqueID {
	return UniqueID{ClassID: PG_CLASS_OID, Oid: r.Oid}
}

/*
 * This function also handles exclude table filtering since the way we do
 * it is currently much simpler than the include case.
 */
func GetUserTableRelations(connectionPool *dbconn.DBConn) []Relation {
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
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)

	return results
}

func GetUserTableRelationsWithIncludeFiltering(connectionPool *dbconn.DBConn) []Relation {
	includeOids := GetOidsFromRelationList(connectionPool, MustGetFlagStringSlice(utils.INCLUDE_RELATION))
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
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)
	return results
}

func GetForeignTableRelations(connectionPool *dbconn.DBConn) []Relation {
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
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)
	return results
}

type Sequence struct {
	Relation
	SequenceDefinition
}

type SequenceDefinition struct {
	LastVal     int64
	StartVal    int64
	Increment   int64
	MaxVal      int64
	MinVal      int64
	CacheVal    int64
	LogCnt      int64
	IsCycled    bool
	IsCalled    bool
	OwningTable string
}

func GetAllSequences(connectionPool *dbconn.DBConn, sequenceOwnerTables map[string]string) []Sequence {
	sequenceRelations := GetAllSequenceRelations(connectionPool)
	sequences := make([]Sequence, 0)
	for _, seqRelation := range sequenceRelations {
		seqDef := GetSequenceDefinition(connectionPool, seqRelation.FQN())
		seqDef.OwningTable = sequenceOwnerTables[seqRelation.FQN()]
		sequence := Sequence{seqRelation, seqDef}
		sequences = append(sequences, sequence)
	}
	return sequences
}

func GetAllSequenceRelations(connectionPool *dbconn.DBConn) []Relation {
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
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)

	return results
}

func GetSequenceDefinition(connectionPool *dbconn.DBConn, seqName string) SequenceDefinition {
	query := fmt.Sprintf(`SELECT
	last_value AS lastval,
	start_value AS startval,
	increment_by AS increment,
	max_value AS maxval,
	min_value AS minval,
	cache_value AS cacheval,
	log_cnt AS logcnt,
	is_cycled AS iscycled,
	is_called AS iscalled
	FROM %s`, seqName)
	result := SequenceDefinition{}
	err := connectionPool.Get(&result, query)
	gplog.FatalOnError(err)
	return result
}

func GetSequenceColumnOwnerMap(connectionPool *dbconn.DBConn) (map[string]string, map[string]string) {
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
	err := connectionPool.Select(&results, query)
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
	Oid        uint32
	Schema     string
	Name       string
	Options    string
	Definition string
}

func (v View) GetUniqueID() UniqueID {
	return UniqueID{ClassID: PG_CLASS_OID, Oid: v.Oid}
}

func (v View) FQN() string {
	return utils.MakeFQN(v.Schema, v.Name)
}

func GetViews(connectionPool *dbconn.DBConn) []View {
	results := make([]View, 0)
	optionsStr := ""
	if connectionPool.Version.AtLeast("6") {
		optionsStr = "coalesce(' WITH (' || array_to_string(c.reloptions, ', ') || ')', '') AS options,"
	}
	query := fmt.Sprintf(`
SELECT
	c.oid AS oid,
	quote_ident(n.nspname) AS schema,
	quote_ident(c.relname) AS name,
	%s
	pg_get_viewdef(c.oid) AS definition
FROM pg_class c
LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = 'v'::"char"
AND %s
AND %s;`, optionsStr, relationAndSchemaFilterClause(), ExtensionFilterClause("c"))
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)
	return results
}

func LockTables(connectionPool *dbconn.DBConn, tables []Relation) {
	gplog.Info("Acquiring ACCESS SHARE locks on tables")
	progressBar := utils.NewProgressBar(len(tables), "Locks acquired: ", utils.PB_VERBOSE)
	progressBar.Start()
	for _, table := range tables {
		connectionPool.MustExec(fmt.Sprintf("LOCK TABLE %s IN ACCESS SHARE MODE", table.FQN()))
		progressBar.Increment()
	}
	progressBar.Finish()
}
