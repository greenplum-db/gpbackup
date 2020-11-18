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
	"github.com/greenplum-db/gpbackup/options"
	"github.com/greenplum-db/gpbackup/toc"
	"github.com/greenplum-db/gpbackup/utils"
)

func relationAndSchemaFilterClause() string {
	if filterRelationClause != "" {
		return filterRelationClause
	}
	filterRelationClause = SchemaFilterClause("n")
	if len(MustGetFlagStringArray(options.EXCLUDE_RELATION)) > 0 {
		excludeOids := getOidsFromRelationList(connectionPool, MustGetFlagStringArray(options.EXCLUDE_RELATION))
		if len(excludeOids) > 0 {
			filterRelationClause += fmt.Sprintf("\nAND c.oid NOT IN (%s)", strings.Join(excludeOids, ", "))
		}
	}
	if len(MustGetFlagStringArray(options.INCLUDE_RELATION)) > 0 {
		quotedIncludeRelations, err := options.QuoteTableNames(connectionPool, MustGetFlagStringArray(options.INCLUDE_RELATION))
		gplog.FatalOnError(err)

		includeOids := getOidsFromRelationList(connectionPool, quotedIncludeRelations)
		filterRelationClause += fmt.Sprintf("\nAND c.oid IN (%s)", strings.Join(includeOids, ", "))
	}
	return filterRelationClause
}

func getOidsFromRelationList(connectionPool *dbconn.DBConn, quotedIncludeRelations []string) []string {
	relList := utils.SliceToQuotedString(quotedIncludeRelations)
	query := fmt.Sprintf(`
	SELECT c.oid AS string
	FROM pg_class c
		JOIN pg_namespace n ON c.relnamespace = n.oid
	WHERE quote_ident(n.nspname) || '.' || quote_ident(c.relname) IN (%s)`, relList)
	return dbconn.MustSelectStringSlice(connectionPool, query)
}

func GetIncludedUserTableRelations(connectionPool *dbconn.DBConn, includedRelationsQuoted []string) []Relation {
	if len(MustGetFlagStringArray(options.INCLUDE_RELATION)) > 0 {
		return getUserTableRelationsWithIncludeFiltering(connectionPool, includedRelationsQuoted)
	}
	return getUserTableRelations(connectionPool)
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
func getUserTableRelations(connectionPool *dbconn.DBConn) []Relation {
	// TODO: fix for gpdb7 partitioning
	childPartitionFilter := ""
	if !MustGetFlagBool(options.LEAF_PARTITION_DATA) && connectionPool.Version.Before("7") {
		//Filter out non-external child partitions
		childPartitionFilter = `
	AND c.oid NOT IN (
		SELECT p.parchildrelid
		FROM pg_partition_rule p
			LEFT JOIN pg_exttable e ON p.parchildrelid = e.reloid
		WHERE e.reloid IS NULL)`
	}

	query := fmt.Sprintf(`
	SELECT n.oid AS schemaoid,
		c.oid AS oid,
		quote_ident(n.nspname) AS schema,
		quote_ident(c.relname) AS name
	FROM pg_class c
		JOIN pg_namespace n ON c.relnamespace = n.oid
	WHERE %s
		%s
		AND relkind = 'r'
		AND %s
		ORDER BY c.oid`,
		relationAndSchemaFilterClause(), childPartitionFilter, ExtensionFilterClause("c"))

	results := make([]Relation, 0)
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)

	return results
}

func getUserTableRelationsWithIncludeFiltering(connectionPool *dbconn.DBConn, includedRelationsQuoted []string) []Relation {
	includeOids := getOidsFromRelationList(connectionPool, includedRelationsQuoted)
	oidStr := strings.Join(includeOids, ", ")
	query := fmt.Sprintf(`
	SELECT n.oid AS schemaoid,
		c.oid AS oid,
		quote_ident(n.nspname) AS schema,
		quote_ident(c.relname) AS name
	FROM pg_class c
		JOIN pg_namespace n ON c.relnamespace = n.oid
	WHERE c.oid IN (%s)
		AND (relkind = 'r')
	ORDER BY c.oid`, oidStr)

	results := make([]Relation, 0)
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)
	return results
}

func GetForeignTableRelations(connectionPool *dbconn.DBConn) []Relation {
	query := fmt.Sprintf(`
	SELECT n.oid AS schemaoid,
		c.oid AS oid,
		quote_ident(n.nspname) AS schema,
		quote_ident(c.relname) AS name
	FROM pg_class c
		JOIN pg_namespace n ON c.relnamespace = n.oid
	WHERE %s
		AND relkind = 'f'
		AND %s
	ORDER BY c.oid`,
		relationAndSchemaFilterClause(), ExtensionFilterClause("c"))

	results := make([]Relation, 0)
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)
	return results
}

type Sequence struct {
	Relation
	OwningTableOid          string
	OwningTableSchema       string
	OwningTable             string
	OwningColumn            string
	OwningColumnAttIdentity string
	IsIdentity              bool
	Definition              SequenceDefinition
}

func (s Sequence) GetMetadataEntry() (string, toc.MetadataEntry) {
	return "predata",
		toc.MetadataEntry{
			Schema:          s.Schema,
			Name:            s.Name,
			ObjectType:      "SEQUENCE",
			ReferenceObject: "",
			StartByte:       0,
			EndByte:         0,
		}
}

type SequenceDefinition struct {
	LastVal     int64
	Type        string
	StartVal    int64
	Increment   int64
	MaxVal      int64
	MinVal      int64
	CacheVal    int64
	IsCycled    bool
	IsCalled    bool
	OwningTable string
}

func GetAllSequences(connectionPool *dbconn.DBConn) []Sequence {
	var query string
	if connectionPool.Version.AtLeast("7") {
		query = fmt.Sprintf(`
		SELECT n.oid AS schemaoid,
			c.oid AS oid,
			quote_ident(n.nspname) AS schema,
			quote_ident(c.relname) AS name,
			coalesce(d.refobjid::text, '') AS owningtableoid,
			coalesce(quote_ident(m.nspname), '') AS owningtableschema,
			coalesce(quote_ident(t.relname), '') AS owningtable,
			coalesce(quote_ident(a.attname), '') AS owningcolumn,
			a.attidentity AS owningcolumnattidentity,
			CASE
				WHEN d.deptype IS NULL THEN false
				ELSE d.deptype = 'i'
			END AS isidentity
		FROM pg_class c
			JOIN pg_namespace n ON n.oid = c.relnamespace
			LEFT JOIN pg_depend d ON c.oid = d.objid AND d.deptype in ('a', 'i')
			LEFT JOIN pg_class t ON t.oid = d.refobjid
			LEFT JOIN pg_namespace m ON m.oid = t.relnamespace
			LEFT JOIN pg_attribute a ON a.attrelid = d.refobjid AND a.attnum = d.refobjsubid
		WHERE c.relkind = 'S'
			AND %s
			AND %s
		ORDER BY n.nspname, c.relname`,
			relationAndSchemaFilterClause(), ExtensionFilterClause("c"))
	} else {
		query = fmt.Sprintf(`
		SELECT n.oid AS schemaoid,
			c.oid AS oid,
			quote_ident(n.nspname) AS schema,
			quote_ident(c.relname) AS name,
			coalesce(d.refobjid::text, '') AS owningtableoid,
			coalesce(quote_ident(m.nspname), '') AS owningtableschema,
			coalesce(quote_ident(t.relname), '') AS owningtable,
			coalesce(quote_ident(a.attname), '') AS owningcolumn
		FROM pg_class c
			JOIN pg_namespace n ON n.oid = c.relnamespace
			LEFT JOIN pg_depend d ON c.oid = d.objid AND d.deptype = 'a'
			LEFT JOIN pg_class t ON t.oid = d.refobjid
			LEFT JOIN pg_namespace m ON m.oid = t.relnamespace
			LEFT JOIN pg_attribute a ON a.attrelid = d.refobjid AND a.attnum = d.refobjsubid
		WHERE c.relkind = 'S'
			AND %s
			AND %s
		ORDER BY n.nspname, c.relname`,
			relationAndSchemaFilterClause(), ExtensionFilterClause("c"))
	}
	results := make([]Sequence, 0)
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)

	// Exclude owning table and owning column info for sequences
	// where owning table is excluded from backup
	excludeOids := make([]string, 0)
	if len(MustGetFlagStringArray(options.EXCLUDE_RELATION)) > 0 {
		excludeOids = getOidsFromRelationList(connectionPool,
			MustGetFlagStringArray(options.EXCLUDE_RELATION))
	}
	for i := range results {
		found := utils.Exists(excludeOids, results[i].OwningTableOid)
		if results[i].OwningTable != "" {
			results[i].OwningTable = fmt.Sprintf("%s.%s",
				results[i].OwningTableSchema, results[i].OwningTable)
		}
		if results[i].OwningColumn != "" {
			results[i].OwningColumn = fmt.Sprintf("%s.%s",
				results[i].OwningTable, results[i].OwningColumn)
		}
		if found {
			results[i].OwningTable = ""
			results[i].OwningColumn = ""
		}
		results[i].Definition = GetSequenceDefinition(connectionPool, results[i].FQN())
	}
	return results
}

func GetSequenceDefinition(connectionPool *dbconn.DBConn, seqName string) SequenceDefinition {
	var query string
	if connectionPool.Version.AtLeast("7") {
		query = fmt.Sprintf(`
		SELECT s.seqstart AS startval,
			r.last_value AS lastval,
			pg_catalog.format_type(s.seqtypid, NULL) AS type,
			s.seqincrement AS increment,
			s.seqmax AS maxval,
			s.seqmin AS minval,
			s.seqcache AS cacheval,
			s.seqcycle AS iscycled,
			r.is_called AS iscalled
		FROM %s r
		JOIN pg_sequence s ON s.seqrelid = '%s'::regclass::oid;`, seqName, seqName)
	} else {
		startValQuery := ""
		if connectionPool.Version.AtLeast("6") {
			startValQuery = "start_value AS startval,"
		}
		query = fmt.Sprintf(`
		SELECT last_value AS lastval,
			%s
			increment_by AS increment,
			max_value AS maxval,
			min_value AS minval,
			cache_value AS cacheval,
			is_cycled AS iscycled,
			is_called AS iscalled
		FROM %s`, startValQuery, seqName)
	}
	result := SequenceDefinition{}
	err := connectionPool.Get(&result, query)
	gplog.FatalOnError(err)
	return result
}

type View struct {
	Oid            uint32
	Schema         string
	Name           string
	Options        string
	Definition     sql.NullString
	Tablespace     string
	IsMaterialized bool
}

func (v View) GetMetadataEntry() (string, toc.MetadataEntry) {
	return "predata",
		toc.MetadataEntry{
			Schema:          v.Schema,
			Name:            v.Name,
			ObjectType:      v.ObjectType(),
			ReferenceObject: "",
			StartByte:       0,
			EndByte:         0,
		}
}

func (v View) GetUniqueID() UniqueID {
	return UniqueID{ClassID: PG_CLASS_OID, Oid: v.Oid}
}

func (v View) FQN() string {
	return utils.MakeFQN(v.Schema, v.Name)
}

func (v View) ObjectType() string {
	if v.IsMaterialized {
		return "MATERIALIZED VIEW"
	}
	return "VIEW"
}

// This function retrieves both regular views and materialized views.
// Materialized views were introduced in GPDB 7 and backported to GPDB 6.2.
func GetAllViews(connectionPool *dbconn.DBConn) []View {

	// When querying the view definition using pg_get_viewdef(), the pg function
	// obtains dependency locks that are not released until the transaction is
	// committed at the end of gpbackup session. This blocks other sessions
	// from commands that need AccessExclusiveLock (e.g. TRUNCATE).
	// NB: SAVEPOINT should be created only if there is transaction in progress
	if connectionPool.Tx[0] != nil {
		connectionPool.MustExec("SAVEPOINT gpbackup_get_views")
		defer connectionPool.MustExec("ROLLBACK TO SAVEPOINT gpbackup_get_views")
	}

	selectClause := `
	SELECT
		c.oid AS oid,
		quote_ident(n.nspname) AS schema,
		quote_ident(c.relname) AS name,
		pg_get_viewdef(c.oid) AS definition`
	if connectionPool.Version.AtLeast("6") {
		selectClause += `,
		coalesce(' WITH (' || array_to_string(c.reloptions, ', ') || ')', '') AS options,
		coalesce(quote_ident(t.spcname), '') AS tablespace,
		c.relkind='m' AS ismaterialized`
	}

	fromClause := `
	FROM pg_class c
		LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
		LEFT JOIN pg_tablespace t ON t.oid = c.reltablespace`

	whereClause := fmt.Sprintf(`
	WHERE c.relkind IN ('m', 'v')
		AND %s
		AND %s`, relationAndSchemaFilterClause(), ExtensionFilterClause("c"))

	results := make([]View, 0)
	query := selectClause + fromClause + whereClause
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)

	// Remove all views that have NULL definitions. This can happen
	// if the query above is run and a concurrent view drop happens
	// just before the pg_get_viewdef function execute.
	verifiedResults := make([]View, 0)
	for _, result := range results {
		if result.Definition.Valid {
			verifiedResults = append(verifiedResults, result)
		} else {
			gplog.Warn("View '%s.%s' not backed up, most likely dropped after gpbackup had begun.", result.Schema, result.Name)
		}
	}

	return verifiedResults
}

func LockTables(connectionPool *dbconn.DBConn, tables []Relation) {
	gplog.Info("Acquiring ACCESS SHARE locks on tables")

	progressBar := utils.NewProgressBar(len(tables), "Locks acquired: ", utils.PB_VERBOSE)
	progressBar.Start()

	const batchSize = 100
	lastBatchSize := len(tables) % batchSize
	tableBatches := GenerateTableBatches(tables, batchSize)
	currentBatchSize := batchSize

	// The LOCK TABLE query could block if someone else is holding an
	// AccessExclusiveLock on the table.  In the case gpbackup is interrupted,
	// cancelBlockedQueries() will cancel these queries during cleanup.
	for i, currentBatch := range tableBatches {
		connectionPool.MustExec(fmt.Sprintf("LOCK TABLE %s IN ACCESS SHARE MODE", currentBatch))

		if i == len(tableBatches)-1 && lastBatchSize > 0 {
			currentBatchSize = lastBatchSize
		}

		progressBar.Add(currentBatchSize)
	}

	progressBar.Finish()
}

// GenerateTableBatches batches tables to reduce network congestion and
// resource contention.  Returns an array of batches where a batch of tables is
// a single string with comma separated tables
func GenerateTableBatches(tables []Relation, batchSize int) []string {
	var tableNames []string
	for _, table := range tables {
		tableNames = append(tableNames, table.FQN())
	}

	var end int
	var batches []string
	i := 0
	for i < len(tables) {
		if i+batchSize < len(tables) {
			end = i + batchSize
		} else {
			end = len(tables)
		}

		batches = append(batches, strings.Join(tableNames[i:end], ", "))
		i = end
	}

	return batches
}
