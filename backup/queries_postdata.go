package backup

/*
 * This file contains structs and functions related to executing specific
 * queries to gather metadata for the objects handled in postdata.go.
 */

import (
	"fmt"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gpbackup/utils"
)

/*
 * This function constructs the names of implicit indexes created by
 * unique constraints on tables, so they can be filtered out of the
 * index list.
 *
 * Primary key indexes can only be created implicitly by a primary
 * key constraint, so they can be filtered out directly in the query
 * to get indexes, but multiple unique indexes can be created on the
 * same column so we only want to filter out the implicit ones.
 */
func ConstructImplicitIndexNames(connection *dbconn.DBConn) *utils.FilterSet {
	query := `
SELECT DISTINCT
	n.nspname || '.' || t.relname || '_' || a.attname || '_key' AS string
FROM pg_constraint c
JOIN pg_class t
	ON c.conrelid = t.oid
JOIN pg_namespace n
	ON (t.relnamespace = n.oid)
JOIN pg_attribute a
	ON c.conrelid = a.attrelid
JOIN pg_index i
	ON i.indrelid = c.conrelid
WHERE a.attnum > 0
AND i.indisunique = 't'
AND i.indisprimary = 'f';
`
	indexNames := dbconn.MustSelectStringSlice(connection, query)
	indexNameSet := utils.NewExcludeSet(indexNames)
	return indexNameSet
}

type IndexDefinition struct {
	Oid          uint32
	Name         string
	OwningSchema string
	OwningTable  string
	Tablespace   string
	Def          string
	IsClustered  bool
}

func GetIndexes(connection *dbconn.DBConn, indexNameSet *utils.FilterSet) []IndexDefinition {
	query := fmt.Sprintf(`
SELECT DISTINCT
	i.indexrelid AS oid,
	quote_ident(c.relname) AS name,
	quote_ident(n.nspname) AS owningschema,
	quote_ident(t.relname) AS owningtable,
	coalesce(quote_ident(s.spcname), '') AS tablespace,
	pg_get_indexdef(i.indexrelid) AS def,
	i.indisclustered AS isclustered
FROM pg_index i
JOIN pg_class c
	ON (c.oid = i.indexrelid)
JOIN pg_namespace n
	ON (c.relnamespace = n.oid)
JOIN pg_class t
	ON (t.oid = i.indrelid)
LEFT JOIN pg_partitions p
	ON (t.relname = p.tablename AND p.partitionlevel = 0)
LEFT JOIN pg_tablespace s
	ON (c.reltablespace = s.oid)
WHERE %s
AND i.indisprimary = 'f'
AND n.nspname || '.' || t.relname NOT IN (SELECT partitionschemaname || '.' || partitiontablename FROM pg_partitions)
ORDER BY name;`, SchemaFilterClause("n"))

	results := make([]IndexDefinition, 0)
	err := connection.Select(&results, query)
	utils.CheckError(err)
	filteredIndexes := make([]IndexDefinition, 0)
	for _, index := range results {
		// We don't want to quote the index name to use it as a map key, just prepend the schema
		indexFQN := fmt.Sprintf("%s.%s", index.OwningSchema, index.Name)
		if indexNameSet.MatchesFilter(indexFQN) {
			filteredIndexes = append(filteredIndexes, index)
		}
	}
	return filteredIndexes
}

/*
 * This struct is for objects that only have a definition, like rules
 * (pg_get_ruledef) and triggers (pg_get_triggerdef), and no owners or the like.
 * We get the owning table for the object because COMMENT ON [object type]
 * statements can require it.
 */
type QuerySimpleDefinition struct {
	Oid          uint32
	Name         string
	OwningSchema string
	OwningTable  string
	Def          string
}

/*
 * Rules named "_RETURN", "pg_settings_n", and "pg_settings_u" are
 * built-in rules and we don't want to back them up. We use two `%` to
 * prevent Go from interpolating the % symbol.
 */
func GetRules(connection *dbconn.DBConn) []QuerySimpleDefinition {
	query := fmt.Sprintf(`
SELECT
	r.oid,
	quote_ident(r.rulename) AS name,
	quote_ident(n.nspname) AS owningschema,
	quote_ident(c.relname) AS owningtable,
	pg_get_ruledef(r.oid) AS def
FROM pg_rewrite r
JOIN pg_class c
	ON (c.oid = r.ev_class)
JOIN pg_namespace n
	ON (c.relnamespace = n.oid)
WHERE %s
AND rulename NOT LIKE '%%RETURN'
AND rulename NOT LIKE 'pg_%%'
ORDER BY rulename;`, SchemaFilterClause("n"))

	results := make([]QuerySimpleDefinition, 0)
	err := connection.Select(&results, query)
	utils.CheckError(err)
	return results
}

func GetTriggers(connection *dbconn.DBConn) []QuerySimpleDefinition {
	query := fmt.Sprintf(`
SELECT
	t.oid,
	quote_ident(t.tgname) AS name,
	quote_ident(n.nspname) AS owningschema,
	quote_ident(c.relname) AS owningtable,
	pg_get_triggerdef(t.oid) AS def
FROM pg_trigger t
JOIN pg_class c
	ON (c.oid = t.tgrelid)
JOIN pg_namespace n
	ON (c.relnamespace = n.oid)
WHERE %s
AND tgname NOT LIKE 'pg_%%'
AND tgisconstraint = 'f'
ORDER BY tgname;`, SchemaFilterClause("n"))

	results := make([]QuerySimpleDefinition, 0)
	err := connection.Select(&results, query)
	utils.CheckError(err)
	return results
}
