package backup

/*
 * This file contains structs and functions related to executing specific
 * queries to gather metadata for the objects handled in postdata.go.
 */

import (
	"fmt"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
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
func ConstructImplicitIndexNames(connectionPool *dbconn.DBConn) *utils.FilterSet {
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
	indexNames := dbconn.MustSelectStringSlice(connectionPool, query)
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

func (i IndexDefinition) GetUniqueID() UniqueID {
	return UniqueID{ClassID: PG_INDEX_OID, Oid: i.Oid}
}

func GetIndexes(connectionPool *dbconn.DBConn) []IndexDefinition {
	resultIndexes := make([]IndexDefinition, 0)
	if connectionPool.Version.Before("6") {
		indexNameSet := ConstructImplicitIndexNames(connectionPool)
		query := fmt.Sprintf(`
SELECT DISTINCT
	i.indexrelid AS oid,
	quote_ident(ic.relname) AS name,
	quote_ident(n.nspname) AS owningschema,
	quote_ident(c.relname) AS owningtable,
	coalesce(quote_ident(s.spcname), '') AS tablespace,
	pg_get_indexdef(i.indexrelid) AS def,
	i.indisclustered AS isclustered
FROM pg_index i
JOIN pg_class ic
	ON (ic.oid = i.indexrelid)
JOIN pg_namespace n
	ON (ic.relnamespace = n.oid)
JOIN pg_class c
	ON (c.oid = i.indrelid)
LEFT JOIN pg_partitions p
	ON (c.relname = p.tablename AND p.partitionlevel = 0)
LEFT JOIN pg_tablespace s
	ON (ic.reltablespace = s.oid)
WHERE %s
AND i.indisvalid
AND i.indisprimary = 'f'
AND n.nspname || '.' || c.relname NOT IN (SELECT partitionschemaname || '.' || partitiontablename FROM pg_partitions)
AND %s
ORDER BY name;`, relationAndSchemaFilterClause(), ExtensionFilterClause("c"))

		results := make([]IndexDefinition, 0)
		err := connectionPool.Select(&results, query)
		gplog.FatalOnError(err)
		for _, index := range results {
			// We don't want to quote the index name to use it as a map key, just prepend the schema
			indexFQN := fmt.Sprintf("%s.%s", index.OwningSchema, index.Name)
			if indexNameSet.MatchesFilter(indexFQN) {
				resultIndexes = append(resultIndexes, index)
			}
		}
	} else {
		query := fmt.Sprintf(`
SELECT DISTINCT
	i.indexrelid AS oid,
	quote_ident(ic.relname) AS name,
	quote_ident(n.nspname) AS owningschema,
	quote_ident(c.relname) AS owningtable,
	coalesce(quote_ident(s.spcname), '') AS tablespace,
	pg_get_indexdef(i.indexrelid) AS def,
	i.indisclustered AS isclustered
FROM pg_index i
JOIN pg_class ic
	ON (ic.oid = i.indexrelid)
JOIN pg_namespace n
	ON (ic.relnamespace = n.oid)
JOIN pg_class c
	ON (c.oid = i.indrelid)
LEFT JOIN pg_partitions p
	ON (c.relname = p.tablename AND p.partitionlevel = 0)
LEFT JOIN pg_tablespace s
	ON (ic.reltablespace = s.oid)
LEFT JOIN pg_constraint con
	ON (i.indexrelid = con.conindid)
WHERE %s
AND i.indisvalid
AND i.indisready
AND i.indisprimary = 'f'
AND n.nspname || '.' || c.relname NOT IN (SELECT partitionschemaname || '.' || partitiontablename FROM pg_partitions)
AND con.conindid IS NULL
AND %s
ORDER BY name;`, relationAndSchemaFilterClause(), ExtensionFilterClause("c")) // The index itself does not have a dependency on the extension, but the index's table does
		err := connectionPool.Select(&resultIndexes, query)
		gplog.FatalOnError(err)
	}
	return resultIndexes
}

type RuleDefinition struct {
	Oid          uint32
	Name         string
	OwningSchema string
	OwningTable  string
	Def          string
}

func (r RuleDefinition) GetMetadataEntry(start uint64, end uint64) (string, utils.MetadataEntry) {
	tableFQN := utils.MakeFQN(r.OwningSchema, r.OwningTable)
	return "postdata",
		utils.MetadataEntry{
			Schema:          r.OwningSchema,
			Name:            r.Name,
			ObjectType:      "RULE",
			ReferenceObject: tableFQN,
			StartByte:       start,
			EndByte:         end,
		}
}

func (r RuleDefinition) GetUniqueID() UniqueID {
	return UniqueID{ClassID: PG_REWRITE_OID, Oid: r.Oid}
}

/*
 * Rules named "_RETURN", "pg_settings_n", and "pg_settings_u" are
 * built-in rules and we don't want to back them up. We use two `%` to
 * prevent Go from interpolating the % symbol.
 */
func GetRules(connectionPool *dbconn.DBConn) []RuleDefinition {
	query := fmt.Sprintf(`
SELECT
	r.oid AS oid,
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
AND %s
ORDER BY rulename;`, relationAndSchemaFilterClause(), ExtensionFilterClause("c"))

	results := make([]RuleDefinition, 0)
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)
	return results
}

type TriggerDefinition struct {
	Oid          uint32
	Name         string
	OwningSchema string
	OwningTable  string
	Def          string
}

func (t TriggerDefinition) GetMetadataEntry(start uint64, end uint64) (string, utils.MetadataEntry) {
	tableFQN := utils.MakeFQN(t.OwningSchema, t.OwningTable)
	return "postdata",
		utils.MetadataEntry{
			Schema:          t.OwningSchema,
			Name:            t.Name,
			ObjectType:      "TRIGGER",
			ReferenceObject: tableFQN,
			StartByte:       start,
			EndByte:         end,
		}
}

func (t TriggerDefinition) GetUniqueID() UniqueID {
	return UniqueID{ClassID: PG_TRIGGER_OID, Oid: t.Oid}
}

func GetTriggers(connectionPool *dbconn.DBConn) []TriggerDefinition {
	constraintClause := "NOT tgisinternal"
	if connectionPool.Version.Before("6") {
		constraintClause = "tgisconstraint = 'f'"
	}
	query := fmt.Sprintf(`
SELECT
	t.oid AS oid,
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
AND %s
AND %s
ORDER BY tgname;`, relationAndSchemaFilterClause(), constraintClause, ExtensionFilterClause("c"))

	results := make([]TriggerDefinition, 0)
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)
	return results
}

type EventTrigger struct {
	Oid          uint32
	Name         string
	Event        string
	FunctionName string
	Enabled      string
	EventTags    string
}

func (et EventTrigger) GetUniqueID() UniqueID {
	return UniqueID{ClassID: PG_EVENT_TRIGGER, Oid: et.Oid}
}

func GetEventTriggers(connectionPool *dbconn.DBConn) []EventTrigger {
	query := fmt.Sprintf(`
SELECT
	et.oid,
	quote_ident(et.evtname) AS name,
	et.evtevent AS event,
	array_to_string(array(select quote_literal(x) from unnest(evttags) as t(x)), ', ') AS eventtags,
	et.evtfoid::regproc AS functionname,
	et.evtenabled AS enabled
FROM pg_event_trigger et
WHERE %s
ORDER BY name;`, ExtensionFilterClause("et"))

	results := make([]EventTrigger, 0)
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)
	return results
}
