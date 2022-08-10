package backup

/*
 * This file contains structs and functions related to executing specific
 * queries to gather metadata for the objects handled in postdata.go.
 */

import (
	"database/sql"
	"fmt"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpbackup/toc"
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
func ConstructImplicitIndexOidList(connectionPool *dbconn.DBConn) string {
	query := fmt.Sprintf(`
	SELECT i.indexrelid
	FROM pg_index i
		JOIN pg_depend d on i.indexrelid = d.objid
		JOIN pg_constraint c on d.refobjid = c.oid
	WHERE i.indexrelid >= %d
		AND i.indisunique is true
		AND i.indisprimary is false;`, FIRST_NORMAL_OBJECT_ID)
	indexNames := dbconn.MustSelectStringSlice(connectionPool, query)
	return utils.SliceToQuotedString(indexNames)
}

type IndexDefinition struct {
	Oid                uint32
	Name               string
	OwningSchema       string
	OwningTable        string
	Tablespace         string
	Def                sql.NullString
	IsClustered        bool
	SupportsConstraint bool
	IsReplicaIdentity  bool
	StatisticsColumns  string
	StatisticsValues   string
}

func (i IndexDefinition) GetMetadataEntry() (string, toc.MetadataEntry) {
	tableFQN := utils.MakeFQN(i.OwningSchema, i.OwningTable)
	return "postdata",
		toc.MetadataEntry{
			Schema:          i.OwningSchema,
			Name:            i.Name,
			ObjectType:      "INDEX",
			ReferenceObject: tableFQN,
			StartByte:       0,
			EndByte:         0,
		}
}

func (i IndexDefinition) GetUniqueID() UniqueID {
	return UniqueID{ClassID: PG_INDEX_OID, Oid: i.Oid}
}

func (i IndexDefinition) FQN() string {
	return utils.MakeFQN(i.OwningSchema, i.Name)
}

/*
 * GetIndexes queries for all user and implicitly created indexes, since
 * implicitly created indexes could still have metadata to be backed up.
 * e.g. comments on implicitly created indexes
 */
func GetIndexes(connectionPool *dbconn.DBConn) []IndexDefinition {
	resultIndexes := make([]IndexDefinition, 0)
	if connectionPool.Version.Before("6") {
		indexOidList := ConstructImplicitIndexOidList(connectionPool)
		implicitIndexStr := ""
		if indexOidList != "" {
			implicitIndexStr = fmt.Sprintf("OR i.indexrelid IN (%s)", indexOidList)
		}
		query := fmt.Sprintf(`
	SELECT DISTINCT i.indexrelid AS oid,
		quote_ident(ic.relname) AS name,
		quote_ident(n.nspname) AS owningschema,
		quote_ident(c.relname) AS owningtable,
		coalesce(quote_ident(s.spcname), '') AS tablespace,
		pg_get_indexdef(i.indexrelid) AS def,
		i.indisclustered AS isclustered,
		CASE
			WHEN i.indisprimary = 't' %s THEN 't'
			ELSE 'f'
		END AS supportsconstraint
	FROM pg_index i
		JOIN pg_class ic ON (ic.oid = i.indexrelid)
		JOIN pg_namespace n ON (ic.relnamespace = n.oid)
		JOIN pg_class c ON (c.oid = i.indrelid)
		LEFT JOIN pg_tablespace s ON (ic.reltablespace = s.oid)
	WHERE %s
		AND i.indisvalid
		AND NOT EXISTS (SELECT 1 FROM pg_partition_rule r WHERE r.parchildrelid = c.oid)
		AND %s
	ORDER BY name`,
	implicitIndexStr, relationAndSchemaFilterClause(), ExtensionFilterClause("c"))

		err := connectionPool.Select(&resultIndexes, query)
		gplog.FatalOnError(err)
	} else {
		query := fmt.Sprintf(`
	SELECT DISTINCT i.indexrelid AS oid,
		quote_ident(ic.relname) AS name,
		quote_ident(n.nspname) AS owningschema,
		quote_ident(c.relname) AS owningtable,
		coalesce(quote_ident(s.spcname), '') AS tablespace,
		pg_get_indexdef(i.indexrelid) AS def,
		i.indisclustered AS isclustered,
		i.indisreplident AS isreplicaidentity,
		CASE
			WHEN conindid > 0 THEN 't'
			ELSE 'f'
		END as supportsconstraint,
		coalesce(array_to_string((SELECT pg_catalog.array_agg(attnum ORDER BY attnum) FROM pg_catalog.pg_attribute WHERE attrelid = i.indexrelid AND attstattarget >= 0), ','), '') as statisticscolumns,
		coalesce(array_to_string((SELECT pg_catalog.array_agg(attstattarget ORDER BY attnum) FROM pg_catalog.pg_attribute WHERE attrelid = i.indexrelid AND attstattarget >= 0), ','), '') as statisticsvalues
	FROM pg_index i
		JOIN pg_class ic ON ic.oid = i.indexrelid
		JOIN pg_namespace n ON ic.relnamespace = n.oid
		JOIN pg_class c ON c.oid = i.indrelid
		LEFT JOIN pg_tablespace s ON ic.reltablespace = s.oid
		LEFT JOIN pg_constraint con ON i.indexrelid = con.conindid
	WHERE %s
		AND i.indisvalid
		AND i.indisready
		AND i.indisprimary = 'f'
		AND NOT EXISTS (SELECT 1 FROM pg_partition_rule r WHERE r.parchildrelid = c.oid)
		AND %s
	ORDER BY name`,
	relationAndSchemaFilterClause(), ExtensionFilterClause("c")) // The index itself does not have a dependency on the extension, but the index's table does
		err := connectionPool.Select(&resultIndexes, query)
		gplog.FatalOnError(err)
	}

	// Remove all indexes that have NULL definitions. This can happen
	// if a concurrent index drop happens before the associated table
	// lock is acquired earlier during gpbackup execution.
	verifiedResultIndexes := make([]IndexDefinition, 0)
	for _, resultIndex := range resultIndexes {
		if resultIndex.Def.Valid {
			verifiedResultIndexes = append(verifiedResultIndexes, resultIndex)
		} else {
			gplog.Warn("Index '%s' on table '%s.%s' not backed up, most likely dropped after gpbackup had begun.",
				resultIndex.Name, resultIndex.OwningSchema, resultIndex.OwningTable)
		}
	}

	return verifiedResultIndexes
}

type RuleDefinition struct {
	Oid          uint32
	Name         string
	OwningSchema string
	OwningTable  string
	Def          sql.NullString
}

func (r RuleDefinition) GetMetadataEntry() (string, toc.MetadataEntry) {
	tableFQN := utils.MakeFQN(r.OwningSchema, r.OwningTable)
	return "postdata",
		toc.MetadataEntry{
			Schema:          r.OwningSchema,
			Name:            r.Name,
			ObjectType:      "RULE",
			ReferenceObject: tableFQN,
			StartByte:       0,
			EndByte:         0,
		}
}

func (r RuleDefinition) GetUniqueID() UniqueID {
	return UniqueID{ClassID: PG_REWRITE_OID, Oid: r.Oid}
}

func (r RuleDefinition) FQN() string {
	return r.Name
}

/*
 * Rules named "_RETURN", "pg_settings_n", and "pg_settings_u" are
 * built-in rules and we don't want to back them up. We use two `%` to
 * prevent Go from interpolating the % symbol.
 */
func GetRules(connectionPool *dbconn.DBConn) []RuleDefinition {
	query := fmt.Sprintf(`
	SELECT r.oid AS oid,
		quote_ident(r.rulename) AS name,
		quote_ident(n.nspname) AS owningschema,
		quote_ident(c.relname) AS owningtable,
		pg_get_ruledef(r.oid) AS def
	FROM pg_rewrite r
		JOIN pg_class c ON c.oid = r.ev_class
		JOIN pg_namespace n ON c.relnamespace = n.oid
	WHERE %s
		AND rulename NOT LIKE '%%RETURN'
		AND rulename NOT LIKE 'pg_%%'
		AND %s
	ORDER BY rulename`,
	relationAndSchemaFilterClause(), ExtensionFilterClause("c"))

	results := make([]RuleDefinition, 0)
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)

	// Remove all rules that have NULL definitions. Not sure how
	// this can happen since pg_get_ruledef uses an SPI query but
	// handle the NULL just in case.
	verifiedResults := make([]RuleDefinition, 0)
	for _, result := range results {
		if result.Def.Valid {
			verifiedResults = append(verifiedResults, result)
		} else {
			gplog.Warn("Rule '%s' on table '%s.%s' not backed up, most likely dropped after gpbackup had begun.",
				result.Name, result.OwningSchema, result.OwningTable)
		}
	}

	return verifiedResults
}

type TriggerDefinition RuleDefinition

func (t TriggerDefinition) GetMetadataEntry() (string, toc.MetadataEntry) {
	tableFQN := utils.MakeFQN(t.OwningSchema, t.OwningTable)
	return "postdata",
		toc.MetadataEntry{
			Schema:          t.OwningSchema,
			Name:            t.Name,
			ObjectType:      "TRIGGER",
			ReferenceObject: tableFQN,
			StartByte:       0,
			EndByte:         0,
		}
}

func (t TriggerDefinition) GetUniqueID() UniqueID {
	return UniqueID{ClassID: PG_TRIGGER_OID, Oid: t.Oid}
}

func (t TriggerDefinition) FQN() string {
	return t.Name
}

func GetTriggers(connectionPool *dbconn.DBConn) []TriggerDefinition {
	constraintClause := "NOT tgisinternal"
	if connectionPool.Version.Before("6") {
		constraintClause = "tgisconstraint = 'f'"
	}
	query := fmt.Sprintf(`
	SELECT t.oid AS oid,
		quote_ident(t.tgname) AS name,
		quote_ident(n.nspname) AS owningschema,
		quote_ident(c.relname) AS owningtable,
		pg_get_triggerdef(t.oid) AS def
	FROM pg_trigger t
		JOIN pg_class c ON c.oid = t.tgrelid
		JOIN pg_namespace n ON c.relnamespace = n.oid
	WHERE %s
		AND tgname NOT LIKE 'pg_%%'
		AND %s
		AND %s
	ORDER BY tgname`,
	relationAndSchemaFilterClause(), constraintClause, ExtensionFilterClause("c"))

	results := make([]TriggerDefinition, 0)
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)

	// Remove all triggers that have NULL definitions. This can happen
	// if the query above is run and a concurrent trigger drop happens
	// just before the pg_get_triggerdef function executes.
	verifiedResults := make([]TriggerDefinition, 0)
	for _, result := range results {
		if result.Def.Valid {
			verifiedResults = append(verifiedResults, result)
		} else {
			gplog.Warn("Trigger '%s' on table '%s.%s' not backed up, most likely dropped after gpbackup had begun.",
				result.Name, result.OwningSchema, result.OwningTable)
		}
	}

	return verifiedResults
}

type EventTrigger struct {
	Oid          uint32
	Name         string
	Event        string
	FunctionName string
	Enabled      string
	EventTags    string
}

func (et EventTrigger) GetMetadataEntry() (string, toc.MetadataEntry) {
	return "postdata",
		toc.MetadataEntry{
			Schema:          "",
			Name:            et.Name,
			ObjectType:      "EVENT TRIGGER",
			ReferenceObject: "",
			StartByte:       0,
			EndByte:         0,
		}
}

func (et EventTrigger) GetUniqueID() UniqueID {
	return UniqueID{ClassID: PG_EVENT_TRIGGER, Oid: et.Oid}
}

func (et EventTrigger) FQN() string {
	return et.Name
}

func GetEventTriggers(connectionPool *dbconn.DBConn) []EventTrigger {
	query := fmt.Sprintf(`
	SELECT et.oid,
		quote_ident(et.evtname) AS name,
		et.evtevent AS event,
		array_to_string(array(select quote_literal(x) from unnest(evttags) as t(x)), ', ') AS eventtags,
		et.evtfoid::regproc AS functionname,
		et.evtenabled AS enabled
	FROM pg_event_trigger et
	WHERE %s
	ORDER BY name`, ExtensionFilterClause("et"))

	results := make([]EventTrigger, 0)
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)
	return results
}
