package backup

/*
 * This file contains structs and functions related to executing specific
 * queries to gather metadata for the objects handled in predata_shared.go.
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

/*
 * All queries in the various queries_*.go files come from one of three sources:
 * - Copied from pg_dump largely unmodified
 * - Derived from the output of a psql flag like \d+ or \df
 * - Constructed from scratch
 * In the former two cases, a reference to the query source is provided for
 * further reference.
 *
 * All structs in these file whose names begin with "Query" are intended only
 * for use with the functions immediately following them.  Structs in the utils
 * package (especially Table and Schema) are intended for more general use.
 */
type Schema struct {
	Oid  uint32
	Name string
}

func (s Schema) GetMetadataEntry() (string, toc.MetadataEntry) {
	return "predata",
		toc.MetadataEntry{
			Schema:          s.Name,
			Name:            s.Name,
			ObjectType:      toc.OBJ_SCHEMA,
			ReferenceObject: "",
			StartByte:       0,
			EndByte:         0,
		}
}

func (s Schema) GetUniqueID() UniqueID {
	return UniqueID{ClassID: PG_NAMESPACE_OID, Oid: s.Oid}
}

func (s Schema) FQN() string {
	return s.Name
}

func GetAllUserSchemas(connectionPool *dbconn.DBConn, partitionAlteredSchemas map[string]bool) []Schema {
	/*
	 * This query is constructed from scratch, but the list of schemas to exclude
	 * is copied from gpcrondump so that gpbackup exhibits similar behavior regarding
	 * which schemas are backed up.
	 */
	query := fmt.Sprintf(`
	SELECT oid, quote_ident(nspname) AS name FROM pg_namespace n
		WHERE %s AND %s ORDER BY name`,
		SchemaFilterClauseWithAlteredPartitionSchemas("n", partitionAlteredSchemas),
		ExtensionFilterClause(""))
	results := make([]Schema, 0)

	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)
	return results
}

type Constraint struct {
	Oid                uint32
	Schema             string
	Name               string
	ConType            string
	Def                sql.NullString
	ConIsLocal         bool
	OwningObject       string
	IsDomainConstraint bool
	IsPartitionParent  bool
}

func (c Constraint) GetMetadataEntry() (string, toc.MetadataEntry) {
	return "postdata",
		toc.MetadataEntry{
			Schema:          c.Schema,
			Name:            c.Name,
			ObjectType:      toc.OBJ_CONSTRAINT,
			ReferenceObject: c.OwningObject,
			StartByte:       0,
			EndByte:         0,
		}
}

func (c Constraint) GetUniqueID() UniqueID {
	return UniqueID{ClassID: PG_CONSTRAINT_OID, Oid: c.Oid}
}

func (c Constraint) FQN() string {
	/*
	 * It is invalid to specify the schema name with the constraint
	 * even though they are technically part of the parent table's schema
	 */
	return c.Name
}

func GetConstraints(connectionPool *dbconn.DBConn, includeTables ...Relation) []Constraint {

	// ConIsLocal should always return true from GetConstraints because we
	// filter out constraints that are inherited using the INHERITS clause, or
	// inherited from a parent partition table. This field only accurately
	// reflects constraints in GPDB6+ because check constraints on parent
	// tables must propogate to children. For GPDB versions 5 or lower, this
	// field will default to false.
	conIsLocal := ""
	if connectionPool.Version.AtLeast("6") {
		conIsLocal = `con.conislocal,`
	}
	// This query is adapted from the queries underlying \d in psql.
	tableQuery := ""
	if connectionPool.Version.Before("6") {
		tableQuery = fmt.Sprintf(`
		SELECT con.oid,
			quote_ident(n.nspname) AS schema,
			quote_ident(conname) AS name,
			contype,
			%s
			pg_get_constraintdef(con.oid, TRUE) AS def,
			quote_ident(n.nspname) || '.' || quote_ident(c.relname) AS owningobject,
			'f' AS isdomainconstraint,
			CASE
				WHEN pt.parrelid IS NULL THEN 'f'
				ELSE 't'
			END AS ispartitionparent
		FROM pg_constraint con
			LEFT JOIN pg_class c ON con.conrelid = c.oid
			LEFT JOIN pg_partition pt ON con.conrelid = pt.parrelid
			JOIN pg_namespace n ON n.oid = con.connamespace
		WHERE %s
			AND %s
			AND c.relname IS NOT NULL
			AND contype != 't'
			AND conrelid NOT IN (SELECT parchildrelid FROM pg_partition_rule)
			AND (conrelid, conname) NOT IN (SELECT i.inhrelid, con.conname FROM pg_inherits i JOIN pg_constraint con ON i.inhrelid = con.conrelid JOIN pg_constraint p ON i.inhparent = p.conrelid WHERE con.conname = p.conname)
		GROUP BY con.oid, conname, contype, c.relname, n.nspname, %s pt.parrelid`, conIsLocal, "%s", ExtensionFilterClause("c"), conIsLocal)
	} else if connectionPool.Version.Is("6") {
		tableQuery = fmt.Sprintf(`
		SELECT con.oid,
			quote_ident(n.nspname) AS schema,
			quote_ident(conname) AS name,
			contype,
			%s
			pg_get_constraintdef(con.oid, TRUE) AS def,
			quote_ident(n.nspname) || '.' || quote_ident(c.relname) AS owningobject,
			'f' AS isdomainconstraint,
			CASE
				WHEN pt.parrelid IS NULL THEN 'f'
				ELSE 't'
			END AS ispartitionparent
		FROM pg_constraint con
			LEFT JOIN pg_class c ON con.conrelid = c.oid
			LEFT JOIN pg_partition pt ON con.conrelid = pt.parrelid
			JOIN pg_namespace n ON n.oid = con.connamespace
		WHERE %s
			AND %s
			AND c.relname IS NOT NULL
			AND contype != 't'
			AND conrelid NOT IN (SELECT parchildrelid FROM pg_partition_rule)
			AND coninhcount = 0
		GROUP BY con.oid, conname, contype, c.relname, n.nspname, %s pt.parrelid`, conIsLocal, "%s", ExtensionFilterClause("c"), conIsLocal)
	} else {
		tableQuery = fmt.Sprintf(`
		SELECT con.oid,
			quote_ident(n.nspname) AS schema,
			quote_ident(conname) AS name,
			contype,
			con.conislocal,
			pg_get_constraintdef(con.oid, TRUE) AS def,
			quote_ident(n.nspname) || '.' || quote_ident(c.relname) AS owningobject,
			'f' AS isdomainconstraint,
			CASE
				WHEN pt.partrelid IS NULL THEN 'f'
				ELSE 't'
			END AS ispartitionparent
		FROM pg_constraint con
			LEFT JOIN pg_class c ON con.conrelid = c.oid
			LEFT JOIN pg_partitioned_table pt ON con.conrelid = pt.partrelid
			JOIN pg_namespace n ON n.oid = con.connamespace
		WHERE %s
			AND %s
			AND c.relname IS NOT NULL
			AND contype != 't'
			AND (c.relispartition IS FALSE OR conislocal IS TRUE)
			AND coninhcount = 0
		GROUP BY con.oid, conname, contype, c.relname, n.nspname, con.conislocal, pt.partrelid`, "%s", ExtensionFilterClause("c"))
	}

	nonTableQuery := fmt.Sprintf(`
	SELECT con.oid,
		quote_ident(n.nspname) AS schema,
		quote_ident(conname) AS name,
		contype,
		%s
		pg_get_constraintdef(con.oid, TRUE) AS def,
		quote_ident(n.nspname) || '.' || quote_ident(t.typname) AS owningobject,
		't' AS isdomainconstraint,
		'f' AS ispartitionparent
	FROM pg_constraint con
		LEFT JOIN pg_type t ON con.contypid = t.oid
		JOIN pg_namespace n ON n.oid = con.connamespace
	WHERE %s
		AND %s
		AND t.typname IS NOT NULL
	GROUP BY con.oid, conname, contype, n.nspname, %s t.typname
	ORDER BY name`, conIsLocal, SchemaFilterClause("n"), ExtensionFilterClause("con"), conIsLocal)

	query := ""
	if len(includeTables) > 0 {
		oidList := make([]string, 0)
		for _, table := range includeTables {
			oidList = append(oidList, fmt.Sprintf("%d", table.Oid))
		}
		filterClause := fmt.Sprintf("%s\nAND c.oid IN (%s)", SchemaFilterClause("n"), strings.Join(oidList, ","))
		query = fmt.Sprintf(tableQuery, filterClause)
	} else {
		tableQuery = fmt.Sprintf(tableQuery, relationAndSchemaFilterClause())
		query = fmt.Sprintf("%s\nUNION\n%s", tableQuery, nonTableQuery)
	}
	results := make([]Constraint, 0)
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)

	if connectionPool.Version.Before("6") {
		// Remove all constraints that have NULL definitions. This can happen
		// if the query above is run and a concurrent constraint drop happens
		// just before the pg_get_constraintdef function executes. Note that
		// GPDB 6+ pg_get_constraintdef uses an MVCC snapshot instead of
		// syscache so we only need to verify for GPDB 4.3 and 5 (technically
		// only GPDB 5 since 4.3 will get a cache error on the query).
		verifiedResults := make([]Constraint, 0)
		for _, result := range results {
			if result.Def.Valid {
				verifiedResults = append(verifiedResults, result)
			} else {
				gplog.Warn("Constraint '%s.%s' not backed up, most likely dropped after gpbackup had begun.", result.Schema, result.Name)
			}
		}
		return verifiedResults
	} else {
		return results
	}
}

func RenameExchangedPartitionConstraints(connectionPool *dbconn.DBConn, constraints *[]Constraint) {
	query := GetRenameExchangedPartitionQuery()
	names := make([]ExchangedPartitionName, 0)
	err := connectionPool.Select(&names, query)
	gplog.FatalOnError(err)

	nameMap := ProcessIndexNames(names)
	if len(nameMap) > 0 {
		for idx := range *constraints {
			newName, hasNewName := nameMap[(*constraints)[idx].Name]
			if hasNewName {
				(*constraints)[idx].Def.String = strings.Replace((*constraints)[idx].Def.String, (*constraints)[idx].Name, newName, 1)
				(*constraints)[idx].Name = newName
			}
		}
	}
}

// A list of schemas we don't want to back up, formatted for use in a WHERE clause
func SchemaFilterClause(namespace string) string {
	schemaFilterClauseStr := ""
	if len(MustGetFlagStringArray(options.INCLUDE_SCHEMA)) > 0 {
		schemaFilterClauseStr = fmt.Sprintf("\nAND %s.nspname IN (%s)", namespace, utils.SliceToQuotedString(MustGetFlagStringArray(options.INCLUDE_SCHEMA)))
	}
	if len(MustGetFlagStringArray(options.EXCLUDE_SCHEMA)) > 0 {
		schemaFilterClauseStr = fmt.Sprintf("\nAND %s.nspname NOT IN (%s)", namespace, utils.SliceToQuotedString(MustGetFlagStringArray(options.EXCLUDE_SCHEMA)))
	}
	return fmt.Sprintf(`%s.nspname NOT LIKE 'pg_temp_%%' AND %s.nspname NOT LIKE 'pg_toast%%' AND %s.nspname NOT IN ('gp_toolkit', 'information_schema', 'pg_aoseg', 'pg_bitmapindex', 'pg_catalog') %s`, namespace, namespace, namespace, schemaFilterClauseStr)
}

/*
 * A list of schemas we don't want to back up, formatted for use in a
 * WHERE clause. This function takes into consideration child
 * partitions that are in different schemas than their root partition.
 */
func SchemaFilterClauseWithAlteredPartitionSchemas(namespace string, partitionAlteredSchemas map[string]bool) string {
	schemaFilterClauseStr := ""
	if len(MustGetFlagStringArray(options.INCLUDE_SCHEMA)) > 0 {
		includeSchemaArray := make([]string, 0)

		// Add partitionAlteredSchemas keys to the string array of options.INCLUDE_SCHEMA
		for _, includeSchema := range MustGetFlagStringArray(options.INCLUDE_SCHEMA) {
			partitionAlteredSchemas[includeSchema] = true
		}
		for key := range partitionAlteredSchemas {
			includeSchemaArray = append(includeSchemaArray, key)
		}

		if len(includeSchemaArray) > 0 {
			schemaFilterClauseStr = fmt.Sprintf("\nAND %s.nspname IN (%s)", namespace, utils.SliceToQuotedString(includeSchemaArray))
		}
	}
	if len(MustGetFlagStringArray(options.EXCLUDE_SCHEMA)) > 0 {
		excludeSchemaArray := make([]string, 0)

		// Remove partitionAlteredSchemas keys from the string array of options.EXCLUDE_SCHEMA
		for _, excludeSchema := range MustGetFlagStringArray(options.EXCLUDE_SCHEMA) {
			if !partitionAlteredSchemas[excludeSchema] {
				excludeSchemaArray = append(excludeSchemaArray, excludeSchema)
			}
		}

		if len(excludeSchemaArray) > 0 {
			schemaFilterClauseStr = fmt.Sprintf("\nAND %s.nspname NOT IN (%s)", namespace, utils.SliceToQuotedString(excludeSchemaArray))
		}
	}
	return fmt.Sprintf(`%s.nspname NOT LIKE 'pg_temp_%%' AND %s.nspname NOT LIKE 'pg_toast%%' AND %s.nspname NOT IN ('gp_toolkit', 'information_schema', 'pg_aoseg', 'pg_bitmapindex', 'pg_catalog') %s`, namespace, namespace, namespace, schemaFilterClauseStr)
}

func ExtensionFilterClause(namespace string) string {
	oidStr := "oid"
	if namespace != "" {
		oidStr = fmt.Sprintf("%s.oid", namespace)
	}

	return fmt.Sprintf("%s NOT IN (select objid from pg_depend where deptype = 'e')", oidStr)
}

type AccessMethod struct {
	Oid     uint32
	Name    string
	Handler string
	Type    string
}

func (a AccessMethod) GetMetadataEntry() (string, toc.MetadataEntry) {
	return "predata",
		toc.MetadataEntry{
			Name:            a.Name,
			ObjectType:      toc.OBJ_ACCESS_METHOD,
			ReferenceObject: "",
			StartByte:       0,
			EndByte:         0,
		}
}

func (a AccessMethod) FQN() string {
	return a.Name
}

func (a AccessMethod) GetUniqueID() UniqueID {
	return UniqueID{ClassID: PG_TYPE_OID, Oid: a.Oid}
}

func GetAccessMethods(connectionPool *dbconn.DBConn) []AccessMethod {
	results := make([]AccessMethod, 0)
	query := fmt.Sprintf(`
	SELECT oid,
       quote_ident(amname) AS name,
       amhandler::pg_catalog.regproc AS handler,
       amtype AS type
	FROM pg_am
	WHERE oid > %d
	ORDER BY oid;`, FIRST_NORMAL_OBJECT_ID)

	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)
	return results
}
