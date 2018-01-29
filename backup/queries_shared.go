package backup

/*
 * This file contains structs and functions related to executing specific
 * queries to gather metadata for the objects handled in predata_shared.go.
 */

import (
	"database/sql"
	"fmt"
	"sort"
	"strings"

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

func GetAllUserSchemas(connection *utils.DBConn) []Schema {
	/*
	 * This query is constructed from scratch, but the list of schemas to exclude
	 * is copied from gpcrondump so that gpbackup exhibits similar behavior regarding
	 * which schemas are backed up.
	 */
	query := fmt.Sprintf(`
SELECT
	oid,
	quote_ident(nspname) AS name
FROM pg_namespace n
WHERE %s
AND oid NOT IN (select objid from pg_depend where deptype = 'e')
ORDER BY name;`, SchemaFilterClause("n"))

	results := make([]Schema, 0)

	err := connection.Select(&results, query)
	utils.CheckError(err)
	return results
}

type Constraint struct {
	Oid                uint32
	Schema             string
	Name               string
	ConType            string
	ConDef             string
	OwningObject       string
	IsDomainConstraint bool
	IsPartitionParent  bool
}

func GetConstraints(connection *utils.DBConn, tables ...Relation) []Constraint {
	// This query is adapted from the queries underlying \d in psql.
	tableQuery := `
SELECT
	c.oid,
	quote_ident(n.nspname) AS schema,
	quote_ident(conname) AS name,
	contype,
	pg_get_constraintdef(c.oid, TRUE) AS condef,
	quote_ident(n.nspname) || '.' || quote_ident(r.relname) AS owningobject,
	'f' AS isdomainconstraint,
	CASE
		WHEN pt.parrelid IS NULL THEN 'f'
		ELSE 't'
	END AS ispartitionparent
FROM pg_constraint c
LEFT JOIN pg_class r ON c.conrelid = r.oid
LEFT JOIN pg_partition pt ON c.conrelid = pt.parrelid
JOIN pg_namespace n ON n.oid = c.connamespace
WHERE %s
AND r.relname IS NOT NULL
AND conrelid NOT IN (SELECT parchildrelid FROM pg_partition_rule)
AND (conrelid, conname) NOT IN (SELECT i.inhrelid, c.conname FROM pg_inherits i JOIN pg_constraint c ON i.inhrelid = c.conrelid JOIN pg_constraint p ON i.inhparent = p.conrelid WHERE c.conname = p.conname)
GROUP BY c.oid, conname, contype, r.relname, n.nspname, pt.parrelid`

	nonTableQuery := fmt.Sprintf(`SELECT
	c.oid,
	quote_ident(n.nspname) AS schema,
	quote_ident(conname) AS name,
	contype,
	pg_get_constraintdef(c.oid, TRUE) AS condef,
	quote_ident(n.nspname) || '.' || quote_ident(t.typname) AS owningobject,
	't' AS isdomainconstraint,
	'f' AS ispartitionparent
FROM pg_constraint c
LEFT JOIN pg_type t ON c.contypid = t.oid
JOIN pg_namespace n ON n.oid = c.connamespace
WHERE %s
AND t.typname IS NOT NULL
GROUP BY c.oid, conname, contype, n.nspname, t.typname
ORDER BY name;

`, SchemaFilterClause("n"))

	query := ""
	if len(tables) > 0 {
		oidList := make([]string, 0)
		for _, table := range tables {
			oidList = append(oidList, fmt.Sprintf("%d", table.Oid))
		}
		filterClause := fmt.Sprintf("%s\nAND r.oid IN (%s)", SchemaFilterClause("n"), strings.Join(oidList, ","))
		query = fmt.Sprintf(tableQuery, filterClause)
	} else {
		tableQuery = fmt.Sprintf(tableQuery, SchemaFilterClause("n"))
		query = fmt.Sprintf("%s\nUNION\n%s", tableQuery, nonTableQuery)
	}
	results := make([]Constraint, 0)
	err := connection.Select(&results, query)
	utils.CheckError(err)
	return results
}

/*
 * Structs and functions relating to generic metadata handling.
 */

type MetadataQueryParams struct {
	NameField    string
	SchemaField  string
	OidField     string
	ACLField     string
	OwnerField   string
	OidTable     string
	CommentTable string
	CatalogTable string
	Shared       bool
}

var (
	TYPE_AGGREGATE          MetadataQueryParams
	TYPE_CAST               MetadataQueryParams
	TYPE_CONSTRAINT         MetadataQueryParams
	TYPE_CONVERSION         MetadataQueryParams
	TYPE_DATABASE           MetadataQueryParams
	TYPE_EXTENSION          MetadataQueryParams
	TYPE_FOREIGNDATAWRAPPER MetadataQueryParams
	TYPE_FOREIGNSERVER      MetadataQueryParams
	TYPE_FUNCTION           MetadataQueryParams
	TYPE_INDEX              MetadataQueryParams
	TYPE_PROCLANGUAGE       MetadataQueryParams
	TYPE_OPERATOR           MetadataQueryParams
	TYPE_OPERATORCLASS      MetadataQueryParams
	TYPE_OPERATORFAMILY     MetadataQueryParams
	TYPE_PROTOCOL           MetadataQueryParams
	TYPE_RELATION           MetadataQueryParams
	TYPE_RESOURCEGROUP      MetadataQueryParams
	TYPE_RESOURCEQUEUE      MetadataQueryParams
	TYPE_ROLE               MetadataQueryParams
	TYPE_RULE               MetadataQueryParams
	TYPE_SCHEMA             MetadataQueryParams
	TYPE_TABLESPACE         MetadataQueryParams
	TYPE_TSCONFIGURATION    MetadataQueryParams
	TYPE_TSDICTIONARY       MetadataQueryParams
	TYPE_TSPARSER           MetadataQueryParams
	TYPE_TSTEMPLATE         MetadataQueryParams
	TYPE_TRIGGER            MetadataQueryParams
	TYPE_TYPE               MetadataQueryParams
)

func InitializeMetadataParams(connection *utils.DBConn) {
	TYPE_AGGREGATE = MetadataQueryParams{NameField: "proname", SchemaField: "pronamespace", OwnerField: "proowner", CatalogTable: "pg_proc"}
	TYPE_CAST = MetadataQueryParams{NameField: "typname", OidField: "oid", OidTable: "pg_type", CatalogTable: "pg_cast"}
	TYPE_CONSTRAINT = MetadataQueryParams{NameField: "conname", SchemaField: "connamespace", OidField: "oid", CatalogTable: "pg_constraint"}
	TYPE_CONVERSION = MetadataQueryParams{NameField: "conname", OidField: "oid", SchemaField: "connamespace", OwnerField: "conowner", CatalogTable: "pg_conversion"}
	TYPE_DATABASE = MetadataQueryParams{NameField: "datname", ACLField: "datacl", OwnerField: "datdba", CatalogTable: "pg_database", Shared: true}
	TYPE_EXTENSION = MetadataQueryParams{NameField: "extname", OidField: "oid", CatalogTable: "pg_extension"}
	TYPE_FOREIGNDATAWRAPPER = MetadataQueryParams{NameField: "fdwname", ACLField: "fdwacl", OwnerField: "fdwowner", CatalogTable: "pg_foreign_data_wrapper"}
	TYPE_FOREIGNSERVER = MetadataQueryParams{NameField: "srvname", ACLField: "srvacl", OwnerField: "srvowner", CatalogTable: "pg_foreign_server"}
	TYPE_FUNCTION = MetadataQueryParams{NameField: "proname", SchemaField: "pronamespace", ACLField: "proacl", OwnerField: "proowner", CatalogTable: "pg_proc"}
	TYPE_INDEX = MetadataQueryParams{NameField: "relname", OidField: "indexrelid", OidTable: "pg_class", CommentTable: "pg_class", CatalogTable: "pg_index"}
	TYPE_PROCLANGUAGE = MetadataQueryParams{NameField: "lanname", ACLField: "lanacl", CatalogTable: "pg_language"}
	if connection.Version.Before("5") {
		TYPE_PROCLANGUAGE.OwnerField = "10" // In GPDB 4.3, there is no lanowner field in pg_language, but languages have an implicit owner
	} else {
		TYPE_PROCLANGUAGE.OwnerField = "lanowner"
	}
	TYPE_OPERATOR = MetadataQueryParams{NameField: "oprname", SchemaField: "oprnamespace", OidField: "oid", OwnerField: "oprowner", CatalogTable: "pg_operator"}
	TYPE_OPERATORCLASS = MetadataQueryParams{NameField: "opcname", SchemaField: "opcnamespace", OidField: "oid", OwnerField: "opcowner", CatalogTable: "pg_opclass"}
	TYPE_OPERATORFAMILY = MetadataQueryParams{NameField: "opfname", SchemaField: "opfnamespace", OidField: "oid", OwnerField: "opfowner", CatalogTable: "pg_opfamily"}
	TYPE_PROTOCOL = MetadataQueryParams{NameField: "ptcname", ACLField: "ptcacl", OwnerField: "ptcowner", CatalogTable: "pg_extprotocol"}
	TYPE_RELATION = MetadataQueryParams{NameField: "relname", SchemaField: "relnamespace", ACLField: "relacl", OwnerField: "relowner", CatalogTable: "pg_class"}
	TYPE_RESOURCEGROUP = MetadataQueryParams{NameField: "rsgname", OidField: "oid", CatalogTable: "pg_resgroup", Shared: true}
	TYPE_RESOURCEQUEUE = MetadataQueryParams{NameField: "rsqname", OidField: "oid", CatalogTable: "pg_resqueue", Shared: true}
	TYPE_ROLE = MetadataQueryParams{NameField: "rolname", OidField: "oid", CatalogTable: "pg_authid", Shared: true}
	TYPE_RULE = MetadataQueryParams{NameField: "rulename", OidField: "oid", CatalogTable: "pg_rewrite"}
	TYPE_SCHEMA = MetadataQueryParams{NameField: "nspname", ACLField: "nspacl", OwnerField: "nspowner", CatalogTable: "pg_namespace"}
	TYPE_TABLESPACE = MetadataQueryParams{NameField: "spcname", ACLField: "spcacl", OwnerField: "spcowner", CatalogTable: "pg_tablespace", Shared: true}
	TYPE_TSCONFIGURATION = MetadataQueryParams{NameField: "cfgname", OidField: "oid", SchemaField: "cfgnamespace", OwnerField: "cfgowner", CatalogTable: "pg_ts_config"}
	TYPE_TSDICTIONARY = MetadataQueryParams{NameField: "dictname", OidField: "oid", SchemaField: "dictnamespace", OwnerField: "dictowner", CatalogTable: "pg_ts_dict"}
	TYPE_TSPARSER = MetadataQueryParams{NameField: "prsname", OidField: "oid", SchemaField: "prsnamespace", CatalogTable: "pg_ts_parser"}
	TYPE_TSTEMPLATE = MetadataQueryParams{NameField: "tmplname", OidField: "oid", SchemaField: "tmplnamespace", CatalogTable: "pg_ts_template"}
	TYPE_TRIGGER = MetadataQueryParams{NameField: "tgname", OidField: "oid", CatalogTable: "pg_trigger"}
	TYPE_TYPE = MetadataQueryParams{NameField: "typname", SchemaField: "typnamespace", OwnerField: "typowner", CatalogTable: "pg_type"}
}

// A list of schemas we don't want to back up, formatted for use in a WHERE clause
func SchemaFilterClause(namespace string) string {
	schemaFilterClauseStr := ""
	if len(includeSchemas) > 0 {
		schemaFilterClauseStr = fmt.Sprintf("\nAND %s.nspname IN (%s)", namespace, utils.SliceToQuotedString(includeSchemas))
	}
	if len(excludeSchemas) > 0 {
		schemaFilterClauseStr = fmt.Sprintf("\nAND %s.nspname NOT IN (%s)", namespace, utils.SliceToQuotedString(excludeSchemas))
	}
	return fmt.Sprintf(`%s.nspname NOT LIKE 'pg_temp_%%' AND %s.nspname NOT LIKE 'pg_toast%%' AND %s.nspname NOT IN ('gp_toolkit', 'information_schema', 'pg_aoseg', 'pg_bitmapindex', 'pg_catalog') %s`, namespace, namespace, namespace, schemaFilterClauseStr)
}

type MetadataQueryStruct struct {
	Oid        uint32
	Privileges sql.NullString
	Kind       string
	Owner      string
	Comment    string
}

func GetMetadataForObjectType(connection *utils.DBConn, params MetadataQueryParams) MetadataMap {
	aclStr := "''"
	kindStr := "''"
	schemaStr := ""
	ownerStr := "''"
	if params.ACLField != "" {
		aclStr = fmt.Sprintf(`CASE
		WHEN %[1]s IS NULL OR array_upper(%[1]s, 1) = 0 THEN %[1]s[0]
		ELSE unnest(%[1]s)
	END`, params.ACLField)
		kindStr = fmt.Sprintf(`CASE
		WHEN %[1]s IS NULL THEN 'Default'
		WHEN array_upper(%[1]s, 1) = 0 THEN 'Empty'
		ELSE ''
	END`, params.ACLField)
	}
	if params.SchemaField != "" {
		schemaStr = fmt.Sprintf(`JOIN pg_namespace n ON o.%s = n.oid
WHERE %s`, params.SchemaField, SchemaFilterClause("n"))
	}
	descFunc := "pg_description"
	subidStr := " AND d.objsubid = 0"
	if params.Shared {
		descFunc = "pg_shdescription"
		subidStr = ""
	}
	if params.OwnerField != "" {
		ownerStr = fmt.Sprintf("pg_get_userbyid(%s)", params.OwnerField)
	}
	query := fmt.Sprintf(`
SELECT
	o.oid,
	%s AS privileges,
	%s AS kind,
	%s AS owner,
	coalesce(description,'') AS comment
FROM %s o LEFT JOIN %s d ON (d.objoid = o.oid AND d.classoid = '%s'::regclass%s)
%s
ORDER BY o.oid;
`, aclStr, kindStr, ownerStr, params.CatalogTable, descFunc, params.CatalogTable, subidStr, schemaStr)

	results := make([]MetadataQueryStruct, 0)
	err := connection.Select(&results, query)
	utils.CheckError(err)
	return ConstructMetadataMap(results)
}

func sortACLs(privileges []ACL) []ACL {
	sort.Slice(privileges, func(i, j int) bool {
		return privileges[i].Grantee < privileges[j].Grantee
	})
	return privileges
}

func GetCommentsForObjectType(connection *utils.DBConn, params MetadataQueryParams) MetadataMap {
	schemaStr := ""
	if params.SchemaField != "" {
		schemaStr = fmt.Sprintf(` JOIN pg_namespace n ON o.%s = n.oid
	 WHERE %s`, params.SchemaField, SchemaFilterClause("n"))
	}
	descTable := "pg_description"
	subidStr := " AND d.objsubid = 0"
	if params.Shared {
		descTable = "pg_shdescription"
		subidStr = ""
	}
	commentTable := params.CatalogTable
	if params.CommentTable != "" {
		commentTable = params.CommentTable
	}
	query := fmt.Sprintf(`
SELECT
	o.%s AS oid,
	coalesce(description,'') AS comment
	FROM %s o JOIN %s d ON (d.objoid = %s AND d.classoid = '%s'::regclass%s)%s;
`, params.OidField, params.CatalogTable, descTable, params.OidField, commentTable, subidStr, schemaStr)

	results := make([]struct {
		Oid     uint32
		Comment string
	}, 0)
	err := connection.Select(&results, query)
	utils.CheckError(err)

	metadataMap := make(MetadataMap)
	if len(results) > 0 {
		for _, result := range results {
			metadataMap[result.Oid] = ObjectMetadata{[]ACL{}, "", result.Comment}
		}
	}
	return metadataMap
}
