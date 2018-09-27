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

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
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

func (s Schema) GetUniqueID() UniqueID {
	return UniqueID{Classid: PG_NAMESPACE_OID, Oid: s.Oid}
}

func GetAllUserSchemas(connectionPool *dbconn.DBConn) []Schema {
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
AND %s
ORDER BY name;`, SchemaFilterClause("n"), ExtensionFilterClause(""))

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
	ConDef             string
	OwningObject       string
	IsDomainConstraint bool
	IsPartitionParent  bool
}

func (c Constraint) GetUniqueID() UniqueID {
	return UniqueID{Classid: PG_CONSTRAINT_OID, Oid: c.Oid}
}

func GetConstraints(connectionPool *dbconn.DBConn, includeTables ...Relation) []Constraint {
	// This query is adapted from the queries underlying \d in psql.
	tableQuery := fmt.Sprintf(`
SELECT
	con.oid,
	quote_ident(n.nspname) AS schema,
	quote_ident(conname) AS name,
	contype,
	pg_get_constraintdef(con.oid, TRUE) AS condef,
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
AND conrelid NOT IN (SELECT parchildrelid FROM pg_partition_rule)
AND (conrelid, conname) NOT IN (SELECT i.inhrelid, con.conname FROM pg_inherits i JOIN pg_constraint con ON i.inhrelid = con.conrelid JOIN pg_constraint p ON i.inhparent = p.conrelid WHERE con.conname = p.conname)
GROUP BY con.oid, conname, contype, c.relname, n.nspname, pt.parrelid`, "%s", ExtensionFilterClause("c"))

	nonTableQuery := fmt.Sprintf(`SELECT
	con.oid,
	quote_ident(n.nspname) AS schema,
	quote_ident(conname) AS name,
	contype,
	pg_get_constraintdef(con.oid, TRUE) AS condef,
	quote_ident(n.nspname) || '.' || quote_ident(t.typname) AS owningobject,
	't' AS isdomainconstraint,
	'f' AS ispartitionparent
FROM pg_constraint con
LEFT JOIN pg_type t ON con.contypid = t.oid
JOIN pg_namespace n ON n.oid = con.connamespace
WHERE %s
AND %s
AND t.typname IS NOT NULL
GROUP BY con.oid, conname, contype, n.nspname, t.typname
ORDER BY name;

`, SchemaFilterClause("n"), ExtensionFilterClause("con"))

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
	TYPE_COLLATION          MetadataQueryParams
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

func InitializeMetadataParams(connectionPool *dbconn.DBConn) {
	TYPE_AGGREGATE = MetadataQueryParams{NameField: "proname", SchemaField: "pronamespace", OwnerField: "proowner", CatalogTable: "pg_proc"}
	TYPE_CAST = MetadataQueryParams{NameField: "typname", OidField: "oid", OidTable: "pg_type", CatalogTable: "pg_cast"}
	TYPE_COLLATION = MetadataQueryParams{NameField: "collname", OidField: "oid", SchemaField: "collnamespace", OwnerField: "collowner", CatalogTable: "pg_collation"}
	TYPE_CONSTRAINT = MetadataQueryParams{NameField: "conname", SchemaField: "connamespace", OidField: "oid", CatalogTable: "pg_constraint"}
	TYPE_CONVERSION = MetadataQueryParams{NameField: "conname", OidField: "oid", SchemaField: "connamespace", OwnerField: "conowner", CatalogTable: "pg_conversion"}
	TYPE_DATABASE = MetadataQueryParams{NameField: "datname", ACLField: "datacl", OwnerField: "datdba", CatalogTable: "pg_database", Shared: true}
	TYPE_EXTENSION = MetadataQueryParams{NameField: "extname", OidField: "oid", CatalogTable: "pg_extension"}
	TYPE_FOREIGNDATAWRAPPER = MetadataQueryParams{NameField: "fdwname", ACLField: "fdwacl", OwnerField: "fdwowner", CatalogTable: "pg_foreign_data_wrapper"}
	TYPE_FOREIGNSERVER = MetadataQueryParams{NameField: "srvname", ACLField: "srvacl", OwnerField: "srvowner", CatalogTable: "pg_foreign_server"}
	TYPE_FUNCTION = MetadataQueryParams{NameField: "proname", SchemaField: "pronamespace", ACLField: "proacl", OwnerField: "proowner", CatalogTable: "pg_proc"}
	TYPE_INDEX = MetadataQueryParams{NameField: "relname", OidField: "indexrelid", OidTable: "pg_class", CommentTable: "pg_class", CatalogTable: "pg_index"}
	TYPE_PROCLANGUAGE = MetadataQueryParams{NameField: "lanname", ACLField: "lanacl", CatalogTable: "pg_language"}
	if connectionPool.Version.Before("5") {
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
	if len(MustGetFlagStringSlice(utils.INCLUDE_SCHEMA)) > 0 {
		schemaFilterClauseStr = fmt.Sprintf("\nAND %s.nspname IN (%s)", namespace, utils.SliceToQuotedString(MustGetFlagStringSlice(utils.INCLUDE_SCHEMA)))
	}
	if len(MustGetFlagStringSlice(utils.EXCLUDE_SCHEMA)) > 0 {
		schemaFilterClauseStr = fmt.Sprintf("\nAND %s.nspname NOT IN (%s)", namespace, utils.SliceToQuotedString(MustGetFlagStringSlice(utils.EXCLUDE_SCHEMA)))
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

type MetadataQueryStruct struct {
	UniqueID
	Privileges sql.NullString
	Kind       string
	Owner      string
	Comment    string
}

func GetMetadataForObjectType(connectionPool *dbconn.DBConn, params MetadataQueryParams) MetadataMap {
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
		ownerStr = fmt.Sprintf("quote_ident(pg_get_userbyid(%s))", params.OwnerField)
	}

	query := fmt.Sprintf(`
SELECT
	'%s'::regclass::oid AS classid,
	o.oid,
	%s AS privileges,
	%s AS kind,
	%s AS owner,
	coalesce(description,'') AS comment
FROM %s o LEFT JOIN %s d ON (d.objoid = o.oid AND d.classoid = '%s'::regclass%s)
%s
AND o.oid NOT IN (SELECT objid FROM pg_depend WHERE deptype='e')
ORDER BY o.oid;
`, params.CatalogTable, aclStr, kindStr, ownerStr, params.CatalogTable, descFunc, params.CatalogTable, subidStr, schemaStr)

	results := make([]MetadataQueryStruct, 0)
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)
	return ConstructMetadataMap(results)
}

func GetQuotedRoleNames(connectionPool *dbconn.DBConn) map[string]string {
	quotedRoleNames := make(map[string]string)

	quotedRoleNamesQuery := `
SELECT rolname AS rolename, quote_ident(rolname) AS quotedrolename FROM pg_authid`
	results := make([]struct {
		RoleName       string
		QuotedRoleName string
	}, 0)
	err := connectionPool.Select(&results, quotedRoleNamesQuery)
	gplog.FatalOnError(err)
	for _, result := range results {
		quotedRoleNames[result.RoleName] = result.QuotedRoleName
	}

	return quotedRoleNames
}

func sortACLs(privileges []ACL) []ACL {
	sort.Slice(privileges, func(i, j int) bool {
		return privileges[i].Grantee < privileges[j].Grantee
	})
	return privileges
}

func GetCommentsForObjectType(connectionPool *dbconn.DBConn, params MetadataQueryParams) MetadataMap {
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
	'%s'::regclass::oid AS classid,
	o.%s AS oid,
	coalesce(description,'') AS comment
	FROM %s o JOIN %s d ON (d.objoid = %s AND d.classoid = '%s'::regclass%s)%s;
`, params.CatalogTable, params.OidField, params.CatalogTable, descTable, params.OidField, commentTable, subidStr, schemaStr)

	results := make([]struct {
		UniqueID
		Comment string
	}, 0)
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)

	metadataMap := make(MetadataMap)
	if len(results) > 0 {
		for _, result := range results {
			metadataMap[result.UniqueID] = ObjectMetadata{[]ACL{}, "", result.Comment}
		}
	}
	return metadataMap
}
