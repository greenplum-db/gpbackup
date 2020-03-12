package backup

/*
 * This file contains structs and functions related to executing specific
 * queries to gather metadata for the objects handled in predata_shared.go.
 */

import (
	"database/sql"
	"fmt"
	"sort"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpbackup/toc"
)

/*
 * Structs and functions relating to generic metadata handling.
 */

type MetadataQueryParams struct {
	NameField    string
	SchemaField  string
	ObjectType   string
	OidField     string
	ACLField     string
	OwnerField   string
	OidTable     string
	CommentTable string
	CatalogTable string
	FilterClause string
	Shared       bool
}

var (
	TYPE_AGGREGATE          MetadataQueryParams
	TYPE_CAST               MetadataQueryParams
	TYPE_COLLATION          MetadataQueryParams
	TYPE_CONSTRAINT         MetadataQueryParams
	TYPE_CONVERSION         MetadataQueryParams
	TYPE_DATABASE           MetadataQueryParams
	TYPE_EVENTTRIGGER       MetadataQueryParams
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
	TYPE_AGGREGATE = MetadataQueryParams{ObjectType: "AGGREGATE", NameField: "proname", SchemaField: "pronamespace", ACLField: "proacl", OwnerField: "proowner", CatalogTable: "pg_proc", FilterClause: "proisagg = 't'"}
	TYPE_CAST = MetadataQueryParams{ObjectType: "CAST", NameField: "typname", OidField: "oid", OidTable: "pg_type", CatalogTable: "pg_cast"}
	TYPE_COLLATION = MetadataQueryParams{ObjectType: "COLLATION", NameField: "collname", OidField: "oid", SchemaField: "collnamespace", OwnerField: "collowner", CatalogTable: "pg_collation"}
	TYPE_CONSTRAINT = MetadataQueryParams{ObjectType: "CONSTRAINT", NameField: "conname", SchemaField: "connamespace", OidField: "oid", CatalogTable: "pg_constraint"}
	TYPE_CONVERSION = MetadataQueryParams{ObjectType: "CONVERSION", NameField: "conname", OidField: "oid", SchemaField: "connamespace", OwnerField: "conowner", CatalogTable: "pg_conversion"}
	TYPE_DATABASE = MetadataQueryParams{ObjectType: "DATABASE", NameField: "datname", ACLField: "datacl", OwnerField: "datdba", CatalogTable: "pg_database", Shared: true}
	TYPE_EVENTTRIGGER = MetadataQueryParams{ObjectType: "EVENT TRIGGER", NameField: "evtname", OidField: "oid", OwnerField: "evtowner", CatalogTable: "pg_event_trigger"}
	TYPE_EXTENSION = MetadataQueryParams{ObjectType: "EXTENSION", NameField: "extname", OidField: "oid", CatalogTable: "pg_extension"}
	TYPE_FOREIGNDATAWRAPPER = MetadataQueryParams{ObjectType: "FOREIGN DATA WRAPPER", NameField: "fdwname", ACLField: "fdwacl", OwnerField: "fdwowner", CatalogTable: "pg_foreign_data_wrapper"}
	TYPE_FOREIGNSERVER = MetadataQueryParams{ObjectType: "SERVER", NameField: "srvname", ACLField: "srvacl", OwnerField: "srvowner", CatalogTable: "pg_foreign_server"}
	TYPE_FUNCTION = MetadataQueryParams{ObjectType: "FUNCTION", NameField: "proname", SchemaField: "pronamespace", ACLField: "proacl", OwnerField: "proowner", CatalogTable: "pg_proc", FilterClause: "proisagg = 'f'"}
	TYPE_INDEX = MetadataQueryParams{ObjectType: "INDEX", NameField: "relname", OidField: "indexrelid", OidTable: "pg_class", CommentTable: "pg_class", CatalogTable: "pg_index"}
	TYPE_PROCLANGUAGE = MetadataQueryParams{ObjectType: "LANGUAGE", NameField: "lanname", ACLField: "lanacl", CatalogTable: "pg_language"}
	if connectionPool.Version.Before("5") {
		TYPE_PROCLANGUAGE.OwnerField = "10" // In GPDB 4.3, there is no lanowner field in pg_language, but languages have an implicit owner
	} else {
		TYPE_PROCLANGUAGE.OwnerField = "lanowner"
	}
	TYPE_OPERATOR = MetadataQueryParams{ObjectType: "OPERATOR", NameField: "oprname", SchemaField: "oprnamespace", OidField: "oid", OwnerField: "oprowner", CatalogTable: "pg_operator"}
	TYPE_OPERATORCLASS = MetadataQueryParams{ObjectType: "OPERATOR CLASS", NameField: "opcname", SchemaField: "opcnamespace", OidField: "oid", OwnerField: "opcowner", CatalogTable: "pg_opclass"}
	TYPE_OPERATORFAMILY = MetadataQueryParams{ObjectType: "OPERATOR FAMILY", NameField: "opfname", SchemaField: "opfnamespace", OidField: "oid", OwnerField: "opfowner", CatalogTable: "pg_opfamily"}
	TYPE_PROTOCOL = MetadataQueryParams{ObjectType: "PROTOCOL", NameField: "ptcname", ACLField: "ptcacl", OwnerField: "ptcowner", CatalogTable: "pg_extprotocol"}
	TYPE_RELATION = MetadataQueryParams{ObjectType: "RELATION", NameField: "relname", SchemaField: "relnamespace", ACLField: "relacl", OwnerField: "relowner", CatalogTable: "pg_class"}
	TYPE_RESOURCEGROUP = MetadataQueryParams{ObjectType: "RESOURCE GROUP", NameField: "rsgname", OidField: "oid", CatalogTable: "pg_resgroup", Shared: true}
	TYPE_RESOURCEQUEUE = MetadataQueryParams{ObjectType: "RESOURCE QUEUE", NameField: "rsqname", OidField: "oid", CatalogTable: "pg_resqueue", Shared: true}
	TYPE_ROLE = MetadataQueryParams{ObjectType: "ROLE", NameField: "rolname", OidField: "oid", CatalogTable: "pg_authid", Shared: true}
	TYPE_RULE = MetadataQueryParams{ObjectType: "RULE", NameField: "rulename", OidField: "oid", CatalogTable: "pg_rewrite"}
	TYPE_SCHEMA = MetadataQueryParams{ObjectType: "SCHEMA", NameField: "nspname", ACLField: "nspacl", OwnerField: "nspowner", CatalogTable: "pg_namespace"}
	TYPE_TABLESPACE = MetadataQueryParams{ObjectType: "TABLESPACE", NameField: "spcname", ACLField: "spcacl", OwnerField: "spcowner", CatalogTable: "pg_tablespace", Shared: true}
	TYPE_TSCONFIGURATION = MetadataQueryParams{ObjectType: "TEXT SEARCH CONFIGURATION", NameField: "cfgname", OidField: "oid", SchemaField: "cfgnamespace", OwnerField: "cfgowner", CatalogTable: "pg_ts_config"}
	TYPE_TSDICTIONARY = MetadataQueryParams{ObjectType: "TEXT SEARCH DICTIONARY", NameField: "dictname", OidField: "oid", SchemaField: "dictnamespace", OwnerField: "dictowner", CatalogTable: "pg_ts_dict"}
	TYPE_TSPARSER = MetadataQueryParams{ObjectType: "TEXT SEARCH PARSER", NameField: "prsname", OidField: "oid", SchemaField: "prsnamespace", CatalogTable: "pg_ts_parser"}
	TYPE_TSTEMPLATE = MetadataQueryParams{ObjectType: "TEXT SEARCH TEMPLATE", NameField: "tmplname", OidField: "oid", SchemaField: "tmplnamespace", CatalogTable: "pg_ts_template"}
	TYPE_TRIGGER = MetadataQueryParams{ObjectType: "TRIGGER", NameField: "tgname", OidField: "oid", CatalogTable: "pg_trigger"}
	TYPE_TYPE = MetadataQueryParams{ObjectType: "TYPE", NameField: "typname", SchemaField: "typnamespace", OwnerField: "typowner", CatalogTable: "pg_type"}
	if connectionPool.Version.AtLeast("6") {
		TYPE_TYPE.ACLField = "typacl"
	}
}

type MetadataQueryStruct struct {
	UniqueID
	Name                  string
	ObjectType            string
	Schema                string
	Owner                 string
	Comment               string
	Privileges            sql.NullString
	Kind                  string
	SecurityLabel         string
	SecurityLabelProvider string
}

func GetMetadataForObjectType(connectionPool *dbconn.DBConn, params MetadataQueryParams) MetadataMap {
	gplog.Verbose("Getting object type metadata from " + params.CatalogTable)

	tableName := params.CatalogTable
	nameCol := params.NameField
	aclCols := "''"
	kindCol := "''"
	if params.ACLField != "" {
		aclCols = fmt.Sprintf(`CASE
		WHEN %[1]s IS NULL THEN NULL
		WHEN array_upper(%[1]s, 1) = 0 THEN %[1]s[0]
		ELSE unnest(%[1]s) END`, params.ACLField)
		kindCol = fmt.Sprintf(`CASE
		WHEN %[1]s IS NULL THEN ''
		WHEN array_upper(%[1]s, 1) = 0 THEN 'Empty'
		ELSE '' END`, params.ACLField)
	}
	schemaCol := "''"
	joinClause := ""
	filterClause := "1 = 1"
	if params.SchemaField != "" {
		schemaCol = "n.nspname"
		joinClause = fmt.Sprintf(`JOIN pg_namespace n ON o.%s = n.oid`, params.SchemaField)
		filterClause = SchemaFilterClause("n")
	}
	descTable := "pg_description"
	subidFilter := " AND d.objsubid = 0"
	if params.Shared {
		descTable = "pg_shdescription"
		subidFilter = ""
	}
	ownerCol := "''"
	if params.OwnerField != "" {
		ownerCol = fmt.Sprintf("quote_ident(pg_get_userbyid(%s))", params.OwnerField)
	}
	secCols := ""
	if connectionPool.Version.AtLeast("6") {
		secCols = `coalesce(sec.label,'') AS securitylabel,
		coalesce(sec.provider, '') AS securitylabelprovider,`
		secTable := "pg_seclabel"
		secSubidFilter := " AND sec.objsubid = 0"
		if params.Shared {
			secTable = "pg_shseclabel"
			secSubidFilter = ""
		}
		joinClause += fmt.Sprintf(
			` LEFT JOIN %s sec ON (sec.objoid = o.oid AND sec.classoid = '%s'::regclass%s)`,
			secTable, tableName, secSubidFilter)
	}
	if params.FilterClause != "" {
		filterClause += " AND " + params.FilterClause
	}

	query := fmt.Sprintf(`SELECT
		'%s' AS objecttype,
		'%s'::regclass::oid AS classid,
		o.oid,
		quote_ident(%s) AS name,
		%s AS kind,
		coalesce(quote_ident(%s),'') AS schema,
		%s AS owner,
		%s AS privileges,
		%s
		coalesce(description,'') AS comment
	FROM %s o LEFT JOIN %s d ON (d.objoid = o.oid AND d.classoid = '%s'::regclass%s)
		%s
	WHERE %s
	ORDER BY o.oid`,
		params.ObjectType, tableName, nameCol, kindCol, schemaCol, ownerCol, aclCols, secCols,
		tableName, descTable, tableName, subidFilter, joinClause, filterClause)
	results := make([]MetadataQueryStruct, 0)
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)

	return ConstructMetadataMap(results)
}

func sortACLs(privileges []ACL) []ACL {
	sort.Slice(privileges, func(i, j int) bool {
		return privileges[i].Grantee < privileges[j].Grantee
	})
	return privileges
}

func GetCommentsForObjectType(connectionPool *dbconn.DBConn, params MetadataQueryParams) MetadataMap {
	joinStr := ""
	if params.SchemaField != "" {
		joinStr = fmt.Sprintf(`JOIN pg_namespace n ON o.%s = n.oid
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
	SELECT '%s'::regclass::oid AS classid,
		o.%s AS oid,
		coalesce(description,'') AS comment
	FROM %s o
		JOIN %s d ON (d.objoid = %s AND d.classoid = '%s'::regclass%s)
		%s`, params.CatalogTable, params.OidField, params.CatalogTable, descTable,
		params.OidField, commentTable, subidStr, joinStr)

	results := make([]struct {
		UniqueID
		Comment string
	}, 0)
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)

	metadataMap := make(MetadataMap)
	if len(results) > 0 {
		for _, result := range results {
			metadataMap[result.UniqueID] = ObjectMetadata{
				[]ACL{},
				params.ObjectType,
				"",
				result.Comment,
				"",
				""}
		}
	}
	return metadataMap
}

type DefaultPrivilegesQueryStruct struct {
	Oid        uint32
	Owner      string
	Schema     string
	Privileges sql.NullString
	Kind       string
	ObjectType string
}

type DefaultPrivileges struct {
	Owner      string
	Schema     string
	Privileges []ACL
	ObjectType string
}

func (dp DefaultPrivileges) GetMetadataEntry() (string, toc.MetadataEntry) {
	return "postdata",
		toc.MetadataEntry{
			Schema:          dp.Schema,
			Name:            "",
			ObjectType:      "DEFAULT PRIVILEGES",
			ReferenceObject: "",
			StartByte:       0,
			EndByte:         0,
		}
}

func GetDefaultPrivileges(connectionPool *dbconn.DBConn) []DefaultPrivileges {
	query := `
	SELECT a.oid,
		quote_ident(r.rolname) AS owner,
		coalesce(quote_ident(n.nspname),'') AS schema,
		CASE
			WHEN a.defaclacl IS NULL THEN NULL
			WHEN array_upper(a.defaclacl, 1) = 0 THEN a.defaclacl[0]
			ELSE unnest(a.defaclacl)
		END AS privileges,
		CASE
			WHEN a.defaclacl IS NULL THEN ''
			WHEN array_upper(a.defaclacl, 1) = 0 THEN 'Empty'
			ELSE ''
		END AS kind,
		a.defaclobjtype AS objecttype
	FROM pg_default_acl a
		JOIN pg_roles r ON r.oid = a.defaclrole
		LEFT JOIN pg_namespace n ON n.oid = a.defaclnamespace
	ORDER BY n.nspname, a.defaclobjtype, r.rolname`
	results := make([]DefaultPrivilegesQueryStruct, 0)
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)

	return ConstructDefaultPrivileges(results)
}

func getQuotedRoleNames(connectionPool *dbconn.DBConn) map[string]string {
	results := make([]struct {
		RoleName       string
		QuotedRoleName string
	}, 0)
	query := `SELECT rolname AS rolename, quote_ident(rolname) AS quotedrolename FROM pg_authid`
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)
	quotedRoleNames = make(map[string]string)
	for _, result := range results {
		quotedRoleNames[result.RoleName] = result.QuotedRoleName
	}
	return quotedRoleNames
}
