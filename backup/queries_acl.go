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
	TYPE_ACCESS_METHOD        MetadataQueryParams
	TYPE_AGGREGATE            MetadataQueryParams
	TYPE_CAST                 MetadataQueryParams
	TYPE_COLLATION            MetadataQueryParams
	TYPE_CONSTRAINT           MetadataQueryParams
	TYPE_CONVERSION           MetadataQueryParams
	TYPE_DATABASE             MetadataQueryParams
	TYPE_EVENT_TRIGGER        MetadataQueryParams
	TYPE_EXTENSION            MetadataQueryParams
	TYPE_FOREIGN_DATA_WRAPPER MetadataQueryParams
	TYPE_FOREIGN_SERVER       MetadataQueryParams
	TYPE_FUNCTION             MetadataQueryParams
	TYPE_INDEX                MetadataQueryParams
	TYPE_PROC_LANGUAGE        MetadataQueryParams
	TYPE_TRANSFORM            MetadataQueryParams
	TYPE_OPERATOR             MetadataQueryParams
	TYPE_OPERATOR_CLASS       MetadataQueryParams
	TYPE_OPERATOR_FAMILY      MetadataQueryParams
	TYPE_PROTOCOL             MetadataQueryParams
	TYPE_RELATION             MetadataQueryParams
	TYPE_RESOURCE_GROUP       MetadataQueryParams
	TYPE_RESOURCE_QUEUE       MetadataQueryParams
	TYPE_ROLE                 MetadataQueryParams
	TYPE_RULE                 MetadataQueryParams
	TYPE_SCHEMA               MetadataQueryParams
	TYPE_STATISTIC_EXT        MetadataQueryParams
	TYPE_TABLESPACE           MetadataQueryParams
	TYPE_TS_CONFIGURATION     MetadataQueryParams
	TYPE_TS_DICTIONARY        MetadataQueryParams
	TYPE_TS_PARSER            MetadataQueryParams
	TYPE_TS_TEMPLATE          MetadataQueryParams
	TYPE_TRIGGER              MetadataQueryParams
	TYPE_TYPE                 MetadataQueryParams
	TYPE_POLICY               MetadataQueryParams
)

func InitializeMetadataParams(connectionPool *dbconn.DBConn) {
	TYPE_ACCESS_METHOD = MetadataQueryParams{ObjectType: toc.OBJ_ACCESS_METHOD, NameField: "amname", OidField: "oid", CatalogTable: "pg_am"}
	TYPE_AGGREGATE = MetadataQueryParams{ObjectType: toc.OBJ_AGGREGATE, NameField: "proname", SchemaField: "pronamespace", ACLField: "proacl", OwnerField: "proowner", CatalogTable: "pg_proc"}
	if connectionPool.Version.AtLeast("7") {
		TYPE_AGGREGATE.FilterClause = "prokind = 'a'"
	} else {
		TYPE_AGGREGATE.FilterClause = "proisagg = 't'"
	}
	TYPE_CAST = MetadataQueryParams{ObjectType: toc.OBJ_CAST, NameField: "typname", OidField: "oid", OidTable: "pg_type", CatalogTable: "pg_cast"}
	TYPE_COLLATION = MetadataQueryParams{ObjectType: toc.OBJ_COLLATION, NameField: "collname", OidField: "oid", SchemaField: "collnamespace", OwnerField: "collowner", CatalogTable: "pg_collation"}
	TYPE_CONSTRAINT = MetadataQueryParams{ObjectType: toc.OBJ_CONSTRAINT, NameField: "conname", SchemaField: "connamespace", OidField: "oid", CatalogTable: "pg_constraint"}
	TYPE_CONVERSION = MetadataQueryParams{ObjectType: toc.OBJ_CONVERSION, NameField: "conname", OidField: "oid", SchemaField: "connamespace", OwnerField: "conowner", CatalogTable: "pg_conversion"}
	TYPE_DATABASE = MetadataQueryParams{ObjectType: toc.OBJ_DATABASE, NameField: "datname", ACLField: "datacl", OwnerField: "datdba", CatalogTable: "pg_database", Shared: true}
	TYPE_EVENT_TRIGGER = MetadataQueryParams{ObjectType: toc.OBJ_EVENT_TRIGGER, NameField: "evtname", OidField: "oid", OwnerField: "evtowner", CatalogTable: "pg_event_trigger"}
	TYPE_EXTENSION = MetadataQueryParams{ObjectType: toc.OBJ_EXTENSION, NameField: "extname", OidField: "oid", CatalogTable: "pg_extension"}
	TYPE_FOREIGN_DATA_WRAPPER = MetadataQueryParams{ObjectType: toc.OBJ_FOREIGN_DATA_WRAPPER, NameField: "fdwname", ACLField: "fdwacl", OwnerField: "fdwowner", CatalogTable: "pg_foreign_data_wrapper"}
	TYPE_FOREIGN_SERVER = MetadataQueryParams{ObjectType: toc.OBJ_SERVER, NameField: "srvname", ACLField: "srvacl", OwnerField: "srvowner", CatalogTable: "pg_foreign_server"}
	TYPE_FUNCTION = MetadataQueryParams{ObjectType: toc.OBJ_FUNCTION, NameField: "proname", SchemaField: "pronamespace", ACLField: "proacl", OwnerField: "proowner", CatalogTable: "pg_proc"}
	if connectionPool.Version.AtLeast("7") {
		TYPE_FUNCTION.FilterClause = "prokind <> 'a'"
	} else {
		TYPE_FUNCTION.FilterClause = "proisagg = 'f'"
	}
	TYPE_INDEX = MetadataQueryParams{ObjectType: toc.OBJ_INDEX, NameField: "relname", OidField: "indexrelid", OidTable: "pg_class", CommentTable: "pg_class", CatalogTable: "pg_index"}
	TYPE_PROC_LANGUAGE = MetadataQueryParams{ObjectType: toc.OBJ_LANGUAGE, NameField: "lanname", ACLField: "lanacl", CatalogTable: "pg_language"}
	TYPE_PROC_LANGUAGE.OwnerField = "lanowner"
	TYPE_OPERATOR = MetadataQueryParams{ObjectType: toc.OBJ_OPERATOR, NameField: "oprname", SchemaField: "oprnamespace", OidField: "oid", OwnerField: "oprowner", CatalogTable: "pg_operator"}
	TYPE_OPERATOR_CLASS = MetadataQueryParams{ObjectType: toc.OBJ_OPERATOR_CLASS, NameField: "opcname", SchemaField: "opcnamespace", OidField: "oid", OwnerField: "opcowner", CatalogTable: "pg_opclass"}
	TYPE_OPERATOR_FAMILY = MetadataQueryParams{ObjectType: toc.OBJ_OPERATOR_FAMILY, NameField: "opfname", SchemaField: "opfnamespace", OidField: "oid", OwnerField: "opfowner", CatalogTable: "pg_opfamily"}
	TYPE_PROTOCOL = MetadataQueryParams{ObjectType: toc.OBJ_PROTOCOL, NameField: "ptcname", ACLField: "ptcacl", OwnerField: "ptcowner", CatalogTable: "pg_extprotocol"}
	TYPE_RELATION = MetadataQueryParams{ObjectType: toc.OBJ_RELATION, NameField: "relname", SchemaField: "relnamespace", ACLField: "relacl", OwnerField: "relowner", CatalogTable: "pg_class"}
	TYPE_RESOURCE_GROUP = MetadataQueryParams{ObjectType: toc.OBJ_RESOURCE_GROUP, NameField: "rsgname", OidField: "oid", CatalogTable: "pg_resgroup", Shared: true}
	TYPE_RESOURCE_QUEUE = MetadataQueryParams{ObjectType: toc.OBJ_RESOURCE_QUEUE, NameField: "rsqname", OidField: "oid", CatalogTable: "pg_resqueue", Shared: true}
	TYPE_ROLE = MetadataQueryParams{ObjectType: toc.OBJ_ROLE, NameField: "rolname", OidField: "oid", CatalogTable: "pg_authid", Shared: true}
	TYPE_RULE = MetadataQueryParams{ObjectType: toc.OBJ_RULE, NameField: "rulename", OidField: "oid", CatalogTable: "pg_rewrite"}
	TYPE_SCHEMA = MetadataQueryParams{ObjectType: toc.OBJ_SCHEMA, NameField: "nspname", ACLField: "nspacl", OwnerField: "nspowner", CatalogTable: "pg_namespace"}
	TYPE_STATISTIC_EXT = MetadataQueryParams{ObjectType: toc.OBJ_STATISTICS_EXT, NameField: "stxname", OwnerField: "stxowner", CatalogTable: "pg_statistic_ext"}
	TYPE_TABLESPACE = MetadataQueryParams{ObjectType: toc.OBJ_TABLESPACE, NameField: "spcname", ACLField: "spcacl", OwnerField: "spcowner", CatalogTable: "pg_tablespace", Shared: true}
	TYPE_TRANSFORM = MetadataQueryParams{ObjectType: toc.OBJ_TRANSFORM, CatalogTable: "pg_transform"}
	TYPE_TS_CONFIGURATION = MetadataQueryParams{ObjectType: toc.OBJ_TEXT_SEARCH_CONFIGURATION, NameField: "cfgname", OidField: "oid", SchemaField: "cfgnamespace", OwnerField: "cfgowner", CatalogTable: "pg_ts_config"}
	TYPE_TS_DICTIONARY = MetadataQueryParams{ObjectType: toc.OBJ_TEXT_SEARCH_DICTIONARY, NameField: "dictname", OidField: "oid", SchemaField: "dictnamespace", OwnerField: "dictowner", CatalogTable: "pg_ts_dict"}
	TYPE_TS_PARSER = MetadataQueryParams{ObjectType: toc.OBJ_TEXT_SEARCH_PARSER, NameField: "prsname", OidField: "oid", SchemaField: "prsnamespace", CatalogTable: "pg_ts_parser"}
	TYPE_TS_TEMPLATE = MetadataQueryParams{ObjectType: toc.OBJ_TEXT_SEARCH_TEMPLATE, NameField: "tmplname", OidField: "oid", SchemaField: "tmplnamespace", CatalogTable: "pg_ts_template"}
	TYPE_TRIGGER = MetadataQueryParams{ObjectType: toc.OBJ_TRIGGER, NameField: "tgname", OidField: "oid", CatalogTable: "pg_trigger"}
	TYPE_TYPE = MetadataQueryParams{ObjectType: toc.OBJ_TYPE, NameField: "typname", SchemaField: "typnamespace", OwnerField: "typowner", CatalogTable: "pg_type"}
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
	nameCol := "''"
	if params.NameField != "" {
		nameCol = params.NameField
	}
	aclCols := "''"
	kindCol := "''"
	aclLateralJoin := ""
	if params.ACLField != "" {
		// Cannot use unnest() in CASE statements anymore in GPDB 7+ so convert
		// it to a LEFT JOIN LATERAL. We do not use LEFT JOIN LATERAL for GPDB 6
		// because the CASE unnest() logic is more performant.
		if connectionPool.Version.AtLeast("7") {
			aclLateralJoin = fmt.Sprintf(
				`LEFT JOIN LATERAL unnest(o.%[1]s) ljl_unnest ON o.%[1]s IS NOT NULL AND array_length(o.%[1]s, 1) != 0`, params.ACLField)
			aclCols = "ljl_unnest"
		} else {
			aclCols = fmt.Sprintf(`CASE
			WHEN %[1]s IS NULL THEN NULL
			WHEN array_upper(%[1]s, 1) = 0 THEN %[1]s[0]
			ELSE unnest(%[1]s) END`, params.ACLField)
		}

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
		coalesce(quote_ident(%s),'') AS name,
		%s AS kind,
		coalesce(quote_ident(%s),'') AS schema,
		%s AS owner,
		%s AS privileges,
		%s
		coalesce(description,'') AS comment
	FROM %s o LEFT JOIN %s d ON (d.objoid = o.oid AND d.classoid = '%s'::regclass%s)
		%s
		%s
	WHERE %s
	ORDER BY o.oid`,
		params.ObjectType, tableName, nameCol, kindCol, schemaCol, ownerCol, aclCols, secCols,
		tableName, descTable, tableName, subidFilter, joinClause, aclLateralJoin, filterClause)
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
	selectClause := fmt.Sprintf(`
	SELECT '%s'::regclass::oid AS classid,
		o.%s AS oid,
		coalesce(description,'') AS comment
		`, params.CatalogTable, params.OidField)

	joinStr := ""
	if params.SchemaField != "" {
		joinStr = fmt.Sprintf(`JOIN pg_namespace n ON o.%s = n.oid`, params.SchemaField)
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

	fromClause := fmt.Sprintf(`
	FROM %s o
		JOIN %s d ON (d.objoid = %s AND d.classoid = '%s'::regclass%s)
		%s`, params.CatalogTable, descTable, params.OidField, commentTable,
		subidStr, joinStr)

	whereClause := fmt.Sprintf("\nWHERE o.%s >= %d", params.OidField, FIRST_NORMAL_OBJECT_ID)
	if params.SchemaField != "" {
		whereClause += " AND " + SchemaFilterClause("n")
	}

	results := make([]struct {
		UniqueID
		Comment string
	}, 0)
	query := selectClause + fromClause + whereClause
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
	// Cannot use unnest() in CASE statements anymore in GPDB 7+ so convert
	// it to a LEFT JOIN LATERAL. We do not use LEFT JOIN LATERAL for GPDB 6
	// because the CASE unnest() logic is more performant.
	aclCols := "''"
	aclLateralJoin := ""
	if connectionPool.Version.AtLeast("7") {
		aclLateralJoin =
			`LEFT JOIN LATERAL unnest(a.defaclacl) ljl_unnest ON a.defaclacl IS NOT NULL AND array_length(a.defaclacl, 1) != 0`
		aclCols = "ljl_unnest"
	} else {
		aclCols = `CASE
			WHEN a.defaclacl IS NULL THEN NULL
			WHEN array_upper(a.defaclacl, 1) = 0 THEN a.defaclacl[0]
			ELSE unnest(a.defaclacl) END`
	}

	query := fmt.Sprintf(`
	SELECT a.oid,
		quote_ident(r.rolname) AS owner,
		coalesce(quote_ident(n.nspname),'') AS schema,
		%s AS privileges,
		CASE
			WHEN a.defaclacl IS NULL THEN ''
			WHEN array_upper(a.defaclacl, 1) = 0 THEN 'Empty'
			ELSE ''
		END AS kind,
		a.defaclobjtype AS objecttype
	FROM pg_default_acl a
		JOIN pg_roles r ON r.oid = a.defaclrole
		LEFT JOIN pg_namespace n ON n.oid = a.defaclnamespace
		%s
	ORDER BY n.nspname, a.defaclobjtype, r.rolname`,
		aclCols, aclLateralJoin)
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
