package backup

/*
 * This file contains structs and functions related to executing specific
 * queries to gather metadata for the objects handled in predata_types.go.
 */

import (
	"fmt"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpbackup/utils"
)

/*
 * We don't want to back up the array types that are automatically generated when
 * creating a base type or the base and composite types that are generated when
 * creating a table, so we construct queries to retrieve those types and use them
 * in an EXCEPT clause to exclude them in larger base and composite type retrieval
 * queries that are constructed in their respective functions.
 */
func getTypeQuery(connectionPool *dbconn.DBConn, selectClause string, groupBy string, typeType string) string {
	arrayTypesClause := ""
	if connectionPool.Version.Before("5") {
		/*
		 * In GPDB 4, all automatically-generated array types are guaranteed to be
		 * the name of the corresponding base type prepended with an underscore.
		 */
		arrayTypesClause = fmt.Sprintf(`
%s
WHERE t.typelem != 0
AND length(t.typname) > 1
AND t.typname[0] = '_'
AND substring(t.typname FROM 2) = (
	SELECT
		it.typname
	FROM pg_type it
	WHERE it.oid = t.typelem
)
GROUP BY %s`, selectClause, groupBy)
		/*
		 * In GPDB 5, automatically-generated array types are NOT guaranteed to be
		 * the name of the corresponding base type prepended with an underscore, as
		 * the array name may differ due to length issues, collisions, or the like.
		 * However, pg_type now has a typarray field giving the OID of the array
		 * type corresponding to a given base type, so that can be used instead.
		 */
	} else {
		arrayTypesClause = fmt.Sprintf(`
%s
WHERE t.typelem != 0
AND t.oid = (
	SELECT
		it.typarray
	FROM pg_type it
	WHERE it.oid = t.typelem
)
GROUP BY %s`, selectClause, groupBy)
	}
	/*
	 * In both GPDB 4 and GPDB 5, we can get the list of base and composite types
	 * created along with a table by joining typrelid in pg_type with pg_class
	 * and checking whether it refers to an actual relation or just a dummy entry
	 * for use with pg_attribute.
	 */
	tableTypesClause := fmt.Sprintf(`
%s
AND %s
JOIN pg_class c ON t.typrelid = c.oid AND c.relkind IN ('f', 'r', 'S', 'v')
GROUP BY %s
UNION ALL
%s
JOIN pg_type it ON t.typelem = it.oid
JOIN pg_class c ON it.typrelid = c.oid AND c.relkind IN ('f', 'r', 'S', 'v')
GROUP BY %s`, selectClause, ExtensionFilterClause("t"), groupBy, selectClause, groupBy)
	return fmt.Sprintf(`
%s
WHERE %s
AND t.typtype = '%s'
AND %s
GROUP BY %s
EXCEPT (
%s
UNION ALL
%s
)
ORDER BY schema, name;`, selectClause, SchemaFilterClause("n"), typeType, ExtensionFilterClause("t"), groupBy, arrayTypesClause, tableTypesClause)
}

func GetTypeMetadataEntry(schema string, name string) (string, utils.MetadataEntry) {
	return "predata",
		utils.MetadataEntry{
			Schema:          schema,
			Name:            name,
			ObjectType:      "TYPE",
			ReferenceObject: "",
			StartByte:       0,
			EndByte:         0,
		}
}

type BaseType struct {
	Oid             uint32
	Schema          string
	Name            string
	Type            string `db:"typtype"`
	Input           string `db:"typinput"`
	Output          string `db:"typoutput"`
	Receive         string
	Send            string
	ModIn           string
	ModOut          string
	InternalLength  int  `db:"typlen"`
	IsPassedByValue bool `db:"typbyval"`
	Alignment       string
	Storage         string `db:"typstorage"`
	DefaultVal      string
	Element         string
	Category        string `db:"typcategory"`
	Preferred       bool   `db:"typispreferred"`
	Delimiter       string `db:"typdelim"`
	StorageOptions  string
	Collatable      bool
	Collation       string
}

func (t BaseType) GetMetadataEntry() (string, utils.MetadataEntry) {
	return GetTypeMetadataEntry(t.Schema, t.Name)
}

func (t BaseType) GetUniqueID() UniqueID {
	return UniqueID{ClassID: PG_TYPE_OID, Oid: t.Oid}
}

func (t BaseType) FQN() string {
	return utils.MakeFQN(t.Schema, t.Name)
}

func GetBaseTypes(connectionPool *dbconn.DBConn) []BaseType {
	typeModClause := ""
	if connectionPool.Version.Before("5") {
		typeModClause = `t.typreceive AS receive,
	t.typsend AS send,`
	} else {
		typeModClause = `CASE WHEN t.typreceive = '-'::regproc THEN '' ELSE t.typreceive::regproc::text END AS receive,
	CASE WHEN t.typsend = '-'::regproc THEN '' ELSE t.typsend::regproc::text END AS send,
	CASE WHEN t.typmodin = '-'::regproc THEN '' ELSE t.typmodin::regproc::text END AS modin,
	CASE WHEN t.typmodout = '-'::regproc THEN '' ELSE t.typmodout::regproc::text END AS modout,`
	}

	typeCategoryClause := ""
	typeCollatableClause := ""
	if connectionPool.Version.Before("6") {
		typeCategoryClause = "'U' AS typcategory,"
	} else {
		typeCategoryClause = "t.typcategory, t.typispreferred,"
		typeCollatableClause = "(t.typcollation <> 0) AS collatable,"
	}
	selectClause := fmt.Sprintf(`
SELECT
	t.oid,
	quote_ident(n.nspname) AS schema,
	quote_ident(t.typname) AS name,
	t.typtype,
	t.typinput,
	t.typoutput,
	%s
	t.typlen,
	t.typbyval,
	CASE WHEN t.typalign = '-' THEN '' ELSE t.typalign END AS alignment,
	t.typstorage,
	coalesce(t.typdefault, '') AS defaultval,
	CASE WHEN t.typelem != 0::regproc THEN pg_catalog.format_type(t.typelem, NULL) ELSE '' END AS element,
	%s
	t.typdelim,
	%s
	coalesce(array_to_string(typoptions, ', '), '') AS storageoptions
FROM pg_type t
JOIN pg_namespace n ON t.typnamespace = n.oid
LEFT JOIN pg_type_encoding e ON t.oid = e.typid`, typeModClause, typeCategoryClause, typeCollatableClause)
	groupBy := "t.oid, schema, name, t.typtype, t.typinput, t.typoutput, receive, send,%st.typlen, t.typbyval, alignment, t.typstorage, defaultval, element, t.typdelim, storageoptions"
	if connectionPool.Version.Is("4") {
		groupBy = fmt.Sprintf(groupBy, " ")
	} else if connectionPool.Version.Is("5") {
		groupBy = fmt.Sprintf(groupBy, " modin, modout, ")
	} else {
		groupBy = fmt.Sprintf(groupBy, " modin, modout, t.typcategory, t.typispreferred, t.typcollation, ")

	}
	query := getTypeQuery(connectionPool, selectClause, groupBy, "b")

	results := make([]BaseType, 0)
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)
	/*
	 * GPDB 4.3 has no built-in regproc-to-text cast and uses "-" in place of
	 * NULL for several fields, so to avoid dealing with hyphens later on we
	 * replace those with empty strings here.
	 */
	if connectionPool.Version.Before("5") {
		for i := range results {
			if results[i].Send == "-" {
				results[i].Send = ""
			}
			if results[i].Receive == "-" {
				results[i].Receive = ""
			}
		}
	}
	return results
}

type CompositeType struct {
	Oid        uint32
	Schema     string
	Name       string
	Attributes []Attribute
}

func (t CompositeType) GetMetadataEntry() (string, utils.MetadataEntry) {
	return GetTypeMetadataEntry(t.Schema, t.Name)
}

func (t CompositeType) GetUniqueID() UniqueID {
	return UniqueID{ClassID: PG_TYPE_OID, Oid: t.Oid}
}

func (t CompositeType) FQN() string {
	return utils.MakeFQN(t.Schema, t.Name)
}

func GetCompositeTypes(connectionPool *dbconn.DBConn) []CompositeType {
	selectClause := `
SELECT
	t.oid,
	quote_ident(n.nspname) AS schema,
	quote_ident(t.typname) AS name
FROM pg_type t
JOIN pg_namespace n ON t.typnamespace = n.oid`
	groupBy := "t.oid, schema, name, t.typtype"
	query := getTypeQuery(connectionPool, selectClause, groupBy, "c")

	compTypes := make([]CompositeType, 0)
	err := connectionPool.Select(&compTypes, query)
	gplog.FatalOnError(err)

	attributeMap := getCompositeTypeAttributes(connectionPool)

	for i, compType := range compTypes {
		compTypes[i].Attributes = attributeMap[compType.Oid]
	}
	return compTypes
}

type Attribute struct {
	CompositeTypeOid uint32
	Name             string
	Type             string
	Comment          string
	Collation        string
}

func getCompositeTypeAttributes(connectionPool *dbconn.DBConn) map[uint32][]Attribute {
	before6query := `SELECT
	t.oid AS compositetypeoid,
	quote_ident(a.attname) AS name,
	pg_catalog.format_type(a.atttypid, a.atttypmod) AS type,
	coalesce(quote_literal(d.description),'') AS comment
	FROM pg_type t
	JOIN pg_attribute a ON t.typrelid = a.attrelid
	LEFT JOIN pg_description d ON (d.objoid = a.attrelid AND d.classoid = 'pg_class'::regclass AND d.objsubid = a.attnum)
	WHERE t.typtype = 'c'
	ORDER BY t.oid, a.attnum;`

	masterQuery := `SELECT
	t.oid AS compositetypeoid,
	quote_ident(a.attname) AS name,
	pg_catalog.format_type(a.atttypid, a.atttypmod) AS type,
	CASE
		WHEN at.typcollation <> a.attcollation
		THEN quote_ident(cn.nspname) || '.' || quote_ident(coll.collname)
		ELSE ''
	END AS collation,
	coalesce(quote_literal(d.description),'') AS comment
	FROM pg_type t
	JOIN pg_attribute a ON t.typrelid = a.attrelid
	LEFT JOIN pg_type at ON at.oid = a.atttypid
	LEFT JOIN pg_collation coll ON a.attcollation = coll.oid
	LEFT JOIN pg_namespace cn on (coll.collnamespace = cn.oid)
	LEFT JOIN pg_description d ON (d.objoid = a.attrelid AND d.classoid = 'pg_class'::regclass AND d.objsubid = a.attnum)
	WHERE t.typtype = 'c'
	ORDER BY t.oid, a.attnum;`

	results := make([]Attribute, 0)
	var err error
	if connectionPool.Version.Before("6") {
		err = connectionPool.Select(&results, before6query)
	} else {
		err = connectionPool.Select(&results, masterQuery)
	}
	gplog.FatalOnError(err)

	attributeMap := make(map[uint32][]Attribute, 0)

	for _, att := range results {
		attributeMap[att.CompositeTypeOid] = append(attributeMap[att.CompositeTypeOid], att)
	}
	return attributeMap
}

type Domain struct {
	Oid        uint32
	Schema     string
	Name       string
	DefaultVal string
	Collation  string
	BaseType   string
	NotNull    bool
}

func (t Domain) GetMetadataEntry() (string, utils.MetadataEntry) {
	return "predata",
		utils.MetadataEntry{
			Schema:          t.Schema,
			Name:            t.Name,
			ObjectType:      "DOMAIN",
			ReferenceObject: "",
			StartByte:       0,
			EndByte:         0,
		}
}

func (t Domain) GetUniqueID() UniqueID {
	return UniqueID{ClassID: PG_TYPE_OID, Oid: t.Oid}
}

func (t Domain) FQN() string {
	return utils.MakeFQN(t.Schema, t.Name)
}

func GetDomainTypes(connectionPool *dbconn.DBConn) []Domain {
	results := make([]Domain, 0)
	before6query := fmt.Sprintf(`
SELECT
	t.oid,
	quote_ident(n.nspname) AS schema,
	quote_ident(t.typname) AS name,
	coalesce(t.typdefault, '') AS defaultval,
	format_type(t.typbasetype, t.typtypmod) AS basetype,
	t.typnotnull AS notnull
FROM pg_type t
JOIN pg_namespace n ON t.typnamespace = n.oid
WHERE %s
AND t.typtype = 'd'
AND %s
ORDER BY n.nspname, t.typname;`, SchemaFilterClause("n"), ExtensionFilterClause("t"))

	masterQuery := fmt.Sprintf(`
SELECT
	t.oid,
	quote_ident(n.nspname) AS schema,
	quote_ident(t.typname) AS name,
	coalesce(t.typdefault, '') AS defaultval,
	CASE WHEN t.typcollation <> u.typcollation THEN quote_ident(cn.nspname) || '.' || quote_ident(c.collname) ELSE '' END AS collation,
	format_type(t.typbasetype, t.typtypmod) AS basetype,
	t.typnotnull AS notnull
FROM pg_type t
JOIN pg_namespace n ON t.typnamespace = n.oid
LEFT JOIN pg_type u ON (t.typbasetype = u.oid)
LEFT JOIN pg_collation c on (t.typcollation = c.oid)
LEFT JOIN pg_namespace cn on (c.collnamespace = cn.oid)
WHERE %s
AND t.typtype = 'd'
AND %s
ORDER BY n.nspname, t.typname;`, SchemaFilterClause("n"), ExtensionFilterClause("t"))
	var err error

	if connectionPool.Version.Before("6") {
		err = connectionPool.Select(&results, before6query)
	} else {
		err = connectionPool.Select(&results, masterQuery)
	}

	gplog.FatalOnError(err)
	return results
}

type EnumType struct {
	Oid        uint32
	Schema     string
	Name       string
	EnumLabels string
}

func (t EnumType) GetMetadataEntry() (string, utils.MetadataEntry) {
	return GetTypeMetadataEntry(t.Schema, t.Name)
}

func (t EnumType) GetUniqueID() UniqueID {
	return UniqueID{ClassID: PG_TYPE_OID, Oid: t.Oid}
}

func (t EnumType) FQN() string {
	return utils.MakeFQN(t.Schema, t.Name)
}

func GetEnumTypes(connectionPool *dbconn.DBConn) []EnumType {
	enumSortClause := "ORDER BY e.enumsortorder"
	if connectionPool.Version.Is("5") {
		enumSortClause = "ORDER BY e.oid"
	}
	query := fmt.Sprintf(`
SELECT
	t.oid,
	quote_ident(n.nspname) AS schema,
	quote_ident(t.typname) AS name,
	enumlabels
FROM pg_type t
LEFT JOIN pg_namespace n ON t.typnamespace = n.oid
LEFT JOIN (
	  SELECT e.enumtypid,string_agg(quote_literal(e.enumlabel), E',\n\t' %s) AS enumlabels FROM pg_enum e GROUP BY enumtypid
	) e ON t.oid = e.enumtypid
WHERE %s
AND t.typtype = 'e'
AND %s
ORDER BY n.nspname, t.typname;`, enumSortClause, SchemaFilterClause("n"), ExtensionFilterClause("t"))

	results := make([]EnumType, 0)
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)
	return results
}

type RangeType struct {
	Oid            uint32
	Schema         string
	Name           string
	SubType        string
	Collation      string
	SubTypeOpClass string
	Canonical      string
	SubTypeDiff    string
}

func (t RangeType) GetMetadataEntry() (string, utils.MetadataEntry) {
	return GetTypeMetadataEntry(t.Schema, t.Name)
}

func (t RangeType) GetUniqueID() UniqueID {
	return UniqueID{ClassID: PG_TYPE_OID, Oid: t.Oid}
}

func (t RangeType) FQN() string {
	return utils.MakeFQN(t.Schema, t.Name)
}

func GetRangeTypes(connectionPool *dbconn.DBConn) []RangeType {
	query := fmt.Sprintf(`
SELECT
	t.oid,
	quote_ident(n.nspname) AS schema,
	quote_ident(t.typname) AS name,
	format_type(st.oid, st.typtypmod) AS subtype,
	CASE
		WHEN c.collname IS NULL THEN ''
		ELSE quote_ident(nc.nspname) || '.' || quote_ident(c.collname)
	END AS collation,
	CASE
		WHEN opc.opcname IS NULL THEN ''
		ELSE quote_ident(nopc.nspname) || '.' || quote_ident(opc.opcname)
	END AS subtypeopclass,
	CASE
		WHEN r.rngcanonical = '-'::regproc THEN ''
		ELSE r.rngcanonical::regproc::text
	END AS canonical,
	CASE
		WHEN r.rngsubdiff = '-'::regproc THEN ''
		ELSE r.rngsubdiff::regproc::text
	END AS subtypediff
FROM pg_range r
JOIN pg_type t ON t.oid = r.rngtypid
JOIN pg_namespace n ON t.typnamespace = n.oid
JOIN pg_type st ON st.oid = r.rngsubtype
LEFT JOIN pg_collation c ON c.oid = r.rngcollation
LEFT JOIN pg_namespace nc ON nc.oid = c.collnamespace
LEFT JOIN pg_opclass opc ON opc.oid = r.rngsubopc
LEFT JOIN pg_namespace nopc ON nopc.oid = opc.opcnamespace
WHERE %s
AND t.typtype = 'r'
AND %s;`, SchemaFilterClause("n"), ExtensionFilterClause("t"))

	results := make([]RangeType, 0)
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)
	return results
}

type ShellType struct {
	Oid    uint32
	Schema string
	Name   string
}

func (t ShellType) GetMetadataEntry() (string, utils.MetadataEntry) {
	return GetTypeMetadataEntry(t.Schema, t.Name)
}

func (t ShellType) GetUniqueID() UniqueID {
	return UniqueID{ClassID: PG_TYPE_OID, Oid: t.Oid}
}

func (t ShellType) FQN() string {
	return utils.MakeFQN(t.Schema, t.Name)
}

func GetShellTypes(connectionPool *dbconn.DBConn) []ShellType {
	query := fmt.Sprintf(`
SELECT
	t.oid,
	quote_ident(n.nspname) AS schema,
	quote_ident(t.typname) AS name
FROM pg_type t
JOIN pg_namespace n ON t.typnamespace = n.oid
WHERE %s
AND t.typtype = 'p'
AND %s
ORDER BY n.nspname, t.typname;`, SchemaFilterClause("n"), ExtensionFilterClause("t"))

	results := make([]ShellType, 0)
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)
	return results
}

type Collation struct {
	Oid     uint32
	Schema  string
	Name    string
	Collate string
	Ctype   string
}

func (c Collation) GetMetadataEntry() (string, utils.MetadataEntry) {
	return "predata",
		utils.MetadataEntry{
			Schema:          c.Schema,
			Name:            c.Name,
			ObjectType:      "COLLATION",
			ReferenceObject: "",
			StartByte:       0,
			EndByte:         0,
		}
}

func (c Collation) GetUniqueID() UniqueID {
	return UniqueID{ClassID: PG_COLLATION_OID, Oid: c.Oid}
}

func (c Collation) FQN() string {
	return utils.MakeFQN(c.Schema, c.Name)
}

func GetCollations(connectionPool *dbconn.DBConn) []Collation {
	results := make([]Collation, 0)

	query := fmt.Sprintf(`
SELECT
	c.oid,
	quote_ident(n.nspname) as schema,
	quote_ident(c.collname) as name,
	c.collcollate as collate,
	c.collctype as ctype
FROM pg_collation c
JOIN pg_namespace n ON c.collnamespace = n.oid
WHERE %s`, SchemaFilterClause("n"))

	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)
	return results
}
