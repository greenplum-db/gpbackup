package backup

/*
 * This file contains structs and functions related to executing specific
 * queries to gather metadata for the objects handled in predata_types.go.
 */

import (
	"fmt"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/lib/pq"
)

/*
 * We don't want to back up the array types that are automatically generated when
 * creating a base type or the base and composite types that are generated when
 * creating a table, so we construct queries to retrieve those types and use them
 * in an EXCEPT clause to exclude them in larger base and composite type retrieval
 * queries that are constructed in their respective functions.
 */
func getTypeQuery(connection *dbconn.DBConn, selectClause string, groupBy string, typeType string) string {
	arrayTypesClause := ""
	if connection.Version.Before("5") {
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
JOIN pg_class c ON t.typrelid = c.oid AND c.relkind IN ('r', 'S', 'v')
GROUP BY %s
UNION ALL
%s
JOIN pg_type it ON t.typelem = it.oid
JOIN pg_class c ON it.typrelid = c.oid AND c.relkind IN ('r', 'S', 'v')
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

type Type struct {
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
	EnumLabels      string
	BaseType        string
	NotNull         bool `db:"typnotnull"`
	Attributes      pq.StringArray
	DependsUpon     []string
	StorageOptions  string
	Collatable      bool
	Collation       string
}

func GetBaseTypes(connection *dbconn.DBConn) []Type {
	typeModClause := ""
	if connection.Version.Before("5") {
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
	if connection.Version.Before("6") {
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
	if connection.Version.Is("4") {
		groupBy = fmt.Sprintf(groupBy, " ")
	} else if connection.Version.Is("5") {
		groupBy = fmt.Sprintf(groupBy, " modin, modout, ")
	} else {
		groupBy = fmt.Sprintf(groupBy, " modin, modout, t.typcategory, t.typispreferred, t.typcollation, ")

	}
	query := getTypeQuery(connection, selectClause, groupBy, "b")

	results := make([]Type, 0)
	err := connection.Select(&results, query)
	gplog.FatalOnError(err)
	/*
	 * GPDB 4.3 has no built-in regproc-to-text cast and uses "-" in place of
	 * NULL for several fields, so to avoid dealing with hyphens later on we
	 * replace those with empty strings here.
	 */
	if connection.Version.Before("5") {
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

func GetCompositeTypes(connection *dbconn.DBConn) []Type {
	selectClause := `
SELECT
	t.oid,
	quote_ident(n.nspname) AS schema,
	quote_ident(t.typname) AS name,
	t.typtype
FROM pg_type t
JOIN pg_namespace n ON t.typnamespace = n.oid`
	groupBy := "t.oid, schema, name, t.typtype"
	query := getTypeQuery(connection, selectClause, groupBy, "c")

	compTypes := make([]Type, 0)
	err := connection.Select(&compTypes, query)
	gplog.FatalOnError(err)

	attributeMap := getCompositeTypeAttributes(connection)

	for i, compType := range compTypes {
		compTypes[i].Attributes = attributeMap[compType.Oid]
	}
	return compTypes
}

type Attributes struct {
	TypeOid    uint32
	Attributes pq.StringArray
}

func getCompositeTypeAttributes(connection *dbconn.DBConn) map[uint32]pq.StringArray {
	query := `
SELECT
	t.oid AS typeoid,
	array_agg(E'\t' || quote_ident(a.attname) || ' ' || pg_catalog.format_type(a.atttypid, a.atttypmod) ORDER BY a.attnum) AS attributes
FROM pg_type t
JOIN pg_attribute a ON t.typrelid = a.attrelid
WHERE t.typtype = 'c'
GROUP BY t.oid`

	results := make([]Attributes, 0)
	err := connection.Select(&results, query)
	gplog.FatalOnError(err)

	attributeMap := make(map[uint32]pq.StringArray, 0)
	for _, att := range results {
		attributeMap[att.TypeOid] = att.Attributes
	}
	return attributeMap
}

func GetDomainTypes(connection *dbconn.DBConn) []Type {
	results := make([]Type, 0)
	version4query := fmt.Sprintf(`
SELECT
	t.oid,
	quote_ident(n.nspname) AS schema,
	quote_ident(t.typname) AS name,
	t.typtype,
	coalesce(t.typdefault, '') AS defaultval,
	format_type(t.typbasetype, t.typtypmod) AS basetype,
	t.typnotnull
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
	t.typtype,
	coalesce(t.typdefault, '') AS defaultval,
	CASE WHEN t.typcollation <> u.typcollation THEN quote_ident(cn.nspname) || '.' || quote_ident(c.collname) ELSE '' END AS collation,
	format_type(t.typbasetype, t.typtypmod) AS basetype,
	t.typnotnull
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

	if connection.Version.Before("6") {
		err = connection.Select(&results, version4query)
	} else {
		err = connection.Select(&results, masterQuery)
	}

	gplog.FatalOnError(err)
	return results
}

func GetEnumTypes(connection *dbconn.DBConn) []Type {
	enumSortClause := "ORDER BY e.enumsortorder"
	if connection.Version.Is("5") {
		enumSortClause = "ORDER BY e.oid"
	}
	query := fmt.Sprintf(`
SELECT
	t.oid,
	quote_ident(n.nspname) AS schema,
	quote_ident(t.typname) AS name,
	t.typtype,
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

	results := make([]Type, 0)
	err := connection.Select(&results, query)
	gplog.FatalOnError(err)
	return results
}

func GetShellTypes(connection *dbconn.DBConn) []Type {
	query := fmt.Sprintf(`
SELECT
	t.oid,
	quote_ident(n.nspname) AS schema,
	quote_ident(t.typname) AS name,
	t.typtype
FROM pg_type t
JOIN pg_namespace n ON t.typnamespace = n.oid
WHERE %s
AND t.typtype = 'p'
AND %s
ORDER BY n.nspname, t.typname;`, SchemaFilterClause("n"), ExtensionFilterClause("t"))

	results := make([]Type, 0)
	err := connection.Select(&results, query)
	gplog.FatalOnError(err)
	return results
}

/*
 * We already have the functions on which a base type depends in the base type's
 * TypeDefinition, but we need to query pg_proc to determine whether one of those
 * functions is a built-in function (and therefore should not be considered a
 * dependency for dependency sorting purposes).
 */
func ConstructBaseTypeDependencies4(connection *dbconn.DBConn, types []Type, funcInfoMap map[uint32]FunctionInfo) []Type {
	query := fmt.Sprintf(`
SELECT DISTINCT
    t.oid,
    p.oid AS referencedoid
FROM pg_depend d
JOIN pg_proc p ON (d.refobjid = p.oid AND p.pronamespace != (SELECT oid FROM pg_namespace WHERE nspname = 'pg_catalog'))
JOIN pg_type t ON (d.objid = t.oid AND t.typtype = 'b')
JOIN pg_namespace n ON n.oid = t.typnamespace
WHERE %s
AND d.refclassid = 'pg_proc'::regclass
AND d.deptype = 'n';`, SchemaFilterClause("n"))

	results := make([]struct {
		Oid           uint32
		ReferencedOid uint32
	}, 0)
	dependencyMap := make(map[uint32][]string, 0)
	err := connection.Select(&results, query)
	gplog.FatalOnError(err)
	for _, dependency := range results {
		referencedFunc := funcInfoMap[dependency.ReferencedOid]
		dependencyStr := fmt.Sprintf("%s(%s)", referencedFunc.QualifiedName, referencedFunc.Arguments)
		dependencyMap[dependency.Oid] = append(dependencyMap[dependency.Oid], dependencyStr)
	}
	for i := 0; i < len(types); i++ {
		if types[i].Type == "b" {
			types[i].DependsUpon = dependencyMap[types[i].Oid]
		}
	}
	return types
}

func ConstructBaseTypeDependencies5(connection *dbconn.DBConn, types []Type) []Type {
	query := fmt.Sprintf(`
SELECT DISTINCT
    t.oid,
    quote_ident(n.nspname) || '.' || quote_ident(p.proname) || '(' || pg_get_function_arguments(p.oid) || ')' AS referencedobject
FROM pg_depend d
JOIN pg_proc p ON (d.refobjid = p.oid AND p.pronamespace != (SELECT oid FROM pg_namespace WHERE nspname = 'pg_catalog'))
JOIN pg_type t ON (d.objid = t.oid AND t.typtype = 'b')
JOIN pg_namespace n ON n.oid = p.pronamespace
WHERE %s
AND d.refclassid = 'pg_proc'::regclass
AND d.deptype = 'n';`, SchemaFilterClause("n"))

	results := make([]Dependency, 0)
	dependencyMap := make(map[uint32][]string, 0)
	err := connection.Select(&results, query)
	gplog.FatalOnError(err)
	for _, dependency := range results {
		dependencyMap[dependency.Oid] = append(dependencyMap[dependency.Oid], dependency.ReferencedObject)
	}
	for i := 0; i < len(types); i++ {
		if types[i].Type == "b" {
			types[i].DependsUpon = dependencyMap[types[i].Oid]
		}
	}
	return types
}

/*
 * We already have the base type of a domain in the domain's TypeDefinition, but
 * we need to query pg_type to determine whether the base type is built in (and
 * therefore should not be considered a dependency for dependency sorting purposes).
 */
func ConstructDomainDependencies(connection *dbconn.DBConn, types []Type) []Type {
	query := fmt.Sprintf(`
SELECT
	t.oid,
	quote_ident(n.nspname) || '.' || quote_ident(bt.typname) AS referencedobject
FROM pg_type t
JOIN pg_type bt ON t.typbasetype = bt.oid
JOIN pg_namespace n ON bt.typnamespace = n.oid
WHERE %s
AND bt.typnamespace != (
	SELECT
		bn.oid
	FROM pg_namespace bn
	WHERE bn.nspname = 'pg_catalog'
);`, SchemaFilterClause("n"))

	results := make([]Dependency, 0)
	dependencyMap := make(map[uint32][]string, 0)
	err := connection.Select(&results, query)
	gplog.FatalOnError(err)
	for _, dependency := range results {
		dependencyMap[dependency.Oid] = append(dependencyMap[dependency.Oid], dependency.ReferencedObject)
	}
	for i := 0; i < len(types); i++ {
		if types[i].Type == "d" {
			types[i].DependsUpon = dependencyMap[types[i].Oid]
		}
	}
	return types
}

func ConstructCompositeTypeDependencies(connection *dbconn.DBConn, types []Type) []Type {
	query := fmt.Sprintf(`
SELECT DISTINCT
	tc.oid,
	coalesce((SELECT quote_ident(n.nspname) || '.' || quote_ident(typname) FROM pg_type WHERE t.typelem = oid), quote_ident(n.nspname) || '.' || quote_ident(t.typname)) AS referencedobject
FROM pg_depend d
JOIN pg_type t
	ON (d.refobjid = t.oid AND t.typtype != 'p' AND t.typtype != 'e' AND t.typnamespace != (SELECT oid FROM pg_namespace WHERE nspname = 'pg_catalog'))
JOIN pg_class c ON (d.objid = c.oid AND c.relkind = 'c')
JOIN pg_type tc ON (tc.typrelid = c.oid AND tc.typtype = 'c')
JOIN pg_namespace n ON n.oid = t.typnamespace
WHERE %s
AND d.refclassid = 'pg_type'::regclass
AND c.reltype != t.oid
AND d.deptype = 'n';`, SchemaFilterClause("n"))

	results := make([]Dependency, 0)
	dependencyMap := make(map[uint32][]string, 0)
	err := connection.Select(&results, query)
	gplog.FatalOnError(err)
	for _, dependency := range results {
		dependencyMap[dependency.Oid] = append(dependencyMap[dependency.Oid], dependency.ReferencedObject)
	}
	for i := 0; i < len(types); i++ {
		if types[i].Type == "c" {
			types[i].DependsUpon = dependencyMap[types[i].Oid]
		}
	}
	return types
}

type Collation struct {
	Oid     uint32
	Schema  string
	Name    string
	Collate string
	Ctype   string
}

func GetCollations(connection *dbconn.DBConn) []Collation {
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

	err := connection.Select(&results, query)
	gplog.FatalOnError(err)
	return results
}
