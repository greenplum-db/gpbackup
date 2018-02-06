package backup

/*
 * This file contains structs and functions related to executing specific
 * queries to gather metadata for the objects handled in predata_general.go.
 */

import (
	"fmt"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gpbackup/utils"
)

type Operator struct {
	Oid              uint32
	Schema           string
	Name             string
	Procedure        string
	LeftArgType      string
	RightArgType     string
	CommutatorOp     string
	NegatorOp        string
	RestrictFunction string
	JoinFunction     string
	CanHash          bool
	CanMerge         bool
}

func GetOperators(connection *dbconn.DBConn) []Operator {
	results := make([]Operator, 0)
	version4query := fmt.Sprintf(`
SELECT
	o.oid,
	quote_ident(n.nspname) AS schema,
	oprname AS name,
	oprcode::regproc AS procedure,
	oprleft::regtype AS leftargtype,
	oprright::regtype AS rightargtype,
	oprcom::regoper AS commutatorop,
	oprnegate::regoper AS negatorop,
	oprrest AS restrictfunction,
	oprjoin AS joinfunction,
	oprcanhash AS canhash
FROM pg_operator o
JOIN pg_namespace n on n.oid = o.oprnamespace
WHERE %s AND oprcode != 0`, SchemaFilterClause("n"))

	masterQuery := fmt.Sprintf(`
SELECT
	o.oid,
	quote_ident(n.nspname) AS schema,
	oprname AS name,
	oprcode::regproc AS procedure,
	oprleft::regtype AS leftargtype,
	oprright::regtype AS rightargtype,
	oprcom::regoper AS commutatorop,
	oprnegate::regoper AS negatorop,
	oprrest AS restrictfunction,
	oprjoin AS joinfunction,
	oprcanmerge AS canmerge,
	oprcanhash AS canhash
FROM pg_operator o
JOIN pg_namespace n on n.oid = o.oprnamespace
WHERE %s AND oprcode != 0
AND o.oid NOT IN (select objid from pg_depend where deptype = 'e')`, SchemaFilterClause("n"))

	var err error
	if connection.Version.Before("5") {
		err = connection.Select(&results, version4query)
	} else {
		err = connection.Select(&results, masterQuery)
	}
	utils.CheckError(err)
	return results
}

/*
 * Operator families are not supported in GPDB 4.3, so OperatorFamily
 * and GetOperatorFamilies are not used in a 4.3 backup.
 */

type OperatorFamily struct {
	Oid         uint32
	Schema      string
	Name        string
	IndexMethod string
}

func GetOperatorFamilies(connection *dbconn.DBConn) []OperatorFamily {
	results := make([]OperatorFamily, 0)
	query := fmt.Sprintf(`
SELECT
	o.oid,
	quote_ident(n.nspname) AS schema,
	quote_ident(opfname) AS name,
	(SELECT quote_ident(amname) FROM pg_am WHERE oid = opfmethod) AS indexMethod
FROM pg_opfamily o
JOIN pg_namespace n on n.oid = o.opfnamespace
WHERE %s
AND o.oid NOT IN (select objid from pg_depend where deptype = 'e')`, SchemaFilterClause("n"))
	err := connection.Select(&results, query)
	utils.CheckError(err)
	return results
}

type OperatorClass struct {
	Oid          uint32
	Schema       string
	Name         string
	FamilySchema string
	FamilyName   string
	IndexMethod  string
	Type         string
	Default      bool
	StorageType  string
	Operators    []OperatorClassOperator
	Functions    []OperatorClassFunction
}

func GetOperatorClasses(connection *dbconn.DBConn) []OperatorClass {
	results := make([]OperatorClass, 0)
	/*
	 * In the GPDB 4.3 query, we assign the class schema and name to both the
	 * class schema/name and family schema/name fields, so that the logic in
	 * PrintCreateOperatorClassStatements to not print FAMILY if the class and
	 * family have the same schema and name will work for both versions.
	 */
	version4query := fmt.Sprintf(`
SELECT
	c.oid,
	quote_ident(cls_ns.nspname) AS schema,
	quote_ident(opcname) AS name,
	'' AS familyschema,
	'' AS familyname,
	(SELECT amname FROM pg_catalog.pg_am WHERE oid = opcamid) AS indexmethod,
	opcintype::pg_catalog.regtype AS type,
	opcdefault AS default,
	opckeytype::pg_catalog.regtype AS storagetype
FROM pg_catalog.pg_opclass c
JOIN pg_catalog.pg_namespace cls_ns ON cls_ns.oid = opcnamespace
WHERE %s`, SchemaFilterClause("cls_ns"))

	masterQuery := fmt.Sprintf(`
SELECT
	c.oid,
	quote_ident(cls_ns.nspname) AS schema,
	quote_ident(opcname) AS name,
	quote_ident(fam_ns.nspname) AS familyschema,
	quote_ident(opfname) AS familyname,
	(SELECT amname FROM pg_catalog.pg_am WHERE oid = opcmethod) AS indexmethod,
	opcintype::pg_catalog.regtype AS type,
	opcdefault AS default,
	opckeytype::pg_catalog.regtype AS storagetype
FROM pg_catalog.pg_opclass c
LEFT JOIN pg_catalog.pg_opfamily f ON f.oid = opcfamily
JOIN pg_catalog.pg_namespace cls_ns ON cls_ns.oid = opcnamespace
JOIN pg_catalog.pg_namespace fam_ns ON fam_ns.oid = opfnamespace
WHERE %s
AND c.oid NOT IN (select objid from pg_depend where deptype = 'e')`, SchemaFilterClause("cls_ns"))

	var err error
	if connection.Version.Before("5") {
		err = connection.Select(&results, version4query)
	} else {
		err = connection.Select(&results, masterQuery)
	}
	utils.CheckError(err)

	operators := GetOperatorClassOperators(connection)
	for i := range results {
		results[i].Operators = operators[results[i].Oid]
	}
	functions := GetOperatorClassFunctions(connection)
	for i := range results {
		results[i].Functions = functions[results[i].Oid]
	}

	return results
}

type OperatorClassOperator struct {
	ClassOid       uint32
	StrategyNumber int
	Operator       string
	Recheck        bool
}

func GetOperatorClassOperators(connection *dbconn.DBConn) map[uint32][]OperatorClassOperator {
	results := make([]OperatorClassOperator, 0)
	version4query := fmt.Sprintf(`
SELECT
	amopclaid AS classoid,
	amopstrategy AS strategynumber,
	amopopr::pg_catalog.regoperator AS operator,
	amopreqcheck AS recheck
FROM pg_catalog.pg_amop
ORDER BY amopstrategy
`)

	version5query := fmt.Sprintf(`
SELECT
	refobjid AS classoid,
	amopstrategy AS strategynumber,
	amopopr::pg_catalog.regoperator AS operator,
	amopreqcheck AS recheck
FROM pg_catalog.pg_amop ao
JOIN pg_catalog.pg_depend d ON d.objid = ao.oid
WHERE refclassid = 'pg_catalog.pg_opclass'::pg_catalog.regclass
AND classid = 'pg_catalog.pg_amop'::pg_catalog.regclass
ORDER BY amopstrategy
`)

	masterQuery := fmt.Sprintf(`
SELECT
	refobjid AS classoid,
	amopstrategy AS strategynumber,
	amopopr::pg_catalog.regoperator AS operator
FROM pg_catalog.pg_amop ao
JOIN pg_catalog.pg_depend d ON d.objid = ao.oid
WHERE refclassid = 'pg_catalog.pg_opclass'::pg_catalog.regclass
AND classid = 'pg_catalog.pg_amop'::pg_catalog.regclass
ORDER BY amopstrategy
`)
	var err error
	if connection.Version.Before("5") {
		err = connection.Select(&results, version4query)
	} else if connection.Version.Before("6") {
		err = connection.Select(&results, version5query)
	} else {
		err = connection.Select(&results, masterQuery)
	}
	utils.CheckError(err)

	operators := make(map[uint32][]OperatorClassOperator, 0)
	for _, result := range results {
		operators[result.ClassOid] = append(operators[result.ClassOid], result)
	}
	return operators
}

type OperatorClassFunction struct {
	ClassOid      uint32
	SupportNumber int
	FunctionName  string
}

func GetOperatorClassFunctions(connection *dbconn.DBConn) map[uint32][]OperatorClassFunction {
	results := make([]OperatorClassFunction, 0)
	version4query := fmt.Sprintf(`
SELECT
	amopclaid AS classoid,
	amprocnum AS supportnumber,
	amproc::regprocedure AS functionname
FROM pg_catalog.pg_amproc
ORDER BY amprocnum
`)

	masterQuery := fmt.Sprintf(`
SELECT
	refobjid AS classoid,
	amprocnum AS supportnumber,
	amproc::regprocedure::text AS functionname
FROM pg_catalog.pg_amproc ap
JOIN pg_catalog.pg_depend d ON d.objid = ap.oid
WHERE refclassid = 'pg_catalog.pg_opclass'::pg_catalog.regclass
AND classid = 'pg_catalog.pg_amproc'::pg_catalog.regclass
ORDER BY amprocnum
`)

	var err error
	if connection.Version.Before("5") {
		err = connection.Select(&results, version4query)
	} else {
		err = connection.Select(&results, masterQuery)
	}
	utils.CheckError(err)

	functions := make(map[uint32][]OperatorClassFunction, 0)
	for _, result := range results {
		functions[result.ClassOid] = append(functions[result.ClassOid], result)
	}
	return functions
}
