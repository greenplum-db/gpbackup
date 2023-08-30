package backup

/*
 * This file contains structs and functions related to executing specific
 * queries to gather metadata for the objects handled in predata_general.go.
 */

import (
	"fmt"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpbackup/toc"
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

func (o Operator) GetMetadataEntry() (string, toc.MetadataEntry) {
	return "predata",
		toc.MetadataEntry{
			Schema:          o.Schema,
			Name:            o.Name,
			ObjectType:      toc.OBJ_OPERATOR,
			ReferenceObject: "",
			StartByte:       0,
			EndByte:         0,
		}
}

func (o Operator) GetUniqueID() UniqueID {
	return UniqueID{ClassID: PG_OPERATOR_OID, Oid: o.Oid}
}

func (o Operator) FQN() string {
	leftArg := "NONE"
	rightArg := "NONE"
	if o.LeftArgType != "-" {
		leftArg = o.LeftArgType
	}
	if o.RightArgType != "-" {
		rightArg = o.RightArgType
	}
	return fmt.Sprintf("%s.%s (%s, %s)", o.Schema, o.Name, leftArg, rightArg)
}

func GetOperators(connectionPool *dbconn.DBConn) []Operator {
	results := make([]Operator, 0)
	before5Query := fmt.Sprintf(`
	SELECT o.oid AS oid,
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

	atLeast5Query := fmt.Sprintf(`
	SELECT o.oid AS oid,
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
		AND %s`, SchemaFilterClause("n"), ExtensionFilterClause("o"))

	query := ""
	if connectionPool.Version.Before("5") {
		query = before5Query
	} else {
		query = atLeast5Query
	}

	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)
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

func (opf OperatorFamily) GetMetadataEntry() (string, toc.MetadataEntry) {
	return "predata",
		toc.MetadataEntry{
			Schema:          opf.Schema,
			Name:            opf.Name,
			ObjectType:      toc.OBJ_OPERATOR_FAMILY,
			ReferenceObject: "",
			StartByte:       0,
			EndByte:         0,
		}
}

func (opf OperatorFamily) GetUniqueID() UniqueID {
	return UniqueID{ClassID: PG_OPFAMILY_OID, Oid: opf.Oid}
}

func (opf OperatorFamily) FQN() string {
	return fmt.Sprintf("%s USING %s", utils.MakeFQN(opf.Schema, opf.Name), opf.IndexMethod)
}

func GetOperatorFamilies(connectionPool *dbconn.DBConn) []OperatorFamily {
	results := make([]OperatorFamily, 0)
	query := fmt.Sprintf(`
	SELECT o.oid AS oid,
		quote_ident(n.nspname) AS schema,
		quote_ident(opfname) AS name,
		(SELECT quote_ident(amname) FROM pg_am WHERE oid = opfmethod) AS indexMethod
	FROM pg_opfamily o
		JOIN pg_namespace n on n.oid = o.opfnamespace
	WHERE %s
		AND %s`,
		SchemaFilterClause("n"), ExtensionFilterClause("o"))
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)
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

func (opc OperatorClass) GetMetadataEntry() (string, toc.MetadataEntry) {
	return "predata",
		toc.MetadataEntry{
			Schema:          opc.Schema,
			Name:            opc.Name,
			ObjectType:      toc.OBJ_OPERATOR_CLASS,
			ReferenceObject: "",
			StartByte:       0,
			EndByte:         0,
		}
}

func (opc OperatorClass) GetUniqueID() UniqueID {
	return UniqueID{ClassID: PG_OPCLASS_OID, Oid: opc.Oid}
}

func (opc OperatorClass) FQN() string {
	return fmt.Sprintf("%s USING %s", utils.MakeFQN(opc.Schema, opc.Name), opc.IndexMethod)
}

func GetOperatorClasses(connectionPool *dbconn.DBConn) []OperatorClass {
	results := make([]OperatorClass, 0)
	/*
	 * In the GPDB 4.3 query, we assign the class schema and name to both the
	 * class schema/name and family schema/name fields, so that the logic in
	 * PrintCreateOperatorClassStatement to not print FAMILY if the class and
	 * family have the same schema and name will work for both versions.
	 */
	before5Query := fmt.Sprintf(`
	SELECT c.oid AS oid,
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

	atLeast5Query := fmt.Sprintf(`
	SELECT c.oid AS oid,
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
		AND %s`,
		SchemaFilterClause("cls_ns"), ExtensionFilterClause("c"))

	query := ""
	if connectionPool.Version.Before("5") {
		query = before5Query
	} else {
		query = atLeast5Query
	}

	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)

	operators := GetOperatorClassOperators(connectionPool)
	for i := range results {
		results[i].Operators = operators[results[i].Oid]
	}
	functions := GetOperatorClassFunctions(connectionPool)
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
	OrderByFamily  string
}

func GetOperatorClassOperators(connectionPool *dbconn.DBConn) map[uint32][]OperatorClassOperator {
	results := make([]OperatorClassOperator, 0)
	before5Query := `
	SELECT amopclaid AS classoid,
		amopstrategy AS strategynumber,
		amopopr::pg_catalog.regoperator AS operator,
		amopreqcheck AS recheck
	FROM pg_catalog.pg_amop
	ORDER BY amopstrategy`

	version5Query := `
	SELECT refobjid AS classoid,
		amopstrategy AS strategynumber,
		amopopr::pg_catalog.regoperator AS operator,
		amopreqcheck AS recheck
	FROM pg_catalog.pg_amop ao
		JOIN pg_catalog.pg_depend d ON d.objid = ao.oid
	WHERE refclassid = 'pg_catalog.pg_opclass'::pg_catalog.regclass
		AND classid = 'pg_catalog.pg_amop'::pg_catalog.regclass
	ORDER BY amopstrategy`

	atLeast6Query := `
	SELECT refobjid AS classoid,
		amopstrategy AS strategynumber,
		amopopr::pg_catalog.regoperator AS operator,
		coalesce(quote_ident(ns.nspname) || '.' || quote_ident(opf.opfname), '') AS orderbyfamily
	FROM pg_catalog.pg_amop ao
		JOIN pg_catalog.pg_depend d ON d.objid = ao.oid
		LEFT JOIN pg_opfamily opf ON opf.oid = ao.amopsortfamily
		LEFT JOIN pg_namespace ns ON ns.oid = opf.opfnamespace
	WHERE refclassid = 'pg_catalog.pg_opclass'::pg_catalog.regclass
		AND classid = 'pg_catalog.pg_amop'::pg_catalog.regclass
	ORDER BY amopstrategy`

	query := ""
	if connectionPool.Version.Before("5") {
		query = before5Query
	} else if connectionPool.Version.Is("5") {
		query = version5Query
	} else {
		query = atLeast6Query
	}

	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)

	operators := make(map[uint32][]OperatorClassOperator)
	for _, result := range results {
		operators[result.ClassOid] = append(operators[result.ClassOid], result)
	}
	return operators
}

type OperatorClassFunction struct {
	ClassOid      uint32
	SupportNumber int
	FunctionName  string
	LeftType      string `db:"amproclefttype"`
	RightType     string `db:"amprocrighttype"`
}

func GetOperatorClassFunctions(connectionPool *dbconn.DBConn) map[uint32][]OperatorClassFunction {
	results := make([]OperatorClassFunction, 0)
	before5Query := `
	SELECT amopclaid AS classoid,
		amprocnum AS supportnumber,
		amproc::regprocedure AS functionname
	FROM pg_catalog.pg_amproc
	ORDER BY amprocnum`

	atLeast5Query := `
	SELECT refobjid AS classoid,
		amprocnum AS supportnumber,
		amproclefttype::regtype,
		amprocrighttype::regtype,
		amproc::regprocedure::text AS functionname
	FROM pg_catalog.pg_amproc ap
		JOIN pg_catalog.pg_depend d ON d.objid = ap.oid
	WHERE refclassid = 'pg_catalog.pg_opclass'::pg_catalog.regclass
		AND classid = 'pg_catalog.pg_amproc'::pg_catalog.regclass
	ORDER BY amprocnum`

	query := ""
	if connectionPool.Version.Before("5") {
		query = before5Query
	} else {
		query = atLeast5Query
	}

	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)

	functions := make(map[uint32][]OperatorClassFunction)
	for _, result := range results {
		functions[result.ClassOid] = append(functions[result.ClassOid], result)
	}
	return functions
}
