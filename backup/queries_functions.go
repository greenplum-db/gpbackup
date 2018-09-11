package backup

/*
 * This file contains structs and functions related to executing specific
 * queries to gather metadata for the objects handled in predata_general.go.
 */

import (
	"fmt"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpbackup/utils"
)

type Function struct {
	Oid               uint32
	Schema            string
	Name              string
	ReturnsSet        bool `db:"proretset"`
	FunctionBody      string
	BinaryPath        string
	Arguments         string
	IdentArgs         string
	ResultType        string
	Volatility        string  `db:"provolatile"`
	IsStrict          bool    `db:"proisstrict"`
	IsLeakProof       bool    `db:"proleakproof"`
	IsSecurityDefiner bool    `db:"prosecdef"`
	Config            string  `db:"proconfig"`
	Cost              float32 `db:"procost"`
	NumRows           float32 `db:"prorows"`
	DataAccess        string  `db:"prodataaccess"`
	Language          string
	DependsUpon       []string
	IsWindow          bool   `db:"proiswindow"`
	ExecLocation      string `db:"proexeclocation"`
}

func (f Function) FQN() string {
	/*
	 * We need to include arguments to differentiate functions with the same name;
	 * we don't use IdentArgs because we already have Arguments in the funcInfoMap.
	 */
	return fmt.Sprintf("%s(%s)", utils.MakeFQN(f.Schema, f.Name), f.Arguments)
}

/*
 * The functions pg_get_function_arguments, pg_getfunction_identity_arguments,
 * and pg_get_function_result were introduced in GPDB 5, so we can use those
 * functions to retrieve arguments, identity arguments, and return values in
 * 5 or later but in GPDB 4.3 we must query pg_proc directly and construct
 * those values here.
 */
func GetFunctionsAllVersions(connection *dbconn.DBConn) []Function {
	if connection.Version.Before("5") {
		functions := GetFunctions4(connection)
		arguments, tableArguments := GetFunctionArgsAndIdentArgs(connection)
		returns := GetFunctionReturnTypes(connection)
		for i := range functions {
			oid := functions[i].Oid
			functions[i].Arguments = arguments[oid]
			functions[i].IdentArgs = arguments[oid]
			functions[i].ReturnsSet = returns[oid].ReturnsSet
			if tableArguments[oid] != "" {
				functions[i].ResultType = fmt.Sprintf("TABLE(%s)", tableArguments[oid])
			} else {
				functions[i].ResultType = returns[oid].ResultType
			}
		}
		return functions
	}
	return GetFunctionsMaster(connection)
}

func GetFunctionsMaster(connection *dbconn.DBConn) []Function {
	masterAtts := ""
	if connection.Version.AtLeast("6") {
		masterAtts = "proiswindow,proexeclocation,proleakproof,"
	} else {
		masterAtts = "'a' AS proexeclocation,"
	}
	query := fmt.Sprintf(`
SELECT
	p.oid,
	quote_ident(nspname) AS schema,
	quote_ident(proname) AS name,
	proretset,
	coalesce(prosrc, '') AS functionbody,
	coalesce(probin, '') AS binarypath,
	pg_catalog.pg_get_function_arguments(p.oid) AS arguments,
	pg_catalog.pg_get_function_identity_arguments(p.oid) AS identargs,
	pg_catalog.pg_get_function_result(p.oid) AS resulttype,
	provolatile,
	proisstrict,
	prosecdef,
	%s
	(
		coalesce(array_to_string(ARRAY(SELECT 'SET ' || option_name || ' TO ' || option_value
		FROM pg_options_to_table(proconfig)), ' '), '')
	) AS proconfig,
	procost,
	prorows,
	prodataaccess,
	(SELECT lanname FROM pg_catalog.pg_language WHERE oid = prolang) AS language
FROM pg_proc p
LEFT JOIN pg_namespace n
	ON p.pronamespace = n.oid
WHERE %s
AND proisagg = 'f'
AND %s
ORDER BY nspname, proname, identargs;`, masterAtts, SchemaFilterClause("n"), ExtensionFilterClause("p"))

	results := make([]Function, 0)
	err := connection.Select(&results, query)
	gplog.FatalOnError(err)
	return results
}

/*
 * In addition to lacking the pg_get_function_* functions, GPDB 4.3 lacks
 * several columns in pg_proc compared to GPDB 5, so we don't retrieve those.
 */
func GetFunctions4(connection *dbconn.DBConn) []Function {
	query := fmt.Sprintf(`
SELECT
	p.oid,
	quote_ident(nspname) AS schema,
	quote_ident(proname) AS name,
	proretset,
	coalesce(prosrc, '') AS functionbody,
	CASE
		WHEN probin = '-' THEN ''
		ELSE probin
		END AS binarypath,
	provolatile,
	proisstrict,
	prosecdef,
	'a' AS proexeclocation,
	(SELECT lanname FROM pg_catalog.pg_language WHERE oid = prolang) AS language
FROM pg_proc p
LEFT JOIN pg_namespace n
	ON p.pronamespace = n.oid
WHERE %s
AND proisagg = 'f'
ORDER BY nspname, proname;`, SchemaFilterClause("n"))

	results := make([]Function, 0)
	err := connection.Select(&results, query)
	gplog.FatalOnError(err)
	return results
}

/*
 * Functions do not have default argument values in GPDB 4.3, so there is no
 * difference between a function's "arguments" and "identity arguments" and
 * we can use the same map for both fields.
 */
func GetFunctionArgsAndIdentArgs(connection *dbconn.DBConn) (map[uint32]string, map[uint32]string) {
	query := `
SELECT
    p.oid,
	CASE
        WHEN proallargtypes IS NOT NULL THEN format_type(unnest(proallargtypes), NULL)
        ELSE format_type(unnest(proargtypes), NULL)
        END AS type,
    CASE
        WHEN proargnames IS NOT NULL THEN quote_ident(unnest(proargnames))
        ELSE ''
        END AS name,
    CASE
        WHEN proargmodes IS NOT NULL THEN unnest(proargmodes)
        ELSE ''
        END AS mode
FROM pg_proc p
JOIN pg_namespace n
ON p.pronamespace = n.oid;`

	results := make([]struct {
		Oid  uint32
		Type string
		Name string
		Mode string
	}, 0)
	err := connection.Select(&results, query)
	gplog.FatalOnError(err)

	argMap := make(map[uint32]string, 0)
	tableArgMap := make(map[uint32]string, 0)
	lastOid := uint32(0)
	arguments := make([]string, 0)
	tableArguments := make([]string, 0)
	for _, funcArgs := range results {
		if funcArgs.Name == `""` {
			funcArgs.Name = ""
		}
		modeStr := ""
		switch funcArgs.Mode {
		case "b":
			modeStr = "INOUT "
		case "o":
			modeStr = "OUT "
		case "v":
			modeStr = "VARIADIC "
		}
		if funcArgs.Name != "" {
			funcArgs.Name += " "
		}
		argStr := fmt.Sprintf("%s%s%s", modeStr, funcArgs.Name, funcArgs.Type)
		if funcArgs.Oid != lastOid && lastOid != 0 {
			argMap[lastOid] = strings.Join(arguments, ", ")
			tableArgMap[lastOid] = strings.Join(tableArguments, ", ")
			arguments = []string{}
			tableArguments = []string{}
		}
		if funcArgs.Mode == "t" {
			tableArguments = append(tableArguments, argStr)
		} else {
			arguments = append(arguments, argStr)
		}
		lastOid = funcArgs.Oid
	}
	argMap[lastOid] = strings.Join(arguments, ", ")
	tableArgMap[lastOid] = strings.Join(tableArguments, ", ")
	return argMap, tableArgMap
}

func GetFunctionReturnTypes(connection *dbconn.DBConn) map[uint32]Function {
	query := fmt.Sprintf(`
SELECT
    p.oid,
	proretset,
	CASE
		WHEN proretset = 't' THEN 'SETOF ' || format_type(prorettype, NULL)
		ELSE format_type(prorettype, NULL)
		END AS resulttype
FROM pg_proc p
JOIN pg_namespace n
ON p.pronamespace = n.oid
WHERE %s`, SchemaFilterClause("n"))

	results := make([]Function, 0)
	err := connection.Select(&results, query)
	gplog.FatalOnError(err)

	returnMap := make(map[uint32]Function, 0)
	for _, result := range results {
		returnMap[result.Oid] = result
	}
	return returnMap
}

type Aggregate struct {
	Oid                 uint32
	Schema              string
	Name                string
	Arguments           string
	IdentArgs           string
	TransitionFunction  uint32 `db:"aggtransfn"`
	PreliminaryFunction uint32 `db:"aggprelimfn"`
	CombineFunction     uint32 `db:"aggcombinefn"`
	SerialFunction      uint32 `db:"aggserialfn"`
	DeserialFunction    uint32 `db:"aggdeserialfn"`
	FinalFunction       uint32 `db:"aggfinalfn"`
	FinalFuncExtra      bool
	SortOperator        uint32 `db:"aggsortop"`
	Hypothetical        bool
	TransitionDataType  string
	TransitionDataSize  int `db:"aggtransspace"`
	InitialValue        string
	InitValIsNull       bool
	IsOrdered           bool `db:"aggordered"`
}

func GetAggregates(connection *dbconn.DBConn) []Aggregate {
	version4query := fmt.Sprintf(`
SELECT
	p.oid,
	quote_ident(n.nspname) AS schema,
	p.proname AS name,
	'' AS arguments,
	'' AS identargs,
	a.aggtransfn::regproc::oid,
	a.aggprelimfn::regproc::oid,
	a.aggfinalfn::regproc::oid,
	a.aggsortop::regproc::oid,
	format_type(a.aggtranstype, NULL) as transitiondatatype,
	coalesce(a.agginitval, '') AS initialvalue,
	(a.agginitval IS NULL) AS initvalisnull,
	a.aggordered
FROM pg_aggregate a
LEFT JOIN pg_proc p ON a.aggfnoid = p.oid
LEFT JOIN pg_namespace n ON p.pronamespace = n.oid
WHERE %s;`, SchemaFilterClause("n"))

	version5query := fmt.Sprintf(`
SELECT
	p.oid,
	quote_ident(n.nspname) AS schema,
	p.proname AS name,
	pg_catalog.pg_get_function_arguments(p.oid) AS arguments,
	pg_catalog.pg_get_function_identity_arguments(p.oid) AS identargs,
	a.aggtransfn::regproc::oid,
	a.aggprelimfn::regproc::oid,
	a.aggfinalfn::regproc::oid,
	a.aggsortop::regproc::oid,
	format_type(a.aggtranstype, NULL) as transitiondatatype,
	coalesce(a.agginitval, '') AS initialvalue,
	(a.agginitval IS NULL) AS initvalisnull,
	a.aggordered
FROM pg_aggregate a
LEFT JOIN pg_proc p ON a.aggfnoid = p.oid
LEFT JOIN pg_namespace n ON p.pronamespace = n.oid
WHERE %s
AND %s;`, SchemaFilterClause("n"), ExtensionFilterClause("p"))

	masterQuery := fmt.Sprintf(`
SELECT
	p.oid,
	quote_ident(n.nspname) AS schema,
	p.proname AS name,
	pg_catalog.pg_get_function_arguments(p.oid) AS arguments,
	pg_catalog.pg_get_function_identity_arguments(p.oid) AS identargs,
	a.aggtransfn::regproc::oid,
	a.aggcombinefn::regproc::oid,
	a.aggserialfn::regproc::oid,
	a.aggdeserialfn::regproc::oid,
	a.aggfinalfn::regproc::oid,
	a.aggfinalextra AS finalfuncextra,
	a.aggsortop::regproc::oid,
	(a.aggkind = 'h') AS hypothetical,
	format_type(a.aggtranstype, NULL) as transitiondatatype,
	aggtransspace,
	coalesce(a.agginitval, '') AS initialvalue,
	(a.agginitval IS NULL) AS initvalisnull
FROM pg_aggregate a
LEFT JOIN pg_proc p ON a.aggfnoid = p.oid
LEFT JOIN pg_namespace n ON p.pronamespace = n.oid
WHERE %s
AND %s;`, SchemaFilterClause("n"), ExtensionFilterClause("p"))

	aggregates := make([]Aggregate, 0)
	query := ""
	if connection.Version.Before("5") {
		query = version4query
	} else if connection.Version.Before("6") {
		query = version5query
	} else {
		query = masterQuery
	}
	err := connection.Select(&aggregates, query)
	gplog.FatalOnError(err)
	if connection.Version.Before("5") {
		arguments, _ := GetFunctionArgsAndIdentArgs(connection)
		for i := range aggregates {
			oid := aggregates[i].Oid
			aggregates[i].Arguments = arguments[oid]
			aggregates[i].IdentArgs = arguments[oid]
		}
	}
	return aggregates
}

type FunctionInfo struct {
	QualifiedName string
	Arguments     string
	IsInternal    bool
}

func GetFunctionOidToInfoMap(connection *dbconn.DBConn) map[uint32]FunctionInfo {
	version4query := `
SELECT
	p.oid,
	quote_ident(n.nspname) AS schema,
	quote_ident(p.proname) AS name
FROM pg_proc p
LEFT JOIN pg_namespace n ON p.pronamespace = n.oid;
`
	query := `
SELECT
	p.oid,
	quote_ident(n.nspname) AS schema,
	quote_ident(p.proname) AS name,
	pg_catalog.pg_get_function_arguments(p.oid) AS arguments
FROM pg_proc p
LEFT JOIN pg_namespace n ON p.pronamespace = n.oid;
`

	results := make([]struct {
		Oid       uint32
		Schema    string
		Name      string
		Arguments string
	}, 0)
	funcMap := make(map[uint32]FunctionInfo, 0)
	var err error
	if connection.Version.Before("5") {
		err = connection.Select(&results, version4query)
		arguments, _ := GetFunctionArgsAndIdentArgs(connection)
		for i := range results {
			results[i].Arguments = arguments[results[i].Oid]
		}
	} else {
		err = connection.Select(&results, query)
	}
	gplog.FatalOnError(err)
	for _, function := range results {
		fqn := utils.MakeFQN(function.Schema, function.Name)

		isInternal := false
		if function.Schema == "pg_catalog" {
			isInternal = true
		}
		funcInfo := FunctionInfo{QualifiedName: fqn, Arguments: function.Arguments, IsInternal: isInternal}
		funcMap[function.Oid] = funcInfo
	}
	return funcMap
}

type Cast struct {
	Oid            uint32
	SourceTypeFQN  string
	TargetTypeFQN  string
	FunctionOid    uint32 // Used with GPDB 4.3 to map function arguments
	FunctionSchema string
	FunctionName   string
	FunctionArgs   string
	CastContext    string
	CastMethod     string
}

func GetCasts(connection *dbconn.DBConn) []Cast {
	/* This query retrieves all casts where either the source type, the target
	 * type, or the cast function is user-defined.
	 */
	argStr := ""
	if connection.Version.Before("5") {
		argStr = `'' AS functionargs,
	coalesce(p.oid, 0::oid) AS functionoid,`
	} else {
		argStr = `coalesce(pg_get_function_arguments(p.oid), '') AS functionargs,`
	}
	methodStr := ""
	if connection.Version.AtLeast("6") {
		methodStr = "castmethod,"
	} else {
		methodStr = "CASE WHEN c.castfunc = 0 THEN 'b' ELSE 'f' END AS castmethod,"
	}
	query := fmt.Sprintf(`
SELECT
	c.oid,
	quote_ident(sn.nspname) || '.' || quote_ident(st.typname) AS sourcetypefqn,
	quote_ident(tn.nspname) || '.' || quote_ident(tt.typname) AS targettypefqn,
	coalesce(quote_ident(n.nspname), '') AS functionschema,
	coalesce(quote_ident(p.proname), '') AS functionname,
	%s
	%s
	c.castcontext
FROM pg_cast c
JOIN pg_type st ON c.castsource = st.oid
JOIN pg_type tt ON c.casttarget = tt.oid
JOIN pg_namespace sn ON st.typnamespace = sn.oid
JOIN pg_namespace tn ON tt.typnamespace = tn.oid
LEFT JOIN pg_proc p ON c.castfunc = p.oid
LEFT JOIN pg_description d ON c.oid = d.objoid
LEFT JOIN pg_namespace n ON p.pronamespace = n.oid
WHERE ((%s) OR (%s) OR (%s))
AND %s
ORDER BY 1, 2;
`, argStr, methodStr, SchemaFilterClause("sn"), SchemaFilterClause("tn"), SchemaFilterClause("n"), ExtensionFilterClause("c"))

	casts := make([]Cast, 0)
	err := connection.Select(&casts, query)
	gplog.FatalOnError(err)
	if connection.Version.Before("5") {
		arguments, _ := GetFunctionArgsAndIdentArgs(connection)
		for i := range casts {
			oid := casts[i].FunctionOid
			casts[i].FunctionArgs = arguments[oid]
		}
	}
	return casts
}

type Extension struct {
	Oid    uint32
	Name   string
	Schema string
}

func GetExtensions(connection *dbconn.DBConn) []Extension {
	results := make([]Extension, 0)

	query := `
SELECT
	e.oid,
	quote_ident(extname) AS name,
	quote_ident(n.nspname) AS schema
FROM pg_extension e
JOIN pg_namespace n ON e.extnamespace = n.oid;
`
	err := connection.Select(&results, query)
	gplog.FatalOnError(err)
	return results
}

type ProceduralLanguage struct {
	Oid       uint32
	Name      string
	Owner     string
	IsPl      bool   `db:"lanispl"`
	PlTrusted bool   `db:"lanpltrusted"`
	Handler   uint32 `db:"lanplcallfoid"`
	Inline    uint32 `db:"laninline"`
	Validator uint32 `db:"lanvalidator"`
}

func GetProceduralLanguages(connection *dbconn.DBConn) []ProceduralLanguage {
	results := make([]ProceduralLanguage, 0)
	// Languages are owned by the bootstrap superuser, OID 10
	version4query := `
SELECT
	oid,
	quote_ident(l.lanname) AS name,
	pg_get_userbyid(10) as owner, 
	l.lanispl,
	l.lanpltrusted,
	l.lanplcallfoid::regprocedure::oid,
	0 AS laninline,
	l.lanvalidator::regprocedure::oid
FROM pg_language l
WHERE l.lanispl='t'
AND l.lanname != 'plpgsql';
`
	query := fmt.Sprintf(`
SELECT
	oid,
	quote_ident(l.lanname) AS name,
	pg_get_userbyid(l.lanowner) as owner,
	l.lanispl,
	l.lanpltrusted,
	l.lanplcallfoid::regprocedure::oid,
	l.laninline::regprocedure::oid,
	l.lanvalidator::regprocedure::oid
FROM pg_language l
WHERE l.lanispl='t'
AND l.lanname != 'plpgsql'
AND %s;`, ExtensionFilterClause("l"))
	var err error
	if connection.Version.Before("5") {
		err = connection.Select(&results, version4query)
	} else {
		err = connection.Select(&results, query)
	}
	gplog.FatalOnError(err)
	return results
}

type Conversion struct {
	Oid                uint32
	Schema             string
	Name               string
	ForEncoding        string
	ToEncoding         string
	ConversionFunction string
	IsDefault          bool `db:"condefault"`
}

func GetConversions(connection *dbconn.DBConn) []Conversion {
	results := make([]Conversion, 0)
	query := fmt.Sprintf(`
SELECT
	c.oid,
	quote_ident(n.nspname) AS schema,
	quote_ident(c.conname) AS name,
	pg_encoding_to_char(c.conforencoding) AS forencoding,
	pg_encoding_to_char(c.contoencoding) AS toencoding,
	quote_ident(fn.nspname) || '.' || quote_ident(p.proname) AS conversionfunction,
	c.condefault
FROM pg_conversion c
JOIN pg_namespace n ON c.connamespace = n.oid
JOIN pg_proc p ON c.conproc = p.oid
JOIN pg_namespace fn ON p.pronamespace = fn.oid
WHERE %s
AND %s
ORDER BY n.nspname, c.conname;`, SchemaFilterClause("n"), ExtensionFilterClause("c"))

	err := connection.Select(&results, query)
	gplog.FatalOnError(err)
	return results
}

/*
 * When we retrieve function dependencies, we don't record a dependency of a
 * function on a base type if the function is part of the definition of the
 * base type, as we print out shell types for all base types at the beginning
 * of the backup and so do not need to consider those dependencies when sorting
 * functions and types.
 */
func ConstructFunctionDependencies(connection *dbconn.DBConn, functions []Function) []Function {
	modStr := ""
	if connection.Version.AtLeast("5") {
		modStr = `
	AND t.typmodin != p.oid
	AND t.typmodout != p.oid`
	}
	query := fmt.Sprintf(`
SELECT
	p.oid,
	coalesce((SELECT quote_ident(n.nspname) || '.' || quote_ident(typname) FROM pg_type WHERE t.typelem = oid), quote_ident(n.nspname) || '.' || quote_ident(t.typname)) AS referencedobject
FROM pg_depend d
JOIN pg_type t ON (d.refobjid = t.oid AND t.typtype != 'e' AND t.typtype != 'p')
JOIN pg_proc p ON d.objid = p.oid
JOIN pg_namespace n ON n.oid = t.typnamespace
WHERE %s
AND d.refclassid = 'pg_type'::regclass
AND t.typinput != p.oid
AND t.typoutput != p.oid
AND t.typreceive != p.oid
AND t.typsend != p.oid%s;`, SchemaFilterClause("n"), modStr)

	results := make([]Dependency, 0)
	dependencyMap := make(map[uint32][]string, 0)
	err := connection.Select(&results, query)
	gplog.FatalOnError(err)
	for _, dependency := range results {
		dependencyMap[dependency.Oid] = append(dependencyMap[dependency.Oid], dependency.ReferencedObject)
	}
	for i := 0; i < len(functions); i++ {
		functions[i].DependsUpon = dependencyMap[functions[i].Oid]
	}
	return functions
}

type ForeignDataWrapper struct {
	Oid       uint32
	Name      string
	Handler   uint32
	Validator uint32
	Options   string
}

func GetForeignDataWrappers(connection *dbconn.DBConn) []ForeignDataWrapper {
	results := make([]ForeignDataWrapper, 0)
	query := fmt.Sprintf(`
SELECT
	oid,
	quote_ident (fdwname) AS name,
	fdwvalidator AS validator,
	fdwhandler AS handler,
	(
		array_to_string(ARRAY(SELECT pg_catalog.quote_ident(option_name) || ' ' || pg_catalog.quote_literal(option_value)
		FROM pg_options_to_table(fdwoptions)
		ORDER BY option_name), ', ')
	) AS options
FROM pg_foreign_data_wrapper
WHERE %s;`, ExtensionFilterClause(""))

	err := connection.Select(&results, query)
	gplog.FatalOnError(err)
	return results
}

type ForeignServer struct {
	Oid                uint32
	Name               string
	Type               string
	Version            string
	ForeignDataWrapper string
	Options            string
}

func GetForeignServers(connection *dbconn.DBConn) []ForeignServer {
	results := make([]ForeignServer, 0)
	query := fmt.Sprintf(`
SELECT
	fs.oid,
	quote_ident (fs.srvname) AS name,
	coalesce(fs.srvtype, '') AS type,
	coalesce(fs.srvversion, '') AS version,
    quote_ident(fdw.fdwname) AS foreigndatawrapper,
	(
		array_to_string(ARRAY(SELECT pg_catalog.quote_ident(option_name) || ' ' || pg_catalog.quote_literal(option_value)
		FROM pg_options_to_table(fs.srvoptions)
		ORDER BY option_name), ', ')
	) AS options
FROM pg_foreign_server fs
LEFT JOIN pg_foreign_data_wrapper fdw ON fdw.oid = srvfdw
WHERE %s`, ExtensionFilterClause("fs"))

	err := connection.Select(&results, query)
	gplog.FatalOnError(err)
	return results
}

type UserMapping struct {
	Oid     uint32
	User    string
	Server  string
	Options string
}

func GetUserMappings(connection *dbconn.DBConn) []UserMapping {
	results := make([]UserMapping, 0)
	query := fmt.Sprintf(`
SELECT
	um.umid AS oid,
	quote_ident(um.usename) AS user,
	quote_ident(um.srvname) AS server,
	(
		array_to_string(ARRAY(SELECT pg_catalog.quote_ident(option_name) || ' ' || pg_catalog.quote_literal(option_value)
		FROM pg_options_to_table(um.umoptions)
		ORDER BY option_name), ', ')
	) AS options
FROM pg_user_mappings um
WHERE um.umid NOT IN (select objid from pg_depend where deptype = 'e')
ORDER by um.usename;`)

	err := connection.Select(&results, query)
	gplog.FatalOnError(err)
	return results
}
