package backup

/*
 * This file contains structs and functions related to executing specific
 * queries to gather metadata for the objects handled in predata_general.go.
 */

import (
	"fmt"
	"strings"

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
	IsSecurityDefiner bool    `db:"prosecdef"`
	Config            string  `db:"proconfig"`
	Cost              float32 `db:"procost"`
	NumRows           float32 `db:"prorows"`
	DataAccess        string  `db:"prodataaccess"`
	Language          string
	DependsUpon       []string
}

/*
 * The functions pg_get_function_arguments, pg_getfunction_identity_arguments,
 * and pg_get_function_result were introduced in GPDB 5, so we can use those
 * functions to retrieve arguments, identity arguments, and return values in
 * 5 or later but in GPDB 4.3 we must query pg_proc directly and construct
 * those values here.
 */
func GetFunctions(connection *utils.DBConn) []Function {
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
	return GetFunctions5(connection)
}

func GetFunctions5(connection *utils.DBConn) []Function {
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
ORDER BY nspname, proname, identargs;`, SchemaFilterClause("n"))

	results := make([]Function, 0)
	err := connection.Select(&results, query)
	utils.CheckError(err)
	return results
}

/*
 * In addition to lacking the pg_get_function_* functions, GPDB 4.3 lacks
 * several columns in pg_proc compared to GPDB 5, so we don't retrieve those.
 */
func GetFunctions4(connection *utils.DBConn) []Function {
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
	(SELECT lanname FROM pg_catalog.pg_language WHERE oid = prolang) AS language
FROM pg_proc p
LEFT JOIN pg_namespace n
	ON p.pronamespace = n.oid
WHERE %s
AND proisagg = 'f'
ORDER BY nspname, proname;`, SchemaFilterClause("n"))

	results := make([]Function, 0)
	err := connection.Select(&results, query)
	utils.CheckError(err)
	return results
}

/*
 * Functions do not have default argument values in GPDB 4.3, so there is no
 * difference between a function's "arguments" and "identity arguments" and
 * we can use the same map for both fields.
 */
func GetFunctionArgsAndIdentArgs(connection *utils.DBConn) (map[uint32]string, map[uint32]string) {
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
	utils.CheckError(err)

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

func GetFunctionReturnTypes(connection *utils.DBConn) map[uint32]Function {
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
	utils.CheckError(err)

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
	FinalFunction       uint32 `db:"aggfinalfn"`
	FinalFuncExtra      bool
	SortOperator        uint32 `db:"aggsortop"`
	Hypothetical        bool
	TransitionDataType  string
	InitialValue        string
	InitValIsNull       bool
	IsOrdered           bool `db:"aggordered"`
}

func GetAggregates(connection *utils.DBConn) []Aggregate {
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
WHERE %s;`, SchemaFilterClause("n"))

	masterQuery := fmt.Sprintf(`
SELECT
	p.oid,
	quote_ident(n.nspname) AS schema,
	p.proname AS name,
	pg_catalog.pg_get_function_arguments(p.oid) AS arguments,
	pg_catalog.pg_get_function_identity_arguments(p.oid) AS identargs,
	a.aggtransfn::regproc::oid,
	a.aggprelimfn::regproc::oid,
	a.aggfinalfn::regproc::oid,
	a.aggfinalextra AS finalfuncextra,
	a.aggsortop::regproc::oid,
	(a.aggkind = 'h') AS hypothetical,
	format_type(a.aggtranstype, NULL) as transitiondatatype,
	coalesce(a.agginitval, '') AS initialvalue,
	(a.agginitval IS NULL) AS initvalisnull
FROM pg_aggregate a
LEFT JOIN pg_proc p ON a.aggfnoid = p.oid
LEFT JOIN pg_namespace n ON p.pronamespace = n.oid
WHERE %s;`, SchemaFilterClause("n"))

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
	utils.CheckError(err)
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

func GetFunctionOidToInfoMap(connection *utils.DBConn) map[uint32]FunctionInfo {
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
	utils.CheckError(err)
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
}

func GetCasts(connection *utils.DBConn) []Cast {
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
	query := fmt.Sprintf(`
SELECT
	c.oid,
	quote_ident(sn.nspname) || '.' || quote_ident(st.typname) AS sourcetypefqn,
	quote_ident(tn.nspname) || '.' || quote_ident(tt.typname) AS targettypefqn,
	coalesce(quote_ident(n.nspname), '') AS functionschema,
	coalesce(quote_ident(p.proname), '') AS functionname,
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
WHERE (%s) OR (%s) OR (%s)
ORDER BY 1, 2;
`, argStr, SchemaFilterClause("sn"), SchemaFilterClause("tn"), SchemaFilterClause("n"))

	casts := make([]Cast, 0)
	err := connection.Select(&casts, query)
	utils.CheckError(err)
	if connection.Version.Before("5") {
		arguments, _ := GetFunctionArgsAndIdentArgs(connection)
		for i := range casts {
			oid := casts[i].FunctionOid
			casts[i].FunctionArgs = arguments[oid]
		}
	}
	return casts
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

func GetProceduralLanguages(connection *utils.DBConn) []ProceduralLanguage {
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
WHERE l.lanispl='t';
`
	query := `
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
WHERE l.lanispl='t';
`
	var err error
	if connection.Version.Before("5") {
		err = connection.Select(&results, version4query)
	} else {
		err = connection.Select(&results, query)
	}
	utils.CheckError(err)
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

func GetConversions(connection *utils.DBConn) []Conversion {
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
ORDER BY n.nspname, c.conname;`, SchemaFilterClause("n"))

	err := connection.Select(&results, query)
	utils.CheckError(err)
	return results
}

/*
 * When we retrieve function dependencies, we don't record a dependency of a
 * function on a base type if the function is part of the definition of the
 * base type, as we print out shell types for all base types at the beginning
 * of the backup and so do not need to consider those dependencies when sorting
 * functions and types.
 */
func ConstructFunctionDependencies(connection *utils.DBConn, functions []Function) []Function {
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
	utils.CheckError(err)
	for _, dependency := range results {
		dependencyMap[dependency.Oid] = append(dependencyMap[dependency.Oid], dependency.ReferencedObject)
	}
	for i := 0; i < len(functions); i++ {
		functions[i].DependsUpon = dependencyMap[functions[i].Oid]
	}
	return functions
}
