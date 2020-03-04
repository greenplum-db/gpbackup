package backup

/*
 * This file contains structs and functions related to executing specific
 * queries to gather metadata for the objects handled in predata_general.go.
 */

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpbackup/toc"
	"github.com/greenplum-db/gpbackup/utils"
)

type Function struct {
	Oid               uint32
	Schema            string
	Name              string
	ReturnsSet        bool    `db:"proretset"`
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
	IsWindow          bool    `db:"proiswindow"`
	ExecLocation      string  `db:"proexeclocation"`
}

func (f Function) GetMetadataEntry() (string, toc.MetadataEntry) {
	nameWithArgs := fmt.Sprintf("%s(%s)", f.Name, f.IdentArgs)
	return "predata",
		toc.MetadataEntry{
			Schema:          f.Schema,
			Name:            nameWithArgs,
			ObjectType:      "FUNCTION",
			ReferenceObject: "",
			StartByte:       0,
			EndByte:         0,
		}
}

func (f Function) GetUniqueID() UniqueID {
	return UniqueID{ClassID: PG_PROC_OID, Oid: f.Oid}
}

func (f Function) FQN() string {
	/*
	 * We need to include arguments to differentiate functions with the same name
	 */
	return fmt.Sprintf("%s.%s(%s)", f.Schema, f.Name, f.IdentArgs)
}

/*
 * The functions pg_get_function_arguments, pg_getfunction_identity_arguments,
 * and pg_get_function_result were introduced in GPDB 5, so we can use those
 * functions to retrieve arguments, identity arguments, and return values in
 * 5 or later but in GPDB 4.3 we must query pg_proc directly and construct
 * those values here.
 */
func GetFunctionsAllVersions(connectionPool *dbconn.DBConn) []Function {
	var functions []Function
	if connectionPool.Version.Before("5") {
		functions = GetFunctions4(connectionPool)
		arguments, tableArguments := GetFunctionArgsAndIdentArgs(connectionPool)
		returns := GetFunctionReturnTypes(connectionPool)
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
	} else {
		functions = GetFunctions(connectionPool)
	}
	return functions
}

func GetFunctions(connectionPool *dbconn.DBConn) []Function {
	excludeImplicitFunctionsClause := ""
	masterAtts := "'a' AS proexeclocation,"
	if connectionPool.Version.AtLeast("6") {
		masterAtts = "proiswindow,proexeclocation,proleakproof,"
		// This excludes implicitly created functions. Currently this is only range type functions
		excludeImplicitFunctionsClause = `
	AND NOT EXISTS (
		SELECT 1 FROM pg_depend
		WHERE classid = 'pg_proc'::regclass::oid
			AND objid = p.oid AND deptype = 'i')`
	}
	query := fmt.Sprintf(`
	SELECT p.oid,
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
		coalesce(array_to_string(ARRAY(SELECT 'SET ' || option_name || ' TO ' || option_value
			FROM pg_options_to_table(proconfig)), ' '), '') AS proconfig,
		procost,
		prorows,
		prodataaccess,
		l.lanname AS language
	FROM pg_proc p
		JOIN pg_catalog.pg_language l ON p.prolang = l.oid
		LEFT JOIN pg_namespace n ON p.pronamespace = n.oid
	WHERE %s
		AND proisagg = 'f'
		AND %s%s
	ORDER BY nspname, proname, identargs`, masterAtts,
		SchemaFilterClause("n"),
		ExtensionFilterClause("p"),
		excludeImplicitFunctionsClause)

	results := make([]Function, 0)
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)

	err = PostProcessFunctionConfigs(results)
	gplog.FatalOnError(err)

	return results
}

func PostProcessFunctionConfigs(allFunctions []Function) error {
	setToNameValuePattern := regexp.MustCompile(`^SET (.*) TO (.*)$`)

	for i, function := range allFunctions {
		if function.Config == "" {
			continue
		}

		captures := setToNameValuePattern.FindStringSubmatch(function.Config)
		if len(captures) != 3 {
			return fmt.Errorf("Function config does not match syntax expectations. Function was: %v", function)
		}
		gucName := strings.ToLower(captures[1])
		gucValue := captures[2]
		quotedValue := QuoteGUCValue(gucName, gucValue)

		// write to struct by referencing the slice rather than the readonly 'function' copy
		allFunctions[i].Config = fmt.Sprintf(`SET %s TO %s`, gucName, quotedValue)
	}
	return nil
}

func QuoteGUCValue(name, value string) string {
	/*
	 * GUC tools have one way to stuff many strings into a single string, with it own
	 * system of quotation. SQL quotation is different, so we have to unwind the
	 * GUC quoting system and use the SQL system.
	 * We are modeling this function after SplitGUCList in psql/dumputils.c
	 */
	var result string
	if name == "temp_tablespaces" ||
		name == "session_preload_libraries" ||
		name == "shared_preload_libraries" ||
		name == "local_preload_libraries" ||
		name == "search_path" {
		strSplit := strings.Split(value, ",")
		for i, item := range strSplit {
			item = strings.Trim(item, " ")
			item = UnescapeDoubleQuote(item)
			item = `'` + item + `'`
			strSplit[i] = item
		}
		result = strings.Join(strSplit, ", ")
	} else {
		result = `'` + value + `'`
	}

	return result
}

func UnescapeDoubleQuote(value string) string {
	result := value
	if len(value) > 1 && value[0] == '"' && value[len(value)-1] == '"' {
		result = value[1 : len(value)-1]
		result = strings.Replace(result, `""`, `"`, -1)
	}
	return result
}

/*
 * In addition to lacking the pg_get_function_* functions, GPDB 4.3 lacks
 * several columns in pg_proc compared to GPDB 5, so we don't retrieve those.
 */
func GetFunctions4(connectionPool *dbconn.DBConn) []Function {
	query := fmt.Sprintf(`
	SELECT p.oid,
		quote_ident(nspname) AS schema,
		quote_ident(proname) AS name,
		proretset,
		coalesce(prosrc, '') AS functionbody,
		CASE WHEN probin = '-' THEN '' ELSE probin END AS binarypath,
		provolatile,
		proisstrict,
		prosecdef,
		'a' AS proexeclocation,
		(SELECT lanname FROM pg_catalog.pg_language WHERE oid = prolang) AS language
	FROM pg_proc p
		LEFT JOIN pg_namespace n ON p.pronamespace = n.oid
	WHERE %s
		AND proisagg = 'f'
	ORDER BY nspname, proname`, SchemaFilterClause("n"))

	results := make([]Function, 0)
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)
	return results
}

/*
 * Functions do not have default argument values in GPDB 4.3, so there is no
 * difference between a function's "arguments" and "identity arguments" and
 * we can use the same map for both fields.
 */
func GetFunctionArgsAndIdentArgs(connectionPool *dbconn.DBConn) (map[uint32]string, map[uint32]string) {
	query := `
	SELECT p.oid,
		CASE WHEN proallargtypes IS NOT NULL THEN format_type(unnest(proallargtypes), NULL)
			ELSE format_type(unnest(proargtypes), NULL) END AS type,
		CASE WHEN proargnames IS NOT NULL THEN quote_ident(unnest(proargnames)) ELSE '' END AS name,
		CASE WHEN proargmodes IS NOT NULL THEN unnest(proargmodes) ELSE '' END AS mode
	FROM pg_proc p
		JOIN pg_namespace n ON p.pronamespace = n.oid`

	results := make([]struct {
		Oid  uint32
		Type string
		Name string
		Mode string
	}, 0)
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)

	argMap := make(map[uint32]string)
	tableArgMap := make(map[uint32]string)
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

func GetFunctionReturnTypes(connectionPool *dbconn.DBConn) map[uint32]Function {
	query := fmt.Sprintf(`
	SELECT p.oid,
		proretset,
		CASE WHEN proretset = 't' THEN 'SETOF ' || format_type(prorettype, NULL)
			ELSE format_type(prorettype, NULL) END AS resulttype
	FROM pg_proc p
		JOIN pg_namespace n ON p.pronamespace = n.oid
	WHERE %s`, SchemaFilterClause("n"))

	results := make([]Function, 0)
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)

	returnMap := make(map[uint32]Function)
	for _, result := range results {
		returnMap[result.Oid] = result
	}
	return returnMap
}

type Aggregate struct {
	Oid                        uint32
	Schema                     string
	Name                       string
	Arguments                  string
	IdentArgs                  string
	TransitionFunction         uint32 `db:"aggtransfn"`
	PreliminaryFunction        uint32 `db:"aggprelimfn"`
	CombineFunction            uint32 `db:"aggcombinefn"`
	SerialFunction             uint32 `db:"aggserialfn"`
	DeserialFunction           uint32 `db:"aggdeserialfn"`
	FinalFunction              uint32 `db:"aggfinalfn"`
	FinalFuncExtra             bool
	SortOperator               string
	SortOperatorSchema         string
	Hypothetical               bool
	TransitionDataType         string
	TransitionDataSize         int    `db:"aggtransspace"`
	InitialValue               string
	InitValIsNull              bool
	IsOrdered                  bool   `db:"aggordered"`
	MTransitionFunction        uint32 `db:"aggmtransfn"`
	MInverseTransitionFunction uint32 `db:"aggminvtransfn"`
	MTransitionDataType        string
	MTransitionDataSize        int    `db:"aggmtransspace"`
	MFinalFunction             uint32 `db:"aggmfinalfn"`
	MFinalFuncExtra            bool
	MInitialValue              string
	MInitValIsNull             bool
}

func (a Aggregate) GetMetadataEntry() (string, toc.MetadataEntry) {
	identArgumentsStr := "*"
	if a.IdentArgs != "" {
		identArgumentsStr = a.IdentArgs
	}
	aggWithArgs := fmt.Sprintf("%s(%s)", a.Name, identArgumentsStr)
	return "predata",
		toc.MetadataEntry{
			Schema:          a.Schema,
			Name:            aggWithArgs,
			ObjectType:      "AGGREGATE",
			ReferenceObject: "",
			StartByte:       0,
			EndByte:         0,
		}
}

func (a Aggregate) GetUniqueID() UniqueID {
	return UniqueID{ClassID: PG_AGGREGATE_OID, Oid: a.Oid}
}

func (a Aggregate) FQN() string {
	identArgumentsStr := "*"
	if a.IdentArgs != "" {
		identArgumentsStr = a.IdentArgs
	}
	return fmt.Sprintf("%s.%s(%s)", a.Schema, a.Name, identArgumentsStr)
}

func GetAggregates(connectionPool *dbconn.DBConn) []Aggregate {
	version4query := fmt.Sprintf(`
	SELECT p.oid,
		quote_ident(n.nspname) AS schema,
		p.proname AS name,
		'' AS arguments,
		'' AS identargs,
		a.aggtransfn::regproc::oid,
		a.aggprelimfn::regproc::oid,
		a.aggfinalfn::regproc::oid,
		coalesce(o.oprname, '') AS sortoperator,
		coalesce(quote_ident(opn.nspname), '') AS sortoperatorschema,
		format_type(a.aggtranstype, NULL) as transitiondatatype,
		coalesce(a.agginitval, '') AS initialvalue,
		(a.agginitval IS NULL) AS initvalisnull,
		true AS minitvalisnull,
		a.aggordered
	FROM pg_aggregate a
		LEFT JOIN pg_proc p ON a.aggfnoid = p.oid
		LEFT JOIN pg_namespace n ON p.pronamespace = n.oid
		LEFT JOIN pg_operator o ON a.aggsortop = o.oid
		LEFT JOIN pg_namespace opn ON o.oprnamespace = opn.oid
	WHERE %s`, SchemaFilterClause("n"))

	version5query := fmt.Sprintf(`
	SELECT p.oid,
		quote_ident(n.nspname) AS schema,
		p.proname AS name,
		pg_catalog.pg_get_function_arguments(p.oid) AS arguments,
		pg_catalog.pg_get_function_identity_arguments(p.oid) AS identargs,
		a.aggtransfn::regproc::oid,
		a.aggprelimfn::regproc::oid,
		a.aggfinalfn::regproc::oid,
		coalesce(o.oprname, '') AS sortoperator,
		coalesce(quote_ident(opn.nspname), '') AS sortoperatorschema, 
		format_type(a.aggtranstype, NULL) as transitiondatatype,
		coalesce(a.agginitval, '') AS initialvalue,
		(a.agginitval IS NULL) AS initvalisnull,
		true AS minitvalisnull,
		a.aggordered
	FROM pg_aggregate a
		LEFT JOIN pg_proc p ON a.aggfnoid = p.oid
		LEFT JOIN pg_namespace n ON p.pronamespace = n.oid
		LEFT JOIN pg_operator o ON a.aggsortop = o.oid
		LEFT JOIN pg_namespace opn ON o.oprnamespace = opn.oid
	WHERE %s
		AND %s`,
		SchemaFilterClause("n"), ExtensionFilterClause("p"))

	masterQuery := fmt.Sprintf(`
	SELECT p.oid,
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
		coalesce(o.oprname, '') AS sortoperator,
		coalesce(quote_ident(opn.nspname), '') AS sortoperatorschema, 
		(a.aggkind = 'h') AS hypothetical,
		format_type(a.aggtranstype, NULL) as transitiondatatype,
		aggtransspace,
		coalesce(a.agginitval, '') AS initialvalue,
		(a.agginitval IS NULL) AS initvalisnull,
		a.aggmtransfn::regproc::oid,
		a.aggminvtransfn::regproc::oid,
		a.aggmfinalfn::regproc::oid,
		a.aggmfinalextra AS mfinalfuncextra,
		format_type(a.aggmtranstype, NULL) as mtransitiondatatype,
		aggmtransspace,
		(a.aggminitval IS NULL) AS minitvalisnull,
		coalesce(a.aggminitval, '') AS minitialvalue
	FROM pg_aggregate a
		LEFT JOIN pg_proc p ON a.aggfnoid = p.oid
		LEFT JOIN pg_namespace n ON p.pronamespace = n.oid
		LEFT JOIN pg_operator o ON a.aggsortop = o.oid
		LEFT JOIN pg_namespace opn ON o.oprnamespace = opn.oid
	WHERE %s
		AND %s`,
		SchemaFilterClause("n"), ExtensionFilterClause("p"))

	aggregates := make([]Aggregate, 0)
	query := ""
	if connectionPool.Version.Before("5") {
		query = version4query
	} else if connectionPool.Version.Before("6") {
		query = version5query
	} else {
		query = masterQuery
	}
	err := connectionPool.Select(&aggregates, query)
	gplog.FatalOnError(err)
	for i := range aggregates {
		if aggregates[i].MTransitionDataType == "-" {
			aggregates[i].MTransitionDataType = ""
		}
	}
	if connectionPool.Version.Before("5") {
		arguments, _ := GetFunctionArgsAndIdentArgs(connectionPool)
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
	IdentArgs     string
	IsInternal    bool
}

func GetFunctionOidToInfoMap(connectionPool *dbconn.DBConn) map[uint32]FunctionInfo {
	version4query := `
	SELECT p.oid,
		quote_ident(n.nspname) AS schema,
		quote_ident(p.proname) AS name
	FROM pg_proc p
		LEFT JOIN pg_namespace n ON p.pronamespace = n.oid`
	query := `
	SELECT p.oid,
		quote_ident(n.nspname) AS schema,
		quote_ident(p.proname) AS name,
		pg_catalog.pg_get_function_arguments(p.oid) AS arguments,
		pg_catalog.pg_get_function_identity_arguments(p.oid) AS identargs
	FROM pg_proc p
		LEFT JOIN pg_namespace n ON p.pronamespace = n.oid`

	results := make([]struct {
		Oid       uint32
		Schema    string
		Name      string
		Arguments string
		IdentArgs string
	}, 0)
	funcMap := make(map[uint32]FunctionInfo)
	var err error
	if connectionPool.Version.Before("5") {
		err = connectionPool.Select(&results, version4query)
		arguments, _ := GetFunctionArgsAndIdentArgs(connectionPool)
		for i := range results {
			results[i].Arguments = arguments[results[i].Oid]
		}
	} else {
		err = connectionPool.Select(&results, query)
	}
	gplog.FatalOnError(err)
	for _, function := range results {
		fqn := utils.MakeFQN(function.Schema, function.Name)

		isInternal := false
		if function.Schema == "pg_catalog" {
			isInternal = true
		}
		funcInfo := FunctionInfo{
			QualifiedName: fqn,
			Arguments: function.Arguments,
			IdentArgs: function.IdentArgs,
			IsInternal: isInternal}
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

func (c Cast) GetMetadataEntry() (string, toc.MetadataEntry) {
	castStr := fmt.Sprintf("(%s AS %s)", c.SourceTypeFQN, c.TargetTypeFQN)
	filterSchema := "pg_catalog"
	if c.CastMethod == "f" {
		filterSchema = c.FunctionSchema // Use the function's schema to allow restore filtering
	}
	return "predata",
		toc.MetadataEntry{
			Schema:          filterSchema,
			Name:            castStr,
			ObjectType:      "CAST",
			ReferenceObject: "",
			StartByte:       0,
			EndByte:         0,
		}
}

func (c Cast) GetUniqueID() UniqueID {
	return UniqueID{ClassID: PG_CAST_OID, Oid: c.Oid}
}

func (c Cast) FQN() string {
	return fmt.Sprintf("(%s AS %s)", c.SourceTypeFQN, c.TargetTypeFQN)
}

func GetCasts(connectionPool *dbconn.DBConn) []Cast {
	/* This query retrieves all casts where either the source type, the target
	 * type, or the cast function is user-defined.
	 */
	argStr := ""
	if connectionPool.Version.Before("5") {
		argStr = `'' AS functionargs, coalesce(p.oid, 0::oid) AS functionoid,`
	} else {
		argStr = `coalesce(pg_get_function_arguments(p.oid), '') AS functionargs,`
	}
	methodStr := ""
	if connectionPool.Version.AtLeast("6") {
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
	ORDER BY 1, 2`, argStr, methodStr,
		SchemaFilterClause("sn"), SchemaFilterClause("tn"),
		SchemaFilterClause("n"), ExtensionFilterClause("c"))

	casts := make([]Cast, 0)
	err := connectionPool.Select(&casts, query)
	gplog.FatalOnError(err)
	if connectionPool.Version.Before("5") {
		arguments, _ := GetFunctionArgsAndIdentArgs(connectionPool)
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

func (e Extension) GetMetadataEntry() (string, toc.MetadataEntry) {
	return "predata",
		toc.MetadataEntry{
			Schema:          "",
			Name:            e.Name,
			ObjectType:      "EXTENSION",
			ReferenceObject: "",
			StartByte:       0,
			EndByte:         0,
		}
}

func (e Extension) GetUniqueID() UniqueID {
	return UniqueID{ClassID: PG_EXTENSION_OID, Oid: e.Oid}
}

func (e Extension) FQN() string {
	return e.Name
}

func GetExtensions(connectionPool *dbconn.DBConn) []Extension {
	results := make([]Extension, 0)

	query := `
	SELECT e.oid,
		quote_ident(extname) AS name,
		quote_ident(n.nspname) AS schema
	FROM pg_extension e
		JOIN pg_namespace n ON e.extnamespace = n.oid`
	err := connectionPool.Select(&results, query)
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

func (pl ProceduralLanguage) GetMetadataEntry() (string, toc.MetadataEntry) {
	return "predata",
		toc.MetadataEntry{
			Schema:          "",
			Name:            pl.Name,
			ObjectType:      "LANGUAGE",
			ReferenceObject: "",
			StartByte:       0,
			EndByte:         0,
		}
}

func (pl ProceduralLanguage) GetUniqueID() UniqueID {
	return UniqueID{ClassID: PG_LANGUAGE_OID, Oid: pl.Oid}
}

func (pl ProceduralLanguage) FQN() string {
	return pl.Name
}

func GetProceduralLanguages(connectionPool *dbconn.DBConn) []ProceduralLanguage {
	results := make([]ProceduralLanguage, 0)
	// Languages are owned by the bootstrap superuser, OID 10
	version4query := `
	SELECT oid,
		quote_ident(l.lanname) AS name,
		pg_get_userbyid(10) AS owner, 
		l.lanispl,
		l.lanpltrusted,
		l.lanplcallfoid::regprocedure::oid,
		0 AS laninline,
		l.lanvalidator::regprocedure::oid
	FROM pg_language l
	WHERE l.lanispl='t'
		AND l.lanname != 'plpgsql'`
	query := fmt.Sprintf(`
	SELECT oid,
		quote_ident(l.lanname) AS name,
		pg_get_userbyid(l.lanowner) AS owner,
		l.lanispl,
		l.lanpltrusted,
		l.lanplcallfoid::regprocedure::oid,
		l.laninline::regprocedure::oid,
		l.lanvalidator::regprocedure::oid
	FROM pg_language l
	WHERE l.lanispl='t'
		AND l.lanname != 'plpgsql'
		AND %s`, ExtensionFilterClause("l"))
	var err error
	if connectionPool.Version.Before("5") {
		err = connectionPool.Select(&results, version4query)
	} else {
		err = connectionPool.Select(&results, query)
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

func (c Conversion) GetMetadataEntry() (string, toc.MetadataEntry) {
	return "predata",
		toc.MetadataEntry{
			Schema:          c.Schema,
			Name:            c.Name,
			ObjectType:      "CONVERSION",
			ReferenceObject: "",
			StartByte:       0,
			EndByte:         0,
		}
}

func (c Conversion) GetUniqueID() UniqueID {
	return UniqueID{ClassID: PG_CONVERSION_OID, Oid: c.Oid}
}

func (c Conversion) FQN() string {
	return utils.MakeFQN(c.Schema, c.Name)
}

func GetConversions(connectionPool *dbconn.DBConn) []Conversion {
	results := make([]Conversion, 0)
	query := fmt.Sprintf(`
	SELECT c.oid,
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
	ORDER BY n.nspname, c.conname`, SchemaFilterClause("n"), ExtensionFilterClause("c"))

	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)
	return results
}

type ForeignDataWrapper struct {
	Oid       uint32
	Name      string
	Handler   uint32
	Validator uint32
	Options   string
}

func (fdw ForeignDataWrapper) GetMetadataEntry() (string, toc.MetadataEntry) {
	return "predata",
		toc.MetadataEntry{
			Schema:          "",
			Name:            fdw.Name,
			ObjectType:      "FOREIGN DATA WRAPPER",
			ReferenceObject: "",
			StartByte:       0,
			EndByte:         0,
		}
}

func (fdw ForeignDataWrapper) GetUniqueID() UniqueID {
	return UniqueID{ClassID: PG_FOREIGN_DATA_WRAPPER_OID, Oid: fdw.Oid}
}

func (fdw ForeignDataWrapper) FQN() string {
	return fdw.Name
}

func GetForeignDataWrappers(connectionPool *dbconn.DBConn) []ForeignDataWrapper {
	results := make([]ForeignDataWrapper, 0)
	query := fmt.Sprintf(`
	SELECT oid,
		quote_ident(fdwname) AS name,
		fdwvalidator AS validator,
		fdwhandler AS handler,
		array_to_string(ARRAY(
			SELECT pg_catalog.quote_ident(option_name) || ' ' || pg_catalog.quote_literal(option_value)
			FROM pg_options_to_table(fdwoptions) ORDER BY option_name), ', ') AS options
	FROM pg_foreign_data_wrapper
	WHERE oid >= %d AND %s`, FIRST_NORMAL_OBJECT_ID, ExtensionFilterClause(""))

	err := connectionPool.Select(&results, query)
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

func (fs ForeignServer) GetMetadataEntry() (string, toc.MetadataEntry) {
	return "predata",
		toc.MetadataEntry{
			Schema:          "",
			Name:            fs.Name,
			ObjectType:      "FOREIGN SERVER",
			ReferenceObject: "",
			StartByte:       0,
			EndByte:         0,
		}
}

func (fs ForeignServer) GetUniqueID() UniqueID {
	return UniqueID{ClassID: PG_FOREIGN_SERVER_OID, Oid: fs.Oid}
}

func (fs ForeignServer) FQN() string {
	return fs.Name
}

func GetForeignServers(connectionPool *dbconn.DBConn) []ForeignServer {
	results := make([]ForeignServer, 0)
	query := fmt.Sprintf(`
	SELECT fs.oid,
		quote_ident(fs.srvname) AS name,
		coalesce(fs.srvtype, '') AS type,
		coalesce(fs.srvversion, '') AS version,
		quote_ident(fdw.fdwname) AS foreigndatawrapper,
		array_to_string(ARRAY(
			SELECT pg_catalog.quote_ident(option_name) || ' ' || pg_catalog.quote_literal(option_value)
			FROM pg_options_to_table(fs.srvoptions) ORDER BY option_name), ', ') AS options
	FROM pg_foreign_server fs
		LEFT JOIN pg_foreign_data_wrapper fdw ON fdw.oid = srvfdw
	WHERE fs.oid >= %d AND %s`, FIRST_NORMAL_OBJECT_ID, ExtensionFilterClause("fs"))

	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)
	return results
}

type UserMapping struct {
	Oid     uint32
	User    string
	Server  string
	Options string
}

func (um UserMapping) GetMetadataEntry() (string, toc.MetadataEntry) {
	return "predata",
		toc.MetadataEntry{
			Schema:          "",
			Name:            um.FQN(),
			ObjectType:      "USER MAPPING",
			ReferenceObject: "",
			StartByte:       0,
			EndByte:         0,
		}
}

func (um UserMapping) GetUniqueID() UniqueID {
	return UniqueID{ClassID: PG_USER_MAPPING_OID, Oid: um.Oid}
}

func (um UserMapping) FQN() string {
	// User mappings don't have a unique name, so we construct an arbitrary identifier
	return fmt.Sprintf("%s ON %s", um.User, um.Server)
}

func GetUserMappings(connectionPool *dbconn.DBConn) []UserMapping {
	query := `
	SELECT um.umid AS oid,
		quote_ident(um.usename) AS user,
		quote_ident(um.srvname) AS server,
		array_to_string(ARRAY(
			SELECT pg_catalog.quote_ident(option_name) || ' ' || pg_catalog.quote_literal(option_value)
			FROM pg_options_to_table(um.umoptions) ORDER BY option_name), ', ') AS options
	FROM pg_user_mappings um
	WHERE um.umid NOT IN (select objid from pg_depend where deptype = 'e')
	ORDER by um.usename`

	results := make([]UserMapping, 0)
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)
	return results
}
