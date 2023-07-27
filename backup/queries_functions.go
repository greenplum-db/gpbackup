package backup

/*
 * This file contains structs and functions related to executing specific
 * queries to gather metadata for the objects handled in predata_general.go.
 */

import (
	"database/sql"
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
	ReturnsSet        bool `db:"proretset"`
	FunctionBody      string
	BinaryPath        string
	Arguments         sql.NullString
	IdentArgs         sql.NullString
	ResultType        sql.NullString
	Volatility        string  `db:"provolatile"`
	IsStrict          bool    `db:"proisstrict"`
	IsLeakProof       bool    `db:"proleakproof"`
	IsSecurityDefiner bool    `db:"prosecdef"`
	Config            string  `db:"proconfig"`
	Cost              float32 `db:"procost"`
	NumRows           float32 `db:"prorows"`
	DataAccess        string  `db:"prodataaccess"`
	Language          string
	Kind              string `db:"prokind"`     // GPDB 7+
	PlannerSupport    string `db:"prosupport"`  // GPDB 7+
	IsWindow          bool   `db:"proiswindow"` // before 7
	ExecLocation      string `db:"proexeclocation"`
	Parallel          string `db:"proparallel"` // GPDB 7+
	TransformTypes    string // GPDB 7+
}

func (f Function) GetMetadataEntry() (string, toc.MetadataEntry) {
	nameWithArgs := fmt.Sprintf("%s(%s)", f.Name, f.IdentArgs.String)
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
	return fmt.Sprintf("%s.%s(%s)", f.Schema, f.Name, f.IdentArgs.String)
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
			functions[i].Arguments.String = arguments[oid]
			functions[i].Arguments.Valid = true // Hardcode for GPDB 4.3 to fit sql.NullString
			functions[i].IdentArgs.String = arguments[oid]
			functions[i].IdentArgs.Valid = true // Hardcode for GPDB 4.3 to fit sql.NullString
			functions[i].ReturnsSet = returns[oid].ReturnsSet
			if tableArguments[oid] != "" {
				functions[i].ResultType.String = fmt.Sprintf("TABLE(%s)", tableArguments[oid])
				functions[i].ResultType.Valid = true // Hardcode for GPDB 4.3 to fit sql.NullString
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
	if connectionPool.Version.AtLeast("6") {
		// This excludes implicitly created functions. Currently this is only range type functions
		excludeImplicitFunctionsClause = `
	AND NOT EXISTS (
		SELECT 1 FROM pg_depend
		WHERE classid = 'pg_proc'::regclass::oid
			AND objid = p.oid AND deptype = 'i')`
	}

	locationAtts := ""
	if connectionPool.Version.Before("6") {
		locationAtts = "'a' AS proexeclocation,"
	} else if connectionPool.Version.Is("6") {
		locationAtts = "proiswindow,proexeclocation,proleakproof,"
	} else {
		locationAtts = "proexeclocation,proleakproof,proparallel,"
	}

	before7Query := fmt.Sprintf(`
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
		ORDER BY nspname, proname, identargs`, locationAtts,
		SchemaFilterClause("n"),
		ExtensionFilterClause("p"),
		excludeImplicitFunctionsClause)

	atLeast7Query := fmt.Sprintf(`
		SELECT p.oid,
			quote_ident(nspname) AS schema,
			quote_ident(p.proname) AS name,
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
			prokind,
			prosupport,
			l.lanname AS language,
            coalesce(array_to_string(ARRAY(SELECT 'FOR TYPE ' || nm.nspname || '.' || typ.typname
                from
                    unnest(p.protrftypes) as trf_unnest
                    left join pg_type typ
                         on trf_unnest = typ.oid
					left join pg_namespace nm
						on typ.typnamespace = nm.oid
                ), ', '), '') AS transformtypes
		FROM pg_proc p
			JOIN pg_catalog.pg_language l ON p.prolang = l.oid
			LEFT JOIN pg_namespace n ON p.pronamespace = n.oid
		WHERE %s
			AND prokind <> 'a'
			AND %s%s
		ORDER BY nspname, proname, identargs`, locationAtts,
		SchemaFilterClause("n"),
		ExtensionFilterClause("p"),
		excludeImplicitFunctionsClause)

	query := ""
	if connectionPool.Version.Before("7") {
		query = before7Query
	} else {
		query = atLeast7Query
	}

	results := make([]Function, 0)
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)

	err = PostProcessFunctionConfigs(results)
	gplog.FatalOnError(err)

	// Process kind value for GPDB7+ window functions, to ensure window
	// attribute is correctly set.
	// Remove all functions that have NULL arguments, NULL identity
	// arguments, or NULL result type. This can happen if the query
	// above is run and a concurrent function drop happens just
	// before the pg_get_function_* functions execute.
	verifiedResults := make([]Function, 0)
	for _, result := range results {
		if connectionPool.Version.AtLeast("7") && result.Kind == "w" {
			result.IsWindow = true
		}

		if result.Arguments.Valid && result.IdentArgs.Valid && result.ResultType.Valid {
			verifiedResults = append(verifiedResults, result)
		} else if connectionPool.Version.AtLeast("7") && result.Kind == "p" && !result.ResultType.Valid { // GPDB7+ stored procedure
			verifiedResults = append(verifiedResults, result)
		} else {
			gplog.Warn("Function '%s.%s' not backed up, most likely dropped after gpbackup had begun.", result.Schema, result.Name)
		}
	}

	return verifiedResults
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
	Arguments                  sql.NullString
	IdentArgs                  sql.NullString
	TransitionFunction         uint32 `db:"aggtransfn"`
	PreliminaryFunction        uint32 `db:"aggprelimfn"`
	CombineFunction            uint32 `db:"aggcombinefn"`
	SerialFunction             uint32 `db:"aggserialfn"`
	DeserialFunction           uint32 `db:"aggdeserialfn"`
	FinalFunction              uint32 `db:"aggfinalfn"`
	FinalFuncExtra             bool
	SortOperator               string
	SortOperatorSchema         string
	Hypothetical               bool   // GPDB < 7
	Kind                       string // GPDB7+
	TransitionDataType         string
	TransitionDataSize         int `db:"aggtransspace"`
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
	Finalmodify                string // GPDB7+
	Mfinalmodify               string // GPDB7+
	Parallel                   string // GPDB7+
}

func (a Aggregate) GetMetadataEntry() (string, toc.MetadataEntry) {
	identArgumentsStr := "*"
	if a.IdentArgs.String != "" {
		identArgumentsStr = a.IdentArgs.String
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
	if a.IdentArgs.String != "" {
		identArgumentsStr = a.IdentArgs.String
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

	version6query := fmt.Sprintf(`
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

	atLeast7Query := fmt.Sprintf(`
	SELECT p.oid,
		quote_ident(n.nspname) AS schema,
		p.proname AS name,
		p.proparallel as parallel,
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
		aggkind AS kind,
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
		coalesce(a.aggminitval, '') AS minitialvalue,
		a.aggfinalmodify AS finalmodify,
		a.aggmfinalmodify AS mfinalmodify
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
	if connectionPool.Version.Is("4") {
		query = version4query
	} else if connectionPool.Version.Is("5") {
		query = version5query
	} else if connectionPool.Version.Is("6") {
		query = version6query
	} else {
		query = atLeast7Query
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
			aggregates[i].Arguments.String = arguments[oid]
			aggregates[i].Arguments.Valid = true // Hardcode for GPDB 4.3 to fit sql.NullString
			aggregates[i].IdentArgs.String = arguments[oid]
			aggregates[i].IdentArgs.Valid = true // Hardcode for GPDB 4.3 to fit sql.NullString
		}

		return aggregates
	} else {
		// Remove all aggregates that have NULL arguments or NULL
		// identity arguments. This can happen if the query above
		// is run and a concurrent aggregate drop happens before
		// the pg_get_function_* functions execute.
		verifiedAggregates := make([]Aggregate, 0)
		for _, aggregate := range aggregates {
			if aggregate.Arguments.Valid && aggregate.IdentArgs.Valid {
				verifiedAggregates = append(verifiedAggregates, aggregate)
			} else {
				gplog.Warn("Aggregate '%s.%s' not backed up, most likely dropped after gpbackup had begun.", aggregate.Schema, aggregate.Name)
			}
		}

		return verifiedAggregates
	}
}

type FunctionInfo struct {
	Oid           uint32
	Name          string
	Schema        string
	QualifiedName string
	Arguments     sql.NullString
	IdentArgs     sql.NullString
	IsInternal    bool
}

func (info FunctionInfo) FQN() string {
	return fmt.Sprintf("%s(%s)", info.QualifiedName, info.IdentArgs.String)
}

func (info FunctionInfo) GetMetadataEntry() (string, toc.MetadataEntry) {
	nameWithArgs := fmt.Sprintf("%s(%s)", info.Name, info.IdentArgs.String)
	return "predata",
		toc.MetadataEntry{
			Schema:          info.Schema,
			Name:            nameWithArgs,
			ObjectType:      "FUNCTION",
			ReferenceObject: "",
			StartByte:       0,
			EndByte:         0,
		}
}

func GetFunctionOidToInfoMap(connectionPool *dbconn.DBConn) map[uint32]FunctionInfo {
	before5Query := `
	SELECT p.oid,
		quote_ident(n.nspname) AS schema,
		quote_ident(p.proname) AS name
	FROM pg_proc p
		LEFT JOIN pg_namespace n ON p.pronamespace = n.oid`
	atLeast5Query := `
	SELECT p.oid,
		quote_ident(n.nspname) AS schema,
		quote_ident(p.proname) AS name,
		pg_catalog.pg_get_function_arguments(p.oid) AS arguments,
		pg_catalog.pg_get_function_identity_arguments(p.oid) AS identargs
	FROM pg_proc p
		LEFT JOIN pg_namespace n ON p.pronamespace = n.oid`

	results := make([]FunctionInfo, 0)
	funcMap := make(map[uint32]FunctionInfo)
	var err error
	if connectionPool.Version.Before("5") {
		err = connectionPool.Select(&results, before5Query)
		arguments, _ := GetFunctionArgsAndIdentArgs(connectionPool)
		for i := range results {
			results[i].Arguments.String = arguments[results[i].Oid]
			results[i].Arguments.Valid = true // Hardcode for GPDB 4.3 to fit sql.NullString
			results[i].IdentArgs.String = arguments[results[i].Oid]
			results[i].IdentArgs.Valid = true // Hardcode for GPDB 4.3 to fit sql.NullString
		}
	} else {
		err = connectionPool.Select(&results, atLeast5Query)
	}
	gplog.FatalOnError(err)
	for _, funcInfo := range results {
		if !funcInfo.Arguments.Valid || !funcInfo.IdentArgs.Valid {
			gplog.Warn("Function '%s.%s' not backed up, most likely dropped after gpbackup had begun.", funcInfo.Schema, funcInfo.Name)
			continue
		}

		if funcInfo.Schema == "pg_catalog" {
			funcInfo.IsInternal = true
		}
		funcInfo.QualifiedName = utils.MakeFQN(funcInfo.Schema, funcInfo.Name)
		funcMap[funcInfo.Oid] = funcInfo
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

	query := fmt.Sprintf(`
	SELECT e.oid,
		quote_ident(extname) AS name,
		quote_ident(n.nspname) AS schema
	FROM pg_extension e
		JOIN pg_namespace n ON e.extnamespace = n.oid
	WHERE e.oid >= %d`, FIRST_NORMAL_OBJECT_ID)
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
	before5Query := `
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
	atLeast5Query := fmt.Sprintf(`
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

type Transform struct {
	Oid           uint32
	TypeNamespace string `db:"typnamespace"`
	TypeName      string `db:"typname"`
	LanguageName  string `db:"lanname"`
	FromSQLFunc   uint32 `db:"trffromsql"`
	ToSQLFunc     uint32 `db:"trftosql"`
}

func (trf Transform) GetMetadataEntry() (string, toc.MetadataEntry) {
	return "predata",
		toc.MetadataEntry{
			Schema:          "",
			Name:            "",
			ObjectType:      "TRANSFORM",
			ReferenceObject: "",
			StartByte:       0,
			EndByte:         0,
		}
}

func (trf Transform) GetUniqueID() UniqueID {
	return UniqueID{ClassID: PG_TRANSFORM_OID, Oid: trf.Oid}
}

func (trf Transform) FQN() string {
	return fmt.Sprintf("FOR %s.%s LANGUAGE %s", trf.TypeNamespace, trf.TypeName, trf.LanguageName)
}

func GetTransforms(connectionPool *dbconn.DBConn) []Transform {
	results := make([]Transform, 0)
	query := `
	SELECT trf.oid,
		quote_ident(ns.nspname) AS typnamespace,
		quote_ident(tp.typname) AS typname,
		l.lanname,
		trf.trffromsql::oid,
		trf.trftosql::oid
	FROM pg_transform trf
		JOIN pg_type tp ON trf.trftype=tp.oid
		JOIN pg_namespace ns ON tp.typnamespace = ns.oid
		JOIN pg_language l ON trf.trflang=l.oid;`

	err := connectionPool.Select(&results, query)
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

type StatisticExt struct {
	Oid         uint32
	Name        string
	Namespace   string // namespace that statistics object belongs to
	Owner       string
	TableSchema string // schema that table covered by statistics belongs to
	TableName   string // table covered by statistics
	Definition  string
}

func (se StatisticExt) GetMetadataEntry() (string, toc.MetadataEntry) {
	return "postdata",
		toc.MetadataEntry{
			Schema:          se.Namespace,
			Name:            se.Name,
			ObjectType:      "STATISTICS",
			ReferenceObject: utils.MakeFQN(se.TableSchema, se.TableName),
			StartByte:       0,
			EndByte:         0,
		}
}

func (se StatisticExt) GetUniqueID() UniqueID {
	return UniqueID{ClassID: PG_STATISTIC_EXT_OID, Oid: se.Oid}
}

func (se StatisticExt) FQN() string {
	return fmt.Sprintf("%s.%s", se.Namespace, se.Name)
}

func GetExtendedStatistics(connectionPool *dbconn.DBConn) []StatisticExt {
	results := make([]StatisticExt, 0)

	query := `
	SELECT 	se.oid,
			stxname AS name,
			regexp_replace(pg_catalog.pg_get_statisticsobjdef(se.oid), '(.* FROM ).*', '\1' || quote_ident(c.relnamespace::regnamespace::text) || '.' || quote_ident(c.relname)) AS definition,
			quote_ident(se.stxnamespace::regnamespace::text) AS namespace,
			se.stxowner::regrole AS owner,
			quote_ident(c.relnamespace::regnamespace::text) AS tableschema,
			quote_ident(c.relname) AS tablename
	FROM pg_catalog.pg_statistic_ext se
	JOIN pg_catalog.pg_class c ON se.stxrelid = c.oid;`
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)
	return results
}
