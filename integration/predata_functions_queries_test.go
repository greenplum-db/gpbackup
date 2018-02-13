package integration

import (
	"github.com/greenplum-db/gp-common-go-libs/structmatcher"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup integration tests", func() {
	Describe("GetFunctions5", func() {
		BeforeEach(func() {
			testutils.SkipIfBefore5(connection)
		})
		It("returns a slice of functions", func() {
			testhelper.AssertQueryRuns(connection, `CREATE FUNCTION add(integer, integer) RETURNS integer
AS 'SELECT $1 + $2'
LANGUAGE SQL`)
			defer testhelper.AssertQueryRuns(connection, "DROP FUNCTION add(integer, integer)")
			testhelper.AssertQueryRuns(connection, `
CREATE FUNCTION append(integer, integer) RETURNS SETOF record
AS 'SELECT ($1, $2)'
LANGUAGE SQL
SECURITY DEFINER
STRICT
STABLE
COST 200
ROWS 200
SET search_path = pg_temp
MODIFIES SQL DATA
`)
			defer testhelper.AssertQueryRuns(connection, "DROP FUNCTION append(integer, integer)")
			testhelper.AssertQueryRuns(connection, "COMMENT ON FUNCTION append(integer, integer) IS 'this is a function comment'")

			results := backup.GetFunctionsMaster(connection)

			addFunction := backup.Function{
				Schema: "public", Name: "add", ReturnsSet: false, FunctionBody: "SELECT $1 + $2",
				BinaryPath: "", Arguments: "integer, integer", IdentArgs: "integer, integer", ResultType: "integer",
				Volatility: "v", IsStrict: false, IsSecurityDefiner: false, Config: "", Cost: 100, NumRows: 0, DataAccess: "c",
				Language: "sql", ExecLocation: "a"}
			appendFunction := backup.Function{
				Schema: "public", Name: "append", ReturnsSet: true, FunctionBody: "SELECT ($1, $2)",
				BinaryPath: "", Arguments: "integer, integer", IdentArgs: "integer, integer", ResultType: "SETOF record",
				Volatility: "s", IsStrict: true, IsSecurityDefiner: true, Config: "SET search_path TO pg_temp", Cost: 200,
				NumRows: 200, DataAccess: "m", Language: "sql", ExecLocation: "a"}

			Expect(len(results)).To(Equal(2))
			structmatcher.ExpectStructsToMatchExcluding(&results[0], &addFunction, "Oid")
			structmatcher.ExpectStructsToMatchExcluding(&results[1], &appendFunction, "Oid")
		})
		It("returns a slice of functions in a specific schema", func() {
			testhelper.AssertQueryRuns(connection, `CREATE FUNCTION add(integer, integer) RETURNS integer
AS 'SELECT $1 + $2'
LANGUAGE SQL`)
			defer testhelper.AssertQueryRuns(connection, "DROP FUNCTION add(integer, integer)")
			testhelper.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
			defer testhelper.AssertQueryRuns(connection, "DROP SCHEMA testschema")
			testhelper.AssertQueryRuns(connection, `CREATE FUNCTION testschema.add(integer, integer) RETURNS integer
AS 'SELECT $1 + $2'
LANGUAGE SQL`)
			defer testhelper.AssertQueryRuns(connection, "DROP FUNCTION testschema.add(integer, integer)")

			addFunction := backup.Function{
				Schema: "testschema", Name: "add", ReturnsSet: false, FunctionBody: "SELECT $1 + $2",
				BinaryPath: "", Arguments: "integer, integer", IdentArgs: "integer, integer", ResultType: "integer",
				Volatility: "v", IsStrict: false, IsSecurityDefiner: false, Config: "", Cost: 100, NumRows: 0, DataAccess: "c",
				Language: "sql", ExecLocation: "a"}
			backup.SetIncludeSchemas([]string{"testschema"})
			results := backup.GetFunctionsMaster(connection)

			Expect(len(results)).To(Equal(1))
			structmatcher.ExpectStructsToMatchExcluding(&results[0], &addFunction, "Oid")
		})
		It("returns a window function", func() {
			testutils.SkipIfBefore6(connection)
			testhelper.AssertQueryRuns(connection, `CREATE FUNCTION add(integer, integer) RETURNS integer
AS 'SELECT $1 + $2'
LANGUAGE SQL WINDOW`)
			defer testhelper.AssertQueryRuns(connection, "DROP FUNCTION add(integer, integer)")

			results := backup.GetFunctionsMaster(connection)

			windowFunction := backup.Function{
				Schema: "public", Name: "add", ReturnsSet: false, FunctionBody: "SELECT $1 + $2",
				BinaryPath: "", Arguments: "integer, integer", IdentArgs: "integer, integer", ResultType: "integer",
				Volatility: "v", IsStrict: false, IsSecurityDefiner: false, Config: "", Cost: 100, NumRows: 0, DataAccess: "c",
				Language: "sql", IsWindow: true, ExecLocation: "a"}

			Expect(len(results)).To(Equal(1))
			structmatcher.ExpectStructsToMatchExcluding(&results[0], &windowFunction, "Oid")
		})
		It("returns a function to execute on master and all segments", func() {
			testutils.SkipIfBefore6(connection)
			testhelper.AssertQueryRuns(connection, `CREATE FUNCTION srf_on_master(integer, integer) RETURNS integer
AS 'SELECT $1 + $2'
LANGUAGE SQL WINDOW
EXECUTE ON MASTER;`)
			defer testhelper.AssertQueryRuns(connection, "DROP FUNCTION srf_on_master(integer, integer)")
			testhelper.AssertQueryRuns(connection, `CREATE FUNCTION srf_on_all_segments(integer, integer) RETURNS integer
AS 'SELECT $1 + $2'
LANGUAGE SQL WINDOW
EXECUTE ON ALL SEGMENTS;`)
			defer testhelper.AssertQueryRuns(connection, "DROP FUNCTION srf_on_all_segments(integer, integer)")

			results := backup.GetFunctionsMaster(connection)

			srfOnMasterFunction := backup.Function{
				Schema: "public", Name: "srf_on_master", ReturnsSet: false, FunctionBody: "SELECT $1 + $2",
				BinaryPath: "", Arguments: "integer, integer", IdentArgs: "integer, integer", ResultType: "integer",
				Volatility: "v", IsStrict: false, IsSecurityDefiner: false, Config: "", Cost: 100, NumRows: 0, DataAccess: "c",
				Language: "sql", IsWindow: true, ExecLocation: "m"}
			srfOnAllSegmentsFunction := backup.Function{
				Schema: "public", Name: "srf_on_all_segments", ReturnsSet: false, FunctionBody: "SELECT $1 + $2",
				BinaryPath: "", Arguments: "integer, integer", IdentArgs: "integer, integer", ResultType: "integer",
				Volatility: "v", IsStrict: false, IsSecurityDefiner: false, Config: "", Cost: 100, NumRows: 0, DataAccess: "c",
				Language: "sql", IsWindow: true, ExecLocation: "s"}

			Expect(len(results)).To(Equal(2))
			structmatcher.ExpectStructsToMatchExcluding(&results[0], &srfOnAllSegmentsFunction, "Oid")
			structmatcher.ExpectStructsToMatchExcluding(&results[1], &srfOnMasterFunction, "Oid")
		})
	})
	Describe("GetFunctions4", func() {
		BeforeEach(func() {
			testutils.SkipIfNot4(connection)
		})
		It("returns a slice of functions", func() {
			testhelper.AssertQueryRuns(connection, `CREATE FUNCTION add(numeric, integer) RETURNS numeric
AS 'SELECT $1 + $2'
LANGUAGE SQL`)
			defer testhelper.AssertQueryRuns(connection, "DROP FUNCTION add(numeric, integer)")
			testhelper.AssertQueryRuns(connection, `
CREATE FUNCTION append(float, integer) RETURNS SETOF record
AS 'SELECT ($1, $2)'
LANGUAGE SQL
SECURITY DEFINER
STRICT
STABLE
`)
			defer testhelper.AssertQueryRuns(connection, "DROP FUNCTION append(float, integer)")
			testhelper.AssertQueryRuns(connection, "COMMENT ON FUNCTION append(float, integer) IS 'this is a function comment'")
			testhelper.AssertQueryRuns(connection, `CREATE FUNCTION "specChar"(t text, "precision" double precision) RETURNS double precision AS $$BEGIN RETURN precision + 1; END;$$ LANGUAGE PLPGSQL;`)
			defer testhelper.AssertQueryRuns(connection, `DROP FUNCTION "specChar"(text, double precision)`)

			results := backup.GetFunctions4(connection)

			addFunction := backup.Function{
				Schema: "public", Name: "add", ReturnsSet: false, FunctionBody: "SELECT $1 + $2",
				BinaryPath: "", Arguments: "", IdentArgs: "", ResultType: "",
				Volatility: "v", IsStrict: false, IsSecurityDefiner: false, NumRows: 0, Language: "sql", ExecLocation: "a"}
			appendFunction := backup.Function{
				Schema: "public", Name: "append", ReturnsSet: true, FunctionBody: "SELECT ($1, $2)",
				BinaryPath: "", Arguments: "", IdentArgs: "", ResultType: "",
				Volatility: "s", IsStrict: true, IsSecurityDefiner: true, Language: "sql", ExecLocation: "a"}
			specCharFunction := backup.Function{
				Schema: "public", Name: `"specChar"`, ReturnsSet: false, FunctionBody: "BEGIN RETURN precision + 1; END;",
				BinaryPath: "", Arguments: "", IdentArgs: "", ResultType: "",
				Volatility: "v", IsStrict: false, IsSecurityDefiner: false, NumRows: 0, Language: "plpgsql", ExecLocation: "a"}

			Expect(len(results)).To(Equal(3))
			structmatcher.ExpectStructsToMatchExcluding(&results[0], &addFunction, "Oid")
			structmatcher.ExpectStructsToMatchExcluding(&results[1], &appendFunction, "Oid")
			structmatcher.ExpectStructsToMatchExcluding(&results[2], &specCharFunction, "Oid")
		})
		It("returns a slice of functions in a specific schema", func() {
			testhelper.AssertQueryRuns(connection, `CREATE FUNCTION add(numeric, integer) RETURNS numeric
AS 'SELECT $1 + $2'
LANGUAGE SQL`)
			defer testhelper.AssertQueryRuns(connection, "DROP FUNCTION add(numeric, integer)")
			testhelper.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
			defer testhelper.AssertQueryRuns(connection, "DROP SCHEMA testschema")
			testhelper.AssertQueryRuns(connection, `CREATE FUNCTION testschema.add(float, integer) RETURNS float
AS 'SELECT $1 + $2'
LANGUAGE SQL`)
			defer testhelper.AssertQueryRuns(connection, "DROP FUNCTION testschema.add(float, integer)")

			addFunction := backup.Function{
				Schema: "testschema", Name: "add", ReturnsSet: false, FunctionBody: "SELECT $1 + $2",
				BinaryPath: "", Arguments: "", IdentArgs: "", ResultType: "",
				Volatility: "v", IsStrict: false, IsSecurityDefiner: false, Language: "sql", ExecLocation: "a"}
			backup.SetIncludeSchemas([]string{"testschema"})
			results := backup.GetFunctions4(connection)

			Expect(len(results)).To(Equal(1))
			structmatcher.ExpectStructsToMatchExcluding(&results[0], &addFunction, "Oid")
		})
	})
	Describe("GetAggregates", func() {
		It("returns a slice of aggregates", func() {
			testhelper.AssertQueryRuns(connection, `
CREATE FUNCTION mysfunc_accum(numeric, numeric, numeric)
   RETURNS numeric
   AS 'select $1 + $2 + $3'
   LANGUAGE SQL
   IMMUTABLE
   RETURNS NULL ON NULL INPUT;
`)
			defer testhelper.AssertQueryRuns(connection, "DROP FUNCTION mysfunc_accum(numeric, numeric, numeric)")
			testhelper.AssertQueryRuns(connection, `
CREATE FUNCTION mypre_accum(numeric, numeric)
   RETURNS numeric
   AS 'select $1 + $2'
   LANGUAGE SQL
   IMMUTABLE
   RETURNS NULL ON NULL INPUT;
`)
			defer testhelper.AssertQueryRuns(connection, "DROP FUNCTION mypre_accum(numeric, numeric)")
			testhelper.AssertQueryRuns(connection, `
CREATE AGGREGATE agg_prefunc(numeric, numeric) (
	SFUNC = mysfunc_accum,
	STYPE = numeric,
	PREFUNC = mypre_accum,
	INITCOND = 0 );
`)
			defer testhelper.AssertQueryRuns(connection, "DROP AGGREGATE agg_prefunc(numeric, numeric)")

			transitionOid := testutils.OidFromObjectName(connection, "public", "mysfunc_accum", backup.TYPE_FUNCTION)
			prelimOid := testutils.OidFromObjectName(connection, "public", "mypre_accum", backup.TYPE_FUNCTION)

			result := backup.GetAggregates(connection)

			aggregateDef := backup.Aggregate{
				Schema: "public", Name: "agg_prefunc", Arguments: "numeric, numeric",
				IdentArgs: "numeric, numeric", TransitionFunction: transitionOid, PreliminaryFunction: prelimOid,
				FinalFunction: 0, SortOperator: 0, TransitionDataType: "numeric", InitialValue: "0", IsOrdered: false,
			}

			Expect(len(result)).To(Equal(1))
			structmatcher.ExpectStructsToMatchExcluding(&result[0], &aggregateDef, "Oid")
		})
		It("returns a slice of aggregates in a specific schema", func() {
			testhelper.AssertQueryRuns(connection, `
CREATE FUNCTION mysfunc_accum(numeric, numeric, numeric)
   RETURNS numeric
   AS 'select $1 + $2 + $3'
   LANGUAGE SQL
   IMMUTABLE
   RETURNS NULL ON NULL INPUT;
`)
			defer testhelper.AssertQueryRuns(connection, "DROP FUNCTION mysfunc_accum(numeric, numeric, numeric)")
			testhelper.AssertQueryRuns(connection, `
CREATE FUNCTION mypre_accum(numeric, numeric)
   RETURNS numeric
   AS 'select $1 + $2'
   LANGUAGE SQL
   IMMUTABLE
   RETURNS NULL ON NULL INPUT;
`)
			defer testhelper.AssertQueryRuns(connection, "DROP FUNCTION mypre_accum(numeric, numeric)")
			testhelper.AssertQueryRuns(connection, `
CREATE AGGREGATE agg_prefunc(numeric, numeric) (
	SFUNC = mysfunc_accum,
	STYPE = numeric,
	PREFUNC = mypre_accum,
	INITCOND = 0 );
`)
			defer testhelper.AssertQueryRuns(connection, "DROP AGGREGATE agg_prefunc(numeric, numeric)")
			testhelper.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
			defer testhelper.AssertQueryRuns(connection, "DROP SCHEMA testschema")
			testhelper.AssertQueryRuns(connection, `
CREATE AGGREGATE testschema.agg_prefunc(numeric, numeric) (
	SFUNC = mysfunc_accum,
	STYPE = numeric,
	PREFUNC = mypre_accum,
	INITCOND = 0 );
`)
			defer testhelper.AssertQueryRuns(connection, "DROP AGGREGATE testschema.agg_prefunc(numeric, numeric)")

			transitionOid := testutils.OidFromObjectName(connection, "public", "mysfunc_accum", backup.TYPE_FUNCTION)
			prelimOid := testutils.OidFromObjectName(connection, "public", "mypre_accum", backup.TYPE_FUNCTION)
			aggregateDef := backup.Aggregate{
				Schema: "testschema", Name: "agg_prefunc", Arguments: "numeric, numeric",
				IdentArgs: "numeric, numeric", TransitionFunction: transitionOid, PreliminaryFunction: prelimOid,
				FinalFunction: 0, SortOperator: 0, TransitionDataType: "numeric", InitialValue: "0", IsOrdered: false,
			}
			backup.SetIncludeSchemas([]string{"testschema"})

			result := backup.GetAggregates(connection)

			Expect(len(result)).To(Equal(1))
			structmatcher.ExpectStructsToMatchExcluding(&result[0], &aggregateDef, "Oid")
		})
		It("returns a slice for a hypothetical ordered-set aggregate", func() {
			testutils.SkipIfBefore6(connection)

			testhelper.AssertQueryRuns(connection, `
CREATE AGGREGATE agg_hypo_ord (VARIADIC "any" ORDER BY VARIADIC "any")
(
	SFUNC = ordered_set_transition_multi,
	STYPE = internal,
	FINALFUNC = rank_final,
	FINALFUNC_EXTRA,
	HYPOTHETICAL
);`)
			defer testhelper.AssertQueryRuns(connection, `DROP AGGREGATE agg_hypo_ord(VARIADIC "any" ORDER BY VARIADIC "any")`)

			transitionOid := testutils.OidFromObjectName(connection, "pg_catalog", "ordered_set_transition_multi", backup.TYPE_FUNCTION)
			finalOid := testutils.OidFromObjectName(connection, "pg_catalog", "rank_final", backup.TYPE_FUNCTION)

			result := backup.GetAggregates(connection)

			aggregateDef := backup.Aggregate{
				Schema: "public", Name: "agg_hypo_ord", Arguments: `VARIADIC "any" ORDER BY VARIADIC "any"`,
				IdentArgs: `VARIADIC "any" ORDER BY VARIADIC "any"`, TransitionFunction: transitionOid, FinalFunction: finalOid,
				TransitionDataType: "internal", InitValIsNull: true, FinalFuncExtra: true, Hypothetical: true,
			}

			Expect(len(result)).To(Equal(1))
			structmatcher.ExpectStructsToMatchExcluding(&result[0], &aggregateDef, "Oid")
		})
	})
	Describe("GetFunctionOidToInfoMap", func() {
		It("returns map containing function information", func() {
			result := backup.GetFunctionOidToInfoMap(connection)
			initialLength := len(result)
			testhelper.AssertQueryRuns(connection, `CREATE FUNCTION add(integer, integer) RETURNS integer
AS 'SELECT $1 + $2'
LANGUAGE SQL`)
			defer testhelper.AssertQueryRuns(connection, "DROP FUNCTION add(integer, integer)")

			result = backup.GetFunctionOidToInfoMap(connection)
			oid := testutils.OidFromObjectName(connection, "public", "add", backup.TYPE_FUNCTION)
			Expect(len(result)).To(Equal(initialLength + 1))
			Expect(result[oid].QualifiedName).To(Equal("public.add"))
			Expect(result[oid].Arguments).To(Equal("integer, integer"))
			Expect(result[oid].IsInternal).To(BeFalse())
		})
		It("returns a map containing an internal function", func() {
			result := backup.GetFunctionOidToInfoMap(connection)

			oid := testutils.OidFromObjectName(connection, "pg_catalog", "boolin", backup.TYPE_FUNCTION)
			Expect(result[oid].QualifiedName).To(Equal("pg_catalog.boolin"))
			Expect(result[oid].IsInternal).To(BeTrue())
		})
	})
	Describe("GetCasts", func() {
		It("returns a slice for a basic cast with a function in 4.3", func() {
			testutils.SkipIfNot4(connection)
			testhelper.AssertQueryRuns(connection, "CREATE FUNCTION casttotext(bool) RETURNS text STRICT IMMUTABLE LANGUAGE PLPGSQL AS $$ BEGIN IF $1 IS TRUE THEN RETURN 'true'; ELSE RETURN 'false'; END IF; END; $$;")
			defer testhelper.AssertQueryRuns(connection, "DROP FUNCTION casttotext(bool)")
			testhelper.AssertQueryRuns(connection, "CREATE CAST (bool AS text) WITH FUNCTION casttotext(bool) AS ASSIGNMENT")
			defer testhelper.AssertQueryRuns(connection, "DROP CAST (bool AS text)")

			results := backup.GetCasts(connection)

			castDef := backup.Cast{Oid: 0, SourceTypeFQN: "pg_catalog.bool", TargetTypeFQN: "pg_catalog.text", FunctionSchema: "public", FunctionName: "casttotext", FunctionArgs: "boolean", CastContext: "a", CastMethod: "f"}

			Expect(len(results)).To(Equal(1))
			structmatcher.ExpectStructsToMatchExcluding(&castDef, &results[0], "Oid", "FunctionOid")
		})
		It("returns a slice for a basic cast with a function in 5 and 6", func() {
			testutils.SkipIfBefore5(connection)
			testhelper.AssertQueryRuns(connection, "CREATE FUNCTION casttoint(text) RETURNS integer STRICT IMMUTABLE LANGUAGE SQL AS 'SELECT cast($1 as integer);'")
			defer testhelper.AssertQueryRuns(connection, "DROP FUNCTION casttoint(text)")
			testhelper.AssertQueryRuns(connection, "CREATE CAST (text AS integer) WITH FUNCTION casttoint(text) AS ASSIGNMENT")
			defer testhelper.AssertQueryRuns(connection, "DROP CAST (text AS int4)")

			results := backup.GetCasts(connection)

			castDef := backup.Cast{Oid: 0, SourceTypeFQN: "pg_catalog.text", TargetTypeFQN: "pg_catalog.int4", FunctionSchema: "public", FunctionName: "casttoint", FunctionArgs: "text", CastContext: "a", CastMethod: "f"}

			Expect(len(results)).To(Equal(1))
			structmatcher.ExpectStructsToMatchExcluding(&castDef, &results[0], "Oid")
		})
		It("returns a slice for a basic cast without a function", func() {
			testhelper.AssertQueryRuns(connection, "CREATE FUNCTION cast_in(cstring) RETURNS casttesttype AS $$textin$$ LANGUAGE internal STRICT NO SQL")
			testhelper.AssertQueryRuns(connection, "CREATE FUNCTION cast_out(casttesttype) RETURNS cstring AS $$textout$$ LANGUAGE internal STRICT NO SQL")
			testhelper.AssertQueryRuns(connection, "CREATE TYPE casttesttype (INTERNALLENGTH = variable, INPUT = cast_in, OUTPUT = cast_out)")
			defer testhelper.AssertQueryRuns(connection, "DROP TYPE casttesttype CASCADE")
			testhelper.AssertQueryRuns(connection, "CREATE CAST (text AS public.casttesttype) WITHOUT FUNCTION AS IMPLICIT")
			defer testhelper.AssertQueryRuns(connection, "DROP CAST (text AS public.casttesttype)")

			results := backup.GetCasts(connection)

			castDef := backup.Cast{Oid: 0, SourceTypeFQN: "pg_catalog.text", TargetTypeFQN: "public.casttesttype", FunctionSchema: "", FunctionName: "", FunctionArgs: "", CastContext: "i", CastMethod: "b"}

			Expect(len(results)).To(Equal(1))
			structmatcher.ExpectStructsToMatchExcluding(&castDef, &results[0], "Oid")
		})
		It("returns a slice of casts with the source and target types in a different schema", func() {
			testhelper.AssertQueryRuns(connection, "CREATE SCHEMA testschema1")
			defer testhelper.AssertQueryRuns(connection, "DROP SCHEMA testschema1")
			testhelper.AssertQueryRuns(connection, "CREATE FUNCTION cast_in(cstring) RETURNS testschema1.casttesttype AS $$textin$$ LANGUAGE internal STRICT NO SQL")
			testhelper.AssertQueryRuns(connection, "CREATE FUNCTION cast_out(testschema1.casttesttype) RETURNS cstring AS $$textout$$ LANGUAGE internal STRICT NO SQL")
			testhelper.AssertQueryRuns(connection, "CREATE TYPE testschema1.casttesttype (INTERNALLENGTH = variable, INPUT = cast_in, OUTPUT = cast_out)")
			defer testhelper.AssertQueryRuns(connection, "DROP TYPE testschema1.casttesttype CASCADE")

			testhelper.AssertQueryRuns(connection, "CREATE CAST (text AS testschema1.casttesttype) WITHOUT FUNCTION AS IMPLICIT")
			defer testhelper.AssertQueryRuns(connection, "DROP CAST (text AS testschema1.casttesttype)")
			testhelper.AssertQueryRuns(connection, "CREATE CAST (testschema1.casttesttype AS text) WITHOUT FUNCTION AS IMPLICIT")
			defer testhelper.AssertQueryRuns(connection, "DROP CAST (testschema1.casttesttype AS text)")

			results := backup.GetCasts(connection)

			castDefTarget := backup.Cast{Oid: 0, SourceTypeFQN: "pg_catalog.text", TargetTypeFQN: "testschema1.casttesttype", FunctionSchema: "", FunctionName: "", FunctionArgs: "", CastContext: "i", CastMethod: "b"}
			castDefSource := backup.Cast{Oid: 0, SourceTypeFQN: "testschema1.casttesttype", TargetTypeFQN: "pg_catalog.text", FunctionSchema: "", FunctionName: "", FunctionArgs: "", CastContext: "i", CastMethod: "b"}

			Expect(len(results)).To(Equal(2))
			structmatcher.ExpectStructsToMatchExcluding(&castDefTarget, &results[0], "Oid")
			structmatcher.ExpectStructsToMatchExcluding(&castDefSource, &results[1], "Oid")
		})
		It("returns a slice for an inout cast", func() {
			testutils.SkipIfBefore6(connection)
			testhelper.AssertQueryRuns(connection, "CREATE TYPE custom_numeric AS (i numeric)")
			defer testhelper.AssertQueryRuns(connection, "DROP TYPE custom_numeric")
			testhelper.AssertQueryRuns(connection, "CREATE CAST (varchar AS custom_numeric) WITH INOUT")
			defer testhelper.AssertQueryRuns(connection, "DROP CAST (varchar AS custom_numeric)")

			results := backup.GetCasts(connection)

			castDef := backup.Cast{Oid: 0, SourceTypeFQN: `pg_catalog."varchar"`, TargetTypeFQN: "public.custom_numeric", FunctionSchema: "", FunctionName: "", FunctionArgs: "", CastContext: "e", CastMethod: "i"}

			Expect(len(results)).To(Equal(1))
			structmatcher.ExpectStructsToMatchExcluding(&castDef, &results[0], "Oid")
		})
	})
	Describe("GetExtensions", func() {
		It("returns a slice of extension", func() {
			testutils.SkipIfBefore5(connection)
			testhelper.AssertQueryRuns(connection, "CREATE EXTENSION plperl")
			defer testhelper.AssertQueryRuns(connection, "DROP EXTENSION plperl")

			results := backup.GetExtensions(connection)

			Expect(len(results)).To(Equal(1))

			extensionDef := backup.Extension{Oid: 0, Name: "plperl", Schema: "pg_catalog"}
			structmatcher.ExpectStructsToMatchExcluding(&extensionDef, &results[0], "Oid")

		})
	})
	Describe("GetProceduralLanguages", func() {
		It("returns a slice of procedural languages", func() {
			testhelper.AssertQueryRuns(connection, "CREATE LANGUAGE plpythonu")
			defer testhelper.AssertQueryRuns(connection, "DROP LANGUAGE plpythonu")

			pythonHandlerOid := testutils.OidFromObjectName(connection, "pg_catalog", "plpython_call_handler", backup.TYPE_FUNCTION)

			expectedPlpythonInfo := backup.ProceduralLanguage{Oid: 1, Name: "plpythonu", Owner: "testrole", IsPl: true, PlTrusted: false, Handler: pythonHandlerOid, Inline: 0}
			if connection.Version.AtLeast("5") {
				pythonInlineOid := testutils.OidFromObjectName(connection, "pg_catalog", "plpython_inline_handler", backup.TYPE_FUNCTION)
				expectedPlpythonInfo.Inline = pythonInlineOid
			}

			resultProcLangs := backup.GetProceduralLanguages(connection)

			Expect(len(resultProcLangs)).To(Equal(1))
			structmatcher.ExpectStructsToMatchExcluding(&expectedPlpythonInfo, &resultProcLangs[0], "Oid", "Owner")
		})
	})
	Describe("GetConversions", func() {
		It("returns a slice of conversions", func() {
			testhelper.AssertQueryRuns(connection, "CREATE CONVERSION testconv FOR 'LATIN1' TO 'MULE_INTERNAL' FROM latin1_to_mic")
			defer testhelper.AssertQueryRuns(connection, "DROP CONVERSION testconv")

			expectedConversion := backup.Conversion{Oid: 0, Schema: "public", Name: "testconv", ForEncoding: "LATIN1", ToEncoding: "MULE_INTERNAL", ConversionFunction: "pg_catalog.latin1_to_mic", IsDefault: false}

			resultConversions := backup.GetConversions(connection)

			Expect(len(resultConversions)).To(Equal(1))
			structmatcher.ExpectStructsToMatchExcluding(&expectedConversion, &resultConversions[0], "Oid")
		})
		It("returns a slice of conversions in a specific schema", func() {
			testhelper.AssertQueryRuns(connection, "CREATE CONVERSION testconv FOR 'LATIN1' TO 'MULE_INTERNAL' FROM latin1_to_mic")
			defer testhelper.AssertQueryRuns(connection, "DROP CONVERSION testconv")
			testhelper.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
			defer testhelper.AssertQueryRuns(connection, "DROP SCHEMA testschema")
			testhelper.AssertQueryRuns(connection, "CREATE CONVERSION testschema.testconv FOR 'LATIN1' TO 'MULE_INTERNAL' FROM latin1_to_mic")
			defer testhelper.AssertQueryRuns(connection, "DROP CONVERSION testschema.testconv")

			expectedConversion := backup.Conversion{Oid: 0, Schema: "testschema", Name: "testconv", ForEncoding: "LATIN1", ToEncoding: "MULE_INTERNAL", ConversionFunction: "pg_catalog.latin1_to_mic", IsDefault: false}

			backup.SetIncludeSchemas([]string{"testschema"})
			resultConversions := backup.GetConversions(connection)

			Expect(len(resultConversions)).To(Equal(1))
			structmatcher.ExpectStructsToMatchExcluding(&expectedConversion, &resultConversions[0], "Oid")
		})
	})
	Describe("ConstructFunctionDependencies", func() {
		BeforeEach(func() {
			testhelper.AssertQueryRuns(connection, "CREATE TYPE composite_ints AS (one integer, two integer)")
		})
		AfterEach(func() {
			testhelper.AssertQueryRuns(connection, "DROP TYPE composite_ints CASCADE")
		})
		It("constructs dependencies correctly for a function dependent on a user-defined type in the arguments", func() {
			testhelper.AssertQueryRuns(connection, "CREATE FUNCTION add(composite_ints) RETURNS integer STRICT IMMUTABLE LANGUAGE SQL AS 'SELECT ($1.one + $1.two);'")
			defer testhelper.AssertQueryRuns(connection, "DROP FUNCTION add(composite_ints)")

			allFunctions := backup.GetFunctionsAllVersions(connection)
			function := backup.Function{}
			for _, funct := range allFunctions {
				if funct.Name == "add" {
					function = funct
					break
				}
			}
			functions := []backup.Function{function}

			functions = backup.ConstructFunctionDependencies(connection, functions)

			Expect(len(functions)).To(Equal(1))
			Expect(len(functions[0].DependsUpon)).To(Equal(1))
			Expect(functions[0].DependsUpon[0]).To(Equal("public.composite_ints"))
		})
		It("constructs dependencies correctly for a function dependent on a user-defined type in the return type", func() {
			testhelper.AssertQueryRuns(connection, "CREATE FUNCTION compose(integer, integer) RETURNS composite_ints STRICT IMMUTABLE LANGUAGE PLPGSQL AS 'DECLARE comp composite_ints; BEGIN SELECT $1, $2 INTO comp; RETURN comp; END;';")
			defer testhelper.AssertQueryRuns(connection, "DROP FUNCTION compose(integer, integer)")

			allFunctions := backup.GetFunctionsAllVersions(connection)
			function := backup.Function{}
			for _, funct := range allFunctions {
				if funct.Name == "compose" {
					function = funct
					break
				}
			}
			functions := []backup.Function{function}

			functions = backup.ConstructFunctionDependencies(connection, functions)

			Expect(len(functions)).To(Equal(1))
			Expect(len(functions[0].DependsUpon)).To(Equal(1))
			Expect(functions[0].DependsUpon[0]).To(Equal("public.composite_ints"))
		})
		It("constructs dependencies correctly for a function dependent on an implicit array type", func() {
			testhelper.AssertQueryRuns(connection, "CREATE TYPE base_type")
			defer testhelper.AssertQueryRuns(connection, "DROP TYPE base_type CASCADE")
			testhelper.AssertQueryRuns(connection, "CREATE FUNCTION base_fn_in(cstring) RETURNS base_type AS 'boolin' LANGUAGE internal")
			testhelper.AssertQueryRuns(connection, "CREATE FUNCTION base_fn_out(base_type) RETURNS cstring AS 'boolout' LANGUAGE internal")
			testhelper.AssertQueryRuns(connection, "CREATE TYPE base_type(INPUT=base_fn_in, OUTPUT=base_fn_out)")
			testhelper.AssertQueryRuns(connection, "CREATE FUNCTION compose(base_type[], composite_ints) RETURNS composite_ints STRICT IMMUTABLE LANGUAGE PLPGSQL AS 'DECLARE comp composite_ints; BEGIN SELECT $1[0].one+$2.one, $1[0].two+$2.two INTO comp; RETURN comp; END;';")
			defer testhelper.AssertQueryRuns(connection, "DROP FUNCTION compose(base_type[], composite_ints)")

			allFunctions := backup.GetFunctionsAllVersions(connection)
			function := backup.Function{}
			for _, funct := range allFunctions {
				if funct.Name == "compose" {
					function = funct
					break
				}
			}
			functions := []backup.Function{function}

			functions = backup.ConstructFunctionDependencies(connection, functions)

			Expect(len(functions)).To(Equal(1))
			Expect(len(functions[0].DependsUpon)).To(Equal(3))
			Expect(functions[0].DependsUpon[0]).To(Equal("public.composite_ints"))
			Expect(functions[0].DependsUpon[1]).To(Equal("public.base_type"))
			Expect(functions[0].DependsUpon[2]).To(Equal("public.composite_ints"))
		})
	})
	Describe("GetForeignDataWrappers", func() {
		It("returns a slice of foreign data wrappers", func() {
			testutils.SkipIfBefore6(connection)
			testhelper.AssertQueryRuns(connection, "CREATE FOREIGN DATA WRAPPER foreigndatawrapper")
			defer testhelper.AssertQueryRuns(connection, "DROP FOREIGN DATA WRAPPER foreigndatawrapper")

			expectedForeignDataWrapper := backup.ForeignDataWrapper{Oid: 0, Name: "foreigndatawrapper"}

			resultForeignDataWrapper := backup.GetForeignDataWrappers(connection)

			Expect(len(resultForeignDataWrapper)).To(Equal(1))
			structmatcher.ExpectStructsToMatchExcluding(&expectedForeignDataWrapper, &resultForeignDataWrapper[0], "Oid")
		})
		It("returns a slice of foreign data wrappers with a validator", func() {
			testutils.SkipIfBefore6(connection)
			testhelper.AssertQueryRuns(connection, "CREATE FOREIGN DATA WRAPPER foreigndatawrapper VALIDATOR postgresql_fdw_validator")
			defer testhelper.AssertQueryRuns(connection, "DROP FOREIGN DATA WRAPPER foreigndatawrapper")

			validatorOid := testutils.OidFromObjectName(connection, "pg_catalog", "postgresql_fdw_validator", backup.TYPE_FUNCTION)
			expectedForeignDataWrapper := backup.ForeignDataWrapper{Oid: 0, Name: "foreigndatawrapper", Validator: validatorOid}

			resultForeignDataWrapper := backup.GetForeignDataWrappers(connection)

			Expect(len(resultForeignDataWrapper)).To(Equal(1))
			structmatcher.ExpectStructsToMatchExcluding(&expectedForeignDataWrapper, &resultForeignDataWrapper[0], "Oid")
		})
		It("returns a slice of foreign data wrappers with options", func() {
			testutils.SkipIfBefore6(connection)
			testhelper.AssertQueryRuns(connection, "CREATE FOREIGN DATA WRAPPER foreigndatawrapper OPTIONS (dbname 'testdb', debug 'true')")
			defer testhelper.AssertQueryRuns(connection, "DROP FOREIGN DATA WRAPPER foreigndatawrapper")

			expectedForeignDataWrapper := backup.ForeignDataWrapper{Oid: 0, Name: "foreigndatawrapper", Options: "dbname 'testdb', debug 'true'"}

			resultForeignDataWrappers := backup.GetForeignDataWrappers(connection)

			Expect(len(resultForeignDataWrappers)).To(Equal(1))
			structmatcher.ExpectStructsToMatchExcluding(&expectedForeignDataWrapper, &resultForeignDataWrappers[0], "Oid")
		})
	})
	Describe("GetForeignServers", func() {
		It("returns a slice of foreign servers", func() {
			testutils.SkipIfBefore6(connection)
			testhelper.AssertQueryRuns(connection, "CREATE FOREIGN DATA WRAPPER foreigndatawrapper")
			defer testhelper.AssertQueryRuns(connection, "DROP FOREIGN DATA WRAPPER foreigndatawrapper CASCADE")
			testhelper.AssertQueryRuns(connection, "CREATE SERVER foreignserver FOREIGN DATA WRAPPER foreigndatawrapper")

			expectedServer := backup.ForeignServer{Oid: 1, Name: "foreignserver", ForeignDataWrapper: "foreigndatawrapper"}

			resultServers := backup.GetForeignServers(connection)

			Expect(len(resultServers)).To(Equal(1))
			structmatcher.ExpectStructsToMatchExcluding(&expectedServer, &resultServers[0], "Oid")
		})
		It("returns a slice of foreign servers with a type and version", func() {
			testutils.SkipIfBefore6(connection)
			testhelper.AssertQueryRuns(connection, "CREATE FOREIGN DATA WRAPPER foreigndatawrapper")
			defer testhelper.AssertQueryRuns(connection, "DROP FOREIGN DATA WRAPPER foreigndatawrapper CASCADE")
			testhelper.AssertQueryRuns(connection, "CREATE SERVER foreignserver TYPE 'mytype' VERSION 'myversion' FOREIGN DATA WRAPPER foreigndatawrapper")

			expectedServer := backup.ForeignServer{Oid: 1, Name: "foreignserver", Type: "mytype", Version: "myversion", ForeignDataWrapper: "foreigndatawrapper"}

			resultServers := backup.GetForeignServers(connection)

			Expect(len(resultServers)).To(Equal(1))
			structmatcher.ExpectStructsToMatchExcluding(&expectedServer, &resultServers[0], "Oid")
		})
		It("returns a slice of foreign servers with options", func() {
			testutils.SkipIfBefore6(connection)
			testhelper.AssertQueryRuns(connection, "CREATE FOREIGN DATA WRAPPER foreigndatawrapper")
			defer testhelper.AssertQueryRuns(connection, "DROP FOREIGN DATA WRAPPER foreigndatawrapper CASCADE")
			testhelper.AssertQueryRuns(connection, "CREATE SERVER foreignserver FOREIGN DATA WRAPPER foreigndatawrapper OPTIONS (dbname 'testdb', host 'localhost')")

			expectedServer := backup.ForeignServer{Oid: 1, Name: "foreignserver", ForeignDataWrapper: "foreigndatawrapper", Options: "dbname 'testdb', host 'localhost'"}

			resultServers := backup.GetForeignServers(connection)

			Expect(len(resultServers)).To(Equal(1))
			structmatcher.ExpectStructsToMatchExcluding(&expectedServer, &resultServers[0], "Oid")
		})
	})
	Describe("GetUserMappings", func() {
		It("returns a slice of user mappings", func() {
			testutils.SkipIfBefore6(connection)
			testhelper.AssertQueryRuns(connection, "CREATE FOREIGN DATA WRAPPER foreigndatawrapper")
			defer testhelper.AssertQueryRuns(connection, "DROP FOREIGN DATA WRAPPER foreigndatawrapper CASCADE")
			testhelper.AssertQueryRuns(connection, "CREATE SERVER foreignserver FOREIGN DATA WRAPPER foreigndatawrapper")
			testhelper.AssertQueryRuns(connection, "CREATE USER MAPPING FOR testrole SERVER foreignserver")

			expectedMapping := backup.UserMapping{Oid: 1, User: "testrole", Server: "foreignserver"}

			resultMappings := backup.GetUserMappings(connection)

			Expect(len(resultMappings)).To(Equal(1))
			structmatcher.ExpectStructsToMatchExcluding(&expectedMapping, &resultMappings[0], "Oid")
		})
		It("returns a slice of user mappings with options", func() {
			testutils.SkipIfBefore6(connection)
			testhelper.AssertQueryRuns(connection, "CREATE FOREIGN DATA WRAPPER foreigndatawrapper")
			defer testhelper.AssertQueryRuns(connection, "DROP FOREIGN DATA WRAPPER foreigndatawrapper CASCADE")
			testhelper.AssertQueryRuns(connection, "CREATE SERVER foreignserver FOREIGN DATA WRAPPER foreigndatawrapper")
			testhelper.AssertQueryRuns(connection, "CREATE USER MAPPING FOR PUBLIC SERVER foreignserver OPTIONS (dbname 'testdb', host 'localhost')")

			expectedMapping := backup.UserMapping{Oid: 1, User: "PUBLIC", Server: "foreignserver", Options: "dbname 'testdb', host 'localhost'"}

			resultMappings := backup.GetUserMappings(connection)

			Expect(len(resultMappings)).To(Equal(1))
			structmatcher.ExpectStructsToMatchExcluding(&expectedMapping, &resultMappings[0], "Oid")
		})
	})
})
