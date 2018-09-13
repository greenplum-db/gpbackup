package integration

import (
	"github.com/greenplum-db/gp-common-go-libs/structmatcher"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/utils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup integration tests", func() {
	Describe("GetFunctionsMaster", func() {
		BeforeEach(func() {
			testutils.SkipIfBefore5(connection)
		})
		It("returns a slice of functions", func() {
			testhelper.AssertQueryRuns(connection, `CREATE FUNCTION public.add(integer, integer) RETURNS integer
AS 'SELECT $1 + $2'
LANGUAGE SQL`)
			defer testhelper.AssertQueryRuns(connection, "DROP FUNCTION public.add(integer, integer)")
			testhelper.AssertQueryRuns(connection, `
CREATE FUNCTION public.append(integer, integer) RETURNS SETOF record
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
			defer testhelper.AssertQueryRuns(connection, "DROP FUNCTION public.append(integer, integer)")
			testhelper.AssertQueryRuns(connection, "COMMENT ON FUNCTION public.append(integer, integer) IS 'this is a function comment'")

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

			Expect(results).To(HaveLen(2))
			structmatcher.ExpectStructsToMatchExcluding(&results[0], &addFunction, "Oid")
			structmatcher.ExpectStructsToMatchExcluding(&results[1], &appendFunction, "Oid")
		})
		It("returns a slice of functions in a specific schema", func() {
			testhelper.AssertQueryRuns(connection, `CREATE FUNCTION public.add(integer, integer) RETURNS integer
AS 'SELECT $1 + $2'
LANGUAGE SQL`)
			defer testhelper.AssertQueryRuns(connection, "DROP FUNCTION public.add(integer, integer)")
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
			backupCmdFlags.Set(utils.INCLUDE_SCHEMA, "testschema")
			results := backup.GetFunctionsMaster(connection)

			Expect(results).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&results[0], &addFunction, "Oid")
		})
		It("returns a window function", func() {
			testutils.SkipIfBefore6(connection)
			testhelper.AssertQueryRuns(connection, `CREATE FUNCTION public.add(integer, integer) RETURNS integer
AS 'SELECT $1 + $2'
LANGUAGE SQL WINDOW`)
			defer testhelper.AssertQueryRuns(connection, "DROP FUNCTION public.add(integer, integer)")

			results := backup.GetFunctionsMaster(connection)

			windowFunction := backup.Function{
				Schema: "public", Name: "add", ReturnsSet: false, FunctionBody: "SELECT $1 + $2",
				BinaryPath: "", Arguments: "integer, integer", IdentArgs: "integer, integer", ResultType: "integer",
				Volatility: "v", IsStrict: false, IsSecurityDefiner: false, Config: "", Cost: 100, NumRows: 0, DataAccess: "c",
				Language: "sql", IsWindow: true, ExecLocation: "a"}

			Expect(results).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&results[0], &windowFunction, "Oid")
		})
		It("returns a function to execute on master and all segments", func() {
			testutils.SkipIfBefore6(connection)
			testhelper.AssertQueryRuns(connection, `CREATE FUNCTION public.srf_on_master(integer, integer) RETURNS integer
AS 'SELECT $1 + $2'
LANGUAGE SQL WINDOW
EXECUTE ON MASTER;`)
			defer testhelper.AssertQueryRuns(connection, "DROP FUNCTION public.srf_on_master(integer, integer)")
			testhelper.AssertQueryRuns(connection, `CREATE FUNCTION public.srf_on_all_segments(integer, integer) RETURNS integer
AS 'SELECT $1 + $2'
LANGUAGE SQL WINDOW
EXECUTE ON ALL SEGMENTS;`)
			defer testhelper.AssertQueryRuns(connection, "DROP FUNCTION public.srf_on_all_segments(integer, integer)")

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

			Expect(results).To(HaveLen(2))
			structmatcher.ExpectStructsToMatchExcluding(&results[0], &srfOnAllSegmentsFunction, "Oid")
			structmatcher.ExpectStructsToMatchExcluding(&results[1], &srfOnMasterFunction, "Oid")
		})
		It("returns a function with LEAKPROOF", func() {
			// other tests that are specific to >=6 can be added to this
			testutils.SkipIfBefore6(connection)
			testhelper.AssertQueryRuns(connection, `
CREATE FUNCTION public.append(integer, integer) RETURNS SETOF record
AS 'SELECT ($1, $2)'
LANGUAGE SQL
SECURITY DEFINER
STRICT
LEAKPROOF
STABLE
COST 200
ROWS 200
SET search_path = pg_temp
MODIFIES SQL DATA
`)
			defer testhelper.AssertQueryRuns(connection, "DROP FUNCTION public.append(integer, integer)")

			results := backup.GetFunctionsMaster(connection)

			appendFunction := backup.Function{
				Schema: "public", Name: "append", ReturnsSet: true, FunctionBody: "SELECT ($1, $2)",
				BinaryPath: "", Arguments: "integer, integer", IdentArgs: "integer, integer", ResultType: "SETOF record",
				Volatility: "s", IsStrict: true, IsLeakProof: true, IsSecurityDefiner: true, Config: "SET search_path TO pg_temp", Cost: 200,
				NumRows: 200, DataAccess: "m", Language: "sql", ExecLocation: "a"}

			Expect(results).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&results[0], &appendFunction, "Oid")
		})
	})
	Describe("GetFunctions4", func() {
		BeforeEach(func() {
			testutils.SkipIfNot4(connection)
		})
		It("returns a slice of functions", func() {
			testhelper.AssertQueryRuns(connection, `CREATE FUNCTION public.add(numeric, integer) RETURNS numeric
AS 'SELECT $1 + $2'
LANGUAGE SQL`)
			defer testhelper.AssertQueryRuns(connection, "DROP FUNCTION public.add(numeric, integer)")
			testhelper.AssertQueryRuns(connection, `
CREATE FUNCTION public.append(float, integer) RETURNS SETOF record
AS 'SELECT ($1, $2)'
LANGUAGE SQL
SECURITY DEFINER
STRICT
STABLE
`)
			defer testhelper.AssertQueryRuns(connection, "DROP FUNCTION public.append(float, integer)")
			testhelper.AssertQueryRuns(connection, "COMMENT ON FUNCTION public.append(float, integer) IS 'this is a function comment'")
			testhelper.AssertQueryRuns(connection, `CREATE FUNCTION public."specChar"(t text, "precision" double precision) RETURNS double precision AS $$BEGIN RETURN precision + 1; END;$$ LANGUAGE PLPGSQL;`)
			defer testhelper.AssertQueryRuns(connection, `DROP FUNCTION public."specChar"(text, double precision)`)

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

			Expect(results).To(HaveLen(3))
			structmatcher.ExpectStructsToMatchExcluding(&results[0], &addFunction, "Oid")
			structmatcher.ExpectStructsToMatchExcluding(&results[1], &appendFunction, "Oid")
			structmatcher.ExpectStructsToMatchExcluding(&results[2], &specCharFunction, "Oid")
		})
		It("returns a slice of functions in a specific schema", func() {
			testhelper.AssertQueryRuns(connection, `CREATE FUNCTION public.add(numeric, integer) RETURNS numeric
AS 'SELECT $1 + $2'
LANGUAGE SQL`)
			defer testhelper.AssertQueryRuns(connection, "DROP FUNCTION public.add(numeric, integer)")
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
			backupCmdFlags.Set(utils.INCLUDE_SCHEMA, "testschema")
			results := backup.GetFunctions4(connection)

			Expect(results).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&results[0], &addFunction, "Oid")
		})
	})
	Describe("GetAggregates", func() {
		BeforeEach(func() {
			testhelper.AssertQueryRuns(connection, `
CREATE FUNCTION public.mysfunc_accum(numeric, numeric, numeric)
   RETURNS numeric
   AS 'select $1 + $2 + $3'
   LANGUAGE SQL
   IMMUTABLE;
`)
			testhelper.AssertQueryRuns(connection, `
CREATE FUNCTION public.mypre_accum(numeric, numeric)
   RETURNS numeric
   AS 'select $1 + $2'
   LANGUAGE SQL
   IMMUTABLE
   RETURNS NULL ON NULL INPUT;
`)
		})
		AfterEach(func() {
			testhelper.AssertQueryRuns(connection, "DROP FUNCTION public.mysfunc_accum(numeric, numeric, numeric)")
			testhelper.AssertQueryRuns(connection, "DROP FUNCTION public.mypre_accum(numeric, numeric)")
		})
		It("returns a slice of aggregates", func() {
			testhelper.AssertQueryRuns(connection, `
CREATE AGGREGATE public.agg_prefunc(numeric, numeric) (
	SFUNC = public.mysfunc_accum,
	STYPE = numeric,
	PREFUNC = public.mypre_accum,
	INITCOND = 0 );
`)
			defer testhelper.AssertQueryRuns(connection, "DROP AGGREGATE public.agg_prefunc(numeric, numeric)")

			transitionOid := testutils.OidFromObjectName(connection, "public", "mysfunc_accum", backup.TYPE_FUNCTION)
			prelimOid := testutils.OidFromObjectName(connection, "public", "mypre_accum", backup.TYPE_FUNCTION)

			result := backup.GetAggregates(connection)

			aggregateDef := backup.Aggregate{
				Schema: "public", Name: "agg_prefunc", Arguments: "numeric, numeric",
				IdentArgs: "numeric, numeric", TransitionFunction: transitionOid, PreliminaryFunction: prelimOid,
				FinalFunction: 0, SortOperator: 0, TransitionDataType: "numeric", InitialValue: "0", MInitValIsNull: true, IsOrdered: false,
			}
			if connection.Version.AtLeast("6") {
				aggregateDef.PreliminaryFunction = 0
				aggregateDef.CombineFunction = prelimOid
			}

			Expect(result).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&result[0], &aggregateDef, "Oid")
		})
		It("returns a slice of aggregates in a specific schema", func() {
			testhelper.AssertQueryRuns(connection, `
CREATE AGGREGATE public.agg_prefunc(numeric, numeric) (
	SFUNC = public.mysfunc_accum,
	STYPE = numeric,
	PREFUNC = public.mypre_accum,
	INITCOND = 0 );
`)
			defer testhelper.AssertQueryRuns(connection, "DROP AGGREGATE public.agg_prefunc(numeric, numeric)")
			testhelper.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
			defer testhelper.AssertQueryRuns(connection, "DROP SCHEMA testschema")
			testhelper.AssertQueryRuns(connection, `
CREATE AGGREGATE testschema.agg_prefunc(numeric, numeric) (
	SFUNC = public.mysfunc_accum,
	STYPE = numeric,
	PREFUNC = public.mypre_accum,
	INITCOND = 0 );
`)
			defer testhelper.AssertQueryRuns(connection, "DROP AGGREGATE testschema.agg_prefunc(numeric, numeric)")

			transitionOid := testutils.OidFromObjectName(connection, "public", "mysfunc_accum", backup.TYPE_FUNCTION)
			prelimOid := testutils.OidFromObjectName(connection, "public", "mypre_accum", backup.TYPE_FUNCTION)
			aggregateDef := backup.Aggregate{
				Schema: "testschema", Name: "agg_prefunc", Arguments: "numeric, numeric",
				IdentArgs: "numeric, numeric", TransitionFunction: transitionOid, PreliminaryFunction: prelimOid,
				FinalFunction: 0, SortOperator: 0, TransitionDataType: "numeric", InitialValue: "0", MInitValIsNull: true, IsOrdered: false,
			}
			if connection.Version.AtLeast("6") {
				aggregateDef.PreliminaryFunction = 0
				aggregateDef.CombineFunction = prelimOid
			}
			backupCmdFlags.Set(utils.INCLUDE_SCHEMA, "testschema")

			result := backup.GetAggregates(connection)

			Expect(result).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&result[0], &aggregateDef, "Oid")
		})
		It("returns a slice for a hypothetical ordered-set aggregate", func() {
			testutils.SkipIfBefore6(connection)

			testhelper.AssertQueryRuns(connection, `
CREATE AGGREGATE public.agg_hypo_ord (VARIADIC "any" ORDER BY VARIADIC "any")
(
	SFUNC = pg_catalog.ordered_set_transition_multi,
	STYPE = internal,
	FINALFUNC = pg_catalog.rank_final,
	FINALFUNC_EXTRA,
	HYPOTHETICAL
);`)
			defer testhelper.AssertQueryRuns(connection, `DROP AGGREGATE public.agg_hypo_ord(VARIADIC "any" ORDER BY VARIADIC "any")`)

			transitionOid := testutils.OidFromObjectName(connection, "pg_catalog", "ordered_set_transition_multi", backup.TYPE_FUNCTION)
			finalOid := testutils.OidFromObjectName(connection, "pg_catalog", "rank_final", backup.TYPE_FUNCTION)

			result := backup.GetAggregates(connection)

			aggregateDef := backup.Aggregate{
				Schema: "public", Name: "agg_hypo_ord", Arguments: `VARIADIC "any" ORDER BY VARIADIC "any"`,
				IdentArgs: `VARIADIC "any" ORDER BY VARIADIC "any"`, TransitionFunction: transitionOid, FinalFunction: finalOid,
				TransitionDataType: "internal", InitValIsNull: true, MInitValIsNull: true, FinalFuncExtra: true, Hypothetical: true,
			}

			Expect(result).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&result[0], &aggregateDef, "Oid")
		})
		It("returns a slice of aggregates with a combine function and transition data size", func() {
			testutils.SkipIfBefore6(connection)
			testhelper.AssertQueryRuns(connection, `
CREATE AGGREGATE public.agg_combinefunc(numeric, numeric) (
	SFUNC = public.mysfunc_accum,
	STYPE = numeric,
	SSPACE = 1000,
	COMBINEFUNC = public.mypre_accum,
	INITCOND = 0 );
`)
			defer testhelper.AssertQueryRuns(connection, "DROP AGGREGATE public.agg_combinefunc(numeric, numeric)")

			transitionOid := testutils.OidFromObjectName(connection, "public", "mysfunc_accum", backup.TYPE_FUNCTION)
			combineOid := testutils.OidFromObjectName(connection, "public", "mypre_accum", backup.TYPE_FUNCTION)

			result := backup.GetAggregates(connection)

			aggregateDef := backup.Aggregate{
				Schema: "public", Name: "agg_combinefunc", Arguments: "numeric, numeric",
				IdentArgs: "numeric, numeric", TransitionFunction: transitionOid, CombineFunction: combineOid,
				FinalFunction: 0, SortOperator: 0, TransitionDataType: "numeric", TransitionDataSize: 1000,
				InitialValue: "0", MInitValIsNull: true, IsOrdered: false,
			}

			Expect(result).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&result[0], &aggregateDef, "Oid")
		})
		It("returns a slice of aggregates with serial/deserial functions", func() {
			testutils.SkipIfBefore6(connection)
			testhelper.AssertQueryRuns(connection, `
CREATE AGGREGATE public.myavg (numeric) (
	stype = internal,
	sfunc = numeric_avg_accum,
	finalfunc = numeric_avg,
	serialfunc = numeric_avg_serialize,
	deserialfunc = numeric_avg_deserialize);
`)
			defer testhelper.AssertQueryRuns(connection, "DROP AGGREGATE public.myavg(numeric)")

			serialOid := testutils.OidFromObjectName(connection, "pg_catalog", "numeric_avg_serialize", backup.TYPE_FUNCTION)
			deserialOid := testutils.OidFromObjectName(connection, "pg_catalog", "numeric_avg_deserialize", backup.TYPE_FUNCTION)

			result := backup.GetAggregates(connection)

			aggregateDef := backup.Aggregate{
				Schema: "public", Name: "myavg", Arguments: "numeric",
				IdentArgs: "numeric", SerialFunction: serialOid, DeserialFunction: deserialOid,
				FinalFunction: 0, SortOperator: 0, TransitionDataType: "internal",
				IsOrdered: false, InitValIsNull: true, MInitValIsNull: true,
			}

			Expect(result).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&result[0], &aggregateDef, "Oid", "TransitionFunction", "FinalFunction")
		})
		It("returns a slice of aggregates with moving attributes", func() {
			testutils.SkipIfBefore6(connection)
			testhelper.AssertQueryRuns(connection, `
CREATE AGGREGATE public.moving_agg(numeric,numeric) (
	SFUNC = public.mysfunc_accum,
	STYPE = numeric,
	MSFUNC = public.mysfunc_accum,
	MINVFUNC = public.mysfunc_accum,
	MSTYPE = numeric,
	MSSPACE = 100,
	MFINALFUNC = public.mysfunc_accum,
	MFINALFUNC_EXTRA,
	MINITCOND = 0
	);
`)
			defer testhelper.AssertQueryRuns(connection, "DROP AGGREGATE public.moving_agg(numeric, numeric)")

			sfuncOid := testutils.OidFromObjectName(connection, "public", "mysfunc_accum", backup.TYPE_FUNCTION)

			result := backup.GetAggregates(connection)

			aggregateDef := backup.Aggregate{
				Schema: "public", Name: "moving_agg", Arguments: "numeric, numeric",
				IdentArgs: "numeric, numeric", TransitionFunction: sfuncOid, TransitionDataType: "numeric",
				InitValIsNull: true, MTransitionFunction: sfuncOid, MInverseTransitionFunction: sfuncOid,
				MTransitionDataType: "numeric", MTransitionDataSize: 100, MFinalFunction: sfuncOid,
				MFinalFuncExtra: true, MInitialValue: "0", MInitValIsNull: false,
			}

			Expect(result).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&result[0], &aggregateDef, "Oid")
		})
	})
	Describe("GetFunctionOidToInfoMap", func() {
		It("returns map containing function information", func() {
			result := backup.GetFunctionOidToInfoMap(connection)
			initialLength := len(result)
			testhelper.AssertQueryRuns(connection, `CREATE FUNCTION public.add(integer, integer) RETURNS integer
AS 'SELECT $1 + $2'
LANGUAGE SQL`)
			defer testhelper.AssertQueryRuns(connection, "DROP FUNCTION public.add(integer, integer)")

			result = backup.GetFunctionOidToInfoMap(connection)
			oid := testutils.OidFromObjectName(connection, "public", "add", backup.TYPE_FUNCTION)
			Expect(result).To(HaveLen(initialLength + 1))
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
			testhelper.AssertQueryRuns(connection, "CREATE FUNCTION public.casttotext(bool) RETURNS pg_catalog.text STRICT IMMUTABLE LANGUAGE PLPGSQL AS $$ BEGIN IF $1 IS TRUE THEN RETURN 'true'; ELSE RETURN 'false'; END IF; END; $$;")
			defer testhelper.AssertQueryRuns(connection, "DROP FUNCTION public.casttotext(bool)")
			testhelper.AssertQueryRuns(connection, "CREATE CAST (bool AS text) WITH FUNCTION public.casttotext(bool) AS ASSIGNMENT")
			defer testhelper.AssertQueryRuns(connection, "DROP CAST (bool AS text)")

			results := backup.GetCasts(connection)

			castDef := backup.Cast{Oid: 0, SourceTypeFQN: "pg_catalog.bool", TargetTypeFQN: "pg_catalog.text", FunctionSchema: "public", FunctionName: "casttotext", FunctionArgs: "boolean", CastContext: "a", CastMethod: "f"}

			Expect(results).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&castDef, &results[0], "Oid", "FunctionOid")
		})
		It("returns a slice for a basic cast with a function in 5 and 6", func() {
			testutils.SkipIfBefore5(connection)
			testhelper.AssertQueryRuns(connection, "CREATE FUNCTION public.casttoint(text) RETURNS integer STRICT IMMUTABLE LANGUAGE SQL AS 'SELECT cast($1 as integer);'")
			defer testhelper.AssertQueryRuns(connection, "DROP FUNCTION public.casttoint(text)")
			testhelper.AssertQueryRuns(connection, "CREATE CAST (text AS integer) WITH FUNCTION public.casttoint(text) AS ASSIGNMENT")
			defer testhelper.AssertQueryRuns(connection, "DROP CAST (text AS int4)")

			results := backup.GetCasts(connection)

			castDef := backup.Cast{Oid: 0, SourceTypeFQN: "pg_catalog.text", TargetTypeFQN: "pg_catalog.int4", FunctionSchema: "public", FunctionName: "casttoint", FunctionArgs: "text", CastContext: "a", CastMethod: "f"}

			Expect(results).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&castDef, &results[0], "Oid")
		})
		It("returns a slice for a basic cast without a function", func() {
			testhelper.AssertQueryRuns(connection, "CREATE FUNCTION public.cast_in(cstring) RETURNS public.casttesttype AS $$textin$$ LANGUAGE internal STRICT NO SQL")
			testhelper.AssertQueryRuns(connection, "CREATE FUNCTION public.cast_out(public.casttesttype) RETURNS cstring AS $$textout$$ LANGUAGE internal STRICT NO SQL")
			testhelper.AssertQueryRuns(connection, "CREATE TYPE public.casttesttype (INTERNALLENGTH = variable, INPUT = public.cast_in, OUTPUT = public.cast_out)")
			defer testhelper.AssertQueryRuns(connection, "DROP TYPE public.casttesttype CASCADE")
			testhelper.AssertQueryRuns(connection, "CREATE CAST (text AS public.casttesttype) WITHOUT FUNCTION AS IMPLICIT")
			defer testhelper.AssertQueryRuns(connection, "DROP CAST (text AS public.casttesttype)")

			results := backup.GetCasts(connection)

			castDef := backup.Cast{Oid: 0, SourceTypeFQN: "pg_catalog.text", TargetTypeFQN: "public.casttesttype", FunctionSchema: "", FunctionName: "", FunctionArgs: "", CastContext: "i", CastMethod: "b"}

			Expect(results).To(HaveLen(1))
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

			Expect(results).To(HaveLen(2))
			structmatcher.ExpectStructsToMatchExcluding(&castDefTarget, &results[0], "Oid")
			structmatcher.ExpectStructsToMatchExcluding(&castDefSource, &results[1], "Oid")
		})
		It("returns a slice for an inout cast", func() {
			testutils.SkipIfBefore6(connection)
			testhelper.AssertQueryRuns(connection, "CREATE TYPE public.custom_numeric AS (i numeric)")
			defer testhelper.AssertQueryRuns(connection, "DROP TYPE public.custom_numeric")
			testhelper.AssertQueryRuns(connection, "CREATE CAST (varchar AS public.custom_numeric) WITH INOUT")
			defer testhelper.AssertQueryRuns(connection, "DROP CAST (varchar AS public.custom_numeric)")

			results := backup.GetCasts(connection)

			castDef := backup.Cast{Oid: 0, SourceTypeFQN: `pg_catalog."varchar"`, TargetTypeFQN: "public.custom_numeric", FunctionSchema: "", FunctionName: "", FunctionArgs: "", CastContext: "e", CastMethod: "i"}

			Expect(results).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&castDef, &results[0], "Oid")
		})
	})
	Describe("GetExtensions", func() {
		It("returns a slice of extension", func() {
			testutils.SkipIfBefore5(connection)
			testhelper.AssertQueryRuns(connection, "CREATE EXTENSION plperl")
			defer testhelper.AssertQueryRuns(connection, "DROP EXTENSION plperl")

			results := backup.GetExtensions(connection)

			Expect(results).To(HaveLen(1))

			plperlDef := backup.Extension{Oid: 0, Name: "plperl", Schema: "pg_catalog"}
			structmatcher.ExpectStructsToMatchExcluding(&plperlDef, &results[0], "Oid")
		})
	})
	Describe("GetProceduralLanguages", func() {
		It("returns a slice of procedural languages", func() {
			testhelper.AssertQueryRuns(connection, "CREATE LANGUAGE plpythonu")
			defer testhelper.AssertQueryRuns(connection, "DROP LANGUAGE plpythonu")

			pythonHandlerOid := testutils.OidFromObjectName(connection, "pg_catalog", "plpython_call_handler", backup.TYPE_FUNCTION)

			expectedPlpythonInfo := backup.ProceduralLanguage{Oid: 1, Name: "plpythonu", Owner: "testrole", IsPl: true, PlTrusted: false, Handler: pythonHandlerOid, Inline: 0, Validator: 0}
			if connection.Version.AtLeast("5") {
				pythonInlineOid := testutils.OidFromObjectName(connection, "pg_catalog", "plpython_inline_handler", backup.TYPE_FUNCTION)
				expectedPlpythonInfo.Inline = pythonInlineOid
			}
			if connection.Version.AtLeast("6") {
				expectedPlpythonInfo.Validator = testutils.OidFromObjectName(connection, "pg_catalog", "plpython_validator", backup.TYPE_FUNCTION)
			}

			resultProcLangs := backup.GetProceduralLanguages(connection)

			Expect(resultProcLangs).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&expectedPlpythonInfo, &resultProcLangs[0], "Oid", "Owner")
		})
	})
	Describe("GetConversions", func() {
		It("returns a slice of conversions", func() {
			testhelper.AssertQueryRuns(connection, "CREATE CONVERSION public.testconv FOR 'LATIN1' TO 'MULE_INTERNAL' FROM latin1_to_mic")
			defer testhelper.AssertQueryRuns(connection, "DROP CONVERSION public.testconv")

			expectedConversion := backup.Conversion{Oid: 0, Schema: "public", Name: "testconv", ForEncoding: "LATIN1", ToEncoding: "MULE_INTERNAL", ConversionFunction: "pg_catalog.latin1_to_mic", IsDefault: false}

			resultConversions := backup.GetConversions(connection)

			Expect(resultConversions).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&expectedConversion, &resultConversions[0], "Oid")
		})
		It("returns a slice of conversions in a specific schema", func() {
			testhelper.AssertQueryRuns(connection, "CREATE CONVERSION public.testconv FOR 'LATIN1' TO 'MULE_INTERNAL' FROM latin1_to_mic")
			defer testhelper.AssertQueryRuns(connection, "DROP CONVERSION public.testconv")
			testhelper.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
			defer testhelper.AssertQueryRuns(connection, "DROP SCHEMA testschema")
			testhelper.AssertQueryRuns(connection, "CREATE CONVERSION testschema.testconv FOR 'LATIN1' TO 'MULE_INTERNAL' FROM latin1_to_mic")
			defer testhelper.AssertQueryRuns(connection, "DROP CONVERSION testschema.testconv")

			expectedConversion := backup.Conversion{Oid: 0, Schema: "testschema", Name: "testconv", ForEncoding: "LATIN1", ToEncoding: "MULE_INTERNAL", ConversionFunction: "pg_catalog.latin1_to_mic", IsDefault: false}

			backupCmdFlags.Set(utils.INCLUDE_SCHEMA, "testschema")
			resultConversions := backup.GetConversions(connection)

			Expect(resultConversions).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&expectedConversion, &resultConversions[0], "Oid")
		})
	})
	// Describe("ConstructFunctionDependencies", func() {
	// 	BeforeEach(func() {
	// 		testhelper.AssertQueryRuns(connection, "CREATE TYPE public.composite_ints AS (one integer, two integer)")
	// 	})
	// 	AfterEach(func() {
	// 		testhelper.AssertQueryRuns(connection, "DROP TYPE public.composite_ints CASCADE")
	// 	})
	// TODO :convert these to tests for general dependency function
	// It("constructs dependencies correctly for a function dependent on a user-defined type in the arguments", func() {
	// 	testhelper.AssertQueryRuns(connection, "CREATE FUNCTION public.add(public.composite_ints) RETURNS integer STRICT IMMUTABLE LANGUAGE SQL AS 'SELECT ($1.one + $1.two);'")
	// 	defer testhelper.AssertQueryRuns(connection, "DROP FUNCTION public.add(public.composite_ints)")

	// 	allFunctions := backup.GetFunctionsAllVersions(connection)
	// 	function := backup.Function{}
	// 	for _, funct := range allFunctions {
	// 		if funct.Name == "add" {
	// 			function = funct
	// 			break
	// 		}
	// 	}
	// 	functions := []backup.Function{function}

	// 	functions = backup.ConstructFunctionDependencies(connection, functions)

	// 	Expect(functions).To(HaveLen(1))
	// 	Expect(functions[0].DependsUpon).To(HaveLen(1))
	// 	Expect(functions[0].DependsUpon[0]).To(Equal("public.composite_ints"))
	// })
	// It("constructs dependencies correctly for a function dependent on a user-defined type in the return type", func() {
	// 	testhelper.AssertQueryRuns(connection, "CREATE FUNCTION public.compose(integer, integer) RETURNS public.composite_ints STRICT IMMUTABLE LANGUAGE PLPGSQL AS 'DECLARE comp public.composite_ints; BEGIN SELECT $1, $2 INTO comp; RETURN comp; END;';")
	// 	defer testhelper.AssertQueryRuns(connection, "DROP FUNCTION public.compose(integer, integer)")

	// 	allFunctions := backup.GetFunctionsAllVersions(connection)
	// 	function := backup.Function{}
	// 	for _, funct := range allFunctions {
	// 		if funct.Name == "compose" {
	// 			function = funct
	// 			break
	// 		}
	// 	}
	// 	functions := []backup.Function{function}

	// 	functions = backup.ConstructFunctionDependencies(connection, functions)

	// 	Expect(functions).To(HaveLen(1))
	// 	Expect(functions[0].DependsUpon).To(HaveLen(1))
	// 	Expect(functions[0].DependsUpon[0]).To(Equal("public.composite_ints"))
	// })
	// It("constructs dependencies correctly for a function dependent on an implicit array type", func() {
	// 	testhelper.AssertQueryRuns(connection, "CREATE TYPE public.base_type")
	// 	defer testhelper.AssertQueryRuns(connection, "DROP TYPE public.base_type CASCADE")
	// 	testhelper.AssertQueryRuns(connection, "CREATE FUNCTION public.base_fn_in(cstring) RETURNS public.base_type AS 'boolin' LANGUAGE internal")
	// 	testhelper.AssertQueryRuns(connection, "CREATE FUNCTION public.base_fn_out(public.base_type) RETURNS cstring AS 'boolout' LANGUAGE internal")
	// 	testhelper.AssertQueryRuns(connection, "CREATE TYPE public.base_type(INPUT=public.base_fn_in, OUTPUT=public.base_fn_out)")
	// 	testhelper.AssertQueryRuns(connection, "CREATE FUNCTION public.compose(public.base_type[], public.composite_ints) RETURNS public.composite_ints STRICT IMMUTABLE LANGUAGE PLPGSQL AS 'DECLARE comp public.composite_ints; BEGIN SELECT $1[0].one+$2.one, $1[0].two+$2.two INTO comp; RETURN comp; END;';")
	// 	defer testhelper.AssertQueryRuns(connection, "DROP FUNCTION public.compose(public.base_type[], public.composite_ints)")

	// 	allFunctions := backup.GetFunctionsAllVersions(connection)
	// 	function := backup.Function{}
	// 	for _, funct := range allFunctions {
	// 		if funct.Name == "compose" {
	// 			function = funct
	// 			break
	// 		}
	// 	}
	// 	functions := []backup.Function{function}

	// 	functions = backup.ConstructFunctionDependencies(connection, functions)

	// 	Expect(functions).To(HaveLen(1))
	// 	Expect(functions[0].DependsUpon).To(HaveLen(3))
	// 	Expect(functions[0].DependsUpon[0]).To(Equal("public.composite_ints"))
	// 	Expect(functions[0].DependsUpon[1]).To(Equal("public.base_type"))
	// 	Expect(functions[0].DependsUpon[2]).To(Equal("public.composite_ints"))
	// })
	// })
	Describe("GetForeignDataWrappers", func() {
		It("returns a slice of foreign data wrappers", func() {
			testutils.SkipIfBefore6(connection)
			testhelper.AssertQueryRuns(connection, "CREATE FOREIGN DATA WRAPPER foreigndatawrapper")
			defer testhelper.AssertQueryRuns(connection, "DROP FOREIGN DATA WRAPPER foreigndatawrapper")

			expectedForeignDataWrapper := backup.ForeignDataWrapper{Oid: 0, Name: "foreigndatawrapper"}

			resultForeignDataWrapper := backup.GetForeignDataWrappers(connection)

			Expect(resultForeignDataWrapper).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&expectedForeignDataWrapper, &resultForeignDataWrapper[0], "Oid")
		})
		It("returns a slice of foreign data wrappers with a validator", func() {
			testutils.SkipIfBefore6(connection)
			testhelper.AssertQueryRuns(connection, "CREATE FOREIGN DATA WRAPPER foreigndatawrapper VALIDATOR postgresql_fdw_validator")
			defer testhelper.AssertQueryRuns(connection, "DROP FOREIGN DATA WRAPPER foreigndatawrapper")

			validatorOid := testutils.OidFromObjectName(connection, "pg_catalog", "postgresql_fdw_validator", backup.TYPE_FUNCTION)
			expectedForeignDataWrapper := backup.ForeignDataWrapper{Oid: 0, Name: "foreigndatawrapper", Validator: validatorOid}

			resultForeignDataWrapper := backup.GetForeignDataWrappers(connection)

			Expect(resultForeignDataWrapper).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&expectedForeignDataWrapper, &resultForeignDataWrapper[0], "Oid")
		})
		It("returns a slice of foreign data wrappers with options", func() {
			testutils.SkipIfBefore6(connection)
			testhelper.AssertQueryRuns(connection, "CREATE FOREIGN DATA WRAPPER foreigndatawrapper OPTIONS (dbname 'testdb', debug 'true')")
			defer testhelper.AssertQueryRuns(connection, "DROP FOREIGN DATA WRAPPER foreigndatawrapper")

			expectedForeignDataWrapper := backup.ForeignDataWrapper{Oid: 0, Name: "foreigndatawrapper", Options: "dbname 'testdb', debug 'true'"}

			resultForeignDataWrappers := backup.GetForeignDataWrappers(connection)

			Expect(resultForeignDataWrappers).To(HaveLen(1))
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

			Expect(resultServers).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&expectedServer, &resultServers[0], "Oid")
		})
		It("returns a slice of foreign servers with a type and version", func() {
			testutils.SkipIfBefore6(connection)
			testhelper.AssertQueryRuns(connection, "CREATE FOREIGN DATA WRAPPER foreigndatawrapper")
			defer testhelper.AssertQueryRuns(connection, "DROP FOREIGN DATA WRAPPER foreigndatawrapper CASCADE")
			testhelper.AssertQueryRuns(connection, "CREATE SERVER foreignserver TYPE 'mytype' VERSION 'myversion' FOREIGN DATA WRAPPER foreigndatawrapper")

			expectedServer := backup.ForeignServer{Oid: 1, Name: "foreignserver", Type: "mytype", Version: "myversion", ForeignDataWrapper: "foreigndatawrapper"}

			resultServers := backup.GetForeignServers(connection)

			Expect(resultServers).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&expectedServer, &resultServers[0], "Oid")
		})
		It("returns a slice of foreign servers with options", func() {
			testutils.SkipIfBefore6(connection)
			testhelper.AssertQueryRuns(connection, "CREATE FOREIGN DATA WRAPPER foreigndatawrapper")
			defer testhelper.AssertQueryRuns(connection, "DROP FOREIGN DATA WRAPPER foreigndatawrapper CASCADE")
			testhelper.AssertQueryRuns(connection, "CREATE SERVER foreignserver FOREIGN DATA WRAPPER foreigndatawrapper OPTIONS (dbname 'testdb', host 'localhost')")

			expectedServer := backup.ForeignServer{Oid: 1, Name: "foreignserver", ForeignDataWrapper: "foreigndatawrapper", Options: "dbname 'testdb', host 'localhost'"}

			resultServers := backup.GetForeignServers(connection)

			Expect(resultServers).To(HaveLen(1))
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

			Expect(resultMappings).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&expectedMapping, &resultMappings[0], "Oid")
		})
		It("returns a slice of user mappings with options", func() {
			testutils.SkipIfBefore6(connection)
			testhelper.AssertQueryRuns(connection, "CREATE FOREIGN DATA WRAPPER foreigndatawrapper")
			defer testhelper.AssertQueryRuns(connection, "DROP FOREIGN DATA WRAPPER foreigndatawrapper CASCADE")
			testhelper.AssertQueryRuns(connection, "CREATE SERVER foreignserver FOREIGN DATA WRAPPER foreigndatawrapper")
			testhelper.AssertQueryRuns(connection, "CREATE USER MAPPING FOR public SERVER foreignserver OPTIONS (dbname 'testdb', host 'localhost')")

			expectedMapping := backup.UserMapping{Oid: 1, User: "public", Server: "foreignserver", Options: "dbname 'testdb', host 'localhost'"}

			resultMappings := backup.GetUserMappings(connection)

			Expect(resultMappings).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&expectedMapping, &resultMappings[0], "Oid")
		})
		It("returns a slice of user mappings in sorted order", func() {
			testutils.SkipIfBefore6(connection)
			testhelper.AssertQueryRuns(connection, "CREATE FOREIGN DATA WRAPPER foreigndatawrapper")
			defer testhelper.AssertQueryRuns(connection, "DROP FOREIGN DATA WRAPPER foreigndatawrapper CASCADE")
			testhelper.AssertQueryRuns(connection, "CREATE SERVER foreignserver FOREIGN DATA WRAPPER foreigndatawrapper")
			testhelper.AssertQueryRuns(connection, "CREATE USER MAPPING FOR testrole SERVER foreignserver")
			testhelper.AssertQueryRuns(connection, "CREATE USER MAPPING FOR anothertestrole SERVER foreignserver")

			expectedMapping := []backup.UserMapping{
				{Oid: 1, User: "anothertestrole", Server: "foreignserver"},
				{Oid: 1, User: "testrole", Server: "foreignserver"},
			}

			resultMappings := backup.GetUserMappings(connection)

			Expect(resultMappings).To(HaveLen(2))
			for idx := range expectedMapping {
				structmatcher.ExpectStructsToMatchExcluding(&expectedMapping[idx], &resultMappings[idx], "Oid")
			}
		})
	})
})
