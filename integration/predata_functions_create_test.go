package integration

import (
	"database/sql"
	"fmt"

	"github.com/greenplum-db/gp-common-go-libs/structmatcher"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup integration create statement tests", func() {
	BeforeEach(func() {
		tocfile, backupfile = testutils.InitializeTestTOC(buffer, "predata")
	})
	Describe("PrintCreateFunctionStatement", func() {
		Context("Tests for GPDB 4.3", func() {
			BeforeEach(func() {
				testutils.SkipIfNot4(connectionPool)
			})
			funcMetadata := backup.ObjectMetadata{}
			It("creates a function with a simple return type", func() {
				addFunction := backup.Function{
					Schema: "public", Name: "add", ReturnsSet: false, FunctionBody: "SELECT $1 + $2",
					BinaryPath: "", Arguments: sql.NullString{String: "integer, integer", Valid: true},
					IdentArgs:  sql.NullString{String: "integer, integer", Valid: true},
					ResultType: sql.NullString{String: "integer", Valid: true},
					Volatility: "v", IsStrict: false, IsSecurityDefiner: false, Config: "", NumRows: 0, Language: "sql", ExecLocation: "a",
				}

				metadata := testutils.DefaultMetadata("FUNCTION", true, true, true, false)
				backup.PrintCreateFunctionStatement(backupfile, tocfile, addFunction, metadata)

				testhelper.AssertQueryRuns(connectionPool, buffer.String())
				defer testhelper.AssertQueryRuns(connectionPool, "DROP FUNCTION public.add(integer, integer)")

				resultFunctions := backup.GetFunctionsAllVersions(connectionPool)

				Expect(resultFunctions).To(HaveLen(1))
				structmatcher.ExpectStructsToMatchExcluding(&addFunction, &resultFunctions[0], "Oid")
			})
			It("creates a function that returns a set", func() {
				appendFunction := backup.Function{
					Schema: "public", Name: "append", ReturnsSet: true, FunctionBody: "SELECT ($1, $2)",
					BinaryPath: "", Arguments: sql.NullString{String: "integer, integer", Valid: true},
					IdentArgs:  sql.NullString{String: "integer, integer", Valid: true},
					ResultType: sql.NullString{String: "SETOF record", Valid: true},
					Volatility: "s", IsStrict: true, IsSecurityDefiner: true, Language: "sql", ExecLocation: "a",
				}

				backup.PrintCreateFunctionStatement(backupfile, tocfile, appendFunction, funcMetadata)

				testhelper.AssertQueryRuns(connectionPool, buffer.String())
				defer testhelper.AssertQueryRuns(connectionPool, "DROP FUNCTION public.append(integer, integer)")

				resultFunctions := backup.GetFunctionsAllVersions(connectionPool)

				Expect(resultFunctions).To(HaveLen(1))
				structmatcher.ExpectStructsToMatchExcluding(&appendFunction, &resultFunctions[0], "Oid")
			})
			It("creates a function that returns a table", func() {
				dupFunction := backup.Function{
					Schema: "public", Name: "dup", ReturnsSet: true, FunctionBody: "SELECT $1, CAST($1 AS text) || ' is text'",
					BinaryPath: "", Arguments: sql.NullString{String: "integer", Valid: true},
					IdentArgs:  sql.NullString{String: "integer", Valid: true},
					ResultType: sql.NullString{String: "TABLE(f1 integer, f2 text)", Valid: true},
					Volatility: "v", IsStrict: false, IsSecurityDefiner: false, Language: "sql", ExecLocation: "a",
				}

				backup.PrintCreateFunctionStatement(backupfile, tocfile, dupFunction, funcMetadata)

				testhelper.AssertQueryRuns(connectionPool, buffer.String())
				defer testhelper.AssertQueryRuns(connectionPool, "DROP FUNCTION public.dup(integer)")

				resultFunctions := backup.GetFunctionsAllVersions(connectionPool)

				Expect(resultFunctions).To(HaveLen(1))
				structmatcher.ExpectStructsToMatchExcluding(&dupFunction, &resultFunctions[0], "Oid")
			})
		})
		Context("Tests for GPDB 5 and above", func() {
			BeforeEach(func() {
				testutils.SkipIfBefore5(connectionPool)
			})
			funcMetadata := backup.ObjectMetadata{}
			It("creates a function with a simple return type", func() {
				addFunction := backup.Function{
					Schema: "public", Name: "add", ReturnsSet: false, FunctionBody: "SELECT $1 + $2",
					BinaryPath: "", Arguments: sql.NullString{String: "integer, integer", Valid: true},
					IdentArgs:  sql.NullString{String: "integer, integer", Valid: true},
					ResultType: sql.NullString{String: "integer", Valid: true},
					Volatility: "v", IsStrict: false, IsSecurityDefiner: false, Config: "", Cost: 100, NumRows: 0, DataAccess: "c",
					Language: "sql", ExecLocation: "a",
				}
				if connectionPool.Version.AtLeast("7") {
					addFunction.PlannerSupport = "-"
					addFunction.Kind = "f"
					addFunction.Parallel = "u"
					addFunction.DataAccess = ""
				}

				metadata := testutils.DefaultMetadata("FUNCTION", true, true, true, includeSecurityLabels)
				backup.PrintCreateFunctionStatement(backupfile, tocfile, addFunction, metadata)

				testhelper.AssertQueryRuns(connectionPool, buffer.String())
				defer testhelper.AssertQueryRuns(connectionPool, "DROP FUNCTION public.add(integer, integer)")

				resultFunctions := backup.GetFunctionsAllVersions(connectionPool)

				Expect(resultFunctions).To(HaveLen(1))

				structmatcher.ExpectStructsToMatchExcluding(&addFunction, &resultFunctions[0], "Oid")
			})
			It("creates a function that returns a set", func() {
				appendFunction := backup.Function{
					Schema: "public", Name: "append", ReturnsSet: true, FunctionBody: "SELECT ($1, $2)",
					BinaryPath: "", Arguments: sql.NullString{String: "integer, integer", Valid: true},
					IdentArgs:  sql.NullString{String: "integer, integer", Valid: true},
					ResultType: sql.NullString{String: "SETOF record", Valid: true},
					Volatility: "s", IsStrict: true, IsSecurityDefiner: true, Config: "SET search_path TO 'pg_temp'", Cost: 200,
					NumRows: 200, DataAccess: "m", Language: "sql", ExecLocation: "a",
				}
				if connectionPool.Version.AtLeast("7") {
					appendFunction.PlannerSupport = "-"
					appendFunction.Kind = "f"
					appendFunction.Parallel = "u"
					appendFunction.DataAccess = ""
				}

				backup.PrintCreateFunctionStatement(backupfile, tocfile, appendFunction, funcMetadata)

				testhelper.AssertQueryRuns(connectionPool, buffer.String())
				defer testhelper.AssertQueryRuns(connectionPool, "DROP FUNCTION public.append(integer, integer)")

				resultFunctions := backup.GetFunctionsAllVersions(connectionPool)

				Expect(resultFunctions).To(HaveLen(1))

				structmatcher.ExpectStructsToMatchExcluding(&appendFunction, &resultFunctions[0], "Oid")
			})
			It("creates a function that returns a table", func() {
				dupFunction := backup.Function{
					Schema: "public", Name: "dup", ReturnsSet: true, FunctionBody: "SELECT $1, CAST($1 AS text) || ' is text'",
					BinaryPath: "", Arguments: sql.NullString{String: "integer", Valid: true},
					IdentArgs:  sql.NullString{String: "integer", Valid: true},
					ResultType: sql.NullString{String: "TABLE(f1 integer, f2 text)", Valid: true},
					Volatility: "v", IsStrict: false, IsSecurityDefiner: false, Config: "", Cost: 100, NumRows: 1000, DataAccess: "c",
					Language: "sql", ExecLocation: "a",
				}
				if connectionPool.Version.AtLeast("7") {
					dupFunction.PlannerSupport = "-"
					dupFunction.Kind = "f"
					dupFunction.Parallel = "u"
					dupFunction.DataAccess = ""
				}

				backup.PrintCreateFunctionStatement(backupfile, tocfile, dupFunction, funcMetadata)

				testhelper.AssertQueryRuns(connectionPool, buffer.String())
				defer testhelper.AssertQueryRuns(connectionPool, "DROP FUNCTION public.dup(integer)")

				resultFunctions := backup.GetFunctionsAllVersions(connectionPool)

				Expect(resultFunctions).To(HaveLen(1))
				structmatcher.ExpectStructsToMatchExcluding(&dupFunction, &resultFunctions[0], "Oid")
			})
		})
		Context("Tests for GPDB 6", func() {
			BeforeEach(func() {
				testutils.SkipIfBefore6(connectionPool)
			})
			funcMetadata := backup.ObjectMetadata{}
			It("creates a window function to execute on coordinator", func() {
				windowFunction := backup.Function{
					Schema: "public", Name: "add", ReturnsSet: false, FunctionBody: "SELECT $1 + $2",
					BinaryPath: "", Arguments: sql.NullString{String: "integer, integer", Valid: true},
					IdentArgs:  sql.NullString{String: "integer, integer", Valid: true},
					ResultType: sql.NullString{String: "integer", Valid: true},
					Volatility: "v", IsStrict: false, IsSecurityDefiner: false, Config: "", Cost: 100, NumRows: 0, DataAccess: "c",
					Language: "sql", ExecLocation: "m", IsWindow: true}
				if connectionPool.Version.AtLeast("7") {
					windowFunction.PlannerSupport = "-"
					windowFunction.Kind = "w"
					windowFunction.Parallel = "u"
					windowFunction.ExecLocation = "c"
					windowFunction.DataAccess = ""

					// GPDB7 only allows set-returning functions to execute on coordinator
					windowFunction.ReturnsSet = true
					windowFunction.NumRows = 1000
					windowFunction.ResultType = sql.NullString{String: "SETOF integer", Valid: true}
				}

				backup.PrintCreateFunctionStatement(backupfile, tocfile, windowFunction, funcMetadata)

				testhelper.AssertQueryRuns(connectionPool, buffer.String())
				defer testhelper.AssertQueryRuns(connectionPool, "DROP FUNCTION public.add(integer, integer)")

				resultFunctions := backup.GetFunctionsAllVersions(connectionPool)

				Expect(resultFunctions).To(HaveLen(1))
				structmatcher.ExpectStructsToMatchExcluding(&windowFunction, &resultFunctions[0], "Oid")
			})
			It("creates a function to execute on segments", func() {
				segmentFunction := backup.Function{
					Schema: "public", Name: "add", ReturnsSet: false, FunctionBody: "SELECT $1 + $2",
					BinaryPath: "", Arguments: sql.NullString{String: "integer, integer", Valid: true},
					IdentArgs:  sql.NullString{String: "integer, integer", Valid: true},
					ResultType: sql.NullString{String: "integer", Valid: true},
					Volatility: "v", IsStrict: false, IsSecurityDefiner: false, Config: "", Cost: 100, NumRows: 0, DataAccess: "c",
					Language: "sql", IsWindow: false, ExecLocation: "s",
				}
				if connectionPool.Version.AtLeast("7") {
					segmentFunction.PlannerSupport = "-"
					segmentFunction.Kind = "f"
					segmentFunction.Parallel = "u"
					segmentFunction.DataAccess = ""

					// GPDB7 only allows set-returning functions to execute on coordinator
					segmentFunction.ReturnsSet = true
					segmentFunction.NumRows = 1000
					segmentFunction.ResultType = sql.NullString{String: "SETOF integer", Valid: true}
				}

				backup.PrintCreateFunctionStatement(backupfile, tocfile, segmentFunction, funcMetadata)

				testhelper.AssertQueryRuns(connectionPool, buffer.String())
				defer testhelper.AssertQueryRuns(connectionPool, "DROP FUNCTION public.add(integer, integer)")

				resultFunctions := backup.GetFunctionsAllVersions(connectionPool)

				Expect(resultFunctions).To(HaveLen(1))
				structmatcher.ExpectStructsToMatchExcluding(&segmentFunction, &resultFunctions[0], "Oid")
			})
			It("creates a function with LEAKPROOF", func() {
				leakProofFunction := backup.Function{
					Schema: "public", Name: "add", ReturnsSet: false, FunctionBody: "SELECT $1 + $2",
					BinaryPath: "", Arguments: sql.NullString{String: "integer, integer", Valid: true},
					IdentArgs:  sql.NullString{String: "integer, integer", Valid: true},
					ResultType: sql.NullString{String: "integer", Valid: true},
					Volatility: "v", IsStrict: false, IsLeakProof: true, IsSecurityDefiner: false, Config: "", Cost: 100, NumRows: 0, DataAccess: "c",
					Language: "sql", IsWindow: false, ExecLocation: "a",
				}
				if connectionPool.Version.AtLeast("7") {
					leakProofFunction.PlannerSupport = "-"
					leakProofFunction.Kind = "f"
					leakProofFunction.Parallel = "u"
					leakProofFunction.DataAccess = ""
				}

				backup.PrintCreateFunctionStatement(backupfile, tocfile, leakProofFunction, funcMetadata)

				testhelper.AssertQueryRuns(connectionPool, buffer.String())
				defer testhelper.AssertQueryRuns(connectionPool, "DROP FUNCTION public.add(integer, integer)")

				resultFunctions := backup.GetFunctionsAllVersions(connectionPool)

				Expect(resultFunctions).To(HaveLen(1))
				structmatcher.ExpectStructsToMatchExcluding(&leakProofFunction, &resultFunctions[0], "Oid")
			})
		})
		Context("Tests for GPDB 7", func() {
			BeforeEach(func() {
				testutils.SkipIfBefore7(connectionPool)
			})
			funcMetadata := backup.ObjectMetadata{}
			It("creates a function with PARALLEL RESTRICTED", func() {
				ParallelFunction := backup.Function{
					Schema: "public", Name: "add", ReturnsSet: false, FunctionBody: "SELECT $1 + $2",
					BinaryPath: "", Arguments: sql.NullString{String: "integer, integer", Valid: true},
					IdentArgs:  sql.NullString{String: "integer, integer", Valid: true},
					ResultType: sql.NullString{String: "integer", Valid: true},
					Volatility: "v", IsStrict: false, IsLeakProof: false, IsSecurityDefiner: false, Config: "", Cost: 100, NumRows: 0, DataAccess: "",
					Language: "sql", IsWindow: false, ExecLocation: "a", PlannerSupport: "-", Kind: "f", Parallel: "r",
				}

				backup.PrintCreateFunctionStatement(backupfile, tocfile, ParallelFunction, funcMetadata)

				testhelper.AssertQueryRuns(connectionPool, buffer.String())
				defer testhelper.AssertQueryRuns(connectionPool, "DROP FUNCTION public.add(integer, integer)")

				resultFunctions := backup.GetFunctionsAllVersions(connectionPool)

				Expect(resultFunctions).To(HaveLen(1))
				structmatcher.ExpectStructsToMatchExcluding(&ParallelFunction, &resultFunctions[0], "Oid")
			})
			// This test is pended because getting the DLL for hstore_plperl compiled and working in our CI is proving very difficult.
			// TODO: get this working in CI, and un-pend it then.
			PIt("creates a function with TRANSFORM FOR TYPE", func() {
				TransformFunction := backup.Function{
					Schema: "public", Name: "add", ReturnsSet: false, FunctionBody: "SELECT $1 + 1",
					BinaryPath: "", Arguments: sql.NullString{String: "hstore", Valid: true},
					IdentArgs:  sql.NullString{String: "hstore", Valid: true},
					ResultType: sql.NullString{String: "integer", Valid: true},
					Volatility: "v", IsStrict: false, IsLeakProof: false, IsSecurityDefiner: false, Config: "", Cost: 100, NumRows: 0, DataAccess: "",
					Language: "plperl", IsWindow: false, ExecLocation: "a", PlannerSupport: "-", Kind: "f", Parallel: "u",
					TransformTypes: "FOR TYPE pg_catalog.hstore",
				}
				backup.PrintCreateFunctionStatement(backupfile, tocfile, TransformFunction, funcMetadata)

				// set up types and transforms needed
				testhelper.AssertQueryRuns(connectionPool, "CREATE EXTENSION hstore;")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP EXTENSION hstore CASCADE;")

				testhelper.AssertQueryRuns(connectionPool, "CREATE EXTENSION plperl;")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP EXTENSION plperl CASCADE;")

				testhelper.AssertQueryRuns(connectionPool, `CREATE FUNCTION hstore_to_plperl(val internal) RETURNS internal 
						AS '$libdir/hstore_plperl.so'  LANGUAGE C STRICT IMMUTABLE;`)
				defer testhelper.AssertQueryRuns(connectionPool, "DROP FUNCTION hstore_to_plperl CASCADE;")

				testhelper.AssertQueryRuns(connectionPool, "CREATE TRANSFORM FOR hstore LANGUAGE plperl (FROM SQL WITH FUNCTION hstore_to_plperl(internal))")

				// create and assess function
				testhelper.AssertQueryRuns(connectionPool, buffer.String())
				defer testhelper.AssertQueryRuns(connectionPool, "DROP FUNCTION public.add(hstore)")

				resultFunctions := backup.GetFunctionsAllVersions(connectionPool)

				Expect(resultFunctions).To(HaveLen(1))
				structmatcher.ExpectStructsToMatchExcluding(&TransformFunction, &resultFunctions[0], "Oid")
			})
		})
	})
	Describe("PrintCreateAggregateStatement", func() {
		emptyMetadata := backup.ObjectMetadata{}
		basicAggregateDef := backup.Aggregate{}
		funcInfoMap := map[uint32]backup.FunctionInfo{
			1: {QualifiedName: "public.mysfunc_accum", Arguments: sql.NullString{String: "numeric, numeric, numeric", Valid: true}},
			2: {QualifiedName: "public.mypre_accum", Arguments: sql.NullString{String: "numeric, numeric", Valid: true}},
			3: {QualifiedName: "pg_catalog.ordered_set_transition_multi", Arguments: sql.NullString{String: `internal, VARIADIC "any"`, Valid: true}},
			4: {QualifiedName: "pg_catalog.rank_final", Arguments: sql.NullString{String: `internal, VARIADIC "any"`, Valid: true}},
			5: {QualifiedName: "pg_catalog.numeric_avg", Arguments: sql.NullString{String: "internal", Valid: true}},
			6: {QualifiedName: "pg_catalog.numeric_avg_serialize", Arguments: sql.NullString{String: "internal", Valid: true}},
			7: {QualifiedName: "pg_catalog.numeric_avg_deserialize", Arguments: sql.NullString{String: "bytea, internal", Valid: true}},
			8: {QualifiedName: "pg_catalog.numeric_avg_accum", Arguments: sql.NullString{String: "numeric, numeric", Valid: true}},
			9: {QualifiedName: "pg_catalog.power", Arguments: sql.NullString{String: "numeric, numeric", Valid: true}},
		}
		BeforeEach(func() {
			//Run queries to set up the database state so we can successfully create an aggregate
			testhelper.AssertQueryRuns(connectionPool, `
			CREATE FUNCTION public.mysfunc_accum(numeric, numeric, numeric)
			   RETURNS numeric
			   AS 'select $1 + $2 + $3'
			   LANGUAGE SQL
			   IMMUTABLE;
			`)
			testhelper.AssertQueryRuns(connectionPool, `
			CREATE FUNCTION public.mypre_accum(numeric, numeric)
			   RETURNS numeric
			   AS 'select $1 + $2'
			   LANGUAGE SQL
			   IMMUTABLE
			   RETURNS NULL ON NULL INPUT;
			`)

			basicAggregateDef = backup.Aggregate{
				Oid: 1, Schema: "public", Name: "agg_prefunc", Arguments: sql.NullString{String: "numeric, numeric", Valid: true},
				IdentArgs: sql.NullString{String: "numeric, numeric", Valid: true}, TransitionFunction: 1, PreliminaryFunction: 2,
				TransitionDataType: "numeric", InitialValue: "0", MInitValIsNull: true,
			}
			if connectionPool.Version.AtLeast("7") {
				basicAggregateDef.Kind = "n"
				basicAggregateDef.Finalmodify = "r"
				basicAggregateDef.Mfinalmodify = "r"
				basicAggregateDef.Parallel = "u"
			}
		})
		AfterEach(func() {
			testhelper.AssertQueryRuns(connectionPool, "DROP FUNCTION public.mysfunc_accum(numeric, numeric, numeric)")
			testhelper.AssertQueryRuns(connectionPool, "DROP FUNCTION public.mypre_accum(numeric, numeric)")
		})
		It("creates a basic aggregate", func() {
			backup.PrintCreateAggregateStatement(backupfile, tocfile, basicAggregateDef, funcInfoMap, emptyMetadata)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP AGGREGATE public.agg_prefunc(numeric, numeric)")

			resultAggregates := backup.GetAggregates(connectionPool)
			Expect(resultAggregates).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&basicAggregateDef, &resultAggregates[0], "Oid", "TransitionFunction", "PreliminaryFunction", "CombineFunction")
		})
		It("creates an aggregate with an owner, security label, and a comment", func() {
			aggMetadata := testutils.DefaultMetadata("AGGREGATE", false, true, true, includeSecurityLabels)
			backup.PrintCreateAggregateStatement(backupfile, tocfile, basicAggregateDef, funcInfoMap, aggMetadata)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP AGGREGATE public.agg_prefunc(numeric, numeric)")

			resultAggregates := backup.GetAggregates(connectionPool)
			Expect(resultAggregates).To(HaveLen(1))
			resultMetadataMap := backup.GetMetadataForObjectType(connectionPool, backup.TYPE_AGGREGATE)
			resultMetadata := resultMetadataMap[resultAggregates[0].GetUniqueID()]
			structmatcher.ExpectStructsToMatchExcluding(&basicAggregateDef, &resultAggregates[0], "Oid", "TransitionFunction", "PreliminaryFunction", "CombineFunction")
			structmatcher.ExpectStructsToMatch(&aggMetadata, &resultMetadata)
		})
		It("creates a hypothetical ordered-set aggregate", func() {
			testutils.SkipIfBefore6(connectionPool)
			complexAggregateDef := backup.Aggregate{
				Schema: "public", Name: "agg_hypo_ord", Arguments: sql.NullString{String: `VARIADIC "any" ORDER BY VARIADIC "any"`, Valid: true},
				IdentArgs: sql.NullString{String: `VARIADIC "any" ORDER BY VARIADIC "any"`, Valid: true}, TransitionFunction: 3, FinalFunction: 4,
				TransitionDataType: "internal", InitValIsNull: true, FinalFuncExtra: true, Hypothetical: true, MInitValIsNull: true,
			}
			if connectionPool.Version.AtLeast("7") {
				complexAggregateDef.Hypothetical = false
				complexAggregateDef.Kind = "h"
				complexAggregateDef.Finalmodify = "w"
				complexAggregateDef.Mfinalmodify = "w"
				complexAggregateDef.Parallel = "u"
			}

			backup.PrintCreateAggregateStatement(backupfile, tocfile, complexAggregateDef, funcInfoMap, emptyMetadata)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, `DROP AGGREGATE public.agg_hypo_ord(VARIADIC "any" ORDER BY VARIADIC "any")`)
			resultAggregates := backup.GetAggregates(connectionPool)

			Expect(resultAggregates).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&complexAggregateDef, &resultAggregates[0], "Oid", "TransitionFunction", "FinalFunction")
		})
		It("creates an aggregate with a sort operator", func() {
			aggregateDef := backup.Aggregate{
				Schema: "public", Name: "agg_sort", Arguments: sql.NullString{String: "numeric", Valid: true},
				IdentArgs: sql.NullString{String: "numeric", Valid: true}, TransitionFunction: 9, FinalFunction: 0,
				SortOperator: "+", SortOperatorSchema: "pg_catalog", TransitionDataType: "numeric",
				InitialValue: "0", IsOrdered: false, MInitValIsNull: true,
			}
			if connectionPool.Version.AtLeast("7") {
				aggregateDef.Kind = "n"
				aggregateDef.Finalmodify = "r"
				aggregateDef.Mfinalmodify = "r"
				aggregateDef.Parallel = "u"
			}

			backup.PrintCreateAggregateStatement(backupfile, tocfile, aggregateDef, funcInfoMap, emptyMetadata)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, `DROP AGGREGATE public.agg_sort(numeric)`)
			resultAggregates := backup.GetAggregates(connectionPool)

			Expect(resultAggregates).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&aggregateDef, &resultAggregates[0], "Oid", "TransitionFunction", "FinalFunction", "CombineFunction")
		})
		It("creates an aggregate with combine function and transition data size", func() {
			testutils.SkipIfBefore6(connectionPool)
			aggregateDef := backup.Aggregate{
				Schema: "public", Name: "agg_6_features", Arguments: sql.NullString{String: "numeric, numeric", Valid: true},
				IdentArgs: sql.NullString{String: "numeric, numeric", Valid: true}, TransitionFunction: 1, CombineFunction: 2,
				FinalFunction: 0, SortOperator: "", TransitionDataType: "numeric", TransitionDataSize: 1000,
				InitialValue: "0", IsOrdered: false, MInitValIsNull: true,
			}
			if connectionPool.Version.AtLeast("7") {
				aggregateDef.Kind = "n"
				aggregateDef.Finalmodify = "r"
				aggregateDef.Mfinalmodify = "r"
				aggregateDef.Parallel = "u"
			}

			backup.PrintCreateAggregateStatement(backupfile, tocfile, aggregateDef, funcInfoMap, emptyMetadata)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, `DROP AGGREGATE public.agg_6_features(numeric, numeric)`)
			resultAggregates := backup.GetAggregates(connectionPool)

			Expect(resultAggregates).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&aggregateDef, &resultAggregates[0], "Oid", "TransitionFunction", "FinalFunction", "CombineFunction")
		})
		It("creates an aggregate with serial/deserial functions", func() {
			testutils.SkipIfBefore6(connectionPool)
			aggregateDef := backup.Aggregate{
				Schema: "public", Name: "myavg", Arguments: sql.NullString{String: "numeric", Valid: true},
				IdentArgs: sql.NullString{String: "numeric", Valid: true}, TransitionFunction: 8,
				FinalFunction: 5, SerialFunction: 6, DeserialFunction: 7, TransitionDataType: "internal",
				IsOrdered: false, InitValIsNull: true, MInitValIsNull: true,
			}
			if connectionPool.Version.AtLeast("7") {
				aggregateDef.Kind = "n"
				aggregateDef.Finalmodify = "r"
				aggregateDef.Mfinalmodify = "r"
				aggregateDef.Parallel = "u"
			}

			backup.PrintCreateAggregateStatement(backupfile, tocfile, aggregateDef, funcInfoMap, emptyMetadata)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, `DROP AGGREGATE public.myavg(numeric)`)
			resultAggregates := backup.GetAggregates(connectionPool)

			Expect(resultAggregates).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&aggregateDef, &resultAggregates[0], "Oid", "TransitionFunction", "FinalFunction", "SerialFunction", "DeserialFunction")
		})
		It("creates an aggregate with moving attributes", func() {
			testutils.SkipIfBefore6(connectionPool)
			aggregateDef := backup.Aggregate{
				Schema: "public", Name: "moving_agg", Arguments: sql.NullString{String: "numeric, numeric", Valid: true},
				IdentArgs: sql.NullString{String: "numeric, numeric", Valid: true}, TransitionFunction: 1, TransitionDataType: "numeric",
				InitValIsNull: true, MTransitionFunction: 1, MInverseTransitionFunction: 1,
				MTransitionDataType: "numeric", MTransitionDataSize: 100, MFinalFunction: 1,
				MFinalFuncExtra: true, MInitialValue: "0", MInitValIsNull: false,
			}
			if connectionPool.Version.AtLeast("7") {
				aggregateDef.Kind = "n"
				aggregateDef.Finalmodify = "r"
				aggregateDef.Mfinalmodify = "r"
				aggregateDef.Parallel = "u"
			}

			backup.PrintCreateAggregateStatement(backupfile, tocfile, aggregateDef, funcInfoMap, emptyMetadata)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, `DROP AGGREGATE public.moving_agg(numeric, numeric)`)
			resultAggregates := backup.GetAggregates(connectionPool)

			Expect(resultAggregates).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&aggregateDef, &resultAggregates[0], "Oid", "TransitionFunction", "MTransitionFunction", "MInverseTransitionFunction", "MFinalFunction")
		})
		// TODO: test for aggregate with aggfinalmodify/aggmfinalmodify (no table, just couple examples to check on query is correct)
	})
	Describe("PrintCreateCastStatement", func() {
		var (
			castMetadata backup.ObjectMetadata
		)
		BeforeEach(func() {
			castMetadata = backup.ObjectMetadata{}
		})
		It("prints a basic cast with a function", func() {
			castDef := backup.Cast{Oid: 0, SourceTypeFQN: "pg_catalog.money", TargetTypeFQN: "pg_catalog.text", FunctionSchema: "public", FunctionName: "money_to_text", FunctionArgs: "money", CastContext: "a", CastMethod: "f"}

			testhelper.AssertQueryRuns(connectionPool, "CREATE FUNCTION public.money_to_text(money) RETURNS TEXT AS $$ SELECT textin(cash_out($1)) $$ LANGUAGE SQL;")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP FUNCTION public.money_to_text(money)")

			backup.PrintCreateCastStatement(backupfile, tocfile, castDef, castMetadata)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP CAST (money AS text)")

			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			resultCasts := backup.GetCasts(connectionPool)
			Expect(resultCasts).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&castDef, &resultCasts[0], "Oid", "FunctionOid")
		})
		It("prints a basic cast without a function", func() {
			castDef := backup.Cast{Oid: 0, SourceTypeFQN: "pg_catalog.text", TargetTypeFQN: "public.casttesttype", FunctionSchema: "", FunctionName: "", FunctionArgs: "", CastContext: "i", CastMethod: "b"}

			testhelper.AssertQueryRuns(connectionPool, "CREATE FUNCTION public.cast_in(cstring) RETURNS public.casttesttype AS $$textin$$ LANGUAGE internal STRICT NO SQL")
			testhelper.AssertQueryRuns(connectionPool, "CREATE FUNCTION public.cast_out(public.casttesttype) RETURNS cstring AS $$textout$$ LANGUAGE internal STRICT NO SQL")
			testhelper.AssertQueryRuns(connectionPool, "CREATE TYPE public.casttesttype (INTERNALLENGTH = variable, INPUT = public.cast_in, OUTPUT = public.cast_out)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TYPE public.casttesttype CASCADE")

			backup.PrintCreateCastStatement(backupfile, tocfile, castDef, castMetadata)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP CAST (text AS public.casttesttype)")

			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			resultCasts := backup.GetCasts(connectionPool)
			Expect(resultCasts).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&castDef, &resultCasts[0], "Oid")
		})
		It("prints a cast with a comment", func() {
			castDef := backup.Cast{Oid: 1, SourceTypeFQN: "pg_catalog.money", TargetTypeFQN: "pg_catalog.text", FunctionSchema: "public", FunctionName: "money_to_text", FunctionArgs: "money", CastContext: "a", CastMethod: "f"}
			castMetadata = testutils.DefaultMetadata("CAST", false, false, true, false)

			testhelper.AssertQueryRuns(connectionPool, "CREATE FUNCTION public.money_to_text(money) RETURNS TEXT AS $$ SELECT textin(cash_out($1)) $$ LANGUAGE SQL;")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP FUNCTION public.money_to_text(money)")

			backup.PrintCreateCastStatement(backupfile, tocfile, castDef, castMetadata)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP CAST (money AS text)")

			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			resultCasts := backup.GetCasts(connectionPool)
			Expect(resultCasts).To(HaveLen(1))
			resultMetadataMap := backup.GetCommentsForObjectType(connectionPool, backup.TYPE_CAST)
			resultMetadata := resultMetadataMap[resultCasts[0].GetUniqueID()]
			structmatcher.ExpectStructsToMatchExcluding(&castDef, &resultCasts[0], "Oid", "FunctionOid")
			structmatcher.ExpectStructsToMatchExcluding(&resultMetadata, &castMetadata, "Oid")
		})
		It("prints an inout cast ", func() {
			testutils.SkipIfBefore6(connectionPool)
			castDef := backup.Cast{Oid: 0, SourceTypeFQN: `pg_catalog."varchar"`, TargetTypeFQN: "public.custom_numeric", FunctionSchema: "", FunctionName: "", FunctionArgs: "", CastContext: "a", CastMethod: "i"}
			testhelper.AssertQueryRuns(connectionPool, "CREATE TYPE public.custom_numeric AS (i numeric)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TYPE public.custom_numeric")

			backup.PrintCreateCastStatement(backupfile, tocfile, castDef, castMetadata)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP CAST (varchar AS public.custom_numeric)")

			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			resultCasts := backup.GetCasts(connectionPool)
			Expect(resultCasts).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&castDef, &resultCasts[0], "Oid")
		})
	})
	Describe("PrintCreateLanguageStatements", func() {
		It("creates procedural languages", func() {
			plpythonString := "plpython"
			if connectionPool.Version.AtLeast("7") {
				plpythonString = "plpython3"
			}

			funcInfoMap := map[uint32]backup.FunctionInfo{
				1: {QualifiedName: fmt.Sprintf("pg_catalog.%s_call_handler", plpythonString), Arguments: sql.NullString{String: "", Valid: true}, IsInternal: true},
				2: {QualifiedName: fmt.Sprintf("pg_catalog.%s_inline_handler", plpythonString), Arguments: sql.NullString{String: "internal", Valid: true}, IsInternal: true},
			}
			langOwner := ""
			var langMetadata backup.ObjectMetadata
			if connectionPool.Version.Before("5") {
				langOwner = testutils.GetUserByID(connectionPool, 10)
				langMetadata = backup.ObjectMetadata{ObjectType: "LANGUAGE", Privileges: []backup.ACL{}, Owner: langOwner, Comment: "This is a language comment"}
			} else {
				langOwner = "testrole"
				langMetadata = testutils.DefaultMetadata("LANGUAGE", false, true, true, includeSecurityLabels)
			}
			plpythonInfo := backup.ProceduralLanguage{Oid: 1, Name: fmt.Sprintf("%su", plpythonString), Owner: langOwner, IsPl: true, PlTrusted: false, Handler: 1, Inline: 2}

			langMetadataMap := map[backup.UniqueID]backup.ObjectMetadata{plpythonInfo.GetUniqueID(): langMetadata}
			if connectionPool.Version.Before("5") {
				plpythonInfo.Inline = 0
			}
			procLangs := []backup.ProceduralLanguage{plpythonInfo}

			backup.PrintCreateLanguageStatements(backupfile, tocfile, procLangs, funcInfoMap, langMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, fmt.Sprintf("DROP LANGUAGE %su", plpythonString))

			resultProcLangs := backup.GetProceduralLanguages(connectionPool)
			resultMetadataMap := backup.GetMetadataForObjectType(connectionPool, backup.TYPE_PROCLANGUAGE)

			plpythonInfo.Oid = testutils.OidFromObjectName(connectionPool, "", fmt.Sprintf("%su", plpythonString), backup.TYPE_PROCLANGUAGE)
			Expect(resultProcLangs).To(HaveLen(1))
			resultMetadata := resultMetadataMap[plpythonInfo.GetUniqueID()]
			structmatcher.ExpectStructsToMatchIncluding(&plpythonInfo, &resultProcLangs[0], "Name", "IsPl", "PlTrusted")
			structmatcher.ExpectStructsToMatch(&langMetadata, &resultMetadata)
		})
	})
	Describe("PrintCreateExtensions", func() {
		It("creates extensions", func() {
			testutils.SkipIfBefore5(connectionPool)
			plperlExtension := backup.Extension{Oid: 1, Name: "plperl", Schema: "pg_catalog"}
			extensions := []backup.Extension{plperlExtension}
			extensionMetadataMap := testutils.DefaultMetadataMap("EXTENSION", false, false, true, false)
			extensionMetadata := extensionMetadataMap[plperlExtension.GetUniqueID()]
			backup.PrintCreateExtensionStatements(backupfile, tocfile, extensions, extensionMetadataMap)
			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP EXTENSION plperl; SET search_path=pg_catalog")
			resultExtensions := backup.GetExtensions(connectionPool)
			resultMetadataMap := backup.GetCommentsForObjectType(connectionPool, backup.TYPE_EXTENSION)
			plperlExtension.Oid = testutils.OidFromObjectName(connectionPool, "", "plperl", backup.TYPE_EXTENSION)
			Expect(resultExtensions).To(HaveLen(1))
			plperlMetadata := resultMetadataMap[plperlExtension.GetUniqueID()]
			structmatcher.ExpectStructsToMatch(&plperlExtension, &resultExtensions[0])
			structmatcher.ExpectStructsToMatch(&extensionMetadata, &plperlMetadata)
		})
	})
	Describe("PrintCreateTransformStatements", func() {
		var fromSQLFuncOid uint32
		var toSQLFuncOid uint32
		var funcInfoMap map[uint32]backup.FunctionInfo

		BeforeEach(func() {
			testutils.SkipIfBefore7(connectionPool)
			fromSQLFuncOid = testutils.OidFromObjectName(connectionPool, "pg_catalog", "numeric_support", backup.TYPE_FUNCTION)
			toSQLFuncOid = testutils.OidFromObjectName(connectionPool, "pg_catalog", "int2recv", backup.TYPE_FUNCTION)

			funcInfoMap = map[uint32]backup.FunctionInfo{
				fromSQLFuncOid: {QualifiedName: "numeric_support", IdentArgs: sql.NullString{String: "internal", Valid: true}},
				toSQLFuncOid:   {QualifiedName: "int2recv", IdentArgs: sql.NullString{String: "internal", Valid: true}},
			}
		})

		DescribeTable("creates transforms", func(fromSql func() uint32, toSql func() uint32) {
			transform := backup.Transform{Oid: 1, TypeNamespace: "pg_catalog", TypeName: "int2", LanguageName: "c", FromSQLFunc: fromSql(), ToSQLFunc: toSql()}
			transMetadata := testutils.DefaultMetadata("TRANSFORM", false, false, false, false)
			backup.PrintCreateTransformStatement(backupfile, tocfile, transform, funcInfoMap, transMetadata)
			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TRANSFORM FOR int2 LANGUAGE c")

			resultTransforms := backup.GetTransforms(connectionPool)
			Expect(resultTransforms).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&transform, &resultTransforms[0], "Oid")
		},
			Entry("both functions are specified", func() uint32 { return fromSQLFuncOid }, func() uint32 { return toSQLFuncOid }),
			Entry("only fromSQL function is specified", func() uint32 { return fromSQLFuncOid }, func() uint32 { return uint32(0) }),
			Entry("only toSql function is specified", func() uint32 { return uint32(0) }, func() uint32 { return toSQLFuncOid }),
		)
	})
	Describe("PrintCreateConversionStatements", func() {
		It("creates conversions", func() {
			convOne := backup.Conversion{Oid: 1, Schema: "public", Name: "conv_one", ForEncoding: "LATIN1", ToEncoding: "MULE_INTERNAL", ConversionFunction: "pg_catalog.latin1_to_mic", IsDefault: false}
			convTwo := backup.Conversion{Oid: 0, Schema: "public", Name: "conv_two", ForEncoding: "LATIN1", ToEncoding: "MULE_INTERNAL", ConversionFunction: "pg_catalog.latin1_to_mic", IsDefault: true}
			conversions := []backup.Conversion{convOne, convTwo}
			convMetadataMap := testutils.DefaultMetadataMap("CONVERSION", false, true, true, false)
			convMetadata := convMetadataMap[convOne.GetUniqueID()]

			backup.PrintCreateConversionStatements(backupfile, tocfile, conversions, convMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP CONVERSION public.conv_one")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP CONVERSION public.conv_two")

			resultConversions := backup.GetConversions(connectionPool)
			resultMetadataMap := backup.GetMetadataForObjectType(connectionPool, backup.TYPE_CONVERSION)

			convOne.Oid = testutils.OidFromObjectName(connectionPool, "public", "conv_one", backup.TYPE_CONVERSION)
			convTwo.Oid = testutils.OidFromObjectName(connectionPool, "public", "conv_two", backup.TYPE_CONVERSION)
			Expect(resultConversions).To(HaveLen(2))
			resultMetadata := resultMetadataMap[convOne.GetUniqueID()]
			structmatcher.ExpectStructsToMatch(&convOne, &resultConversions[0])
			structmatcher.ExpectStructsToMatch(&convTwo, &resultConversions[1])
			structmatcher.ExpectStructsToMatch(&convMetadata, &resultMetadata)
		})
	})
	Describe("PrintCreateForeignDataWrapperStatement", func() {
		emptyMetadata := backup.ObjectMetadata{}
		funcInfoMap := map[uint32]backup.FunctionInfo{
			1: {QualifiedName: "pg_catalog.postgresql_fdw_validator", Arguments: sql.NullString{String: "", Valid: true}, IsInternal: true},
		}
		It("creates foreign data wrappers with a validator and options", func() {
			testutils.SkipIfBefore6(connectionPool)
			foreignDataWrapperValidator := backup.ForeignDataWrapper{Name: "foreigndata1", Validator: 1}
			foreignDataWrapperOptions := backup.ForeignDataWrapper{Name: "foreigndata2", Options: "dbname 'testdb'"}

			backup.PrintCreateForeignDataWrapperStatement(backupfile, tocfile, foreignDataWrapperValidator, funcInfoMap, emptyMetadata)
			backup.PrintCreateForeignDataWrapperStatement(backupfile, tocfile, foreignDataWrapperOptions, funcInfoMap, emptyMetadata)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP FOREIGN DATA WRAPPER foreigndata1")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP FOREIGN DATA WRAPPER foreigndata2")

			resultWrappers := backup.GetForeignDataWrappers(connectionPool)

			Expect(resultWrappers).To(HaveLen(2))
			structmatcher.ExpectStructsToMatchExcluding(&foreignDataWrapperValidator, &resultWrappers[0], "Oid", "Validator")
			structmatcher.ExpectStructsToMatchExcluding(&foreignDataWrapperOptions, &resultWrappers[1], "Oid", "Validator")
		})
	})
	Describe("PrintCreateServerStatement", func() {
		emptyMetadata := backup.ObjectMetadata{}
		It("creates a foreign server with all options", func() {
			testutils.SkipIfBefore6(connectionPool)
			foreignServer := backup.ForeignServer{Name: "foreignserver", Type: "mytype", Version: "myversion", ForeignDataWrapper: "foreigndatawrapper", Options: "dbname 'testdb', host 'localhost'"}

			backup.PrintCreateServerStatement(backupfile, tocfile, foreignServer, emptyMetadata)

			testhelper.AssertQueryRuns(connectionPool, "CREATE FOREIGN DATA WRAPPER foreigndatawrapper")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP FOREIGN DATA WRAPPER foreigndatawrapper CASCADE")
			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			resultServers := backup.GetForeignServers(connectionPool)

			Expect(resultServers).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&foreignServer, &resultServers[0], "Oid")
		})
	})
	Describe("PrintCreateUserMappingStatement", func() {
		It("creates a user mapping for a specific user with all options", func() {
			testutils.SkipIfBefore6(connectionPool)
			testhelper.AssertQueryRuns(connectionPool, "CREATE FOREIGN DATA WRAPPER foreigndatawrapper")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP FOREIGN DATA WRAPPER foreigndatawrapper CASCADE")
			testhelper.AssertQueryRuns(connectionPool, "CREATE SERVER server FOREIGN DATA WRAPPER foreigndatawrapper")
			userMapping := backup.UserMapping{User: "testrole", Server: "server", Options: "dbname 'testdb', host 'localhost'"}

			backup.PrintCreateUserMappingStatement(backupfile, tocfile, userMapping)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			resultMappings := backup.GetUserMappings(connectionPool)

			Expect(resultMappings).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&userMapping, &resultMappings[0], "Oid")
		})
		It("creates a user mapping for public", func() {
			testutils.SkipIfBefore6(connectionPool)
			testhelper.AssertQueryRuns(connectionPool, "CREATE FOREIGN DATA WRAPPER foreigndatawrapper")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP FOREIGN DATA WRAPPER foreigndatawrapper CASCADE")
			testhelper.AssertQueryRuns(connectionPool, "CREATE SERVER server FOREIGN DATA WRAPPER foreigndatawrapper")
			userMapping := backup.UserMapping{User: "public", Server: "server"}

			backup.PrintCreateUserMappingStatement(backupfile, tocfile, userMapping)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			resultMappings := backup.GetUserMappings(connectionPool)

			Expect(resultMappings).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&userMapping, &resultMappings[0], "Oid")
		})
	})
})
