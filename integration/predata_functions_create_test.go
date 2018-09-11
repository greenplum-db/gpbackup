package integration

import (
	"github.com/greenplum-db/gp-common-go-libs/structmatcher"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup integration create statement tests", func() {
	BeforeEach(func() {
		toc, backupfile = testutils.InitializeTestTOC(buffer, "predata")
	})
	Describe("PrintCreateFunctionStatement", func() {
		Context("Tests for GPDB 4.3", func() {
			BeforeEach(func() {
				testutils.SkipIfNot4(connection)
			})
			funcMetadata := backup.ObjectMetadata{}
			It("creates a function with a simple return type", func() {
				addFunction := backup.Function{
					Schema: "public", Name: "add", ReturnsSet: false, FunctionBody: "SELECT $1 + $2",
					BinaryPath: "", Arguments: "integer, integer", IdentArgs: "integer, integer", ResultType: "integer",
					Volatility: "v", IsStrict: false, IsSecurityDefiner: false, Config: "", NumRows: 0, Language: "sql", ExecLocation: "a",
				}

				backup.PrintCreateFunctionStatement(backupfile, toc, addFunction, funcMetadata)

				testhelper.AssertQueryRuns(connection, buffer.String())
				defer testhelper.AssertQueryRuns(connection, "DROP FUNCTION public.add(integer, integer)")

				resultFunctions := backup.GetFunctionsAllVersions(connection)

				Expect(resultFunctions).To(HaveLen(1))
				structmatcher.ExpectStructsToMatchExcluding(&addFunction, &resultFunctions[0], "Oid")
			})
			It("creates a function that returns a set", func() {
				appendFunction := backup.Function{
					Schema: "public", Name: "append", ReturnsSet: true, FunctionBody: "SELECT ($1, $2)",
					BinaryPath: "", Arguments: "integer, integer", IdentArgs: "integer, integer", ResultType: "SETOF record",
					Volatility: "s", IsStrict: true, IsSecurityDefiner: true, Language: "sql", ExecLocation: "a",
				}

				backup.PrintCreateFunctionStatement(backupfile, toc, appendFunction, funcMetadata)

				testhelper.AssertQueryRuns(connection, buffer.String())
				defer testhelper.AssertQueryRuns(connection, "DROP FUNCTION public.append(integer, integer)")

				resultFunctions := backup.GetFunctionsAllVersions(connection)

				Expect(resultFunctions).To(HaveLen(1))
				structmatcher.ExpectStructsToMatchExcluding(&appendFunction, &resultFunctions[0], "Oid")
			})
			It("creates a function that returns a table", func() {
				dupFunction := backup.Function{
					Schema: "public", Name: "dup", ReturnsSet: true, FunctionBody: "SELECT $1, CAST($1 AS text) || ' is text'",
					BinaryPath: "", Arguments: "integer", IdentArgs: "integer", ResultType: "TABLE(f1 integer, f2 text)",
					Volatility: "v", IsStrict: false, IsSecurityDefiner: false, Language: "sql", ExecLocation: "a",
				}

				backup.PrintCreateFunctionStatement(backupfile, toc, dupFunction, funcMetadata)

				testhelper.AssertQueryRuns(connection, buffer.String())
				defer testhelper.AssertQueryRuns(connection, "DROP FUNCTION public.dup(integer)")

				resultFunctions := backup.GetFunctionsAllVersions(connection)

				Expect(resultFunctions).To(HaveLen(1))
				structmatcher.ExpectStructsToMatchExcluding(&dupFunction, &resultFunctions[0], "Oid")
			})
		})
		Context("Tests for GPDB 5 and GPDB 6", func() {
			BeforeEach(func() {
				testutils.SkipIfBefore5(connection)
			})
			funcMetadata := backup.ObjectMetadata{}
			It("creates a function with a simple return type", func() {
				addFunction := backup.Function{
					Schema: "public", Name: "add", ReturnsSet: false, FunctionBody: "SELECT $1 + $2",
					BinaryPath: "", Arguments: "integer, integer", IdentArgs: "integer, integer", ResultType: "integer",
					Volatility: "v", IsStrict: false, IsSecurityDefiner: false, Config: "", Cost: 100, NumRows: 0, DataAccess: "c",
					Language: "sql", ExecLocation: "a",
				}

				backup.PrintCreateFunctionStatement(backupfile, toc, addFunction, funcMetadata)

				testhelper.AssertQueryRuns(connection, buffer.String())
				defer testhelper.AssertQueryRuns(connection, "DROP FUNCTION public.add(integer, integer)")

				resultFunctions := backup.GetFunctionsAllVersions(connection)

				Expect(resultFunctions).To(HaveLen(1))

				structmatcher.ExpectStructsToMatchExcluding(&addFunction, &resultFunctions[0], "Oid")
			})
			It("creates a function that returns a set", func() {
				appendFunction := backup.Function{
					Schema: "public", Name: "append", ReturnsSet: true, FunctionBody: "SELECT ($1, $2)",
					BinaryPath: "", Arguments: "integer, integer", IdentArgs: "integer, integer", ResultType: "SETOF record",
					Volatility: "s", IsStrict: true, IsSecurityDefiner: true, Config: "SET search_path TO pg_temp", Cost: 200,
					NumRows: 200, DataAccess: "m", Language: "sql", ExecLocation: "a",
				}

				backup.PrintCreateFunctionStatement(backupfile, toc, appendFunction, funcMetadata)

				testhelper.AssertQueryRuns(connection, buffer.String())
				defer testhelper.AssertQueryRuns(connection, "DROP FUNCTION public.append(integer, integer)")

				resultFunctions := backup.GetFunctionsAllVersions(connection)

				Expect(resultFunctions).To(HaveLen(1))

				structmatcher.ExpectStructsToMatchExcluding(&appendFunction, &resultFunctions[0], "Oid")
			})
			It("creates a function that returns a table", func() {
				dupFunction := backup.Function{
					Schema: "public", Name: "dup", ReturnsSet: true, FunctionBody: "SELECT $1, CAST($1 AS text) || ' is text'",
					BinaryPath: "", Arguments: "integer", IdentArgs: "integer", ResultType: "TABLE(f1 integer, f2 text)",
					Volatility: "v", IsStrict: false, IsSecurityDefiner: false, Config: "", Cost: 100, NumRows: 1000, DataAccess: "c",
					Language: "sql", ExecLocation: "a",
				}

				backup.PrintCreateFunctionStatement(backupfile, toc, dupFunction, funcMetadata)

				testhelper.AssertQueryRuns(connection, buffer.String())
				defer testhelper.AssertQueryRuns(connection, "DROP FUNCTION public.dup(integer)")

				resultFunctions := backup.GetFunctionsAllVersions(connection)

				Expect(resultFunctions).To(HaveLen(1))
				structmatcher.ExpectStructsToMatchExcluding(&dupFunction, &resultFunctions[0], "Oid")
			})
		})
		Context("Tests for GPDB 6", func() {
			BeforeEach(func() {
				testutils.SkipIfBefore6(connection)
			})
			funcMetadata := backup.ObjectMetadata{}
			It("creates a window function to execute on master", func() {
				windowFunction := backup.Function{
					Schema: "public", Name: "add", ReturnsSet: false, FunctionBody: "SELECT $1 + $2",
					BinaryPath: "", Arguments: "integer, integer", IdentArgs: "integer, integer", ResultType: "integer",
					Volatility: "v", IsStrict: false, IsSecurityDefiner: false, Config: "", Cost: 100, NumRows: 0, DataAccess: "c",
					Language: "sql", IsWindow: true, ExecLocation: "m",
				}

				backup.PrintCreateFunctionStatement(backupfile, toc, windowFunction, funcMetadata)

				testhelper.AssertQueryRuns(connection, buffer.String())
				defer testhelper.AssertQueryRuns(connection, "DROP FUNCTION public.add(integer, integer)")

				resultFunctions := backup.GetFunctionsAllVersions(connection)

				Expect(resultFunctions).To(HaveLen(1))
				structmatcher.ExpectStructsToMatchExcluding(&windowFunction, &resultFunctions[0], "Oid")
			})
			It("creates a function to execute on segments", func() {
				segmentFunction := backup.Function{
					Schema: "public", Name: "add", ReturnsSet: false, FunctionBody: "SELECT $1 + $2",
					BinaryPath: "", Arguments: "integer, integer", IdentArgs: "integer, integer", ResultType: "integer",
					Volatility: "v", IsStrict: false, IsSecurityDefiner: false, Config: "", Cost: 100, NumRows: 0, DataAccess: "c",
					Language: "sql", IsWindow: false, ExecLocation: "s",
				}

				backup.PrintCreateFunctionStatement(backupfile, toc, segmentFunction, funcMetadata)

				testhelper.AssertQueryRuns(connection, buffer.String())
				defer testhelper.AssertQueryRuns(connection, "DROP FUNCTION public.add(integer, integer)")

				resultFunctions := backup.GetFunctionsAllVersions(connection)

				Expect(resultFunctions).To(HaveLen(1))
				structmatcher.ExpectStructsToMatchExcluding(&segmentFunction, &resultFunctions[0], "Oid")
			})
			It("creates a function with LEAKPROOF", func() {
				leakProofFunction := backup.Function{
					Schema: "public", Name: "add", ReturnsSet: false, FunctionBody: "SELECT $1 + $2",
					BinaryPath: "", Arguments: "integer, integer", IdentArgs: "integer, integer", ResultType: "integer",
					Volatility: "v", IsStrict: false, IsLeakProof: true, IsSecurityDefiner: false, Config: "", Cost: 100, NumRows: 0, DataAccess: "c",
					Language: "sql", IsWindow: false, ExecLocation: "a",
				}

				backup.PrintCreateFunctionStatement(backupfile, toc, leakProofFunction, funcMetadata)

				testhelper.AssertQueryRuns(connection, buffer.String())
				defer testhelper.AssertQueryRuns(connection, "DROP FUNCTION public.add(integer, integer)")

				resultFunctions := backup.GetFunctionsAllVersions(connection)

				Expect(resultFunctions).To(HaveLen(1))
				structmatcher.ExpectStructsToMatchExcluding(&leakProofFunction, &resultFunctions[0], "Oid")
			})
		})
	})
	Describe("PrintCreateAggregateStatements", func() {
		emptyMetadataMap := backup.MetadataMap{}
		basicAggregateDef := backup.Aggregate{
			Oid: 1, Schema: "public", Name: "agg_prefunc", Arguments: "numeric, numeric",
			IdentArgs: "numeric, numeric", TransitionFunction: 1, PreliminaryFunction: 2,
			TransitionDataType: "numeric", InitialValue: "0", MInitValIsNull: true,
		}

		funcInfoMap := map[uint32]backup.FunctionInfo{
			1: {QualifiedName: "public.mysfunc_accum", Arguments: "numeric, numeric, numeric"},
			2: {QualifiedName: "public.mypre_accum", Arguments: "numeric, numeric"},
			3: {QualifiedName: "pg_catalog.ordered_set_transition_multi", Arguments: `internal, VARIADIC "any"`},
			4: {QualifiedName: "pg_catalog.rank_final", Arguments: `internal, VARIADIC "any"`},
			5: {QualifiedName: "pg_catalog.numeric_avg", Arguments: `internal`},
			6: {QualifiedName: "pg_catalog.numeric_avg_serialize", Arguments: `internal`},
			7: {QualifiedName: "pg_catalog.numeric_avg_deserialize", Arguments: `bytea, internal`},
			8: {QualifiedName: "pg_catalog.numeric_avg_accum", Arguments: `numeric, numeric`},
		}
		BeforeEach(func() {
			//Run queries to set up the database state so we can successfully create an aggregate
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
		It("creates a basic aggregate", func() {
			backup.PrintCreateAggregateStatements(backupfile, toc, []backup.Aggregate{basicAggregateDef}, funcInfoMap, emptyMetadataMap)

			testhelper.AssertQueryRuns(connection, buffer.String())
			defer testhelper.AssertQueryRuns(connection, "DROP AGGREGATE public.agg_prefunc(numeric, numeric)")

			resultAggregates := backup.GetAggregates(connection)
			Expect(resultAggregates).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&basicAggregateDef, &resultAggregates[0], "Oid", "TransitionFunction", "PreliminaryFunction", "CombineFunction")
		})
		It("creates an aggregate with an owner and a comment", func() {
			aggMetadata := backup.ObjectMetadata{Privileges: []backup.ACL{}, Owner: "testrole", Comment: "This is an aggregate comment."}
			aggMetadataMap := backup.MetadataMap{1: aggMetadata}
			backup.PrintCreateAggregateStatements(backupfile, toc, []backup.Aggregate{basicAggregateDef}, funcInfoMap, aggMetadataMap)

			testhelper.AssertQueryRuns(connection, buffer.String())
			defer testhelper.AssertQueryRuns(connection, "DROP AGGREGATE public.agg_prefunc(numeric, numeric)")

			oid := testutils.OidFromObjectName(connection, "", "agg_prefunc", backup.TYPE_AGGREGATE)
			resultAggregates := backup.GetAggregates(connection)
			Expect(resultAggregates).To(HaveLen(1))
			resultMetadataMap := backup.GetMetadataForObjectType(connection, backup.TYPE_AGGREGATE)
			resultMetadata := resultMetadataMap[oid]
			structmatcher.ExpectStructsToMatchExcluding(&basicAggregateDef, &resultAggregates[0], "Oid", "TransitionFunction", "PreliminaryFunction", "CombineFunction")
			structmatcher.ExpectStructsToMatch(&aggMetadata, &resultMetadata)
		})
		It("creates a hypothetical ordered-set aggregate", func() {
			testutils.SkipIfBefore6(connection)
			complexAggregateDef := backup.Aggregate{
				Schema: "public", Name: "agg_hypo_ord", Arguments: `VARIADIC "any" ORDER BY VARIADIC "any"`,
				IdentArgs: `VARIADIC "any" ORDER BY VARIADIC "any"`, TransitionFunction: 3, FinalFunction: 4,
				TransitionDataType: "internal", InitValIsNull: true, FinalFuncExtra: true, Hypothetical: true, MInitValIsNull: true,
			}

			backup.PrintCreateAggregateStatements(backupfile, toc, []backup.Aggregate{complexAggregateDef}, funcInfoMap, emptyMetadataMap)

			testhelper.AssertQueryRuns(connection, buffer.String())
			defer testhelper.AssertQueryRuns(connection, `DROP AGGREGATE public.agg_hypo_ord(VARIADIC "any" ORDER BY VARIADIC "any")`)
			resultAggregates := backup.GetAggregates(connection)

			Expect(resultAggregates).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&complexAggregateDef, &resultAggregates[0], "Oid", "TransitionFunction", "FinalFunction")
		})
		It("creates an aggregate with combine function and transition data size", func() {
			testutils.SkipIfBefore6(connection)
			aggregateDef := backup.Aggregate{
				Schema: "public", Name: "agg_6_features", Arguments: "numeric, numeric",
				IdentArgs: "numeric, numeric", TransitionFunction: 1, CombineFunction: 2,
				FinalFunction: 0, SortOperator: 0, TransitionDataType: "numeric", TransitionDataSize: 1000,
				InitialValue: "0", IsOrdered: false, MInitValIsNull: true,
			}
			backup.PrintCreateAggregateStatements(backupfile, toc, []backup.Aggregate{aggregateDef}, funcInfoMap, emptyMetadataMap)

			testhelper.AssertQueryRuns(connection, buffer.String())
			defer testhelper.AssertQueryRuns(connection, `DROP AGGREGATE public.agg_6_features(numeric, numeric)`)
			resultAggregates := backup.GetAggregates(connection)

			Expect(resultAggregates).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&aggregateDef, &resultAggregates[0], "Oid", "TransitionFunction", "FinalFunction", "CombineFunction")
		})
		It("creates an aggregate with serial/deserial functions", func() {
			testutils.SkipIfBefore6(connection)
			aggregateDef := backup.Aggregate{
				Schema: "public", Name: "myavg", Arguments: "numeric",
				IdentArgs: "numeric", TransitionFunction: 8,
				FinalFunction: 5, SerialFunction: 6, DeserialFunction: 7, TransitionDataType: "internal",
				IsOrdered: false, InitValIsNull: true, MInitValIsNull: true,
			}

			backup.PrintCreateAggregateStatements(backupfile, toc, []backup.Aggregate{aggregateDef}, funcInfoMap, emptyMetadataMap)

			testhelper.AssertQueryRuns(connection, buffer.String())
			defer testhelper.AssertQueryRuns(connection, `DROP AGGREGATE public.myavg(numeric)`)
			resultAggregates := backup.GetAggregates(connection)

			Expect(resultAggregates).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&aggregateDef, &resultAggregates[0], "Oid", "TransitionFunction", "FinalFunction", "SerialFunction", "DeserialFunction")
		})
		It("creates an aggregate with moving attributes", func() {
			testutils.SkipIfBefore6(connection)
			aggregateDef := backup.Aggregate{
				Schema: "public", Name: "moving_agg", Arguments: "numeric, numeric",
				IdentArgs: "numeric, numeric", TransitionFunction: 1, TransitionDataType: "numeric",
				InitValIsNull: true, MTransitionFunction: 1, MInverseTransitionFunction: 1,
				MTransitionDataType: "numeric", MTransitionDataSize: 100, MFinalFunction: 1,
				MFinalFuncExtra: true, MInitialValue: "0", MInitValIsNull: false,
			}

			backup.PrintCreateAggregateStatements(backupfile, toc, []backup.Aggregate{aggregateDef}, funcInfoMap, emptyMetadataMap)

			testhelper.AssertQueryRuns(connection, buffer.String())
			defer testhelper.AssertQueryRuns(connection, `DROP AGGREGATE public.moving_agg(numeric, numeric)`)
			resultAggregates := backup.GetAggregates(connection)

			Expect(resultAggregates).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&aggregateDef, &resultAggregates[0], "Oid", "TransitionFunction", "MTransitionFunction", "MInverseTransitionFunction", "MFinalFunction")
		})
	})
	Describe("PrintCreateCastStatements", func() {
		var (
			castMetadataMap backup.MetadataMap
		)
		BeforeEach(func() {
			castMetadataMap = backup.MetadataMap{}
		})
		It("prints a basic cast with a function", func() {
			castDef := backup.Cast{Oid: 0, SourceTypeFQN: "pg_catalog.money", TargetTypeFQN: "pg_catalog.text", FunctionSchema: "public", FunctionName: "money_to_text", FunctionArgs: "money", CastContext: "a", CastMethod: "f"}

			testhelper.AssertQueryRuns(connection, "CREATE FUNCTION public.money_to_text(money) RETURNS TEXT AS $$ SELECT textin(cash_out($1)) $$ LANGUAGE SQL;")
			defer testhelper.AssertQueryRuns(connection, "DROP FUNCTION public.money_to_text(money)")

			backup.PrintCreateCastStatements(backupfile, toc, []backup.Cast{castDef}, castMetadataMap)
			defer testhelper.AssertQueryRuns(connection, "DROP CAST (money AS text)")

			testhelper.AssertQueryRuns(connection, buffer.String())

			resultCasts := backup.GetCasts(connection)
			Expect(resultCasts).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&castDef, &resultCasts[0], "Oid", "FunctionOid")
		})
		It("prints a basic cast without a function", func() {
			castDef := backup.Cast{Oid: 0, SourceTypeFQN: "pg_catalog.text", TargetTypeFQN: "public.casttesttype", FunctionSchema: "", FunctionName: "", FunctionArgs: "", CastContext: "i", CastMethod: "b"}

			testhelper.AssertQueryRuns(connection, "CREATE FUNCTION public.cast_in(cstring) RETURNS public.casttesttype AS $$textin$$ LANGUAGE internal STRICT NO SQL")
			testhelper.AssertQueryRuns(connection, "CREATE FUNCTION public.cast_out(public.casttesttype) RETURNS cstring AS $$textout$$ LANGUAGE internal STRICT NO SQL")
			testhelper.AssertQueryRuns(connection, "CREATE TYPE public.casttesttype (INTERNALLENGTH = variable, INPUT = public.cast_in, OUTPUT = public.cast_out)")
			defer testhelper.AssertQueryRuns(connection, "DROP TYPE public.casttesttype CASCADE")

			backup.PrintCreateCastStatements(backupfile, toc, []backup.Cast{castDef}, castMetadataMap)
			defer testhelper.AssertQueryRuns(connection, "DROP CAST (text AS public.casttesttype)")

			testhelper.AssertQueryRuns(connection, buffer.String())

			resultCasts := backup.GetCasts(connection)
			Expect(resultCasts).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&castDef, &resultCasts[0], "Oid")
		})
		It("prints a cast with a comment", func() {
			castDef := backup.Cast{Oid: 1, SourceTypeFQN: "pg_catalog.money", TargetTypeFQN: "pg_catalog.text", FunctionSchema: "public", FunctionName: "money_to_text", FunctionArgs: "money", CastContext: "a", CastMethod: "f"}
			castMetadataMap = testutils.DefaultMetadataMap("CAST", false, false, true)
			castMetadata := castMetadataMap[1]

			testhelper.AssertQueryRuns(connection, "CREATE FUNCTION public.money_to_text(money) RETURNS TEXT AS $$ SELECT textin(cash_out($1)) $$ LANGUAGE SQL;")
			defer testhelper.AssertQueryRuns(connection, "DROP FUNCTION public.money_to_text(money)")

			backup.PrintCreateCastStatements(backupfile, toc, []backup.Cast{castDef}, castMetadataMap)
			defer testhelper.AssertQueryRuns(connection, "DROP CAST (money AS text)")

			testhelper.AssertQueryRuns(connection, buffer.String())

			resultCasts := backup.GetCasts(connection)
			Expect(resultCasts).To(HaveLen(1))
			resultMetadataMap := backup.GetCommentsForObjectType(connection, backup.TYPE_CAST)
			resultMetadata := resultMetadataMap[resultCasts[0].Oid]
			structmatcher.ExpectStructsToMatchExcluding(&castDef, &resultCasts[0], "Oid", "FunctionOid")
			structmatcher.ExpectStructsToMatchExcluding(&resultMetadata, &castMetadata, "Oid")
		})
		It("prints an inout cast ", func() {
			testutils.SkipIfBefore6(connection)
			castDef := backup.Cast{Oid: 0, SourceTypeFQN: `pg_catalog."varchar"`, TargetTypeFQN: "public.custom_numeric", FunctionSchema: "", FunctionName: "", FunctionArgs: "", CastContext: "a", CastMethod: "i"}
			testhelper.AssertQueryRuns(connection, "CREATE TYPE public.custom_numeric AS (i numeric)")
			defer testhelper.AssertQueryRuns(connection, "DROP TYPE public.custom_numeric")

			backup.PrintCreateCastStatements(backupfile, toc, []backup.Cast{castDef}, castMetadataMap)
			defer testhelper.AssertQueryRuns(connection, "DROP CAST (varchar AS public.custom_numeric)")

			testhelper.AssertQueryRuns(connection, buffer.String())

			resultCasts := backup.GetCasts(connection)
			Expect(resultCasts).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&castDef, &resultCasts[0], "Oid")
		})
	})
	Describe("PrintCreateLanguageStatements", func() {
		It("creates procedural languages", func() {
			funcInfoMap := map[uint32]backup.FunctionInfo{
				1: {QualifiedName: "pg_catalog.plpython_call_handler", Arguments: "", IsInternal: true},
				2: {QualifiedName: "pg_catalog.plpython_inline_handler", Arguments: "internal", IsInternal: true},
			}
			langOwner := ""
			if connection.Version.Before("5") {
				langOwner = testutils.GetUserByID(connection, 10)
			} else {
				langOwner = "testrole"
			}
			plpythonInfo := backup.ProceduralLanguage{Oid: 1, Name: "plpythonu", Owner: langOwner, IsPl: true, PlTrusted: false, Handler: 1, Inline: 2}
			langMetadata := backup.ObjectMetadata{Privileges: []backup.ACL{}, Owner: langOwner, Comment: "This is a language comment"}
			langMetadataMap := map[uint32]backup.ObjectMetadata{1: langMetadata}
			if connection.Version.Before("5") {
				plpythonInfo.Inline = 0
			}
			procLangs := []backup.ProceduralLanguage{plpythonInfo}

			backup.PrintCreateLanguageStatements(backupfile, toc, procLangs, funcInfoMap, langMetadataMap)

			testhelper.AssertQueryRuns(connection, buffer.String())
			defer testhelper.AssertQueryRuns(connection, "DROP LANGUAGE plpythonu")

			resultProcLangs := backup.GetProceduralLanguages(connection)
			resultMetadataMap := backup.GetMetadataForObjectType(connection, backup.TYPE_PROCLANGUAGE)

			plpythonInfo.Oid = testutils.OidFromObjectName(connection, "", "plpythonu", backup.TYPE_PROCLANGUAGE)
			Expect(resultProcLangs).To(HaveLen(1))
			resultMetadata := resultMetadataMap[plpythonInfo.Oid]
			structmatcher.ExpectStructsToMatchIncluding(&plpythonInfo, &resultProcLangs[0], "Name", "IsPl", "PlTrusted")
			structmatcher.ExpectStructsToMatch(&langMetadata, &resultMetadata)
		})
	})
	Describe("PrintCreateExtensions", func() {
		It("creates extensions", func() {
			testutils.SkipIfBefore5(connection)
			plperlExtension := backup.Extension{Oid: 1, Name: "plperl", Schema: "pg_catalog"}
			extensions := []backup.Extension{plperlExtension}
			extensionMetadataMap := testutils.DefaultMetadataMap("EXTENSION", false, false, true)
			extensionMetadata := extensionMetadataMap[1]
			backup.PrintCreateExtensionStatements(backupfile, toc, extensions, extensionMetadataMap)
			testhelper.AssertQueryRuns(connection, buffer.String())
			defer testhelper.AssertQueryRuns(connection, "DROP EXTENSION plperl; SET search_path=pg_catalog")
			resultExtensions := backup.GetExtensions(connection)
			resultMetadataMap := backup.GetCommentsForObjectType(connection, backup.TYPE_EXTENSION)
			plperlExtension.Oid = testutils.OidFromObjectName(connection, "", "plperl", backup.TYPE_EXTENSION)
			Expect(resultExtensions).To(HaveLen(1))
			plperlMetadata := resultMetadataMap[plperlExtension.Oid]
			structmatcher.ExpectStructsToMatch(&plperlExtension, &resultExtensions[0])
			structmatcher.ExpectStructsToMatch(&extensionMetadata, &plperlMetadata)
		})
	})
	Describe("PrintCreateConversionStatements", func() {
		It("creates conversions", func() {
			convOne := backup.Conversion{Oid: 1, Schema: "public", Name: "conv_one", ForEncoding: "LATIN1", ToEncoding: "MULE_INTERNAL", ConversionFunction: "pg_catalog.latin1_to_mic", IsDefault: false}
			convTwo := backup.Conversion{Oid: 0, Schema: "public", Name: "conv_two", ForEncoding: "LATIN1", ToEncoding: "MULE_INTERNAL", ConversionFunction: "pg_catalog.latin1_to_mic", IsDefault: true}
			conversions := []backup.Conversion{convOne, convTwo}
			convMetadataMap := testutils.DefaultMetadataMap("CONVERSION", false, true, true)
			convMetadata := convMetadataMap[1]

			backup.PrintCreateConversionStatements(backupfile, toc, conversions, convMetadataMap)

			testhelper.AssertQueryRuns(connection, buffer.String())
			defer testhelper.AssertQueryRuns(connection, "DROP CONVERSION public.conv_one")
			defer testhelper.AssertQueryRuns(connection, "DROP CONVERSION public.conv_two")

			resultConversions := backup.GetConversions(connection)
			resultMetadataMap := backup.GetMetadataForObjectType(connection, backup.TYPE_CONVERSION)

			convOne.Oid = testutils.OidFromObjectName(connection, "public", "conv_one", backup.TYPE_CONVERSION)
			convTwo.Oid = testutils.OidFromObjectName(connection, "public", "conv_two", backup.TYPE_CONVERSION)
			Expect(resultConversions).To(HaveLen(2))
			resultMetadata := resultMetadataMap[convOne.Oid]
			structmatcher.ExpectStructsToMatch(&convOne, &resultConversions[0])
			structmatcher.ExpectStructsToMatch(&convTwo, &resultConversions[1])
			structmatcher.ExpectStructsToMatch(&convMetadata, &resultMetadata)
		})
	})
	Describe("PrintCreateForeignDataWrapperStatements", func() {
		emptyMetadataMap := backup.MetadataMap{}
		funcInfoMap := map[uint32]backup.FunctionInfo{
			1: {QualifiedName: "pg_catalog.postgresql_fdw_validator", Arguments: "", IsInternal: true},
		}
		It("creates foreign data wrappers with a validator and options", func() {
			testutils.SkipIfBefore6(connection)
			foreignDataWrapperValidator := backup.ForeignDataWrapper{Name: "foreigndata1", Validator: 1}
			foreignDataWrapperOptions := backup.ForeignDataWrapper{Name: "foreigndata2", Options: "dbname 'testdb'"}

			backup.PrintCreateForeignDataWrapperStatements(backupfile, toc, []backup.ForeignDataWrapper{foreignDataWrapperValidator, foreignDataWrapperOptions}, funcInfoMap, emptyMetadataMap)

			testhelper.AssertQueryRuns(connection, buffer.String())
			defer testhelper.AssertQueryRuns(connection, "DROP FOREIGN DATA WRAPPER foreigndata1")
			defer testhelper.AssertQueryRuns(connection, "DROP FOREIGN DATA WRAPPER foreigndata2")

			resultWrappers := backup.GetForeignDataWrappers(connection)

			Expect(resultWrappers).To(HaveLen(2))
			structmatcher.ExpectStructsToMatchExcluding(&foreignDataWrapperValidator, &resultWrappers[0], "Oid", "Validator")
			structmatcher.ExpectStructsToMatchExcluding(&foreignDataWrapperOptions, &resultWrappers[1], "Oid", "Validator")
		})
	})
	Describe("PrintCreateServerStatements", func() {
		emptyMetadataMap := backup.MetadataMap{}
		It("creates a foreign server with all options", func() {
			testutils.SkipIfBefore6(connection)
			foreignServer := backup.ForeignServer{Name: "foreignserver", Type: "mytype", Version: "myversion", ForeignDataWrapper: "foreigndatawrapper", Options: "dbname 'testdb', host 'localhost'"}

			backup.PrintCreateServerStatements(backupfile, toc, []backup.ForeignServer{foreignServer}, emptyMetadataMap)

			testhelper.AssertQueryRuns(connection, "CREATE FOREIGN DATA WRAPPER foreigndatawrapper")
			defer testhelper.AssertQueryRuns(connection, "DROP FOREIGN DATA WRAPPER foreigndatawrapper CASCADE")
			testhelper.AssertQueryRuns(connection, buffer.String())

			resultServers := backup.GetForeignServers(connection)

			Expect(resultServers).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&foreignServer, &resultServers[0], "Oid")
		})
	})
	Describe("PrintCreateUserMappingStatements", func() {
		It("creates a user mapping for a specific user with all options", func() {
			testutils.SkipIfBefore6(connection)
			testhelper.AssertQueryRuns(connection, "CREATE FOREIGN DATA WRAPPER foreigndatawrapper")
			defer testhelper.AssertQueryRuns(connection, "DROP FOREIGN DATA WRAPPER foreigndatawrapper CASCADE")
			testhelper.AssertQueryRuns(connection, "CREATE SERVER server FOREIGN DATA WRAPPER foreigndatawrapper")
			userMapping := backup.UserMapping{User: "testrole", Server: "server", Options: "dbname 'testdb', host 'localhost'"}

			backup.PrintCreateUserMappingStatements(backupfile, toc, []backup.UserMapping{userMapping})

			testhelper.AssertQueryRuns(connection, buffer.String())

			resultMappings := backup.GetUserMappings(connection)

			Expect(resultMappings).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&userMapping, &resultMappings[0], "Oid")
		})
		It("creates a user mapping for public", func() {
			testutils.SkipIfBefore6(connection)
			testhelper.AssertQueryRuns(connection, "CREATE FOREIGN DATA WRAPPER foreigndatawrapper")
			defer testhelper.AssertQueryRuns(connection, "DROP FOREIGN DATA WRAPPER foreigndatawrapper CASCADE")
			testhelper.AssertQueryRuns(connection, "CREATE SERVER server FOREIGN DATA WRAPPER foreigndatawrapper")
			userMapping := backup.UserMapping{User: "public", Server: "server"}

			backup.PrintCreateUserMappingStatements(backupfile, toc, []backup.UserMapping{userMapping})

			testhelper.AssertQueryRuns(connection, buffer.String())

			resultMappings := backup.GetUserMappings(connection)

			Expect(resultMappings).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&userMapping, &resultMappings[0], "Oid")
		})
	})
})
