package backup_test

import (
	"database/sql"
	"fmt"

	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup/predata_functions tests", func() {
	BeforeEach(func() {
		tocfile, backupfile = testutils.InitializeTestTOC(buffer, "predata")
	})
	Describe("Functions involved in printing CREATE FUNCTION statements", func() {
		var funcDef backup.Function
		var DEFAULT_PARALLEL string
		BeforeEach(func() {
			funcDef = backup.Function{Oid: 1, Schema: "public", Name: "func_name", ReturnsSet: false, FunctionBody: "add_two_ints", BinaryPath: "", Arguments: sql.NullString{String: "integer, integer", Valid: true}, IdentArgs: sql.NullString{String: "integer, integer", Valid: true}, ResultType: sql.NullString{String: "integer", Valid: true}, Volatility: "v", IsStrict: false, IsSecurityDefiner: false, Config: "", Cost: float32(1), NumRows: float32(0), DataAccess: "", Language: "internal", ExecLocation: "a"}
			funcDef.Parallel = ""
			funcDef.PlannerSupport = ""
			DEFAULT_PARALLEL = ""
			if connectionPool.Version.AtLeast("7") {
				funcDef.Parallel = "u"
				funcDef.PlannerSupport = "-"
				DEFAULT_PARALLEL = " PARALLEL UNSAFE"
			}
		})

		Describe("PrintCreateFunctionStatement", func() {
			var (
				funcMetadata backup.ObjectMetadata
			)
			BeforeEach(func() {
				funcMetadata = backup.ObjectMetadata{}
			})
			It("prints a function definition for an internal function without a binary path", func() {
				backup.PrintCreateFunctionStatement(backupfile, tocfile, funcDef, funcMetadata)
				testutils.ExpectEntry(tocfile.PredataEntries, 0, "public", "", "func_name(integer, integer)", "FUNCTION")
				testutils.AssertBufferContents(tocfile.PredataEntries, buffer, fmt.Sprintf(`CREATE FUNCTION public.func_name(integer, integer) RETURNS integer AS
$$add_two_ints$$
LANGUAGE internal%s;`, DEFAULT_PARALLEL))
			})
			It("prints a function definition for a function that returns a set", func() {
				funcDef.ReturnsSet = true
				funcDef.ResultType = sql.NullString{String: "SETOF integer", Valid: true}
				backup.PrintCreateFunctionStatement(backupfile, tocfile, funcDef, funcMetadata)
				testutils.AssertBufferContents(tocfile.PredataEntries, buffer, fmt.Sprintf(`CREATE FUNCTION public.func_name(integer, integer) RETURNS SETOF integer AS
$$add_two_ints$$
LANGUAGE internal%s;`, DEFAULT_PARALLEL))
			})
			It("prints a function definition for a function with permissions, an owner, security label, and a comment", func() {
				funcMetadata := testutils.DefaultMetadata("FUNCTION", true, true, true, true)
				backup.PrintCreateFunctionStatement(backupfile, tocfile, funcDef, funcMetadata)
				expectedStatements := []string{fmt.Sprintf(`CREATE FUNCTION public.func_name(integer, integer) RETURNS integer AS
$$add_two_ints$$
LANGUAGE internal%s;`, DEFAULT_PARALLEL),
					"COMMENT ON FUNCTION public.func_name(integer, integer) IS 'This is a function comment.';",
					"ALTER FUNCTION public.func_name(integer, integer) OWNER TO testrole;",
					`REVOKE ALL ON FUNCTION public.func_name(integer, integer) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.func_name(integer, integer) FROM testrole;
GRANT ALL ON FUNCTION public.func_name(integer, integer) TO testrole;`,
					"SECURITY LABEL FOR dummy ON FUNCTION public.func_name(integer, integer) IS 'unclassified';"}
				testutils.AssertBufferContents(tocfile.PredataEntries, buffer, expectedStatements...)

			})
			It("prints a function definition for a stored procedure", func() {
				testutils.SkipIfBefore7(connectionPool)
				procDef := backup.Function{Oid: 1, Schema: "public", Name: "my_procedure", Kind: "p", ReturnsSet: false, FunctionBody: "do_something", BinaryPath: "", Arguments: sql.NullString{String: "", Valid: true}, IdentArgs: sql.NullString{String: "", Valid: true}, ResultType: sql.NullString{String: "", Valid: false}, Volatility: "", IsStrict: false, IsSecurityDefiner: false, Config: "", NumRows: float32(0), DataAccess: "", Language: "SQL", ExecLocation: "a"}
				procDef.PlannerSupport = "-"
				backup.PrintCreateFunctionStatement(backupfile, tocfile, procDef, funcMetadata)
				testutils.ExpectEntry(tocfile.PredataEntries, 0, "public", "", "my_procedure()", "FUNCTION")
				testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE PROCEDURE public.my_procedure() AS
$$do_something$$
LANGUAGE SQL;`)
			})
		})
		Describe("PrintFunctionBodyOrPath", func() {
			It("prints a function definition for an internal function with 'NULL' binary path using '-'", func() {
				funcDef.BinaryPath = "-"
				backup.PrintFunctionBodyOrPath(backupfile, funcDef)
				testhelper.ExpectRegexp(buffer, `
$$add_two_ints$$
`)
			})
			It("prints a function definition for an internal function with a binary path", func() {
				funcDef.BinaryPath = "$libdir/binary"
				backup.PrintFunctionBodyOrPath(backupfile, funcDef)
				testhelper.ExpectRegexp(buffer, `
'$libdir/binary', 'add_two_ints'
`)
			})
			It("prints a function definition for a function with a one-line function definition", func() {
				funcDef.FunctionBody = "SELECT $1+$2"
				funcDef.Language = "sql"
				backup.PrintFunctionBodyOrPath(backupfile, funcDef)
				testhelper.ExpectRegexp(buffer, `$_$SELECT $1+$2$_$`)
			})
			It("prints a function definition for a function with a multi-line function definition", func() {
				funcDef.FunctionBody = `
BEGIN
	SELECT $1 + $2
END
`
				funcDef.Language = "sql"
				backup.PrintFunctionBodyOrPath(backupfile, funcDef)
				testhelper.ExpectRegexp(buffer, `$_$
BEGIN
	SELECT $1 + $2
END
$_$`)
			})
		})
		Describe("PrintFunctionModifiers", func() {
			Context("SqlUsage cases", func() {
				BeforeEach(func() {
					if connectionPool.Version.AtLeast("7") {
						Skip("Test not applicable to GP7 and above")
					}
				})
				It("prints 'c' as CONTAINS SQL", func() {
					funcDef.DataAccess = "c"
					backup.PrintFunctionModifiers(backupfile, funcDef)
					testhelper.ExpectRegexp(buffer, "CONTAINS SQL")
				})
				It("prints 'm' as MODIFIES SQL DATA", func() {
					funcDef.DataAccess = "m"
					backup.PrintFunctionModifiers(backupfile, funcDef)
					testhelper.ExpectRegexp(buffer, "MODIFIES SQL DATA")
				})
				It("prints 'n' as NO SQL", func() {
					funcDef.DataAccess = "n"
					backup.PrintFunctionModifiers(backupfile, funcDef)
					testhelper.ExpectRegexp(buffer, "NO SQL")
				})
				It("prints 'r' as READS SQL DATA", func() {
					funcDef.DataAccess = "r"
					backup.PrintFunctionModifiers(backupfile, funcDef)
					testhelper.ExpectRegexp(buffer, "READS SQL DATA")
				})
			})
			Context("Volatility cases", func() {
				It("does not print anything for 'v'", func() {
					funcDef.Volatility = "v"
					backup.PrintFunctionModifiers(backupfile, funcDef)
					testhelper.NotExpectRegexp(buffer, "STABLE")
					testhelper.NotExpectRegexp(buffer, "IMMUTABLE")
				})
				It("prints 's' as STABLE", func() {
					funcDef.Volatility = "s"
					backup.PrintFunctionModifiers(backupfile, funcDef)
					testhelper.ExpectRegexp(buffer, "STABLE")
				})
				It("prints 'i' as IMMUTABLE", func() {
					funcDef.Volatility = "i"
					backup.PrintFunctionModifiers(backupfile, funcDef)
					testhelper.ExpectRegexp(buffer, "IMMUTABLE")
				})
			})
			It("prints 'LEAKPROOF' if IsLeakProof is set", func() {
				funcDef.IsLeakProof = true
				backup.PrintFunctionModifiers(backupfile, funcDef)
				testhelper.ExpectRegexp(buffer, "LEAKPROOF")
			})
			It("prints 'STRICT' if IsStrict is set", func() {
				funcDef.IsStrict = true
				backup.PrintFunctionModifiers(backupfile, funcDef)
				testhelper.ExpectRegexp(buffer, "STRICT")
			})
			It("prints 'SECURITY DEFINER' if IsSecurityDefiner is set", func() {
				funcDef.IsSecurityDefiner = true
				backup.PrintFunctionModifiers(backupfile, funcDef)
				testhelper.ExpectRegexp(buffer, "SECURITY DEFINER")
			})
			It("print 'WINDOW' if IsWindow is set", func() {
				funcDef.IsWindow = true
				backup.PrintFunctionModifiers(backupfile, funcDef)
				testhelper.ExpectRegexp(buffer, "WINDOW")
			})
			It("print 'WINDOW' if Kind is 'w'", func() {
				funcDef.Kind = "w"
				backup.PrintFunctionModifiers(backupfile, funcDef)
				testhelper.ExpectRegexp(buffer, "WINDOW")
			})
			It("print 'TRANSFORM' block if transforms are present", func() {
				testutils.SkipIfBefore7(connectionPool)
				funcDef.TransformTypes = "FOR TYPE public.hstore, FOR TYPE pg_catalog.jsonb"
				backup.PrintFunctionModifiers(backupfile, funcDef)
				Expect(string(buffer.Contents())).To(ContainSubstring("TRANSFORM FOR TYPE public.hstore, FOR TYPE pg_catalog.jsonb"))
			})
			It("print 'SUPPORT' if PlannerSupport is set", func() {
				testutils.SkipIfBefore7(connectionPool)
				funcDef.PlannerSupport = "my_planner_support"
				backup.PrintFunctionModifiers(backupfile, funcDef)
				testhelper.ExpectRegexp(buffer, "SUPPORT my_planner_support")
			})
			Context("Execlocation cases", func() {
				It("Default", func() {
					funcDef.ExecLocation = "a"
					backup.PrintFunctionModifiers(backupfile, funcDef)
					testhelper.NotExpectRegexp(buffer, "EXECUTE")
				})
				It("print 'm' as EXECUTE ON MASTER", func() {
					funcDef.ExecLocation = "m"
					backup.PrintFunctionModifiers(backupfile, funcDef)
					testhelper.ExpectRegexp(buffer, "EXECUTE ON MASTER")
				})
				It("print 'c' as EXECUTE ON COORDINATOR", func() {
					funcDef.ExecLocation = "c"
					backup.PrintFunctionModifiers(backupfile, funcDef)
					testhelper.ExpectRegexp(buffer, "EXECUTE ON COORDINATOR")
				})
				It("print 's' as EXECUTE ON ALL SEGMENTS", func() {
					funcDef.ExecLocation = "s"
					backup.PrintFunctionModifiers(backupfile, funcDef)
					testhelper.ExpectRegexp(buffer, "EXECUTE ON ALL SEGMENTS")
				})
				It("print 'i' as EXECUTE ON INITPLAN", func() {
					funcDef.ExecLocation = "i"
					backup.PrintFunctionModifiers(backupfile, funcDef)
					testhelper.ExpectRegexp(buffer, "EXECUTE ON INITPLAN")
				})
			})
			Context("Cost cases", func() {
				/*
				 * The default COST values are 1 for C and internal functions and
				 * 100 for any other language, so it should not print COST clauses
				 * for those values but print any other COST.
				 */
				It("prints 'COST 5' if Cost is set to 5", func() {
					funcDef.Cost = 5
					backup.PrintFunctionModifiers(backupfile, funcDef)
					testhelper.ExpectRegexp(buffer, "COST 5")
				})
				It("prints 'COST 1' if Cost is set to 1 and language is not c or internal", func() {
					funcDef.Cost = 1
					funcDef.Language = "sql"
					backup.PrintFunctionModifiers(backupfile, funcDef)
					testhelper.ExpectRegexp(buffer, "COST 1")
				})
				It("does not print 'COST 1' if Cost is set to 1 and language is c", func() {
					funcDef.Cost = 1
					funcDef.Language = "c"
					backup.PrintFunctionModifiers(backupfile, funcDef)
					testhelper.NotExpectRegexp(buffer, "COST")
				})
				It("does not print 'COST 1' if Cost is set to 1 and language is internal", func() {
					funcDef.Cost = 1
					funcDef.Language = "internal"
					backup.PrintFunctionModifiers(backupfile, funcDef)
					testhelper.NotExpectRegexp(buffer, "COST")
				})
				It("prints 'COST 100' if Cost is set to 100 and language is c", func() {
					funcDef.Cost = 100
					funcDef.Language = "c"
					backup.PrintFunctionModifiers(backupfile, funcDef)
					testhelper.ExpectRegexp(buffer, "COST 100")
				})
				It("prints 'COST 100' if Cost is set to 100 and language is internal", func() {
					funcDef.Cost = 100
					funcDef.Language = "internal"
					backup.PrintFunctionModifiers(backupfile, funcDef)
					testhelper.ExpectRegexp(buffer, "COST 100")
				})
				It("does not print 'COST 100' if Cost is set to 100 and language is not c or internal", func() {
					funcDef.Cost = 100
					funcDef.Language = "sql"
					backup.PrintFunctionModifiers(backupfile, funcDef)
					testhelper.NotExpectRegexp(buffer, "COST 100")
				})
			})
			Context("NumRows cases", func() {
				/*
				 * A ROWS value of 0 means "no estimate" and 1000 means "too high
				 * to estimate", so those should not be printed but any other ROWS
				 * value should be.
				 */
				It("prints 'ROWS 5' if Rows is set to 5", func() {
					funcDef.NumRows = 5
					funcDef.ReturnsSet = true
					backup.PrintFunctionModifiers(backupfile, funcDef)
					testhelper.ExpectRegexp(buffer, "ROWS 5")
				})
				It("does not print 'ROWS' if Rows is set but ReturnsSet is false", func() {
					funcDef.NumRows = 100
					funcDef.ReturnsSet = false
					backup.PrintFunctionModifiers(backupfile, funcDef)
					testhelper.NotExpectRegexp(buffer, "ROWS")
				})
				It("does not print 'ROWS' if Rows is set to 0", func() {
					funcDef.NumRows = 0
					funcDef.ReturnsSet = true
					backup.PrintFunctionModifiers(backupfile, funcDef)
					testhelper.NotExpectRegexp(buffer, "ROWS")
				})
				It("does not print 'ROWS' if Rows is set to 1000", func() {
					funcDef.NumRows = 1000
					funcDef.ReturnsSet = true
					backup.PrintFunctionModifiers(backupfile, funcDef)
					testhelper.NotExpectRegexp(buffer, "ROWS")
				})
			})
			It("prints config statements if any are set", func() {
				funcDef.Config = "SET client_min_messages TO error"
				backup.PrintFunctionModifiers(backupfile, funcDef)
				testhelper.ExpectRegexp(buffer, "SET client_min_messages TO error")
			})
			Context("Parallel cases", func() {
				It("prints 'u' as 'PARALLEL UNSAFE'", func() {
					testutils.SkipIfBefore7(connectionPool)
					funcDef.Parallel = "u"
					backup.PrintFunctionModifiers(backupfile, funcDef)
					testhelper.ExpectRegexp(buffer, "PARALLEL UNSAFE")
				})
				It("prints 's' as 'PARALLEL SAFE'", func() {
					testutils.SkipIfBefore7(connectionPool)
					funcDef.Parallel = "s"
					backup.PrintFunctionModifiers(backupfile, funcDef)
					testhelper.ExpectRegexp(buffer, "PARALLEL SAFE")
				})
				It("prints 'r' as 'PARALLEL RESTRICTED'", func() {
					testutils.SkipIfBefore7(connectionPool)
					funcDef.Parallel = "r"
					backup.PrintFunctionModifiers(backupfile, funcDef)
					testhelper.ExpectRegexp(buffer, "PARALLEL RESTRICTED")
				})
				It("panics is there is an unrecognized parallel value", func() {
					testutils.SkipIfBefore7(connectionPool)
					defer testhelper.ShouldPanicWithMessage("unrecognized proparallel value for function public.func_name")
					funcDef.Parallel = "unknown_value"
					backup.PrintFunctionModifiers(backupfile, funcDef)
				})
			})

		})

	})
	Describe("PrintCreateAggregateStatement", func() {
		var (
			aggDefinition backup.Aggregate
			emptyMetadata backup.ObjectMetadata
			aggMetadata   backup.ObjectMetadata
		)
		funcInfoMap := map[uint32]backup.FunctionInfo{
			1: {QualifiedName: "public.mysfunc", Arguments: sql.NullString{String: "integer", Valid: true}},
			2: {QualifiedName: "public.mypfunc", Arguments: sql.NullString{String: "numeric, numeric", Valid: true}},
			3: {QualifiedName: "public.myffunc", Arguments: sql.NullString{String: "text", Valid: true}},
			4: {QualifiedName: "pg_catalog.ordered_set_transition_multi", Arguments: sql.NullString{String: `internal, VARIADIC "any"`, Valid: true}},
			5: {QualifiedName: "pg_catalog.rank_final", Arguments: sql.NullString{String: `internal, VARIADIC "any"`, Valid: true}},
		}
		BeforeEach(func() {
			aggDefinition = backup.Aggregate{Oid: 1, Schema: "public", Name: "agg_name", Arguments: sql.NullString{String: "integer, integer", Valid: true}, IdentArgs: sql.NullString{String: "integer, integer", Valid: true}, TransitionFunction: 1, TransitionDataType: "integer", InitValIsNull: true, MInitValIsNull: true}
			emptyMetadata = backup.ObjectMetadata{}
			aggMetadata = testutils.DefaultMetadata("AGGREGATE", false, true, true, true)
		})

		It("prints an aggregate definition for an unordered aggregate with no optional specifications", func() {
			backup.PrintCreateAggregateStatement(backupfile, tocfile, aggDefinition, funcInfoMap, emptyMetadata)
			testutils.ExpectEntry(tocfile.PredataEntries, 0, "public", "", "agg_name(integer, integer)", "AGGREGATE")
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE AGGREGATE public.agg_name(integer, integer) (
	SFUNC = public.mysfunc,
	STYPE = integer
);`)
		})
		It("prints an aggregate definition for an ordered aggregate with no optional specifications", func() {
			aggDefinition.IsOrdered = true
			backup.PrintCreateAggregateStatement(backupfile, tocfile, aggDefinition, funcInfoMap, emptyMetadata)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE ORDERED AGGREGATE public.agg_name(integer, integer) (
	SFUNC = public.mysfunc,
	STYPE = integer
);`)
		})
		It("prints an aggregate definition for an unordered aggregate with no arguments", func() {
			aggDefinition.Arguments = sql.NullString{String: "", Valid: true}
			aggDefinition.IdentArgs = sql.NullString{String: "", Valid: true}
			backup.PrintCreateAggregateStatement(backupfile, tocfile, aggDefinition, funcInfoMap, emptyMetadata)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE AGGREGATE public.agg_name(*) (
	SFUNC = public.mysfunc,
	STYPE = integer
);`)
		})
		It("prints an aggregate with a preliminary function", func() {
			aggDefinition.PreliminaryFunction = 2
			backup.PrintCreateAggregateStatement(backupfile, tocfile, aggDefinition, funcInfoMap, emptyMetadata)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE AGGREGATE public.agg_name(integer, integer) (
	SFUNC = public.mysfunc,
	STYPE = integer,
	PREFUNC = public.mypfunc
);`)
		})
		It("prints an aggregate with a combine function", func() {
			aggDefinition.CombineFunction = 2
			backup.PrintCreateAggregateStatement(backupfile, tocfile, aggDefinition, funcInfoMap, emptyMetadata)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE AGGREGATE public.agg_name(integer, integer) (
	SFUNC = public.mysfunc,
	STYPE = integer,
	COMBINEFUNC = public.mypfunc
);`)
		})
		It("prints an aggregate with a serial function", func() {
			aggDefinition.SerialFunction = 2
			backup.PrintCreateAggregateStatement(backupfile, tocfile, aggDefinition, funcInfoMap, emptyMetadata)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE AGGREGATE public.agg_name(integer, integer) (
	SFUNC = public.mysfunc,
	STYPE = integer,
	SERIALFUNC = public.mypfunc
);`)
		})
		It("prints an aggregate with a deserial function", func() {
			aggDefinition.DeserialFunction = 2
			backup.PrintCreateAggregateStatement(backupfile, tocfile, aggDefinition, funcInfoMap, emptyMetadata)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE AGGREGATE public.agg_name(integer, integer) (
	SFUNC = public.mysfunc,
	STYPE = integer,
	DESERIALFUNC = public.mypfunc
);`)
		})
		It("prints an aggregate with a final function", func() {
			aggDefinition.FinalFunction = 3
			backup.PrintCreateAggregateStatement(backupfile, tocfile, aggDefinition, funcInfoMap, emptyMetadata)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE AGGREGATE public.agg_name(integer, integer) (
	SFUNC = public.mysfunc,
	STYPE = integer,
	FINALFUNC = public.myffunc
);`)
		})
		It("prints an aggregate with a final function extra attribute", func() {
			aggDefinition.FinalFuncExtra = true
			backup.PrintCreateAggregateStatement(backupfile, tocfile, aggDefinition, funcInfoMap, emptyMetadata)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE AGGREGATE public.agg_name(integer, integer) (
	SFUNC = public.mysfunc,
	STYPE = integer,
	FINALFUNC_EXTRA
);`)
		})
		It("prints an aggregate with an initial condition", func() {
			aggDefinition.InitialValue = "0"
			aggDefinition.InitValIsNull = false
			backup.PrintCreateAggregateStatement(backupfile, tocfile, aggDefinition, funcInfoMap, emptyMetadata)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE AGGREGATE public.agg_name(integer, integer) (
	SFUNC = public.mysfunc,
	STYPE = integer,
	INITCOND = '0'
);`)
		})
		It("prints an aggregate with a sort operator", func() {
			aggDefinition.SortOperator = "+"
			aggDefinition.SortOperatorSchema = "myschema"
			backup.PrintCreateAggregateStatement(backupfile, tocfile, aggDefinition, funcInfoMap, emptyMetadata)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE AGGREGATE public.agg_name(integer, integer) (
	SFUNC = public.mysfunc,
	STYPE = integer,
	SORTOP = myschema."+"
);`)
		})
		It("prints an aggregate with a specified transition data size", func() {
			aggDefinition.TransitionDataSize = 1000
			backup.PrintCreateAggregateStatement(backupfile, tocfile, aggDefinition, funcInfoMap, emptyMetadata)
			testutils.ExpectEntry(tocfile.PredataEntries, 0, "public", "", "agg_name(integer, integer)", "AGGREGATE")
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE AGGREGATE public.agg_name(integer, integer) (
	SFUNC = public.mysfunc,
	STYPE = integer,
	SSPACE = 1000
);`)
		})
		It("prints an aggregate with a specified moving transition function", func() {
			aggDefinition.MTransitionFunction = 1
			backup.PrintCreateAggregateStatement(backupfile, tocfile, aggDefinition, funcInfoMap, emptyMetadata)
			testutils.ExpectEntry(tocfile.PredataEntries, 0, "public", "", "agg_name(integer, integer)", "AGGREGATE")
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE AGGREGATE public.agg_name(integer, integer) (
	SFUNC = public.mysfunc,
	STYPE = integer,
	MSFUNC = public.mysfunc
);`)
		})
		It("prints an aggregate with a specified moving inverse transition function", func() {
			aggDefinition.MInverseTransitionFunction = 1
			backup.PrintCreateAggregateStatement(backupfile, tocfile, aggDefinition, funcInfoMap, emptyMetadata)
			testutils.ExpectEntry(tocfile.PredataEntries, 0, "public", "", "agg_name(integer, integer)", "AGGREGATE")
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE AGGREGATE public.agg_name(integer, integer) (
	SFUNC = public.mysfunc,
	STYPE = integer,
	MINVFUNC = public.mysfunc
);`)
		})
		It("prints an aggregate with a specified moving state type", func() {
			aggDefinition.MTransitionDataType = "numeric"
			backup.PrintCreateAggregateStatement(backupfile, tocfile, aggDefinition, funcInfoMap, emptyMetadata)
			testutils.ExpectEntry(tocfile.PredataEntries, 0, "public", "", "agg_name(integer, integer)", "AGGREGATE")
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE AGGREGATE public.agg_name(integer, integer) (
	SFUNC = public.mysfunc,
	STYPE = integer,
	MSTYPE = numeric
);`)
		})
		It("prints an aggregate with a specified moving transition size", func() {
			aggDefinition.MTransitionDataSize = 100
			backup.PrintCreateAggregateStatement(backupfile, tocfile, aggDefinition, funcInfoMap, emptyMetadata)
			testutils.ExpectEntry(tocfile.PredataEntries, 0, "public", "", "agg_name(integer, integer)", "AGGREGATE")
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE AGGREGATE public.agg_name(integer, integer) (
	SFUNC = public.mysfunc,
	STYPE = integer,
	MSSPACE = 100
);`)
		})
		It("prints an aggregate with a specified moving final function", func() {
			aggDefinition.MFinalFunction = 3
			backup.PrintCreateAggregateStatement(backupfile, tocfile, aggDefinition, funcInfoMap, emptyMetadata)
			testutils.ExpectEntry(tocfile.PredataEntries, 0, "public", "", "agg_name(integer, integer)", "AGGREGATE")
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE AGGREGATE public.agg_name(integer, integer) (
	SFUNC = public.mysfunc,
	STYPE = integer,
	MFINALFUNC = public.myffunc
);`)
		})
		It("prints an aggregate with a moving final function extra attribute", func() {
			aggDefinition.MFinalFuncExtra = true
			backup.PrintCreateAggregateStatement(backupfile, tocfile, aggDefinition, funcInfoMap, emptyMetadata)
			testutils.ExpectEntry(tocfile.PredataEntries, 0, "public", "", "agg_name(integer, integer)", "AGGREGATE")
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE AGGREGATE public.agg_name(integer, integer) (
	SFUNC = public.mysfunc,
	STYPE = integer,
	MFINALFUNC_EXTRA
);`)
		})
		It("prints an aggregate with a moving initial condition", func() {
			aggDefinition.MInitialValue = "0"
			aggDefinition.MInitValIsNull = false
			backup.PrintCreateAggregateStatement(backupfile, tocfile, aggDefinition, funcInfoMap, emptyMetadata)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE AGGREGATE public.agg_name(integer, integer) (
	SFUNC = public.mysfunc,
	STYPE = integer,
	MINITCOND = '0'
);`)
		})
		It("prints an aggregate with multiple specifications", func() {
			aggDefinition.FinalFunction = 3
			aggDefinition.SortOperator = "~>~"
			aggDefinition.SortOperatorSchema = "myschema"
			backup.PrintCreateAggregateStatement(backupfile, tocfile, aggDefinition, funcInfoMap, emptyMetadata)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE AGGREGATE public.agg_name(integer, integer) (
	SFUNC = public.mysfunc,
	STYPE = integer,
	FINALFUNC = public.myffunc,
	SORTOP = myschema."~>~"
);`)
		})
		It("prints a hypothetical ordered-set aggregate", func() {
			complexAggDefinition := backup.Aggregate{
				Schema: "public", Name: "agg_hypo_ord", Arguments: sql.NullString{String: `VARIADIC "any" ORDER BY VARIADIC "any"`, Valid: true},
				IdentArgs: sql.NullString{String: `VARIADIC "any" ORDER BY VARIADIC "any"`, Valid: true}, TransitionFunction: 4, FinalFunction: 5,
				TransitionDataType: "internal", InitValIsNull: true, MInitValIsNull: true, FinalFuncExtra: true,
			}
			if connectionPool.Version.AtLeast("7") {
				complexAggDefinition.Kind = "h"
			} else {
				complexAggDefinition.Hypothetical = true
			}
			aggDefinition = complexAggDefinition
			backup.PrintCreateAggregateStatement(backupfile, tocfile, aggDefinition, funcInfoMap, emptyMetadata)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE AGGREGATE public.agg_hypo_ord(VARIADIC "any" ORDER BY VARIADIC "any") (
	SFUNC = pg_catalog.ordered_set_transition_multi,
	STYPE = internal,
	FINALFUNC = pg_catalog.rank_final,
	FINALFUNC_EXTRA,
	HYPOTHETICAL
);`)
		})
		It("prints an aggregate with owner, security label and comment", func() {
			backup.PrintCreateAggregateStatement(backupfile, tocfile, aggDefinition, funcInfoMap, aggMetadata)
			expectedStatements := []string{
				`CREATE AGGREGATE public.agg_name(integer, integer) (
	SFUNC = public.mysfunc,
	STYPE = integer
);`, "COMMENT ON AGGREGATE public.agg_name(integer, integer) IS 'This is an aggregate comment.';",
				"ALTER AGGREGATE public.agg_name(integer, integer) OWNER TO testrole;",
				"SECURITY LABEL FOR dummy ON AGGREGATE public.agg_name(integer, integer) IS 'unclassified';"}
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, expectedStatements...)
		})
		It("prints an aggregate with owner, comment, and no arguments", func() {
			aggDefinition.Arguments = sql.NullString{String: "", Valid: true}
			aggDefinition.IdentArgs = sql.NullString{String: "", Valid: true}
			backup.PrintCreateAggregateStatement(backupfile, tocfile, aggDefinition, funcInfoMap, aggMetadata)
			expectedStatements := []string{
				`CREATE AGGREGATE public.agg_name(*) (
	SFUNC = public.mysfunc,
	STYPE = integer
);`,
				"COMMENT ON AGGREGATE public.agg_name(*) IS 'This is an aggregate comment.';",
				"ALTER AGGREGATE public.agg_name(*) OWNER TO testrole;",
				"SECURITY LABEL FOR dummy ON AGGREGATE public.agg_name(*) IS 'unclassified';"}
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, expectedStatements...)
		})
		It("prints an aggregate definition with parallel safe modifier", func() {
			aggDefinition.Parallel = "s"
			backup.PrintCreateAggregateStatement(backupfile, tocfile, aggDefinition, funcInfoMap, emptyMetadata)
			testutils.ExpectEntry(tocfile.PredataEntries, 0, "public", "", "agg_name(integer, integer)", "AGGREGATE")
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE AGGREGATE public.agg_name(integer, integer) (
	SFUNC = public.mysfunc,
	STYPE = integer,
	PARALLEL = SAFE
);`)
		})
		DescribeTable("prints aggregate with aggfinalmodify or aggmfinalmodify",
			func(kind string, finalMod string, mfinalMod string, expected string) {
				testutils.SkipIfBefore7(connectionPool)
				aggDefinition = backup.Aggregate{Oid: 1, Schema: "public", Name: "agg_name", Arguments: sql.NullString{String: "", Valid: true}, IdentArgs: sql.NullString{String: "", Valid: true}, TransitionFunction: 1, TransitionDataType: "integer", InitValIsNull: true, MInitValIsNull: true}
				aggDefinition.Kind = kind
				aggDefinition.Finalmodify = finalMod
				aggDefinition.Mfinalmodify = mfinalMod
				backup.PrintCreateAggregateStatement(backupfile, tocfile, aggDefinition, funcInfoMap, aggMetadata)
				expectedStatements := []string{
					fmt.Sprintf(`CREATE AGGREGATE public.agg_name(*) (
	SFUNC = public.mysfunc,
	STYPE = integer%s
);`, expected),
					"COMMENT ON AGGREGATE public.agg_name(*) IS 'This is an aggregate comment.';",
					"ALTER AGGREGATE public.agg_name(*) OWNER TO testrole;",
					"SECURITY LABEL FOR dummy ON AGGREGATE public.agg_name(*) IS 'unclassified';"}
				testutils.AssertBufferContents(tocfile.PredataEntries, buffer, expectedStatements...)
			},
			Entry("kind: n, aggfinalmodify: r", "n", "r", "", ""), // default, don't print
			Entry("kind: n, aggfinalmodify: s", "n", "s", "", ",\n\tFINALFUNC_MODIFY = SHAREABLE"),
			Entry("kind: n, aggfinalmodify: w", "n", "w", "", ",\n\tFINALFUNC_MODIFY = READ_WRITE"),
			Entry("kind: o or h, aggfinalmodify: r", "o", "r", "", ",\n\tFINALFUNC_MODIFY = READ_ONLY"),
			Entry("kind: o or h, aggfinalmodify: s", "o", "s", "", ",\n\tFINALFUNC_MODIFY = SHAREABLE"),
			Entry("kind: o or h, aggfinalmodify: w", "o", "w", "", ""), // default, don't print

			Entry("kind: n, aggmfinalmodify: r", "n", "", "r", ""), // default, don't print
			Entry("kind: n, aggmfinalmodify: s", "n", "", "s", ",\n\tMFINALFUNC_MODIFY = SHAREABLE"),
			Entry("kind: n, aggmfinalmodify: w", "n", "", "w", ",\n\tMFINALFUNC_MODIFY = READ_WRITE"),
			Entry("kind: o or h, aggmfinalmodify: r", "o", "", "r", ",\n\tMFINALFUNC_MODIFY = READ_ONLY"),
			Entry("kind: o or h, aggmfinalmodify: s", "o", "", "s", ",\n\tMFINALFUNC_MODIFY = SHAREABLE"),
			Entry("kind: o or h, aggmfinalmodify: w", "o", "", "w", ""), // default, don't print
		)
	})
	Describe("PrintCreateCastStatement", func() {
		emptyMetadata := backup.ObjectMetadata{}
		It("prints an explicit cast with a function", func() {
			castDef := backup.Cast{Oid: 1, SourceTypeFQN: "src", TargetTypeFQN: "dst", FunctionSchema: "public", FunctionName: "cast_func", FunctionArgs: "integer, integer", CastContext: "e", CastMethod: "f"}
			backup.PrintCreateCastStatement(backupfile, tocfile, castDef, emptyMetadata)
			testutils.ExpectEntry(tocfile.PredataEntries, 0, "public", "", "(src AS dst)", "CAST")
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE CAST (src AS dst)
	WITH FUNCTION public.cast_func(integer, integer);`)
		})
		It("prints an implicit cast with a function", func() {
			castDef := backup.Cast{Oid: 1, SourceTypeFQN: "src", TargetTypeFQN: "dst", FunctionSchema: "public", FunctionName: "cast_func", FunctionArgs: "integer, integer", CastContext: "i", CastMethod: "f"}
			backup.PrintCreateCastStatement(backupfile, tocfile, castDef, emptyMetadata)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE CAST (src AS dst)
	WITH FUNCTION public.cast_func(integer, integer)
AS IMPLICIT;`)
		})
		It("prints an assignment cast with a function", func() {
			castDef := backup.Cast{Oid: 1, SourceTypeFQN: "src", TargetTypeFQN: "dst", FunctionSchema: "public", FunctionName: "cast_func", FunctionArgs: "integer, integer", CastContext: "a", CastMethod: "f"}
			backup.PrintCreateCastStatement(backupfile, tocfile, castDef, emptyMetadata)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE CAST (src AS dst)
	WITH FUNCTION public.cast_func(integer, integer)
AS ASSIGNMENT;`)
		})
		It("prints an explicit cast without a function", func() {
			castDef := backup.Cast{Oid: 1, SourceTypeFQN: "src", TargetTypeFQN: "dst", FunctionSchema: "", FunctionName: "", FunctionArgs: "", CastContext: "e", CastMethod: "b"}
			backup.PrintCreateCastStatement(backupfile, tocfile, castDef, emptyMetadata)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE CAST (src AS dst)
	WITHOUT FUNCTION;`)
		})
		It("prints an implicit cast without a function", func() {
			castDef := backup.Cast{Oid: 1, SourceTypeFQN: "src", TargetTypeFQN: "dst", FunctionSchema: "", FunctionName: "", FunctionArgs: "", CastContext: "i", CastMethod: "b"}
			backup.PrintCreateCastStatement(backupfile, tocfile, castDef, emptyMetadata)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE CAST (src AS dst)
	WITHOUT FUNCTION
AS IMPLICIT;`)
		})
		It("prints an assignment cast without a function", func() {
			castDef := backup.Cast{Oid: 1, SourceTypeFQN: "src", TargetTypeFQN: "dst", FunctionSchema: "", FunctionName: "", FunctionArgs: "", CastContext: "a", CastMethod: "b"}
			backup.PrintCreateCastStatement(backupfile, tocfile, castDef, emptyMetadata)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE CAST (src AS dst)
	WITHOUT FUNCTION
AS ASSIGNMENT;`)
		})
		It("prints an inout cast", func() {
			castDef := backup.Cast{Oid: 1, SourceTypeFQN: "src", TargetTypeFQN: "dst", FunctionSchema: "", FunctionName: "", FunctionArgs: "", CastContext: "e", CastMethod: "i"}
			backup.PrintCreateCastStatement(backupfile, tocfile, castDef, emptyMetadata)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE CAST (src AS dst)
	WITH INOUT;`)
		})
		It("prints a cast with a comment", func() {
			castDef := backup.Cast{Oid: 1, SourceTypeFQN: "src", TargetTypeFQN: "dst", FunctionSchema: "", FunctionName: "", FunctionArgs: "", CastContext: "e", CastMethod: "b"}
			castMetadata := testutils.DefaultMetadata("CAST", false, false, true, false)
			backup.PrintCreateCastStatement(backupfile, tocfile, castDef, castMetadata)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE CAST (src AS dst)
	WITHOUT FUNCTION;`, "COMMENT ON CAST (src AS dst) IS 'This is a cast comment.';")
		})
	})
	Describe("PrintCreateExtensionStatement", func() {
		emptyMetadataMap := backup.MetadataMap{}
		It("prints a create extension statement", func() {
			extensionDef := backup.Extension{Oid: 1, Name: "extension1", Schema: "schema1"}
			backup.PrintCreateExtensionStatements(backupfile, tocfile, []backup.Extension{extensionDef}, emptyMetadataMap)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `SET search_path=schema1,pg_catalog;
CREATE EXTENSION IF NOT EXISTS extension1 WITH SCHEMA schema1;
SET search_path=pg_catalog;`)
		})
		It("prints a create extension statement with a comment", func() {
			extensionDef := backup.Extension{Oid: 1, Name: "extension1", Schema: "schema1"}
			extensionMetadataMap := testutils.DefaultMetadataMap("EXTENSION", false, false, true, false)
			backup.PrintCreateExtensionStatements(backupfile, tocfile, []backup.Extension{extensionDef}, extensionMetadataMap)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `SET search_path=schema1,pg_catalog;
CREATE EXTENSION IF NOT EXISTS extension1 WITH SCHEMA schema1;
SET search_path=pg_catalog;`, "COMMENT ON EXTENSION extension1 IS 'This is an extension comment.';")
		})
	})
	Describe("ExtractLanguageFunctions", func() {
		customLang1 := backup.ProceduralLanguage{Oid: 1, Name: "custom_language", Owner: "testrole", IsPl: true, PlTrusted: true, Handler: 3, Inline: 4, Validator: 5}
		customLang2 := backup.ProceduralLanguage{Oid: 2, Name: "custom_language2", Owner: "testrole", IsPl: true, PlTrusted: true, Handler: 5, Inline: 6, Validator: 7}
		procLangs := []backup.ProceduralLanguage{customLang1, customLang2}
		langFunc := backup.Function{Oid: 3, Name: "custom_handler"}
		nonLangFunc := backup.Function{Oid: 2, Name: "random_function"}
		It("handles a case where all functions are language-associated functions", func() {
			funcDefs := []backup.Function{langFunc}
			langFuncs, otherFuncs := backup.ExtractLanguageFunctions(funcDefs, procLangs)
			Expect(langFuncs).To(HaveLen(1))
			Expect(otherFuncs).To(BeEmpty())
			Expect(langFuncs[0].Name).To(Equal("custom_handler"))
		})
		It("handles a case where no functions are language-associated functions", func() {
			funcDefs := []backup.Function{nonLangFunc}
			langFuncs, otherFuncs := backup.ExtractLanguageFunctions(funcDefs, procLangs)
			Expect(langFuncs).To(BeEmpty())
			Expect(otherFuncs).To(HaveLen(1))
			Expect(otherFuncs[0].Name).To(Equal("random_function"))
		})
		It("handles a case where some functions are language-associated functions", func() {
			funcDefs := []backup.Function{langFunc, nonLangFunc}
			langFuncs, otherFuncs := backup.ExtractLanguageFunctions(funcDefs, procLangs)
			Expect(langFuncs).To(HaveLen(1))
			Expect(otherFuncs).To(HaveLen(1))
			Expect(langFuncs[0].Name).To(Equal("custom_handler"))
			Expect(otherFuncs[0].Name).To(Equal("random_function"))
		})
	})
	Describe("PrintCreateLanguageStatements", func() {
		plUntrustedHandlerOnly := backup.ProceduralLanguage{Oid: 1, Name: "plpythonu", Owner: "testrole", IsPl: true, PlTrusted: false, Handler: 4, Inline: 0, Validator: 0}
		plAllFields := backup.ProceduralLanguage{Oid: 1, Name: "plperl", Owner: "testrole", IsPl: true, PlTrusted: true, Handler: 1, Inline: 2, Validator: 3}
		plComment := backup.ProceduralLanguage{Oid: 1, Name: "plpythonu", Owner: "testrole", IsPl: true, PlTrusted: false, Handler: 4, Inline: 0, Validator: 0}
		funcInfoMap := map[uint32]backup.FunctionInfo{
			1: {QualifiedName: "pg_catalog.plperl_call_handler", Arguments: sql.NullString{String: "", Valid: true}, IsInternal: true},
			2: {QualifiedName: "pg_catalog.plperl_inline_handler", Arguments: sql.NullString{String: "internal", Valid: true}, IsInternal: true},
			3: {QualifiedName: "pg_catalog.plperl_validator", Arguments: sql.NullString{String: "oid", Valid: true}, IsInternal: true},
			4: {QualifiedName: "pg_catalog.plpython_call_handler", Arguments: sql.NullString{String: "", Valid: true}, IsInternal: true},
		}
		emptyMetadataMap := backup.MetadataMap{}

		It("prints untrusted language with a handler only", func() {
			langs := []backup.ProceduralLanguage{plUntrustedHandlerOnly}

			backup.PrintCreateLanguageStatements(backupfile, tocfile, langs, funcInfoMap, emptyMetadataMap)
			testutils.ExpectEntry(tocfile.PredataEntries, 0, "", "", "plpythonu", "LANGUAGE")

			createStatement1 := "CREATE PROCEDURAL LANGUAGE plpythonu HANDLER pg_catalog.plpython_call_handler;"
			if connectionPool.Version.AtLeast("6") {
				createStatement1 = "CREATE OR REPLACE PROCEDURAL LANGUAGE plpythonu HANDLER pg_catalog.plpython_call_handler;"
			}
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, createStatement1, "ALTER FUNCTION pg_catalog.plpython_call_handler() OWNER TO testrole;")
		})
		It("prints trusted language with handler, inline, and validator", func() {
			langs := []backup.ProceduralLanguage{plAllFields}

			backup.PrintCreateLanguageStatements(backupfile, tocfile, langs, funcInfoMap, emptyMetadataMap)

			createStatement1 := "CREATE TRUSTED PROCEDURAL LANGUAGE plperl HANDLER pg_catalog.plperl_call_handler INLINE pg_catalog.plperl_inline_handler VALIDATOR pg_catalog.plperl_validator;"
			if connectionPool.Version.AtLeast("6") {
				createStatement1 = "CREATE OR REPLACE TRUSTED PROCEDURAL LANGUAGE plperl HANDLER pg_catalog.plperl_call_handler INLINE pg_catalog.plperl_inline_handler VALIDATOR pg_catalog.plperl_validator;"
			}

			expectedStatements := []string{
				createStatement1,
				`ALTER FUNCTION pg_catalog.plperl_call_handler() OWNER TO testrole;
ALTER FUNCTION pg_catalog.plperl_inline_handler(internal) OWNER TO testrole;
ALTER FUNCTION pg_catalog.plperl_validator(oid) OWNER TO testrole;`,
			}
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, expectedStatements...)
		})
		It("prints multiple create language statements", func() {
			langs := []backup.ProceduralLanguage{plUntrustedHandlerOnly, plAllFields}

			backup.PrintCreateLanguageStatements(backupfile, tocfile, langs, funcInfoMap, emptyMetadataMap)

			createStatement1 := "CREATE PROCEDURAL LANGUAGE plpythonu HANDLER pg_catalog.plpython_call_handler;"
			createStatement2 := "CREATE TRUSTED PROCEDURAL LANGUAGE plperl HANDLER pg_catalog.plperl_call_handler INLINE pg_catalog.plperl_inline_handler VALIDATOR pg_catalog.plperl_validator;"
			if connectionPool.Version.AtLeast("6") {
				createStatement1 = "CREATE OR REPLACE PROCEDURAL LANGUAGE plpythonu HANDLER pg_catalog.plpython_call_handler;"
				createStatement2 = "CREATE OR REPLACE TRUSTED PROCEDURAL LANGUAGE plperl HANDLER pg_catalog.plperl_call_handler INLINE pg_catalog.plperl_inline_handler VALIDATOR pg_catalog.plperl_validator;"
			}
			expectedStatements := []string{
				createStatement1,
				"ALTER FUNCTION pg_catalog.plpython_call_handler() OWNER TO testrole;",
				createStatement2,
				`ALTER FUNCTION pg_catalog.plperl_call_handler() OWNER TO testrole;
ALTER FUNCTION pg_catalog.plperl_inline_handler(internal) OWNER TO testrole;
ALTER FUNCTION pg_catalog.plperl_validator(oid) OWNER TO testrole;`,
			}
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, expectedStatements...)
		})
		It("prints a language with privileges, an owner, security label, and a comment", func() {
			langs := []backup.ProceduralLanguage{plComment}
			langMetadataMap := testutils.DefaultMetadataMap("LANGUAGE", true, true, true, true)

			backup.PrintCreateLanguageStatements(backupfile, tocfile, langs, funcInfoMap, langMetadataMap)

			createStatement1 := "CREATE PROCEDURAL LANGUAGE plpythonu HANDLER pg_catalog.plpython_call_handler;"
			if connectionPool.Version.AtLeast("6") {
				createStatement1 = "CREATE OR REPLACE PROCEDURAL LANGUAGE plpythonu HANDLER pg_catalog.plpython_call_handler;"
			}

			expectedStatements := []string{
				createStatement1,
				"ALTER FUNCTION pg_catalog.plpython_call_handler() OWNER TO testrole;",
				"COMMENT ON LANGUAGE plpythonu IS 'This is a language comment.';",
			}
			if connectionPool.Version.AtLeast("5") {
				// Languages have implicit owners in 4.3, but do not support ALTER OWNER
				expectedStatements = append(expectedStatements, "ALTER LANGUAGE plpythonu OWNER TO testrole;")
			}
			expectedStatements = append(expectedStatements, `REVOKE ALL ON LANGUAGE plpythonu FROM PUBLIC;
REVOKE ALL ON LANGUAGE plpythonu FROM testrole;
GRANT ALL ON LANGUAGE plpythonu TO testrole;`,
				"SECURITY LABEL FOR dummy ON LANGUAGE plpythonu IS 'unclassified';")

			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, expectedStatements...)
		})
		It("prints a language using a role with % in its name", func() {
			langWithValidatorAndPercentOwner := backup.ProceduralLanguage{Oid: 1, Name: "plperl", Owner: "owner%percentage", IsPl: true, PlTrusted: true, Handler: 1, Inline: 2, Validator: 3}
			langs := []backup.ProceduralLanguage{langWithValidatorAndPercentOwner}

			langMetadataMap := testutils.DefaultMetadataMap("LANGUAGE", true, true, true, true)

			backup.PrintCreateLanguageStatements(backupfile, tocfile, langs, funcInfoMap, langMetadataMap)

			createStatement1 := "CREATE TRUSTED PROCEDURAL LANGUAGE plperl HANDLER pg_catalog.plperl_call_handler INLINE pg_catalog.plperl_inline_handler VALIDATOR pg_catalog.plperl_validator;"
			if connectionPool.Version.AtLeast("6") {
				createStatement1 = "CREATE OR REPLACE TRUSTED PROCEDURAL LANGUAGE plperl HANDLER pg_catalog.plperl_call_handler INLINE pg_catalog.plperl_inline_handler VALIDATOR pg_catalog.plperl_validator;"
			}
			expectedStatements := []string{
				createStatement1,
				"ALTER FUNCTION pg_catalog.plperl_call_handler() OWNER TO owner%percentage;\nALTER FUNCTION pg_catalog.plperl_inline_handler(internal) OWNER TO owner%percentage;\nALTER FUNCTION pg_catalog.plperl_validator(oid) OWNER TO owner%percentage;",
				`COMMENT ON LANGUAGE plperl IS 'This is a language comment.';`,
			}
			if connectionPool.Version.AtLeast("5") {
				// Languages have implicit owners in 4.3, but do not support ALTER OWNER
				expectedStatements = append(expectedStatements, `ALTER LANGUAGE plperl OWNER TO testrole;`)
			}
			expectedStatements = append(expectedStatements, `REVOKE ALL ON LANGUAGE plperl FROM PUBLIC;
REVOKE ALL ON LANGUAGE plperl FROM testrole;
GRANT ALL ON LANGUAGE plperl TO testrole;`,
				"SECURITY LABEL FOR dummy ON LANGUAGE plperl IS 'unclassified';")

			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, expectedStatements...)
		})
	})
	Describe("PrintCreateTransformStatement", func() {
		funcInfoMap := map[uint32]backup.FunctionInfo{
			1: {QualifiedName: "somenamespace.from_sql_f", IdentArgs: sql.NullString{String: "internal", Valid: true}},
			2: {QualifiedName: "somenamespace.to_sql_f", IdentArgs: sql.NullString{String: "internal", Valid: true}},
		}

		DescribeTable("prints transform statements with at least one transform function", func(fromSql uint32, toSql uint32, expected string) {
			testutils.SkipIfBefore7(connectionPool)
			transform := backup.Transform{Oid: 1, TypeNamespace: "mynamespace", TypeName: "mytype", LanguageName: "somelang", FromSQLFunc: fromSql, ToSQLFunc: toSql}
			transMetadata := testutils.DefaultMetadata("TRANSFORM", false, false, false, false)
			backup.PrintCreateTransformStatement(backupfile, tocfile, transform, funcInfoMap, transMetadata)
			expectedStatements := []string{fmt.Sprintf(`CREATE TRANSFORM FOR mynamespace.mytype LANGUAGE somelang %s;`, expected)}
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, expectedStatements...)
		},
			Entry("both functions are specified", uint32(1), uint32(2), "(FROM SQL WITH FUNCTION somenamespace.from_sql_f(internal), TO SQL WITH FUNCTION somenamespace.to_sql_f(internal))"),
			Entry("only fromSQL function is specified", uint32(1), uint32(0), "(FROM SQL WITH FUNCTION somenamespace.from_sql_f(internal))"),
			Entry("only toSql function is specified", uint32(0), uint32(2), "(TO SQL WITH FUNCTION somenamespace.to_sql_f(internal))"),
		)
		It("prints a warning if there are no transform functions specified", func() {
			testutils.SkipIfBefore7(connectionPool)
			_, _, logfile = testhelper.SetupTestLogger()
			transform := backup.Transform{Oid: 1, TypeNamespace: "mynamespace", TypeName: "mycustomtype", LanguageName: "someproclanguage", FromSQLFunc: 0, ToSQLFunc: 0}
			transMetadata := testutils.DefaultMetadata("TRANSFORM", false, false, false, false)
			backup.PrintCreateTransformStatement(backupfile, tocfile, transform, funcInfoMap, transMetadata)
			testhelper.ExpectRegexp(logfile, "[WARNING]:-Skipping invalid transform object for type mynamespace.mycustomtype and language someproclanguage; At least one of FROM and TO functions should be specified")
		})
	})

	Describe("PrintCreateConversionStatements", func() {
		var (
			convOne     backup.Conversion
			convTwo     backup.Conversion
			metadataMap backup.MetadataMap
		)
		BeforeEach(func() {
			convOne = backup.Conversion{Oid: 1, Schema: "public", Name: "conv_one", ForEncoding: "UTF8", ToEncoding: "LATIN1", ConversionFunction: "public.converter", IsDefault: false}
			convTwo = backup.Conversion{Oid: 0, Schema: "public", Name: "conv_two", ForEncoding: "UTF8", ToEncoding: "LATIN1", ConversionFunction: "public.converter", IsDefault: true}
			metadataMap = backup.MetadataMap{}
		})

		It("prints a non-default conversion", func() {
			conversions := []backup.Conversion{convOne}
			backup.PrintCreateConversionStatements(backupfile, tocfile, conversions, metadataMap)
			testutils.ExpectEntry(tocfile.PredataEntries, 0, "public", "", "conv_one", "CONVERSION")
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE CONVERSION public.conv_one FOR 'UTF8' TO 'LATIN1' FROM public.converter;`)
		})
		It("prints a default conversion", func() {
			conversions := []backup.Conversion{convTwo}
			backup.PrintCreateConversionStatements(backupfile, tocfile, conversions, metadataMap)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE DEFAULT CONVERSION public.conv_two FOR 'UTF8' TO 'LATIN1' FROM public.converter;`)
		})
		It("prints multiple create conversion statements", func() {
			conversions := []backup.Conversion{convOne, convTwo}
			backup.PrintCreateConversionStatements(backupfile, tocfile, conversions, metadataMap)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer,
				`CREATE CONVERSION public.conv_one FOR 'UTF8' TO 'LATIN1' FROM public.converter;`,
				`CREATE DEFAULT CONVERSION public.conv_two FOR 'UTF8' TO 'LATIN1' FROM public.converter;`)
		})
		It("prints a conversion with an owner and a comment", func() {
			conversions := []backup.Conversion{convOne}
			metadataMap = testutils.DefaultMetadataMap("CONVERSION", false, true, true, false)
			backup.PrintCreateConversionStatements(backupfile, tocfile, conversions, metadataMap)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, "CREATE CONVERSION public.conv_one FOR 'UTF8' TO 'LATIN1' FROM public.converter;",
				"COMMENT ON CONVERSION public.conv_one IS 'This is a conversion comment.';",
				"ALTER CONVERSION public.conv_one OWNER TO testrole;")
		})
	})
	Describe("PrintCreateForeignDataWrapperStatement", func() {
		funcInfoMap := map[uint32]backup.FunctionInfo{
			1: {QualifiedName: "pg_catalog.postgresql_fdw_handler", Arguments: sql.NullString{String: "", Valid: true}, IsInternal: true},
			2: {QualifiedName: "pg_catalog.postgresql_fdw_validator", Arguments: sql.NullString{String: "", Valid: true}, IsInternal: true},
		}
		It("prints a basic foreign data wrapper", func() {
			foreignDataWrapper := backup.ForeignDataWrapper{Oid: 1, Name: "foreigndata"}
			backup.PrintCreateForeignDataWrapperStatement(backupfile, tocfile, foreignDataWrapper, funcInfoMap, backup.ObjectMetadata{})
			testutils.ExpectEntry(tocfile.PredataEntries, 0, "", "", "foreigndata", "FOREIGN DATA WRAPPER")
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE FOREIGN DATA WRAPPER foreigndata;`)
		})
		It("prints a foreign data wrapper with a handler", func() {
			foreignDataWrapper := backup.ForeignDataWrapper{Name: "foreigndata", Handler: 1}
			backup.PrintCreateForeignDataWrapperStatement(backupfile, tocfile, foreignDataWrapper, funcInfoMap, backup.ObjectMetadata{})
			testutils.ExpectEntry(tocfile.PredataEntries, 0, "", "", "foreigndata", "FOREIGN DATA WRAPPER")
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE FOREIGN DATA WRAPPER foreigndata
	HANDLER pg_catalog.postgresql_fdw_handler;`)
		})
		It("prints a foreign data wrapper with a validator", func() {
			foreignDataWrapper := backup.ForeignDataWrapper{Name: "foreigndata", Validator: 2}
			backup.PrintCreateForeignDataWrapperStatement(backupfile, tocfile, foreignDataWrapper, funcInfoMap, backup.ObjectMetadata{})
			testutils.ExpectEntry(tocfile.PredataEntries, 0, "", "", "foreigndata", "FOREIGN DATA WRAPPER")
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE FOREIGN DATA WRAPPER foreigndata
	VALIDATOR pg_catalog.postgresql_fdw_validator;`)
		})
		It("prints a foreign data wrapper with one option", func() {
			foreignDataWrapper := backup.ForeignDataWrapper{Name: "foreigndata", Options: "debug 'true'"}
			backup.PrintCreateForeignDataWrapperStatement(backupfile, tocfile, foreignDataWrapper, funcInfoMap, backup.ObjectMetadata{})
			testutils.ExpectEntry(tocfile.PredataEntries, 0, "", "", "foreigndata", "FOREIGN DATA WRAPPER")
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE FOREIGN DATA WRAPPER foreigndata
	OPTIONS (debug 'true');`)
		})
		It("prints a foreign data wrapper with two options", func() {
			foreignDataWrapper := backup.ForeignDataWrapper{Name: "foreigndata", Options: "debug 'true', host 'localhost'"}
			backup.PrintCreateForeignDataWrapperStatement(backupfile, tocfile, foreignDataWrapper, funcInfoMap, backup.ObjectMetadata{})
			testutils.ExpectEntry(tocfile.PredataEntries, 0, "", "", "foreigndata", "FOREIGN DATA WRAPPER")
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE FOREIGN DATA WRAPPER foreigndata
	OPTIONS (debug 'true', host 'localhost');`)
		})
	})
	Describe("PrintCreateServerStatement", func() {
		It("prints a basic foreign server", func() {
			foreignServer := backup.ForeignServer{Oid: 1, Name: "foreignserver", ForeignDataWrapper: "foreignwrapper"}
			backup.PrintCreateServerStatement(backupfile, tocfile, foreignServer, backup.ObjectMetadata{})
			testutils.ExpectEntry(tocfile.PredataEntries, 0, "", "", "foreignserver", "FOREIGN SERVER")
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE SERVER foreignserver
	FOREIGN DATA WRAPPER foreignwrapper;`)
		})
		It("prints a foreign server with one option", func() {
			foreignServer := backup.ForeignServer{Oid: 1, Name: "foreignserver", ForeignDataWrapper: "foreignwrapper", Options: "host 'localhost'"}
			backup.PrintCreateServerStatement(backupfile, tocfile, foreignServer, backup.ObjectMetadata{})
			testutils.ExpectEntry(tocfile.PredataEntries, 0, "", "", "foreignserver", "FOREIGN SERVER")
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE SERVER foreignserver
	FOREIGN DATA WRAPPER foreignwrapper
	OPTIONS (host 'localhost');`)
		})
		It("prints a foreign server with two options", func() {
			foreignServer := backup.ForeignServer{Oid: 1, Name: "foreignserver", ForeignDataWrapper: "foreignwrapper", Options: "host 'localhost', dbname 'testdb'"}
			backup.PrintCreateServerStatement(backupfile, tocfile, foreignServer, backup.ObjectMetadata{})
			testutils.ExpectEntry(tocfile.PredataEntries, 0, "", "", "foreignserver", "FOREIGN SERVER")
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE SERVER foreignserver
	FOREIGN DATA WRAPPER foreignwrapper
	OPTIONS (host 'localhost', dbname 'testdb');`)
		})
		It("prints a foreign server with type and version", func() {
			foreignServer := backup.ForeignServer{Oid: 1, Name: "foreignserver", Type: "server type", Version: "server version", ForeignDataWrapper: "foreignwrapper"}
			backup.PrintCreateServerStatement(backupfile, tocfile, foreignServer, backup.ObjectMetadata{})
			testutils.ExpectEntry(tocfile.PredataEntries, 0, "", "", "foreignserver", "FOREIGN SERVER")
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE SERVER foreignserver
	TYPE 'server type'
	VERSION 'server version'
	FOREIGN DATA WRAPPER foreignwrapper;`)
		})
	})
	Describe("PrintCreateuserMappingtatement", func() {
		It("prints a basic user mapping", func() {
			userMapping := backup.UserMapping{Oid: 1, User: "testrole", Server: "foreignserver"}
			backup.PrintCreateUserMappingStatement(backupfile, tocfile, userMapping)
			testutils.ExpectEntry(tocfile.PredataEntries, 0, "", "", "testrole ON foreignserver", "USER MAPPING")
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE USER MAPPING FOR testrole
	SERVER foreignserver;`)
		})
		It("prints a user mapping with one option", func() {
			userMapping := backup.UserMapping{Oid: 1, User: "testrole", Server: "foreignserver", Options: "host 'localhost'"}
			backup.PrintCreateUserMappingStatement(backupfile, tocfile, userMapping)
			testutils.ExpectEntry(tocfile.PredataEntries, 0, "", "", "testrole ON foreignserver", "USER MAPPING")
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE USER MAPPING FOR testrole
	SERVER foreignserver
	OPTIONS (host 'localhost');`)
		})
		It("prints a user mapping with two options", func() {
			userMapping := backup.UserMapping{Oid: 1, User: "testrole", Server: "foreignserver", Options: "host 'localhost', dbname 'testdb'"}
			backup.PrintCreateUserMappingStatement(backupfile, tocfile, userMapping)
			testutils.ExpectEntry(tocfile.PredataEntries, 0, "", "", "testrole ON foreignserver", "USER MAPPING")
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE USER MAPPING FOR testrole
	SERVER foreignserver
	OPTIONS (host 'localhost', dbname 'testdb');`)
		})
	})
})
