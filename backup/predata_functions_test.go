package backup_test

import (
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup/predata_functions tests", func() {
	BeforeEach(func() {
		toc, backupfile = testutils.InitializeTestTOC(buffer, "predata")
	})
	Describe("Functions involved in printing CREATE FUNCTION statements", func() {
		var funcDef backup.Function
		funcDefault := backup.Function{Oid: 1, Schema: "public", Name: "func_name", ReturnsSet: false, FunctionBody: "add_two_ints", BinaryPath: "", Arguments: "integer, integer", IdentArgs: "integer, integer", ResultType: "integer", Volatility: "v", IsStrict: false, IsSecurityDefiner: false, Config: "", Cost: float32(1), NumRows: float32(0), DataAccess: "", Language: "internal", ExecLocation: "a"}
		BeforeEach(func() {
			funcDef = funcDefault
		})

		Describe("PrintCreateFunctionStatement", func() {
			var (
				funcMetadata backup.ObjectMetadata
			)
			BeforeEach(func() {
				funcMetadata = backup.ObjectMetadata{}
			})
			It("prints a function definition for an internal function without a binary path", func() {
				backup.PrintCreateFunctionStatement(backupfile, toc, funcDef, funcMetadata)
				testutils.ExpectEntry(toc.PredataEntries, 0, "public", "", "func_name(integer, integer)", "FUNCTION")
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE FUNCTION public.func_name(integer, integer) RETURNS integer AS
$$add_two_ints$$
LANGUAGE internal;`)
			})
			It("prints a function definition for a function that returns a set", func() {
				funcDef.ReturnsSet = true
				funcDef.ResultType = "SETOF integer"
				backup.PrintCreateFunctionStatement(backupfile, toc, funcDef, funcMetadata)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE FUNCTION public.func_name(integer, integer) RETURNS SETOF integer AS
$$add_two_ints$$
LANGUAGE internal;`)
			})
			It("prints a function definition for a function with permissions, an owner, and a comment", func() {
				funcMetadata := backup.ObjectMetadata{Privileges: []backup.ACL{testutils.DefaultACLForType("testrole", "FUNCTION")}, Owner: "testrole", Comment: "This is a function comment."}
				backup.PrintCreateFunctionStatement(backupfile, toc, funcDef, funcMetadata)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE FUNCTION public.func_name(integer, integer) RETURNS integer AS
$$add_two_ints$$
LANGUAGE internal;


COMMENT ON FUNCTION public.func_name(integer, integer) IS 'This is a function comment.';


ALTER FUNCTION public.func_name(integer, integer) OWNER TO testrole;


REVOKE ALL ON FUNCTION public.func_name(integer, integer) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.func_name(integer, integer) FROM testrole;
GRANT ALL ON FUNCTION public.func_name(integer, integer) TO testrole;`)
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
					Expect(buffer.Contents()).To(Equal([]byte{}))
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
			Context("Execlocation cases", func() {
				It("Default", func() {
					funcDef.ExecLocation = "a"
					backup.PrintFunctionModifiers(backupfile, funcDef)
					Expect(buffer.Contents()).To(Equal([]byte{}))
				})
				It("print 'm' as EXECUTE ON MASTER", func() {
					funcDef.ExecLocation = "m"
					backup.PrintFunctionModifiers(backupfile, funcDef)
					testhelper.ExpectRegexp(buffer, "EXECUTE ON MASTER")
				})
				It("print 's' as EXECUTE ON ALL SEGMENTS", func() {
					funcDef.ExecLocation = "s"
					backup.PrintFunctionModifiers(backupfile, funcDef)
					testhelper.ExpectRegexp(buffer, "EXECUTE ON ALL SEGMENTS")
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
					Expect(buffer.Contents()).To(Equal([]byte{}))
				})
				It("does not print 'COST 1' if Cost is set to 1 and language is internal", func() {
					funcDef.Cost = 1
					funcDef.Language = "internal"
					backup.PrintFunctionModifiers(backupfile, funcDef)
					Expect(buffer.Contents()).To(Equal([]byte{}))
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
					Expect(buffer.Contents()).To(Equal([]byte{}))
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
					Expect(buffer.Contents()).To(Equal([]byte{}))
				})
				It("does not print 'ROWS' if Rows is set to 0", func() {
					funcDef.NumRows = 0
					funcDef.ReturnsSet = true
					backup.PrintFunctionModifiers(backupfile, funcDef)
					Expect(buffer.Contents()).To(Equal([]byte{}))
				})
				It("does not print 'ROWS' if Rows is set to 1000", func() {
					funcDef.NumRows = 1000
					funcDef.ReturnsSet = true
					backup.PrintFunctionModifiers(backupfile, funcDef)
					Expect(buffer.Contents()).To(Equal([]byte{}))
				})
			})
			It("prints config statements if any are set", func() {
				funcDef.Config = "SET client_min_messages TO error"
				backup.PrintFunctionModifiers(backupfile, funcDef)
				testhelper.ExpectRegexp(buffer, "SET client_min_messages TO error")
			})
		})

	})
	Describe("PrintCreateAggregateStatements", func() {
		var (
			aggDefinition backup.Aggregate
			emptyMetadata backup.ObjectMetadata
			aggMetadata   backup.ObjectMetadata
		)
		funcInfoMap := map[uint32]backup.FunctionInfo{
			1: {QualifiedName: "public.mysfunc", Arguments: "integer"},
			2: {QualifiedName: "public.mypfunc", Arguments: "numeric, numeric"},
			3: {QualifiedName: "public.myffunc", Arguments: "text"},
			4: {QualifiedName: "pg_catalog.ordered_set_transition_multi", Arguments: `internal, VARIADIC "any"`},
			5: {QualifiedName: "pg_catalog.rank_final", Arguments: `internal, VARIADIC "any"`},
		}
		BeforeEach(func() {
			aggDefinition = backup.Aggregate{Oid: 1, Schema: "public", Name: "agg_name", Arguments: "integer, integer", IdentArgs: "integer, integer", TransitionFunction: 1, TransitionDataType: "integer", InitValIsNull: true, MInitValIsNull: true}
			emptyMetadata = backup.ObjectMetadata{}
			aggMetadata = backup.ObjectMetadata{Privileges: []backup.ACL{}, Owner: "testrole", Comment: "This is an aggregate comment."}
		})

		It("prints an aggregate definition for an unordered aggregate with no optional specifications", func() {
			backup.PrintCreateAggregateStatements(backupfile, toc, aggDefinition, funcInfoMap, emptyMetadata)
			testutils.ExpectEntry(toc.PredataEntries, 0, "public", "", "agg_name(integer, integer)", "AGGREGATE")
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE AGGREGATE public.agg_name(integer, integer) (
	SFUNC = public.mysfunc,
	STYPE = integer
);`)
		})
		It("prints an aggregate definition for an ordered aggregate with no optional specifications", func() {
			aggDefinition.IsOrdered = true
			backup.PrintCreateAggregateStatements(backupfile, toc, aggDefinition, funcInfoMap, emptyMetadata)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE ORDERED AGGREGATE public.agg_name(integer, integer) (
	SFUNC = public.mysfunc,
	STYPE = integer
);`)
		})
		It("prints an aggregate definition for an unordered aggregate with no arguments", func() {
			aggDefinition.Arguments = ""
			aggDefinition.IdentArgs = ""
			backup.PrintCreateAggregateStatements(backupfile, toc, aggDefinition, funcInfoMap, emptyMetadata)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE AGGREGATE public.agg_name(*) (
	SFUNC = public.mysfunc,
	STYPE = integer
);`)
		})
		It("prints an aggregate with a preliminary function", func() {
			aggDefinition.PreliminaryFunction = 2
			backup.PrintCreateAggregateStatements(backupfile, toc, aggDefinition, funcInfoMap, emptyMetadata)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE AGGREGATE public.agg_name(integer, integer) (
	SFUNC = public.mysfunc,
	STYPE = integer,
	PREFUNC = public.mypfunc
);`)
		})
		It("prints an aggregate with a combine function", func() {
			aggDefinition.CombineFunction = 2
			backup.PrintCreateAggregateStatements(backupfile, toc, aggDefinition, funcInfoMap, emptyMetadata)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE AGGREGATE public.agg_name(integer, integer) (
	SFUNC = public.mysfunc,
	STYPE = integer,
	COMBINEFUNC = public.mypfunc
);`)
		})
		It("prints an aggregate with a serial function", func() {
			aggDefinition.SerialFunction = 2
			backup.PrintCreateAggregateStatements(backupfile, toc, aggDefinition, funcInfoMap, emptyMetadata)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE AGGREGATE public.agg_name(integer, integer) (
	SFUNC = public.mysfunc,
	STYPE = integer,
	SERIALFUNC = public.mypfunc
);`)
		})
		It("prints an aggregate with a deserial function", func() {
			aggDefinition.DeserialFunction = 2
			backup.PrintCreateAggregateStatements(backupfile, toc, aggDefinition, funcInfoMap, emptyMetadata)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE AGGREGATE public.agg_name(integer, integer) (
	SFUNC = public.mysfunc,
	STYPE = integer,
	DESERIALFUNC = public.mypfunc
);`)
		})
		It("prints an aggregate with a final function", func() {
			aggDefinition.FinalFunction = 3
			backup.PrintCreateAggregateStatements(backupfile, toc, aggDefinition, funcInfoMap, emptyMetadata)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE AGGREGATE public.agg_name(integer, integer) (
	SFUNC = public.mysfunc,
	STYPE = integer,
	FINALFUNC = public.myffunc
);`)
		})
		It("prints an aggregate with a final function extra attribute", func() {
			aggDefinition.FinalFuncExtra = true
			backup.PrintCreateAggregateStatements(backupfile, toc, aggDefinition, funcInfoMap, emptyMetadata)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE AGGREGATE public.agg_name(integer, integer) (
	SFUNC = public.mysfunc,
	STYPE = integer,
	FINALFUNC_EXTRA
);`)
		})
		It("prints an aggregate with an initial condition", func() {
			aggDefinition.InitialValue = "0"
			aggDefinition.InitValIsNull = false
			backup.PrintCreateAggregateStatements(backupfile, toc, aggDefinition, funcInfoMap, emptyMetadata)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE AGGREGATE public.agg_name(integer, integer) (
	SFUNC = public.mysfunc,
	STYPE = integer,
	INITCOND = '0'
);`)
		})
		It("prints an aggregate with a sort operator", func() {
			aggDefinition.SortOperator = "+"
			aggDefinition.SortOperatorSchema = "myschema"
			backup.PrintCreateAggregateStatements(backupfile, toc, aggDefinition, funcInfoMap, emptyMetadata)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE AGGREGATE public.agg_name(integer, integer) (
	SFUNC = public.mysfunc,
	STYPE = integer,
	SORTOP = myschema."+"
);`)
		})
		It("prints an aggregate with a specified transition data size", func() {
			aggDefinition.TransitionDataSize = 1000
			backup.PrintCreateAggregateStatements(backupfile, toc, aggDefinition, funcInfoMap, emptyMetadata)
			testutils.ExpectEntry(toc.PredataEntries, 0, "public", "", "agg_name(integer, integer)", "AGGREGATE")
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE AGGREGATE public.agg_name(integer, integer) (
	SFUNC = public.mysfunc,
	STYPE = integer,
	SSPACE = 1000
);`)
		})
		It("prints an aggregate with a specified moving transition function", func() {
			aggDefinition.MTransitionFunction = 1
			backup.PrintCreateAggregateStatements(backupfile, toc, aggDefinition, funcInfoMap, emptyMetadata)
			testutils.ExpectEntry(toc.PredataEntries, 0, "public", "", "agg_name(integer, integer)", "AGGREGATE")
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE AGGREGATE public.agg_name(integer, integer) (
	SFUNC = public.mysfunc,
	STYPE = integer,
	MSFUNC = public.mysfunc
);`)
		})
		It("prints an aggregate with a specified moving inverse transition function", func() {
			aggDefinition.MInverseTransitionFunction = 1
			backup.PrintCreateAggregateStatements(backupfile, toc, aggDefinition, funcInfoMap, emptyMetadata)
			testutils.ExpectEntry(toc.PredataEntries, 0, "public", "", "agg_name(integer, integer)", "AGGREGATE")
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE AGGREGATE public.agg_name(integer, integer) (
	SFUNC = public.mysfunc,
	STYPE = integer,
	MINVFUNC = public.mysfunc
);`)
		})
		It("prints an aggregate with a specified moving state type", func() {
			aggDefinition.MTransitionDataType = "numeric"
			backup.PrintCreateAggregateStatements(backupfile, toc, aggDefinition, funcInfoMap, emptyMetadata)
			testutils.ExpectEntry(toc.PredataEntries, 0, "public", "", "agg_name(integer, integer)", "AGGREGATE")
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE AGGREGATE public.agg_name(integer, integer) (
	SFUNC = public.mysfunc,
	STYPE = integer,
	MSTYPE = numeric
);`)
		})
		It("prints an aggregate with a specified moving transition size", func() {
			aggDefinition.MTransitionDataSize = 100
			backup.PrintCreateAggregateStatements(backupfile, toc, aggDefinition, funcInfoMap, emptyMetadata)
			testutils.ExpectEntry(toc.PredataEntries, 0, "public", "", "agg_name(integer, integer)", "AGGREGATE")
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE AGGREGATE public.agg_name(integer, integer) (
	SFUNC = public.mysfunc,
	STYPE = integer,
	MSSPACE = 100
);`)
		})
		It("prints an aggregate with a specified moving final function", func() {
			aggDefinition.MFinalFunction = 3
			backup.PrintCreateAggregateStatements(backupfile, toc, aggDefinition, funcInfoMap, emptyMetadata)
			testutils.ExpectEntry(toc.PredataEntries, 0, "public", "", "agg_name(integer, integer)", "AGGREGATE")
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE AGGREGATE public.agg_name(integer, integer) (
	SFUNC = public.mysfunc,
	STYPE = integer,
	MFINALFUNC = public.myffunc
);`)
		})
		It("prints an aggregate with a moving final function extra attribute", func() {
			aggDefinition.MFinalFuncExtra = true
			backup.PrintCreateAggregateStatements(backupfile, toc, aggDefinition, funcInfoMap, emptyMetadata)
			testutils.ExpectEntry(toc.PredataEntries, 0, "public", "", "agg_name(integer, integer)", "AGGREGATE")
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE AGGREGATE public.agg_name(integer, integer) (
	SFUNC = public.mysfunc,
	STYPE = integer,
	MFINALFUNC_EXTRA
);`)
		})
		It("prints an aggregate with a moving initial condition", func() {
			aggDefinition.MInitialValue = "0"
			aggDefinition.MInitValIsNull = false
			backup.PrintCreateAggregateStatements(backupfile, toc, aggDefinition, funcInfoMap, emptyMetadata)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE AGGREGATE public.agg_name(integer, integer) (
	SFUNC = public.mysfunc,
	STYPE = integer,
	MINITCOND = '0'
);`)
		})
		It("prints an aggregate with multiple specifications", func() {
			aggDefinition.FinalFunction = 3
			aggDefinition.SortOperator = "~>~"
			aggDefinition.SortOperatorSchema = "myschema"
			backup.PrintCreateAggregateStatements(backupfile, toc, aggDefinition, funcInfoMap, emptyMetadata)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE AGGREGATE public.agg_name(integer, integer) (
	SFUNC = public.mysfunc,
	STYPE = integer,
	FINALFUNC = public.myffunc,
	SORTOP = myschema."~>~"
);`)
		})
		It("prints a hypothetical ordered-set aggregate", func() {
			complexAggDefinition := backup.Aggregate{
				Schema: "public", Name: "agg_hypo_ord", Arguments: `VARIADIC "any" ORDER BY VARIADIC "any"`,
				IdentArgs: `VARIADIC "any" ORDER BY VARIADIC "any"`, TransitionFunction: 4, FinalFunction: 5,
				TransitionDataType: "internal", InitValIsNull: true, MInitValIsNull: true, FinalFuncExtra: true, Hypothetical: true,
			}
			aggDefinition = complexAggDefinition
			backup.PrintCreateAggregateStatements(backupfile, toc, aggDefinition, funcInfoMap, emptyMetadata)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE AGGREGATE public.agg_hypo_ord(VARIADIC "any" ORDER BY VARIADIC "any") (
	SFUNC = pg_catalog.ordered_set_transition_multi,
	STYPE = internal,
	FINALFUNC = pg_catalog.rank_final,
	FINALFUNC_EXTRA,
	HYPOTHETICAL
);`)
		})
		It("prints an aggregate with owner and comment", func() {
			backup.PrintCreateAggregateStatements(backupfile, toc, aggDefinition, funcInfoMap, aggMetadata)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE AGGREGATE public.agg_name(integer, integer) (
	SFUNC = public.mysfunc,
	STYPE = integer
);


COMMENT ON AGGREGATE public.agg_name(integer, integer) IS 'This is an aggregate comment.';


ALTER AGGREGATE public.agg_name(integer, integer) OWNER TO testrole;`)
		})
		It("prints an aggregate with owner, comment, and no arguments", func() {
			aggDefinition.Arguments = ""
			aggDefinition.IdentArgs = ""
			backup.PrintCreateAggregateStatements(backupfile, toc, aggDefinition, funcInfoMap, aggMetadata)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE AGGREGATE public.agg_name(*) (
	SFUNC = public.mysfunc,
	STYPE = integer
);


COMMENT ON AGGREGATE public.agg_name(*) IS 'This is an aggregate comment.';


ALTER AGGREGATE public.agg_name(*) OWNER TO testrole;`)
		})
	})
	Describe("PrintCreateCastStatement", func() {
		emptyMetadata := backup.ObjectMetadata{}
		It("prints an explicit cast with a function", func() {
			castDef := backup.Cast{Oid: 1, SourceTypeFQN: "src", TargetTypeFQN: "dst", FunctionSchema: "public", FunctionName: "cast_func", FunctionArgs: "integer, integer", CastContext: "e", CastMethod: "f"}
			backup.PrintCreateCastStatement(backupfile, toc, castDef, emptyMetadata)
			testutils.ExpectEntry(toc.PredataEntries, 0, "public", "", "(src AS dst)", "CAST")
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE CAST (src AS dst)
	WITH FUNCTION public.cast_func(integer, integer);`)
		})
		It("prints an implicit cast with a function", func() {
			castDef := backup.Cast{Oid: 1, SourceTypeFQN: "src", TargetTypeFQN: "dst", FunctionSchema: "public", FunctionName: "cast_func", FunctionArgs: "integer, integer", CastContext: "i", CastMethod: "f"}
			backup.PrintCreateCastStatement(backupfile, toc, castDef, emptyMetadata)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE CAST (src AS dst)
	WITH FUNCTION public.cast_func(integer, integer)
AS IMPLICIT;`)
		})
		It("prints an assignment cast with a function", func() {
			castDef := backup.Cast{Oid: 1, SourceTypeFQN: "src", TargetTypeFQN: "dst", FunctionSchema: "public", FunctionName: "cast_func", FunctionArgs: "integer, integer", CastContext: "a", CastMethod: "f"}
			backup.PrintCreateCastStatement(backupfile, toc, castDef, emptyMetadata)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE CAST (src AS dst)
	WITH FUNCTION public.cast_func(integer, integer)
AS ASSIGNMENT;`)
		})
		It("prints an explicit cast without a function", func() {
			castDef := backup.Cast{Oid: 1, SourceTypeFQN: "src", TargetTypeFQN: "dst", FunctionSchema: "", FunctionName: "", FunctionArgs: "", CastContext: "e", CastMethod: "b"}
			backup.PrintCreateCastStatement(backupfile, toc, castDef, emptyMetadata)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE CAST (src AS dst)
	WITHOUT FUNCTION;`)
		})
		It("prints an implicit cast without a function", func() {
			castDef := backup.Cast{Oid: 1, SourceTypeFQN: "src", TargetTypeFQN: "dst", FunctionSchema: "", FunctionName: "", FunctionArgs: "", CastContext: "i", CastMethod: "b"}
			backup.PrintCreateCastStatement(backupfile, toc, castDef, emptyMetadata)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE CAST (src AS dst)
	WITHOUT FUNCTION
AS IMPLICIT;`)
		})
		It("prints an assignment cast without a function", func() {
			castDef := backup.Cast{Oid: 1, SourceTypeFQN: "src", TargetTypeFQN: "dst", FunctionSchema: "", FunctionName: "", FunctionArgs: "", CastContext: "a", CastMethod: "b"}
			backup.PrintCreateCastStatement(backupfile, toc, castDef, emptyMetadata)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE CAST (src AS dst)
	WITHOUT FUNCTION
AS ASSIGNMENT;`)
		})
		It("prints an inout cast", func() {
			castDef := backup.Cast{Oid: 1, SourceTypeFQN: "src", TargetTypeFQN: "dst", FunctionSchema: "", FunctionName: "", FunctionArgs: "", CastContext: "e", CastMethod: "i"}
			backup.PrintCreateCastStatement(backupfile, toc, castDef, emptyMetadata)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE CAST (src AS dst)
	WITH INOUT;`)
		})
		It("prints a cast with a comment", func() {
			castDef := backup.Cast{Oid: 1, SourceTypeFQN: "src", TargetTypeFQN: "dst", FunctionSchema: "", FunctionName: "", FunctionArgs: "", CastContext: "e", CastMethod: "b"}
			castMetadata := testutils.DefaultMetadata("CAST", false, false, true)
			backup.PrintCreateCastStatement(backupfile, toc, castDef, castMetadata)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE CAST (src AS dst)
	WITHOUT FUNCTION;

COMMENT ON CAST (src AS dst) IS 'This is a cast comment.';`)
		})
	})
	Describe("PrintCreateExtensionStatement", func() {
		emptyMetadataMap := backup.MetadataMap{}
		It("prints a create extension statement", func() {
			extensionDef := backup.Extension{Oid: 1, Name: "extension1", Schema: "schema1"}
			backup.PrintCreateExtensionStatements(backupfile, toc, []backup.Extension{extensionDef}, emptyMetadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `SET search_path=schema1,pg_catalog;
CREATE EXTENSION IF NOT EXISTS extension1 WITH SCHEMA schema1;
SET search_path=pg_catalog;`)
		})
		It("prints a create extension statement with a comment", func() {
			extensionDef := backup.Extension{Oid: 1, Name: "extension1", Schema: "schema1"}
			extensionMetadataMap := testutils.DefaultMetadataMap("EXTENSION", false, false, true)
			backup.PrintCreateExtensionStatements(backupfile, toc, []backup.Extension{extensionDef}, extensionMetadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `SET search_path=schema1,pg_catalog;
CREATE EXTENSION IF NOT EXISTS extension1 WITH SCHEMA schema1;
SET search_path=pg_catalog;

COMMENT ON EXTENSION extension1 IS 'This is an extension comment.';`)
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
			1: {QualifiedName: "pg_catalog.plperl_call_handler", Arguments: "", IsInternal: true},
			2: {QualifiedName: "pg_catalog.plperl_inline_handler", Arguments: "internal", IsInternal: true},
			3: {QualifiedName: "pg_catalog.plperl_validator", Arguments: "oid", IsInternal: true},
			4: {QualifiedName: "pg_catalog.plpython_call_handler", Arguments: "", IsInternal: true},
		}
		emptyMetadataMap := backup.MetadataMap{}

		It("prints untrusted language with a handler only", func() {
			langs := []backup.ProceduralLanguage{plUntrustedHandlerOnly}

			backup.PrintCreateLanguageStatements(backupfile, toc, langs, funcInfoMap, emptyMetadataMap)
			testutils.ExpectEntry(toc.PredataEntries, 0, "", "", "plpythonu", "PROCEDURAL LANGUAGE")
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE PROCEDURAL LANGUAGE plpythonu HANDLER pg_catalog.plpython_call_handler;
ALTER FUNCTION pg_catalog.plpython_call_handler() OWNER TO testrole;`)
		})
		It("prints trusted language with handler, inline, and validator", func() {
			langs := []backup.ProceduralLanguage{plAllFields}

			backup.PrintCreateLanguageStatements(backupfile, toc, langs, funcInfoMap, emptyMetadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TRUSTED PROCEDURAL LANGUAGE plperl HANDLER pg_catalog.plperl_call_handler INLINE pg_catalog.plperl_inline_handler VALIDATOR pg_catalog.plperl_validator;
ALTER FUNCTION pg_catalog.plperl_call_handler() OWNER TO testrole;
ALTER FUNCTION pg_catalog.plperl_inline_handler(internal) OWNER TO testrole;
ALTER FUNCTION pg_catalog.plperl_validator(oid) OWNER TO testrole;`)
		})
		It("prints multiple create language statements", func() {
			langs := []backup.ProceduralLanguage{plUntrustedHandlerOnly, plAllFields}

			backup.PrintCreateLanguageStatements(backupfile, toc, langs, funcInfoMap, emptyMetadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE PROCEDURAL LANGUAGE plpythonu HANDLER pg_catalog.plpython_call_handler;
ALTER FUNCTION pg_catalog.plpython_call_handler() OWNER TO testrole;`, `CREATE TRUSTED PROCEDURAL LANGUAGE plperl HANDLER pg_catalog.plperl_call_handler INLINE pg_catalog.plperl_inline_handler VALIDATOR pg_catalog.plperl_validator;
ALTER FUNCTION pg_catalog.plperl_call_handler() OWNER TO testrole;
ALTER FUNCTION pg_catalog.plperl_inline_handler(internal) OWNER TO testrole;
ALTER FUNCTION pg_catalog.plperl_validator(oid) OWNER TO testrole;`)
		})
		It("prints a language with privileges, an owner, and a comment", func() {
			langs := []backup.ProceduralLanguage{plComment}
			langMetadataMap := testutils.DefaultMetadataMap("LANGUAGE", true, true, true)

			backup.PrintCreateLanguageStatements(backupfile, toc, langs, funcInfoMap, langMetadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE PROCEDURAL LANGUAGE plpythonu HANDLER pg_catalog.plpython_call_handler;
ALTER FUNCTION pg_catalog.plpython_call_handler() OWNER TO testrole;

COMMENT ON LANGUAGE plpythonu IS 'This is a language comment.';


ALTER LANGUAGE plpythonu OWNER TO testrole;


REVOKE ALL ON LANGUAGE plpythonu FROM PUBLIC;
REVOKE ALL ON LANGUAGE plpythonu FROM testrole;
GRANT ALL ON LANGUAGE plpythonu TO testrole;`)
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
			backup.PrintCreateConversionStatements(backupfile, toc, conversions, metadataMap)
			testutils.ExpectEntry(toc.PredataEntries, 0, "public", "", "conv_one", "CONVERSION")
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE CONVERSION public.conv_one FOR 'UTF8' TO 'LATIN1' FROM public.converter;`)
		})
		It("prints a default conversion", func() {
			conversions := []backup.Conversion{convTwo}
			backup.PrintCreateConversionStatements(backupfile, toc, conversions, metadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE DEFAULT CONVERSION public.conv_two FOR 'UTF8' TO 'LATIN1' FROM public.converter;`)
		})
		It("prints multiple create conversion statements", func() {
			conversions := []backup.Conversion{convOne, convTwo}
			backup.PrintCreateConversionStatements(backupfile, toc, conversions, metadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer,
				`CREATE CONVERSION public.conv_one FOR 'UTF8' TO 'LATIN1' FROM public.converter;`,
				`CREATE DEFAULT CONVERSION public.conv_two FOR 'UTF8' TO 'LATIN1' FROM public.converter;`)
		})
		It("prints a conversion with an owner and a comment", func() {
			conversions := []backup.Conversion{convOne}
			metadataMap = testutils.DefaultMetadataMap("CONVERSION", false, true, true)
			backup.PrintCreateConversionStatements(backupfile, toc, conversions, metadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE CONVERSION public.conv_one FOR 'UTF8' TO 'LATIN1' FROM public.converter;

COMMENT ON CONVERSION public.conv_one IS 'This is a conversion comment.';


ALTER CONVERSION public.conv_one OWNER TO testrole;`)
		})
	})
	Describe("PrintCreateForeignDataWrapperStatements", func() {
		funcInfoMap := map[uint32]backup.FunctionInfo{
			1: {QualifiedName: "pg_catalog.postgresql_fdw_handler", Arguments: "", IsInternal: true},
			2: {QualifiedName: "pg_catalog.postgresql_fdw_validator", Arguments: "", IsInternal: true},
		}
		It("prints a basic foreign data wrapper", func() {
			foreignDataWrappers := []backup.ForeignDataWrapper{{Oid: 1, Name: "foreigndata"}}
			backup.PrintCreateForeignDataWrapperStatements(backupfile, toc, foreignDataWrappers, funcInfoMap, backup.MetadataMap{})
			testutils.ExpectEntry(toc.PredataEntries, 0, "", "", "foreigndata", "FOREIGN DATA WRAPPER")
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE FOREIGN DATA WRAPPER foreigndata;`)
		})
		It("prints a foreign data wrapper with a handler", func() {
			foreignDataWrappers := []backup.ForeignDataWrapper{{Name: "foreigndata", Handler: 1}}
			backup.PrintCreateForeignDataWrapperStatements(backupfile, toc, foreignDataWrappers, funcInfoMap, backup.MetadataMap{})
			testutils.ExpectEntry(toc.PredataEntries, 0, "", "", "foreigndata", "FOREIGN DATA WRAPPER")
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE FOREIGN DATA WRAPPER foreigndata
	HANDLER pg_catalog.postgresql_fdw_handler;`)
		})
		It("prints a foreign data wrapper with a validator", func() {
			foreignDataWrappers := []backup.ForeignDataWrapper{{Name: "foreigndata", Validator: 2}}
			backup.PrintCreateForeignDataWrapperStatements(backupfile, toc, foreignDataWrappers, funcInfoMap, backup.MetadataMap{})
			testutils.ExpectEntry(toc.PredataEntries, 0, "", "", "foreigndata", "FOREIGN DATA WRAPPER")
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE FOREIGN DATA WRAPPER foreigndata
	VALIDATOR pg_catalog.postgresql_fdw_validator;`)
		})
		It("prints a foreign data wrapper with one option", func() {
			foreignDataWrappers := []backup.ForeignDataWrapper{{Name: "foreigndata", Options: "debug 'true'"}}
			backup.PrintCreateForeignDataWrapperStatements(backupfile, toc, foreignDataWrappers, funcInfoMap, backup.MetadataMap{})
			testutils.ExpectEntry(toc.PredataEntries, 0, "", "", "foreigndata", "FOREIGN DATA WRAPPER")
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE FOREIGN DATA WRAPPER foreigndata
	OPTIONS (debug 'true');`)
		})
		It("prints a foreign data wrapper with two options", func() {
			foreignDataWrappers := []backup.ForeignDataWrapper{{Name: "foreigndata", Options: "debug 'true', host 'localhost'"}}
			backup.PrintCreateForeignDataWrapperStatements(backupfile, toc, foreignDataWrappers, funcInfoMap, backup.MetadataMap{})
			testutils.ExpectEntry(toc.PredataEntries, 0, "", "", "foreigndata", "FOREIGN DATA WRAPPER")
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE FOREIGN DATA WRAPPER foreigndata
	OPTIONS (debug 'true', host 'localhost');`)
		})
	})
	Describe("PrintCreateServerStatements", func() {
		It("prints a basic foreign server", func() {
			foreignServers := []backup.ForeignServer{{Oid: 1, Name: "foreignserver", ForeignDataWrapper: "foreignwrapper"}}
			backup.PrintCreateServerStatements(backupfile, toc, foreignServers, backup.MetadataMap{})
			testutils.ExpectEntry(toc.PredataEntries, 0, "", "", "foreignserver", "FOREIGN SERVER")
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE SERVER foreignserver
	FOREIGN DATA WRAPPER foreignwrapper;`)
		})
		It("prints a foreign server with one option", func() {
			foreignServers := []backup.ForeignServer{{Oid: 1, Name: "foreignserver", ForeignDataWrapper: "foreignwrapper", Options: "host 'localhost'"}}
			backup.PrintCreateServerStatements(backupfile, toc, foreignServers, backup.MetadataMap{})
			testutils.ExpectEntry(toc.PredataEntries, 0, "", "", "foreignserver", "FOREIGN SERVER")
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE SERVER foreignserver
	FOREIGN DATA WRAPPER foreignwrapper
	OPTIONS (host 'localhost');`)
		})
		It("prints a foreign server with two options", func() {
			foreignServers := []backup.ForeignServer{{Oid: 1, Name: "foreignserver", ForeignDataWrapper: "foreignwrapper", Options: "host 'localhost', dbname 'testdb'"}}
			backup.PrintCreateServerStatements(backupfile, toc, foreignServers, backup.MetadataMap{})
			testutils.ExpectEntry(toc.PredataEntries, 0, "", "", "foreignserver", "FOREIGN SERVER")
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE SERVER foreignserver
	FOREIGN DATA WRAPPER foreignwrapper
	OPTIONS (host 'localhost', dbname 'testdb');`)
		})
		It("prints a foreign server with type and version", func() {
			foreignServers := []backup.ForeignServer{{Oid: 1, Name: "foreignserver", Type: "server type", Version: "server version", ForeignDataWrapper: "foreignwrapper"}}
			backup.PrintCreateServerStatements(backupfile, toc, foreignServers, backup.MetadataMap{})
			testutils.ExpectEntry(toc.PredataEntries, 0, "", "", "foreignserver", "FOREIGN SERVER")
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE SERVER foreignserver
	TYPE 'server type'
	VERSION 'server version'
	FOREIGN DATA WRAPPER foreignwrapper;`)
		})
	})
	Describe("PrintCreateUserMappingStatements", func() {
		It("prints a basic user mapping", func() {
			userMappings := []backup.UserMapping{{Oid: 1, User: "testrole", Server: "foreignserver"}}
			backup.PrintCreateUserMappingStatements(backupfile, toc, userMappings)
			testutils.ExpectEntry(toc.PredataEntries, 0, "", "", "testrole ON foreignserver", "USER MAPPING")
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE USER MAPPING FOR testrole
	SERVER foreignserver;`)
		})
		It("prints a user mapping with one option", func() {
			userMappings := []backup.UserMapping{{Oid: 1, User: "testrole", Server: "foreignserver", Options: "host 'localhost'"}}
			backup.PrintCreateUserMappingStatements(backupfile, toc, userMappings)
			testutils.ExpectEntry(toc.PredataEntries, 0, "", "", "testrole ON foreignserver", "USER MAPPING")
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE USER MAPPING FOR testrole
	SERVER foreignserver
	OPTIONS (host 'localhost');`)
		})
		It("prints a user mapping with two options", func() {
			userMappings := []backup.UserMapping{{Oid: 1, User: "testrole", Server: "foreignserver", Options: "host 'localhost', dbname 'testdb'"}}
			backup.PrintCreateUserMappingStatements(backupfile, toc, userMappings)
			testutils.ExpectEntry(toc.PredataEntries, 0, "", "", "testrole ON foreignserver", "USER MAPPING")
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE USER MAPPING FOR testrole
	SERVER foreignserver
	OPTIONS (host 'localhost', dbname 'testdb');`)
		})
	})
})
