package integration

import (
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"

	"bytes"

	"github.com/greenplum-db/gpbackup/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup integration create statement tests", func() {

	var buffer *bytes.Buffer

	BeforeEach(func() {
		buffer = bytes.NewBuffer([]byte(""))
	})
	Describe("PrintCreateSchemaStatements", func() {
		It("creates a non public schema", func() {
			schemas := []utils.Schema{{0, "test_schema", "test comment", "testrole"}}

			backup.PrintCreateSchemaStatements(buffer, schemas)

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP SCHEMA test_schema")

			resultSchemas := backup.GetAllUserSchemas(connection)

			Expect(len(resultSchemas)).To(Equal(2))
			Expect(resultSchemas[0].SchemaName).To(Equal("public"))

			testutils.ExpectStructsToMatchExcluding(&resultSchemas[1], &schemas[0], []string{"SchemaOid"})

		})

		It("modifies the public schema", func() {
			schemas := []utils.Schema{{0, "public", "test comment", "testrole"}}

			backup.PrintCreateSchemaStatements(buffer, schemas)

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "ALTER SCHEMA public OWNER TO pivotal")
			defer testutils.AssertQueryRuns(connection, "COMMENT ON SCHEMA public IS 'standard public schema'")

			resultSchemas := backup.GetAllUserSchemas(connection)

			Expect(len(resultSchemas)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&resultSchemas[0], &schemas[0], []string{"SchemaOid"})
		})
	})

	Describe("PrintTypeStatements", func() {
		var (
			shellType         backup.TypeDefinition
			baseType          backup.TypeDefinition
			compositeTypeAtt1 backup.TypeDefinition
			compositeTypeAtt2 backup.TypeDefinition
			enumType          backup.TypeDefinition
			types             []backup.TypeDefinition
		)
		BeforeEach(func() {
			shellType = backup.TypeDefinition{Type: "p", TypeSchema: "public", TypeName: "shell_type"}
			baseType = backup.TypeDefinition{
				Type: "b", TypeSchema: "public", TypeName: "base_type", Input: "base_fn_in", Output: "base_fn_out", Receive: "-",
				Send: "-", ModIn: "-", ModOut: "-", InternalLength: 4, IsPassedByValue: true, Alignment: "i", Storage: "p",
				DefaultVal: "default", Element: "text", Delimiter: ";", Comment: "base type comment", Owner: "testrole",
			}
			compositeTypeAtt1 = backup.TypeDefinition{
				Type: "c", TypeSchema: "public", TypeName: "composite_type", Comment: "comment", Owner: "testrole",
				AttName: "att1", AttType: "text",
			}
			compositeTypeAtt2 = backup.TypeDefinition{
				Type: "c", TypeSchema: "public", TypeName: "composite_type", Comment: "comment", Owner: "testrole",
				AttName: "att2", AttType: "integer",
			}
			enumType = backup.TypeDefinition{
				Type: "e", TypeSchema: "public", TypeName: "enum_type", Comment: "comment", Owner: "testrole", EnumLabels: "'enum_labels'"}
			types = []backup.TypeDefinition{shellType, baseType, compositeTypeAtt1, compositeTypeAtt2, enumType}
		})

		It("creates shell types for base and shell types only", func() {
			backup.PrintShellTypeStatements(buffer, types)

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP TYPE shell_type")
			defer testutils.AssertQueryRuns(connection, "DROP TYPE base_type")

			resultTypes := backup.GetTypeDefinitions(connection)

			Expect(len(resultTypes)).To(Equal(2))
			Expect(resultTypes[0].TypeName).To(Equal("base_type"))
			Expect(resultTypes[1].TypeName).To(Equal("shell_type"))
		})

		It("creates composite and enum types", func() {
			backup.PrintCreateCompositeAndEnumTypeStatements(buffer, types)

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP TYPE composite_type")
			defer testutils.AssertQueryRuns(connection, "DROP TYPE enum_type")

			resultTypes := backup.GetTypeDefinitions(connection)

			Expect(len(resultTypes)).To(Equal(3))
			testutils.ExpectStructsToMatchIncluding(&resultTypes[0], &compositeTypeAtt1, []string{"Type", "TypeSchema", "TypeName", "Comment", "Owner", "AttName", "AttType"})
			testutils.ExpectStructsToMatchIncluding(&resultTypes[1], &compositeTypeAtt2, []string{"Type", "TypeSchema", "TypeName", "Comment", "Owner", "AttName", "AttType"})
			testutils.ExpectStructsToMatchIncluding(&resultTypes[2], &enumType, []string{"Type", "TypeSchema", "TypeName", "Comment", "Owner", "EnumLabels"})
		})

		It("creates base types", func() {
			backup.PrintCreateBaseTypeStatements(buffer, types)

			//Run queries to set up the database state so we can successfully create base types
			testutils.AssertQueryRuns(connection, "CREATE TYPE base_type")
			defer testutils.AssertQueryRuns(connection, "DROP TYPE base_type CASCADE")
			testutils.AssertQueryRuns(connection, "CREATE FUNCTION base_fn_in(cstring) RETURNS base_type AS 'boolin' LANGUAGE internal")
			testutils.AssertQueryRuns(connection, "CREATE FUNCTION base_fn_out(base_type) RETURNS cstring AS 'boolout' LANGUAGE internal")

			testutils.AssertQueryRuns(connection, buffer.String())

			resultTypes := backup.GetTypeDefinitions(connection)

			Expect(len(resultTypes)).To(Equal(1))
			testutils.ExpectStructsToMatch(&resultTypes[0], &baseType)
		})
	})
	Describe("PrintCreateLanguageStatements", func() {
		It("creates procedural languages", func() {
			funcInfoMap := map[uint32]backup.FunctionInfo{
				11907:  {"pg_catalog.plpgsql_validator", "oid"},
				11906:  {"pg_catalog.plpgsql_inline_handler", "internal"},
				11905:  {"pg_catalog.plpgsql_call_handler", ""},
				228851: {"pg_catalog.plpython_call_handler", ""},
				228852: {"pg_catalog.plpython_inline_handler", "internal"},
			}
			plpgsqlInfo := backup.QueryProceduralLanguage{"plpgsql", "testrole", true, true, 11905, 11906, 11907, "", ""}
			plpythonuInfo := backup.QueryProceduralLanguage{"plpythonu", "testrole", true, false, 228851, 228852, 0, "", "this is a language comment"}
			procLangs := []backup.QueryProceduralLanguage{plpgsqlInfo, plpythonuInfo}

			backup.PrintCreateLanguageStatements(buffer, procLangs, funcInfoMap)

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP LANGUAGE plpythonu")

			resultProcLangs := backup.GetProceduralLanguages(connection)

			Expect(len(resultProcLangs)).To(Equal(2))
			testutils.ExpectStructsToMatch(&resultProcLangs[0], &plpgsqlInfo)
			testutils.ExpectStructsToMatchExcluding(&resultProcLangs[1], &plpythonuInfo, []string{"Handler", "Inline"})
		})
	})
	Describe("PrintCreateFunctionStatements", func() {
		It("creates a function with a simple return type", func() {
			addFunction := backup.QueryFunctionDefinition{
				SchemaName: "public", FunctionName: "add", ReturnsSet: false, FunctionBody: "SELECT $1 + $2",
				BinaryPath: "", Arguments: "integer, integer", IdentArgs: "integer, integer", ResultType: "integer",
				Volatility: "v", IsStrict: false, IsSecurityDefiner: false, Config: "", Cost: 100, NumRows: 0, SqlUsage: "c",
				Language: "sql", Comment: "", Owner: "testrole",
			}

			backup.PrintCreateFunctionStatements(buffer, []backup.QueryFunctionDefinition{addFunction})

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION add(integer, integer)")

			resultFunctions := backup.GetFunctionDefinitions(connection)

			Expect(len(resultFunctions)).To(Equal(1))
			testutils.ExpectStructsToMatch(&resultFunctions[0], &addFunction)
		})
		It("creates a function that returns a set", func() {
			appendFunction := backup.QueryFunctionDefinition{
				SchemaName: "public", FunctionName: "append", ReturnsSet: true, FunctionBody: "SELECT ($1, $2)",
				BinaryPath: "", Arguments: "integer, integer", IdentArgs: "integer, integer", ResultType: "SETOF record",
				Volatility: "s", IsStrict: true, IsSecurityDefiner: true, Config: "SET search_path TO pg_temp", Cost: 200,
				NumRows: 200, SqlUsage: "m", Language: "sql", Comment: "this is a function comment", Owner: "testrole",
			}

			backup.PrintCreateFunctionStatements(buffer, []backup.QueryFunctionDefinition{appendFunction})

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION append(integer, integer)")

			resultFunctions := backup.GetFunctionDefinitions(connection)

			Expect(len(resultFunctions)).To(Equal(1))
			testutils.ExpectStructsToMatch(&resultFunctions[0], &appendFunction)
		})
		It("creates a function that returns a table", func() {
			dupFunction := backup.QueryFunctionDefinition{
				SchemaName: "public", FunctionName: "dup", ReturnsSet: true, FunctionBody: "SELECT $1, CAST($1 AS text) || ' is text'",
				BinaryPath: "", Arguments: "integer", IdentArgs: "integer", ResultType: "TABLE(f1 integer, f2 text)",
				Volatility: "v", IsStrict: false, IsSecurityDefiner: false, Config: "", Cost: 100, NumRows: 1000, SqlUsage: "c",
				Language: "sql", Comment: "", Owner: "testrole",
			}

			backup.PrintCreateFunctionStatements(buffer, []backup.QueryFunctionDefinition{dupFunction})

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION dup(integer)")

			resultFunctions := backup.GetFunctionDefinitions(connection)

			Expect(len(resultFunctions)).To(Equal(1))
			testutils.ExpectStructsToMatch(&resultFunctions[0], &dupFunction)
		})
	})
	Describe("PrintCreateAggregateStatements", func() {
		It("creates an aggregate", func() {
			aggregateDef := backup.QueryAggregateDefinition{
				SchemaName: "public", AggregateName: "agg_prefunc", Arguments: "numeric, numeric",
				IdentArgs: "numeric, numeric", TransitionFunction: 1, PreliminaryFunction: 2, FinalFunction: 0,
				SortOperator: 0, TransitionDataType: "numeric", InitialValue: "0", IsOrdered: false,
				Comment: "this is an aggregate comment", Owner: "testrole",
			}
			funcInfoMap := map[uint32]backup.FunctionInfo{
				1: {QualifiedName: "public.mysfunc_accum", Arguments: "numeric, numeric, numeric"},
				2: {QualifiedName: "public.mypre_accum", Arguments: "numeric, numeric"},
			}

			backup.PrintCreateAggregateStatements(buffer, []backup.QueryAggregateDefinition{aggregateDef}, funcInfoMap)

			//Run queries to set up the database state so we can successfully create an aggregate
			testutils.AssertQueryRuns(connection, `
			CREATE FUNCTION mysfunc_accum(numeric, numeric, numeric)
			   RETURNS numeric
			   AS 'select $1 + $2 + $3'
			   LANGUAGE SQL
			   IMMUTABLE
			   RETURNS NULL ON NULL INPUT;
			`)
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION mysfunc_accum(numeric, numeric, numeric)")
			testutils.AssertQueryRuns(connection, `
			CREATE FUNCTION mypre_accum(numeric, numeric)
			   RETURNS numeric
			   AS 'select $1 + $2'
			   LANGUAGE SQL
			   IMMUTABLE
			   RETURNS NULL ON NULL INPUT;
			`)
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION mypre_accum(numeric, numeric)")
			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP AGGREGATE agg_prefunc(numeric, numeric)")

			resultAggregates := backup.GetAggregateDefinitions(connection)
			Expect(len(resultAggregates)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&resultAggregates[0], &aggregateDef, []string{"TransitionFunction", "PreliminaryFunction"})
		})
	})
})
