package backup_test

import (
	"fmt"

	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/toc"

	. "github.com/onsi/ginkgo/v2"
)

var _ = Describe("backup/predata_operators tests", func() {
	emptyMetadata := backup.ObjectMetadata{}
	BeforeEach(func() {
		tocfile, backupfile = testutils.InitializeTestTOC(buffer, "predata")
	})
	Describe("PrintCreateOperatorStatement", func() {
		var (
			operatorMetadata        backup.ObjectMetadata
			operator                backup.Operator
			createStatementFuncRepl string
		)
		BeforeEach(func() {
			operatorMetadata = testutils.DefaultMetadata(toc.OBJ_OPERATOR, false, true, true, false)
			operator = backup.Operator{Oid: 0, Schema: "public", Name: "##", Procedure: "public.path_inter", LeftArgType: "public.path", RightArgType: `public."PATH"`, CommutatorOp: "0", NegatorOp: "0", RestrictFunction: "-", JoinFunction: "-", CanHash: false, CanMerge: false}
			if connectionPool.Version.AtLeast("7") {
				createStatementFuncRepl = toc.OBJ_FUNCTION
			} else {
				createStatementFuncRepl = toc.OBJ_PROCEDURE
			}
		})
		It("prints a basic operator", func() {
			backup.PrintCreateOperatorStatement(backupfile, tocfile, operator, emptyMetadata)

			testutils.ExpectEntry(tocfile.PredataEntries, 0, "public", "", "##", toc.OBJ_OPERATOR)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, fmt.Sprintf(`CREATE OPERATOR public.## (
	%s = public.path_inter,
	LEFTARG = public.path,
	RIGHTARG = public."PATH"
);`, createStatementFuncRepl))
		})
		It("prints a full-featured operator with a comment and owner", func() {
			complexOperator := backup.Operator{Oid: 1, Schema: "testschema", Name: "##", Procedure: "public.path_inter", LeftArgType: "public.path", RightArgType: "public.path", CommutatorOp: "testschema.##", NegatorOp: "testschema.###", RestrictFunction: "eqsel(internal,oid,internal,integer)", JoinFunction: "eqjoinsel(internal,oid,internal,smallint)", CanHash: true, CanMerge: true}

			backup.PrintCreateOperatorStatement(backupfile, tocfile, complexOperator, operatorMetadata)

			expectedStatements := []string{
				fmt.Sprintf(`CREATE OPERATOR testschema.## (
	%s = public.path_inter,
	LEFTARG = public.path,
	RIGHTARG = public.path,
	COMMUTATOR = OPERATOR(testschema.##),
	NEGATOR = OPERATOR(testschema.###),
	RESTRICT = eqsel(internal,oid,internal,integer),
	JOIN = eqjoinsel(internal,oid,internal,smallint),
	HASHES,
	MERGES
);`, createStatementFuncRepl),
				"COMMENT ON OPERATOR testschema.## (public.path, public.path) IS 'This is an operator comment.';",
				"ALTER OPERATOR testschema.## (public.path, public.path) OWNER TO testrole;"}
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, expectedStatements...)
		})
		It("prints an operator with only a left argument", func() {
			operator.RightArgType = "-"

			backup.PrintCreateOperatorStatement(backupfile, tocfile, operator, emptyMetadata)

			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, fmt.Sprintf(`CREATE OPERATOR public.## (
	%s = public.path_inter,
	LEFTARG = public.path
);`, createStatementFuncRepl))
		})
		It("prints an operator with only a right argument", func() {
			operator.LeftArgType = "-"

			backup.PrintCreateOperatorStatement(backupfile, tocfile, operator, emptyMetadata)

			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, fmt.Sprintf(`CREATE OPERATOR public.## (
	%s = public.path_inter,
	RIGHTARG = public."PATH"
);`, createStatementFuncRepl))
		})
	})
	Describe("PrintCreateOperatorFamilyStatements", func() {
		It("prints a basic operator family", func() {
			operatorFamily := backup.OperatorFamily{Oid: 0, Schema: "public", Name: "testfam", IndexMethod: "hash"}

			backup.PrintCreateOperatorFamilyStatements(backupfile, tocfile, []backup.OperatorFamily{operatorFamily}, backup.MetadataMap{})

			testutils.ExpectEntry(tocfile.PredataEntries, 0, "public", "", "testfam", toc.OBJ_OPERATOR_FAMILY)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE OPERATOR FAMILY public.testfam USING hash;`)
		})
		It("prints an operator family with an owner and comment", func() {
			operatorFamily := backup.OperatorFamily{Oid: 1, Schema: "public", Name: "testfam", IndexMethod: "hash"}

			metadataMap := testutils.DefaultMetadataMap(toc.OBJ_OPERATOR_FAMILY, false, true, true, false)

			backup.PrintCreateOperatorFamilyStatements(backupfile, tocfile, []backup.OperatorFamily{operatorFamily}, metadataMap)

			expectedStatements := []string{"CREATE OPERATOR FAMILY public.testfam USING hash;",
				"COMMENT ON OPERATOR FAMILY public.testfam USING hash IS 'This is an operator family comment.';",
				"ALTER OPERATOR FAMILY public.testfam USING hash OWNER TO testrole;"}
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, expectedStatements...)
		})
	})
	Describe("PrintCreateOperatorClassStatement", func() {
		var (
			operatorClass backup.OperatorClass
			operator1     backup.OperatorClassOperator
			function1     backup.OperatorClassFunction
		)
		BeforeEach(func() {
			emptyMetadata = backup.ObjectMetadata{}
			operatorClass = backup.OperatorClass{Oid: 0, Schema: "public", Name: "testclass", FamilySchema: "public", FamilyName: "testclass", IndexMethod: "hash", Type: "uuid", Default: false, StorageType: "-", Operators: nil, Functions: nil}
			operator1 = backup.OperatorClassOperator{ClassOid: 0, StrategyNumber: 1, Operator: "=(uuid,uuid)", Recheck: false}
			function1 = backup.OperatorClassFunction{ClassOid: 0, SupportNumber: 1, FunctionName: "abs(integer)"}
		})
		It("prints a basic operator class", func() {
			backup.PrintCreateOperatorClassStatement(backupfile, tocfile, operatorClass, emptyMetadata)

			testutils.ExpectEntry(tocfile.PredataEntries, 0, "public", "", "testclass", toc.OBJ_OPERATOR_CLASS)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE OPERATOR CLASS public.testclass
	FOR TYPE uuid USING hash AS
	STORAGE uuid;`)
		})
		It("prints an operator class with default and family", func() {
			operatorClass.FamilyName = "testfam"
			operatorClass.Default = true

			backup.PrintCreateOperatorClassStatement(backupfile, tocfile, operatorClass, emptyMetadata)

			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE OPERATOR CLASS public.testclass
	DEFAULT FOR TYPE uuid USING hash FAMILY public.testfam AS
	STORAGE uuid;`)
		})
		It("prints an operator class with class and family in different schemas", func() {
			operatorClass.Schema = "schema1"
			operatorClass.FamilySchema = `"Schema2"`
			operatorClass.FamilyName = "testfam"

			backup.PrintCreateOperatorClassStatement(backupfile, tocfile, operatorClass, emptyMetadata)

			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE OPERATOR CLASS schema1.testclass
	FOR TYPE uuid USING hash FAMILY "Schema2".testfam AS
	STORAGE uuid;`)
		})
		It("prints an operator class with an operator", func() {
			operatorClass.Operators = []backup.OperatorClassOperator{operator1}

			backup.PrintCreateOperatorClassStatement(backupfile, tocfile, operatorClass, emptyMetadata)

			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE OPERATOR CLASS public.testclass
	FOR TYPE uuid USING hash AS
	OPERATOR 1 =(uuid,uuid);`)
		})
		It("prints an operator class with two operators and recheck", func() {
			operator2 := backup.OperatorClassOperator{ClassOid: 0, StrategyNumber: 2, Operator: ">(uuid,uuid)", Recheck: true}
			operatorClass.Operators = []backup.OperatorClassOperator{operator1, operator2}

			backup.PrintCreateOperatorClassStatement(backupfile, tocfile, operatorClass, emptyMetadata)

			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE OPERATOR CLASS public.testclass
	FOR TYPE uuid USING hash AS
	OPERATOR 1 =(uuid,uuid),
	OPERATOR 2 >(uuid,uuid) RECHECK;`)
		})
		It("prints an operator class with an operator and a sort family", func() {
			operator1.OrderByFamily = "schema.family_name"
			operatorClass.Operators = []backup.OperatorClassOperator{operator1}

			backup.PrintCreateOperatorClassStatement(backupfile, tocfile, operatorClass, emptyMetadata)

			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE OPERATOR CLASS public.testclass
	FOR TYPE uuid USING hash AS
	OPERATOR 1 =(uuid,uuid) FOR ORDER BY schema.family_name;`)
		})
		It("prints an operator class with a function", func() {
			operatorClass.Functions = []backup.OperatorClassFunction{function1}

			backup.PrintCreateOperatorClassStatement(backupfile, tocfile, operatorClass, emptyMetadata)

			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE OPERATOR CLASS public.testclass
	FOR TYPE uuid USING hash AS
	FUNCTION 1 abs(integer);`)
		})
		It("prints an operator class with a function that has left- and righttype", func() {
			function1.LeftType = "text"
			function1.RightType = "text"
			operatorClass.Functions = []backup.OperatorClassFunction{function1}

			backup.PrintCreateOperatorClassStatement(backupfile, tocfile, operatorClass, emptyMetadata)

			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE OPERATOR CLASS public.testclass
	FOR TYPE uuid USING hash AS
	FUNCTION 1 (text, text) abs(integer);`)
		})
		It("prints an operator class with a function and owner and comment", func() {
			operatorClass.Functions = []backup.OperatorClassFunction{function1}

			objMetadata := testutils.DefaultMetadata(toc.OBJ_OPERATOR_CLASS, false, true, true, false)
			backup.PrintCreateOperatorClassStatement(backupfile, tocfile, operatorClass, objMetadata)

			expectedStatements := []string{
				`CREATE OPERATOR CLASS public.testclass
	FOR TYPE uuid USING hash AS
	FUNCTION 1 abs(integer);`,
				"COMMENT ON OPERATOR CLASS public.testclass USING hash IS 'This is an operator class comment.';",
				"ALTER OPERATOR CLASS public.testclass USING hash OWNER TO testrole;"}
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, expectedStatements...)
		})
	})
})
