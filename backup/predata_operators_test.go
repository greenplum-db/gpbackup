package backup_test

import (
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"

	. "github.com/onsi/ginkgo"
)

var _ = Describe("backup/predata_operators tests", func() {
	BeforeEach(func() {
		toc, backupfile = testutils.InitializeTestTOC(buffer, "predata")
	})
	Describe("PrintCreateOperatorStatements", func() {
		It("prints a basic operator", func() {
			operator := backup.Operator{Oid: 0, Schema: "public", Name: "##", Procedure: "public.path_inter", LeftArgType: "public.path", RightArgType: "public.path", CommutatorOp: "0", NegatorOp: "0", RestrictFunction: "-", JoinFunction: "-", CanHash: false, CanMerge: false}

			backup.PrintCreateOperatorStatements(backupfile, toc, []backup.Operator{operator}, backup.MetadataMap{})

			testutils.ExpectEntry(toc.PredataEntries, 0, "public", "", "##", "OPERATOR")
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE OPERATOR public.## (
	PROCEDURE = public.path_inter,
	LEFTARG = public.path,
	RIGHTARG = public.path
);`)
		})
		It("prints a full-featured operator", func() {
			operator := backup.Operator{Oid: 1, Schema: "testschema", Name: "##", Procedure: "public.path_inter", LeftArgType: "public.path", RightArgType: "public.path", CommutatorOp: "testschema.##", NegatorOp: "testschema.###", RestrictFunction: "eqsel(internal,oid,internal,integer)", JoinFunction: "eqjoinsel(internal,oid,internal,smallint)", CanHash: true, CanMerge: true}

			metadataMap := testutils.DefaultMetadataMap("OPERATOR", false, true, true)

			backup.PrintCreateOperatorStatements(backupfile, toc, []backup.Operator{operator}, metadataMap)

			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE OPERATOR testschema.## (
	PROCEDURE = public.path_inter,
	LEFTARG = public.path,
	RIGHTARG = public.path,
	COMMUTATOR = OPERATOR(testschema.##),
	NEGATOR = OPERATOR(testschema.###),
	RESTRICT = eqsel(internal,oid,internal,integer),
	JOIN = eqjoinsel(internal,oid,internal,smallint),
	HASHES,
	MERGES
);

COMMENT ON OPERATOR testschema.## (public.path, public.path) IS 'This is an operator comment.';


ALTER OPERATOR testschema.## (public.path, public.path) OWNER TO testrole;`)
		})
		It("prints an operator with only a left argument", func() {
			operator := backup.Operator{Oid: 1, Schema: "public", Name: "##", Procedure: "public.path_inter", LeftArgType: "public.path", RightArgType: "-", CommutatorOp: "0", NegatorOp: "0", RestrictFunction: "-", JoinFunction: "-", CanHash: false, CanMerge: false}

			metadataMap := testutils.DefaultMetadataMap("OPERATOR", false, true, true)

			backup.PrintCreateOperatorStatements(backupfile, toc, []backup.Operator{operator}, metadataMap)

			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE OPERATOR public.## (
	PROCEDURE = public.path_inter,
	LEFTARG = public.path
);

COMMENT ON OPERATOR public.## (public.path, NONE) IS 'This is an operator comment.';


ALTER OPERATOR public.## (public.path, NONE) OWNER TO testrole;`)
		})
		It("prints an operator with only a right argument", func() {
			operator := backup.Operator{Oid: 1, Schema: "public", Name: "##", Procedure: "public.path_inter", LeftArgType: "-", RightArgType: "public.\"PATH\"", CommutatorOp: "0", NegatorOp: "0", RestrictFunction: "-", JoinFunction: "-", CanHash: false, CanMerge: false}

			metadataMap := testutils.DefaultMetadataMap("OPERATOR", false, true, true)

			backup.PrintCreateOperatorStatements(backupfile, toc, []backup.Operator{operator}, metadataMap)

			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE OPERATOR public.## (
	PROCEDURE = public.path_inter,
	RIGHTARG = public."PATH"
);

COMMENT ON OPERATOR public.## (NONE, public."PATH") IS 'This is an operator comment.';


ALTER OPERATOR public.## (NONE, public."PATH") OWNER TO testrole;`)
		})
	})
	Describe("PrintCreateOperatorFamilyStatements", func() {
		It("prints a basic operator family", func() {
			operatorFamily := backup.OperatorFamily{Oid: 0, Schema: "public", Name: "testfam", IndexMethod: "hash"}

			backup.PrintCreateOperatorFamilyStatements(backupfile, toc, []backup.OperatorFamily{operatorFamily}, backup.MetadataMap{})

			testutils.ExpectEntry(toc.PredataEntries, 0, "public", "", "testfam", "OPERATOR FAMILY")
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE OPERATOR FAMILY public.testfam USING hash;`)
		})
		It("prints an operator family with an owner and comment", func() {
			operatorFamily := backup.OperatorFamily{Oid: 1, Schema: "public", Name: "testfam", IndexMethod: "hash"}

			metadataMap := testutils.DefaultMetadataMap("OPERATOR FAMILY", false, true, true)

			backup.PrintCreateOperatorFamilyStatements(backupfile, toc, []backup.OperatorFamily{operatorFamily}, metadataMap)

			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE OPERATOR FAMILY public.testfam USING hash;

COMMENT ON OPERATOR FAMILY public.testfam USING hash IS 'This is an operator family comment.';


ALTER OPERATOR FAMILY public.testfam USING hash OWNER TO testrole;`)
		})
	})
	Describe("PrintCreateOperatorClassStatements", func() {
		It("prints a basic operator class", func() {
			operatorClass := backup.OperatorClass{Oid: 0, Schema: "public", Name: "testclass", FamilySchema: "public", FamilyName: "testclass", IndexMethod: "hash", Type: "uuid", Default: false, StorageType: "-", Operators: nil, Functions: nil}

			backup.PrintCreateOperatorClassStatements(backupfile, toc, []backup.OperatorClass{operatorClass}, backup.MetadataMap{})

			testutils.ExpectEntry(toc.PredataEntries, 0, "public", "", "testclass", "OPERATOR CLASS")
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE OPERATOR CLASS public.testclass
	FOR TYPE uuid USING hash AS
	STORAGE uuid;`)
		})
		It("prints an operator class with default and family", func() {
			operatorClass := backup.OperatorClass{Oid: 0, Schema: "public", Name: "testclass", FamilySchema: "public", FamilyName: "testfam", IndexMethod: "hash", Type: "uuid", Default: true, StorageType: "-", Operators: nil, Functions: nil}

			backup.PrintCreateOperatorClassStatements(backupfile, toc, []backup.OperatorClass{operatorClass}, backup.MetadataMap{})

			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE OPERATOR CLASS public.testclass
	DEFAULT FOR TYPE uuid USING hash FAMILY public.testfam AS
	STORAGE uuid;`)
		})
		It("prints an operator class with class and family in different schemas", func() {
			operatorClass := backup.OperatorClass{Oid: 0, Schema: "schema1", Name: "testclass", FamilySchema: `"Schema2"`, FamilyName: "testfam", IndexMethod: "hash", Type: "uuid", Default: true, StorageType: "-", Operators: nil, Functions: nil}

			backup.PrintCreateOperatorClassStatements(backupfile, toc, []backup.OperatorClass{operatorClass}, backup.MetadataMap{})

			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE OPERATOR CLASS schema1.testclass
	DEFAULT FOR TYPE uuid USING hash FAMILY "Schema2".testfam AS
	STORAGE uuid;`)
		})
		It("prints an operator class with an operator", func() {
			operatorClass := backup.OperatorClass{Oid: 0, Schema: "public", Name: "testclass", FamilySchema: "public", FamilyName: "testclass", IndexMethod: "hash", Type: "uuid", Default: false, StorageType: "-", Operators: nil, Functions: nil}
			operatorClass.Operators = []backup.OperatorClassOperator{{ClassOid: 0, StrategyNumber: 1, Operator: "=(uuid,uuid)", Recheck: false}}

			backup.PrintCreateOperatorClassStatements(backupfile, toc, []backup.OperatorClass{operatorClass}, backup.MetadataMap{})

			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE OPERATOR CLASS public.testclass
	FOR TYPE uuid USING hash AS
	OPERATOR 1 =(uuid,uuid);`)
		})
		It("prints an operator class with two operators and recheck", func() {
			operatorClass := backup.OperatorClass{Oid: 0, Schema: "public", Name: "testclass", FamilySchema: "public", FamilyName: "testclass", IndexMethod: "hash", Type: "uuid", Default: false, StorageType: "-", Operators: nil, Functions: nil}
			operatorClass.Operators = []backup.OperatorClassOperator{{ClassOid: 0, StrategyNumber: 1, Operator: "=(uuid,uuid)", Recheck: true}, {ClassOid: 0, StrategyNumber: 2, Operator: ">(uuid,uuid)", Recheck: false}}

			backup.PrintCreateOperatorClassStatements(backupfile, toc, []backup.OperatorClass{operatorClass}, backup.MetadataMap{})

			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE OPERATOR CLASS public.testclass
	FOR TYPE uuid USING hash AS
	OPERATOR 1 =(uuid,uuid) RECHECK,
	OPERATOR 2 >(uuid,uuid);`)
		})
		It("prints an operator class with a function", func() {
			operatorClass := backup.OperatorClass{Oid: 0, Schema: "public", Name: "testclass", FamilySchema: "public", FamilyName: "testclass", IndexMethod: "hash", Type: "uuid", Default: false, StorageType: "-", Operators: nil, Functions: nil}
			operatorClass.Functions = []backup.OperatorClassFunction{{ClassOid: 0, SupportNumber: 1, FunctionName: "abs(integer)"}}

			backup.PrintCreateOperatorClassStatements(backupfile, toc, []backup.OperatorClass{operatorClass}, backup.MetadataMap{})

			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE OPERATOR CLASS public.testclass
	FOR TYPE uuid USING hash AS
	FUNCTION 1 abs(integer);`)
		})
	})
})
