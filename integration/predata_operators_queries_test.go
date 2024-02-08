package integration

import (
	"github.com/greenplum-db/gp-common-go-libs/structmatcher"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/options"
	"github.com/greenplum-db/gpbackup/testutils"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup integration tests", func() {
	Describe("GetOperators", func() {
		It("returns a slice of operators", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE OPERATOR public.## (LEFTARG = bigint, PROCEDURE = numeric_fac)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP OPERATOR public.## (bigint, NONE)")

			expectedOperator := backup.Operator{Oid: 0, Schema: "public", Name: "##", Procedure: "numeric_fac", LeftArgType: "bigint", RightArgType: "-", CommutatorOp: "0", NegatorOp: "0", RestrictFunction: "-", JoinFunction: "-", CanHash: false, CanMerge: false}

			results := backup.GetOperators(connectionPool)

			Expect(results).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&expectedOperator, &results[0], "Oid")
		})
		It("returns a slice of operators with a full-featured operator", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE SCHEMA testschema")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA testschema")

			testhelper.AssertQueryRuns(connectionPool, `CREATE FUNCTION testschema."testFunc"(path,path) RETURNS BOOLEAN AS 'SELECT true' LANGUAGE SQL IMMUTABLE`)
			defer testhelper.AssertQueryRuns(connectionPool, `DROP FUNCTION testschema."testFunc"(path,path)`)

			testhelper.AssertQueryRuns(connectionPool, `
			CREATE OPERATOR testschema.## (
				LEFTARG = path,
				RIGHTARG = path,
				PROCEDURE = testschema."testFunc",
				COMMUTATOR = OPERATOR(testschema.##),
				NEGATOR = OPERATOR(public.###),
				RESTRICT = eqsel,
				JOIN = eqjoinsel,
				HASHES,
				MERGES
			)`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP OPERATOR testschema.## (path, path)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP OPERATOR public.### (path, path)")

			expectedOperator := backup.Operator{Oid: 0, Schema: "testschema", Name: "##", Procedure: `testschema."testFunc"`, LeftArgType: "path", RightArgType: "path", CommutatorOp: "testschema.##", NegatorOp: "public.###", RestrictFunction: "eqsel", JoinFunction: "eqjoinsel", CanHash: true, CanMerge: true}

			results := backup.GetOperators(connectionPool)

			Expect(results).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&expectedOperator, &results[0], "Oid")

		})
		It("returns a slice of operators from a specific schema", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE OPERATOR public.## (LEFTARG = bigint, PROCEDURE = numeric_fac)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP OPERATOR public.## (bigint, NONE)")
			testhelper.AssertQueryRuns(connectionPool, "CREATE SCHEMA testschema")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA testschema")
			testhelper.AssertQueryRuns(connectionPool, "CREATE OPERATOR testschema.## (LEFTARG = bigint, PROCEDURE = numeric_fac)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP OPERATOR testschema.## (bigint, NONE)")
			_ = backupCmdFlags.Set(options.INCLUDE_SCHEMA, "testschema")

			expectedOperator := backup.Operator{Oid: 0, Schema: "testschema", Name: "##", Procedure: "numeric_fac", LeftArgType: "bigint", RightArgType: "-", CommutatorOp: "0", NegatorOp: "0", RestrictFunction: "-", JoinFunction: "-", CanHash: false, CanMerge: false}

			results := backup.GetOperators(connectionPool)

			Expect(results).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&expectedOperator, &results[0], "Oid")
		})
	})
	Describe("GetOperatorFamilies", func() {
		It("returns a slice of operator families", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE OPERATOR FAMILY public.testfam USING hash;")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP OPERATOR FAMILY public.testfam USING hash")

			expectedOperator := backup.OperatorFamily{Oid: 0, Schema: "public", Name: "testfam", IndexMethod: "hash"}

			results := backup.GetOperatorFamilies(connectionPool)

			Expect(results).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&expectedOperator, &results[0], "Oid")
		})
		It("returns a slice of operator families in a specific schema", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE OPERATOR FAMILY public.testfam USING hash;")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP OPERATOR FAMILY public.testfam USING hash")
			testhelper.AssertQueryRuns(connectionPool, "CREATE SCHEMA testschema")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA testschema")
			testhelper.AssertQueryRuns(connectionPool, "CREATE OPERATOR FAMILY testschema.testfam USING hash;")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP OPERATOR FAMILY testschema.testfam USING hash")
			_ = backupCmdFlags.Set(options.INCLUDE_SCHEMA, "testschema")

			expectedOperator := backup.OperatorFamily{Oid: 0, Schema: "testschema", Name: "testfam", IndexMethod: "hash"}

			results := backup.GetOperatorFamilies(connectionPool)

			Expect(results).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&expectedOperator, &results[0], "Oid")
		})
	})
	Describe("GetOperatorClasses", func() {
		It("returns a slice of operator classes", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE OPERATOR CLASS public.testclass FOR TYPE int USING hash AS STORAGE int")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP OPERATOR FAMILY public.testclass USING hash")

			expected := backup.OperatorClass{Oid: 0, Schema: "public", Name: "testclass", FamilySchema: "public", FamilyName: "testclass", IndexMethod: "hash", Type: "integer", Default: false, StorageType: "-", Operators: nil, Functions: nil}

			results := backup.GetOperatorClasses(connectionPool)

			Expect(results).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&expected, &results[0], "Oid")

		})
		It("returns a slice of operator classes with an operator family", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE SCHEMA testschema")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA testschema CASCADE")

			testhelper.AssertQueryRuns(connectionPool, "CREATE OPERATOR FAMILY testschema.testfam USING gist;")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP OPERATOR FAMILY testschema.testfam USING gist CASCADE")
			testhelper.AssertQueryRuns(connectionPool, "CREATE OPERATOR CLASS public.testclass FOR TYPE int USING gist FAMILY testschema.testfam AS STORAGE int")

			expected := backup.OperatorClass{Oid: 0, Schema: "public", Name: "testclass", FamilySchema: "testschema", FamilyName: "testfam", IndexMethod: "gist", Type: "integer", Default: false, StorageType: "-", Operators: nil, Functions: nil}

			results := backup.GetOperatorClasses(connectionPool)

			Expect(results).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&expected, &results[0], "Oid")
		})
		It("returns a slice of operator classes with different type and storage type", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE OPERATOR CLASS public.testclass DEFAULT FOR TYPE int USING gist AS STORAGE text")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP OPERATOR FAMILY public.testclass USING gist")

			expected := backup.OperatorClass{Oid: 0, Schema: "public", Name: "testclass", FamilySchema: "public", FamilyName: "testclass", IndexMethod: "gist", Type: "integer", Default: true, StorageType: "text", Operators: nil, Functions: nil}

			results := backup.GetOperatorClasses(connectionPool)

			Expect(results).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&expected, &results[0], "Oid")

		})
		It("returns a slice of operator classes with operators and functions", func() {
			opClassQuery := ""
			expectedRecheck := false
			if connectionPool.Version.Before("6") {
				opClassQuery = "CREATE OPERATOR CLASS public.testclass FOR TYPE int USING gist AS OPERATOR 1 = RECHECK, OPERATOR 2 < , FUNCTION 1 abs(integer), FUNCTION 2 int4out(integer)"
				expectedRecheck = true
			} else {
				opClassQuery = "CREATE OPERATOR CLASS public.testclass FOR TYPE int USING gist AS OPERATOR 1 =, OPERATOR 2 < , FUNCTION 1 abs(integer), FUNCTION 2 int4out(integer)"
			}

			testhelper.AssertQueryRuns(connectionPool, opClassQuery)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP OPERATOR FAMILY public.testclass USING gist")

			expectedOperators := []backup.OperatorClassOperator{{ClassOid: 0, StrategyNumber: 1, Operator: "=(integer,integer)", Recheck: expectedRecheck}, {ClassOid: 0, StrategyNumber: 2, Operator: "<(integer,integer)", Recheck: false}}
			expectedFunctions := []backup.OperatorClassFunction{{ClassOid: 0, SupportNumber: 1, LeftType: "integer", RightType: "integer", FunctionName: "abs(integer)"}, {ClassOid: 0, SupportNumber: 2, LeftType: "integer", RightType: "integer", FunctionName: "int4out(integer)"}}
			expected := backup.OperatorClass{Oid: 0, Schema: "public", Name: "testclass", FamilySchema: "public", FamilyName: "testclass", IndexMethod: "gist", Type: "integer", Default: false, StorageType: "-", Operators: expectedOperators, Functions: expectedFunctions}

			results := backup.GetOperatorClasses(connectionPool)

			Expect(results).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&expected, &results[0], "Oid", "Operators.ClassOid", "Functions.ClassOid")

		})
		It("returns a slice of operator classes for a specific schema", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE OPERATOR CLASS public.testclass FOR TYPE int USING hash AS STORAGE int")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP OPERATOR FAMILY public.testclass USING hash")

			testhelper.AssertQueryRuns(connectionPool, "CREATE SCHEMA testschema")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA testschema CASCADE")
			testhelper.AssertQueryRuns(connectionPool, "CREATE OPERATOR CLASS testschema.testclass FOR TYPE int USING hash AS STORAGE int")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP OPERATOR FAMILY testschema.testclass USING hash")

			_ = backupCmdFlags.Set(options.INCLUDE_SCHEMA, "testschema")

			expected := backup.OperatorClass{Oid: 0, Schema: "testschema", Name: "testclass", FamilySchema: "testschema", FamilyName: "testclass", IndexMethod: "hash", Type: "integer", Default: false, StorageType: "-", Operators: nil, Functions: nil}

			results := backup.GetOperatorClasses(connectionPool)

			Expect(results).To(HaveLen(1))
				structmatcher.ExpectStructsToMatchExcluding(&expected, &results[0], "Oid")

		})
		It("returns a slice of operator classes with an operator with a sort family", func() {
			testutils.SkipIfBefore6(connectionPool)

			testhelper.AssertQueryRuns(connectionPool, "CREATE OPERATOR FAMILY public.sort_family USING btree")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP OPERATOR FAMILY public.sort_family USING btree")
			opClassQuery := "CREATE OPERATOR CLASS public.testclass FOR TYPE int USING gist AS OPERATOR 1 = FOR ORDER BY public.sort_family"
			testhelper.AssertQueryRuns(connectionPool, opClassQuery)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP OPERATOR FAMILY public.testclass USING gist")

			expectedOperators := []backup.OperatorClassOperator{{ClassOid: 0, StrategyNumber: 1, Operator: "=(integer,integer)", Recheck: false, OrderByFamily: "public.sort_family"}}
			expected := backup.OperatorClass{Oid: 0, Schema: "public", Name: "testclass", FamilySchema: "public", FamilyName: "testclass", IndexMethod: "gist", Type: "integer", Default: false, StorageType: "-", Operators: expectedOperators, Functions: nil}

			results := backup.GetOperatorClasses(connectionPool)

			Expect(results).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&expected, &results[0], "Oid", "Operators.ClassOid", "Functions.ClassOid")
		})
	})
})
