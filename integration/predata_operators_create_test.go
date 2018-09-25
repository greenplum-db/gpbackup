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
	Describe("PrintCreateOperatorStatements", func() {
		It("creates operator", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE SCHEMA testschema")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA testschema")

			testhelper.AssertQueryRuns(connectionPool, "CREATE FUNCTION testschema.\"testFunc\" (path,path) RETURNS path AS 'SELECT $1' LANGUAGE SQL IMMUTABLE")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP FUNCTION testschema.\"testFunc\" (path,path)")

			operator := backup.Operator{Oid: 0, Schema: "testschema", Name: "##", Procedure: "testschema.\"testFunc\"", LeftArgType: "path", RightArgType: "path", CommutatorOp: "0", NegatorOp: "0", RestrictFunction: "-", JoinFunction: "-", CanHash: false, CanMerge: false}
			operators := []backup.Operator{operator}

			backup.PrintCreateOperatorStatements(backupfile, toc, operators, backup.MetadataMap{})

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP OPERATOR testschema.##(path, path)")

			resultOperators := backup.GetOperators(connectionPool)
			Expect(resultOperators).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&operator, &resultOperators[0], "Oid")
		})
		It("creates operator with owner and comment", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE SCHEMA testschema")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA testschema")

			testhelper.AssertQueryRuns(connectionPool, "CREATE FUNCTION testschema.\"testFunc\" (path,path) RETURNS path AS 'SELECT $1' LANGUAGE SQL IMMUTABLE")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP FUNCTION testschema.\"testFunc\" (path,path)")

			operatorMetadataMap := testutils.DefaultMetadataMap("OPERATOR", false, false, true)
			operator := backup.Operator{Oid: 1, Schema: "testschema", Name: "##", Procedure: "testschema.\"testFunc\"", LeftArgType: "path", RightArgType: "path", CommutatorOp: "0", NegatorOp: "0", RestrictFunction: "-", JoinFunction: "-", CanHash: false, CanMerge: false}
			operatorMetadata := operatorMetadataMap[operator.GetUniqueID()]
			operators := []backup.Operator{operator}

			backup.PrintCreateOperatorStatements(backupfile, toc, operators, operatorMetadataMap)
			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP OPERATOR testschema.##(path, path)")

			resultOperators := backup.GetOperators(connectionPool)
			Expect(resultOperators).To(HaveLen(1))
			resultMetadataMap := backup.GetCommentsForObjectType(connectionPool, backup.TYPE_OPERATOR)
			resultMetadata := resultMetadataMap[resultOperators[0].GetUniqueID()]
			structmatcher.ExpectStructsToMatchExcluding(&operator, &resultOperators[0], "Oid")
			structmatcher.ExpectStructsToMatchExcluding(&resultMetadata, &operatorMetadata, "Oid")
		})
	})
	Describe("PrintCreateOperatorFamilyStatements", func() {
		BeforeEach(func() {
			testutils.SkipIfBefore5(connectionPool)
		})
		It("creates operator family", func() {
			operatorFamily := backup.OperatorFamily{Oid: 1, Schema: "public", Name: "testfam", IndexMethod: "hash"}
			operatorFamilies := []backup.OperatorFamily{operatorFamily}

			backup.PrintCreateOperatorFamilyStatements(backupfile, toc, operatorFamilies, backup.MetadataMap{})

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP OPERATOR FAMILY public.testfam USING hash")

			resultOperatorFamilies := backup.GetOperatorFamilies(connectionPool)
			Expect(resultOperatorFamilies).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&operatorFamily, &resultOperatorFamilies[0], "Oid")
		})
		It("creates operator family with owner and comment", func() {
			operatorFamily := backup.OperatorFamily{Oid: 1, Schema: "public", Name: "testfam", IndexMethod: "hash"}
			operatorFamilies := []backup.OperatorFamily{operatorFamily}
			operatorFamilyMetadataMap := testutils.DefaultMetadataMap("OPERATOR FAMILY", false, true, true)
			operatorFamilyMetadata := operatorFamilyMetadataMap[operatorFamily.GetUniqueID()]

			backup.PrintCreateOperatorFamilyStatements(backupfile, toc, operatorFamilies, operatorFamilyMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP OPERATOR FAMILY public.testfam USING hash")

			resultOperatorFamilies := backup.GetOperatorFamilies(connectionPool)
			Expect(resultOperatorFamilies).To(HaveLen(1))
			resultMetadataMap := backup.GetMetadataForObjectType(connectionPool, backup.TYPE_OPERATORFAMILY)
			resultMetadata := resultMetadataMap[resultOperatorFamilies[0].GetUniqueID()]
			structmatcher.ExpectStructsToMatchExcluding(&operatorFamily, &resultOperatorFamilies[0], "Oid")
			structmatcher.ExpectStructsToMatchExcluding(&resultMetadata, &operatorFamilyMetadata, "Oid")
		})
	})
	Describe("PrintCreateOperatorClassStatements", func() {
		It("creates basic operator class", func() {
			operatorClass := backup.OperatorClass{Oid: 0, Schema: "public", Name: "testclass", FamilySchema: "public", FamilyName: "testclass", IndexMethod: "hash", Type: "integer", Default: false, StorageType: "-", Operators: nil, Functions: nil}
			if connectionPool.Version.Before("5") { // Operator families do not exist prior to GPDB5
				operatorClass.FamilySchema = ""
				operatorClass.FamilyName = ""
			}

			backup.PrintCreateOperatorClassStatements(backupfile, toc, []backup.OperatorClass{operatorClass}, nil)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			if connectionPool.Version.Before("5") {
				defer testhelper.AssertQueryRuns(connectionPool, "DROP OPERATOR CLASS public.testclass USING hash")
			} else {
				defer testhelper.AssertQueryRuns(connectionPool, "DROP OPERATOR FAMILY public.testclass USING hash CASCADE")
			}

			resultOperatorClasses := backup.GetOperatorClasses(connectionPool)
			Expect(resultOperatorClasses).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&operatorClass, &resultOperatorClasses[0], "Oid")
		})
		It("creates complex operator class", func() {
			testutils.SkipIfBefore5(connectionPool)
			operatorClass := backup.OperatorClass{Oid: 0, Schema: "public", Name: "testclass", FamilySchema: "public", FamilyName: "testfam", IndexMethod: "gist", Type: "integer", Default: true, StorageType: "-", Operators: nil, Functions: nil}

			operatorClass.Functions = []backup.OperatorClassFunction{{ClassOid: 0, SupportNumber: 1, RightType: "integer", LeftType: "integer", FunctionName: "abs(integer)"}}
			if connectionPool.Version.Before("5") { // Operator families do not exist prior to GPDB5
				operatorClass.FamilySchema = ""
				operatorClass.FamilyName = ""
				operatorClass.Functions = []backup.OperatorClassFunction{{ClassOid: 0, SupportNumber: 1, FunctionName: "abs(integer)"}}
			}

			expectedRecheck := false
			if connectionPool.Version.Before("6") {
				expectedRecheck = true
			}
			operatorClass.Operators = []backup.OperatorClassOperator{{ClassOid: 0, StrategyNumber: 1, Operator: "=(integer,integer)", Recheck: expectedRecheck}}

			testhelper.AssertQueryRuns(connectionPool, "CREATE OPERATOR FAMILY public.testfam USING gist")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP OPERATOR FAMILY public.testfam USING gist CASCADE")
			backup.PrintCreateOperatorClassStatements(backupfile, toc, []backup.OperatorClass{operatorClass}, nil)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			resultOperatorClasses := backup.GetOperatorClasses(connectionPool)
			Expect(resultOperatorClasses).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&operatorClass, &resultOperatorClasses[0], "Oid", "Operators.ClassOid", "Functions.ClassOid")
		})
		It("creates an operator class with an operator that has a sort family", func() {
			testutils.SkipIfBefore6(connectionPool)
			operatorClass := backup.OperatorClass{Oid: 0, Schema: "public", Name: "testclass", FamilySchema: "public", FamilyName: "testclass", IndexMethod: "gist", Type: "integer", Default: true, StorageType: "-", Operators: nil, Functions: nil}
			operatorClass.Operators = []backup.OperatorClassOperator{{ClassOid: 0, StrategyNumber: 1, Operator: "=(integer,integer)", Recheck: false, OrderByFamily: "public.sort_family_name"}}

			testhelper.AssertQueryRuns(connectionPool, "CREATE OPERATOR FAMILY public.sort_family_name USING btree")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP OPERATOR FAMILY public.sort_family_name USING btree")

			backup.PrintCreateOperatorClassStatements(backupfile, toc, []backup.OperatorClass{operatorClass}, nil)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP OPERATOR FAMILY public.testclass USING gist CASCADE")

			resultOperatorClasses := backup.GetOperatorClasses(connectionPool)
			Expect(resultOperatorClasses).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&operatorClass, &resultOperatorClasses[0], "Oid", "Operators.ClassOid", "Functions.ClassOid")
		})
		It("creates basic operator class with a comment and owner", func() {
			operatorClass := backup.OperatorClass{Oid: 1, Schema: "public", Name: "testclass", FamilySchema: "public", FamilyName: "testclass", IndexMethod: "hash", Type: "integer", Default: false, StorageType: "-", Operators: nil, Functions: nil}
			operatorClassMetadataMap := testutils.DefaultMetadataMap("OPERATOR CLASS", false, true, true)
			operatorClassMetadata := operatorClassMetadataMap[operatorClass.GetUniqueID()]
			if connectionPool.Version.Before("5") { // Operator families do not exist prior to GPDB5
				operatorClass.FamilySchema = ""
				operatorClass.FamilyName = ""
			}

			backup.PrintCreateOperatorClassStatements(backupfile, toc, []backup.OperatorClass{operatorClass}, operatorClassMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			if connectionPool.Version.Before("5") {
				defer testhelper.AssertQueryRuns(connectionPool, "DROP OPERATOR CLASS public.testclass USING hash")
			} else {
				defer testhelper.AssertQueryRuns(connectionPool, "DROP OPERATOR FAMILY public.testclass USING hash CASCADE")
			}

			resultOperatorClasses := backup.GetOperatorClasses(connectionPool)
			Expect(resultOperatorClasses).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&operatorClass, &resultOperatorClasses[0], "Oid")

			resultMetadataMap := backup.GetMetadataForObjectType(connectionPool, backup.TYPE_OPERATORCLASS)
			resultMetadata := resultMetadataMap[resultOperatorClasses[0].GetUniqueID()]
			structmatcher.ExpectStructsToMatchExcluding(&resultMetadata, &operatorClassMetadata, "Oid")

		})
	})
})
