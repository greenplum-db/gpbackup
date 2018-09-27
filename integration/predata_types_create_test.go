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
	Describe("PrintTypeStatements", func() {
		var (
			shellType       backup.Type
			baseType        backup.Type
			compositeType   backup.Type
			enumType        backup.Type
			domainType      backup.Type
			types           []backup.Type
			typeMetadata    backup.ObjectMetadata
			typeMetadataMap backup.MetadataMap
		)
		BeforeEach(func() {
			shellType = backup.Type{Type: "p", Schema: "public", Name: "shell_type"}
			baseType = backup.Type{
				Type: "b", Schema: "public", Name: "base_type", Input: "public.base_fn_in", Output: "public.base_fn_out", Receive: "",
				Send: "", ModIn: "", ModOut: "", InternalLength: 4, IsPassedByValue: true, Alignment: "i", Storage: "p",
				DefaultVal: "default", Element: "text", Category: "U", Preferred: false, Delimiter: ";", StorageOptions: "compresstype=zlib, compresslevel=1, blocksize=32768",
			}
			atts := []backup.Attribute{{Name: "att1", Type: "text"}, {Name: "att2", Type: "integer"}}
			compositeType = backup.Type{
				Type: "c", Schema: "public", Name: "composite_type",
				Attributes: atts, Category: "U",
			}
			enumType = backup.Type{Type: "e", Schema: "public", Name: "enum_type", EnumLabels: "'enum_labels'", Category: "U"}
			domainType = testutils.DefaultTypeDefinition("d", "domain_type")
			domainType.BaseType = "character(8)"
			domainType.DefaultVal = "'abc'::bpchar"
			domainType.NotNull = true
			types = []backup.Type{shellType, baseType, compositeType, domainType}
			if connectionPool.Version.AtLeast("5") {
				types = append(types, enumType)
			}
			typeMetadata = backup.ObjectMetadata{}
		})

		It("creates shell types for base and shell types only", func() {
			backup.PrintCreateShellTypeStatements(backupfile, toc, types)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TYPE public.shell_type")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TYPE public.base_type")

			shells := backup.GetShellTypes(connectionPool)
			Expect(shells).To(HaveLen(2))
			Expect(shells[0].Name).To(Equal("base_type"))
			Expect(shells[1].Name).To(Equal("shell_type"))
		})

		It("creates composite types", func() {
			backup.PrintCreateCompositeTypeStatement(backupfile, toc, compositeType, typeMetadata)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TYPE public.composite_type")

			resultTypes := backup.GetCompositeTypes(connectionPool)

			Expect(resultTypes).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&compositeType, &resultTypes[0], "Oid", "Attributes.CompositeTypeOid", "Category")
		})
		It("creates composite types with a collation", func() {
			testutils.SkipIfBefore6(connectionPool)
			testhelper.AssertQueryRuns(connectionPool, `CREATE COLLATION public.some_coll (lc_collate = 'POSIX', lc_ctype = 'POSIX');`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP COLLATION public.some_coll")
			compositeType.Attributes[0].Collation = "public.some_coll"
			backup.PrintCreateCompositeTypeStatement(backupfile, toc, compositeType, typeMetadata)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TYPE public.composite_type")

			resultTypes := backup.GetCompositeTypes(connectionPool)

			Expect(resultTypes).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&compositeType, &resultTypes[0], "Oid", "Attributes.CompositeTypeOid", "Category")
		})
		It("creates composite types with attribute comments", func() {
			compositeType.Attributes[0].Comment = "'comment for att1'"
			backup.PrintCreateCompositeTypeStatement(backupfile, toc, compositeType, typeMetadata)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TYPE public.composite_type")

			resultTypes := backup.GetCompositeTypes(connectionPool)

			Expect(resultTypes).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&compositeType, &resultTypes[0], "Oid", "Attributes.CompositeTypeOid", "Category")
		})

		It("creates enum types", func() {
			testutils.SkipIfBefore5(connectionPool)
			enums := []backup.Type{enumType}
			backup.PrintCreateEnumTypeStatements(backupfile, toc, enums, typeMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TYPE public.enum_type")

			resultTypes := backup.GetEnumTypes(connectionPool)

			Expect(resultTypes).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchIncluding(&resultTypes[0], &enumType, "Type", "Schema", "Name", "Comment", "Owner", "EnumLabels")
		})

		It("creates base types", func() {
			if connectionPool.Version.AtLeast("6") {
				baseType.Category = "N"
				baseType.Preferred = true
				baseType.Collatable = true
			}
			backup.PrintCreateBaseTypeStatement(backupfile, toc, baseType, typeMetadata)

			//Run queries to set up the database state so we can successfully create base types
			testhelper.AssertQueryRuns(connectionPool, "CREATE TYPE public.base_type")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TYPE public.base_type CASCADE")
			testhelper.AssertQueryRuns(connectionPool, "CREATE FUNCTION public.base_fn_in(cstring) RETURNS public.base_type AS 'boolin' LANGUAGE internal")
			testhelper.AssertQueryRuns(connectionPool, "CREATE FUNCTION public.base_fn_out(public.base_type) RETURNS cstring AS 'boolout' LANGUAGE internal")

			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			resultTypes := backup.GetBaseTypes(connectionPool)

			Expect(resultTypes).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&baseType, &resultTypes[0], "Oid")
		})
		It("creates domain types", func() {
			constraints := []backup.Constraint{}
			if connectionPool.Version.AtLeast("6") {
				testhelper.AssertQueryRuns(connectionPool, "CREATE COLLATION public.some_coll (lc_collate = 'POSIX', lc_ctype = 'POSIX')")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP COLLATION public.some_coll")
				domainType.Collation = "public.some_coll"
			}
			backup.PrintCreateDomainStatement(backupfile, toc, domainType, typeMetadata, constraints)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TYPE public.domain_type")

			resultTypes := backup.GetDomainTypes(connectionPool)

			Expect(resultTypes).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchIncluding(&domainType, &resultTypes[0], "Schema", "Name", "Type", "DefaultVal", "BaseType", "NotNull", "Collation")
		})
	})
	Describe("PrintCreateCollationStatement", func() {
		collation := backup.Collation{Oid: 1, Schema: "public", Name: "testcollation", Collate: "POSIX", Ctype: "POSIX"}
		It("creates a basic collation", func() {
			testutils.SkipIfBefore6(connectionPool)

			backup.PrintCreateCollationStatements(backupfile, toc, []backup.Collation{collation}, backup.MetadataMap{})

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP COLLATION public.testcollation")

			resultCollations := backup.GetCollations(connectionPool)

			Expect(resultCollations).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&collation, &resultCollations[0], "Oid")
		})
		It("creates a basic collation with comment and owner", func() {
			testutils.SkipIfBefore6(connectionPool)
			collationMetadataMap := testutils.DefaultMetadataMap("COLLATION", false, true, true)
			collationMetadata := collationMetadataMap[collation.GetUniqueID()]

			backup.PrintCreateCollationStatements(backupfile, toc, []backup.Collation{collation}, collationMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP COLLATION public.testcollation")

			resultCollations := backup.GetCollations(connectionPool)
			resultMetadataMap := backup.GetMetadataForObjectType(connectionPool, backup.TYPE_COLLATION)

			Expect(resultCollations).To(HaveLen(1))
			uniqueID := testutils.UniqueIDFromObjectName(connectionPool, "public", "testcollation", backup.TYPE_COLLATION)
			resultMetadata := resultMetadataMap[uniqueID]
			structmatcher.ExpectStructsToMatchExcluding(&collation, &resultCollations[0], "Oid")
			structmatcher.ExpectStructsToMatch(&collationMetadata, &resultMetadata)

		})
	})
})
