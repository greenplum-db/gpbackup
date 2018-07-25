package integration

import (
	"github.com/greenplum-db/gp-common-go-libs/structmatcher"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/lib/pq"

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
			atts := pq.StringArray{"\tatt1 text", "\tatt2 integer"}
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
			if connection.Version.AtLeast("5") {
				types = append(types, enumType)
			}
			typeMetadata = backup.ObjectMetadata{}
		})

		It("creates shell types for base and shell types only", func() {
			backup.PrintCreateShellTypeStatements(backupfile, toc, types)

			testhelper.AssertQueryRuns(connection, buffer.String())
			defer testhelper.AssertQueryRuns(connection, "DROP TYPE public.shell_type")
			defer testhelper.AssertQueryRuns(connection, "DROP TYPE public.base_type")

			shells := backup.GetShellTypes(connection)
			Expect(len(shells)).To(Equal(2))
			Expect(shells[0].Name).To(Equal("base_type"))
			Expect(shells[1].Name).To(Equal("shell_type"))
		})

		It("creates composite types", func() {
			if connection.Version.AtLeast("6") {
				testhelper.AssertQueryRuns(connection, `CREATE COLLATION public.some_coll (lc_collate = 'POSIX', lc_ctype = 'POSIX');`)
				defer testhelper.AssertQueryRuns(connection, "DROP COLLATION public.some_coll")
				compositeType.Attributes = pq.StringArray{"\tatt1 text COLLATE public.some_coll", "\tatt2 integer"}
			}
			backup.PrintCreateCompositeTypeStatement(backupfile, toc, compositeType, typeMetadata)

			testhelper.AssertQueryRuns(connection, buffer.String())
			defer testhelper.AssertQueryRuns(connection, "DROP TYPE public.composite_type")

			resultTypes := backup.GetCompositeTypes(connection)

			Expect(len(resultTypes)).To(Equal(1))
			structmatcher.ExpectStructsToMatchIncluding(&compositeType, &resultTypes[0], "Type", "Schema", "Name", "Comment", "Owner", "Attributes")
		})

		It("creates enum types", func() {
			testutils.SkipIfBefore5(connection)
			enums := []backup.Type{enumType}
			backup.PrintCreateEnumTypeStatements(backupfile, toc, enums, typeMetadataMap)

			testhelper.AssertQueryRuns(connection, buffer.String())
			defer testhelper.AssertQueryRuns(connection, "DROP TYPE public.enum_type")

			resultTypes := backup.GetEnumTypes(connection)

			Expect(len(resultTypes)).To(Equal(1))
			structmatcher.ExpectStructsToMatchIncluding(&resultTypes[0], &enumType, "Type", "Schema", "Name", "Comment", "Owner", "EnumLabels")
		})

		It("creates base types", func() {
			if connection.Version.AtLeast("6") {
				baseType.Category = "N"
				baseType.Preferred = true
				baseType.Collatable = true
			}
			backup.PrintCreateBaseTypeStatement(backupfile, toc, baseType, typeMetadata)

			//Run queries to set up the database state so we can successfully create base types
			testhelper.AssertQueryRuns(connection, "CREATE TYPE public.base_type")
			defer testhelper.AssertQueryRuns(connection, "DROP TYPE public.base_type CASCADE")
			testhelper.AssertQueryRuns(connection, "CREATE FUNCTION public.base_fn_in(cstring) RETURNS public.base_type AS 'boolin' LANGUAGE internal")
			testhelper.AssertQueryRuns(connection, "CREATE FUNCTION public.base_fn_out(public.base_type) RETURNS cstring AS 'boolout' LANGUAGE internal")

			testhelper.AssertQueryRuns(connection, buffer.String())

			resultTypes := backup.GetBaseTypes(connection)

			Expect(len(resultTypes)).To(Equal(1))
			structmatcher.ExpectStructsToMatchExcluding(&baseType, &resultTypes[0], "Oid")
		})
		It("creates domain types", func() {
			constraints := []backup.Constraint{}
			if connection.Version.AtLeast("6") {
				domainType.Collation = "public.some_coll"
				testhelper.AssertQueryRuns(connection, "CREATE COLLATION public.some_coll (lc_collate = 'POSIX', lc_ctype = 'POSIX')")
				defer testhelper.AssertQueryRuns(connection, "DROP COLLATION public.some_coll")
			}
			backup.PrintCreateDomainStatement(backupfile, toc, domainType, typeMetadata, constraints)

			testhelper.AssertQueryRuns(connection, buffer.String())
			defer testhelper.AssertQueryRuns(connection, "DROP TYPE public.domain_type")

			resultTypes := backup.GetDomainTypes(connection)

			Expect(len(resultTypes)).To(Equal(1))
			structmatcher.ExpectStructsToMatchIncluding(&domainType, &resultTypes[0], "Schema", "Name", "Type", "DefaultVal", "BaseType", "NotNull", "Collation")
		})
	})
	Describe("PrintCreateCollationStatement", func() {
		It("creates a basic collation", func() {
			testutils.SkipIfBefore6(connection)
			collations := []backup.Collation{{Oid: 0, Schema: "public", Name: "testcollation", Collate: "POSIX", Ctype: "POSIX"}}

			backup.PrintCreateCollationStatements(backupfile, toc, collations, backup.MetadataMap{})

			testhelper.AssertQueryRuns(connection, buffer.String())
			defer testhelper.AssertQueryRuns(connection, "DROP COLLATION public.testcollation")

			resultCollations := backup.GetCollations(connection)

			Expect(len(resultCollations)).To(Equal(1))
			structmatcher.ExpectStructsToMatchExcluding(&collations[0], &resultCollations[0], "Oid")
		})
		It("creates a basic collation with comment and owner", func() {
			testutils.SkipIfBefore6(connection)
			collations := []backup.Collation{{Oid: 1, Schema: "public", Name: "testcollation", Collate: "POSIX", Ctype: "POSIX"}}
			collationMetadataMap := testutils.DefaultMetadataMap("COLLATION", false, true, true)
			collationMetadata := collationMetadataMap[1]

			backup.PrintCreateCollationStatements(backupfile, toc, collations, collationMetadataMap)

			testhelper.AssertQueryRuns(connection, buffer.String())
			defer testhelper.AssertQueryRuns(connection, "DROP COLLATION public.testcollation")

			resultCollations := backup.GetCollations(connection)
			resultMetadataMap := backup.GetMetadataForObjectType(connection, backup.TYPE_COLLATION)

			Expect(len(resultCollations)).To(Equal(1))
			oid := testutils.OidFromObjectName(connection, "public", "testcollation", backup.TYPE_COLLATION)
			resultMetadata := resultMetadataMap[oid]
			structmatcher.ExpectStructsToMatchExcluding(&collations[0], &resultCollations[0], "Oid")
			structmatcher.ExpectStructsToMatch(&collationMetadata, &resultMetadata)

		})
	})
})
