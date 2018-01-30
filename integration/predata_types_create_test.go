package integration

import (
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
				Type: "b", Schema: "public", Name: "base_type", Input: "base_fn_in", Output: "base_fn_out", Receive: "",
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
			domainType.BaseType = `"numeric"`
			domainType.DefaultVal = "5"
			domainType.NotNull = true
			types = []backup.Type{shellType, baseType, compositeType, domainType}
			if connection.Version.AtLeast("5") {
				types = append(types, enumType)
			}
			typeMetadata = backup.ObjectMetadata{}
		})

		It("creates shell types for base and shell types only", func() {
			backup.PrintCreateShellTypeStatements(backupfile, toc, types)

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP TYPE shell_type")
			defer testutils.AssertQueryRuns(connection, "DROP TYPE base_type")

			shells := backup.GetShellTypes(connection)
			Expect(len(shells)).To(Equal(2))
			Expect(shells[0].Name).To(Equal("base_type"))
			Expect(shells[1].Name).To(Equal("shell_type"))
		})

		It("creates composite types", func() {
			backup.PrintCreateCompositeTypeStatement(backupfile, toc, compositeType, typeMetadata)

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP TYPE composite_type")

			resultTypes := backup.GetCompositeTypes(connection)

			Expect(len(resultTypes)).To(Equal(1))
			testutils.ExpectStructsToMatchIncluding(&compositeType, &resultTypes[0], "Type", "Schema", "Name", "Comment", "Owner", "Attributes")
		})

		It("creates enum types", func() {
			testutils.SkipIf4(connection)
			enums := []backup.Type{enumType}
			backup.PrintCreateEnumTypeStatements(backupfile, toc, enums, typeMetadataMap)

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP TYPE enum_type")

			resultTypes := backup.GetEnumTypes(connection)

			Expect(len(resultTypes)).To(Equal(1))
			testutils.ExpectStructsToMatchIncluding(&resultTypes[0], &enumType, "Type", "Schema", "Name", "Comment", "Owner", "EnumLabels")
		})

		It("creates base types", func() {
			if connection.Version.AtLeast("6") {
				baseType.Category = "N"
				baseType.Preferred = true
			}
			backup.PrintCreateBaseTypeStatement(backupfile, toc, baseType, typeMetadata)

			//Run queries to set up the database state so we can successfully create base types
			testutils.AssertQueryRuns(connection, "CREATE TYPE base_type")
			defer testutils.AssertQueryRuns(connection, "DROP TYPE base_type CASCADE")
			testutils.AssertQueryRuns(connection, "CREATE FUNCTION base_fn_in(cstring) RETURNS base_type AS 'boolin' LANGUAGE internal")
			testutils.AssertQueryRuns(connection, "CREATE FUNCTION base_fn_out(base_type) RETURNS cstring AS 'boolout' LANGUAGE internal")

			testutils.AssertQueryRuns(connection, buffer.String())

			resultTypes := backup.GetBaseTypes(connection)

			Expect(len(resultTypes)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&baseType, &resultTypes[0], "Oid")
		})
		It("creates domain types", func() {
			constraints := []backup.Constraint{}
			backup.PrintCreateDomainStatement(backupfile, toc, domainType, typeMetadata, constraints)

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP TYPE domain_type")

			resultTypes := backup.GetDomainTypes(connection)

			Expect(len(resultTypes)).To(Equal(1))
			testutils.ExpectStructsToMatchIncluding(&domainType, &resultTypes[0], "Schema", "Name", "Type", "DefaultVal", "BaseType", "NotNull")
		})
	})
})
