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
	var (
		includeSecurityLabels bool
		emptyMetadata         backup.ObjectMetadata
		emptyMetadataMap      backup.MetadataMap
	)
	BeforeEach(func() {
		if connectionPool.Version.AtLeast("6") {
			includeSecurityLabels = true
		}
		toc, backupfile = testutils.InitializeTestTOC(buffer, "predata")
		emptyMetadata = backup.ObjectMetadata{}
		emptyMetadataMap = backup.MetadataMap{}
	})
	Describe("PrintTypeStatements", func() {
		var (
			shellType backup.ShellType
			baseType  backup.BaseType
			rangeType backup.RangeType
		)
		BeforeEach(func() {
			shellType = backup.ShellType{Schema: "public", Name: "shell_type"}
			baseType = backup.BaseType{
				Type: "b", Schema: "public", Name: "base_type", Input: "public.base_fn_in", Output: "public.base_fn_out", Receive: "",
				Send: "", ModIn: "", ModOut: "", InternalLength: 4, IsPassedByValue: true, Alignment: "i", Storage: "p",
				DefaultVal: "default", Element: "text", Category: "U", Preferred: false, Delimiter: ";", StorageOptions: "compresstype=zlib, compresslevel=1, blocksize=32768",
			}
			rangeType = backup.RangeType{
				Oid:            0,
				Schema:         "public",
				Name:           "textrange",
				SubType:        "text",
				Collation:      "public.some_coll",
				SubTypeOpClass: "pg_catalog.text_ops",
			}
		})
		Describe("PrintCreateShellTypeStatements", func() {
			It("creates shell types for base, shell and range types", func() {
				backup.PrintCreateShellTypeStatements(backupfile, toc, []backup.ShellType{shellType}, []backup.BaseType{baseType}, []backup.RangeType{rangeType})

				testhelper.AssertQueryRuns(connectionPool, buffer.String())
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TYPE public.shell_type")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TYPE public.base_type")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TYPE public.textrange")

				shells := backup.GetShellTypes(connectionPool)
				Expect(shells).To(HaveLen(3))
				Expect(shells[0].Name).To(Equal("base_type"))
				Expect(shells[1].Name).To(Equal("shell_type"))
				Expect(shells[2].Name).To(Equal("textrange"))
			})
		})

		Describe("PrintCreateCompositeTypeStatement", func() {
			var compositeType backup.CompositeType
			BeforeEach(func() {
				atts := []backup.Attribute{{Name: "att1", Type: "text"}, {Name: "att2", Type: "integer"}}
				compositeType = backup.CompositeType{
					Schema: "public", Name: "composite_type", Attributes: atts,
				}
			})
			It("creates composite types", func() {
				backup.PrintCreateCompositeTypeStatement(backupfile, toc, compositeType, emptyMetadata)

				testhelper.AssertQueryRuns(connectionPool, buffer.String())
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TYPE public.composite_type")

				resultTypes := backup.GetCompositeTypes(connectionPool)

				Expect(resultTypes).To(HaveLen(1))
				structmatcher.ExpectStructsToMatchExcluding(&compositeType, &resultTypes[0], "Oid", "Attributes.CompositeTypeOid")
			})
			It("creates composite types with a collation", func() {
				testutils.SkipIfBefore6(connectionPool)
				testhelper.AssertQueryRuns(connectionPool, `CREATE COLLATION public.some_coll (lc_collate = 'POSIX', lc_ctype = 'POSIX');`)
				defer testhelper.AssertQueryRuns(connectionPool, "DROP COLLATION public.some_coll")
				compositeType.Attributes[0].Collation = "public.some_coll"
				backup.PrintCreateCompositeTypeStatement(backupfile, toc, compositeType, emptyMetadata)

				testhelper.AssertQueryRuns(connectionPool, buffer.String())
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TYPE public.composite_type")

				resultTypes := backup.GetCompositeTypes(connectionPool)

				Expect(resultTypes).To(HaveLen(1))
				structmatcher.ExpectStructsToMatchExcluding(&compositeType, &resultTypes[0], "Oid", "Attributes.CompositeTypeOid")
			})
			It("creates composite types with attribute comments", func() {
				compositeType.Attributes[0].Comment = "'comment for att1'"
				backup.PrintCreateCompositeTypeStatement(backupfile, toc, compositeType, emptyMetadata)

				testhelper.AssertQueryRuns(connectionPool, buffer.String())
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TYPE public.composite_type")

				resultTypes := backup.GetCompositeTypes(connectionPool)

				Expect(resultTypes).To(HaveLen(1))
				structmatcher.ExpectStructsToMatchExcluding(&compositeType, &resultTypes[0], "Oid", "Attributes.CompositeTypeOid")
			})
		})
		Describe("PrintCreateBaseTypeStatement", func() {
			It("creates base types", func() {
				if connectionPool.Version.AtLeast("6") {
					baseType.Category = "N"
					baseType.Preferred = true
					baseType.Collatable = true
				}
				metadata := testutils.DefaultMetadata("TYPE", false, true, true, includeSecurityLabels)
				backup.PrintCreateBaseTypeStatement(backupfile, toc, baseType, metadata)

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
		})
		Describe("PrintCreateEnumTypeStatements", func() {
			It("creates enum types", func() {
				testutils.SkipIfBefore5(connectionPool)
				enumType := backup.EnumType{Schema: "public", Name: "enum_type", EnumLabels: "'enum_labels'"}
				enums := []backup.EnumType{enumType}
				backup.PrintCreateEnumTypeStatements(backupfile, toc, enums, emptyMetadataMap)

				testhelper.AssertQueryRuns(connectionPool, buffer.String())
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TYPE public.enum_type")

				resultTypes := backup.GetEnumTypes(connectionPool)

				Expect(resultTypes).To(HaveLen(1))
				structmatcher.ExpectStructsToMatchExcluding(&resultTypes[0], &enumType, "Oid")
			})
		})
		Describe("PrintCreateDomainStatement", func() {
			domainType := backup.Domain{
				Oid: 1, Schema: "public", Name: "domain_type", BaseType: "character(8)", DefaultVal: "'abc'::bpchar", NotNull: true, Collation: ""}
			It("creates domain types", func() {
				constraints := []backup.Constraint{}
				if connectionPool.Version.AtLeast("6") {
					testhelper.AssertQueryRuns(connectionPool, "CREATE COLLATION public.some_coll (lc_collate = 'POSIX', lc_ctype = 'POSIX')")
					defer testhelper.AssertQueryRuns(connectionPool, "DROP COLLATION public.some_coll")
					domainType.Collation = "public.some_coll"
				}
				metadata := testutils.DefaultMetadata("DOMAIN", false, true, true, includeSecurityLabels)
				backup.PrintCreateDomainStatement(backupfile, toc, domainType, metadata, constraints)

				testhelper.AssertQueryRuns(connectionPool, buffer.String())
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TYPE public.domain_type")

				resultTypes := backup.GetDomainTypes(connectionPool)

				Expect(resultTypes).To(HaveLen(1))
				structmatcher.ExpectStructsToMatchIncluding(&domainType, &resultTypes[0], "Schema", "Name", "Type", "DefaultVal", "BaseType", "NotNull", "Collation")
			})
		})
		Describe("PrintCreateRangeTypeStatement", func() {
			It("creates a range type with a collation and opclass", func() {
				testutils.SkipIfBefore6(connectionPool)
				testhelper.AssertQueryRuns(connectionPool, "CREATE COLLATION public.some_coll (lc_collate = 'POSIX', lc_ctype = 'POSIX');")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP COLLATION public.some_coll")

				metadata := testutils.DefaultMetadata("TYPE", false, true, true, includeSecurityLabels)
				backup.PrintCreateRangeTypeStatement(backupfile, toc, rangeType, metadata)

				testhelper.AssertQueryRuns(connectionPool, buffer.String())
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TYPE public.textrange")

				resultTypes := backup.GetRangeTypes(connectionPool)

				Expect(len(resultTypes)).To(Equal(1))
				structmatcher.ExpectStructsToMatchExcluding(&rangeType, &resultTypes[0], "Oid")
			})
			It("creates a range type in a specific schema with a subtype diff function", func() {
				testutils.SkipIfBefore6(connectionPool)
				rangeType := backup.RangeType{
					Oid:            0,
					Schema:         "testschema",
					Name:           "timerange",
					SubType:        "time without time zone",
					SubTypeOpClass: "pg_catalog.time_ops",
					SubTypeDiff:    "testschema.time_subtype_diff",
				}
				testhelper.AssertQueryRuns(connectionPool, "CREATE SCHEMA testschema;")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA testschema CASCADE;")
				testhelper.AssertQueryRuns(connectionPool, "CREATE FUNCTION testschema.time_subtype_diff(x time, y time) RETURNS float8 AS 'SELECT EXTRACT(EPOCH FROM (x - y))' LANGUAGE sql STRICT IMMUTABLE;")

				backup.PrintCreateRangeTypeStatement(backupfile, toc, rangeType, emptyMetadata)

				testhelper.AssertQueryRuns(connectionPool, buffer.String())

				resultTypes := backup.GetRangeTypes(connectionPool)

				Expect(len(resultTypes)).To(Equal(1))
				structmatcher.ExpectStructsToMatchExcluding(&rangeType, &resultTypes[0], "Oid")
			})
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
			collationMetadataMap := testutils.DefaultMetadataMap("COLLATION", false, true, true, false)
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
