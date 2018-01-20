package integration

import (
	"sort"

	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/lib/pq"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup integration tests", func() {
	Describe("Get[type]Types functions", func() {
		var (
			shellType       backup.Type
			baseTypeDefault backup.Type
			baseTypeCustom  backup.Type
			compositeType   backup.Type
			enumType        backup.Type
		)
		BeforeEach(func() {
			shellType = backup.Type{Type: "p", Schema: "public", Name: "shell_type"}
			baseTypeDefault = backup.Type{
				Oid: 1, Type: "b", Schema: "public", Name: "base_type", Input: "base_fn_in", Output: "base_fn_out", Receive: "",
				Send: "", ModIn: "", ModOut: "", InternalLength: -1, IsPassedByValue: false, Alignment: "i", Storage: "p",
				DefaultVal: "", Element: "", Delimiter: ",", Category: "U",
			}
			baseTypeCustom = backup.Type{
				Oid: 1, Type: "b", Schema: "public", Name: "base_type", Input: "base_fn_in", Output: "base_fn_out", Receive: "",
				Send: "", ModIn: "", ModOut: "", InternalLength: 8, IsPassedByValue: true, Alignment: "d", Storage: "p",
				DefaultVal: "0", Element: "integer", Delimiter: ";", Category: "U",
			}
			compositeType = backup.Type{
				Oid: 1, Type: "c", Schema: "public", Name: "composite_type",
				Attributes: pq.StringArray{"\tname integer", "\tname2 integer", "\tname1 text"},
			}
			enumType = backup.Type{
				Oid: 1, Type: "e", Schema: "public", Name: "enum_type", EnumLabels: "'label1',\n\t'label2',\n\t'label3'",
			}
		})
		It("returns a slice for a shell type", func() {
			testutils.AssertQueryRuns(connection, "CREATE TYPE shell_type")
			defer testutils.AssertQueryRuns(connection, "DROP TYPE shell_type")

			results := backup.GetShellTypes(connection)

			Expect(len(results)).To(Equal(1))
			testutils.ExpectStructsToMatchIncluding(&shellType, &results[0], "Schema", "Name", "Type")
		})
		It("returns a slice of composite types", func() {
			testutils.AssertQueryRuns(connection, "CREATE TYPE composite_type AS (name int4, name2 int, name1 text);")
			defer testutils.AssertQueryRuns(connection, "DROP TYPE composite_type")

			results := backup.GetCompositeTypes(connection)

			Expect(len(results)).To(Equal(1))
			testutils.ExpectStructsToMatchIncluding(&compositeType, &results[0], "Type", "Schema", "Name")
		})
		It("returns a slice for a base type with default values", func() {
			testutils.AssertQueryRuns(connection, "CREATE TYPE base_type")
			defer testutils.AssertQueryRuns(connection, "DROP TYPE base_type CASCADE")
			testutils.AssertQueryRuns(connection, "CREATE FUNCTION base_fn_in(cstring) RETURNS base_type AS 'boolin' LANGUAGE internal")
			testutils.AssertQueryRuns(connection, "CREATE FUNCTION base_fn_out(base_type) RETURNS cstring AS 'boolout' LANGUAGE internal")
			testutils.AssertQueryRuns(connection, "CREATE TYPE base_type(INPUT=base_fn_in, OUTPUT=base_fn_out)")

			results := backup.GetBaseTypes(connection)

			Expect(len(results)).To(Equal(1))
			if connection.Version.Before("5") {
				testutils.ExpectStructsToMatchExcluding(&results[0], &baseTypeDefault, "Oid", "ModIn", "ModOut")
			} else {
				testutils.ExpectStructsToMatchExcluding(&results[0], &baseTypeDefault, "Oid")
			}
		})
		It("returns a slice for a base type with custom configuration", func() {
			testutils.AssertQueryRuns(connection, "CREATE TYPE base_type")
			defer testutils.AssertQueryRuns(connection, "DROP TYPE base_type CASCADE")
			testutils.AssertQueryRuns(connection, "CREATE FUNCTION base_fn_in(cstring) RETURNS base_type AS 'boolin' LANGUAGE internal")
			testutils.AssertQueryRuns(connection, "CREATE FUNCTION base_fn_out(base_type) RETURNS cstring AS 'boolout' LANGUAGE internal")
			if connection.Version.Before("6") {
				testutils.AssertQueryRuns(connection, "CREATE TYPE base_type(INPUT=base_fn_in, OUTPUT=base_fn_out, INTERNALLENGTH=8, PASSEDBYVALUE, ALIGNMENT=double, STORAGE=plain, DEFAULT=0, ELEMENT=integer, DELIMITER=';')")
			} else {
				testutils.AssertQueryRuns(connection, "CREATE TYPE base_type(INPUT=base_fn_in, OUTPUT=base_fn_out, INTERNALLENGTH=8, PASSEDBYVALUE, ALIGNMENT=double, STORAGE=plain, DEFAULT=0, ELEMENT=integer, DELIMITER=';', CATEGORY='N', PREFERRED=true)")
			}

			results := backup.GetBaseTypes(connection)

			Expect(len(results)).To(Equal(1))
			if connection.Version.Before("5") {
				testutils.ExpectStructsToMatchExcluding(&results[0], &baseTypeCustom, "Oid", "ModIn", "ModOut")
			} else if connection.Version.Before("6") {
				testutils.ExpectStructsToMatchExcluding(&results[0], &baseTypeCustom, "Oid")
			} else {
				baseTypeCustom.Category = "N"
				baseTypeCustom.Preferred = true
				testutils.ExpectStructsToMatchExcluding(&results[0], &baseTypeCustom, "Oid")
			}
		})
		It("returns a slice for an enum type", func() {
			testutils.SkipIf4(connection)
			testutils.AssertQueryRuns(connection, "CREATE TYPE enum_type AS ENUM ('label1','label2','label3')")
			defer testutils.AssertQueryRuns(connection, "DROP TYPE enum_type")

			results := backup.GetEnumTypes(connection)

			Expect(len(results)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&results[0], &enumType, "Oid")
		})
		It("does not return types for sequences or views", func() {
			testutils.AssertQueryRuns(connection, "CREATE SEQUENCE my_sequence START 10")
			defer testutils.AssertQueryRuns(connection, "DROP SEQUENCE my_sequence")
			testutils.AssertQueryRuns(connection, "CREATE VIEW simpleview AS SELECT rolname FROM pg_roles")
			defer testutils.AssertQueryRuns(connection, "DROP VIEW simpleview")

			results := backup.GetCompositeTypes(connection)

			Expect(len(results)).To(Equal(0))
		})
		It("does not return implicit base or composite types for tables with length > NAMEDATALEN", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE looooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooong(i int)")
			// The table's name will be truncated to 63 characters upon creation, as will the names of its implicit types
			defer testutils.AssertQueryRuns(connection, "DROP TABLE loooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo;")

			bases := backup.GetBaseTypes(connection)
			composites := backup.GetCompositeTypes(connection)

			Expect(len(bases)).To(Equal(0))
			Expect(len(composites)).To(Equal(0))
		})
		It("returns a slice for a domain type", func() {
			domainType := backup.Type{
				Oid: 1, Type: "d", Schema: "public", Name: "domain1", DefaultVal: "4", BaseType: `"numeric"`, NotNull: false,
			}
			testutils.AssertQueryRuns(connection, "CREATE DOMAIN domain1 AS numeric DEFAULT 4")
			defer testutils.AssertQueryRuns(connection, "DROP DOMAIN domain1")

			results := backup.GetDomainTypes(connection)

			Expect(len(results)).To(Equal(1))
			testutils.ExpectStructsToMatchIncluding(&results[0], &domainType, "Schema", "Name", "Type", "DefaultVal", "BaseType", "NotNull")
		})
		It("returns a slice for a type in a specific schema", func() {
			testutils.AssertQueryRuns(connection, "CREATE TYPE shell_type")
			defer testutils.AssertQueryRuns(connection, "DROP TYPE shell_type")
			testutils.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
			defer testutils.AssertQueryRuns(connection, "DROP SCHEMA testschema")
			testutils.AssertQueryRuns(connection, "CREATE TYPE testschema.shell_type")
			defer testutils.AssertQueryRuns(connection, "DROP TYPE testschema.shell_type")
			backup.SetIncludeSchemas([]string{"testschema"})

			results := backup.GetShellTypes(connection)
			shellTypeOtherSchema := backup.Type{Type: "p", Schema: "testschema", Name: "shell_type"}

			Expect(len(results)).To(Equal(1))
			testutils.ExpectStructsToMatchIncluding(&shellTypeOtherSchema, &results[0], "Schema", "Name", "Type")
		})
	})
	Describe("ConstructCompositeTypeDependencies", func() {
		BeforeEach(func() {
			testutils.AssertQueryRuns(connection, "CREATE FUNCTION base_fn_in(cstring) RETURNS base_type AS 'boolin' LANGUAGE internal")
			testutils.AssertQueryRuns(connection, "CREATE FUNCTION base_fn_out(base_type) RETURNS cstring AS 'boolout' LANGUAGE internal")
			testutils.AssertQueryRuns(connection, "CREATE FUNCTION base_fn_in2(cstring) RETURNS base_type2 AS 'boolin' LANGUAGE internal")
			testutils.AssertQueryRuns(connection, "CREATE FUNCTION base_fn_out2(base_type2) RETURNS cstring AS 'boolout' LANGUAGE internal")
			testutils.AssertQueryRuns(connection, "CREATE TYPE base_type(INPUT=base_fn_in, OUTPUT=base_fn_out)")
			testutils.AssertQueryRuns(connection, "CREATE TYPE base_type2(INPUT=base_fn_in2, OUTPUT=base_fn_out2)")
		})
		AfterEach(func() {
			testutils.AssertQueryRuns(connection, "DROP TYPE base_type CASCADE")
			testutils.AssertQueryRuns(connection, "DROP TYPE base_type2 CASCADE")
		})
		It("constructs dependencies correctly for a composite type dependent on one user-defined type", func() {
			testutils.AssertQueryRuns(connection, "CREATE TYPE comp_type AS (base base_type, builtin integer)")
			defer testutils.AssertQueryRuns(connection, "DROP TYPE comp_type")

			composites := backup.GetCompositeTypes(connection)
			compTypes := backup.ConstructCompositeTypeDependencies(connection, composites)

			Expect(len(compTypes)).To(Equal(1))
			Expect(len(compTypes[0].DependsUpon)).To(Equal(1))
			Expect(compTypes[0].DependsUpon[0]).To(Equal("public.base_type"))
		})
		It("constructs dependencies correctly for a composite type dependent on multiple user-defined types", func() {
			testutils.AssertQueryRuns(connection, "CREATE TYPE comp_type AS (base base_type, base2 base_type2)")
			defer testutils.AssertQueryRuns(connection, "DROP TYPE comp_type")

			composites := backup.GetCompositeTypes(connection)
			compTypes := backup.ConstructCompositeTypeDependencies(connection, composites)

			Expect(len(compTypes)).To(Equal(1))
			Expect(len(compTypes[0].DependsUpon)).To(Equal(2))
			sort.Strings(compTypes[0].DependsUpon)
			Expect(compTypes[0].DependsUpon).To(Equal([]string{"public.base_type", "public.base_type2"}))
		})
		It("constructs dependencies correctly for a composite type dependent on the same user-defined type multiple times", func() {
			testutils.AssertQueryRuns(connection, "CREATE TYPE comp_type AS (base base_type, base2 base_type)")
			defer testutils.AssertQueryRuns(connection, "DROP TYPE comp_type")

			composites := backup.GetCompositeTypes(connection)
			compTypes := backup.ConstructCompositeTypeDependencies(connection, composites)

			Expect(len(compTypes)).To(Equal(1))
			Expect(len(compTypes[0].DependsUpon)).To(Equal(1))
			Expect(compTypes[0].DependsUpon[0]).To(Equal("public.base_type"))
		})
	})
	Describe("ConstructBaseTypeDependencies4", func() {
		funcInfoMap := map[uint32]backup.FunctionInfo{}
		BeforeEach(func() {
			testutils.SkipIfNot4(connection)
			testutils.AssertQueryRuns(connection, "CREATE FUNCTION base_fn_in(cstring) RETURNS base_type AS 'boolin' LANGUAGE internal")
			testutils.AssertQueryRuns(connection, "CREATE FUNCTION base_fn_out(base_type) RETURNS cstring AS 'boolout' LANGUAGE internal")
			inOid := testutils.OidFromObjectName(connection, "public", "base_fn_in", backup.TYPE_FUNCTION)
			outOid := testutils.OidFromObjectName(connection, "public", "base_fn_out", backup.TYPE_FUNCTION)
			funcInfoMap[inOid] = backup.FunctionInfo{QualifiedName: "public.base_fn_in", Arguments: "cstring"}
			funcInfoMap[outOid] = backup.FunctionInfo{QualifiedName: "public.base_fn_out", Arguments: "base_type"}
		})
		AfterEach(func() {
			testutils.AssertQueryRuns(connection, "DROP TYPE base_type CASCADE")
		})
		It("constructs dependencies on user-defined functions", func() {
			testutils.AssertQueryRuns(connection, "CREATE TYPE base_type(INPUT=base_fn_in, OUTPUT=base_fn_out)")

			bases := backup.GetBaseTypes(connection)
			baseTypes := backup.ConstructBaseTypeDependencies4(connection, bases, funcInfoMap)

			Expect(len(baseTypes)).To(Equal(1))
			Expect(len(baseTypes[0].DependsUpon)).To(Equal(2))
			sort.Strings(baseTypes[0].DependsUpon)
			Expect(baseTypes[0].DependsUpon[0]).To(Equal("public.base_fn_in(cstring)"))
			Expect(baseTypes[0].DependsUpon[1]).To(Equal("public.base_fn_out(base_type)"))
		})
		It("doesn't construct dependencies on built-in functions", func() {
			testutils.AssertQueryRuns(connection, "CREATE TYPE base_type(INPUT=base_fn_in, OUTPUT=base_fn_out, TYPMOD_IN=numerictypmodin, TYPMOD_OUT=numerictypmodout)")

			bases := backup.GetBaseTypes(connection)
			baseTypes := backup.ConstructBaseTypeDependencies4(connection, bases, funcInfoMap)

			Expect(len(baseTypes)).To(Equal(1))
			Expect(len(baseTypes[0].DependsUpon)).To(Equal(2))
			sort.Strings(baseTypes[0].DependsUpon)
			Expect(baseTypes[0].DependsUpon[0]).To(Equal("public.base_fn_in(cstring)"))
			Expect(baseTypes[0].DependsUpon[1]).To(Equal("public.base_fn_out(base_type)"))
		})
	})
	Describe("ConstructBaseTypeDependencies5", func() {
		BeforeEach(func() {
			testutils.SkipIf4(connection)
			testutils.AssertQueryRuns(connection, "CREATE FUNCTION base_fn_in(cstring) RETURNS base_type AS 'boolin' LANGUAGE internal")
			testutils.AssertQueryRuns(connection, "CREATE FUNCTION base_fn_out(base_type) RETURNS cstring AS 'boolout' LANGUAGE internal")
		})
		AfterEach(func() {
			testutils.AssertQueryRuns(connection, "DROP TYPE base_type CASCADE")
		})
		It("constructs dependencies on user-defined functions", func() {
			testutils.AssertQueryRuns(connection, "CREATE TYPE base_type(INPUT=base_fn_in, OUTPUT=base_fn_out)")

			bases := backup.GetBaseTypes(connection)
			baseTypes := backup.ConstructBaseTypeDependencies5(connection, bases)

			Expect(len(baseTypes)).To(Equal(1))
			Expect(len(baseTypes[0].DependsUpon)).To(Equal(2))
			sort.Strings(baseTypes[0].DependsUpon)
			Expect(baseTypes[0].DependsUpon[0]).To(Equal("public.base_fn_in(cstring)"))
			Expect(baseTypes[0].DependsUpon[1]).To(Equal("public.base_fn_out(base_type)"))
		})
		It("doesn't construct dependencies on built-in functions", func() {
			testutils.AssertQueryRuns(connection, "CREATE TYPE base_type(INPUT=base_fn_in, OUTPUT=base_fn_out, TYPMOD_IN=numerictypmodin, TYPMOD_OUT=numerictypmodout)")

			bases := backup.GetBaseTypes(connection)
			baseTypes := backup.ConstructBaseTypeDependencies5(connection, bases)

			Expect(len(baseTypes)).To(Equal(1))
			Expect(len(baseTypes[0].DependsUpon)).To(Equal(2))
			sort.Strings(baseTypes[0].DependsUpon)
			Expect(baseTypes[0].DependsUpon[0]).To(Equal("public.base_fn_in(cstring)"))
			Expect(baseTypes[0].DependsUpon[1]).To(Equal("public.base_fn_out(base_type)"))
		})
	})
	Describe("ConstructDomainDependencies", func() {
		It("constructs dependencies on user-defined types", func() {
			testutils.AssertQueryRuns(connection, "CREATE DOMAIN parent_domain AS integer")
			defer testutils.AssertQueryRuns(connection, "DROP DOMAIN parent_domain")
			testutils.AssertQueryRuns(connection, "CREATE DOMAIN domain_type AS parent_domain")
			defer testutils.AssertQueryRuns(connection, "DROP DOMAIN domain_type")

			domains := backup.GetDomainTypes(connection)
			domains = backup.ConstructDomainDependencies(connection, domains)

			Expect(len(domains)).To(Equal(2))
			Expect(len(domains[0].DependsUpon)).To(Equal(1))
			Expect(domains[0].DependsUpon[0]).To(Equal("public.parent_domain"))
		})
		It("doesn't construct dependencies on built-in types", func() {
			testutils.AssertQueryRuns(connection, "CREATE DOMAIN parent_domain AS integer")
			defer testutils.AssertQueryRuns(connection, "DROP DOMAIN parent_domain")

			domains := backup.GetDomainTypes(connection)
			domains = backup.ConstructDomainDependencies(connection, domains)

			Expect(len(domains)).To(Equal(1))
			Expect(domains[0].DependsUpon).To(BeNil())
		})
	})
})
