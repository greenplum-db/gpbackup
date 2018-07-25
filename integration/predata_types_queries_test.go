package integration

import (
	"sort"

	"github.com/greenplum-db/gp-common-go-libs/structmatcher"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/utils"
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
			enumType2       backup.Type
		)
		BeforeEach(func() {
			shellType = backup.Type{Type: "p", Schema: "public", Name: "shell_type"}
			baseTypeDefault = backup.Type{
				Oid: 1, Type: "b", Schema: "public", Name: "base_type", Input: "public.base_fn_in", Output: "public.base_fn_out", Receive: "",
				Send: "", ModIn: "", ModOut: "", InternalLength: -1, IsPassedByValue: false, Alignment: "i", Storage: "p",
				DefaultVal: "", Element: "", Delimiter: ",", Category: "U",
			}
			baseTypeCustom = backup.Type{
				Oid: 1, Type: "b", Schema: "public", Name: "base_type", Input: "public.base_fn_in", Output: "public.base_fn_out", Receive: "",
				Send: "", ModIn: "", ModOut: "", InternalLength: 8, IsPassedByValue: true, Alignment: "d", Storage: "p",
				DefaultVal: "0", Element: "integer", Delimiter: ";", Category: "U", StorageOptions: "compresstype=zlib, compresslevel=1, blocksize=32768",
			}
			compositeType = backup.Type{
				Oid: 1, Type: "c", Schema: "public", Name: "composite_type",
				Attributes: pq.StringArray{"\tname integer", "\tname2 numeric(8,2)", "\tname1 character(8)"},
			}
			enumType = backup.Type{
				Oid: 1, Type: "e", Schema: "public", Name: "enum_type", EnumLabels: "'label1',\n\t'label2',\n\t'label3'",
			}
			enumType2 = backup.Type{
				Oid: 1, Type: "e", Schema: "public", Name: "enum_type2", EnumLabels: "'label3',\n\t'label2',\n\t'label1'",
			}
		})
		It("returns a slice for a shell type", func() {
			testhelper.AssertQueryRuns(connection, "CREATE TYPE public.shell_type")
			defer testhelper.AssertQueryRuns(connection, "DROP TYPE public.shell_type")

			results := backup.GetShellTypes(connection)

			Expect(len(results)).To(Equal(1))
			structmatcher.ExpectStructsToMatchIncluding(&shellType, &results[0], "Schema", "Name", "Type")
		})
		It("returns a slice of composite types", func() {
			if connection.Version.AtLeast("6") {
				testhelper.AssertQueryRuns(connection, `CREATE COLLATION public.some_coll (lc_collate = 'POSIX', lc_ctype = 'POSIX');`)
				defer testhelper.AssertQueryRuns(connection, "DROP COLLATION public.some_coll")
				testhelper.AssertQueryRuns(connection, "CREATE TYPE public.composite_type AS (name int4, name2 numeric(8,2), name1 character(8) COLLATE public.some_coll);")
				compositeType.Attributes = pq.StringArray{"\tname integer", "\tname2 numeric(8,2)", "\tname1 character(8) COLLATE public.some_coll"}
			} else {
				testhelper.AssertQueryRuns(connection, "CREATE TYPE public.composite_type AS (name int4, name2 numeric(8,2), name1 character(8));")
			}
			defer testhelper.AssertQueryRuns(connection, "DROP TYPE public.composite_type")

			results := backup.GetCompositeTypes(connection)

			Expect(len(results)).To(Equal(1))
			structmatcher.ExpectStructsToMatchIncluding(&compositeType, &results[0], "Type", "Schema", "Name", "Attributes")
		})
		It("returns a slice for a base type with default values", func() {
			testhelper.AssertQueryRuns(connection, "CREATE TYPE public.base_type")
			defer testhelper.AssertQueryRuns(connection, "DROP TYPE public.base_type CASCADE")
			testhelper.AssertQueryRuns(connection, "CREATE FUNCTION public.base_fn_in(cstring) RETURNS public.base_type AS 'boolin' LANGUAGE internal")
			testhelper.AssertQueryRuns(connection, "CREATE FUNCTION public.base_fn_out(public.base_type) RETURNS cstring AS 'boolout' LANGUAGE internal")
			testhelper.AssertQueryRuns(connection, "CREATE TYPE public.base_type(INPUT=public.base_fn_in, OUTPUT=public.base_fn_out)")

			results := backup.GetBaseTypes(connection)

			Expect(len(results)).To(Equal(1))
			if connection.Version.Before("5") {
				structmatcher.ExpectStructsToMatchExcluding(&baseTypeDefault, &results[0], "Oid", "ModIn", "ModOut")
			} else {
				structmatcher.ExpectStructsToMatchExcluding(&baseTypeDefault, &results[0], "Oid")
			}
		})
		It("returns a slice for a base type with custom configuration", func() {
			testhelper.AssertQueryRuns(connection, "CREATE TYPE public.base_type")
			defer testhelper.AssertQueryRuns(connection, "DROP TYPE public.base_type CASCADE")
			testhelper.AssertQueryRuns(connection, "CREATE FUNCTION public.base_fn_in(cstring) RETURNS public.base_type AS 'boolin' LANGUAGE internal")
			testhelper.AssertQueryRuns(connection, "CREATE FUNCTION public.base_fn_out(public.base_type) RETURNS cstring AS 'boolout' LANGUAGE internal")
			if connection.Version.Before("6") {
				testhelper.AssertQueryRuns(connection, "CREATE TYPE public.base_type(INPUT=public.base_fn_in, OUTPUT=public.base_fn_out, INTERNALLENGTH=8, PASSEDBYVALUE, ALIGNMENT=double, STORAGE=plain, DEFAULT=0, ELEMENT=integer, DELIMITER=';')")
			} else {
				testhelper.AssertQueryRuns(connection, "CREATE TYPE public.base_type(INPUT=public.base_fn_in, OUTPUT=public.base_fn_out, INTERNALLENGTH=8, PASSEDBYVALUE, ALIGNMENT=double, STORAGE=plain, DEFAULT=0, ELEMENT=integer, DELIMITER=';', CATEGORY='N', PREFERRED=true, COLLATABLE=true)")
			}
			testhelper.AssertQueryRuns(connection, "ALTER TYPE public.base_type SET DEFAULT ENCODING (compresstype=zlib)")

			results := backup.GetBaseTypes(connection)

			Expect(len(results)).To(Equal(1))
			if connection.Version.Before("5") {
				structmatcher.ExpectStructsToMatchExcluding(&baseTypeCustom, &results[0], "Oid", "ModIn", "ModOut")
			} else if connection.Version.Before("6") {
				structmatcher.ExpectStructsToMatchExcluding(&baseTypeCustom, &results[0], "Oid")
			} else {
				baseTypeCustom.Category = "N"
				baseTypeCustom.Preferred = true
				baseTypeCustom.Collatable = true
				structmatcher.ExpectStructsToMatchExcluding(&baseTypeCustom, &results[0], "Oid")
			}
		})
		It("returns a slice for an enum type", func() {
			testutils.SkipIfBefore5(connection)
			testhelper.AssertQueryRuns(connection, "CREATE TYPE public.enum_type AS ENUM ('label1','label2','label3')")
			defer testhelper.AssertQueryRuns(connection, "DROP TYPE public.enum_type")

			results := backup.GetEnumTypes(connection)

			Expect(len(results)).To(Equal(1))
			structmatcher.ExpectStructsToMatchExcluding(&enumType, &results[0], "Oid")
		})
		It("returns a slice for enum types with labels in the correct order", func() {
			testutils.SkipIfBefore5(connection)

			testhelper.AssertQueryRuns(connection, "CREATE TYPE public.enum_type AS ENUM ('label1','label2','label3')")
			defer testhelper.AssertQueryRuns(connection, "DROP TYPE public.enum_type")
			if connection.Version.Before("6") {
				testhelper.AssertQueryRuns(connection, "CREATE TYPE public.enum_type2 AS ENUM ('label3','label2','label1')")
			} else {
				testhelper.AssertQueryRuns(connection, "CREATE TYPE public.enum_type2 AS ENUM ('label3', 'label1')")
				testhelper.AssertQueryRuns(connection, "ALTER TYPE public.enum_type2 ADD VALUE 'label2' BEFORE 'label1'")
			}
			defer testhelper.AssertQueryRuns(connection, "DROP TYPE public.enum_type2")

			results := backup.GetEnumTypes(connection)

			Expect(len(results)).To(Equal(2))
			structmatcher.ExpectStructsToMatchExcluding(&enumType, &results[0], "Oid")
			structmatcher.ExpectStructsToMatchExcluding(&enumType2, &results[1], "Oid")
		})
		It("does not return types for sequences or views", func() {
			testhelper.AssertQueryRuns(connection, "CREATE SEQUENCE public.my_sequence START 10")
			defer testhelper.AssertQueryRuns(connection, "DROP SEQUENCE public.my_sequence")
			testhelper.AssertQueryRuns(connection, "CREATE VIEW public.simpleview AS SELECT rolname FROM pg_roles")
			defer testhelper.AssertQueryRuns(connection, "DROP VIEW public.simpleview")

			results := backup.GetCompositeTypes(connection)

			Expect(len(results)).To(Equal(0))
		})
		It("does not return implicit base or composite types for tables with length > NAMEDATALEN", func() {
			testhelper.AssertQueryRuns(connection, "CREATE TABLE public.looooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooong(i int)")
			// The table's name will be truncated to 63 characters upon creation, as will the names of its implicit types
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.loooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo;")

			bases := backup.GetBaseTypes(connection)
			composites := backup.GetCompositeTypes(connection)

			Expect(len(bases)).To(Equal(0))
			Expect(len(composites)).To(Equal(0))
		})
		It("returns a slice for a domain type", func() {
			domainType := backup.Type{
				Oid: 1, Type: "d", Schema: "public", Name: "domain1", DefaultVal: "'abc'::bpchar", BaseType: "character(8)", NotNull: false,
			}
			if connection.Version.Before("6") {
				testhelper.AssertQueryRuns(connection, "CREATE DOMAIN public.domain1 AS character(8) DEFAULT 'abc'")
			} else {
				domainType.Collation = "public.some_coll"
				testhelper.AssertQueryRuns(connection, "CREATE COLLATION public.some_coll (lc_collate = 'POSIX', lc_ctype = 'POSIX')")
				defer testhelper.AssertQueryRuns(connection, "DROP COLLATION public.some_coll")
				testhelper.AssertQueryRuns(connection, "CREATE DOMAIN public.domain1 AS character(8) DEFAULT 'abc' COLLATE public.some_coll")
			}
			defer testhelper.AssertQueryRuns(connection, "DROP DOMAIN public.domain1")

			results := backup.GetDomainTypes(connection)

			Expect(len(results)).To(Equal(1))
			structmatcher.ExpectStructsToMatchIncluding(&domainType, &results[0], "Schema", "Name", "Type", "DefaultVal", "BaseType", "NotNull")
		})
		It("returns a slice for a type in a specific schema", func() {
			testhelper.AssertQueryRuns(connection, "CREATE TYPE public.shell_type")
			defer testhelper.AssertQueryRuns(connection, "DROP TYPE public.shell_type")
			testhelper.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
			defer testhelper.AssertQueryRuns(connection, "DROP SCHEMA testschema")
			testhelper.AssertQueryRuns(connection, "CREATE TYPE testschema.shell_type")
			defer testhelper.AssertQueryRuns(connection, "DROP TYPE testschema.shell_type")
			cmdFlags.Set(utils.INCLUDE_SCHEMA, "testschema")

			results := backup.GetShellTypes(connection)
			shellTypeOtherSchema := backup.Type{Type: "p", Schema: "testschema", Name: "shell_type"}

			Expect(len(results)).To(Equal(1))
			structmatcher.ExpectStructsToMatchIncluding(&shellTypeOtherSchema, &results[0], "Schema", "Name", "Type")
		})
	})
	Describe("ConstructCompositeTypeDependencies", func() {
		BeforeEach(func() {
			testhelper.AssertQueryRuns(connection, "CREATE FUNCTION public.base_fn_in(cstring) RETURNS public.base_type AS 'boolin' LANGUAGE internal")
			testhelper.AssertQueryRuns(connection, "CREATE FUNCTION public.base_fn_out(public.base_type) RETURNS cstring AS 'boolout' LANGUAGE internal")
			testhelper.AssertQueryRuns(connection, "CREATE FUNCTION public.base_fn_in2(cstring) RETURNS public.base_type2 AS 'boolin' LANGUAGE internal")
			testhelper.AssertQueryRuns(connection, "CREATE FUNCTION public.base_fn_out2(public.base_type2) RETURNS cstring AS 'boolout' LANGUAGE internal")
			testhelper.AssertQueryRuns(connection, "CREATE TYPE public.base_type(INPUT=public.base_fn_in, OUTPUT=public.base_fn_out)")
			testhelper.AssertQueryRuns(connection, "CREATE TYPE public.base_type2(INPUT=public.base_fn_in2, OUTPUT=public.base_fn_out2)")
		})
		AfterEach(func() {
			testhelper.AssertQueryRuns(connection, "DROP TYPE public.base_type CASCADE")
			testhelper.AssertQueryRuns(connection, "DROP TYPE public.base_type2 CASCADE")
		})
		It("constructs dependencies correctly for a composite type dependent on one user-defined type", func() {
			testhelper.AssertQueryRuns(connection, "CREATE TYPE public.comp_type AS (base public.base_type, builtin integer)")
			defer testhelper.AssertQueryRuns(connection, "DROP TYPE public.comp_type")

			composites := backup.GetCompositeTypes(connection)
			compTypes := backup.ConstructCompositeTypeDependencies(connection, composites)

			Expect(len(compTypes)).To(Equal(1))
			Expect(len(compTypes[0].DependsUpon)).To(Equal(1))
			Expect(compTypes[0].DependsUpon[0]).To(Equal("public.base_type"))
		})
		It("constructs dependencies correctly for a composite type dependent on multiple user-defined types", func() {
			testhelper.AssertQueryRuns(connection, "CREATE TYPE public.comp_type AS (base public.base_type, base2 public.base_type2)")
			defer testhelper.AssertQueryRuns(connection, "DROP TYPE public.comp_type")

			composites := backup.GetCompositeTypes(connection)
			compTypes := backup.ConstructCompositeTypeDependencies(connection, composites)

			Expect(len(compTypes)).To(Equal(1))
			Expect(len(compTypes[0].DependsUpon)).To(Equal(2))
			sort.Strings(compTypes[0].DependsUpon)
			Expect(compTypes[0].DependsUpon).To(Equal([]string{"public.base_type", "public.base_type2"}))
		})
		It("constructs dependencies correctly for a composite type dependent on the same user-defined type multiple times", func() {
			testhelper.AssertQueryRuns(connection, "CREATE TYPE public.comp_type AS (base public.base_type, base2 public.base_type)")
			defer testhelper.AssertQueryRuns(connection, "DROP TYPE public.comp_type")

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
			testhelper.AssertQueryRuns(connection, "CREATE FUNCTION public.base_fn_in(cstring) RETURNS public.base_type AS 'boolin' LANGUAGE internal")
			testhelper.AssertQueryRuns(connection, "CREATE FUNCTION public.base_fn_out(public.base_type) RETURNS cstring AS 'boolout' LANGUAGE internal")
			inOid := testutils.OidFromObjectName(connection, "public", "base_fn_in", backup.TYPE_FUNCTION)
			outOid := testutils.OidFromObjectName(connection, "public", "base_fn_out", backup.TYPE_FUNCTION)
			funcInfoMap[inOid] = backup.FunctionInfo{QualifiedName: "public.base_fn_in", Arguments: "cstring"}
			funcInfoMap[outOid] = backup.FunctionInfo{QualifiedName: "public.base_fn_out", Arguments: "public.base_type"}
		})
		AfterEach(func() {
			testhelper.AssertQueryRuns(connection, "DROP TYPE public.base_type CASCADE")
		})
		It("constructs dependencies on user-defined functions", func() {
			testhelper.AssertQueryRuns(connection, "CREATE TYPE public.base_type(INPUT=public.base_fn_in, OUTPUT=public.base_fn_out)")

			bases := backup.GetBaseTypes(connection)
			baseTypes := backup.ConstructBaseTypeDependencies4(connection, bases, funcInfoMap)

			Expect(len(baseTypes)).To(Equal(1))
			Expect(len(baseTypes[0].DependsUpon)).To(Equal(2))
			sort.Strings(baseTypes[0].DependsUpon)
			Expect(baseTypes[0].DependsUpon[0]).To(Equal("public.base_fn_in(cstring)"))
			Expect(baseTypes[0].DependsUpon[1]).To(Equal("public.base_fn_out(public.base_type)"))
		})
		It("doesn't construct dependencies on built-in functions", func() {
			testhelper.AssertQueryRuns(connection, "CREATE TYPE public.base_type(INPUT=public.base_fn_in, OUTPUT=public.base_fn_out, TYPMOD_IN=numerictypmodin, TYPMOD_OUT=numerictypmodout)")

			bases := backup.GetBaseTypes(connection)
			baseTypes := backup.ConstructBaseTypeDependencies4(connection, bases, funcInfoMap)

			Expect(len(baseTypes)).To(Equal(1))
			Expect(len(baseTypes[0].DependsUpon)).To(Equal(2))
			sort.Strings(baseTypes[0].DependsUpon)
			Expect(baseTypes[0].DependsUpon[0]).To(Equal("public.base_fn_in(cstring)"))
			Expect(baseTypes[0].DependsUpon[1]).To(Equal("public.base_fn_out(public.base_type)"))
		})
	})
	Describe("ConstructBaseTypeDependencies5", func() {
		BeforeEach(func() {
			testutils.SkipIfBefore5(connection)
			testhelper.AssertQueryRuns(connection, "CREATE FUNCTION public.base_fn_in(cstring) RETURNS public.base_type AS 'boolin' LANGUAGE internal")
			testhelper.AssertQueryRuns(connection, "CREATE FUNCTION public.base_fn_out(public.base_type) RETURNS cstring AS 'boolout' LANGUAGE internal")
		})
		AfterEach(func() {
			testhelper.AssertQueryRuns(connection, "DROP TYPE public.base_type CASCADE")
		})
		It("constructs dependencies on user-defined functions", func() {
			testhelper.AssertQueryRuns(connection, "CREATE TYPE public.base_type(INPUT=public.base_fn_in, OUTPUT=public.base_fn_out)")

			bases := backup.GetBaseTypes(connection)
			baseTypes := backup.ConstructBaseTypeDependencies5(connection, bases)

			Expect(len(baseTypes)).To(Equal(1))
			Expect(len(baseTypes[0].DependsUpon)).To(Equal(2))
			sort.Strings(baseTypes[0].DependsUpon)
			Expect(baseTypes[0].DependsUpon[0]).To(Equal("public.base_fn_in(cstring)"))
			Expect(baseTypes[0].DependsUpon[1]).To(Equal("public.base_fn_out(public.base_type)"))
		})
		It("doesn't construct dependencies on built-in functions", func() {
			testhelper.AssertQueryRuns(connection, "CREATE TYPE public.base_type(INPUT=public.base_fn_in, OUTPUT=public.base_fn_out, TYPMOD_IN=numerictypmodin, TYPMOD_OUT=numerictypmodout)")

			bases := backup.GetBaseTypes(connection)
			baseTypes := backup.ConstructBaseTypeDependencies5(connection, bases)

			Expect(len(baseTypes)).To(Equal(1))
			Expect(len(baseTypes[0].DependsUpon)).To(Equal(2))
			sort.Strings(baseTypes[0].DependsUpon)
			Expect(baseTypes[0].DependsUpon[0]).To(Equal("public.base_fn_in(cstring)"))
			Expect(baseTypes[0].DependsUpon[1]).To(Equal("public.base_fn_out(public.base_type)"))
		})
	})
	Describe("ConstructDomainDependencies", func() {
		It("constructs dependencies on user-defined types", func() {
			testhelper.AssertQueryRuns(connection, "CREATE DOMAIN public.parent_domain AS integer")
			defer testhelper.AssertQueryRuns(connection, "DROP DOMAIN public.parent_domain")
			testhelper.AssertQueryRuns(connection, "CREATE DOMAIN public.domain_type AS public.parent_domain")
			defer testhelper.AssertQueryRuns(connection, "DROP DOMAIN public.domain_type")

			domains := backup.GetDomainTypes(connection)
			domains = backup.ConstructDomainDependencies(connection, domains)

			Expect(len(domains)).To(Equal(2))
			Expect(len(domains[0].DependsUpon)).To(Equal(1))
			Expect(domains[0].DependsUpon[0]).To(Equal("public.parent_domain"))
		})
		It("doesn't construct dependencies on built-in types", func() {
			testhelper.AssertQueryRuns(connection, "CREATE DOMAIN public.parent_domain AS integer")
			defer testhelper.AssertQueryRuns(connection, "DROP DOMAIN public.parent_domain")

			domains := backup.GetDomainTypes(connection)
			domains = backup.ConstructDomainDependencies(connection, domains)

			Expect(len(domains)).To(Equal(1))
			Expect(domains[0].DependsUpon).To(BeNil())
		})
	})
	Describe("GetCollations", func() {
		It("returns a slice of collations", func() {
			testutils.SkipIfBefore6(connection)
			testhelper.AssertQueryRuns(connection, `CREATE COLLATION public.some_coll (lc_collate = 'POSIX', lc_ctype = 'POSIX');`)
			defer testhelper.AssertQueryRuns(connection, "DROP COLLATION public.some_coll")

			results := backup.GetCollations(connection)

			Expect(len(results)).To(Equal(1))

			collationDef := backup.Collation{Oid: 0, Schema: "public", Name: "some_coll", Collate: "POSIX", Ctype: "POSIX"}
			structmatcher.ExpectStructsToMatchExcluding(&collationDef, &results[0], "Oid")

		})
		It("returns a slice of collations in a specific schema", func() {
			testutils.SkipIfBefore6(connection)
			testhelper.AssertQueryRuns(connection, `CREATE COLLATION public.some_coll (lc_collate = 'POSIX', lc_ctype = 'POSIX');`)
			defer testhelper.AssertQueryRuns(connection, "DROP COLLATION public.some_coll")
			testhelper.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
			defer testhelper.AssertQueryRuns(connection, "DROP SCHEMA testschema")
			testhelper.AssertQueryRuns(connection, `CREATE COLLATION testschema.some_coll (lc_collate = 'POSIX', lc_ctype = 'POSIX');`)
			defer testhelper.AssertQueryRuns(connection, "DROP COLLATION testschema.some_coll")
			cmdFlags.Set(utils.INCLUDE_SCHEMA, "testschema")

			results := backup.GetCollations(connection)

			Expect(len(results)).To(Equal(1))

			collationDef := backup.Collation{Oid: 0, Schema: "testschema", Name: "some_coll", Collate: "POSIX", Ctype: "POSIX"}
			structmatcher.ExpectStructsToMatchExcluding(&collationDef, &results[0], "Oid")

		})
	})
})
