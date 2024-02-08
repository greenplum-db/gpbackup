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
	Describe("Get[type]Types functions", func() {
		var (
			shellType       backup.ShellType
			baseTypeDefault backup.BaseType
			baseTypeCustom  backup.BaseType
			compositeType   backup.CompositeType
			enumType        backup.EnumType
			enumType2       backup.EnumType
		)
		BeforeEach(func() {
			shellType = backup.ShellType{Schema: "public", Name: "shell_type"}
			baseTypeDefault = backup.BaseType{
				Oid: 1, Schema: "public", Name: "base_type", Input: "public.base_fn_in", Output: "public.base_fn_out", Receive: "",
				Send: "", ModIn: "", ModOut: "", InternalLength: -1, IsPassedByValue: false, Alignment: "i", Storage: "p",
				DefaultVal: "", Element: "", Delimiter: ",", Category: "U",
			}
			baseTypeCustom = backup.BaseType{
				Oid: 1, Schema: "public", Name: "base_type", Input: "public.base_fn_in", Output: "public.base_fn_out", Receive: "",
				Send: "", ModIn: "", ModOut: "", InternalLength: 8, IsPassedByValue: true, Alignment: "d", Storage: "p",
				DefaultVal: "0", Element: "integer", Delimiter: ";", Category: "U", StorageOptions: "compresstype=zlib, compresslevel=1, blocksize=32768",
			}
			compositeType = backup.CompositeType{
				Oid: 1, Schema: "public", Name: "composite_type",
				Attributes: []backup.Attribute{
					{Name: "name", Type: "integer"},
					{Name: "name2", Type: "numeric(8,2)"},
					{Name: "name1", Type: "character(8)"},
				},
			}
			enumType = backup.EnumType{
				Oid: 1, Schema: "public", Name: "enum_type", EnumLabels: "'label1',\n\t'label2',\n\t'label3'",
			}
			enumType2 = backup.EnumType{
				Oid: 1, Schema: "public", Name: "enum_type2", EnumLabels: "'label3',\n\t'label2',\n\t'label1'",
			}
		})
		It("returns a slice for a shell type", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TYPE public.shell_type")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TYPE public.shell_type")

			results := backup.GetShellTypes(connectionPool)

			Expect(results).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchIncluding(&shellType, &results[0], "Schema", "Name", "Type")
		})
		It("returns a slice of composite types", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TYPE public.composite_type AS (name int4, name2 numeric(8,2), name1 character(8));")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TYPE public.composite_type")

			results := backup.GetCompositeTypes(connectionPool)

			Expect(results).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&compositeType, &results[0], "Oid", "Attributes.CompositeTypeOid")
		})
		It("returns a slice of composite types with attribute comments", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TYPE public.composite_type AS (name int4, name2 numeric(8,2), name1 character(8));")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TYPE public.composite_type")
			testhelper.AssertQueryRuns(connectionPool, "COMMENT ON COLUMN public.composite_type.name IS 'name comment';")

			results := backup.GetCompositeTypes(connectionPool)

			Expect(results).To(HaveLen(1))
			compositeType.Attributes[0].Comment = "'name comment'"
			structmatcher.ExpectStructsToMatchExcluding(&compositeType, &results[0], "Oid", "Attributes.CompositeTypeOid")
		})
		It("returns a slice of composite types with collations", func() {
			testutils.SkipIfBefore6(connectionPool)
			testhelper.AssertQueryRuns(connectionPool, `CREATE COLLATION public.some_coll (lc_collate = 'POSIX', lc_ctype = 'POSIX');`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP COLLATION public.some_coll")
			testhelper.AssertQueryRuns(connectionPool, "CREATE TYPE public.composite_type AS (name int4, name2 numeric(8,2), name1 character(8) COLLATE public.some_coll);")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TYPE public.composite_type")

			results := backup.GetCompositeTypes(connectionPool)

			Expect(results).To(HaveLen(1))
			compositeType.Attributes = []backup.Attribute{
				{Name: "name", Type: "integer"},
				{Name: "name2", Type: "numeric(8,2)"},
				{Name: "name1", Type: "character(8)", Collation: "public.some_coll"},
			}
			structmatcher.ExpectStructsToMatchExcluding(&compositeType, &results[0], "Oid", "Attributes.CompositeTypeOid")
		})
		It("returns a slice for a base type with default values", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TYPE public.base_type")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TYPE public.base_type CASCADE")
			testhelper.AssertQueryRuns(connectionPool, "CREATE FUNCTION public.base_fn_in(cstring) RETURNS public.base_type AS 'boolin' LANGUAGE internal")
			testhelper.AssertQueryRuns(connectionPool, "CREATE FUNCTION public.base_fn_out(public.base_type) RETURNS cstring AS 'boolout' LANGUAGE internal")
			testhelper.AssertQueryRuns(connectionPool, "CREATE TYPE public.base_type(INPUT=public.base_fn_in, OUTPUT=public.base_fn_out)")

			results := backup.GetBaseTypes(connectionPool)

			Expect(results).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&baseTypeDefault, &results[0], "Oid")

		})
		It("returns a slice for a base type with custom configuration", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TYPE public.base_type")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TYPE public.base_type CASCADE")
			testhelper.AssertQueryRuns(connectionPool, "CREATE FUNCTION public.base_fn_in(cstring) RETURNS public.base_type AS 'boolin' LANGUAGE internal")
			testhelper.AssertQueryRuns(connectionPool, "CREATE FUNCTION public.base_fn_out(public.base_type) RETURNS cstring AS 'boolout' LANGUAGE internal")
			if connectionPool.Version.Before("6") {
				testhelper.AssertQueryRuns(connectionPool, "CREATE TYPE public.base_type(INPUT=public.base_fn_in, OUTPUT=public.base_fn_out, INTERNALLENGTH=8, PASSEDBYVALUE, ALIGNMENT=double, STORAGE=plain, DEFAULT=0, ELEMENT=integer, DELIMITER=';')")
			} else {
				testhelper.AssertQueryRuns(connectionPool, "CREATE TYPE public.base_type(INPUT=public.base_fn_in, OUTPUT=public.base_fn_out, INTERNALLENGTH=8, PASSEDBYVALUE, ALIGNMENT=double, STORAGE=plain, DEFAULT=0, ELEMENT=integer, DELIMITER=';', CATEGORY='N', PREFERRED=true, COLLATABLE=true)")
			}
			testhelper.AssertQueryRuns(connectionPool, "ALTER TYPE public.base_type SET DEFAULT ENCODING (compresstype=zlib)")

			results := backup.GetBaseTypes(connectionPool)

			Expect(results).To(HaveLen(1))
			if connectionPool.Version.Before("6") {
				structmatcher.ExpectStructsToMatchExcluding(&baseTypeCustom, &results[0], "Oid")
			} else {
				baseTypeCustom.Category = "N"
				baseTypeCustom.Preferred = true
				baseTypeCustom.Collatable = true
				structmatcher.ExpectStructsToMatchExcluding(&baseTypeCustom, &results[0], "Oid")
			}
		})
		It("returns a slice for a user-created array type", func() {
			arrayType := backup.BaseType{
				Oid: 1, Schema: "public", Name: "base_array_type", Input: "public.base_array_fn_in", Output: "public.base_array_fn_out", Receive: "",
				Send: "", ModIn: "", ModOut: "", InternalLength: -1, IsPassedByValue: false, Alignment: "i", Storage: "p",
				DefaultVal: "", Element: "text", Delimiter: ",", Category: "U",
			}

			testhelper.AssertQueryRuns(connectionPool, "CREATE TYPE public.base_array_type")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TYPE public.base_array_type CASCADE")
			testhelper.AssertQueryRuns(connectionPool, "CREATE FUNCTION public.base_array_fn_in(cstring) RETURNS public.base_array_type AS 'boolin' LANGUAGE internal")
			testhelper.AssertQueryRuns(connectionPool, "CREATE FUNCTION public.base_array_fn_out(public.base_array_type) RETURNS cstring AS 'boolout' LANGUAGE internal")
			testhelper.AssertQueryRuns(connectionPool, "CREATE TYPE public.base_array_type(INPUT=public.base_array_fn_in, OUTPUT=public.base_array_fn_out, ELEMENT=text)")

			results := backup.GetBaseTypes(connectionPool)

			Expect(results).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&arrayType, &results[0], "Oid")
		})
		It("returns a slice for an enum type", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TYPE public.enum_type AS ENUM ('label1','label2','label3')")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TYPE public.enum_type")

			results := backup.GetEnumTypes(connectionPool)

			Expect(results).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&enumType, &results[0], "Oid")
		})
		It("returns a slice for enum types with labels in the correct order", func() {

			testhelper.AssertQueryRuns(connectionPool, "CREATE TYPE public.enum_type AS ENUM ('label1','label2','label3')")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TYPE public.enum_type")
			if connectionPool.Version.Before("6") {
				testhelper.AssertQueryRuns(connectionPool, "CREATE TYPE public.enum_type2 AS ENUM ('label3','label2','label1')")
			} else {
				testhelper.AssertQueryRuns(connectionPool, "CREATE TYPE public.enum_type2 AS ENUM ('label3', 'label1')")
				testhelper.AssertQueryRuns(connectionPool, "ALTER TYPE public.enum_type2 ADD VALUE 'label2' BEFORE 'label1'")
			}
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TYPE public.enum_type2")

			results := backup.GetEnumTypes(connectionPool)

			Expect(results).To(HaveLen(2))
			structmatcher.ExpectStructsToMatchExcluding(&enumType, &results[0], "Oid")
			structmatcher.ExpectStructsToMatchExcluding(&enumType2, &results[1], "Oid")
		})
		It("does not return types for sequences or views", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE SEQUENCE public.my_sequence START 10")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SEQUENCE public.my_sequence")
			testhelper.AssertQueryRuns(connectionPool, "CREATE VIEW public.simpleview AS SELECT rolname FROM pg_roles")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP VIEW public.simpleview")

			bases := backup.GetBaseTypes(connectionPool)
			composites := backup.GetCompositeTypes(connectionPool)

			Expect(bases).To(BeEmpty())
			Expect(composites).To(BeEmpty())
		})
		It("does not return types for foreign tables", func() {
			testutils.SkipIfBefore6(connectionPool)
			testhelper.AssertQueryRuns(connectionPool, "CREATE FOREIGN DATA WRAPPER foreignwrapper")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP FOREIGN DATA WRAPPER foreignwrapper")
			testhelper.AssertQueryRuns(connectionPool, "CREATE SERVER foreignserver FOREIGN DATA WRAPPER foreignwrapper")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SERVER foreignserver")
			testhelper.AssertQueryRuns(connectionPool, "CREATE FOREIGN TABLE public.ft1 (c1 integer) SERVER foreignserver;")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP FOREIGN TABLE public.ft1")

			results := backup.GetCompositeTypes(connectionPool)

			Expect(results).To(BeEmpty())
		})
		It("does not return implicit base or composite types for tables with length > NAMEDATALEN", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.looooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooong(i int)")
			// The table's name will be truncated to 63 characters upon creation, as will the names of its implicit types
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.loooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo;")

			bases := backup.GetBaseTypes(connectionPool)
			composites := backup.GetCompositeTypes(connectionPool)

			Expect(bases).To(BeEmpty())
			Expect(composites).To(BeEmpty())
		})
		It("returns a slice for a domain type", func() {
			domainType := backup.Domain{
				Oid: 1, Schema: "public", Name: "domain1", DefaultVal: "'abc'::bpchar", BaseType: "character(8)", NotNull: false, Collation: "",
			}
			testhelper.AssertQueryRuns(connectionPool, "CREATE DOMAIN public.domain1 AS character(8) DEFAULT 'abc'")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP DOMAIN public.domain1")

			results := backup.GetDomainTypes(connectionPool)

			Expect(results).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&domainType, &results[0], "Oid")
		})
		It("returns a slice for a domain type with a collation", func() {
			testutils.SkipIfBefore6(connectionPool)
			domainType := backup.Domain{
				Oid: 1, Schema: "public", Name: "domain1", DefaultVal: "'abc'::bpchar", BaseType: "character(8)", NotNull: false, Collation: "public.some_coll",
			}
			testhelper.AssertQueryRuns(connectionPool, "CREATE COLLATION public.some_coll (lc_collate = 'POSIX', lc_ctype = 'POSIX')")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP COLLATION public.some_coll")
			testhelper.AssertQueryRuns(connectionPool, "CREATE DOMAIN public.domain1 AS character(8) DEFAULT 'abc' COLLATE public.some_coll")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP DOMAIN public.domain1")

			results := backup.GetDomainTypes(connectionPool)

			Expect(results).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&domainType, &results[0], "Oid")
		})
		It("returns a slice for a type in a specific schema", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TYPE public.shell_type")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TYPE public.shell_type")
			testhelper.AssertQueryRuns(connectionPool, "CREATE SCHEMA testschema")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA testschema")
			testhelper.AssertQueryRuns(connectionPool, "CREATE TYPE testschema.shell_type")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TYPE testschema.shell_type")
			_ = backupCmdFlags.Set(options.INCLUDE_SCHEMA, "testschema")

			results := backup.GetShellTypes(connectionPool)
			shellTypeOtherSchema := backup.ShellType{Schema: "testschema", Name: "shell_type"}

			Expect(results).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&shellTypeOtherSchema, &results[0], "Oid")
		})
	})
	Describe("GetRangeTypes", func() {
		It("returns a slice of a range type with a collation", func() {
			testutils.SkipIfBefore6(connectionPool)
			testhelper.AssertQueryRuns(connectionPool, "CREATE COLLATION public.some_coll (lc_collate = 'POSIX', lc_ctype = 'POSIX');")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP COLLATION public.some_coll")
			testhelper.AssertQueryRuns(connectionPool, `CREATE TYPE public.textrange AS RANGE (
	SUBTYPE = pg_catalog.text,
	COLLATION = public.some_coll
);`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TYPE public.textrange")
			results := backup.GetRangeTypes(connectionPool)

			Expect(len(results)).To(Equal(1))

			expectedRangeType := backup.RangeType{
				Oid:            0,
				Schema:         "public",
				Name:           "textrange",
				SubType:        "text",
				Collation:      "public.some_coll",
				SubTypeOpClass: "pg_catalog.text_ops",
			}
			structmatcher.ExpectStructsToMatchExcluding(&expectedRangeType, &results[0], "Oid")
		})
		It("returns a slice of a range type in a specific schema with a subtype diff function", func() {
			testutils.SkipIfBefore6(connectionPool)
			testhelper.AssertQueryRuns(connectionPool, "CREATE SCHEMA testschema;")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA testschema CASCADE;")
			testhelper.AssertQueryRuns(connectionPool, "CREATE FUNCTION testschema.time_subtype_diff(x time, y time) RETURNS float8 AS 'SELECT EXTRACT(EPOCH FROM (x - y))' LANGUAGE sql STRICT IMMUTABLE;")
			testhelper.AssertQueryRuns(connectionPool, `CREATE TYPE testschema.timerange AS RANGE (
	SUBTYPE = pg_catalog.time,
	SUBTYPE_DIFF = testschema.time_subtype_diff
);`)

			results := backup.GetRangeTypes(connectionPool)

			Expect(len(results)).To(Equal(1))

			expectedRangeType := backup.RangeType{
				Oid:            0,
				Schema:         "testschema",
				Name:           "timerange",
				SubType:        "time without time zone",
				SubTypeOpClass: "pg_catalog.time_ops",
				SubTypeDiff:    "testschema.time_subtype_diff",
			}
			structmatcher.ExpectStructsToMatchExcluding(&expectedRangeType, &results[0], "Oid")
		})
	})
	Describe("GetCollations", func() {
		It("returns a slice of collations", func() {
			testutils.SkipIfBefore6(connectionPool)
			testhelper.AssertQueryRuns(connectionPool, `CREATE COLLATION public.some_coll (lc_collate = 'POSIX', lc_ctype = 'POSIX');`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP COLLATION public.some_coll")

			results := backup.GetCollations(connectionPool)

			Expect(results).To(HaveLen(1))

			collationDef := backup.Collation{Oid: 0, Schema: "public", Name: "some_coll", Collate: "POSIX", Ctype: "POSIX"}
			if connectionPool.Version.AtLeast("7") {
				collationDef.IsDeterministic = "true"
				collationDef.Provider = "c"
			}
			structmatcher.ExpectStructsToMatchExcluding(&collationDef, &results[0], "Oid")

		})
		It("returns a slice of collations in a specific schema", func() {
			testutils.SkipIfBefore6(connectionPool)
			testhelper.AssertQueryRuns(connectionPool, `CREATE COLLATION public.some_coll (lc_collate = 'POSIX', lc_ctype = 'POSIX');`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP COLLATION public.some_coll")
			testhelper.AssertQueryRuns(connectionPool, "CREATE SCHEMA testschema")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA testschema")
			testhelper.AssertQueryRuns(connectionPool, `CREATE COLLATION testschema.some_coll (lc_collate = 'POSIX', lc_ctype = 'POSIX');`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP COLLATION testschema.some_coll")
			_ = backupCmdFlags.Set(options.INCLUDE_SCHEMA, "testschema")

			results := backup.GetCollations(connectionPool)

			Expect(results).To(HaveLen(1))

			collationDef := backup.Collation{Oid: 0, Schema: "testschema", Name: "some_coll", Collate: "POSIX", Ctype: "POSIX"}
			if connectionPool.Version.AtLeast("7") {
				collationDef.IsDeterministic = "true"
				collationDef.Provider = "c"
			}
			structmatcher.ExpectStructsToMatchExcluding(&collationDef, &results[0], "Oid")

		})
		It("returns a slice of with specific collation", func() {
			testutils.SkipIfBefore7(connectionPool)
			testhelper.AssertQueryRuns(connectionPool, `CREATE COLLATION public.some_coll (provider = 'libc', locale = 'de_DE');`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP COLLATION public.some_coll")

			results := backup.GetCollations(connectionPool)

			Expect(results).To(HaveLen(1))

			collationDef := backup.Collation{Oid: 0, Schema: "public", Name: "some_coll", Collate: "de_DE", Ctype: "de_DE", Provider: "c", IsDeterministic: "true"}
			structmatcher.ExpectStructsToMatchExcluding(&collationDef, &results[0], "Oid")

		})
	})
})
