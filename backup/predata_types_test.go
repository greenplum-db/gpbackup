package backup_test

import (
	"database/sql"
	"fmt"

	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/toc"

	. "github.com/onsi/ginkgo/v2"
)

var _ = Describe("backup/predata_types tests", func() {
	emptyMetadata := backup.ObjectMetadata{}
	emptyMetadataMap := backup.MetadataMap{}
	typeMetadata := testutils.DefaultMetadata(toc.OBJ_TYPE, false, true, true, true)
	typeMetadataMap := testutils.DefaultMetadataMap(toc.OBJ_TYPE, false, true, true, true)

	BeforeEach(func() {
		tocfile, backupfile = testutils.InitializeTestTOC(buffer, "predata")
	})
	Describe("PrintCreateEnumTypeStatements", func() {
		enumOne := backup.EnumType{Oid: 1, Schema: "public", Name: "enum_type", EnumLabels: "'bar',\n\t'baz',\n\t'foo'"}
		enumTwo := backup.EnumType{Oid: 1, Schema: "public", Name: "enum_type", EnumLabels: "'bar',\n\t'baz',\n\t'foo'"}

		It("prints an enum type with multiple attributes", func() {
			backup.PrintCreateEnumTypeStatements(backupfile, tocfile, []backup.EnumType{enumOne}, emptyMetadataMap)
			testutils.ExpectEntry(tocfile.PredataEntries, 0, "public", "", "enum_type", toc.OBJ_TYPE)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE TYPE public.enum_type AS ENUM (
	'bar',
	'baz',
	'foo'
);`)
		})
		It("prints an enum type with comment, security label, and owner", func() {
			backup.PrintCreateEnumTypeStatements(backupfile, tocfile, []backup.EnumType{enumTwo}, typeMetadataMap)
			expectedStatements := []string{`CREATE TYPE public.enum_type AS ENUM (
	'bar',
	'baz',
	'foo'
);`,
				"COMMENT ON TYPE public.enum_type IS 'This is a type comment.';",
				"ALTER TYPE public.enum_type OWNER TO testrole;",
				"SECURITY LABEL FOR dummy ON TYPE public.enum_type IS 'unclassified';"}
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, expectedStatements...)
		})
	})
	Describe("PrintCreateCompositeTypeStatement", func() {
		var compType backup.CompositeType
		var oneAtt, oneAttWithCollation, twoAtts, attWithComment []backup.Attribute
		BeforeEach(func() {
			compType = backup.CompositeType{Oid: 1, Schema: "public", Name: "composite_type"}
			oneAtt = []backup.Attribute{{Name: "foo", Type: "integer"}}
			oneAttWithCollation = []backup.Attribute{{Name: "foo", Type: "integer", Collation: "public.some_coll"}}
			twoAtts = []backup.Attribute{{Name: "foo", Type: "integer"}, {Name: "bar", Type: "text"}}
			attWithComment = []backup.Attribute{{Name: "foo", Type: "integer", Comment: "'attribute comment'"}}
		})

		It("prints a composite type with one attribute", func() {
			compType.Attributes = oneAtt
			backup.PrintCreateCompositeTypeStatement(backupfile, tocfile, compType, emptyMetadata)
			testutils.ExpectEntry(tocfile.PredataEntries, 0, "public", "", "composite_type", toc.OBJ_TYPE)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE TYPE public.composite_type AS (
	foo integer
);`)
		})
		It("prints a composite type with one attribute with a collation", func() {
			compType.Attributes = oneAttWithCollation
			backup.PrintCreateCompositeTypeStatement(backupfile, tocfile, compType, emptyMetadata)
			testutils.ExpectEntry(tocfile.PredataEntries, 0, "public", "", "composite_type", toc.OBJ_TYPE)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE TYPE public.composite_type AS (
	foo integer COLLATE public.some_coll
);`)
		})
		It("prints a composite type with multiple attributes", func() {
			compType.Attributes = twoAtts
			backup.PrintCreateCompositeTypeStatement(backupfile, tocfile, compType, emptyMetadata)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE TYPE public.composite_type AS (
	foo integer,
	bar text
);`)
		})
		It("prints a composite type with comment, security label, and owner", func() {
			compType.Attributes = twoAtts
			backup.PrintCreateCompositeTypeStatement(backupfile, tocfile, compType, typeMetadata)
			expectedEntries := []string{`CREATE TYPE public.composite_type AS (
	foo integer,
	bar text
);`,
				"COMMENT ON TYPE public.composite_type IS 'This is a type comment.';",
				"ALTER TYPE public.composite_type OWNER TO testrole;",
				"SECURITY LABEL FOR dummy ON TYPE public.composite_type IS 'unclassified';"}
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, expectedEntries...)
		})
		It("prints a composite type with attribute comment", func() {
			compType.Attributes = attWithComment
			backup.PrintCreateCompositeTypeStatement(backupfile, tocfile, compType, emptyMetadata)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE TYPE public.composite_type AS (
	foo integer
);`, "COMMENT ON COLUMN public.composite_type.foo IS 'attribute comment';")
		})
	})
	Describe("PrintCreateBaseTypeStatement", func() {
		baseSimple := backup.BaseType{Oid: 1, Schema: "public", Name: "base_type", Input: "input_fn", Output: "output_fn", Receive: "", Send: "", ModIn: "", ModOut: "", InternalLength: -1, IsPassedByValue: false, Alignment: "c", Storage: "p", DefaultVal: "", Element: "", Category: "U", Preferred: false, Delimiter: ""}
		basePartial := backup.BaseType{Oid: 1, Schema: "public", Name: "base_type", Input: "input_fn", Output: "output_fn", Receive: "receive_fn", Send: "send_fn", ModIn: "modin_fn", ModOut: "modout_fn", InternalLength: -1, IsPassedByValue: false, Alignment: "c", Storage: "p", DefaultVal: "42", Element: "int4", Category: "U", Delimiter: ","}
		baseFull := backup.BaseType{Oid: 1, Schema: "public", Name: "base_type", Input: "input_fn", Output: "output_fn", Receive: "receive_fn", Send: "send_fn", ModIn: "modin_fn", ModOut: "modout_fn", InternalLength: 16, IsPassedByValue: true, Alignment: "s", Storage: "e", DefaultVal: "42", Element: "int4", Category: "N", Preferred: true, Delimiter: ",", StorageOptions: "compresstype=zlib, compresslevel=1, blocksize=32768", Collatable: true}
		basePermOne := backup.BaseType{Oid: 1, Schema: "public", Name: "base_type", Input: "input_fn", Output: "output_fn", Receive: "", Send: "", ModIn: "", ModOut: "", InternalLength: -1, IsPassedByValue: false, Alignment: "d", Storage: "m", DefaultVal: "", Element: "", Category: "U", Delimiter: ""}
		basePermTwo := backup.BaseType{Oid: 1, Schema: "public", Name: "base_type", Input: "input_fn", Output: "output_fn", Receive: "", Send: "", ModIn: "", ModOut: "", InternalLength: -1, IsPassedByValue: false, Alignment: "i", Storage: "x", DefaultVal: "", Element: "", Category: "U", Delimiter: ""}

		It("prints a base type with no optional arguments", func() {
			backup.PrintCreateBaseTypeStatement(backupfile, tocfile, baseSimple, emptyMetadata)
			testutils.ExpectEntry(tocfile.PredataEntries, 0, "public", "", "base_type", toc.OBJ_TYPE)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE TYPE public.base_type (
	INPUT = input_fn,
	OUTPUT = output_fn
);`)
		})
		It("prints a base type where all optional arguments have default values where possible", func() {
			expectedArgsReplace := ""
			if connectionPool.Version.AtLeast("5") {
				expectedArgsReplace = `
	TYPMOD_IN = modin_fn,
	TYPMOD_OUT = modout_fn,`
			}

			backup.PrintCreateBaseTypeStatement(backupfile, tocfile, basePartial, emptyMetadata)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, fmt.Sprintf(`CREATE TYPE public.base_type (
	INPUT = input_fn,
	OUTPUT = output_fn,
	RECEIVE = receive_fn,
	SEND = send_fn,%s
	DEFAULT = '42',
	ELEMENT = int4,
	DELIMITER = ','
);`, expectedArgsReplace))
		})
		It("prints a base type with all optional arguments provided", func() {
			expectedArgsReplace := ""
			if connectionPool.Version.AtLeast("5") {
				expectedArgsReplace = `
	TYPMOD_IN = modin_fn,
	TYPMOD_OUT = modout_fn,`
			}

			backup.PrintCreateBaseTypeStatement(backupfile, tocfile, baseFull, emptyMetadata)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, fmt.Sprintf(`CREATE TYPE public.base_type (
	INPUT = input_fn,
	OUTPUT = output_fn,
	RECEIVE = receive_fn,
	SEND = send_fn,%s
	INTERNALLENGTH = 16,
	PASSEDBYVALUE,
	ALIGNMENT = int2,
	STORAGE = external,
	DEFAULT = '42',
	ELEMENT = int4,
	DELIMITER = ',',
	CATEGORY = 'N',
	PREFERRED = true,
	COLLATABLE = true
);

ALTER TYPE public.base_type
	SET DEFAULT ENCODING (compresstype=zlib, compresslevel=1, blocksize=32768);`, expectedArgsReplace))
		})
		It("prints a base type with double alignment and main storage", func() {
			backup.PrintCreateBaseTypeStatement(backupfile, tocfile, basePermOne, emptyMetadata)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE TYPE public.base_type (
	INPUT = input_fn,
	OUTPUT = output_fn,
	ALIGNMENT = double,
	STORAGE = main
);`)
		})
		It("prints a base type with int4 alignment and external storage", func() {
			backup.PrintCreateBaseTypeStatement(backupfile, tocfile, basePermTwo, emptyMetadata)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE TYPE public.base_type (
	INPUT = input_fn,
	OUTPUT = output_fn,
	ALIGNMENT = int4,
	STORAGE = extended
);`)
		})
		It("prints a base type with comment, security label, and owner", func() {
			backup.PrintCreateBaseTypeStatement(backupfile, tocfile, baseSimple, typeMetadata)
			expectedEntries := []string{`CREATE TYPE public.base_type (
	INPUT = input_fn,
	OUTPUT = output_fn
);`,
				"COMMENT ON TYPE public.base_type IS 'This is a type comment.';",
				"ALTER TYPE public.base_type OWNER TO testrole;",
				"SECURITY LABEL FOR dummy ON TYPE public.base_type IS 'unclassified';"}
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, expectedEntries...)
		})
	})
	Describe("PrintCreateShellTypeStatements", func() {
		shellOne := backup.ShellType{Oid: 1, Schema: "public", Name: "shell_type1"}
		baseOne := backup.BaseType{Oid: 1, Schema: "public", Name: "base_type1", Input: "input_fn", Output: "output_fn", Receive: "", Send: "", ModIn: "", ModOut: "", InternalLength: -1, IsPassedByValue: false, Alignment: "c", Storage: "p", DefaultVal: "", Element: "", Category: "U", Delimiter: ""}
		baseTwo := backup.BaseType{Oid: 1, Schema: "public", Name: "base_type2", Input: "input_fn", Output: "output_fn", Receive: "", Send: "", ModIn: "", ModOut: "", InternalLength: -1, IsPassedByValue: false, Alignment: "c", Storage: "p", DefaultVal: "", Element: "", Category: "U", Delimiter: ""}
		rangeOne := backup.RangeType{Oid: 1, Schema: "public", Name: "range_type1"}
		It("prints shell type for a shell type", func() {
			backup.PrintCreateShellTypeStatements(backupfile, tocfile, []backup.ShellType{shellOne}, []backup.BaseType{}, []backup.RangeType{})
			testutils.ExpectEntry(tocfile.PredataEntries, 0, "public", "", "shell_type1", toc.OBJ_TYPE)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, "CREATE TYPE public.shell_type1;")
		})
		It("prints shell type for a base type", func() {
			backup.PrintCreateShellTypeStatements(backupfile, tocfile, []backup.ShellType{}, []backup.BaseType{baseOne, baseTwo}, []backup.RangeType{})
			testutils.ExpectEntry(tocfile.PredataEntries, 0, "public", "", "base_type1", toc.OBJ_TYPE)
			testutils.ExpectEntry(tocfile.PredataEntries, 1, "public", "", "base_type2", toc.OBJ_TYPE)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, "CREATE TYPE public.base_type1;", "CREATE TYPE public.base_type2;")
		})
		It("prints shell type for a range type", func() {
			backup.PrintCreateShellTypeStatements(backupfile, tocfile, []backup.ShellType{}, []backup.BaseType{}, []backup.RangeType{rangeOne})
			testutils.ExpectEntry(tocfile.PredataEntries, 0, "public", "", "range_type1", toc.OBJ_TYPE)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, "CREATE TYPE public.range_type1;")
		})
	})
	Describe("PrintCreateDomainStatement", func() {
		emptyConstraint := make([]backup.Constraint, 0)
		checkConstraint := []backup.Constraint{{Name: "domain1_check", Def: sql.NullString{String: "CHECK (VALUE > 2)", Valid: true}, OwningObject: "public.domain1"}}
		domainOne := backup.Domain{Oid: 1, Schema: "public", Name: "domain1", DefaultVal: "4", BaseType: "numeric", NotNull: true, Collation: "public.mycollation"}
		domainTwo := backup.Domain{Oid: 1, Schema: "public", Name: "domain2", DefaultVal: "", BaseType: "varchar", NotNull: false, Collation: ""}
		It("prints a domain with a constraint", func() {
			backup.PrintCreateDomainStatement(backupfile, tocfile, domainOne, emptyMetadata, checkConstraint)
			testutils.ExpectEntry(tocfile.PredataEntries, 0, "public", "", "domain1", toc.OBJ_DOMAIN)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE DOMAIN public.domain1 AS numeric DEFAULT 4 COLLATE public.mycollation NOT NULL
	CONSTRAINT domain1_check CHECK (VALUE > 2);`)
		})
		It("prints a domain without constraint", func() {
			backup.PrintCreateDomainStatement(backupfile, tocfile, domainOne, emptyMetadata, emptyConstraint)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE DOMAIN public.domain1 AS numeric DEFAULT 4 COLLATE public.mycollation NOT NULL;`)
		})
		It("prints a domain without constraint with comment, security label, and owner", func() {
			domainMetadata := testutils.DefaultMetadata(toc.OBJ_DOMAIN, false, true, true, true)
			backup.PrintCreateDomainStatement(backupfile, tocfile, domainTwo, domainMetadata, emptyConstraint)
			expectedEntries := []string{"CREATE DOMAIN public.domain2 AS varchar;",
				"COMMENT ON DOMAIN public.domain2 IS 'This is a domain comment.';",
				"ALTER DOMAIN public.domain2 OWNER TO testrole;",
				"SECURITY LABEL FOR dummy ON DOMAIN public.domain2 IS 'unclassified';"}
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, expectedEntries...)
		})
	})
	Describe("PrintCreateRangeTypeStatement", func() {
		basicRangeType := backup.RangeType{
			Name:    "rangetype",
			Schema:  "public",
			SubType: "test_subtype_schema.test_subtype",
		}
		complexRangeType := backup.RangeType{
			Name: "rangetype", Schema: "public",
			SubType:        "test_subtype_schema.test_subtype",
			SubTypeOpClass: "opclass_schema.test_opclass",
			Collation:      "collation_schema.test_collation",
			Canonical:      "canonical_schema.test_canonical",
			SubTypeDiff:    "diff_schema.test_diff",
		}
		It("prints a basic range type", func() {
			backup.PrintCreateRangeTypeStatement(backupfile, tocfile, basicRangeType, emptyMetadata)
			testutils.ExpectEntry(tocfile.PredataEntries, 0, "public", "", "rangetype", toc.OBJ_TYPE)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE TYPE public.rangetype AS RANGE (
	SUBTYPE = test_subtype_schema.test_subtype
);`)
		})
		It("prints a complex range type", func() {
			backup.PrintCreateRangeTypeStatement(backupfile, tocfile, complexRangeType, emptyMetadata)
			testutils.ExpectEntry(tocfile.PredataEntries, 0, "public", "", "rangetype", toc.OBJ_TYPE)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE TYPE public.rangetype AS RANGE (
	SUBTYPE = test_subtype_schema.test_subtype,
	SUBTYPE_OPCLASS = opclass_schema.test_opclass,
	COLLATION = collation_schema.test_collation,
	CANONICAL = canonical_schema.test_canonical,
	SUBTYPE_DIFF = diff_schema.test_diff
);`)
		})
		It("prints a range type with an owner, security label, and a comment", func() {
			backup.PrintCreateRangeTypeStatement(backupfile, tocfile, basicRangeType, typeMetadata)
			expectedStatements := []string{`CREATE TYPE public.rangetype AS RANGE (
	SUBTYPE = test_subtype_schema.test_subtype
);`,
				"COMMENT ON TYPE public.rangetype IS 'This is a type comment.';",
				"ALTER TYPE public.rangetype OWNER TO testrole;",
				"SECURITY LABEL FOR dummy ON TYPE public.rangetype IS 'unclassified';"}
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, expectedStatements...)
		})
	})
	Describe("PrintCreateCollationStatement", func() {
		It("prints a create collation statement", func() {
			collation := backup.Collation{Oid: 1, Name: "collation1", Collate: "collate1", Ctype: "ctype1", Schema: "schema1"}
			backup.PrintCreateCollationStatements(backupfile, tocfile, []backup.Collation{collation}, emptyMetadataMap)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE COLLATION schema1.collation1 (LC_COLLATE = 'collate1', LC_CTYPE = 'ctype1');`)
		})
		It("prints a create collation statement with owner and comment", func() {
			collation := backup.Collation{Oid: 1, Name: "collation1", Collate: "collate1", Ctype: "ctype1", Schema: "schema1"}
			collationMetadataMap := testutils.DefaultMetadataMap(toc.OBJ_COLLATION, false, true, true, false)
			backup.PrintCreateCollationStatements(backupfile, tocfile, []backup.Collation{collation}, collationMetadataMap)
			expectedStatements := []string{
				"CREATE COLLATION schema1.collation1 (LC_COLLATE = 'collate1', LC_CTYPE = 'ctype1');",
				"COMMENT ON COLLATION schema1.collation1 IS 'This is a collation comment.';",
				"ALTER COLLATION schema1.collation1 OWNER TO testrole;"}
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, expectedStatements...)
		})
	})
})
