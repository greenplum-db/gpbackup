package backup_test

import (
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/lib/pq"

	. "github.com/onsi/ginkgo"
)

var _ = Describe("backup/predata_types tests", func() {
	typeMetadata := backup.ObjectMetadata{}
	typeMetadataMap := backup.MetadataMap{}

	BeforeEach(func() {
		typeMetadata = backup.ObjectMetadata{}
		typeMetadataMap = backup.MetadataMap{}
		toc, backupfile = testutils.InitializeTestTOC(buffer, "predata")
	})
	Describe("PrintCreateEnumTypeStatements", func() {
		enumOne := backup.Type{Oid: 1, Schema: "public", Name: "enum_type", Type: "e", EnumLabels: "'bar',\n\t'baz',\n\t'foo'", Category: "U"}
		enumTwo := backup.Type{Oid: 1, Schema: "public", Name: "enum_type", Type: "e", EnumLabels: "'bar',\n\t'baz',\n\t'foo'", Category: "U"}

		It("prints an enum type with multiple attributes", func() {
			backup.PrintCreateEnumTypeStatements(backupfile, toc, []backup.Type{enumOne}, typeMetadataMap)
			testutils.ExpectEntry(toc.PredataEntries, 0, "public", "", "enum_type", "TYPE")
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TYPE public.enum_type AS ENUM (
	'bar',
	'baz',
	'foo'
);`)
		})
		It("prints an enum type with comment and owner", func() {
			typeMetadataMap = testutils.DefaultMetadataMap("TYPE", false, true, true)
			backup.PrintCreateEnumTypeStatements(backupfile, toc, []backup.Type{enumTwo}, typeMetadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TYPE public.enum_type AS ENUM (
	'bar',
	'baz',
	'foo'
);


COMMENT ON TYPE public.enum_type IS 'This is a type comment.';


ALTER TYPE public.enum_type OWNER TO testrole;`)
		})
	})
	Describe("PrintCreateCompositeTypeStatement", func() {
		oneAtt := pq.StringArray{"\tfoo integer"}
		twoAtts := pq.StringArray{"\tfoo integer", "\tbar text"}
		compType := backup.Type{Oid: 1, Schema: "public", Name: "composite_type", Type: "c", Category: "U"}

		It("prints a composite type with one attribute", func() {
			compType.Attributes = oneAtt
			backup.PrintCreateCompositeTypeStatement(backupfile, toc, compType, typeMetadata)
			testutils.ExpectEntry(toc.PredataEntries, 0, "public", "", "composite_type", "TYPE")
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TYPE public.composite_type AS (
	foo integer
);`)
		})
		It("prints a composite type with multiple attributes", func() {
			compType.Attributes = twoAtts
			backup.PrintCreateCompositeTypeStatement(backupfile, toc, compType, typeMetadata)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TYPE public.composite_type AS (
	foo integer,
	bar text
);`)
		})
		It("prints a composite type with comment and owner", func() {
			compType.Attributes = twoAtts
			typeMetadata = testutils.DefaultMetadataMap("TYPE", false, true, true)[1]
			backup.PrintCreateCompositeTypeStatement(backupfile, toc, compType, typeMetadata)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TYPE public.composite_type AS (
	foo integer,
	bar text
);

COMMENT ON TYPE public.composite_type IS 'This is a type comment.';


ALTER TYPE public.composite_type OWNER TO testrole;`)
		})
	})
	Describe("PrintCreateBaseTypeStatement", func() {
		baseSimple := backup.Type{Oid: 1, Schema: "public", Name: "base_type", Type: "b", Input: "input_fn", Output: "output_fn", Receive: "", Send: "", ModIn: "", ModOut: "", InternalLength: -1, IsPassedByValue: false, Alignment: "c", Storage: "p", DefaultVal: "", Element: "", Category: "U", Preferred: false, Delimiter: "", EnumLabels: "", BaseType: "", NotNull: false, Attributes: nil, DependsUpon: nil}
		basePartial := backup.Type{Oid: 1, Schema: "public", Name: "base_type", Type: "b", Input: "input_fn", Output: "output_fn", Receive: "receive_fn", Send: "send_fn", ModIn: "modin_fn", ModOut: "modout_fn", InternalLength: -1, IsPassedByValue: false, Alignment: "c", Storage: "p", DefaultVal: "42", Element: "int4", Category: "U", Delimiter: ",", EnumLabels: "", BaseType: "", NotNull: false, Attributes: nil, DependsUpon: nil}
		baseFull := backup.Type{Oid: 1, Schema: "public", Name: "base_type", Type: "b", Input: "input_fn", Output: "output_fn", Receive: "receive_fn", Send: "send_fn", ModIn: "modin_fn", ModOut: "modout_fn", InternalLength: 16, IsPassedByValue: true, Alignment: "s", Storage: "e", DefaultVal: "42", Element: "int4", Category: "N", Preferred: true, Delimiter: ",", EnumLabels: "", BaseType: "", NotNull: false, Attributes: nil, DependsUpon: nil, StorageOptions: "compresstype=zlib, compresslevel=1, blocksize=32768", Collatable: true}
		basePermOne := backup.Type{Oid: 1, Schema: "public", Name: "base_type", Type: "b", Input: "input_fn", Output: "output_fn", Receive: "", Send: "", ModIn: "", ModOut: "", InternalLength: -1, IsPassedByValue: false, Alignment: "d", Storage: "m", DefaultVal: "", Element: "", Category: "U", Delimiter: "", EnumLabels: "", BaseType: "", NotNull: false, Attributes: nil, DependsUpon: nil}
		basePermTwo := backup.Type{Oid: 1, Schema: "public", Name: "base_type", Type: "b", Input: "input_fn", Output: "output_fn", Receive: "", Send: "", ModIn: "", ModOut: "", InternalLength: -1, IsPassedByValue: false, Alignment: "i", Storage: "x", DefaultVal: "", Element: "", Category: "U", Delimiter: "", EnumLabels: "", BaseType: "", NotNull: false, Attributes: nil, DependsUpon: nil}
		baseCommentOwner := backup.Type{Oid: 1, Schema: "public", Name: "base_type", Type: "b", Input: "input_fn", Output: "output_fn", Receive: "", Send: "", ModIn: "", ModOut: "", InternalLength: -1, IsPassedByValue: false, Alignment: "c", Storage: "p", DefaultVal: "", Element: "", Category: "U", Delimiter: "", EnumLabels: "", BaseType: "", NotNull: false, Attributes: nil, DependsUpon: nil}

		It("prints a base type with no optional arguments", func() {
			backup.PrintCreateBaseTypeStatement(backupfile, toc, baseSimple, typeMetadata)
			testutils.ExpectEntry(toc.PredataEntries, 0, "public", "", "base_type", "TYPE")
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TYPE public.base_type (
	INPUT = input_fn,
	OUTPUT = output_fn
);`)
		})
		It("prints a base type where all optional arguments have default values where possible", func() {
			backup.PrintCreateBaseTypeStatement(backupfile, toc, basePartial, typeMetadata)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TYPE public.base_type (
	INPUT = input_fn,
	OUTPUT = output_fn,
	RECEIVE = receive_fn,
	SEND = send_fn,
	TYPMOD_IN = modin_fn,
	TYPMOD_OUT = modout_fn,
	DEFAULT = '42',
	ELEMENT = int4,
	DELIMITER = ','
);`)
		})
		It("prints a base type with all optional arguments provided", func() {
			backup.PrintCreateBaseTypeStatement(backupfile, toc, baseFull, typeMetadata)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TYPE public.base_type (
	INPUT = input_fn,
	OUTPUT = output_fn,
	RECEIVE = receive_fn,
	SEND = send_fn,
	TYPMOD_IN = modin_fn,
	TYPMOD_OUT = modout_fn,
	INTERNALLENGTH = 16,
	PASSEDBYVALUE,
	ALIGNMENT = int2,
	STORAGE = extended,
	DEFAULT = '42',
	ELEMENT = int4,
	DELIMITER = ',',
	CATEGORY = 'N',
	PREFERRED = true,
	COLLATABLE = true
);

ALTER TYPE public.base_type
	SET DEFAULT ENCODING (compresstype=zlib, compresslevel=1, blocksize=32768);`)
		})
		It("prints a base type with double alignment and main storage", func() {
			backup.PrintCreateBaseTypeStatement(backupfile, toc, basePermOne, typeMetadata)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TYPE public.base_type (
	INPUT = input_fn,
	OUTPUT = output_fn,
	ALIGNMENT = double,
	STORAGE = main
);`)
		})
		It("prints a base type with int4 alignment and external storage", func() {
			backup.PrintCreateBaseTypeStatement(backupfile, toc, basePermTwo, typeMetadata)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TYPE public.base_type (
	INPUT = input_fn,
	OUTPUT = output_fn,
	ALIGNMENT = int4,
	STORAGE = external
);`)
		})
		It("prints a base type with comment and owner", func() {
			backup.PrintCreateBaseTypeStatement(backupfile, toc, baseCommentOwner, typeMetadata)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TYPE public.base_type (
	INPUT = input_fn,
	OUTPUT = output_fn
);`)
		})
	})
	Describe("PrintCreateShellTypeStatements", func() {
		baseOne := backup.Type{Oid: 1, Schema: "public", Name: "base_type1", Type: "b", Input: "input_fn", Output: "output_fn", Receive: "", Send: "", ModIn: "", ModOut: "", InternalLength: -1, IsPassedByValue: false, Alignment: "c", Storage: "p", DefaultVal: "", Element: "", Category: "U", Delimiter: "", EnumLabels: "", BaseType: "", NotNull: false, Attributes: nil, DependsUpon: nil}
		baseTwo := backup.Type{Oid: 1, Schema: "public", Name: "base_type2", Type: "b", Input: "input_fn", Output: "output_fn", Receive: "", Send: "", ModIn: "", ModOut: "", InternalLength: -1, IsPassedByValue: false, Alignment: "c", Storage: "p", DefaultVal: "", Element: "", Category: "U", Delimiter: "", EnumLabels: "", BaseType: "", NotNull: false, Attributes: nil, DependsUpon: nil}
		compOne := backup.Type{Oid: 1, Schema: "public", Name: "composite_type1", Type: "c", Category: "U"}
		compTwo := backup.Type{Oid: 1, Schema: "public", Name: "composite_type2", Type: "c", Category: "U"}
		enumOne := backup.Type{Oid: 1, Schema: "public", Name: "enum_type", Type: "e", EnumLabels: "'bar',\n\t'baz',\n\t'foo'", Category: "U"}
		It("prints shell type for only a base type", func() {
			backup.PrintCreateShellTypeStatements(backupfile, toc, []backup.Type{baseOne, baseTwo, compOne, compTwo, enumOne})
			testutils.ExpectEntry(toc.PredataEntries, 0, "public", "", "base_type1", "TYPE")
			testutils.ExpectEntry(toc.PredataEntries, 1, "public", "", "base_type2", "TYPE")
			testutils.AssertBufferContents(toc.PredataEntries, buffer, "CREATE TYPE public.base_type1;", "CREATE TYPE public.base_type2;")
		})
	})
	Describe("PrintCreateDomainStatement", func() {
		emptyMetadata := backup.ObjectMetadata{}
		emptyConstraint := []backup.Constraint{}
		checkConstraint := []backup.Constraint{{Name: "domain1_check", ConDef: "CHECK (VALUE > 2)", OwningObject: "public.domain1"}}
		domainOne := testutils.DefaultTypeDefinition("d", "domain1")
		domainOne.DefaultVal = "4"
		domainOne.BaseType = "numeric"
		domainOne.NotNull = true
		domainTwo := testutils.DefaultTypeDefinition("d", "domain2")
		domainTwo.BaseType = "varchar"
		It("prints a basic domain with a constraint", func() {
			backup.PrintCreateDomainStatement(backupfile, toc, domainOne, emptyMetadata, checkConstraint)
			testutils.ExpectEntry(toc.PredataEntries, 0, "public", "", "domain1", "DOMAIN")
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE DOMAIN public.domain1 AS numeric DEFAULT 4 NOT NULL
	CONSTRAINT domain1_check CHECK (VALUE > 2);`)
		})
		It("prints a basic domain without constraint", func() {
			backup.PrintCreateDomainStatement(backupfile, toc, domainOne, emptyMetadata, emptyConstraint)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE DOMAIN public.domain1 AS numeric DEFAULT 4 NOT NULL;`)
		})
		It("prints a domain without constraint with comment and owner", func() {
			typeMetadata = testutils.DefaultMetadataMap("DOMAIN", false, true, true)[1]
			backup.PrintCreateDomainStatement(backupfile, toc, domainTwo, typeMetadata, emptyConstraint)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE DOMAIN public.domain2 AS varchar;


COMMENT ON DOMAIN public.domain2 IS 'This is a domain comment.';


ALTER DOMAIN public.domain2 OWNER TO testrole;`)
		})
	})
	Describe("PrintCreateCollationStatement", func() {
		emptyMetadataMap := backup.MetadataMap{}
		It("prints a create collation statement", func() {
			collation := backup.Collation{Oid: 1, Name: "collation1", Collate: "collate1", Ctype: "ctype1", Schema: "schema1"}
			backup.PrintCreateCollationStatements(backupfile, toc, []backup.Collation{collation}, emptyMetadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE COLLATION schema1.collation1 (LC_COLLATE = 'collate1', LC_CTYPE = 'ctype1');`)
		})
		It("prints a create collation statement with owner and comment", func() {
			collation := backup.Collation{Oid: 1, Name: "collation1", Collate: "collate1", Ctype: "ctype1", Schema: "schema1"}
			collationMetadataMap := testutils.DefaultMetadataMap("COLLATION", false, true, true)
			backup.PrintCreateCollationStatements(backupfile, toc, []backup.Collation{collation}, collationMetadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE COLLATION schema1.collation1 (LC_COLLATE = 'collate1', LC_CTYPE = 'ctype1');

COMMENT ON COLLATION schema1.collation1 IS 'This is a collation comment.';


ALTER COLLATION schema1.collation1 OWNER TO testrole;`)
		})
	})
})
