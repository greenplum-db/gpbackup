package backup_test

import (
	"database/sql"

	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/toc"

	. "github.com/onsi/ginkgo/v2"
)

var _ = Describe("backup/predata_relations tests", func() {
	rowOne := backup.ColumnDefinition{Oid: 0, Num: 1, Name: "i", Type: "integer", StatTarget: -1}
	rowTwo := backup.ColumnDefinition{Oid: 1, Num: 2, Name: "j", Type: "character varying(20)", StatTarget: -1}

	noMetadata := backup.ObjectMetadata{}

	var testTable backup.Table
	BeforeEach(func() {
		tocfile, backupfile = testutils.InitializeTestTOC(buffer, "predata")
		extTableEmpty := backup.ExternalTableDefinition{Oid: 0, Type: -2, Protocol: -2, Location: sql.NullString{String: "", Valid: false}, ExecLocation: "ALL_SEGMENTS", FormatType: "t", FormatOpts: "", Command: "", RejectLimit: 0, RejectLimitType: "", ErrTableName: "", ErrTableSchema: "", Encoding: "UTF-8", Writable: false, URIs: nil}
		testTable = backup.Table{
			Relation:        backup.Relation{Schema: "public", Name: "tablename"},
			TableDefinition: backup.TableDefinition{DistPolicy: "DISTRIBUTED RANDOMLY", PartDef: "", PartTemplateDef: "", StorageOpts: "", ExtTableDef: extTableEmpty},
		}
	})
	Describe("PrintCreateTableStatement", func() {
		It("calls PrintRegularTableCreateStatement for a regular table", func() {
			tableMetadata := backup.ObjectMetadata{Owner: "testrole"}

			testTable.IsExternal = false
			backup.PrintCreateTableStatement(backupfile, tocfile, testTable, tableMetadata)
			testutils.ExpectEntry(tocfile.PredataEntries, 0, "public", "", "tablename", toc.OBJ_TABLE)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE TABLE public.tablename (
) DISTRIBUTED RANDOMLY;`, "ALTER TABLE public.tablename OWNER TO testrole;")
		})
		It("calls PrintExternalTableCreateStatement for an external table", func() {
			testTable.IsExternal = true
			backup.PrintCreateTableStatement(backupfile, tocfile, testTable, noMetadata)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE READABLE EXTERNAL WEB TABLE public.tablename (
) 
FORMAT 'TEXT'
ENCODING 'UTF-8';`)
		})
	})
	Describe("PrintRegularTableCreateStatement", func() {
		rowOneEncoding := backup.ColumnDefinition{Oid: 0, Num: 1, Name: "i", Type: "integer", Encoding: "compresstype=none,blocksize=32768,compresslevel=0", StatTarget: -1}
		rowTwoEncoding := backup.ColumnDefinition{Oid: 0, Num: 2, Name: "j", Type: "character varying(20)", Encoding: "compresstype=zlib,blocksize=65536,compresslevel=1", StatTarget: -1}
		rowNotNull := backup.ColumnDefinition{Oid: 0, Num: 2, Name: "j", NotNull: true, Type: "character varying(20)", StatTarget: -1}
		rowEncodingNotNull := backup.ColumnDefinition{Oid: 0, Num: 2, Name: "j", NotNull: true, Type: "character varying(20)", Encoding: "compresstype=zlib,blocksize=65536,compresslevel=1", StatTarget: -1}
		rowOneDef := backup.ColumnDefinition{Oid: 0, Num: 1, Name: "i", HasDefault: true, Type: "integer", StatTarget: -1, DefaultVal: "42"}
		rowTwoDef := backup.ColumnDefinition{Oid: 0, Num: 2, Name: "j", HasDefault: true, Type: "character varying(20)", StatTarget: -1, DefaultVal: "'bar'::text"}
		rowTwoEncodingDef := backup.ColumnDefinition{Oid: 0, Num: 2, Name: "j", HasDefault: true, Type: "character varying(20)", Encoding: "compresstype=zlib,blocksize=65536,compresslevel=1", StatTarget: -1, DefaultVal: "'bar'::text"}
		rowNotNullDef := backup.ColumnDefinition{Oid: 0, Num: 2, Name: "j", NotNull: true, HasDefault: true, Type: "character varying(20)", StatTarget: -1, DefaultVal: "'bar'::text"}
		rowEncodingNotNullDef := backup.ColumnDefinition{Oid: 0, Num: 2, Name: "j", NotNull: true, HasDefault: true, Type: "character varying(20)", Encoding: "compresstype=zlib,blocksize=65536,compresslevel=1", StatTarget: -1, DefaultVal: "'bar'::text"}
		rowStats := backup.ColumnDefinition{Oid: 0, Num: 1, Name: "i", Type: "integer", StatTarget: 3}
		colOptions := backup.ColumnDefinition{Oid: 0, Num: 1, Name: "i", Type: "integer", Options: "n_distinct=1", StatTarget: -1}
		colStorageType := backup.ColumnDefinition{Oid: 0, Num: 1, Name: "i", Type: "integer", StatTarget: -1, StorageType: "PLAIN"}
		colWithCollation := backup.ColumnDefinition{Oid: 0, Num: 1, Name: "c", Type: "character (8)", StatTarget: -1, Collation: "public.some_coll"}
		rowTwoGenerated := backup.ColumnDefinition{Oid: 0, Num: 2, Name: "j", HasDefault: true, Type: "integer", StatTarget: -1, DefaultVal: "(i * 2)", AttGenerated: "STORED"}
		rowTwoGeneratedInherited := backup.ColumnDefinition{Oid: 0, Num: 2, Name: "j", HasDefault: true, Type: "integer", StatTarget: -1, DefaultVal: "(i * 2)", AttGenerated: "STORED", IsInherited: true}

		Context("No special table attributes", func() {
			It("prints a CREATE TABLE OF type block with one attribute", func() {
				col := []backup.ColumnDefinition{rowOne}
				testTable.ColumnDefs = col
				testTable.TableType = "public.some_type"
				backup.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)
				testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE TABLE public.tablename OF public.some_type (
	i WITH OPTIONS
) DISTRIBUTED RANDOMLY;`)
			})

			It("prints a CREATE TABLE block with one attribute", func() {
				col := []backup.ColumnDefinition{rowOne}
				testTable.ColumnDefs = col
				backup.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)
				testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block with one line per attribute", func() {
				col := []backup.ColumnDefinition{rowOne, rowTwo}
				testTable.ColumnDefs = col
				backup.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)
				testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block with no attributes", func() {
				testTable.ColumnDefs = []backup.ColumnDefinition{}
				backup.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)
				testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE TABLE public.tablename (
) DISTRIBUTED RANDOMLY;`)
			})
		})
		Context("One special table attribute", func() {
			It("prints a CREATE TABLE block where one line has the given ENCODING and the other has the default ENCODING", func() {
				col := []backup.ColumnDefinition{rowOneEncoding, rowTwoEncoding}
				testTable.ColumnDefs = col
				backup.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)
				testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer ENCODING (compresstype=none,blocksize=32768,compresslevel=0),
	j character varying(20) ENCODING (compresstype=zlib,blocksize=65536,compresslevel=1)
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block where one line contains NOT NULL", func() {
				col := []backup.ColumnDefinition{rowOne, rowNotNull}
				testTable.ColumnDefs = col
				backup.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)
				testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20) NOT NULL
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE OF type block where one line contains NOT NULL", func() {
				col := []backup.ColumnDefinition{rowOne, rowNotNull}
				testTable.TableType = "public.some_type"
				testTable.ColumnDefs = col
				backup.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)
				testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE TABLE public.tablename OF public.some_type (
	i WITH OPTIONS,
	j WITH OPTIONS NOT NULL
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block where one line contains DEFAULT", func() {
				col := []backup.ColumnDefinition{rowOneDef, rowTwo}
				testTable.ColumnDefs = col
				backup.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)
				testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer DEFAULT 42,
	j character varying(20)
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block where both lines contain DEFAULT", func() {
				col := []backup.ColumnDefinition{rowOneDef, rowTwoDef}
				testTable.ColumnDefs = col
				backup.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)
				testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer DEFAULT 42,
	j character varying(20) DEFAULT 'bar'::text
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block where one line contains COLLATE", func() {
				col := []backup.ColumnDefinition{rowOne, colWithCollation}
				testTable.ColumnDefs = col
				backup.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)
				testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer,
	c character (8) COLLATE public.some_coll
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block followed by an ALTER COLUMN ... SET STATISTICS statement", func() {
				col := []backup.ColumnDefinition{rowStats}
				testTable.ColumnDefs = col
				backup.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)
				testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer
) DISTRIBUTED RANDOMLY;

ALTER TABLE ONLY public.tablename ALTER COLUMN i SET STATISTICS 3;`)
			})
			It("prints a CREATE TABLE block followed by an ALTER COLUMN ... SET STORAGE statement", func() {
				col := []backup.ColumnDefinition{colStorageType}
				testTable.ColumnDefs = col
				backup.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)
				testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer
) DISTRIBUTED RANDOMLY;

ALTER TABLE ONLY public.tablename ALTER COLUMN i SET STORAGE PLAIN;`)
			})
			It("prints a CREATE TABLE block followed by an ALTER COLUMN ... SET ... statement", func() {
				col := []backup.ColumnDefinition{colOptions}
				testTable.ColumnDefs = col
				backup.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)
				testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer
) DISTRIBUTED RANDOMLY;

ALTER TABLE ONLY public.tablename ALTER COLUMN i SET (n_distinct=1);`)
			})
			It("prints a CREATE TABLE block with one line with regular attribute and a line with generated attribute", func() {
				col := []backup.ColumnDefinition{rowOne, rowTwoGenerated}
				testTable.ColumnDefs = col
				backup.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)
				testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer,
	j integer GENERATED ALWAYS AS (i * 2) STORED
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block that omits the generated attribute if it inherits from another table", func() {
				col := []backup.ColumnDefinition{rowOne, rowTwoGeneratedInherited}
				testTable.ColumnDefs = col
				backup.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)
				testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer,
	j integer
) DISTRIBUTED RANDOMLY;`)
			})
		})
		Context("Multiple special table attributes on one column", func() {
			It("prints a CREATE TABLE block where one line contains both NOT NULL and ENCODING", func() {
				col := []backup.ColumnDefinition{rowOneEncoding, rowEncodingNotNull}
				testTable.ColumnDefs = col
				backup.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)
				testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer ENCODING (compresstype=none,blocksize=32768,compresslevel=0),
	j character varying(20) NOT NULL ENCODING (compresstype=zlib,blocksize=65536,compresslevel=1)
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block where one line contains both DEFAULT and NOT NULL", func() {
				col := []backup.ColumnDefinition{rowOne, rowNotNullDef}
				testTable.ColumnDefs = col
				backup.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)
				testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20) DEFAULT 'bar'::text NOT NULL
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block where one line contains both DEFAULT and ENCODING", func() {
				col := []backup.ColumnDefinition{rowOneEncoding, rowTwoEncodingDef}
				testTable.ColumnDefs = col
				backup.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)
				testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer ENCODING (compresstype=none,blocksize=32768,compresslevel=0),
	j character varying(20) DEFAULT 'bar'::text ENCODING (compresstype=zlib,blocksize=65536,compresslevel=1)
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block where one line contains all three of DEFAULT, NOT NULL, and ENCODING", func() {
				col := []backup.ColumnDefinition{rowOneEncoding, rowEncodingNotNullDef}
				testTable.ColumnDefs = col
				backup.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)
				testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer ENCODING (compresstype=none,blocksize=32768,compresslevel=0),
	j character varying(20) DEFAULT 'bar'::text NOT NULL ENCODING (compresstype=zlib,blocksize=65536,compresslevel=1)
) DISTRIBUTED RANDOMLY;`)
			})
		})
		Context("Table qualities (distribution keys and storage options)", func() {
			distSingle := "DISTRIBUTED BY (i)"
			distComposite := "DISTRIBUTED BY (i, j)"

			aoOpts := "appendonly=true"
			coOpts := "appendonly=true, orientation=column"
			heapFillOpts := "fillfactor=42"
			coManyOpts := "appendonly=true, orientation=column, fillfactor=42, compresstype=zlib, blocksize=32768, compresslevel=1"

			col := []backup.ColumnDefinition{rowOne, rowTwo}
			BeforeEach(func() {
				testTable.ColumnDefs = col
			})
			It("has a single-column distribution key", func() {
				testTable.DistPolicy = distSingle
				backup.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)
				testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) DISTRIBUTED BY (i);`)
			})
			It("has a multiple-column composite distribution key", func() {
				testTable.DistPolicy = distComposite
				backup.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)
				testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) DISTRIBUTED BY (i, j);`)
			})
			It("is an append-optimized table", func() {
				testTable.StorageOpts = aoOpts
				backup.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)
				testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) WITH (appendonly=true) DISTRIBUTED RANDOMLY;`)
			})
			It("is an append-optimized table with a single-column distribution key", func() {
				testTable.DistPolicy = distSingle
				testTable.StorageOpts = aoOpts
				backup.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)
				testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) WITH (appendonly=true) DISTRIBUTED BY (i);`)
			})
			It("is an append-optimized table with a two-column composite distribution key", func() {
				testTable.DistPolicy = distComposite
				testTable.StorageOpts = aoOpts
				backup.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)
				testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) WITH (appendonly=true) DISTRIBUTED BY (i, j);`)
			})
			It("is an append-optimized column-oriented table", func() {
				testTable.StorageOpts = coOpts
				backup.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)
				testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) WITH (appendonly=true, orientation=column) DISTRIBUTED RANDOMLY;`)
			})
			It("is an append-optimized column-oriented table with a single-column distribution key", func() {
				testTable.DistPolicy = distSingle
				testTable.StorageOpts = coOpts
				backup.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)
				testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) WITH (appendonly=true, orientation=column) DISTRIBUTED BY (i);`)
			})
			It("is an append-optimized column-oriented table with a two-column composite distribution key", func() {
				testTable.DistPolicy = distComposite
				testTable.StorageOpts = coOpts
				backup.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)
				testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) WITH (appendonly=true, orientation=column) DISTRIBUTED BY (i, j);`)
			})
			It("is a heap table with a fill factor", func() {
				testTable.StorageOpts = heapFillOpts
				backup.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)
				testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) WITH (fillfactor=42) DISTRIBUTED RANDOMLY;`)
			})
			It("is a heap table with a fill factor and a single-column distribution key", func() {
				testTable.DistPolicy = distSingle
				testTable.StorageOpts = heapFillOpts
				backup.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)
				testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) WITH (fillfactor=42) DISTRIBUTED BY (i);`)
			})
			It("is a heap table with a fill factor and a multiple-column composite distribution key", func() {
				testTable.DistPolicy = distComposite
				testTable.StorageOpts = heapFillOpts
				backup.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)
				testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) WITH (fillfactor=42) DISTRIBUTED BY (i, j);`)
			})
			It("is an append-optimized column-oriented table with complex storage options", func() {
				testTable.StorageOpts = coManyOpts
				backup.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)
				testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) WITH (appendonly=true, orientation=column, fillfactor=42, compresstype=zlib, blocksize=32768, compresslevel=1) DISTRIBUTED RANDOMLY;`)
			})
			It("is an append-optimized column-oriented table with complex storage options and a single-column distribution key", func() {
				testTable.DistPolicy = distSingle
				testTable.StorageOpts = coManyOpts
				backup.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)
				testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) WITH (appendonly=true, orientation=column, fillfactor=42, compresstype=zlib, blocksize=32768, compresslevel=1) DISTRIBUTED BY (i);`)
			})
			It("is an append-optimized column-oriented table with complex storage options and a two-column composite distribution key", func() {
				testTable.DistPolicy = distComposite
				testTable.StorageOpts = coManyOpts
				backup.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)
				testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) WITH (appendonly=true, orientation=column, fillfactor=42, compresstype=zlib, blocksize=32768, compresslevel=1) DISTRIBUTED BY (i, j);`)
			})
			It("is a GPDB 7+ root partition", func() {
				testutils.SkipIfBefore7(connectionPool)
				testTable.PartitionKeyDef = "RANGE (b)"
				backup.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)
				testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) PARTITION BY RANGE (b) DISTRIBUTED RANDOMLY;`)
			})
		})
		Context("Table partitioning", func() {
			col := []backup.ColumnDefinition{rowOne, rowTwo}
			BeforeEach(func() {
				partDef := `PARTITION BY LIST(gender)
	(
	PARTITION girls VALUES('F') WITH (tablename='rank_1_prt_girls', appendonly=false ),
	PARTITION boys VALUES('M') WITH (tablename='rank_1_prt_boys', appendonly=false ),
	DEFAULT PARTITION other  WITH (tablename='rank_1_prt_other', appendonly=false )
	)
`
				testTable.PartDef = partDef
				testTable.ColumnDefs = col
			})
			It("is a partition table with no table attributes", func() {
				backup.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)
				testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) DISTRIBUTED RANDOMLY PARTITION BY LIST(gender)
	(
	PARTITION girls VALUES('F') WITH (tablename='rank_1_prt_girls', appendonly=false ),
	PARTITION boys VALUES('M') WITH (tablename='rank_1_prt_boys', appendonly=false ),
	DEFAULT PARTITION other  WITH (tablename='rank_1_prt_other', appendonly=false )
	);`)
			})
			It("is a partition table with table attributes", func() {
				testTable.StorageOpts = "appendonly=true, orientation=column"
				backup.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)
				testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) WITH (appendonly=true, orientation=column) DISTRIBUTED RANDOMLY PARTITION BY LIST(gender)
	(
	PARTITION girls VALUES('F') WITH (tablename='rank_1_prt_girls', appendonly=false ),
	PARTITION boys VALUES('M') WITH (tablename='rank_1_prt_boys', appendonly=false ),
	DEFAULT PARTITION other  WITH (tablename='rank_1_prt_other', appendonly=false )
	);`)
			})
			It("is a partition table with subpartitions and table attributes", func() {
				partTemplateDef := `ALTER TABLE tablename
SET SUBPARTITION TEMPLATE
          (
          SUBPARTITION usa VALUES('usa') WITH (tablename='tablename'),
          SUBPARTITION asia VALUES('asia') WITH (tablename='tablename'),
          SUBPARTITION europe VALUES('europe') WITH (tablename='tablename'),
          DEFAULT SUBPARTITION other_regions  WITH (tablename='tablename')
          )
`
				testTable.PartTemplateDef = partTemplateDef
				backup.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)
				testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) DISTRIBUTED RANDOMLY PARTITION BY LIST(gender)
	(
	PARTITION girls VALUES('F') WITH (tablename='rank_1_prt_girls', appendonly=false ),
	PARTITION boys VALUES('M') WITH (tablename='rank_1_prt_boys', appendonly=false ),
	DEFAULT PARTITION other  WITH (tablename='rank_1_prt_other', appendonly=false )
	);
ALTER TABLE tablename
SET SUBPARTITION TEMPLATE
          (
          SUBPARTITION usa VALUES('usa') WITH (tablename='tablename'),
          SUBPARTITION asia VALUES('asia') WITH (tablename='tablename'),
          SUBPARTITION europe VALUES('europe') WITH (tablename='tablename'),
          DEFAULT SUBPARTITION other_regions  WITH (tablename='tablename')
          );`)
			})
		})
		Context("Tablespaces", func() {
			It("prints a CREATE TABLE block with a TABLESPACE clause", func() {
				testTable.TablespaceName = "test_tablespace"
				backup.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)
				testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE TABLE public.tablename (
) TABLESPACE test_tablespace DISTRIBUTED RANDOMLY;`)
			})
		})
		Context("Inheritance", func() {
			It("prints a CREATE TABLE block with a single-inheritance INHERITS clause", func() {
				col := []backup.ColumnDefinition{rowOne}
				testTable.ColumnDefs = col
				testTable.Inherits = []string{"public.parent"}
				backup.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)
				testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer
) INHERITS (public.parent) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block with a multiple-inheritance INHERITS clause", func() {
				col := []backup.ColumnDefinition{rowOne, rowTwo}
				testTable.ColumnDefs = col
				testTable.Inherits = []string{"public.parent_one", "public.parent_two"}
				backup.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)
				testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) INHERITS (public.parent_one, public.parent_two) DISTRIBUTED RANDOMLY;`)
			})
		})
		Context("Foreign Table", func() {
			BeforeEach(func() {
				testTable.DistPolicy = ""
			})
			It("prints a CREATE TABLE block without options", func() {
				col := []backup.ColumnDefinition{rowOne, rowTwo}
				testTable.ColumnDefs = col
				foreignTable := backup.ForeignTableDefinition{Options: "", Server: "fs"}
				testTable.ForeignDef = foreignTable
				backup.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)
				testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE FOREIGN TABLE public.tablename (
	i integer,
	j character varying(20)
) SERVER fs ;`)
			})
			It("prints a CREATE TABLE block with options", func() {
				col := []backup.ColumnDefinition{rowOne, rowTwo}
				testTable.ColumnDefs = col
				foreignTable := backup.ForeignTableDefinition{Options: "delimiter=',' quote='\"'", Server: "fs"}
				testTable.ForeignDef = foreignTable
				backup.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)
				testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE FOREIGN TABLE public.tablename (
	i integer,
	j character varying(20)
) SERVER fs OPTIONS (delimiter=',' quote='"') ;`)
			})
			It("prints a CREATE TABLE block with foreign data options on attributes", func() {
				rowWithFdwOptions := backup.ColumnDefinition{Oid: 0, Num: 1, Name: "i", Type: "integer", StatTarget: -1, FdwOptions: "option1 'val1', option2 'val2'"}
				col := []backup.ColumnDefinition{rowWithFdwOptions, rowTwo}
				testTable.ColumnDefs = col
				foreignTable := backup.ForeignTableDefinition{Server: "fs"}
				testTable.ForeignDef = foreignTable
				backup.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)
				testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE FOREIGN TABLE public.tablename (
	i integer OPTIONS (option1 'val1', option2 'val2'),
	j character varying(20)
) SERVER fs ;`)
			})
		})
	})
	Describe("PrintPostCreateTableStatements", func() {
		rowCommentOne := backup.ColumnDefinition{Oid: 0, Num: 1, Name: "i", Type: "integer", StatTarget: -1, Comment: "This is a column comment."}
		rowCommentTwo := backup.ColumnDefinition{Oid: 0, Num: 2, Name: "j", Type: "integer", StatTarget: -1, Comment: "This is another column comment."}

		It("does not print default replica identity statement", func() {
			testTable.ReplicaIdentity = "d"
			backup.PrintPostCreateTableStatements(backupfile, tocfile, testTable, noMetadata, []uint32{0, 0})
			testhelper.NotExpectRegexp(buffer, `REPLICA IDENTITY`)
		})
		It("does not print index replica identity statement", func() {
			testTable.ReplicaIdentity = "i"
			backup.PrintPostCreateTableStatements(backupfile, tocfile, testTable, noMetadata, []uint32{0, 0})
			testhelper.NotExpectRegexp(buffer, `REPLICA IDENTITY`)
		})
		It("does not print null replica identity statement", func() {
			testTable.ReplicaIdentity = ""
			backup.PrintPostCreateTableStatements(backupfile, tocfile, testTable, noMetadata, []uint32{0, 0})
			testhelper.NotExpectRegexp(buffer, `REPLICA IDENTITY`)
		})
		It("prints replica identity full", func() {
			testTable.ReplicaIdentity = "f"
			backup.PrintPostCreateTableStatements(backupfile, tocfile, testTable, noMetadata, []uint32{0, 0})
			testhelper.ExpectRegexp(buffer, `ALTER TABLE public.tablename REPLICA IDENTITY FULL;`)
		})
		It("prints replica identity nothing", func() {
			testTable.ReplicaIdentity = "n"
			backup.PrintPostCreateTableStatements(backupfile, tocfile, testTable, noMetadata, []uint32{0, 0})
			testhelper.ExpectRegexp(buffer, `ALTER TABLE public.tablename REPLICA IDENTITY NOTHING;`)
		})
		It("prints a block with a table comment", func() {
			col := []backup.ColumnDefinition{rowOne}
			testTable.ColumnDefs = col
			tableMetadata := testutils.DefaultMetadata(toc.OBJ_TABLE, false, false, true, false)
			backup.PrintPostCreateTableStatements(backupfile, tocfile, testTable, tableMetadata, []uint32{0, 0})
			testhelper.ExpectRegexp(buffer, `

COMMENT ON TABLE public.tablename IS 'This is a table comment.';`)
		})
		It("prints a block with a single column comment", func() {
			col := []backup.ColumnDefinition{rowCommentOne}
			testTable.ColumnDefs = col
			backup.PrintPostCreateTableStatements(backupfile, tocfile, testTable, noMetadata, []uint32{0, 0})
			testhelper.ExpectRegexp(buffer, `

COMMENT ON COLUMN public.tablename.i IS 'This is a column comment.';`)
		})
		It("prints a block with a single column comment containing special characters", func() {
			rowCommentSpecialCharacters := backup.ColumnDefinition{Oid: 0, Num: 1, Name: "i", Type: "integer", StatTarget: -1, Comment: `This is a ta'ble 1+=;,./\>,<@\\n^comment.`}

			col := []backup.ColumnDefinition{rowCommentSpecialCharacters}
			testTable.ColumnDefs = col
			backup.PrintPostCreateTableStatements(backupfile, tocfile, testTable, noMetadata, []uint32{0, 0})
			testhelper.ExpectRegexp(buffer, `

COMMENT ON COLUMN public.tablename.i IS 'This is a ta''ble 1+=;,./\>,<@\\n^comment.';`)
		})
		It("prints a block with multiple column comments", func() {
			col := []backup.ColumnDefinition{rowCommentOne, rowCommentTwo}
			testTable.ColumnDefs = col
			backup.PrintPostCreateTableStatements(backupfile, tocfile, testTable, noMetadata, []uint32{0, 0})
			testhelper.ExpectRegexp(buffer, `

COMMENT ON COLUMN public.tablename.i IS 'This is a column comment.';


COMMENT ON COLUMN public.tablename.j IS 'This is another column comment.';`)
		})
		It("prints an ALTER TABLE ... OWNER TO statement to set the table owner", func() {
			col := []backup.ColumnDefinition{rowOne}
			testTable.ColumnDefs = col
			tableMetadata := testutils.DefaultMetadata(toc.OBJ_TABLE, false, true, false, false)
			backup.PrintPostCreateTableStatements(backupfile, tocfile, testTable, tableMetadata, []uint32{0, 0})
			testhelper.ExpectRegexp(buffer, `

ALTER TABLE public.tablename OWNER TO testrole;`)
		})
		It("prints a SECURITY LABEL statement for the table", func() {
			col := []backup.ColumnDefinition{rowOne}
			testTable.ColumnDefs = col
			tableMetadata := testutils.DefaultMetadata(toc.OBJ_TABLE, false, false, false, true)
			backup.PrintPostCreateTableStatements(backupfile, tocfile, testTable, tableMetadata, []uint32{0, 0})
			testhelper.ExpectRegexp(buffer, `

SECURITY LABEL FOR dummy ON TABLE public.tablename IS 'unclassified';`)
		})
		It("does not print an ALTER TABLE... REPLICA IDENTITY for foreign tables", func() {
			testTable.ForeignDef = backup.ForeignTableDefinition{Options: "", Server: "fs"}
			testTable.ReplicaIdentity = "n"
			tableMetadata := testutils.DefaultMetadata(toc.OBJ_FOREIGN_TABLE, true, true, true, true)
			backup.PrintPostCreateTableStatements(backupfile, tocfile, testTable, tableMetadata, []uint32{0, 0})
			testhelper.NotExpectRegexp(buffer, `REPLICA IDENTITY`)
		})
		It("prints owner, comment, security label, and ACL statements for foreign table", func() {
			col := []backup.ColumnDefinition{rowOne}
			testTable.ColumnDefs = col
			testTable.ForeignDef = backup.ForeignTableDefinition{Options: "", Server: "fs"}
			tableMetadata := testutils.DefaultMetadata(toc.OBJ_FOREIGN_TABLE, true, true, true, true)
			backup.PrintPostCreateTableStatements(backupfile, tocfile, testTable, tableMetadata, []uint32{0, 0})
			testhelper.ExpectRegexp(buffer, `

COMMENT ON FOREIGN TABLE public.tablename IS 'This is a foreign table comment.';


ALTER FOREIGN TABLE public.tablename OWNER TO testrole;


REVOKE ALL ON TABLE public.tablename FROM PUBLIC;
REVOKE ALL ON TABLE public.tablename FROM testrole;
GRANT ALL ON TABLE public.tablename TO testrole;


SECURITY LABEL FOR dummy ON FOREIGN TABLE public.tablename IS 'unclassified';`)
		})
		It("prints both an ALTER TABLE ... OWNER TO statement and comments", func() {
			col := []backup.ColumnDefinition{rowCommentOne, rowCommentTwo}
			testTable.ColumnDefs = col
			tableMetadata := backup.ObjectMetadata{Owner: "testrole", Comment: "This is a table comment."}
			backup.PrintPostCreateTableStatements(backupfile, tocfile, testTable, tableMetadata, []uint32{0, 0})
			testhelper.ExpectRegexp(buffer, `

COMMENT ON TABLE public.tablename IS 'This is a table comment.';


ALTER TABLE public.tablename OWNER TO testrole;


COMMENT ON COLUMN public.tablename.i IS 'This is a column comment.';


COMMENT ON COLUMN public.tablename.j IS 'This is another column comment.';`)
		})
		It("prints a GRANT statement on a table column", func() {
			privilegesColumnOne := backup.ColumnDefinition{Oid: 0, Num: 1, Name: "i", Type: "integer", StatTarget: -1, Privileges: sql.NullString{String: "testrole=r/testrole", Valid: true}}
			privilegesColumnTwo := backup.ColumnDefinition{Oid: 1, Num: 2, Name: "j", Type: "character varying(20)", StatTarget: -1, Privileges: sql.NullString{String: "testrole2=arwx/testrole2", Valid: true}}
			col := []backup.ColumnDefinition{privilegesColumnOne, privilegesColumnTwo}
			testTable.ColumnDefs = col
			tableMetadata := backup.ObjectMetadata{Owner: "testrole"}
			backup.PrintPostCreateTableStatements(backupfile, tocfile, testTable, tableMetadata, []uint32{0, 0})
			testhelper.ExpectRegexp(buffer, `

ALTER TABLE public.tablename OWNER TO testrole;


REVOKE ALL (i) ON TABLE public.tablename FROM PUBLIC;
REVOKE ALL (i) ON TABLE public.tablename FROM testrole;
GRANT SELECT (i) ON TABLE public.tablename TO testrole;


REVOKE ALL (j) ON TABLE public.tablename FROM PUBLIC;
REVOKE ALL (j) ON TABLE public.tablename FROM testrole;
GRANT ALL (j) ON TABLE public.tablename TO testrole2;`)
		})
		It("prints a security group statement on a table column", func() {
			privilegesColumnOne := backup.ColumnDefinition{Oid: 0, Num: 1, Name: "i", Type: "integer", StatTarget: -1, SecurityLabelProvider: "dummy", SecurityLabel: "unclassified"}
			privilegesColumnTwo := backup.ColumnDefinition{Oid: 1, Num: 2, Name: "j", Type: "character varying(20)", StatTarget: -1, SecurityLabelProvider: "dummy", SecurityLabel: "unclassified"}
			col := []backup.ColumnDefinition{privilegesColumnOne, privilegesColumnTwo}
			testTable.ColumnDefs = col
			backup.PrintPostCreateTableStatements(backupfile, tocfile, testTable, backup.ObjectMetadata{}, []uint32{0, 0})
			testhelper.ExpectRegexp(buffer, `

SECURITY LABEL FOR dummy ON COLUMN public.tablename.i IS 'unclassified';


SECURITY LABEL FOR dummy ON COLUMN public.tablename.j IS 'unclassified';`)
		})
		It("prints altered schemas of child partitions different from the root partition", func() {
			testTable.PartitionAlteredSchemas = []backup.AlteredPartitionRelation{
				{OldSchema: "schema1", NewSchema: "schema2", Name: "table1"},
				{OldSchema: "schema2", NewSchema: "schema1", Name: "table2"},
			}
			backup.PrintPostCreateTableStatements(backupfile, tocfile, testTable, backup.ObjectMetadata{}, []uint32{0, 0})
			testhelper.ExpectRegexp(buffer, `

ALTER TABLE schema1.table1 SET SCHEMA schema2;


ALTER TABLE schema2.table2 SET SCHEMA schema1;`)
		})
		It("prints force row security", func() {
			testutils.SkipIfBefore7(connectionPool)
			testTable.ForceRowSecurity = true
			backup.PrintPostCreateTableStatements(backupfile, tocfile, testTable, backup.ObjectMetadata{}, []uint32{0, 0})
			testhelper.ExpectRegexp(buffer, `ALTER TABLE ONLY public.tablename FORCE ROW LEVEL SECURITY;`)
		})
	})
})
