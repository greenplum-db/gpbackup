package backup_test

import (
	"database/sql"
	"math"
	"sort"

	"github.com/greenplum-db/gp-common-go-libs/structmatcher"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/utils"
	sqlmock "gopkg.in/DATA-DOG/go-sqlmock.v1"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup/predata_relations tests", func() {
	rowOne := backup.ColumnDefinition{Oid: 0, Num: 1, Name: "i", Type: "integer", StatTarget: -1, ACL: []backup.ACL{}}
	rowTwo := backup.ColumnDefinition{Oid: 1, Num: 2, Name: "j", Type: "character varying(20)", StatTarget: -1, ACL: []backup.ACL{}}

	noMetadata := backup.ObjectMetadata{}

	var testTable backup.Table
	BeforeEach(func() {
		toc, backupfile = testutils.InitializeTestTOC(buffer, "predata")
		extTableEmpty := backup.ExternalTableDefinition{Oid: 0, Type: -2, Protocol: -2, Location: "", ExecLocation: "ALL_SEGMENTS", FormatType: "t", FormatOpts: "", Options: "", Command: "", RejectLimit: 0, RejectLimitType: "", ErrTableName: "", ErrTableSchema: "", Encoding: "UTF-8", Writable: false, URIs: nil}
		testTable = backup.Table{
			Relation:        backup.Relation{Schema: "public", Name: "tablename"},
			TableDefinition: backup.TableDefinition{DistPolicy: "DISTRIBUTED RANDOMLY", PartDef: "", PartTemplateDef: "", StorageOpts: "", ExtTableDef: extTableEmpty},
		}
	})
	Describe("PrintCreateTableStatement", func() {
		It("calls PrintRegularTableCreateStatement for a regular table", func() {
			tableMetadata := backup.ObjectMetadata{Owner: "testrole"}

			testTable.IsExternal = false
			backup.PrintCreateTableStatement(backupfile, toc, testTable, tableMetadata)
			testutils.ExpectEntry(toc.PredataEntries, 0, "public", "", "tablename", "TABLE")
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
) DISTRIBUTED RANDOMLY;


ALTER TABLE public.tablename OWNER TO testrole;`)
		})
		It("calls PrintExternalTableCreateStatement for an external table", func() {
			testTable.IsExternal = true
			backup.PrintCreateTableStatement(backupfile, toc, testTable, noMetadata)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE READABLE EXTERNAL WEB TABLE public.tablename (
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

		Context("No special table attributes", func() {
			It("prints a CREATE TABLE OF type block with one attribute", func() {
				col := []backup.ColumnDefinition{rowOne}
				testTable.ColumnDefs = col
				testTable.TableType = "public.some_type"
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename OF public.some_type (
	i WITH OPTIONS
) DISTRIBUTED RANDOMLY;`)
			})

			It("prints a CREATE TABLE block with one attribute", func() {
				col := []backup.ColumnDefinition{rowOne}
				testTable.ColumnDefs = col
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block with one line per attribute", func() {
				col := []backup.ColumnDefinition{rowOne, rowTwo}
				testTable.ColumnDefs = col
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block with no attributes", func() {
				testTable.ColumnDefs = []backup.ColumnDefinition{}
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
) DISTRIBUTED RANDOMLY;`)
			})
		})
		Context("One special table attribute", func() {
			It("prints a CREATE TABLE block where one line has the given ENCODING and the other has the default ENCODING", func() {
				col := []backup.ColumnDefinition{rowOneEncoding, rowTwoEncoding}
				testTable.ColumnDefs = col
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer ENCODING (compresstype=none,blocksize=32768,compresslevel=0),
	j character varying(20) ENCODING (compresstype=zlib,blocksize=65536,compresslevel=1)
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block where one line contains NOT NULL", func() {
				col := []backup.ColumnDefinition{rowOne, rowNotNull}
				testTable.ColumnDefs = col
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20) NOT NULL
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE OF type block where one line contains NOT NULL", func() {
				col := []backup.ColumnDefinition{rowOne, rowNotNull}
				testTable.TableType = "public.some_type"
				testTable.ColumnDefs = col
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename OF public.some_type (
	i WITH OPTIONS,
	j WITH OPTIONS NOT NULL
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block where one line contains DEFAULT", func() {
				col := []backup.ColumnDefinition{rowOneDef, rowTwo}
				testTable.ColumnDefs = col
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer DEFAULT 42,
	j character varying(20)
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block where both lines contain DEFAULT", func() {
				col := []backup.ColumnDefinition{rowOneDef, rowTwoDef}
				testTable.ColumnDefs = col
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer DEFAULT 42,
	j character varying(20) DEFAULT 'bar'::text
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block where one line contains COLLATE", func() {
				col := []backup.ColumnDefinition{rowOne, colWithCollation}
				testTable.ColumnDefs = col
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer,
	c character (8) COLLATE public.some_coll
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block followed by an ALTER COLUMN ... SET STATISTICS statement", func() {
				col := []backup.ColumnDefinition{rowStats}
				testTable.ColumnDefs = col
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer
) DISTRIBUTED RANDOMLY;

ALTER TABLE ONLY public.tablename ALTER COLUMN i SET STATISTICS 3;`)
			})
			It("prints a CREATE TABLE block followed by an ALTER COLUMN ... SET STORAGE statement", func() {
				col := []backup.ColumnDefinition{colStorageType}
				testTable.ColumnDefs = col
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer
) DISTRIBUTED RANDOMLY;

ALTER TABLE ONLY public.tablename ALTER COLUMN i SET STORAGE PLAIN;`)
			})
			It("prints a CREATE TABLE block followed by an ALTER COLUMN ... SET ... statement", func() {
				col := []backup.ColumnDefinition{colOptions}
				testTable.ColumnDefs = col
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer
) DISTRIBUTED RANDOMLY;

ALTER TABLE ONLY public.tablename ALTER COLUMN i SET (n_distinct=1);`)
			})
		})
		Context("Multiple special table attributes on one column", func() {
			It("prints a CREATE TABLE block where one line contains both NOT NULL and ENCODING", func() {
				col := []backup.ColumnDefinition{rowOneEncoding, rowEncodingNotNull}
				testTable.ColumnDefs = col
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer ENCODING (compresstype=none,blocksize=32768,compresslevel=0),
	j character varying(20) NOT NULL ENCODING (compresstype=zlib,blocksize=65536,compresslevel=1)
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block where one line contains both DEFAULT and NOT NULL", func() {
				col := []backup.ColumnDefinition{rowOne, rowNotNullDef}
				testTable.ColumnDefs = col
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20) DEFAULT 'bar'::text NOT NULL
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block where one line contains both DEFAULT and ENCODING", func() {
				col := []backup.ColumnDefinition{rowOneEncoding, rowTwoEncodingDef}
				testTable.ColumnDefs = col
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer ENCODING (compresstype=none,blocksize=32768,compresslevel=0),
	j character varying(20) DEFAULT 'bar'::text ENCODING (compresstype=zlib,blocksize=65536,compresslevel=1)
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block where one line contains all three of DEFAULT, NOT NULL, and ENCODING", func() {
				col := []backup.ColumnDefinition{rowOneEncoding, rowEncodingNotNullDef}
				testTable.ColumnDefs = col
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
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
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) DISTRIBUTED BY (i);`)
			})
			It("has a multiple-column composite distribution key", func() {
				testTable.DistPolicy = distComposite
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) DISTRIBUTED BY (i, j);`)
			})
			It("is an append-optimized table", func() {
				testTable.StorageOpts = aoOpts
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) WITH (appendonly=true) DISTRIBUTED RANDOMLY;`)
			})
			It("is an append-optimized table with a single-column distribution key", func() {
				testTable.DistPolicy = distSingle
				testTable.StorageOpts = aoOpts
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) WITH (appendonly=true) DISTRIBUTED BY (i);`)
			})
			It("is an append-optimized table with a two-column composite distribution key", func() {
				testTable.DistPolicy = distComposite
				testTable.StorageOpts = aoOpts
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) WITH (appendonly=true) DISTRIBUTED BY (i, j);`)
			})
			It("is an append-optimized column-oriented table", func() {
				testTable.StorageOpts = coOpts
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) WITH (appendonly=true, orientation=column) DISTRIBUTED RANDOMLY;`)
			})
			It("is an append-optimized column-oriented table with a single-column distribution key", func() {
				testTable.DistPolicy = distSingle
				testTable.StorageOpts = coOpts
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) WITH (appendonly=true, orientation=column) DISTRIBUTED BY (i);`)
			})
			It("is an append-optimized column-oriented table with a two-column composite distribution key", func() {
				testTable.DistPolicy = distComposite
				testTable.StorageOpts = coOpts
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) WITH (appendonly=true, orientation=column) DISTRIBUTED BY (i, j);`)
			})
			It("is a heap table with a fill factor", func() {
				testTable.StorageOpts = heapFillOpts
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) WITH (fillfactor=42) DISTRIBUTED RANDOMLY;`)
			})
			It("is a heap table with a fill factor and a single-column distribution key", func() {
				testTable.DistPolicy = distSingle
				testTable.StorageOpts = heapFillOpts
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) WITH (fillfactor=42) DISTRIBUTED BY (i);`)
			})
			It("is a heap table with a fill factor and a multiple-column composite distribution key", func() {
				testTable.DistPolicy = distComposite
				testTable.StorageOpts = heapFillOpts
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) WITH (fillfactor=42) DISTRIBUTED BY (i, j);`)
			})
			It("is an append-optimized column-oriented table with complex storage options", func() {
				testTable.StorageOpts = coManyOpts
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) WITH (appendonly=true, orientation=column, fillfactor=42, compresstype=zlib, blocksize=32768, compresslevel=1) DISTRIBUTED RANDOMLY;`)
			})
			It("is an append-optimized column-oriented table with complex storage options and a single-column distribution key", func() {
				testTable.DistPolicy = distSingle
				testTable.StorageOpts = coManyOpts
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) WITH (appendonly=true, orientation=column, fillfactor=42, compresstype=zlib, blocksize=32768, compresslevel=1) DISTRIBUTED BY (i);`)
			})
			It("is an append-optimized column-oriented table with complex storage options and a two-column composite distribution key", func() {
				testTable.DistPolicy = distComposite
				testTable.StorageOpts = coManyOpts
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer,
	j character varying(20)
) WITH (appendonly=true, orientation=column, fillfactor=42, compresstype=zlib, blocksize=32768, compresslevel=1) DISTRIBUTED BY (i, j);`)
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
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
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
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
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
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
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
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
) TABLESPACE test_tablespace DISTRIBUTED RANDOMLY;`)
			})
		})
		Context("Inheritance", func() {
			It("prints a CREATE TABLE block with a single-inheritance INHERITS clause", func() {
				col := []backup.ColumnDefinition{rowOne}
				testTable.ColumnDefs = col
				testTable.Inherits = []string{"public.parent"}
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
	i integer
) INHERITS (public.parent) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block with a multiple-inheritance INHERITS clause", func() {
				col := []backup.ColumnDefinition{rowOne, rowTwo}
				testTable.ColumnDefs = col
				testTable.Inherits = []string{"public.parent_one", "public.parent_two"}
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TABLE public.tablename (
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
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE FOREIGN TABLE public.tablename (
	i integer,
	j character varying(20)
) SERVER fs ;`)
			})
			It("prints a CREATE TABLE block with options", func() {
				col := []backup.ColumnDefinition{rowOne, rowTwo}
				testTable.ColumnDefs = col
				foreignTable := backup.ForeignTableDefinition{Options: "delimiter=',' quote='\"'", Server: "fs"}
				testTable.ForeignDef = foreignTable
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE FOREIGN TABLE public.tablename (
	i integer,
	j character varying(20)
) SERVER fs OPTIONS (delimiter=',' quote='"') ;`)
			})
			It("prints a CREATE TABLE block with foreign data options on attributes", func() {
				rowWithFdwOptions := backup.ColumnDefinition{Oid: 0, Num: 1, Name: "i", Type: "integer", StatTarget: -1, ACL: []backup.ACL{}, FdwOptions: "option1 'val1', option2 'val2'"}
				col := []backup.ColumnDefinition{rowWithFdwOptions, rowTwo}
				testTable.ColumnDefs = col
				foreignTable := backup.ForeignTableDefinition{Server: "fs"}
				testTable.ForeignDef = foreignTable
				backup.PrintRegularTableCreateStatement(backupfile, toc, testTable)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE FOREIGN TABLE public.tablename (
	i integer OPTIONS (option1 'val1', option2 'val2'),
	j character varying(20)
) SERVER fs ;`)
			})
		})
	})
	Describe("PrintPostCreateTableStatements", func() {
		rowCommentOne := backup.ColumnDefinition{Oid: 0, Num: 1, Name: "i", Type: "integer", StatTarget: -1, Comment: "This is a column comment.", ACL: []backup.ACL{}}
		rowCommentTwo := backup.ColumnDefinition{Oid: 0, Num: 2, Name: "j", Type: "integer", StatTarget: -1, Comment: "This is another column comment.", ACL: []backup.ACL{}}

		It("prints a block with a table comment", func() {
			col := []backup.ColumnDefinition{rowOne}
			testTable.ColumnDefs = col
			tableMetadata := testutils.DefaultMetadata("TABLE", false, false, true, false)
			backup.PrintPostCreateTableStatements(backupfile, testTable, tableMetadata)
			testhelper.ExpectRegexp(buffer, `

COMMENT ON TABLE public.tablename IS 'This is a table comment.';`)
		})
		It("prints a block with a single column comment", func() {
			col := []backup.ColumnDefinition{rowCommentOne}
			testTable.ColumnDefs = col
			backup.PrintPostCreateTableStatements(backupfile, testTable, noMetadata)
			testhelper.ExpectRegexp(buffer, `

COMMENT ON COLUMN public.tablename.i IS 'This is a column comment.';`)
		})
		It("prints a block with a single column comment containing special characters", func() {
			rowCommentSpecialCharacters := backup.ColumnDefinition{Oid: 0, Num: 1, Name: "i", Type: "integer", StatTarget: -1, Comment: `This is a ta'ble 1+=;,./\>,<@\\n^comment.`, ACL: []backup.ACL{}}

			col := []backup.ColumnDefinition{rowCommentSpecialCharacters}
			testTable.ColumnDefs = col
			backup.PrintPostCreateTableStatements(backupfile, testTable, noMetadata)
			testhelper.ExpectRegexp(buffer, `

COMMENT ON COLUMN public.tablename.i IS 'This is a ta''ble 1+=;,./\>,<@\\n^comment.';`)
		})
		It("prints a block with multiple column comments", func() {
			col := []backup.ColumnDefinition{rowCommentOne, rowCommentTwo}
			testTable.ColumnDefs = col
			backup.PrintPostCreateTableStatements(backupfile, testTable, noMetadata)
			testhelper.ExpectRegexp(buffer, `

COMMENT ON COLUMN public.tablename.i IS 'This is a column comment.';


COMMENT ON COLUMN public.tablename.j IS 'This is another column comment.';`)
		})
		It("prints an ALTER TABLE ... OWNER TO statement to set the table owner", func() {
			col := []backup.ColumnDefinition{rowOne}
			testTable.ColumnDefs = col
			tableMetadata := testutils.DefaultMetadata("TABLE", false, true, false, false)
			backup.PrintPostCreateTableStatements(backupfile, testTable, tableMetadata)
			testhelper.ExpectRegexp(buffer, `

ALTER TABLE public.tablename OWNER TO testrole;`)
		})
		It("prints a SECURITY LABEL statement for the table", func() {
			col := []backup.ColumnDefinition{rowOne}
			testTable.ColumnDefs = col
			tableMetadata := testutils.DefaultMetadata("TABLE", false, false, false, true)
			backup.PrintPostCreateTableStatements(backupfile, testTable, tableMetadata)
			testhelper.ExpectRegexp(buffer, `

SECURITY LABEL FOR dummy ON TABLE public.tablename IS 'unclassified';`)
		})
		It("prints owner, comment, security label, and ACL statements for foreign table", func() {
			col := []backup.ColumnDefinition{rowOne}
			testTable.ColumnDefs = col
			testTable.ForeignDef = backup.ForeignTableDefinition{Oid: 23, Options: "", Server: "fs"}
			tableMetadata := testutils.DefaultMetadata("FOREIGN TABLE", true, true, true, true)
			backup.PrintPostCreateTableStatements(backupfile, testTable, tableMetadata)
			testhelper.ExpectRegexp(buffer, `

COMMENT ON FOREIGN TABLE public.tablename IS 'This is a foreign table comment.';


ALTER FOREIGN TABLE public.tablename OWNER TO testrole;


REVOKE ALL ON public.tablename FROM PUBLIC;
REVOKE ALL ON public.tablename FROM testrole;
GRANT ALL ON public.tablename TO testrole;


SECURITY LABEL FOR dummy ON FOREIGN TABLE public.tablename IS 'unclassified';`)
		})
		It("prints both an ALTER TABLE ... OWNER TO statement and comments", func() {
			col := []backup.ColumnDefinition{rowCommentOne, rowCommentTwo}
			testTable.ColumnDefs = col
			tableMetadata := backup.ObjectMetadata{Owner: "testrole", Comment: "This is a table comment."}
			backup.PrintPostCreateTableStatements(backupfile, testTable, tableMetadata)
			testhelper.ExpectRegexp(buffer, `

COMMENT ON TABLE public.tablename IS 'This is a table comment.';


ALTER TABLE public.tablename OWNER TO testrole;


COMMENT ON COLUMN public.tablename.i IS 'This is a column comment.';


COMMENT ON COLUMN public.tablename.j IS 'This is another column comment.';`)
		})
		It("prints a GRANT statement on a table column", func() {
			privilegesColumnOne := backup.ColumnDefinition{Oid: 0, Num: 1, Name: "i", Type: "integer", StatTarget: -1, ACL: []backup.ACL{{Grantee: "testrole", Select: true}}}
			privilegesColumnTwo := backup.ColumnDefinition{Oid: 1, Num: 2, Name: "j", Type: "character varying(20)", StatTarget: -1, ACL: []backup.ACL{{Grantee: "testrole2", Select: true, Insert: true, Update: true, References: true}}}
			col := []backup.ColumnDefinition{privilegesColumnOne, privilegesColumnTwo}
			testTable.ColumnDefs = col
			tableMetadata := backup.ObjectMetadata{Owner: "testrole"}
			backup.PrintPostCreateTableStatements(backupfile, testTable, tableMetadata)
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
			backup.PrintPostCreateTableStatements(backupfile, testTable, backup.ObjectMetadata{})
			testhelper.ExpectRegexp(buffer, `

SECURITY LABEL FOR dummy ON COLUMN public.tablename.i IS 'unclassified';

SECURITY LABEL FOR dummy ON COLUMN public.tablename.j IS 'unclassified';`)
		})
	})
	Describe("PrintCreateSequenceStatements", func() {
		baseSequence := backup.Relation{SchemaOid: 0, Oid: 1, Schema: "public", Name: "seq_name"}
		seqDefault := backup.Sequence{Relation: baseSequence, SequenceDefinition: backup.SequenceDefinition{Name: "seq_name", LastVal: 7, Increment: 1, MaxVal: math.MaxInt64, MinVal: 1, CacheVal: 5, LogCnt: 42, IsCycled: false, IsCalled: true}}
		seqNegIncr := backup.Sequence{Relation: baseSequence, SequenceDefinition: backup.SequenceDefinition{Name: "seq_name", LastVal: 7, Increment: -1, MaxVal: -1, MinVal: math.MinInt64, CacheVal: 5, LogCnt: 42, IsCycled: false, IsCalled: true}}
		seqMaxPos := backup.Sequence{Relation: baseSequence, SequenceDefinition: backup.SequenceDefinition{Name: "seq_name", LastVal: 7, Increment: 1, MaxVal: 100, MinVal: 1, CacheVal: 5, LogCnt: 42, IsCycled: false, IsCalled: true}}
		seqMinPos := backup.Sequence{Relation: baseSequence, SequenceDefinition: backup.SequenceDefinition{Name: "seq_name", LastVal: 7, Increment: 1, MaxVal: math.MaxInt64, MinVal: 10, CacheVal: 5, LogCnt: 42, IsCycled: false, IsCalled: true}}
		seqMaxNeg := backup.Sequence{Relation: baseSequence, SequenceDefinition: backup.SequenceDefinition{Name: "seq_name", LastVal: 7, Increment: -1, MaxVal: -10, MinVal: math.MinInt64, CacheVal: 5, LogCnt: 42, IsCycled: false, IsCalled: true}}
		seqMinNeg := backup.Sequence{Relation: baseSequence, SequenceDefinition: backup.SequenceDefinition{Name: "seq_name", LastVal: 7, Increment: -1, MaxVal: -1, MinVal: -100, CacheVal: 5, LogCnt: 42, IsCycled: false, IsCalled: true}}
		seqCycle := backup.Sequence{Relation: baseSequence, SequenceDefinition: backup.SequenceDefinition{Name: "seq_name", LastVal: 7, Increment: 1, MaxVal: math.MaxInt64, MinVal: 1, CacheVal: 5, LogCnt: 42, IsCycled: true, IsCalled: true}}
		seqStart := backup.Sequence{Relation: baseSequence, SequenceDefinition: backup.SequenceDefinition{Name: "seq_name", LastVal: 7, Increment: 1, MaxVal: math.MaxInt64, MinVal: 1, CacheVal: 5, LogCnt: 42, IsCycled: false, IsCalled: false}}
		emptySequenceMetadataMap := backup.MetadataMap{}

		It("can print a sequence with all default options", func() {
			sequences := []backup.Sequence{seqDefault}
			backup.PrintCreateSequenceStatements(backupfile, toc, sequences, emptySequenceMetadataMap)
			testutils.ExpectEntry(toc.PredataEntries, 0, "public", "", "seq_name", "SEQUENCE")
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE SEQUENCE public.seq_name
	INCREMENT BY 1
	NO MAXVALUE
	NO MINVALUE
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, true);`)
		})
		It("can print a decreasing sequence", func() {
			sequences := []backup.Sequence{seqNegIncr}
			backup.PrintCreateSequenceStatements(backupfile, toc, sequences, emptySequenceMetadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE SEQUENCE public.seq_name
	INCREMENT BY -1
	NO MAXVALUE
	NO MINVALUE
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, true);`)
		})
		It("can print an increasing sequence with a maximum value", func() {
			sequences := []backup.Sequence{seqMaxPos}
			backup.PrintCreateSequenceStatements(backupfile, toc, sequences, emptySequenceMetadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE SEQUENCE public.seq_name
	INCREMENT BY 1
	MAXVALUE 100
	NO MINVALUE
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, true);`)
		})
		It("can print an increasing sequence with a minimum value", func() {
			sequences := []backup.Sequence{seqMinPos}
			backup.PrintCreateSequenceStatements(backupfile, toc, sequences, emptySequenceMetadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE SEQUENCE public.seq_name
	INCREMENT BY 1
	NO MAXVALUE
	MINVALUE 10
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, true);`)
		})
		It("can print a decreasing sequence with a maximum value", func() {
			sequences := []backup.Sequence{seqMaxNeg}
			backup.PrintCreateSequenceStatements(backupfile, toc, sequences, emptySequenceMetadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE SEQUENCE public.seq_name
	INCREMENT BY -1
	MAXVALUE -10
	NO MINVALUE
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, true);`)
		})
		It("can print a decreasing sequence with a minimum value", func() {
			sequences := []backup.Sequence{seqMinNeg}
			backup.PrintCreateSequenceStatements(backupfile, toc, sequences, emptySequenceMetadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE SEQUENCE public.seq_name
	INCREMENT BY -1
	NO MAXVALUE
	MINVALUE -100
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, true);`)
		})
		It("can print a sequence that cycles", func() {
			sequences := []backup.Sequence{seqCycle}
			backup.PrintCreateSequenceStatements(backupfile, toc, sequences, emptySequenceMetadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE SEQUENCE public.seq_name
	INCREMENT BY 1
	NO MAXVALUE
	NO MINVALUE
	CACHE 5
	CYCLE;

SELECT pg_catalog.setval('public.seq_name', 7, true);`)
		})
		It("can print a sequence with a start value", func() {
			sequences := []backup.Sequence{seqStart}
			backup.PrintCreateSequenceStatements(backupfile, toc, sequences, emptySequenceMetadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE SEQUENCE public.seq_name
	START WITH 7
	INCREMENT BY 1
	NO MAXVALUE
	NO MINVALUE
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, false);`)
		})
		It("escapes a sequence containing single quotes", func() {
			baseSequenceWithQuote := backup.Relation{SchemaOid: 0, Oid: 1, Schema: "public", Name: "seq_'name"}
			seqWithQuote := backup.Sequence{Relation: baseSequenceWithQuote, SequenceDefinition: backup.SequenceDefinition{Name: "seq_'name", LastVal: 7, Increment: 1, MaxVal: math.MaxInt64, MinVal: 1, CacheVal: 5, LogCnt: 42, IsCycled: false, IsCalled: true}}
			sequences := []backup.Sequence{seqWithQuote}
			backup.PrintCreateSequenceStatements(backupfile, toc, sequences, emptySequenceMetadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE SEQUENCE public.seq_'name
	INCREMENT BY 1
	NO MAXVALUE
	NO MINVALUE
	CACHE 5;

SELECT pg_catalog.setval('public.seq_''name', 7, true);`)
		})
		It("can print a sequence with privileges, an owner, and a comment for version < 6", func() {
			testhelper.SetDBVersion(connectionPool, "5.0.0")
			sequenceMetadataMap := testutils.DefaultMetadataMap("SEQUENCE", true, true, true, false)
			sequenceMetadata := sequenceMetadataMap[seqDefault.GetUniqueID()]
			sequenceMetadata.Privileges[0].Update = false
			sequenceMetadataMap[seqDefault.GetUniqueID()] = sequenceMetadata
			sequences := []backup.Sequence{seqDefault}
			backup.PrintCreateSequenceStatements(backupfile, toc, sequences, sequenceMetadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE SEQUENCE public.seq_name
	INCREMENT BY 1
	NO MAXVALUE
	NO MINVALUE
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, true);


COMMENT ON SEQUENCE public.seq_name IS 'This is a sequence comment.';


ALTER TABLE public.seq_name OWNER TO testrole;


REVOKE ALL ON SEQUENCE public.seq_name FROM PUBLIC;
REVOKE ALL ON SEQUENCE public.seq_name FROM testrole;
GRANT SELECT,USAGE ON SEQUENCE public.seq_name TO testrole;`)
			testhelper.SetDBVersion(connectionPool, "5.1.0")
		})
		It("can print a sequence with privileges, an owner, security label, and a comment for version >= 6", func() {
			testhelper.SetDBVersion(connectionPool, "6.0.0")
			sequenceMetadataMap := testutils.DefaultMetadataMap("SEQUENCE", true, true, true, true)
			sequenceMetadata := sequenceMetadataMap[seqDefault.GetUniqueID()]
			sequenceMetadata.Privileges[0].Update = false
			sequenceMetadataMap[seqDefault.GetUniqueID()] = sequenceMetadata
			sequences := []backup.Sequence{seqDefault}
			backup.PrintCreateSequenceStatements(backupfile, toc, sequences, sequenceMetadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE SEQUENCE public.seq_name
	START WITH 0
	INCREMENT BY 1
	NO MAXVALUE
	NO MINVALUE
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, true);


COMMENT ON SEQUENCE public.seq_name IS 'This is a sequence comment.';


ALTER SEQUENCE public.seq_name OWNER TO testrole;


REVOKE ALL ON SEQUENCE public.seq_name FROM PUBLIC;
REVOKE ALL ON SEQUENCE public.seq_name FROM testrole;
GRANT SELECT,USAGE ON SEQUENCE public.seq_name TO testrole;


SECURITY LABEL FOR dummy ON SEQUENCE public.seq_name IS 'unclassified';`)
			testhelper.SetDBVersion(connectionPool, "5.1.0")
		})
		It("can print a sequence with privileges WITH GRANT OPTION", func() {
			sequenceMetadata := backup.ObjectMetadata{Privileges: []backup.ACL{testutils.DefaultACLWithGrantWithout("testrole", "SEQUENCE", "UPDATE")}}
			sequenceMetadataMap := backup.MetadataMap{seqDefault.GetUniqueID(): sequenceMetadata}
			sequences := []backup.Sequence{seqDefault}
			backup.PrintCreateSequenceStatements(backupfile, toc, sequences, sequenceMetadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE SEQUENCE public.seq_name
	INCREMENT BY 1
	NO MAXVALUE
	NO MINVALUE
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, true);


REVOKE ALL ON SEQUENCE public.seq_name FROM PUBLIC;
GRANT SELECT,USAGE ON SEQUENCE public.seq_name TO testrole WITH GRANT OPTION;`)
		})
	})
	Describe("PrintCreateViewStatement", func() {
		var (
			view          backup.View
			emptyMetadata backup.ObjectMetadata
		)
		BeforeEach(func() {
			view = backup.View{Oid: 1, Schema: "shamwow", Name: "shazam", Definition: "SELECT count(*) FROM pg_tables;"}
			emptyMetadata = backup.ObjectMetadata{}
		})
		It("can print a basic view", func() {
			backup.PrintCreateViewStatement(backupfile, toc, view, emptyMetadata)
			testutils.ExpectEntry(toc.PredataEntries, 0, "shamwow", "", "shazam", "VIEW")
			testutils.AssertBufferContents(toc.PredataEntries, buffer,
				`CREATE VIEW shamwow.shazam AS SELECT count(*) FROM pg_tables;`)
		})
		It("can print a view with privileges, an owner, and a comment for version < 6", func() {
			testhelper.SetDBVersion(connectionPool, "5.0.0")
			defer testhelper.SetDBVersion(connectionPool, "5.1.0")

			viewMetadata := testutils.DefaultMetadata("VIEW", true, true, true, false)
			backup.PrintCreateViewStatement(backupfile, toc, view, viewMetadata)
			testutils.AssertBufferContents(toc.PredataEntries, buffer,
				`CREATE VIEW shamwow.shazam AS SELECT count(*) FROM pg_tables;


COMMENT ON VIEW shamwow.shazam IS 'This is a view comment.';


ALTER TABLE shamwow.shazam OWNER TO testrole;


REVOKE ALL ON shamwow.shazam FROM PUBLIC;
REVOKE ALL ON shamwow.shazam FROM testrole;
GRANT ALL ON shamwow.shazam TO testrole;`)
		})
		It("can print a view with privileges, an owner, security label, and a comment for version >= 6", func() {
			testhelper.SetDBVersion(connectionPool, "6.0.0")
			defer testhelper.SetDBVersion(connectionPool, "5.1.0")

			viewMetadata := testutils.DefaultMetadata("VIEW", true, true, true, true)
			backup.PrintCreateViewStatement(backupfile, toc, view, viewMetadata)
			testutils.AssertBufferContents(toc.PredataEntries, buffer,
				`CREATE VIEW shamwow.shazam AS SELECT count(*) FROM pg_tables;


COMMENT ON VIEW shamwow.shazam IS 'This is a view comment.';


ALTER VIEW shamwow.shazam OWNER TO testrole;


REVOKE ALL ON shamwow.shazam FROM PUBLIC;
REVOKE ALL ON shamwow.shazam FROM testrole;
GRANT ALL ON shamwow.shazam TO testrole;


SECURITY LABEL FOR dummy ON VIEW shamwow.shazam IS 'unclassified';`)
		})
		It("can print a view with options", func() {
			view.Options = " WITH (security_barrier=true)"
			backup.PrintCreateViewStatement(backupfile, toc, view, emptyMetadata)
			testutils.ExpectEntry(toc.PredataEntries, 0, "shamwow", "", "shazam", "VIEW")
			testutils.AssertBufferContents(toc.PredataEntries, buffer,
				`CREATE VIEW shamwow.shazam WITH (security_barrier=true) AS SELECT count(*) FROM pg_tables;`)
		})
	})
	Describe("PrintAlterSequenceStatements", func() {
		baseSequence := backup.Relation{Schema: "public", Name: "seq_name"}
		seqDefault := backup.Sequence{Relation: baseSequence, SequenceDefinition: backup.SequenceDefinition{Name: "seq_name", LastVal: 7, Increment: 1, MaxVal: math.MaxInt64, MinVal: 1, CacheVal: 5, LogCnt: 42, IsCycled: false, IsCalled: true}}
		emptyColumnOwnerMap := make(map[string]string, 0)
		It("prints nothing for a sequence without an owning column", func() {
			sequences := []backup.Sequence{seqDefault}
			backup.PrintAlterSequenceStatements(backupfile, toc, sequences, emptyColumnOwnerMap)
			Expect(toc.PredataEntries).To(BeEmpty())
			testhelper.NotExpectRegexp(buffer, `ALTER SEQUENCE`)
		})
		It("does not write an alter sequence statement for a sequence that is not in the backup", func() {
			columnOwnerMap := map[string]string{"public.seq_name2": "public.tablename.col_one"}
			sequences := []backup.Sequence{seqDefault}
			backup.PrintAlterSequenceStatements(backupfile, toc, sequences, columnOwnerMap)
			Expect(toc.PredataEntries).To(BeEmpty())
			testhelper.NotExpectRegexp(buffer, `ALTER SEQUENCE`)
		})
		It("can print an ALTER SEQUENCE statement for a sequence with an owning column", func() {
			columnOwnerMap := map[string]string{"public.seq_name": "public.tablename.col_one"}
			sequences := []backup.Sequence{seqDefault}
			backup.PrintAlterSequenceStatements(backupfile, toc, sequences, columnOwnerMap)
			testutils.ExpectEntry(toc.PredataEntries, 0, "public", "", "seq_name", "SEQUENCE OWNER")
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `ALTER SEQUENCE public.seq_name OWNED BY public.tablename.col_one;`)
		})
	})
	Describe("SplitTablesByPartitionType", func() {
		var tables []backup.Table
		var includeList []string
		var expectedMetadataTables = []backup.Table{
			{
				Relation:        backup.Relation{Oid: 1, Schema: "public", Name: "part_parent1"},
				TableDefinition: backup.TableDefinition{PartitionLevelInfo: backup.PartitionLevelInfo{Level: "p"}},
			},
			{
				Relation:        backup.Relation{Oid: 2, Schema: "public", Name: "part_parent2"},
				TableDefinition: backup.TableDefinition{PartitionLevelInfo: backup.PartitionLevelInfo{Level: "p"}},
			},
			{
				Relation:        backup.Relation{Oid: 8, Schema: "public", Name: "test_table"},
				TableDefinition: backup.TableDefinition{PartitionLevelInfo: backup.PartitionLevelInfo{Level: "n"}},
			},
		}
		BeforeEach(func() {
			tables = []backup.Table{
				{
					Relation:        backup.Relation{Oid: 1, Schema: "public", Name: "part_parent1"},
					TableDefinition: backup.TableDefinition{PartitionLevelInfo: backup.PartitionLevelInfo{Level: "p"}},
				},
				{
					Relation:        backup.Relation{Oid: 2, Schema: "public", Name: "part_parent2"},
					TableDefinition: backup.TableDefinition{PartitionLevelInfo: backup.PartitionLevelInfo{Level: "p"}},
				},
				{
					Relation:        backup.Relation{Oid: 3, Schema: "public", Name: "part_parent1_inter1"},
					TableDefinition: backup.TableDefinition{PartitionLevelInfo: backup.PartitionLevelInfo{Level: "i"}},
				},
				{
					Relation:        backup.Relation{Oid: 4, Schema: "public", Name: "part_parent1_child1"},
					TableDefinition: backup.TableDefinition{PartitionLevelInfo: backup.PartitionLevelInfo{Level: "l"}},
				},
				{
					Relation:        backup.Relation{Oid: 5, Schema: "public", Name: "part_parent1_child2"},
					TableDefinition: backup.TableDefinition{PartitionLevelInfo: backup.PartitionLevelInfo{Level: "l"}},
				},
				{
					Relation:        backup.Relation{Oid: 6, Schema: "public", Name: "part_parent2_child1"},
					TableDefinition: backup.TableDefinition{PartitionLevelInfo: backup.PartitionLevelInfo{Level: "l"}},
				},
				{
					Relation:        backup.Relation{Oid: 7, Schema: "public", Name: "part_parent2_child2"},
					TableDefinition: backup.TableDefinition{PartitionLevelInfo: backup.PartitionLevelInfo{Level: "l"}},
				},
				{
					Relation:        backup.Relation{Oid: 8, Schema: "public", Name: "test_table"},
					TableDefinition: backup.TableDefinition{PartitionLevelInfo: backup.PartitionLevelInfo{Level: "n"}},
				},
			}
		})
		Context("leafPartitionData and includeTables", func() {
			It("gets only parent partitions of included tables for metadata and only child partitions for data", func() {
				includeList = []string{"public.part_parent1", "public.part_parent2_child1", "public.part_parent2_child2", "public.test_table"}
				cmdFlags.Set(utils.LEAF_PARTITION_DATA, "true")

				metadataTables, dataTables := backup.SplitTablesByPartitionType(tables, includeList)

				Expect(metadataTables).To(Equal(expectedMetadataTables))

				expectedDataTables := []string{"public.part_parent1_child1", "public.part_parent1_child2", "public.part_parent2_child1", "public.part_parent2_child2", "public.test_table"}
				dataTableNames := make([]string, 0)
				for _, table := range dataTables {
					dataTableNames = append(dataTableNames, table.FQN())
				}
				sort.Strings(dataTableNames)

				Expect(dataTables).To(HaveLen(5))
				Expect(dataTableNames).To(Equal(expectedDataTables))
			})
		})
		Context("leafPartitionData only", func() {
			It("gets only parent partitions for metadata and only child partitions in data", func() {
				cmdFlags.Set(utils.LEAF_PARTITION_DATA, "true")
				includeList = []string{}
				metadataTables, dataTables := backup.SplitTablesByPartitionType(tables, includeList)

				Expect(metadataTables).To(Equal(expectedMetadataTables))

				expectedDataTables := []string{"public.part_parent1_child1", "public.part_parent1_child2", "public.part_parent2_child1", "public.part_parent2_child2", "public.test_table"}
				dataTableNames := make([]string, 0)
				for _, table := range dataTables {
					dataTableNames = append(dataTableNames, table.FQN())
				}
				sort.Strings(dataTableNames)

				Expect(dataTables).To(HaveLen(5))
				Expect(dataTableNames).To(Equal(expectedDataTables))
			})
		})
		Context("includeTables only", func() {
			It("gets only parent partitions of included tables for metadata and only included tables for data", func() {
				cmdFlags.Set(utils.LEAF_PARTITION_DATA, "false")
				includeList = []string{"public.part_parent1", "public.part_parent2_child1", "public.part_parent2_child2", "public.test_table"}
				metadataTables, dataTables := backup.SplitTablesByPartitionType(tables, includeList)

				Expect(metadataTables).To(Equal(expectedMetadataTables))

				expectedDataTables := []string{"public.part_parent1", "public.part_parent2_child1", "public.part_parent2_child2", "public.test_table"}
				dataTableNames := make([]string, 0)
				for _, table := range dataTables {
					dataTableNames = append(dataTableNames, table.FQN())
				}
				sort.Strings(dataTableNames)

				Expect(dataTables).To(HaveLen(4))
				Expect(dataTableNames).To(Equal(expectedDataTables))
			})
		})
		Context("neither leafPartitionData nor includeTables", func() {
			It("gets the same table list for both metadata and data", func() {
				includeList = []string{}
				tables = []backup.Table{
					{
						Relation:        backup.Relation{Oid: 1, Schema: "public", Name: "part_parent1"},
						TableDefinition: backup.TableDefinition{PartitionLevelInfo: backup.PartitionLevelInfo{Level: "p"}},
					},
					{
						Relation:        backup.Relation{Oid: 2, Schema: "public", Name: "part_parent2"},
						TableDefinition: backup.TableDefinition{PartitionLevelInfo: backup.PartitionLevelInfo{Level: "p"}},
					},
					{
						Relation:        backup.Relation{Oid: 8, Schema: "public", Name: "test_table"},
						TableDefinition: backup.TableDefinition{PartitionLevelInfo: backup.PartitionLevelInfo{Level: "n"}},
					},
				}
				cmdFlags.Set(utils.LEAF_PARTITION_DATA, "false")
				cmdFlags.Set(utils.INCLUDE_RELATION, "")
				metadataTables, dataTables := backup.SplitTablesByPartitionType(tables, includeList)

				Expect(metadataTables).To(Equal(expectedMetadataTables))

				expectedDataTables := []string{"public.part_parent1", "public.part_parent2", "public.test_table"}
				dataTableNames := make([]string, 0)
				for _, table := range dataTables {
					dataTableNames = append(dataTableNames, table.FQN())
				}
				sort.Strings(dataTableNames)

				Expect(dataTables).To(HaveLen(3))
				Expect(dataTableNames).To(Equal(expectedDataTables))
			})
			It("adds a suffix to external partition tables", func() {
				includeList = []string{}
				tables = []backup.Table{
					{
						Relation:        backup.Relation{Oid: 1, Schema: "public", Name: "part_parent1_prt_1"},
						TableDefinition: backup.TableDefinition{PartitionLevelInfo: backup.PartitionLevelInfo{Level: "l"}, IsExternal: true},
					},
					{
						Relation:        backup.Relation{Oid: 2, Schema: "public", Name: "long_naaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaame"},
						TableDefinition: backup.TableDefinition{PartitionLevelInfo: backup.PartitionLevelInfo{Level: "l"}, IsExternal: true},
					},
				}
				cmdFlags.Set(utils.LEAF_PARTITION_DATA, "false")
				cmdFlags.Set(utils.INCLUDE_RELATION, "")
				metadataTables, _ := backup.SplitTablesByPartitionType(tables, includeList)

				expectedTables := []backup.Table{
					{
						Relation:        backup.Relation{Oid: 1, Schema: "public", Name: "part_parent1_prt_1_ext_part_"},
						TableDefinition: backup.TableDefinition{PartitionLevelInfo: backup.PartitionLevelInfo{Level: "l"}, IsExternal: true},
					},
					{
						Relation:        backup.Relation{Oid: 2, Schema: "public", Name: "long_naaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa_ext_part_"},
						TableDefinition: backup.TableDefinition{PartitionLevelInfo: backup.PartitionLevelInfo{Level: "l"}, IsExternal: true},
					},
				}
				Expect(metadataTables).To(HaveLen(2))
				structmatcher.ExpectStructsToMatch(&expectedTables[0], &metadataTables[0])
				structmatcher.ExpectStructsToMatch(&expectedTables[1], &metadataTables[1])
			})
		})
	})
	Describe("AppendExtPartSuffix", func() {
		It("adds a suffix to an unquoted external partition table", func() {
			tablename := "name"
			expectedName := "name_ext_part_"
			suffixName := backup.AppendExtPartSuffix(tablename)
			Expect(suffixName).To(Equal(expectedName))
		})
		It("adds a suffix to an unquoted external partition table that is too long", func() {
			tablename := "long_naaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaame"
			expectedName := "long_naaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa_ext_part_"
			suffixName := backup.AppendExtPartSuffix(tablename)
			Expect(suffixName).To(Equal(expectedName))
		})
		It("adds a suffix to a quoted external partition table", func() {
			tablename := `"!name"`
			expectedName := `"!name_ext_part_"`
			suffixName := backup.AppendExtPartSuffix(tablename)
			Expect(suffixName).To(Equal(expectedName))
		})
		It("adds a suffix to a quoted external partition table that is too long", func() {
			tablename := `"long!naaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaame"`
			expectedName := `"long!naaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa_ext_part_"`
			suffixName := backup.AppendExtPartSuffix(tablename)
			Expect(suffixName).To(Equal(expectedName))
		})
	})
	Describe("ExpandIncludeRelations", func() {
		testTables := []backup.Relation{{Schema: "testschema", Name: "foo1"}, {Schema: "testschema", Name: "foo2"}}
		It("returns an empty slice if no includeRelations were specified", func() {
			cmdFlags.Set(utils.INCLUDE_RELATION, "")
			backup.ExpandIncludeRelations(testTables)

			Expect(backup.MustGetFlagStringSlice(utils.INCLUDE_RELATION)).To(BeEmpty())
		})
		It("returns original include list if the new tables list is a subset of existing list", func() {
			cmdFlags.Set(utils.INCLUDE_RELATION, "testschema.foo1,testschema.foo2,testschema.foo3")
			backup.ExpandIncludeRelations(testTables)

			Expect(backup.MustGetFlagStringSlice(utils.INCLUDE_RELATION)).To(HaveLen(3))
			Expect(backup.MustGetFlagStringSlice(utils.INCLUDE_RELATION)).
				To(ConsistOf([]string{"testschema.foo1", "testschema.foo2", "testschema.foo3"}))
		})
		It("returns expanded include list if there are new tables to add", func() {
			cmdFlags.Set(utils.INCLUDE_RELATION, "testschema.foo2,testschema.foo3")
			backup.ExpandIncludeRelations(testTables)

			Expect(backup.MustGetFlagStringSlice(utils.INCLUDE_RELATION)).To(HaveLen(3))
			Expect(backup.MustGetFlagStringSlice(utils.INCLUDE_RELATION)).
				To(ConsistOf([]string{"testschema.foo1", "testschema.foo2", "testschema.foo3"}))
		})
	})
	Describe("ConstructColumnPrivilegesMap", func() {
		expectedACL := []backup.ACL{{Grantee: "gpadmin", Select: true}}
		colI := backup.ColumnPrivilegesQueryStruct{TableOid: 1, Name: "i", Privileges: sql.NullString{String: "gpadmin=r/gpadmin", Valid: true}, Kind: ""}
		colJ := backup.ColumnPrivilegesQueryStruct{TableOid: 1, Name: "j", Privileges: sql.NullString{String: "gpadmin=r/gpadmin", Valid: true}, Kind: ""}
		colK1 := backup.ColumnPrivilegesQueryStruct{TableOid: 2, Name: "k", Privileges: sql.NullString{String: "gpadmin=r/gpadmin", Valid: true}, Kind: ""}
		colK2 := backup.ColumnPrivilegesQueryStruct{TableOid: 2, Name: "k", Privileges: sql.NullString{String: "testrole=r/testrole", Valid: true}, Kind: ""}
		colDefault := backup.ColumnPrivilegesQueryStruct{TableOid: 2, Name: "l", Privileges: sql.NullString{String: "", Valid: false}, Kind: "Default"}
		colEmpty := backup.ColumnPrivilegesQueryStruct{TableOid: 2, Name: "m", Privileges: sql.NullString{String: "", Valid: false}, Kind: "Empty"}
		privileges := []backup.ColumnPrivilegesQueryStruct{}
		BeforeEach(func() {
			rolnames := sqlmock.NewRows([]string{"rolename", "quotedrolename"}).
				AddRow("gpadmin", "gpadmin").
				AddRow("testrole", "testrole")
			mock.ExpectQuery("SELECT rolname (.*)").
				WillReturnRows(rolnames)
			privileges = []backup.ColumnPrivilegesQueryStruct{}
		})
		It("No columns", func() {
			metadataMap := backup.ConstructColumnPrivilegesMap(privileges)
			Expect(metadataMap).To(BeEmpty())
		})
		It("One column", func() {
			privileges = []backup.ColumnPrivilegesQueryStruct{colI}
			metadataMap := backup.ConstructColumnPrivilegesMap(privileges)
			Expect(metadataMap).To(HaveLen(1))
			Expect(metadataMap[1]).To(HaveLen(1))
			Expect(metadataMap[1]["i"]).To(Equal(expectedACL))
		})
		It("Multiple columns on same table", func() {
			privileges = []backup.ColumnPrivilegesQueryStruct{colI, colJ}
			metadataMap := backup.ConstructColumnPrivilegesMap(privileges)
			Expect(metadataMap).To(HaveLen(1))
			Expect(metadataMap[1]).To(HaveLen(2))
			Expect(metadataMap[1]["i"]).To(Equal(expectedACL))
			Expect(metadataMap[1]["j"]).To(Equal(expectedACL))
		})
		It("Multiple columns on multiple tables", func() {
			privileges = []backup.ColumnPrivilegesQueryStruct{colI, colJ, colK1, colK2}
			metadataMap := backup.ConstructColumnPrivilegesMap(privileges)

			expectedACLForK := []backup.ACL{{Grantee: "gpadmin", Select: true}, {Grantee: "testrole", Select: true}}

			Expect(metadataMap).To(HaveLen(2))
			Expect(metadataMap[1]).To(HaveLen(2))
			Expect(metadataMap[2]).To(HaveLen(1))
			Expect(metadataMap[1]["i"]).To(Equal(expectedACL))
			Expect(metadataMap[1]["j"]).To(Equal(expectedACL))
			Expect(metadataMap[2]["k"]).To(Equal(expectedACLForK))
		})
		It("Default kind", func() {
			privileges = []backup.ColumnPrivilegesQueryStruct{colDefault}
			metadataMap := backup.ConstructColumnPrivilegesMap(privileges)

			expectedACLForDefaultKind := []backup.ACL{}

			Expect(metadataMap).To(HaveLen(1))
			Expect(metadataMap[2]).To(HaveLen(1))
			Expect(metadataMap[2]["l"]).To(Equal(expectedACLForDefaultKind))
		})
		It("'Empty' kind", func() {
			privileges = []backup.ColumnPrivilegesQueryStruct{colEmpty}
			metadataMap := backup.ConstructColumnPrivilegesMap(privileges)

			expectedACLForEmptyKind := []backup.ACL{{Grantee: "GRANTEE"}}

			Expect(metadataMap).To(HaveLen(1))
			Expect(metadataMap[2]).To(HaveLen(1))
			Expect(metadataMap[2]["m"]).To(Equal(expectedACLForEmptyKind))
		})
	})
})
