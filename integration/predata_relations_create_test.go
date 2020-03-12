package integration

import (
	"database/sql"
	"fmt"
	"math"

	"github.com/greenplum-db/gp-common-go-libs/structmatcher"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup integration create statement tests", func() {
	BeforeEach(func() {
		tocfile, backupfile = testutils.InitializeTestTOC(buffer, "predata")
	})
	Describe("PrintRegularTableCreateStatement", func() {
		var (
			extTableEmpty                 backup.ExternalTableDefinition
			testTable                     backup.Table
			partitionPartFalseExpectation = "false"
		)
		BeforeEach(func() {
			extTableEmpty = backup.ExternalTableDefinition{Oid: 0, Type: -2, Protocol: -2, Location: "", ExecLocation: "ALL_SEGMENTS", FormatType: "t", FormatOpts: "", Options: "", Command: "", RejectLimit: 0, RejectLimitType: "", ErrTableName: "", ErrTableSchema: "", Encoding: "UTF-8", Writable: false, URIs: nil}
			testTable = backup.Table{
				Relation:        backup.Relation{Schema: "public", Name: "testtable"},
				TableDefinition: backup.TableDefinition{DistPolicy: "DISTRIBUTED RANDOMLY", ExtTableDef: extTableEmpty, Inherits: []string{}},
			}
			if connectionPool.Version.AtLeast("6") {
				partitionPartFalseExpectation = "'false'"
				testTable.ReplicaIdentity = "d"
			}
		})
		AfterEach(func() {
			testhelper.AssertQueryRuns(connectionPool, "DROP TABLE IF EXISTS public.testtable")
		})
		It("creates a table with no attributes", func() {
			backup.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			testTable.Oid = testutils.OidFromObjectName(connectionPool, "public", "testtable", backup.TYPE_RELATION)
			resultTable := backup.ConstructDefinitionsForTables(connectionPool, []backup.Relation{testTable.Relation})[0]
			structmatcher.ExpectStructsToMatchExcluding(testTable.TableDefinition, resultTable.TableDefinition, "ColumnDefs.Oid", "ExtTableDef")
		})
		It("creates a table of a type", func() {
			testutils.SkipIfBefore6(connectionPool)
			testhelper.AssertQueryRuns(connectionPool, `CREATE TYPE public.some_type AS (i text, j numeric)`)
			defer testhelper.AssertQueryRuns(connectionPool, `DROP TYPE public.some_type CASCADE`)

			rowOne := backup.ColumnDefinition{Oid: 0, Num: 1, Name: "i", NotNull: false, HasDefault: false, Type: "text", Encoding: "", StatTarget: -1, StorageType: "", DefaultVal: "", Comment: ""}
			rowTwo := backup.ColumnDefinition{Oid: 0, Num: 2, Name: "j", NotNull: false, HasDefault: false, Type: "numeric", Encoding: "", StatTarget: -1, StorageType: "", DefaultVal: "", Comment: ""}
			testTable.ColumnDefs = []backup.ColumnDefinition{rowOne, rowTwo}

			testTable.TableType = "public.some_type"
			backup.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)
			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			testTable.Oid = testutils.OidFromObjectName(connectionPool, "public", "testtable", backup.TYPE_RELATION)
			resultTable := backup.ConstructDefinitionsForTables(connectionPool, []backup.Relation{testTable.Relation})[0]

			structmatcher.ExpectStructsToMatchExcluding(testTable.TableDefinition, resultTable.TableDefinition, "ColumnDefs.Oid", "ExtTableDef")
		})

		It("creates a basic heap table", func() {
			rowOne := backup.ColumnDefinition{Oid: 0, Num: 1, Name: "i", NotNull: false, HasDefault: false, Type: "integer", Encoding: "", StatTarget: -1, StorageType: "", DefaultVal: "", Comment: ""}
			rowTwo := backup.ColumnDefinition{Oid: 0, Num: 2, Name: "j", NotNull: false, HasDefault: false, Type: "character varying(20)", Encoding: "", StatTarget: -1, StorageType: "", DefaultVal: "", Comment: ""}
			testTable.ColumnDefs = []backup.ColumnDefinition{rowOne, rowTwo}

			backup.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			testTable.Oid = testutils.OidFromObjectName(connectionPool, "public", "testtable", backup.TYPE_RELATION)
			resultTable := backup.ConstructDefinitionsForTables(connectionPool, []backup.Relation{testTable.Relation})[0]
			structmatcher.ExpectStructsToMatchExcluding(testTable.TableDefinition, resultTable.TableDefinition, "ColumnDefs.Oid", "ExtTableDef")
		})
		It("creates a complex heap table", func() {
			rowOneDefault := backup.ColumnDefinition{Oid: 0, Num: 1, Name: "i", NotNull: false, HasDefault: true, Type: "integer", Encoding: "", StatTarget: -1, StorageType: "", DefaultVal: "42", Comment: ""}
			rowNotNullDefault := backup.ColumnDefinition{Oid: 0, Num: 2, Name: "j", NotNull: true, HasDefault: true, Type: "character varying(20)", Encoding: "", StatTarget: -1, StorageType: "", DefaultVal: "'bar'::text", Comment: ""}
			rowNonDefaultStorageAndStats := backup.ColumnDefinition{Oid: 0, Num: 3, Name: "k", NotNull: false, HasDefault: false, Type: "text", Encoding: "", StatTarget: 3, StorageType: "PLAIN", DefaultVal: "", Comment: ""}
			if connectionPool.Version.AtLeast("6") {
				testhelper.AssertQueryRuns(connectionPool, "CREATE COLLATION public.some_coll (lc_collate = 'POSIX', lc_ctype = 'POSIX')")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP COLLATION public.some_coll CASCADE")
				rowNonDefaultStorageAndStats.Collation = "public.some_coll"
			}
			testTable.DistPolicy = "DISTRIBUTED BY (i, j)"
			testTable.ColumnDefs = []backup.ColumnDefinition{rowOneDefault, rowNotNullDefault, rowNonDefaultStorageAndStats}

			backup.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)
			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			testTable.Oid = testutils.OidFromObjectName(connectionPool, "public", "testtable", backup.TYPE_RELATION)
			resultTable := backup.ConstructDefinitionsForTables(connectionPool, []backup.Relation{testTable.Relation})[0]
			structmatcher.ExpectStructsToMatchExcluding(testTable.TableDefinition, resultTable.TableDefinition, "ColumnDefs.Oid", "ExtTableDef")
		})
		It("creates a basic append-optimized column-oriented table", func() {
			rowOne := backup.ColumnDefinition{Oid: 0, Num: 1, Name: "i", NotNull: false, HasDefault: false, Type: "integer", Encoding: "compresstype=zlib,blocksize=32768,compresslevel=1", StatTarget: -1, StorageType: "", DefaultVal: "", Comment: ""}
			rowTwo := backup.ColumnDefinition{Oid: 0, Num: 2, Name: "j", NotNull: false, HasDefault: false, Type: "character varying(20)", Encoding: "compresstype=zlib,blocksize=32768,compresslevel=1", StatTarget: -1, StorageType: "", DefaultVal: "", Comment: ""}
			testTable.StorageOpts = "appendonly=true, orientation=column, fillfactor=42, compresstype=zlib, blocksize=32768, compresslevel=1"
			testTable.ColumnDefs = []backup.ColumnDefinition{rowOne, rowTwo}

			backup.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			testTable.Oid = testutils.OidFromObjectName(connectionPool, "public", "testtable", backup.TYPE_RELATION)
			resultTable := backup.ConstructDefinitionsForTables(connectionPool, []backup.Relation{testTable.Relation})[0]
			structmatcher.ExpectStructsToMatchExcluding(testTable.TableDefinition, resultTable.TableDefinition, "ColumnDefs.Oid", "ExtTableDef")
		})
		It("creates a one-level partition table", func() {
			rowOne := backup.ColumnDefinition{Oid: 0, Num: 1, Name: "region", NotNull: false, HasDefault: false, Type: "text", Encoding: "", StatTarget: -1, StorageType: "", DefaultVal: "", Comment: ""}
			rowTwo := backup.ColumnDefinition{Oid: 0, Num: 2, Name: "gender", NotNull: false, HasDefault: false, Type: "text", Encoding: "", StatTarget: -1, StorageType: "", DefaultVal: "", Comment: ""}
			testTable.PartDef = fmt.Sprintf(`PARTITION BY LIST(gender) `+`
          (
          PARTITION girls VALUES('F') WITH (tablename='public.rank_1_prt_girls', appendonly=%[1]s ), `+`
          PARTITION boys VALUES('M') WITH (tablename='public.rank_1_prt_boys', appendonly=%[1]s ), `+`
          DEFAULT PARTITION other  WITH (tablename='public.rank_1_prt_other', appendonly=%[1]s )
          )`, partitionPartFalseExpectation)

			testTable.ColumnDefs = []backup.ColumnDefinition{rowOne, rowTwo}
			testTable.PartitionLevelInfo.Level = "p"

			backup.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			testTable.PartitionLevelInfo.Oid = testutils.OidFromObjectName(connectionPool, "public", "testtable", backup.TYPE_RELATION)
			testTable.Oid = testutils.OidFromObjectName(connectionPool, "public", "testtable", backup.TYPE_RELATION)
			resultTable := backup.ConstructDefinitionsForTables(connectionPool, []backup.Relation{testTable.Relation})[0]
			structmatcher.ExpectStructsToMatchExcluding(testTable.TableDefinition, resultTable.TableDefinition, "ColumnDefs.Oid", "ExtTableDef")
		})
		It("creates a two-level partition table", func() {
			/*
			 * The spacing is very specific here and is output from the postgres function
			 * The only difference between the below statements is spacing
			 */
			subpartitionDef := ""
			partTemplateDef := ""
			if connectionPool.Version.Before("6") {
				subpartitionDef = `PARTITION BY LIST(gender)
          SUBPARTITION BY LIST(region) ` + `
          (
          PARTITION girls VALUES('F') WITH (tablename='public.rank_1_prt_girls', appendonly=false ) ` + `
                  (
                  SUBPARTITION usa VALUES('usa') WITH (tablename='public.rank_1_prt_girls_2_prt_usa', appendonly=false ), ` + `
                  SUBPARTITION asia VALUES('asia') WITH (tablename='public.rank_1_prt_girls_2_prt_asia', appendonly=false ), ` + `
                  SUBPARTITION europe VALUES('europe') WITH (tablename='public.rank_1_prt_girls_2_prt_europe', appendonly=false ), ` + `
                  DEFAULT SUBPARTITION other_regions  WITH (tablename='public.rank_1_prt_girls_2_prt_other_regions', appendonly=false )
                  ), ` + `
          PARTITION boys VALUES('M') WITH (tablename='rank_1_prt_boys', appendonly=false ) ` + `
                  (
                  SUBPARTITION usa VALUES('usa') WITH (tablename='public.rank_1_prt_boys_2_prt_usa', appendonly=false ), ` + `
                  SUBPARTITION asia VALUES('asia') WITH (tablename='public.rank_1_prt_boys_2_prt_asia', appendonly=false ), ` + `
                  SUBPARTITION europe VALUES('europe') WITH (tablename='public.rank_1_prt_boys_2_prt_europe', appendonly=false ), ` + `
                  DEFAULT SUBPARTITION other_regions  WITH (tablename='public.rank_1_prt_boys_2_prt_other_regions', appendonly=false )
                  ), ` + `
          DEFAULT PARTITION other  WITH (tablename='public.rank_1_prt_other', appendonly=false ) ` + `
                  (
                  SUBPARTITION usa VALUES('usa') WITH (tablename='public.rank_1_prt_other_2_prt_usa', appendonly=false ), ` + `
                  SUBPARTITION asia VALUES('asia') WITH (tablename='public.rank_1_prt_other_2_prt_asia', appendonly=false ), ` + `
                  SUBPARTITION europe VALUES('europe') WITH (tablename='public.rank_1_prt_other_2_prt_europe', appendonly=false ), ` + `
                  DEFAULT SUBPARTITION other_regions  WITH (tablename='public.rank_1_prt_other_2_prt_other_regions', appendonly=false )
                  )
          )`
				partTemplateDef = `ALTER TABLE public.testtable ` + `
SET SUBPARTITION TEMPLATE  ` + `
          (
          SUBPARTITION usa VALUES('usa') WITH (tablename='testtable'), ` + `
          SUBPARTITION asia VALUES('asia') WITH (tablename='testtable'), ` + `
          SUBPARTITION europe VALUES('europe') WITH (tablename='testtable'), ` + `
          DEFAULT SUBPARTITION other_regions  WITH (tablename='testtable')
          )
`
			} else {
				subpartitionDef = fmt.Sprintf(`PARTITION BY LIST(gender)
          SUBPARTITION BY LIST(region) `+`
          (
          PARTITION girls VALUES('F') WITH (tablename='public.rank_1_prt_girls', appendonly=%[1]s )`+`
                  (
                  SUBPARTITION usa VALUES('usa') WITH (tablename='public.rank_1_prt_girls_2_prt_usa', appendonly=%[1]s ), `+`
                  SUBPARTITION asia VALUES('asia') WITH (tablename='public.rank_1_prt_girls_2_prt_asia', appendonly=%[1]s ), `+`
                  SUBPARTITION europe VALUES('europe') WITH (tablename='public.rank_1_prt_girls_2_prt_europe', appendonly=%[1]s ), `+`
                  DEFAULT SUBPARTITION other_regions  WITH (tablename='public.rank_1_prt_girls_2_prt_other_regions', appendonly=%[1]s )
                  ), `+`
          PARTITION boys VALUES('M') WITH (tablename='rank_1_prt_boys', appendonly=%[1]s )`+`
                  (
                  SUBPARTITION usa VALUES('usa') WITH (tablename='public.rank_1_prt_boys_2_prt_usa', appendonly=%[1]s ), `+`
                  SUBPARTITION asia VALUES('asia') WITH (tablename='public.rank_1_prt_boys_2_prt_asia', appendonly=%[1]s ), `+`
                  SUBPARTITION europe VALUES('europe') WITH (tablename='public.rank_1_prt_boys_2_prt_europe', appendonly=%[1]s ), `+`
                  DEFAULT SUBPARTITION other_regions  WITH (tablename='public.rank_1_prt_boys_2_prt_other_regions', appendonly=%[1]s )
                  ), `+`
          DEFAULT PARTITION other  WITH (tablename='public.rank_1_prt_other', appendonly=%[1]s )`+`
                  (
                  SUBPARTITION usa VALUES('usa') WITH (tablename='public.rank_1_prt_other_2_prt_usa', appendonly=%[1]s ), `+`
                  SUBPARTITION asia VALUES('asia') WITH (tablename='public.rank_1_prt_other_2_prt_asia', appendonly=%[1]s ), `+`
                  SUBPARTITION europe VALUES('europe') WITH (tablename='public.rank_1_prt_other_2_prt_europe', appendonly=%[1]s ), `+`
                  DEFAULT SUBPARTITION other_regions  WITH (tablename='public.rank_1_prt_other_2_prt_other_regions', appendonly=%[1]s )
                  )
          )`, partitionPartFalseExpectation)
				partTemplateDef = `ALTER TABLE public.testtable ` + `
SET SUBPARTITION TEMPLATE ` + `
          (
          SUBPARTITION usa VALUES('usa') WITH (tablename='testtable'), ` + `
          SUBPARTITION asia VALUES('asia') WITH (tablename='testtable'), ` + `
          SUBPARTITION europe VALUES('europe') WITH (tablename='testtable'), ` + `
          DEFAULT SUBPARTITION other_regions  WITH (tablename='testtable')
          )
`
			}
			rowOne := backup.ColumnDefinition{Oid: 0, Num: 1, Name: "region", NotNull: false, HasDefault: false, Type: "text", Encoding: "", StatTarget: -1, StorageType: "", DefaultVal: "", Comment: ""}
			rowTwo := backup.ColumnDefinition{Oid: 0, Num: 2, Name: "gender", NotNull: false, HasDefault: false, Type: "text", Encoding: "", StatTarget: -1, StorageType: "", DefaultVal: "", Comment: ""}
			testTable.PartDef = subpartitionDef
			testTable.PartTemplateDef = partTemplateDef
			testTable.ColumnDefs = []backup.ColumnDefinition{rowOne, rowTwo}
			testTable.PartitionLevelInfo.Level = "p"

			backup.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			testTable.PartitionLevelInfo.Oid = testutils.OidFromObjectName(connectionPool, "public", "testtable", backup.TYPE_RELATION)
			testTable.Oid = testutils.OidFromObjectName(connectionPool, "public", "testtable", backup.TYPE_RELATION)
			resultTable := backup.ConstructDefinitionsForTables(connectionPool, []backup.Relation{testTable.Relation})[0]
			structmatcher.ExpectStructsToMatchExcluding(testTable.TableDefinition, resultTable.TableDefinition, "ColumnDefs.Oid", "ExtTableDef")

		})
		It("creates a table with a non-default tablespace", func() {
			if connectionPool.Version.Before("6") {
				testhelper.AssertQueryRuns(connectionPool, "CREATE TABLESPACE test_tablespace FILESPACE test_dir")
			} else {
				testhelper.AssertQueryRuns(connectionPool, "CREATE TABLESPACE test_tablespace LOCATION '/tmp/test_dir'")
			}
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLESPACE test_tablespace")
			testTable.TablespaceName = "test_tablespace"

			backup.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.testtable")

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			testTable.Oid = testutils.OidFromObjectName(connectionPool, "public", "testtable", backup.TYPE_RELATION)
			resultTable := backup.ConstructDefinitionsForTables(connectionPool, []backup.Relation{testTable.Relation})[0]
			structmatcher.ExpectStructsToMatchExcluding(testTable.TableDefinition, resultTable.TableDefinition, "ColumnDefs.Oid", "ExtTableDef")

		})
		It("creates a table that inherits from one table", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.parent (i int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.parent")
			testTable.ColumnDefs = []backup.ColumnDefinition{}
			testTable.Inherits = []string{"public.parent"}

			backup.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.testtable")

			testTable.Oid = testutils.OidFromObjectName(connectionPool, "public", "testtable", backup.TYPE_RELATION)
			backup.ConstructDefinitionsForTables(connectionPool, []backup.Relation{testTable.Relation})

			Expect(testTable.Inherits).To(ConsistOf("public.parent"))
		})
		It("creates a table that inherits from two tables", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.parent_one (i int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.parent_one")
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.parent_two (j character varying(20))")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.parent_two")
			testTable.ColumnDefs = []backup.ColumnDefinition{}
			testTable.Inherits = []string{"public.parent_one", "public.parent_two"}

			backup.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.testtable")

			testTable.Oid = testutils.OidFromObjectName(connectionPool, "public", "testtable", backup.TYPE_RELATION)
			backup.ConstructDefinitionsForTables(connectionPool, []backup.Relation{testTable.Relation})

			Expect(testTable.Inherits).To(Equal([]string{"public.parent_one", "public.parent_two"}))
		})
		It("creates an unlogged table", func() {
			testutils.SkipIfBefore6(connectionPool)
			rowOne := backup.ColumnDefinition{Oid: 0, Num: 1, Name: "i", NotNull: false, HasDefault: false, Type: "integer", Encoding: "", StatTarget: -1, StorageType: "", DefaultVal: "", Comment: ""}
			rowTwo := backup.ColumnDefinition{Oid: 0, Num: 2, Name: "j", NotNull: false, HasDefault: false, Type: "character varying(20)", Encoding: "", StatTarget: -1, StorageType: "", DefaultVal: "", Comment: ""}
			testTable.ColumnDefs = []backup.ColumnDefinition{rowOne, rowTwo}
			testTable.IsUnlogged = true

			backup.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			testTable.Oid = testutils.OidFromObjectName(connectionPool, "public", "testtable", backup.TYPE_RELATION)
			resultTable := backup.ConstructDefinitionsForTables(connectionPool, []backup.Relation{testTable.Relation})[0]
			structmatcher.ExpectStructsToMatchExcluding(testTable.TableDefinition, resultTable.TableDefinition, "ColumnDefs.Oid", "ExtTableDef")

		})
		It("creates a foreign table", func() {
			testutils.SkipIfBefore6(connectionPool)
			testhelper.AssertQueryRuns(connectionPool, "CREATE FOREIGN DATA WRAPPER dummy;")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP FOREIGN DATA WRAPPER dummy")
			testhelper.AssertQueryRuns(connectionPool, "CREATE SERVER sc FOREIGN DATA WRAPPER dummy;")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SERVER sc")

			testTable.TableDefinition = backup.TableDefinition{DistPolicy: "", ExtTableDef: extTableEmpty, Inherits: []string{}}
			rowOne := backup.ColumnDefinition{Oid: 0, Num: 1, Name: "i", NotNull: false, HasDefault: false, Type: "integer", Encoding: "", StatTarget: -1, StorageType: "", DefaultVal: "", Comment: "", FdwOptions: "option1 'value1', option2 'value2'"}
			testTable.ColumnDefs = []backup.ColumnDefinition{rowOne}
			testTable.ForeignDef = backup.ForeignTableDefinition{Oid: 0, Options: "", Server: "sc"}
			backup.PrintRegularTableCreateStatement(backupfile, tocfile, testTable)

			metadata := testutils.DefaultMetadata("TABLE", true, true, true, true)
			backup.PrintPostCreateTableStatements(backupfile, tocfile, testTable, metadata)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP FOREIGN TABLE public.testtable")

			testTable.Oid = testutils.OidFromObjectName(connectionPool, "public", "testtable", backup.TYPE_RELATION)
			testTable.ForeignDef.Oid = testTable.Oid
			resultTable := backup.ConstructDefinitionsForTables(connectionPool, []backup.Relation{testTable.Relation})[0]
			structmatcher.ExpectStructsToMatchExcluding(testTable.TableDefinition, resultTable.TableDefinition, "ColumnDefs.Oid", "ExtTableDef")
		})
	})
	Describe("PrintPostCreateTableStatements", func() {
		var (
			extTableEmpty = backup.ExternalTableDefinition{Oid: 0, Type: -2, Protocol: -2, Location: "", ExecLocation: "ALL_SEGMENTS", FormatType: "t", FormatOpts: "", Options: "", Command: "", RejectLimit: 0, RejectLimitType: "", ErrTableName: "", ErrTableSchema: "", Encoding: "UTF-8", Writable: false, URIs: nil}
			tableRow      = backup.ColumnDefinition{Oid: 0, Num: 1, Name: "i", NotNull: false, HasDefault: false, Type: "integer", Encoding: "", StatTarget: -1, StorageType: "", DefaultVal: "", Comment: ""}
			testTable     backup.Table
			tableMetadata backup.ObjectMetadata
		)
		BeforeEach(func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.testtable(i int)")
			tableMetadata = backup.ObjectMetadata{Privileges: []backup.ACL{}, ObjectType: "RELATION"}
			testTable = backup.Table{
				Relation:        backup.Relation{Schema: "public", Name: "testtable"},
				TableDefinition: backup.TableDefinition{DistPolicy: "DISTRIBUTED BY (i)", ColumnDefs: []backup.ColumnDefinition{tableRow}, ExtTableDef: extTableEmpty, Inherits: []string{}},
			}
			if connectionPool.Version.AtLeast("6") {
				testTable.ReplicaIdentity = "d"
			}
		})
		AfterEach(func() {
			testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.testtable")
		})
		It("prints only owner for a table with no comment or column comments", func() {
			tableMetadata.Owner = "testrole"
			backup.PrintPostCreateTableStatements(backupfile, tocfile, testTable, tableMetadata)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			testTableUniqueID := testutils.UniqueIDFromObjectName(connectionPool, "public", "testtable", backup.TYPE_RELATION)
			testTable.Oid = testTableUniqueID.Oid

			resultMetadata := backup.GetMetadataForObjectType(connectionPool, backup.TYPE_RELATION)
			resultTableMetadata := resultMetadata[testTableUniqueID]

			structmatcher.ExpectStructsToMatch(&tableMetadata, &resultTableMetadata)
			resultTable := backup.ConstructDefinitionsForTables(connectionPool, []backup.Relation{testTable.Relation})[0]
			structmatcher.ExpectStructsToMatchExcluding(&testTable.TableDefinition, &resultTable.TableDefinition, "ColumnDefs.Oid", "ColumnDefs.ACL", "ExtTableDef")
		})
		It("prints table comment, table privileges, table owner, table security label, and column comments for a table", func() {
			tableMetadata = testutils.DefaultMetadata("TABLE", true, true, true, includeSecurityLabels)
			testTable.ColumnDefs[0].Comment = "This is a column comment."
			backup.PrintPostCreateTableStatements(backupfile, tocfile, testTable, tableMetadata)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			testTableUniqueID := testutils.UniqueIDFromObjectName(connectionPool, "public", "testtable", backup.TYPE_RELATION)
			testTable.Oid = testutils.OidFromObjectName(connectionPool, "public", "testtable", backup.TYPE_RELATION)
			resultTable := backup.ConstructDefinitionsForTables(connectionPool, []backup.Relation{testTable.Relation})[0]
			structmatcher.ExpectStructsToMatchExcluding(&testTable.TableDefinition, &resultTable.TableDefinition, "ColumnDefs.Oid", "ExtTableDef")

			resultMetadata := backup.GetMetadataForObjectType(connectionPool, backup.TYPE_RELATION)
			resultTableMetadata := resultMetadata[testTableUniqueID]
			structmatcher.ExpectStructsToMatch(&tableMetadata, &resultTableMetadata)
		})
		It("prints column level privileges", func() {
			testutils.SkipIfBefore6(connectionPool)
			privilegesColumnOne := backup.ColumnDefinition{Oid: 0, Num: 1, Name: "i", Type: "integer", StatTarget: -1, Privileges: sql.NullString{String: "testrole=r/testrole", Valid: true}}
			tableMetadata.Owner = "testrole"
			testTable.ColumnDefs = []backup.ColumnDefinition{privilegesColumnOne}
			backup.PrintPostCreateTableStatements(backupfile, tocfile, testTable, tableMetadata)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			testTable.Oid = testutils.OidFromObjectName(connectionPool, "public", "testtable", backup.TYPE_RELATION)
			resultTable := backup.ConstructDefinitionsForTables(connectionPool, []backup.Relation{testTable.Relation})[0]
			resultColumnOne := resultTable.ColumnDefs[0]
			structmatcher.ExpectStructsToMatchExcluding(privilegesColumnOne, resultColumnOne, "Oid")
		})
		It("prints column level security label", func() {
			testutils.SkipIfBefore6(connectionPool)
			securityLabelColumnOne := backup.ColumnDefinition{Oid: 0, Num: 1, Name: "i", Type: "integer", StatTarget: -1, SecurityLabelProvider: "dummy", SecurityLabel: "unclassified"}
			testTable.ColumnDefs = []backup.ColumnDefinition{securityLabelColumnOne}
			backup.PrintPostCreateTableStatements(backupfile, tocfile, testTable, tableMetadata)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			testTable.Oid = testutils.OidFromObjectName(connectionPool, "public", "testtable", backup.TYPE_RELATION)
			resultTable := backup.ConstructDefinitionsForTables(connectionPool, []backup.Relation{testTable.Relation})[0]
			resultColumnOne := resultTable.ColumnDefs[0]
			structmatcher.ExpectStructsToMatchExcluding(securityLabelColumnOne, resultColumnOne, "Oid")
		})
		It("prints table replica identity value", func() {
			testutils.SkipIfBefore6(connectionPool)

			testTable.ReplicaIdentity = "f"
			backup.PrintPostCreateTableStatements(backupfile, tocfile, testTable, tableMetadata)
			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			testTable.Oid = testutils.OidFromObjectName(connectionPool, "public", "testtable", backup.TYPE_RELATION)
			resultTable := backup.ConstructDefinitionsForTables(connectionPool, []backup.Relation{testTable.Relation})[0]
			Expect(resultTable.ReplicaIdentity).To(Equal("f"))
		})
	})
	Describe("PrintCreateViewStatements", func() {
		var viewDef string
		BeforeEach(func() {
			if connectionPool.Version.Before("6") {
				viewDef = "SELECT 1;"
			} else {
				viewDef = " SELECT 1;"
			}
		})
		It("creates a view with privileges, owner, security label, and comment", func() {
			view := backup.View{Oid: 1, Schema: "public", Name: "simpleview", Definition: viewDef}
			viewMetadata := testutils.DefaultMetadata("VIEW", true, true, true, includeSecurityLabels)

			backup.PrintCreateViewStatement(backupfile, tocfile, view, viewMetadata)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP VIEW public.simpleview")

			resultViews, _ := backup.GetAllViews(connectionPool)
			resultMetadataMap := backup.GetMetadataForObjectType(connectionPool, backup.TYPE_RELATION)

			view.Oid = testutils.OidFromObjectName(connectionPool, "public", "simpleview", backup.TYPE_RELATION)
			Expect(resultViews).To(HaveLen(1))
			resultMetadata := resultMetadataMap[view.GetUniqueID()]
			structmatcher.ExpectStructsToMatch(&view, &resultViews[0])
			structmatcher.ExpectStructsToMatch(&viewMetadata, &resultMetadata)
		})
		It("creates a view with options", func() {
			testutils.SkipIfBefore6(connectionPool)
			view := backup.View{Oid: 1, Schema: "public", Name: "simpleview", Options: " WITH (security_barrier=true)", Definition: viewDef}

			backup.PrintCreateViewStatement(backupfile, tocfile, view, backup.ObjectMetadata{})

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP VIEW public.simpleview")

			resultViews, _ := backup.GetAllViews(connectionPool)

			view.Oid = testutils.OidFromObjectName(connectionPool, "public", "simpleview", backup.TYPE_RELATION)
			Expect(resultViews).To(HaveLen(1))
			structmatcher.ExpectStructsToMatch(&view, &resultViews[0])
		})
	})
	Describe("PrintMaterializedCreateViewStatements", func() {
		BeforeEach(func() {
			if connectionPool.Version.Before("6.2") {
				Skip("test only applicable to GPDB 6.2 and above")
			}
		})
		It("creates a view with privileges, owner, security label, and comment", func() {
			view := backup.MaterializedView{Oid: 1, Schema: "public", Name: "simplemview", Definition: " SELECT 1;"}
			viewMetadata := testutils.DefaultMetadata("MATERIALIZED VIEW", true, true, true, includeSecurityLabels)

			backup.PrintCreateMaterializedViewStatement(backupfile, tocfile, view, viewMetadata)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP MATERIALIZED VIEW public.simplemview")

			_, resultViews := backup.GetAllViews(connectionPool)
			resultMetadataMap := backup.GetMetadataForObjectType(connectionPool, backup.TYPE_RELATION)

			view.Oid = testutils.OidFromObjectName(connectionPool, "public", "simplemview", backup.TYPE_RELATION)
			Expect(resultViews).To(HaveLen(1))
			resultMetadata := resultMetadataMap[view.GetUniqueID()]
			structmatcher.ExpectStructsToMatch(&view, &resultViews[0])
			structmatcher.ExpectStructsToMatch(&viewMetadata, &resultMetadata)
		})
		It("creates a materialized view with options", func() {
			view := backup.MaterializedView{Oid: 1, Schema: "public", Name: "simplemview", Options: " WITH (fillfactor=10)", Definition: " SELECT 1;"}

			backup.PrintCreateMaterializedViewStatement(backupfile, tocfile, view, backup.ObjectMetadata{})

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP MATERIALIZED VIEW public.simplemview")

			_, resultViews := backup.GetAllViews(connectionPool)

			view.Oid = testutils.OidFromObjectName(connectionPool, "public", "simplemview", backup.TYPE_RELATION)
			Expect(resultViews).To(HaveLen(1))
			structmatcher.ExpectStructsToMatch(&view, &resultViews[0])
		})
	})
	Describe("PrintCreateSequenceStatements", func() {
		var (
			sequence            backup.Relation
			sequenceDef         backup.Sequence
			sequenceMetadataMap backup.MetadataMap
		)
		BeforeEach(func() {
			sequence = backup.Relation{SchemaOid: 0, Oid: 1, Schema: "public", Name: "my_sequence"}
			sequenceDef = backup.Sequence{Relation: sequence}
			sequenceMetadataMap = backup.MetadataMap{}
		})
		It("creates a basic sequence", func() {
			startValue := int64(0)
			if connectionPool.Version.AtLeast("6") {
				startValue = 1
			}
			sequenceDef.SequenceDefinition = backup.SequenceDefinition{LastVal: 1, Increment: 1, MaxVal: math.MaxInt64, MinVal: 1, CacheVal: 1, StartVal: startValue}
			backup.PrintCreateSequenceStatements(backupfile, tocfile, []backup.Sequence{sequenceDef}, sequenceMetadataMap)
			if connectionPool.Version.Before("5") {
				sequenceDef.LogCnt = 1 // In GPDB 4.3, sequence log count is one-indexed
			}

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SEQUENCE public.my_sequence")

			resultSequences := backup.GetAllSequences(connectionPool, map[string]string{})

			Expect(resultSequences).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&sequence, &resultSequences[0].Relation, "SchemaOid", "Oid")
			structmatcher.ExpectStructsToMatch(&sequenceDef.SequenceDefinition, &resultSequences[0].SequenceDefinition)
		})
		It("creates a complex sequence", func() {
			startValue := int64(0)
			if connectionPool.Version.AtLeast("6") {
				startValue = 105
			}
			sequenceDef.SequenceDefinition = backup.SequenceDefinition{LastVal: 105, Increment: 5, MaxVal: 1000, MinVal: 20, CacheVal: 1, LogCnt: 0, IsCycled: false, IsCalled: true, StartVal: startValue}
			backup.PrintCreateSequenceStatements(backupfile, tocfile, []backup.Sequence{sequenceDef}, sequenceMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SEQUENCE public.my_sequence")

			resultSequences := backup.GetAllSequences(connectionPool, map[string]string{})

			Expect(resultSequences).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&sequence, &resultSequences[0].Relation, "SchemaOid", "Oid")
			structmatcher.ExpectStructsToMatch(&sequenceDef.SequenceDefinition, &resultSequences[0].SequenceDefinition)
		})
		It("creates a sequence with privileges, owner, and comment", func() {
			startValue := int64(0)
			if connectionPool.Version.AtLeast("6") {
				startValue = 1
			}
			sequenceDef.SequenceDefinition = backup.SequenceDefinition{LastVal: 1, Increment: 1, MaxVal: math.MaxInt64, MinVal: 1, CacheVal: 1, StartVal: startValue}
			sequenceMetadata := testutils.DefaultMetadata("SEQUENCE", true, true, true, includeSecurityLabels)
			sequenceMetadataMap[backup.UniqueID{ClassID: backup.PG_CLASS_OID, Oid: 1}] = sequenceMetadata
			backup.PrintCreateSequenceStatements(backupfile, tocfile, []backup.Sequence{sequenceDef}, sequenceMetadataMap)
			if connectionPool.Version.Before("5") {
				sequenceDef.LogCnt = 1 // In GPDB 4.3, sequence log count is one-indexed
			}

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SEQUENCE public.my_sequence")

			resultSequences := backup.GetAllSequences(connectionPool, map[string]string{})

			Expect(resultSequences).To(HaveLen(1))
			resultMetadataMap := backup.GetMetadataForObjectType(connectionPool, backup.TYPE_RELATION)
			uniqueID := testutils.UniqueIDFromObjectName(connectionPool, "public", "my_sequence", backup.TYPE_RELATION)
			resultMetadata := resultMetadataMap[uniqueID]
			structmatcher.ExpectStructsToMatchExcluding(&sequence, &resultSequences[0].Relation, "SchemaOid", "Oid")
			structmatcher.ExpectStructsToMatch(&sequenceDef.SequenceDefinition, &resultSequences[0].SequenceDefinition)
			structmatcher.ExpectStructsToMatch(&sequenceMetadata, &resultMetadata)
		})
	})
	Describe("PrintAlterSequenceStatements", func() {
		It("creates a sequence owned by a table column", func() {
			startValue := int64(0)
			if connectionPool.Version.AtLeast("6") {
				startValue = 1
			}
			sequenceDef := backup.Sequence{Relation: backup.Relation{SchemaOid: 0, Oid: 1, Schema: "public", Name: "my_sequence"}}
			columnOwnerMap := map[string]string{"public.my_sequence": "public.sequence_table.a"}

			sequenceDef.SequenceDefinition = backup.SequenceDefinition{LastVal: 1, Increment: 1, MaxVal: math.MaxInt64, MinVal: 1, CacheVal: 1, StartVal: startValue}
			backup.PrintCreateSequenceStatements(backupfile, tocfile, []backup.Sequence{sequenceDef}, backup.MetadataMap{})
			backup.PrintAlterSequenceStatements(backupfile, tocfile, []backup.Sequence{sequenceDef}, columnOwnerMap)

			//Create table that sequence can be owned by
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.sequence_table(a int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.sequence_table")

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SEQUENCE public.my_sequence")

			sequenceOwnerTables, sequenceOwnerColumns := backup.GetSequenceColumnOwnerMap(connectionPool)
			Expect(sequenceOwnerTables).To(HaveLen(1))
			Expect(sequenceOwnerColumns).To(HaveLen(1))
			Expect(sequenceOwnerTables["public.my_sequence"]).To(Equal("public.sequence_table"))
			Expect(sequenceOwnerColumns["public.my_sequence"]).To(Equal("public.sequence_table.a"))
		})
	})
})
