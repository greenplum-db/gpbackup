package integration

import (
	"github.com/greenplum-db/gp-common-go-libs/structmatcher"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"math"
)

var _ = Describe("backup integration create statement tests", func() {
	BeforeEach(func() {
		toc, backupfile = testutils.InitializeTestTOC(buffer, "predata")
	})
	Describe("PrintRegularTableCreateStatement", func() {
		var (
			extTableEmpty backup.ExternalTableDefinition
			testTable     backup.Relation
			tableDef      backup.TableDefinition
			/*
			 * We need to construct partitionDef and partTemplateDef piecemeal like this,
			 * or go fmt will remove the trailing whitespace and prevent literal comparison.
			 */
			partitionDef = `PARTITION BY LIST(gender) ` + `
          (
          PARTITION girls VALUES('F') WITH (tablename='public.rank_1_prt_girls', appendonly=false ), ` + `
          PARTITION boys VALUES('M') WITH (tablename='public.rank_1_prt_boys', appendonly=false ), ` + `
          DEFAULT PARTITION other  WITH (tablename='public.rank_1_prt_other', appendonly=false )
          )`
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
			emptyACL = []backup.ACL{}
		)
		BeforeEach(func() {
			extTableEmpty = backup.ExternalTableDefinition{Oid: 0, Type: -2, Protocol: -2, Location: "", ExecLocation: "ALL_SEGMENTS", FormatType: "t", FormatOpts: "", Options: "", Command: "", RejectLimit: 0, RejectLimitType: "", ErrTableName: "", ErrTableSchema: "", Encoding: "UTF-8", Writable: false, URIs: nil}
			testTable = backup.Relation{Schema: "public", Name: "testtable"}
			tableDef = backup.TableDefinition{DistPolicy: "DISTRIBUTED RANDOMLY", ExtTableDef: extTableEmpty}
		})
		AfterEach(func() {
			testTable.Inherits = []string{}
			testhelper.AssertQueryRuns(connection, "DROP TABLE IF EXISTS public.testtable")
		})
		It("creates a table with no attributes", func() {
			backup.PrintRegularTableCreateStatement(backupfile, toc, testTable, tableDef)

			testhelper.AssertQueryRuns(connection, buffer.String())
			testTable.Oid = testutils.OidFromObjectName(connection, "public", "testtable", backup.TYPE_RELATION)
			resultTableDef := backup.ConstructDefinitionsForTables(connection, []backup.Relation{testTable})[testTable.Oid]
			structmatcher.ExpectStructsToMatchExcluding(&tableDef, &resultTableDef, "ColumnDefs.Oid", "ExtTableDef")
		})
		It("creates a table of a type", func() {
			testutils.SkipIfBefore6(connection)
			testhelper.AssertQueryRuns(connection, `CREATE TYPE public.some_type AS (i text, j numeric)`)
			defer testhelper.AssertQueryRuns(connection, `DROP TYPE public.some_type CASCADE`)

			rowOne := backup.ColumnDefinition{Oid: 0, Num: 1, Name: "i", NotNull: false, HasDefault: false, Type: "text", Encoding: "", StatTarget: -1, StorageType: "", DefaultVal: "", Comment: "", ACL: emptyACL}
			rowTwo := backup.ColumnDefinition{Oid: 0, Num: 2, Name: "j", NotNull: false, HasDefault: false, Type: "numeric", Encoding: "", StatTarget: -1, StorageType: "", DefaultVal: "", Comment: "", ACL: emptyACL}
			tableDef.ColumnDefs = []backup.ColumnDefinition{rowOne, rowTwo}

			tableDef.TableType = "public.some_type"
			backup.PrintRegularTableCreateStatement(backupfile, toc, testTable, tableDef)
			testhelper.AssertQueryRuns(connection, buffer.String())

			testTable.Oid = testutils.OidFromObjectName(connection, "public", "testtable", backup.TYPE_RELATION)
			resultTableDef := backup.ConstructDefinitionsForTables(connection, []backup.Relation{testTable})[testTable.Oid]

			structmatcher.ExpectStructsToMatchExcluding(&tableDef, &resultTableDef, "ColumnDefs.Oid", "ExtTableDef")
		})

		It("creates a basic heap table", func() {
			rowOne := backup.ColumnDefinition{Oid: 0, Num: 1, Name: "i", NotNull: false, HasDefault: false, Type: "integer", Encoding: "", StatTarget: -1, StorageType: "", DefaultVal: "", Comment: "", ACL: emptyACL}
			rowTwo := backup.ColumnDefinition{Oid: 0, Num: 2, Name: "j", NotNull: false, HasDefault: false, Type: "character varying(20)", Encoding: "", StatTarget: -1, StorageType: "", DefaultVal: "", Comment: "", ACL: emptyACL}
			tableDef.ColumnDefs = []backup.ColumnDefinition{rowOne, rowTwo}

			backup.PrintRegularTableCreateStatement(backupfile, toc, testTable, tableDef)

			testhelper.AssertQueryRuns(connection, buffer.String())
			testTable.Oid = testutils.OidFromObjectName(connection, "public", "testtable", backup.TYPE_RELATION)
			resultTableDef := backup.ConstructDefinitionsForTables(connection, []backup.Relation{testTable})[testTable.Oid]
			structmatcher.ExpectStructsToMatchExcluding(&tableDef, &resultTableDef, "ColumnDefs.Oid", "ExtTableDef")
		})
		It("creates a complex heap table", func() {
			rowOneDefault := backup.ColumnDefinition{Oid: 0, Num: 1, Name: "i", NotNull: false, HasDefault: true, Type: "integer", Encoding: "", StatTarget: -1, StorageType: "", DefaultVal: "42", Comment: "", ACL: emptyACL}
			rowNotNullDefault := backup.ColumnDefinition{Oid: 0, Num: 2, Name: "j", NotNull: true, HasDefault: true, Type: "character varying(20)", Encoding: "", StatTarget: -1, StorageType: "", DefaultVal: "'bar'::text", Comment: "", ACL: emptyACL}
			rowNonDefaultStorageAndStats := backup.ColumnDefinition{Oid: 0, Num: 3, Name: "k", NotNull: false, HasDefault: false, Type: "text", Encoding: "", StatTarget: 3, StorageType: "PLAIN", DefaultVal: "", Comment: "", ACL: emptyACL}
			if connection.Version.AtLeast("6") {
				testhelper.AssertQueryRuns(connection, "CREATE COLLATION public.some_coll (lc_collate = 'POSIX', lc_ctype = 'POSIX')")
				defer testhelper.AssertQueryRuns(connection, "DROP COLLATION public.some_coll CASCADE")
				rowNonDefaultStorageAndStats.Collation = "public.some_coll"
			}
			tableDef.DistPolicy = "DISTRIBUTED BY (i, j)"
			tableDef.ColumnDefs = []backup.ColumnDefinition{rowOneDefault, rowNotNullDefault, rowNonDefaultStorageAndStats}

			backup.PrintRegularTableCreateStatement(backupfile, toc, testTable, tableDef)

			testhelper.AssertQueryRuns(connection, buffer.String())
			testTable.Oid = testutils.OidFromObjectName(connection, "public", "testtable", backup.TYPE_RELATION)
			resultTableDef := backup.ConstructDefinitionsForTables(connection, []backup.Relation{testTable})[testTable.Oid]
			structmatcher.ExpectStructsToMatchExcluding(&tableDef, &resultTableDef, "ColumnDefs.Oid", "ExtTableDef")
		})
		It("creates a basic append-optimized column-oriented table", func() {
			rowOne := backup.ColumnDefinition{Oid: 0, Num: 1, Name: "i", NotNull: false, HasDefault: false, Type: "integer", Encoding: "compresstype=zlib,blocksize=32768,compresslevel=1", StatTarget: -1, StorageType: "", DefaultVal: "", Comment: "", ACL: emptyACL}
			rowTwo := backup.ColumnDefinition{Oid: 0, Num: 2, Name: "j", NotNull: false, HasDefault: false, Type: "character varying(20)", Encoding: "compresstype=zlib,blocksize=32768,compresslevel=1", StatTarget: -1, StorageType: "", DefaultVal: "", Comment: "", ACL: emptyACL}
			tableDef.StorageOpts = "appendonly=true, orientation=column, fillfactor=42, compresstype=zlib, blocksize=32768, compresslevel=1"
			tableDef.ColumnDefs = []backup.ColumnDefinition{rowOne, rowTwo}

			backup.PrintRegularTableCreateStatement(backupfile, toc, testTable, tableDef)

			testhelper.AssertQueryRuns(connection, buffer.String())
			testTable.Oid = testutils.OidFromObjectName(connection, "public", "testtable", backup.TYPE_RELATION)
			resultTableDef := backup.ConstructDefinitionsForTables(connection, []backup.Relation{testTable})[testTable.Oid]
			structmatcher.ExpectStructsToMatchExcluding(&tableDef, &resultTableDef, "ColumnDefs.Oid", "ExtTableDef")
		})
		It("creates a one-level partition table", func() {
			rowOne := backup.ColumnDefinition{Oid: 0, Num: 1, Name: "region", NotNull: false, HasDefault: false, Type: "text", Encoding: "", StatTarget: -1, StorageType: "", DefaultVal: "", Comment: "", ACL: emptyACL}
			rowTwo := backup.ColumnDefinition{Oid: 0, Num: 2, Name: "gender", NotNull: false, HasDefault: false, Type: "text", Encoding: "", StatTarget: -1, StorageType: "", DefaultVal: "", Comment: "", ACL: emptyACL}
			tableDef.PartDef = partitionDef
			tableDef.ColumnDefs = []backup.ColumnDefinition{rowOne, rowTwo}
			tableDef.PartitionLevelInfo.Level = "p"

			backup.PrintRegularTableCreateStatement(backupfile, toc, testTable, tableDef)

			testhelper.AssertQueryRuns(connection, buffer.String())
			testTable.Oid = testutils.OidFromObjectName(connection, "public", "testtable", backup.TYPE_RELATION)
			tableDef.PartitionLevelInfo.Oid = testutils.OidFromObjectName(connection, "public", "testtable", backup.TYPE_RELATION)
			resultTableDef := backup.ConstructDefinitionsForTables(connection, []backup.Relation{testTable})[testTable.Oid]
			structmatcher.ExpectStructsToMatchExcluding(&tableDef, &resultTableDef, "ColumnDefs.Oid", "ExtTableDef")
		})
		It("creates a two-level partition table", func() {
			rowOne := backup.ColumnDefinition{Oid: 0, Num: 1, Name: "region", NotNull: false, HasDefault: false, Type: "text", Encoding: "", StatTarget: -1, StorageType: "", DefaultVal: "", Comment: "", ACL: emptyACL}
			rowTwo := backup.ColumnDefinition{Oid: 0, Num: 2, Name: "gender", NotNull: false, HasDefault: false, Type: "text", Encoding: "", StatTarget: -1, StorageType: "", DefaultVal: "", Comment: "", ACL: emptyACL}
			tableDef.PartDef = subpartitionDef
			tableDef.PartTemplateDef = partTemplateDef
			tableDef.ColumnDefs = []backup.ColumnDefinition{rowOne, rowTwo}
			tableDef.PartitionLevelInfo.Level = "p"

			backup.PrintRegularTableCreateStatement(backupfile, toc, testTable, tableDef)

			testhelper.AssertQueryRuns(connection, buffer.String())
			testTable.Oid = testutils.OidFromObjectName(connection, "public", "testtable", backup.TYPE_RELATION)
			tableDef.PartitionLevelInfo.Oid = testutils.OidFromObjectName(connection, "public", "testtable", backup.TYPE_RELATION)
			resultTableDef := backup.ConstructDefinitionsForTables(connection, []backup.Relation{testTable})[testTable.Oid]
			structmatcher.ExpectStructsToMatchExcluding(&tableDef, &resultTableDef, "ColumnDefs.Oid", "ExtTableDef")
		})
		It("creates a table with a non-default tablespace", func() {
			testTable = backup.Relation{Schema: "public", Name: "testtable2"}
			if connection.Version.Before("6") {
				testhelper.AssertQueryRuns(connection, "CREATE TABLESPACE test_tablespace FILESPACE test_dir")
			} else {
				testhelper.AssertQueryRuns(connection, "CREATE TABLESPACE test_tablespace LOCATION '/tmp/test_dir'")
			}
			defer testhelper.AssertQueryRuns(connection, "DROP TABLESPACE test_tablespace")
			tableDef.TablespaceName = "test_tablespace"

			backup.PrintRegularTableCreateStatement(backupfile, toc, testTable, tableDef)
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.testtable2")

			testhelper.AssertQueryRuns(connection, buffer.String())
			testTable.Oid = testutils.OidFromObjectName(connection, "public", "testtable2", backup.TYPE_RELATION)
			resultTableDef := backup.ConstructDefinitionsForTables(connection, []backup.Relation{testTable})[testTable.Oid]
			structmatcher.ExpectStructsToMatchExcluding(&tableDef, &resultTableDef, "ColumnDefs.Oid", "ExtTableDef")
		})
		It("creates a table that inherits from one table", func() {
			testhelper.AssertQueryRuns(connection, "CREATE TABLE public.parent (i int)")
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.parent")
			tableDef.ColumnDefs = []backup.ColumnDefinition{}
			testTable.Inherits = []string{"public.parent"}

			backup.PrintRegularTableCreateStatement(backupfile, toc, testTable, tableDef)

			testhelper.AssertQueryRuns(connection, buffer.String())
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.testtable")

			backup.ConstructDefinitionsForTables(connection, []backup.Relation{testTable})

			Expect(testTable.Inherits).To(ConsistOf("public.parent"))
		})
		It("creates a table that inherits from two tables", func() {
			testhelper.AssertQueryRuns(connection, "CREATE TABLE public.parent_one (i int)")
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.parent_one")
			testhelper.AssertQueryRuns(connection, "CREATE TABLE public.parent_two (j character varying(20))")
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.parent_two")
			tableDef.ColumnDefs = []backup.ColumnDefinition{}
			testTable.Inherits = []string{"public.parent_one", "public.parent_two"}

			backup.PrintRegularTableCreateStatement(backupfile, toc, testTable, tableDef)

			testhelper.AssertQueryRuns(connection, buffer.String())
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.testtable")

			backup.ConstructDefinitionsForTables(connection, []backup.Relation{testTable})

			Expect(testTable.Inherits).To(Equal([]string{"public.parent_one", "public.parent_two"}))
		})
		It("creates an unlogged table", func() {
			testutils.SkipIfBefore6(connection)
			rowOne := backup.ColumnDefinition{Oid: 0, Num: 1, Name: "i", NotNull: false, HasDefault: false, Type: "integer", Encoding: "", StatTarget: -1, StorageType: "", DefaultVal: "", Comment: "", ACL: emptyACL}
			rowTwo := backup.ColumnDefinition{Oid: 0, Num: 2, Name: "j", NotNull: false, HasDefault: false, Type: "character varying(20)", Encoding: "", StatTarget: -1, StorageType: "", DefaultVal: "", Comment: "", ACL: emptyACL}
			tableDef.ColumnDefs = []backup.ColumnDefinition{rowOne, rowTwo}
			tableDef.IsUnlogged = true

			backup.PrintRegularTableCreateStatement(backupfile, toc, testTable, tableDef)

			testhelper.AssertQueryRuns(connection, buffer.String())
			testTable.Oid = testutils.OidFromObjectName(connection, "public", "testtable", backup.TYPE_RELATION)
			resultTableDef := backup.ConstructDefinitionsForTables(connection, []backup.Relation{testTable})[testTable.Oid]
			structmatcher.ExpectStructsToMatchExcluding(&tableDef, &resultTableDef, "ColumnDefs.Oid", "ExtTableDef")
		})
		It("creates a foreign table", func() {
			testutils.SkipIfBefore6(connection)
			testhelper.AssertQueryRuns(connection, "CREATE FOREIGN DATA WRAPPER dummy;")
			defer testhelper.AssertQueryRuns(connection, "DROP FOREIGN DATA WRAPPER dummy")
			testhelper.AssertQueryRuns(connection, "CREATE SERVER sc FOREIGN DATA WRAPPER dummy;")
			defer testhelper.AssertQueryRuns(connection, "DROP SERVER sc")

			tableDef = backup.TableDefinition{DistPolicy: "", ExtTableDef: extTableEmpty}
			rowOne := backup.ColumnDefinition{Oid: 0, Num: 1, Name: "i", NotNull: false, HasDefault: false, Type: "integer", Encoding: "", StatTarget: -1, StorageType: "", DefaultVal: "", Comment: "", ACL: emptyACL, FdwOptions: "option1 'value1', option2 'value2'"}
			tableDef.ColumnDefs = []backup.ColumnDefinition{rowOne}
			tableDef.ForeignDef = backup.ForeignTableDefinition{0, "", "sc"}
			backup.PrintRegularTableCreateStatement(backupfile, toc, testTable, tableDef)

			testhelper.AssertQueryRuns(connection, buffer.String())
			defer testhelper.AssertQueryRuns(connection, "DROP FOREIGN TABLE public.testtable")

			testTable.Oid = testutils.OidFromObjectName(connection, "public", "testtable", backup.TYPE_RELATION)
			tableDef.ForeignDef.Oid = testTable.Oid
			resultTableDef := backup.ConstructDefinitionsForTables(connection, []backup.Relation{testTable})[testTable.Oid]
			structmatcher.ExpectStructsToMatchExcluding(&tableDef, &resultTableDef, "ColumnDefs.Oid", "ExtTableDef")
		})
	})
	Describe("PrintPostCreateTableStatements", func() {
		var (
			extTableEmpty = backup.ExternalTableDefinition{Oid: 0, Type: -2, Protocol: -2, Location: "", ExecLocation: "ALL_SEGMENTS", FormatType: "t", FormatOpts: "", Options: "", Command: "", RejectLimit: 0, RejectLimitType: "", ErrTableName: "", ErrTableSchema: "", Encoding: "UTF-8", Writable: false, URIs: nil}
			testTable     = backup.Relation{Schema: "public", Name: "testtable"}
			tableRow      = backup.ColumnDefinition{Oid: 0, Num: 1, Name: "i", NotNull: false, HasDefault: false, Type: "integer", Encoding: "", StatTarget: -1, StorageType: "", DefaultVal: "", Comment: "", ACL: []backup.ACL{}}
			tableMetadata backup.ObjectMetadata
			tableDef      backup.TableDefinition
		)
		BeforeEach(func() {
			testhelper.AssertQueryRuns(connection, "CREATE TABLE public.testtable(i int)")
			tableMetadata = backup.ObjectMetadata{Privileges: []backup.ACL{}}
			tableDef = backup.TableDefinition{DistPolicy: "DISTRIBUTED BY (i)", ColumnDefs: []backup.ColumnDefinition{tableRow}, ExtTableDef: extTableEmpty}
		})
		AfterEach(func() {
			testhelper.AssertQueryRuns(connection, "DROP TABLE public.testtable")
		})
		It("prints only owner for a table with no comment or column comments", func() {
			tableMetadata.Owner = "testrole"
			backup.PrintPostCreateTableStatements(backupfile, testTable, tableDef, tableMetadata)

			testhelper.AssertQueryRuns(connection, buffer.String())
			testTable.Oid = testutils.OidFromObjectName(connection, "public", "testtable", backup.TYPE_RELATION)
			resultMetadata := backup.GetMetadataForObjectType(connection, backup.TYPE_RELATION)
			resultTableMetadata := resultMetadata[testTable.Oid]
			structmatcher.ExpectStructsToMatch(&tableMetadata, &resultTableMetadata)
			resultTableDef := backup.ConstructDefinitionsForTables(connection, []backup.Relation{testTable})[testTable.Oid]
			structmatcher.ExpectStructsToMatchExcluding(&tableDef, &resultTableDef, "ColumnDefs.Oid", "ColumnDefs.ACL", "ExtTableDef")
		})
		It("prints table comment, table owner, and column comments for a table with all three", func() {
			tableMetadata.Owner = "testrole"
			tableMetadata.Comment = "This is a table comment."
			tableDef.ColumnDefs[0].Comment = "This is a column comment."
			backup.PrintPostCreateTableStatements(backupfile, testTable, tableDef, tableMetadata)

			testhelper.AssertQueryRuns(connection, buffer.String())
			testTable.Oid = testutils.OidFromObjectName(connection, "public", "testtable", backup.TYPE_RELATION)
			resultTableDef := backup.ConstructDefinitionsForTables(connection, []backup.Relation{testTable})[testTable.Oid]
			structmatcher.ExpectStructsToMatchExcluding(&tableDef, &resultTableDef, "ColumnDefs.Oid", "ColumnDefs.ACL", "ExtTableDef")
			resultMetadata := backup.GetMetadataForObjectType(connection, backup.TYPE_RELATION)
			resultTableMetadata := resultMetadata[testTable.Oid]
			structmatcher.ExpectStructsToMatch(&tableMetadata, &resultTableMetadata)
		})
		It("prints column level privileges", func() {
			testutils.SkipIfBefore6(connection)
			privilegesColumnOne := backup.ColumnDefinition{Oid: 0, Num: 1, Name: "i", Type: "integer", StatTarget: -1, ACL: []backup.ACL{{Grantee: "testrole", Select: true}}}
			tableMetadata.Owner = "testrole"
			tableDef.ColumnDefs = []backup.ColumnDefinition{privilegesColumnOne}
			backup.PrintPostCreateTableStatements(backupfile, testTable, tableDef, tableMetadata)

			testhelper.AssertQueryRuns(connection, buffer.String())
			testTable.Oid = testutils.OidFromObjectName(connection, "public", "testtable", backup.TYPE_RELATION)
			resultTableDef := backup.ConstructDefinitionsForTables(connection, []backup.Relation{testTable})[testTable.Oid]
			resultColumnOne := resultTableDef.ColumnDefs[0]
			structmatcher.ExpectStructsToMatchExcluding(privilegesColumnOne, resultColumnOne, "Oid")
		})
	})
	Describe("PrintCreateViewStatements", func() {
		var viewDef string
		BeforeEach(func() {
			if connection.Version.Before("6") {
				viewDef = "SELECT 1;"
			} else {
				viewDef = " SELECT 1;"
			}
		})
		It("creates a view with privileges and a comment and owner", func() {
			viewDef := backup.View{Oid: 1, Schema: "public", Name: "simpleview", Definition: viewDef}
			viewMetadata := testutils.DefaultMetadataMap("VIEW", true, true, true)[1]

			backup.PrintCreateViewStatement(backupfile, toc, viewDef, viewMetadata)

			testhelper.AssertQueryRuns(connection, buffer.String())
			defer testhelper.AssertQueryRuns(connection, "DROP VIEW public.simpleview")

			resultViews := backup.GetViews(connection)
			resultMetadataMap := backup.GetMetadataForObjectType(connection, backup.TYPE_RELATION)

			viewDef.Oid = testutils.OidFromObjectName(connection, "public", "simpleview", backup.TYPE_RELATION)
			Expect(resultViews).To(HaveLen(1))
			resultMetadata := resultMetadataMap[viewDef.Oid]
			structmatcher.ExpectStructsToMatch(&viewDef, &resultViews[0])
			structmatcher.ExpectStructsToMatch(&viewMetadata, &resultMetadata)
		})
		It("creates a view with options", func() {
			testutils.SkipIfBefore6(connection)
			viewDef := backup.View{Oid: 1, Schema: "public", Name: "simpleview", Options: " WITH (security_barrier=true)", Definition: viewDef}

			backup.PrintCreateViewStatement(backupfile, toc, viewDef, backup.ObjectMetadata{})

			testhelper.AssertQueryRuns(connection, buffer.String())
			defer testhelper.AssertQueryRuns(connection, "DROP VIEW public.simpleview")

			resultViews := backup.GetViews(connection)

			viewDef.Oid = testutils.OidFromObjectName(connection, "public", "simpleview", backup.TYPE_RELATION)
			Expect(resultViews).To(HaveLen(1))
			structmatcher.ExpectStructsToMatch(&viewDef, &resultViews[0])
		})
	})
	Describe("PrintCreateSequenceStatements", func() {
		var (
			sequence            backup.Relation
			sequenceDef         backup.Sequence
			sequenceMetadataMap backup.MetadataMap
		)
		BeforeEach(func() {
			sequence = backup.Relation{SchemaOid: 0, Oid: 1, Schema: "public", Name: "my_sequence", Inherits: nil}
			sequenceDef = backup.Sequence{Relation: sequence}
			sequenceMetadataMap = backup.MetadataMap{}
		})
		It("creates a basic sequence", func() {
			startValue := int64(0)
			if connection.Version.AtLeast("6") {
				startValue = 1
			}
			sequenceDef.SequenceDefinition = backup.SequenceDefinition{Name: "my_sequence", LastVal: 1, Increment: 1, MaxVal: math.MaxInt64, MinVal: 1, CacheVal: 1, StartVal: startValue}
			backup.PrintCreateSequenceStatements(backupfile, toc, []backup.Sequence{sequenceDef}, sequenceMetadataMap)
			if connection.Version.Before("5") {
				sequenceDef.LogCnt = 1 // In GPDB 4.3, sequence log count is one-indexed
			}

			testhelper.AssertQueryRuns(connection, buffer.String())
			defer testhelper.AssertQueryRuns(connection, "DROP SEQUENCE public.my_sequence")

			resultSequences := backup.GetAllSequences(connection, map[string]string{})

			Expect(resultSequences).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&sequence, &resultSequences[0].Relation, "SchemaOid", "Oid")
			structmatcher.ExpectStructsToMatch(&sequenceDef.SequenceDefinition, &resultSequences[0].SequenceDefinition)
		})
		It("creates a complex sequence", func() {
			startValue := int64(0)
			if connection.Version.AtLeast("6") {
				startValue = 105
			}
			sequenceDef.SequenceDefinition = backup.SequenceDefinition{Name: "my_sequence", LastVal: 105, Increment: 5, MaxVal: 1000, MinVal: 20, CacheVal: 1, LogCnt: 0, IsCycled: false, IsCalled: true, StartVal: startValue}
			backup.PrintCreateSequenceStatements(backupfile, toc, []backup.Sequence{sequenceDef}, sequenceMetadataMap)

			testhelper.AssertQueryRuns(connection, buffer.String())
			defer testhelper.AssertQueryRuns(connection, "DROP SEQUENCE public.my_sequence")

			resultSequences := backup.GetAllSequences(connection, map[string]string{})

			Expect(resultSequences).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&sequence, &resultSequences[0].Relation, "SchemaOid", "Oid")
			structmatcher.ExpectStructsToMatch(&sequenceDef.SequenceDefinition, &resultSequences[0].SequenceDefinition)
		})
		It("creates a sequence with privileges, owner, and comment", func() {
			startValue := int64(0)
			if connection.Version.AtLeast("6") {
				startValue = 1
			}
			sequenceDef.SequenceDefinition = backup.SequenceDefinition{Name: "my_sequence", LastVal: 1, Increment: 1, MaxVal: math.MaxInt64, MinVal: 1, CacheVal: 1, StartVal: startValue}
			sequenceMetadata := backup.ObjectMetadata{Privileges: []backup.ACL{testutils.DefaultACLWithout("testrole", "SEQUENCE", "UPDATE")}, Owner: "testrole", Comment: "This is a sequence comment."}
			sequenceMetadataMap[1] = sequenceMetadata
			backup.PrintCreateSequenceStatements(backupfile, toc, []backup.Sequence{sequenceDef}, sequenceMetadataMap)
			if connection.Version.Before("5") {
				sequenceDef.LogCnt = 1 // In GPDB 4.3, sequence log count is one-indexed
			}

			testhelper.AssertQueryRuns(connection, buffer.String())
			defer testhelper.AssertQueryRuns(connection, "DROP SEQUENCE public.my_sequence")

			resultSequences := backup.GetAllSequences(connection, map[string]string{})

			Expect(resultSequences).To(HaveLen(1))
			resultMetadataMap := backup.GetMetadataForObjectType(connection, backup.TYPE_RELATION)
			oid := testutils.OidFromObjectName(connection, "public", "my_sequence", backup.TYPE_RELATION)
			resultMetadata := resultMetadataMap[oid]
			structmatcher.ExpectStructsToMatchExcluding(&sequence, &resultSequences[0].Relation, "SchemaOid", "Oid")
			structmatcher.ExpectStructsToMatch(&sequenceDef.SequenceDefinition, &resultSequences[0].SequenceDefinition)
			structmatcher.ExpectStructsToMatch(&sequenceMetadata, &resultMetadata)
		})
	})
	Describe("PrintAlterSequenceStatements", func() {
		It("creates a sequence owned by a table column", func() {
			startValue := int64(0)
			if connection.Version.AtLeast("6") {
				startValue = 1
			}
			sequenceDef := backup.Sequence{Relation: backup.Relation{SchemaOid: 0, Oid: 1, Schema: "public", Name: "my_sequence", Inherits: nil}}
			columnOwnerMap := map[string]string{"public.my_sequence": "public.sequence_table.a"}

			sequenceDef.SequenceDefinition = backup.SequenceDefinition{Name: "my_sequence",
				LastVal: 1, Increment: 1, MaxVal: math.MaxInt64, MinVal: 1, CacheVal: 1, StartVal: startValue}
			backup.PrintCreateSequenceStatements(backupfile, toc, []backup.Sequence{sequenceDef}, backup.MetadataMap{})
			backup.PrintAlterSequenceStatements(backupfile, toc, []backup.Sequence{sequenceDef}, columnOwnerMap)

			//Create table that sequence can be owned by
			testhelper.AssertQueryRuns(connection, "CREATE TABLE public.sequence_table(a int)")
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.sequence_table")

			testhelper.AssertQueryRuns(connection, buffer.String())
			defer testhelper.AssertQueryRuns(connection, "DROP SEQUENCE public.my_sequence")

			sequenceOwnerTables, sequenceOwnerColumns := backup.GetSequenceColumnOwnerMap(connection)
			Expect(sequenceOwnerTables).To(HaveLen(1))
			Expect(sequenceOwnerColumns).To(HaveLen(1))
			Expect(sequenceOwnerTables["public.my_sequence"]).To(Equal("public.sequence_table"))
			Expect(sequenceOwnerColumns["public.my_sequence"]).To(Equal("public.sequence_table.a"))
		})
	})
})
