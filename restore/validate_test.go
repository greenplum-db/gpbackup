package restore_test

import (
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/history"
	"github.com/greenplum-db/gpbackup/options"
	"github.com/greenplum-db/gpbackup/restore"
	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/toc"
	"github.com/greenplum-db/gpbackup/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("restore/validate tests", func() {
	var filterList []string
	var tocfile *toc.TOC
	var backupfile *utils.FileWithByteCount
	AfterEach(func() {
		filterList = []string{}
	})
	Describe("ValidateSchemasInBackupSet", func() {
		sequence := toc.StatementWithType{ObjectType: "SEQUENCE", Statement: "CREATE SEQUENCE schema.somesequence"}
		sequenceLen := uint64(len(sequence.Statement))
		table1 := toc.StatementWithType{ObjectType: "TABLE", Statement: "CREATE TABLE schema1.table1"}
		table1Len := uint64(len(table1.Statement))
		table2 := toc.StatementWithType{ObjectType: "TABLE", Statement: "CREATE TABLE schema2.table2"}
		table2Len := uint64(len(table2.Statement))
		BeforeEach(func() {
			tocfile, backupfile = testutils.InitializeTestTOC(buffer, "predata")
			backupfile.ByteCount = table1Len
			tocfile.AddMetadataEntry("predata", toc.MetadataEntry{Schema: "schema1", Name: "table1", ObjectType: "TABLE"}, 0, backupfile.ByteCount)
			tocfile.AddMasterDataEntry("schema1", "table1", 1, "(i)", 0, "")
			backupfile.ByteCount += table2Len
			tocfile.AddMetadataEntry("predata", toc.MetadataEntry{Schema: "schema2", Name: "table2", ObjectType: "TABLE"}, table1Len, backupfile.ByteCount)
			tocfile.AddMasterDataEntry("schema2", "table2", 2, "(j)", 0, "")
			backupfile.ByteCount += sequenceLen
			tocfile.AddMetadataEntry("predata", toc.MetadataEntry{Schema: "schema", Name: "somesequence", ObjectType: "SEQUENCE"}, table1Len+table2Len, backupfile.ByteCount)
			restore.SetTOC(tocfile)
		})
		It("passes when schema exists in normal backup", func() {
			restore.SetBackupConfig(&history.BackupConfig{})
			filterList = []string{"schema1"}
			restore.ValidateIncludeSchemasInBackupSet(filterList)
		})
		It("panics when schema does not exist in normal backup", func() {
			restore.SetBackupConfig(&history.BackupConfig{})
			filterList = []string{"schema3"}
			defer testhelper.ShouldPanicWithMessage("Could not find the following schema(s) in the backup set: schema3")
			restore.ValidateIncludeSchemasInBackupSet(filterList)
		})
		It("passes when schema exists in data-only backup", func() {
			restore.SetBackupConfig(&history.BackupConfig{DataOnly: true})
			filterList = []string{"schema1"}
			restore.ValidateIncludeSchemasInBackupSet(filterList)
		})
		It("panics when schema does not exist in data-only backup", func() {
			restore.SetBackupConfig(&history.BackupConfig{DataOnly: true})
			filterList = []string{"schema3"}
			defer testhelper.ShouldPanicWithMessage("Could not find the following schema(s) in the backup set: schema3")
			restore.ValidateIncludeSchemasInBackupSet(filterList)
		})
		It("generates warning when exclude-schema does not exist in backup and noFatal is true", func() {
			_, _, logfile = testhelper.SetupTestLogger()
			restore.SetBackupConfig(&history.BackupConfig{})
			filterList = []string{"schema3"}
			restore.ValidateExcludeSchemasInBackupSet(filterList)
			testhelper.ExpectRegexp(logfile, "[WARNING]:-Could not find the following excluded schema(s) in the backup set: schema3")
		})
	})
	Describe("GenerateRestoreRelationList", func() {
		var opts *options.Options
		BeforeEach(func() {
			tocfile, _ = testutils.InitializeTestTOC(buffer, "metadata")
			tocfile.AddMasterDataEntry("s1", "table1", 1, "(j)", 0, "")
			tocfile.AddMasterDataEntry("s1", "table2", 2, "(j)", 0, "")
			tocfile.AddMasterDataEntry("s2", "table1", 3, "(j)", 0, "")
			tocfile.AddMasterDataEntry("s2", "table2", 4, "(j)", 0, "")
			restore.SetTOC(tocfile)

			opts = &options.Options{}
		})
		It("returns all tables if no filtering is used", func() {
			opts.IncludedRelations = []string{"s1.table1", "s1.table2", "s2.table1", "s2.table2"}

			resultRelations := restore.GenerateRestoreRelationList(*opts)

			expectedRelations := []string{"s1.table1", "s1.table2", "s2.table1", "s2.table2"}
			Expect(resultRelations).To(ConsistOf(expectedRelations))
		})
		It("filters on include relations", func() {
			opts.IncludedRelations = []string{"s1.table1", "s1.table2"}

			resultRelations := restore.GenerateRestoreRelationList(*opts)

			expectedRelations := []string{"s1.table1", "s1.table2"}
			Expect(resultRelations).To(ConsistOf(expectedRelations))
		})
		It("filters on exclude relations", func() {
			opts.ExcludedRelations = []string{"s1.table2", "s2.table1"}

			resultRelations := restore.GenerateRestoreRelationList(*opts)

			expectedRelations := []string{"s1.table1", "s2.table2"}
			Expect(resultRelations).To(ConsistOf(expectedRelations))
		})
		It("filters on include schema", func() {
			opts.IncludedSchemas = []string{"s1"}

			resultRelations := restore.GenerateRestoreRelationList(*opts)

			expectedRelations := []string{"s1.table1", "s1.table2"}
			Expect(resultRelations).To(ConsistOf(expectedRelations))
		})
		It("filters on exclude schema", func() {
			opts.ExcludedSchemas = []string{"s2"}

			resultRelations := restore.GenerateRestoreRelationList(*opts)

			expectedRelations := []string{"s1.table1", "s1.table2"}
			Expect(resultRelations).To(ConsistOf(expectedRelations))
		})
		It("filters on include schema with exclude relation", func() {
			opts.IncludedSchemas = []string{"s1"}
			opts.ExcludedRelations = []string{"s1.table1"}

			resultRelations := restore.GenerateRestoreRelationList(*opts)

			expectedRelations := []string{"s1.table2"}
			Expect(resultRelations).To(ConsistOf(expectedRelations))
		})
	})
	Describe("ValidateRelationsInRestoreDatabase", func() {
		BeforeEach(func() {
			restore.SetBackupConfig(&history.BackupConfig{DataOnly: false})
			_ = cmdFlags.Set(options.DATA_ONLY, "false")
		})
		Context("data-only restore", func() {
			BeforeEach(func() {
				_ = cmdFlags.Set(options.DATA_ONLY, "true")
			})
			It("panics if all tables missing from database", func() {
				noTableRows := sqlmock.NewRows([]string{"string"})
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(noTableRows)
				filterList = []string{"public.table2"}
				defer testhelper.ShouldPanicWithMessage("Relation public.table2 must exist for data-only restore")
				restore.ValidateRelationsInRestoreDatabase(connectionPool, filterList)
			})
			It("panics if some tables missing from database", func() {
				singleTableRow := sqlmock.NewRows([]string{"string"}).
					AddRow("public.table1")
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(singleTableRow)
				filterList = []string{"public.table1", "public.table2"}
				defer testhelper.ShouldPanicWithMessage("Relation public.table2 must exist for data-only restore")
				restore.ValidateRelationsInRestoreDatabase(connectionPool, filterList)
			})
			It("passes if all tables are present in database", func() {
				twoTableRows := sqlmock.NewRows([]string{"string"}).
					AddRow("public.table1").AddRow("public.table2")
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(twoTableRows)
				filterList = []string{"public.table1", "public.table2"}
				restore.ValidateRelationsInRestoreDatabase(connectionPool, filterList)
			})
		})
		Context("restore includes metadata", func() {
			It("passes if table is not present in database", func() {
				noTableRows := sqlmock.NewRows([]string{"string"})
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(noTableRows)
				filterList = []string{"public.table2"}
				restore.ValidateRelationsInRestoreDatabase(connectionPool, filterList)
			})
			It("panics if single table is present in database", func() {
				singleTableRow := sqlmock.NewRows([]string{"string"}).
					AddRow("public.table1")
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(singleTableRow)
				filterList = []string{"public.table1", "public.table2"}
				defer testhelper.ShouldPanicWithMessage("Relation public.table1 already exists")
				restore.ValidateRelationsInRestoreDatabase(connectionPool, filterList)
			})
			It("panics if multiple tables are present in database", func() {
				twoTableRows := sqlmock.NewRows([]string{"string"}).
					AddRow("public.table1").AddRow("public.view1")
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(twoTableRows)
				filterList = []string{"public.table1", "public.view1"}
				defer testhelper.ShouldPanicWithMessage("Relation public.table1 already exists")
				restore.ValidateRelationsInRestoreDatabase(connectionPool, filterList)
			})
		})
	})
	Describe("ValidateRelationsInBackupSet", func() {
		var tocfile *toc.TOC
		var backupfile *utils.FileWithByteCount
		BeforeEach(func() {
			tocfile, backupfile = testutils.InitializeTestTOC(buffer, "predata")
			tocfile.AddMetadataEntry("predata", toc.MetadataEntry{Schema: "schema1", Name: "table1", ObjectType: "TABLE"}, 0, backupfile.ByteCount)
			tocfile.AddMasterDataEntry("schema1", "table1", 1, "(i)", 0, "")

			tocfile.AddMetadataEntry("predata", toc.MetadataEntry{Schema: "schema2", Name: "table2", ObjectType: "TABLE"}, 0, backupfile.ByteCount)
			tocfile.AddMasterDataEntry("schema2", "table2", 2, "(j)", 0, "")

			tocfile.AddMetadataEntry("predata", toc.MetadataEntry{Schema: "schema1", Name: "somesequence", ObjectType: "SEQUENCE"}, 0, backupfile.ByteCount)
			tocfile.AddMetadataEntry("predata", toc.MetadataEntry{Schema: "schema1", Name: "someview", ObjectType: "VIEW"}, 0, backupfile.ByteCount)
			tocfile.AddMetadataEntry("predata", toc.MetadataEntry{Schema: "schema1", Name: "somefunction", ObjectType: "FUNCTION"}, 0, backupfile.ByteCount)

			restore.SetTOC(tocfile)
		})
		It("passes when table exists in normal backup", func() {
			restore.SetBackupConfig(&history.BackupConfig{})
			filterList = []string{"schema1.table1"}
			restore.ValidateIncludeRelationsInBackupSet(filterList)
		})
		It("panics when table does not exist in normal backup", func() {
			restore.SetBackupConfig(&history.BackupConfig{})
			filterList = []string{"schema1.table3"}
			defer testhelper.ShouldPanicWithMessage("Could not find the following relation(s) in the backup set: schema1.table3")
			restore.ValidateIncludeRelationsInBackupSet(filterList)
		})
		It("passes when sequence exists in normal backup", func() {
			restore.SetBackupConfig(&history.BackupConfig{})
			filterList = []string{"schema1.somesequence"}
			restore.ValidateIncludeRelationsInBackupSet(filterList)
		})
		It("generates a warning if the exclude-schema is not in the backup set and noFatal is true", func() {
			_, _, logfile = testhelper.SetupTestLogger()
			restore.SetBackupConfig(&history.BackupConfig{})
			filterList = []string{"schema1.table3"}
			restore.ValidateExcludeRelationsInBackupSet(filterList)
			testhelper.ExpectRegexp(logfile, "[WARNING]:-Could not find the following excluded relation(s) in the backup set: schema1.table3")
		})
		It("passes when view exists in normal backup", func() {
			restore.SetBackupConfig(&history.BackupConfig{})
			filterList = []string{"schema1.someview"}
			restore.ValidateIncludeRelationsInBackupSet(filterList)
		})
		It("passes when table exists in data-only backup", func() {
			restore.SetBackupConfig(&history.BackupConfig{DataOnly: true})
			filterList = []string{"schema1.table1"}
			restore.ValidateIncludeRelationsInBackupSet(filterList)
		})
		It("panics when relation does not exist in backup but function with same name does", func() {
			restore.SetBackupConfig(&history.BackupConfig{})
			filterList = []string{"schema1.somefunction"}
			defer testhelper.ShouldPanicWithMessage("Could not find the following relation(s) in the backup set: schema1.somefunction")
			restore.ValidateIncludeRelationsInBackupSet(filterList)
		})
		It("table does not exist in data-only backup", func() {
			restore.SetBackupConfig(&history.BackupConfig{DataOnly: true})
			filterList = []string{"schema1.table3"}
			defer testhelper.ShouldPanicWithMessage("Could not find the following relation(s) in the backup set: schema1.table3")
			restore.ValidateIncludeRelationsInBackupSet(filterList)
		})
		It("passes when table exists in most recent restore plan entry", func() {
			restore.SetBackupConfig(&history.BackupConfig{RestorePlan: []history.RestorePlanEntry{{TableFQNs: []string{"schema1.table1_part_1"}}}})
			filterList = []string{"schema1.table1_part_1"}
			restore.ValidateIncludeRelationsInBackupSet(filterList)
		})
		It("passes when table exists in previous restore plan entry", func() {
			restore.SetBackupConfig(&history.BackupConfig{RestorePlan: []history.RestorePlanEntry{{TableFQNs: []string{"schema1.random_table"}}, {TableFQNs: []string{"schema1.table1_part_1"}}}})
			filterList = []string{"schema1.table1_part_1"}
			restore.ValidateIncludeRelationsInBackupSet(filterList)
		})
	})
	Describe("ValidateDatabaseExistence", func() {
		It("panics if createdb passed when db exists", func() {
			dbExists := sqlmock.NewRows([]string{"string"}).
				AddRow("true")
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(dbExists)
			defer testhelper.ShouldPanicWithMessage(`Database "testdb" already exists.`)
			restore.ValidateDatabaseExistence("testdb", true, false)
		})
		It("passes if db exists and --create-db not passed", func() {
			dbExists := sqlmock.NewRows([]string{"string"}).
				AddRow("true")
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(dbExists)
			restore.ValidateDatabaseExistence("testdb", false, false)
		})
		It("panics and tells user to manually create db when db does not exist and filtered", func() {
			dbExists := sqlmock.NewRows([]string{"string"}).
				AddRow("false")
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(dbExists)
			defer testhelper.ShouldPanicWithMessage(`Database "testdb" must be created manually`)
			restore.ValidateDatabaseExistence("testdb", true, true)
		})
		It("panics and tells user to pass --create-db when db does not exist, not filtered, and no --create-db", func() {
			dbExists := sqlmock.NewRows([]string{"string"}).
				AddRow("false")
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(dbExists)
			defer testhelper.ShouldPanicWithMessage(`Database "testdb" does not exist. Use the --create-db flag`)
			restore.ValidateDatabaseExistence("testdb", false, false)
		})
	})
})
