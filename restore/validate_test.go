package restore_test

import (
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup_history"
	"github.com/greenplum-db/gpbackup/restore"
	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/utils"
	"github.com/DATA-DOG/go-sqlmock"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("restore/validate tests", func() {
	var filterList []string
	var toc *utils.TOC
	var backupfile *utils.FileWithByteCount
	AfterEach(func() {
		filterList = []string{}
	})
	Describe("ValidateSchemasInBackupSet", func() {
		sequence := utils.StatementWithType{ObjectType: "SEQUENCE", Statement: "CREATE SEQUENCE schema.somesequence"}
		sequenceLen := uint64(len(sequence.Statement))
		table1 := utils.StatementWithType{ObjectType: "TABLE", Statement: "CREATE TABLE schema1.table1"}
		table1Len := uint64(len(table1.Statement))
		table2 := utils.StatementWithType{ObjectType: "TABLE", Statement: "CREATE TABLE schema2.table2"}
		table2Len := uint64(len(table2.Statement))
		BeforeEach(func() {
			toc, backupfile = testutils.InitializeTestTOC(buffer, "predata")
			backupfile.ByteCount = table1Len
			toc.AddMetadataEntry("predata", utils.MetadataEntry{Schema: "schema1", Name: "table1", ObjectType: "TABLE"}, 0, backupfile.ByteCount)
			toc.AddMasterDataEntry("schema1", "table1", 1, "(i)", 0, "")
			backupfile.ByteCount += table2Len
			toc.AddMetadataEntry("predata", utils.MetadataEntry{Schema: "schema2", Name: "table2", ObjectType: "TABLE"}, table1Len, backupfile.ByteCount)
			toc.AddMasterDataEntry("schema2", "table2", 2, "(j)", 0, "")
			backupfile.ByteCount += sequenceLen
			toc.AddMetadataEntry("predata", utils.MetadataEntry{Schema: "schema", Name: "somesequence", ObjectType: "SEQUENCE"}, table1Len+table2Len, backupfile.ByteCount)
			restore.SetTOC(toc)
		})
		It("passes when schema exists in normal backup", func() {
			restore.SetBackupConfig(&backup_history.BackupConfig{})
			filterList = []string{"schema1"}
			restore.ValidateIncludeSchemasInBackupSet(filterList)
		})
		It("panics when schema does not exist in normal backup", func() {
			restore.SetBackupConfig(&backup_history.BackupConfig{})
			filterList = []string{"schema3"}
			defer testhelper.ShouldPanicWithMessage("Could not find the following schema(s) in the backup set: schema3")
			restore.ValidateIncludeSchemasInBackupSet(filterList)
		})
		It("passes when schema exists in data-only backup", func() {
			restore.SetBackupConfig(&backup_history.BackupConfig{DataOnly: true})
			filterList = []string{"schema1"}
			restore.ValidateIncludeSchemasInBackupSet(filterList)
		})
		It("panics when schema does not exist in data-only backup", func() {
			restore.SetBackupConfig(&backup_history.BackupConfig{DataOnly: true})
			filterList = []string{"schema3"}
			defer testhelper.ShouldPanicWithMessage("Could not find the following schema(s) in the backup set: schema3")
			restore.ValidateIncludeSchemasInBackupSet(filterList)
		})
		It("generates warning when exclude-schema does not exist in backup and noFatal is true", func() {
			_, _, logfile = testhelper.SetupTestLogger()
			restore.SetBackupConfig(&backup_history.BackupConfig{})
			filterList = []string{"schema3"}
			restore.ValidateExcludeSchemasInBackupSet(filterList)
			testhelper.ExpectRegexp(logfile, "[WARNING]:-Could not find the following excluded schema(s) in the backup set: schema3")
		})
	})
	Describe("GenerateRestoreRelationList", func() {
		BeforeEach(func() {
			toc, _ = testutils.InitializeTestTOC(buffer, "metadata")
			toc.AddMasterDataEntry("s1", "table1", 1, "(j)", 0, "")
			toc.AddMasterDataEntry("s1", "table2", 2, "(j)", 0, "")
			toc.AddMasterDataEntry("s2", "table1", 3, "(j)", 0, "")
			toc.AddMasterDataEntry("s2", "table2", 4, "(j)", 0, "")
			restore.SetTOC(toc)
			cmdFlags.Set(utils.INCLUDE_RELATION, "")
			cmdFlags.Set(utils.EXCLUDE_RELATION, "")
			cmdFlags.Set(utils.INCLUDE_SCHEMA, "")
			cmdFlags.Set(utils.EXCLUDE_SCHEMA, "")
		})
		It("returns all tables if no filtering is used", func() {
			expectedRelations := []string{"s1.table1", "s1.table2", "s2.table1", "s2.table2"}

			resultRelations := restore.GenerateRestoreRelationList()

			Expect(resultRelations).To(ConsistOf(expectedRelations))
		})
		It("filters on include relations", func() {
			cmdFlags.Set(utils.INCLUDE_RELATION, "s1.table1,s2.table2")
			expectedRelations := []string{"s1.table1", "s2.table2"}

			resultRelations := restore.GenerateRestoreRelationList()

			Expect(resultRelations).To(ConsistOf(expectedRelations))
		})
		It("filters on exclude relations", func() {
			cmdFlags.Set(utils.EXCLUDE_RELATION, "s1.table2,s2.table1")
			expectedRelations := []string{"s1.table1", "s2.table2"}

			resultRelations := restore.GenerateRestoreRelationList()

			Expect(resultRelations).To(ConsistOf(expectedRelations))
		})
		It("filters on include schema", func() {
			cmdFlags.Set(utils.INCLUDE_SCHEMA, "s1")
			expectedRelations := []string{"s1.table1", "s1.table2"}

			resultRelations := restore.GenerateRestoreRelationList()

			Expect(resultRelations).To(ConsistOf(expectedRelations))
		})
		It("filters on exclude schema", func() {
			cmdFlags.Set(utils.EXCLUDE_SCHEMA, "s2")
			expectedRelations := []string{"s1.table1", "s1.table2"}

			resultRelations := restore.GenerateRestoreRelationList()

			Expect(resultRelations).To(ConsistOf(expectedRelations))
		})
		It("filters on include schema with exclude relation", func() {
			cmdFlags.Set(utils.INCLUDE_SCHEMA, "s1")
			cmdFlags.Set(utils.EXCLUDE_RELATION, "s1.table1")
			expectedRelations := []string{"s1.table2"}

			resultRelations := restore.GenerateRestoreRelationList()

			Expect(resultRelations).To(ConsistOf(expectedRelations))
		})
	})
	Describe("ValidateRelationsInRestoreDatabase", func() {
		BeforeEach(func() {
			restore.SetBackupConfig(&backup_history.BackupConfig{DataOnly: false})
			cmdFlags.Set(utils.DATA_ONLY, "false")
		})
		Context("data-only restore", func() {
			BeforeEach(func() {
				cmdFlags.Set(utils.DATA_ONLY, "true")
			})
			It("panics if all tables missing from database", func() {
				no_table_rows := sqlmock.NewRows([]string{"string"})
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(no_table_rows)
				filterList = []string{"public.table2"}
				defer testhelper.ShouldPanicWithMessage("Relation public.table2 must exist for data-only restore")
				restore.ValidateRelationsInRestoreDatabase(connectionPool, filterList)
			})
			It("panics if some tables missing from database", func() {
				single_table_row := sqlmock.NewRows([]string{"string"}).
					AddRow("public.table1")
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(single_table_row)
				filterList = []string{"public.table1", "public.table2"}
				defer testhelper.ShouldPanicWithMessage("Relation public.table2 must exist for data-only restore")
				restore.ValidateRelationsInRestoreDatabase(connectionPool, filterList)
			})
			It("passes if all tables are present in database", func() {
				two_table_rows := sqlmock.NewRows([]string{"string"}).
					AddRow("public.table1").AddRow("public.table2")
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(two_table_rows)
				filterList = []string{"public.table1", "public.table2"}
				restore.ValidateRelationsInRestoreDatabase(connectionPool, filterList)
			})
		})
		Context("restore includes metadata", func() {
			It("passes if table is not present in database", func() {
				no_table_rows := sqlmock.NewRows([]string{"string"})
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(no_table_rows)
				filterList = []string{"public.table2"}
				restore.ValidateRelationsInRestoreDatabase(connectionPool, filterList)
			})
			It("panics if single table is present in database", func() {
				single_table_row := sqlmock.NewRows([]string{"string"}).
					AddRow("public.table1")
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(single_table_row)
				filterList = []string{"public.table1", "public.table2"}
				defer testhelper.ShouldPanicWithMessage("Relation public.table1 already exists")
				restore.ValidateRelationsInRestoreDatabase(connectionPool, filterList)
			})
			It("panics if multiple tables are present in database", func() {
				two_table_rows := sqlmock.NewRows([]string{"string"}).
					AddRow("public.table1").AddRow("public.view1")
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(two_table_rows)
				filterList = []string{"public.table1", "public.view1"}
				defer testhelper.ShouldPanicWithMessage("Relation public.table1 already exists")
				restore.ValidateRelationsInRestoreDatabase(connectionPool, filterList)
			})
		})
	})
	Describe("ValidateRelationsInBackupSet", func() {
		var toc *utils.TOC
		var backupfile *utils.FileWithByteCount
		BeforeEach(func() {
			toc, backupfile = testutils.InitializeTestTOC(buffer, "predata")
			toc.AddMetadataEntry("predata", utils.MetadataEntry{Schema: "schema1", Name: "table1", ObjectType: "TABLE"}, 0, backupfile.ByteCount)
			toc.AddMasterDataEntry("schema1", "table1", 1, "(i)", 0, "")

			toc.AddMetadataEntry("predata", utils.MetadataEntry{Schema: "schema2", Name: "table2", ObjectType: "TABLE"}, 0, backupfile.ByteCount)
			toc.AddMasterDataEntry("schema2", "table2", 2, "(j)", 0, "")

			toc.AddMetadataEntry("predata", utils.MetadataEntry{Schema: "schema1", Name: "somesequence", ObjectType: "SEQUENCE"}, 0, backupfile.ByteCount)
			toc.AddMetadataEntry("predata", utils.MetadataEntry{Schema: "schema1", Name: "someview", ObjectType: "VIEW"}, 0, backupfile.ByteCount)
			toc.AddMetadataEntry("predata", utils.MetadataEntry{Schema: "schema1", Name: "somefunction", ObjectType: "FUNCTION"}, 0, backupfile.ByteCount)

			restore.SetTOC(toc)
		})
		It("passes when table exists in normal backup", func() {
			restore.SetBackupConfig(&backup_history.BackupConfig{})
			filterList = []string{"schema1.table1"}
			restore.ValidateIncludeRelationsInBackupSet(filterList)
		})
		It("panics when table does not exist in normal backup", func() {
			restore.SetBackupConfig(&backup_history.BackupConfig{})
			filterList = []string{"schema1.table3"}
			defer testhelper.ShouldPanicWithMessage("Could not find the following relation(s) in the backup set: schema1.table3")
			restore.ValidateIncludeRelationsInBackupSet(filterList)
		})
		It("passes when sequence exists in normal backup", func() {
			restore.SetBackupConfig(&backup_history.BackupConfig{})
			filterList = []string{"schema1.somesequence"}
			restore.ValidateIncludeRelationsInBackupSet(filterList)
		})
		It("generates a warning if the exclude-schema is not in the backup set and noFatal is true", func() {
			_, _, logfile = testhelper.SetupTestLogger()
			restore.SetBackupConfig(&backup_history.BackupConfig{})
			filterList = []string{"schema1.table3"}
			restore.ValidateExcludeRelationsInBackupSet(filterList)
			testhelper.ExpectRegexp(logfile, "[WARNING]:-Could not find the following excluded relation(s) in the backup set: schema1.table3")
		})
		It("passes when view exists in normal backup", func() {
			restore.SetBackupConfig(&backup_history.BackupConfig{})
			filterList = []string{"schema1.someview"}
			restore.ValidateIncludeRelationsInBackupSet(filterList)
		})
		It("passes when table exists in data-only backup", func() {
			restore.SetBackupConfig(&backup_history.BackupConfig{DataOnly: true})
			filterList = []string{"schema1.table1"}
			restore.ValidateIncludeRelationsInBackupSet(filterList)
		})
		It("panics when relation does not exist in backup but function with same name does", func() {
			restore.SetBackupConfig(&backup_history.BackupConfig{})
			filterList = []string{"schema1.somefunction"}
			defer testhelper.ShouldPanicWithMessage("Could not find the following relation(s) in the backup set: schema1.somefunction")
			restore.ValidateIncludeRelationsInBackupSet(filterList)
		})
		It("table does not exist in data-only backup", func() {
			restore.SetBackupConfig(&backup_history.BackupConfig{DataOnly: true})
			filterList = []string{"schema1.table3"}
			defer testhelper.ShouldPanicWithMessage("Could not find the following relation(s) in the backup set: schema1.table3")
			restore.ValidateIncludeRelationsInBackupSet(filterList)
		})
		It("passes when table exists in most recent restore plan entry", func() {
			restore.SetBackupConfig(&backup_history.BackupConfig{RestorePlan: []backup_history.RestorePlanEntry{{TableFQNs: []string{"schema1.table1_part_1"}}}})
			filterList = []string{"schema1.table1_part_1"}
			restore.ValidateIncludeRelationsInBackupSet(filterList)
		})
		It("passes when table exists in previous restore plan entry", func() {
			restore.SetBackupConfig(&backup_history.BackupConfig{RestorePlan: []backup_history.RestorePlanEntry{{TableFQNs: []string{"schema1.random_table"}}, {TableFQNs: []string{"schema1.table1_part_1"}}}})
			filterList = []string{"schema1.table1_part_1"}
			restore.ValidateIncludeRelationsInBackupSet(filterList)
		})
	})
	Describe("ValidateDatabaseExistence", func() {
		It("panics if createdb passed when db exists", func() {
			db_exists := sqlmock.NewRows([]string{"string"}).
				AddRow("true")
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(db_exists)
			defer testhelper.ShouldPanicWithMessage(`Database "testdb" already exists.`)
			restore.ValidateDatabaseExistence("testdb", true, false)
		})
		It("passes if db exists and --create-db not passed", func() {
			db_exists := sqlmock.NewRows([]string{"string"}).
				AddRow("true")
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(db_exists)
			restore.ValidateDatabaseExistence("testdb", false, false)
		})
		It("panics and tells user to manually create db when db does not exist and filtered", func() {
			db_exists := sqlmock.NewRows([]string{"string"}).
				AddRow("false")
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(db_exists)
			defer testhelper.ShouldPanicWithMessage(`Database "testdb" must be created manually`)
			restore.ValidateDatabaseExistence("testdb", true, true)
		})
		It("panics and tells user to pass --create-db when db does not exist, not filtered, and no --create-db", func() {
			db_exists := sqlmock.NewRows([]string{"string"}).
				AddRow("false")
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(db_exists)
			defer testhelper.ShouldPanicWithMessage(`Database "testdb" does not exist. Use the --create-db flag`)
			restore.ValidateDatabaseExistence("testdb", false, false)
		})
	})
})
