package restore_test

import (
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/restore"
	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/utils"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"

	. "github.com/onsi/ginkgo"
)

var _ = Describe("restore/validate tests", func() {
	var filterList []string
	var toc *utils.TOC
	var backupfile *utils.FileWithByteCount
	AfterEach(func() {
		filterList = []string{}
	})
	Describe("ValidateFilterSchemasInBackupSet", func() {
		sequence := utils.StatementWithType{ObjectType: "SEQUENCE", Statement: "CREATE SEQUENCE schema.somesequence"}
		sequenceLen := uint64(len(sequence.Statement))
		table1 := utils.StatementWithType{ObjectType: "TABLE", Statement: "CREATE TABLE schema1.table1"}
		table1Len := uint64(len(table1.Statement))
		table2 := utils.StatementWithType{ObjectType: "TABLE", Statement: "CREATE TABLE schema2.table2"}
		table2Len := uint64(len(table2.Statement))
		BeforeEach(func() {
			toc, backupfile = testutils.InitializeTestTOC(buffer, "predata")
			backupfile.ByteCount = table1Len
			toc.AddPredataEntry("schema1", "table1", "", "TABLE", 0, backupfile)
			toc.AddMasterDataEntry("schema1", "table1", 1, "(i)", 0, "")
			backupfile.ByteCount += table2Len
			toc.AddPredataEntry("schema2", "table2", "TABLE", "", table1Len, backupfile)
			toc.AddMasterDataEntry("schema2", "table2", 2, "(j)", 0, "")
			backupfile.ByteCount += sequenceLen
			toc.AddPredataEntry("schema", "somesequence", "SEQUENCE", "", table1Len+table2Len, backupfile)
			restore.SetTOC(toc)
		})
		It("passes when schema exists in normal backup", func() {
			restore.SetBackupConfig(&utils.BackupConfig{})
			filterList = []string{"schema1"}
			restore.ValidateFilterSchemasInBackupSet(filterList)
		})
		It("panics when schema does not exist in normal backup", func() {
			restore.SetBackupConfig(&utils.BackupConfig{})
			filterList = []string{"schema3"}
			defer testhelper.ShouldPanicWithMessage("Could not find the following schema(s) in the backup set: schema3")
			restore.ValidateFilterSchemasInBackupSet(filterList)
		})
		It("passes when schema exists in data-only backup", func() {
			restore.SetBackupConfig(&utils.BackupConfig{DataOnly: true})
			filterList = []string{"schema1"}
			restore.ValidateFilterSchemasInBackupSet(filterList)
		})
		It("panics when schema does not exist in data-only backup", func() {
			restore.SetBackupConfig(&utils.BackupConfig{DataOnly: true})
			filterList = []string{"schema3"}
			defer testhelper.ShouldPanicWithMessage("Could not find the following schema(s) in the backup set: schema3")
			restore.ValidateFilterSchemasInBackupSet(filterList)
		})
	})
	Describe("ValidateRelationsInRestoreDatabase", func() {
		BeforeEach(func() {
			restore.SetBackupConfig(&utils.BackupConfig{DataOnly: false})
			cmdFlags.Set(utils.DATA_ONLY, "false")
			toc, _ = testutils.InitializeTestTOC(buffer, "metadata")
			toc.AddMasterDataEntry("public", "table1", 1, "(j)", 0, "")
			toc.AddMasterDataEntry("public", "table2", 2, "(j)", 0, "")
			restore.SetTOC(toc)

		})
		Context("data-only restore", func() {
			BeforeEach(func() {
				cmdFlags.Set(utils.DATA_ONLY, "true")
			})
			Context("with filtering", func() {
				It("panics if all tables missing from database", func() {
					no_table_rows := sqlmock.NewRows([]string{"string"})
					mock.ExpectQuery("SELECT (.*)").WillReturnRows(no_table_rows)
					filterList = []string{"public.table2"}
					defer testhelper.ShouldPanicWithMessage("Relation public.table2 must exist for data-only restore")
					restore.ValidateRelationsInRestoreDatabase(connection, filterList)
				})
				It("panics if some tables missing from database", func() {
					single_table_row := sqlmock.NewRows([]string{"string"}).
						AddRow("public.table1")
					mock.ExpectQuery("SELECT (.*)").WillReturnRows(single_table_row)
					filterList = []string{"public.table1", "public.table2"}
					defer testhelper.ShouldPanicWithMessage("Relation public.table2 must exist for data-only restore")
					restore.ValidateRelationsInRestoreDatabase(connection, filterList)
				})
				It("passes if all tables are present in database", func() {
					two_table_rows := sqlmock.NewRows([]string{"string"}).
						AddRow("public.table1").AddRow("public.table2")
					mock.ExpectQuery("SELECT (.*)").WillReturnRows(two_table_rows)
					filterList = []string{"public.table1", "public.table2"}
					restore.ValidateRelationsInRestoreDatabase(connection, filterList)
				})
			})
			Context("without filtering", func() {
				It("panics if all tables missing from database", func() {
					no_table_rows := sqlmock.NewRows([]string{"string"})
					mock.ExpectQuery("SELECT (.*)").WillReturnRows(no_table_rows)
					defer testhelper.ShouldPanicWithMessage("Relation public.table2 must exist for data-only restore")
					restore.ValidateRelationsInRestoreDatabase(connection, filterList)
				})
				It("panics if some tables missing from database", func() {
					single_table_row := sqlmock.NewRows([]string{"string"}).
						AddRow("public.table1")
					mock.ExpectQuery("SELECT (.*)").WillReturnRows(single_table_row)
					defer testhelper.ShouldPanicWithMessage("Relation public.table2 must exist for data-only restore")
					restore.ValidateRelationsInRestoreDatabase(connection, filterList)
				})
				It("passes if all tables are present in database", func() {
					two_table_rows := sqlmock.NewRows([]string{"string"}).
						AddRow("public.table1").AddRow("public.view1")
					mock.ExpectQuery("SELECT (.*)").WillReturnRows(two_table_rows)
					restore.ValidateRelationsInRestoreDatabase(connection, filterList)
				})
			})
		})
		Context("restore includes metadata", func() {
			Context("with filtering", func() {
				It("passes if table is not present in database", func() {
					no_table_rows := sqlmock.NewRows([]string{"string"})
					mock.ExpectQuery("SELECT (.*)").WillReturnRows(no_table_rows)
					filterList = []string{"public.table2"}
					restore.ValidateRelationsInRestoreDatabase(connection, filterList)
				})
				It("panics if single table is present in database", func() {
					single_table_row := sqlmock.NewRows([]string{"string"}).
						AddRow("public.table1")
					mock.ExpectQuery("SELECT (.*)").WillReturnRows(single_table_row)
					filterList = []string{"public.table1"}
					defer testhelper.ShouldPanicWithMessage("Relation public.table1 already exists")
					restore.ValidateRelationsInRestoreDatabase(connection, filterList)
				})
				It("panics if multiple tables are present in database", func() {
					two_table_rows := sqlmock.NewRows([]string{"string"}).
						AddRow("public.table1").AddRow("public.view1")
					mock.ExpectQuery("SELECT (.*)").WillReturnRows(two_table_rows)
					filterList = []string{"public.table1", "public.view1"}
					defer testhelper.ShouldPanicWithMessage("Relation public.table1 already exists")
					restore.ValidateRelationsInRestoreDatabase(connection, filterList)
				})
			})
			Context("without filtering", func() {
				It("passes if table is not present in database", func() {
					no_table_rows := sqlmock.NewRows([]string{"string"})
					mock.ExpectQuery("SELECT (.*)").WillReturnRows(no_table_rows)
					restore.ValidateRelationsInRestoreDatabase(connection, filterList)
				})
				It("passes if there are no data entries in the backup", func() {
					toc, _ = testutils.InitializeTestTOC(buffer, "metadata")
					restore.SetTOC(toc)

					restore.ValidateRelationsInRestoreDatabase(connection, filterList)
				})
				It("panics if single table is present in database", func() {
					single_table_row := sqlmock.NewRows([]string{"string"}).
						AddRow("public.table1")
					mock.ExpectQuery("SELECT (.*)").WillReturnRows(single_table_row)
					defer testhelper.ShouldPanicWithMessage("Relation public.table1 already exists")
					restore.ValidateRelationsInRestoreDatabase(connection, filterList)
				})
				It("panics if multiple tables are present in database", func() {
					two_table_rows := sqlmock.NewRows([]string{"string"}).
						AddRow("public.table1").AddRow("public.table2")
					mock.ExpectQuery("SELECT (.*)").WillReturnRows(two_table_rows)
					defer testhelper.ShouldPanicWithMessage("Relation public.table1 already exists")
					restore.ValidateRelationsInRestoreDatabase(connection, filterList)
				})
			})
		})
	})
	Describe("ValidateFilterRelationsInBackupSet", func() {
		var toc *utils.TOC
		var backupfile *utils.FileWithByteCount
		BeforeEach(func() {
			toc, backupfile = testutils.InitializeTestTOC(buffer, "predata")
			toc.AddPredataEntry("schema1", "table1", "TABLE", "", 0, backupfile)
			toc.AddMasterDataEntry("schema1", "table1", 1, "(i)", 0, "")

			toc.AddPredataEntry("schema2", "table2", "TABLE", "", 0, backupfile)
			toc.AddMasterDataEntry("schema2", "table2", 2, "(j)", 0, "")

			toc.AddPredataEntry("schema1", "somesequence", "SEQUENCE", "", 0, backupfile)
			toc.AddPredataEntry("schema1", "someview", "VIEW", "", 0, backupfile)
			toc.AddPredataEntry("schema1", "somefunction", "FUNCTION", "", 0, backupfile)

			restore.SetTOC(toc)
		})
		It("passes when table exists in normal backup", func() {
			restore.SetBackupConfig(&utils.BackupConfig{})
			filterList = []string{"schema1.table1"}
			restore.ValidateFilterRelationsInBackupSet(filterList)
		})
		It("panics when table does not exist in normal backup", func() {
			restore.SetBackupConfig(&utils.BackupConfig{})
			filterList = []string{"schema1.table3"}
			defer testhelper.ShouldPanicWithMessage("Could not find the following relation(s) in the backup set: schema1.table3")
			restore.ValidateFilterRelationsInBackupSet(filterList)
		})
		It("passes when sequence exists in normal backup", func() {
			restore.SetBackupConfig(&utils.BackupConfig{})
			filterList = []string{"schema1.somesequence"}
			restore.ValidateFilterRelationsInBackupSet(filterList)
		})
		It("passes when view exists in normal backup", func() {
			restore.SetBackupConfig(&utils.BackupConfig{})
			filterList = []string{"schema1.someview"}
			restore.ValidateFilterRelationsInBackupSet(filterList)
		})
		It("passes when table exists in data-only backup", func() {
			restore.SetBackupConfig(&utils.BackupConfig{DataOnly: true})
			filterList = []string{"schema1.table1"}
			restore.ValidateFilterRelationsInBackupSet(filterList)
		})
		It("panics when relation does not exist in backup but function with same name does", func() {
			restore.SetBackupConfig(&utils.BackupConfig{})
			filterList = []string{"schema1.somefunction"}
			defer testhelper.ShouldPanicWithMessage("Could not find the following relation(s) in the backup set: schema1.somefunction")
			restore.ValidateFilterRelationsInBackupSet(filterList)
		})
		It("table does not exist in data-only backup", func() {
			restore.SetBackupConfig(&utils.BackupConfig{DataOnly: true})
			filterList = []string{"schema1.table3"}
			defer testhelper.ShouldPanicWithMessage("Could not find the following relation(s) in the backup set: schema1.table3")
			restore.ValidateFilterRelationsInBackupSet(filterList)
		})
		It("passes when table exists in most recent restore plan entry", func() {
			restore.SetBackupConfig(&utils.BackupConfig{RestorePlan: []utils.RestorePlanEntry{{TableFQNs: []string{"schema1.table1_part_1"}}}})
			filterList = []string{"schema1.table1_part_1"}
			restore.ValidateFilterRelationsInBackupSet(filterList)
		})
		It("passes when table exists in previous restore plan entry", func() {
			restore.SetBackupConfig(&utils.BackupConfig{RestorePlan: []utils.RestorePlanEntry{{TableFQNs: []string{"schema1.random_table"}}, {TableFQNs: []string{"schema1.table1_part_1"}}}})
			filterList = []string{"schema1.table1_part_1"}
			restore.ValidateFilterRelationsInBackupSet(filterList)
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
