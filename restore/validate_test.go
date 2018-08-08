package restore_test

import (
	"regexp"

	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/restore"
	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/utils"
	sqlmock "gopkg.in/DATA-DOG/go-sqlmock.v1"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
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
			toc.AddMasterDataEntry("schema1", "table1", 1, "(i)", 0)
			backupfile.ByteCount += table2Len
			toc.AddPredataEntry("schema2", "table2", "TABLE", "", table1Len, backupfile)
			toc.AddMasterDataEntry("schema2", "table2", 2, "(j)", 0)
			backupfile.ByteCount += sequenceLen
			toc.AddPredataEntry("schema", "somesequence", "SEQUENCE", "", table1Len+table2Len, backupfile)
			restore.SetTOC(toc)
		})
		It("schema exists in normal backup", func() {
			restore.SetBackupConfig(&utils.BackupConfig{})
			filterList = []string{"schema1"}
			restore.ValidateFilterSchemasInBackupSet(filterList)
		})
		It("schema does not exist in normal backup", func() {
			restore.SetBackupConfig(&utils.BackupConfig{})
			filterList = []string{"schema3"}
			defer testhelper.ShouldPanicWithMessage("Could not find the following schema(s) in the backup set: schema3")
			restore.ValidateFilterSchemasInBackupSet(filterList)
		})
		It("schema exists in data-only backup", func() {
			restore.SetBackupConfig(&utils.BackupConfig{DataOnly: true})
			filterList = []string{"schema1"}
			restore.ValidateFilterSchemasInBackupSet(filterList)
		})
		It("schema does not exist in data-only backup", func() {
			restore.SetBackupConfig(&utils.BackupConfig{DataOnly: true})
			filterList = []string{"schema3"}
			defer testhelper.ShouldPanicWithMessage("Could not find the following schema(s) in the backup set: schema3")
			restore.ValidateFilterSchemasInBackupSet(filterList)
		})
	})
	Describe("ValidateRelationsInRestoreDatabase", func() {
		BeforeEach(func() {
			cmdFlags.Set(utils.DATA_ONLY, "false")
			cmdFlags.Set(utils.ON_ERROR_CONTINUE, "false")
			toc, _ = testutils.InitializeTestTOC(buffer, "metadata")
			toc.AddMasterDataEntry("public", "table1", 1, "(j)", 0)
			toc.AddMasterDataEntry("public", "table2", 2, "(j)", 0)
			restore.SetTOC(toc)
		})
		Context("data-only", func() {
			BeforeEach(func() {
				cmdFlags.Set(utils.DATA_ONLY, "true")
			})
			Context("on error continue", func() {
				It("logs an error message, but does not panic with on-error-continue", func() {
					cmdFlags.Set(utils.ON_ERROR_CONTINUE, "true")
					no_table_rows := sqlmock.NewRows([]string{"string"})
					mock.ExpectQuery("SELECT (.*)").WillReturnRows(no_table_rows)
					filterList = []string{"public.table2"}
					restore.ValidateRelationsInRestoreDatabase(connection, filterList)
					Expect(stderr).To(gbytes.Say(regexp.QuoteMeta("[ERROR]:-Relation public.table2 must exist for data-only restore")))
				})
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
				It("logs an error message, but does not panic with on-error-continue", func() {
					cmdFlags.Set(utils.ON_ERROR_CONTINUE, "true")
					no_table_rows := sqlmock.NewRows([]string{"string"})
					mock.ExpectQuery("SELECT (.*)").WillReturnRows(no_table_rows)
					filterList = []string{"public.table2"}
					restore.ValidateRelationsInRestoreDatabase(connection, filterList)
					Expect(stderr).To(gbytes.Say(regexp.QuoteMeta("[ERROR]:-Relation public.table2 must exist for data-only restore")))
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
		Context("all stages restore", func() {
			Context("on error continue", func() {
				It("logs an error message, but does not panic with on-error-continue", func() {
					cmdFlags.Set(utils.ON_ERROR_CONTINUE, "true")
					two_table_rows := sqlmock.NewRows([]string{"string"}).
						AddRow("public.table1").AddRow("public.view1")
					mock.ExpectQuery("SELECT (.*)").WillReturnRows(two_table_rows)
					filterList = []string{"public.table1", "public.view1"}
					restore.ValidateRelationsInRestoreDatabase(connection, filterList)
					Expect(stderr).To(gbytes.Say(regexp.QuoteMeta("[ERROR]:-Relation public.table1 already exists")))
				})
			})
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
			toc.AddMasterDataEntry("schema1", "table1", 1, "(i)", 0)

			toc.AddPredataEntry("schema2", "table2", "TABLE", "", 0, backupfile)
			toc.AddMasterDataEntry("schema2", "table2", 2, "(j)", 0)

			toc.AddPredataEntry("schema1", "somesequence", "SEQUENCE", "", 0, backupfile)
			toc.AddPredataEntry("schema1", "someview", "VIEW", "", 0, backupfile)
			toc.AddPredataEntry("schema1", "somefunction", "FUNCTION", "", 0, backupfile)

			restore.SetTOC(toc)
		})
		It("table exists in normal backup", func() {
			restore.SetBackupConfig(&utils.BackupConfig{})
			filterList = []string{"schema1.table1"}
			restore.ValidateFilterRelationsInBackupSet(filterList)
		})
		It("table does not exist in normal backup", func() {
			restore.SetBackupConfig(&utils.BackupConfig{})
			filterList = []string{"schema1.table3"}
			defer testhelper.ShouldPanicWithMessage("Could not find the following relation(s) in the backup set: schema1.table3")
			restore.ValidateFilterRelationsInBackupSet(filterList)
		})
		It("sequence exists in normal backup", func() {
			restore.SetBackupConfig(&utils.BackupConfig{})
			filterList = []string{"schema1.somesequence"}
			restore.ValidateFilterRelationsInBackupSet(filterList)
		})
		It("view exists in normal backup", func() {
			restore.SetBackupConfig(&utils.BackupConfig{})
			filterList = []string{"schema1.someview"}
			restore.ValidateFilterRelationsInBackupSet(filterList)
		})
		It("table exists in data-only backup", func() {
			restore.SetBackupConfig(&utils.BackupConfig{DataOnly: true})
			filterList = []string{"schema1.table1"}
			restore.ValidateFilterRelationsInBackupSet(filterList)
		})
		It("relation does not exist in backup but function with same name does", func() {
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
	})
	Describe("ValidateDatabaseExistence", func() {
		BeforeEach(func() {
		})
		It("fails if createdb passed when db exists", func() {
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
		It("tells user to manually create db when db does not exist and filtered", func() {
			db_exists := sqlmock.NewRows([]string{"string"}).
				AddRow("false")
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(db_exists)
			defer testhelper.ShouldPanicWithMessage(`Database "testdb" must be created manually`)
			restore.ValidateDatabaseExistence("testdb", true, true)
		})
		It("tells user to pass --create-db when db does not exist, not filtered, and no --create-db", func() {
			db_exists := sqlmock.NewRows([]string{"string"}).
				AddRow("false")
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(db_exists)
			defer testhelper.ShouldPanicWithMessage(`Database "testdb" does not exist. Use the --create-db flag`)
			restore.ValidateDatabaseExistence("testdb", false, false)
		})
	})
})
