package restore_test

import (
	"github.com/greenplum-db/gpbackup/restore"
	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/utils"
	sqlmock "gopkg.in/DATA-DOG/go-sqlmock.v1"

	. "github.com/onsi/ginkgo"
)

var _ = Describe("restore/validate tests", func() {
	var filterList []string
	AfterEach(func() {
		filterList = []string{}
	})
	Describe("ValidateFilterSchemasInRestoreDatabase", func() {
		It("passes if there are no filter schemas", func() {
			restore.ValidateFilterSchemasInRestoreDatabase(connection, filterList)
		})
		It("passes if schema is not present in database", func() {
			no_schema_rows := sqlmock.NewRows([]string{"string"})
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(no_schema_rows)
			filterList = []string{"schema2"}
			restore.ValidateFilterSchemasInRestoreDatabase(connection, filterList)
		})
		It("panics if single schema is present in database", func() {
			single_schema_row := sqlmock.NewRows([]string{"string"}).
				AddRow("schema1")
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(single_schema_row)
			filterList = []string{"schema1"}
			defer testutils.ShouldPanicWithMessage("Schema schema1 already exists")
			restore.ValidateFilterSchemasInRestoreDatabase(connection, filterList)
		})
		It("panics if multiple schemas are present in database", func() {
			two_schema_rows := sqlmock.NewRows([]string{"string"}).
				AddRow("schema1").AddRow("schema2")
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(two_schema_rows)
			filterList = []string{"schema1", "schema2"}
			defer testutils.ShouldPanicWithMessage("Schema schema1 already exists")
			restore.ValidateFilterSchemasInRestoreDatabase(connection, filterList)
		})
	})
	Describe("ValidateFilterSchemasInRestoreDatabase", func() {
		sequence := utils.StatementWithType{ObjectType: "SEQUENCE", Statement: "CREATE SEQUENCE schema.somesequence"}
		sequenceLen := uint64(len(sequence.Statement))
		table1 := utils.StatementWithType{ObjectType: "TABLE", Statement: "CREATE TABLE schema1.table1"}
		table1Len := uint64(len(table1.Statement))
		table2 := utils.StatementWithType{ObjectType: "TABLE", Statement: "CREATE TABLE schema2.table2"}
		table2Len := uint64(len(table2.Statement))
		var toc *utils.TOC
		var backupfile *utils.FileWithByteCount
		BeforeEach(func() {
			toc, backupfile = testutils.InitializeTestTOC(buffer, "predata")
			backupfile.ByteCount = table1Len
			toc.AddMetadataEntry("schema1", "table1", "TABLE", 0, backupfile)
			toc.AddMasterDataEntry("schema1", "table1", 1, "(i)")
			backupfile.ByteCount += table2Len
			toc.AddMetadataEntry("schema2", "table2", "TABLE", table1Len, backupfile)
			toc.AddMasterDataEntry("schema2", "table2", 2, "(j)")
			backupfile.ByteCount += sequenceLen
			toc.AddMetadataEntry("schema", "somesequence", "SEQUENCE", table1Len+table2Len, backupfile)
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
			defer testutils.ShouldPanicWithMessage("Could not find the following schema(s) in the backup set: schema3")
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
			defer testutils.ShouldPanicWithMessage("Could not find the following schema(s) in the backup set: schema3")
			restore.ValidateFilterSchemasInBackupSet(filterList)
		})
	})
	Describe("ValidateFilterTablesInRestoreDatabase", func() {
		It("passes if there are no filter tables", func() {
			restore.ValidateFilterTablesInRestoreDatabase(connection, filterList)
		})
		It("passes if table is not present in database", func() {
			no_table_rows := sqlmock.NewRows([]string{"string"})
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(no_table_rows)
			filterList = []string{"public.table2"}
			restore.ValidateFilterTablesInRestoreDatabase(connection, filterList)
		})
		It("panics if single table is present in database", func() {
			single_table_row := sqlmock.NewRows([]string{"string"}).
				AddRow("public.table1")
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(single_table_row)
			filterList = []string{"public.table1"}
			defer testutils.ShouldPanicWithMessage("Table public.table1 already exists")
			restore.ValidateFilterTablesInRestoreDatabase(connection, filterList)
		})
		It("panics if multiple tables are present in database", func() {
			two_table_rows := sqlmock.NewRows([]string{"string"}).
				AddRow("public.table1").AddRow("public.table2")
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(two_table_rows)
			filterList = []string{"public.table1", "public.table2"}
			defer testutils.ShouldPanicWithMessage("Table public.table1 already exists")
			restore.ValidateFilterTablesInRestoreDatabase(connection, filterList)
		})
	})
	Describe("ValidateFilterTablesInRestoreDatabase", func() {
		sequence := utils.StatementWithType{ObjectType: "SEQUENCE", Statement: "CREATE SEQUENCE schema1.somesequence"}
		sequenceLen := uint64(len(sequence.Statement))
		table1 := utils.StatementWithType{ObjectType: "TABLE", Statement: "CREATE TABLE schema1.table1"}
		table1Len := uint64(len(table1.Statement))
		table2 := utils.StatementWithType{ObjectType: "TABLE", Statement: "CREATE TABLE schema2.table2"}
		table2Len := uint64(len(table2.Statement))
		var toc *utils.TOC
		var backupfile *utils.FileWithByteCount
		BeforeEach(func() {
			toc, backupfile = testutils.InitializeTestTOC(buffer, "predata")
			backupfile.ByteCount = table1Len
			toc.AddMetadataEntry("schema1", "table1", "TABLE", 0, backupfile)
			toc.AddMasterDataEntry("schema1", "table1", 1, "(i)")
			backupfile.ByteCount += table2Len
			toc.AddMetadataEntry("schema2", "table2", "TABLE", table1Len, backupfile)
			toc.AddMasterDataEntry("schema2", "table2", 2, "(j)")
			backupfile.ByteCount += sequenceLen
			toc.AddMetadataEntry("schema1", "somesequence", "SEQUENCE", table1Len+table2Len, backupfile)
			restore.SetTOC(toc)
		})
		It("table exists in normal backup", func() {
			restore.SetBackupConfig(&utils.BackupConfig{})
			filterList = []string{"schema1.table1"}
			restore.ValidateFilterTablesInBackupSet(filterList)
		})
		It("table does not exist in normal backup", func() {
			restore.SetBackupConfig(&utils.BackupConfig{})
			filterList = []string{"schema1.table3"}
			defer testutils.ShouldPanicWithMessage("Could not find the following table(s) in the backup set: schema1.table3")
			restore.ValidateFilterTablesInBackupSet(filterList)
		})
		It("table does not exist in normal backup but sequence with same name exists", func() {
			restore.SetBackupConfig(&utils.BackupConfig{})
			filterList = []string{"schema1.somesequence"}
			defer testutils.ShouldPanicWithMessage("Could not find the following table(s) in the backup set: schema1.somesequence")
			restore.ValidateFilterTablesInBackupSet(filterList)
		})
		It("table exists in data-only backup", func() {
			restore.SetBackupConfig(&utils.BackupConfig{DataOnly: true})
			filterList = []string{"schema1.table1"}
			restore.ValidateFilterTablesInBackupSet(filterList)
		})
		It("table does not exist in data-only backup", func() {
			restore.SetBackupConfig(&utils.BackupConfig{DataOnly: true})
			filterList = []string{"schema1.table3"}
			defer testutils.ShouldPanicWithMessage("Could not find the following table(s) in the backup set: schema1.table3")
			restore.ValidateFilterTablesInBackupSet(filterList)
		})
	})
})
