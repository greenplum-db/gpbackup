package backup_test

import (
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/utils"
	sqlmock "github.com/DATA-DOG/go-sqlmock"

	. "github.com/onsi/ginkgo"
)

var _ = Describe("backup/validate tests", func() {
	var filterList []string
	AfterEach(func() {
		filterList = []string{}
	})
	Describe("ValidateFilterSchemas", func() {
		It("passes if there are no filter schemas", func() {
			backup.ValidateFilterSchemas(connectionPool, filterList, false)
		})
		It("passes if single schema is present in database", func() {
			single_schema_row := sqlmock.NewRows([]string{"string"}).
				AddRow("schema1")
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(single_schema_row)
			filterList = []string{"schema1"}
			backup.ValidateFilterSchemas(connectionPool, filterList, false)
		})
		It("passes if multiple schemas are present in database", func() {
			two_schema_rows := sqlmock.NewRows([]string{"string"}).
				AddRow("schema1").AddRow("schema2")
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(two_schema_rows)
			filterList = []string{"schema1", "schema2"}
			backup.ValidateFilterSchemas(connectionPool, filterList, false)
		})
		It("panics if schema is not present in database", func() {
			two_schema_rows := sqlmock.NewRows([]string{"string"}).
				AddRow("schema1")
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(two_schema_rows)
			filterList = []string{"schema1", "schema2"}
			defer testhelper.ShouldPanicWithMessage("Schema schema2 does not exist")
			backup.ValidateFilterSchemas(connectionPool, filterList, false)
		})
		It("panics if all include-schema is not present in database", func() {
			two_schema_rows := sqlmock.NewRows([]string{"string"})
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(two_schema_rows)
			filterList = []string{"schema2", "schema3"}
			defer testhelper.ShouldPanicWithMessage("Schema schema2 does not exist")
			backup.ValidateFilterSchemas(connectionPool, filterList, false)
		})
		It("does not panic if schema is not present in database and noFatal is true", func() {
			_, _, logfile = testhelper.SetupTestLogger()
			two_schema_rows := sqlmock.NewRows([]string{"string"}).
				AddRow("schema1")
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(two_schema_rows)
			filterList = []string{"schema1", "schema2"}
			backup.ValidateFilterSchemas(connectionPool, filterList, true)
			testhelper.ExpectRegexp(logfile, "[WARNING]:-Excluded schema schema2 does not exist")
		})
	})
	Describe("ValidateFilterTables", func() {
		var tableRows, partitionTables, schemaAndTable, schemaAndTable2 *sqlmock.Rows
		BeforeEach(func() {
			tableRows = sqlmock.NewRows([]string{"oid", "name"})
			schemaAndTable = sqlmock.NewRows([]string{"schemaname", "tablename"})
			schemaAndTable2 = sqlmock.NewRows([]string{"schemaname", "tablename"})
			partitionTables = sqlmock.NewRows([]string{"oid", "level", "rootname"})
		})
		Context("Non-partition tables", func() {
			It("passes if there are no filter tables", func() {
				backup.ValidateFilterTables(connectionPool, filterList, false)
			})
			It("passes if single table is present in database", func() {
				// Added to handle call to `quote_ident`
				schemaAndTable.AddRow("public", "table1")
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(schemaAndTable)
				//
				tableRows.AddRow("1", "public.table1")
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(tableRows)
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(partitionTables)
				filterList = []string{"public.table1"}
				backup.ValidateFilterTables(connectionPool, filterList, false)
			})
			It("passes if multiple tables are present in database", func() {
				// Added to handle call to `quote_ident`
				schemaAndTable.AddRow("public", "table1")
				schemaAndTable2.AddRow("public", "table2")
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(schemaAndTable)
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(schemaAndTable2)
				//
				tableRows.AddRow("1", "public.table1").AddRow("2", "public.table2")
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(tableRows)
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(partitionTables)
				filterList = []string{"public.table1", "public.table2"}
				backup.ValidateFilterTables(connectionPool, filterList, false)
			})
			It("panics if table is not present in database", func() {
				// Added to handle call to `quote_ident`
				schemaAndTable.AddRow("public", "table1")
				schemaAndTable2.AddRow("public", "table2")
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(schemaAndTable)
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(schemaAndTable2)
				//
				tableRows.AddRow("1", "public.table1")
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(tableRows)
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(partitionTables)
				filterList = []string{"public.table1", "public.table2"}
				defer testhelper.ShouldPanicWithMessage("Table public.table2 does not exist")
				backup.ValidateFilterTables(connectionPool, filterList, false)
			})
			It("does not panic if table is not present in database and noFatal is true", func() {
				// Added to handle call to `quote_ident`
				schemaAndTable.AddRow("public", "table1")
				schemaAndTable2.AddRow("public", "table2")
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(schemaAndTable)
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(schemaAndTable2)
				//
				_, _, logfile = testhelper.SetupTestLogger()
				tableRows.AddRow("1", "public.table1")
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(tableRows)
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(partitionTables)
				filterList = []string{"public.table1", "public.table2"}
				backup.ValidateFilterTables(connectionPool, filterList, true)
				testhelper.ExpectRegexp(logfile, "[WARNING]:-Excluded table public.table2 does not exist")
			})
		})
		Context("Partition tables", func() {
			It("passes if given a parent partition table", func() {
				// Added to handle call to `quote_ident`
				schemaAndTable.AddRow("public", "table1")
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(schemaAndTable)
				//
				tableRows.AddRow("1", "public.table1")
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(tableRows)
				partitionTables.AddRow("1", "p", "")
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(partitionTables)
				filterList = []string{"public.table1"}
				backup.ValidateFilterTables(connectionPool, filterList, false)
			})
			It("passes if given a leaf partition table", func() {
				// Added to handle call to `quote_ident`
				schemaAndTable.AddRow("public", "table1")
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(schemaAndTable)
				//
				tableRows.AddRow("1", "public.table1")
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(tableRows)
				partitionTables.AddRow("1", "l", "root")
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(partitionTables)
				filterList = []string{"public.table1"}
				backup.ValidateFilterTables(connectionPool, filterList, false)
			})
			It("panics if given an intermediate partition table and --leaf-partition-data is set", func() {
				// Added to handle call to `quote_ident`
				schemaAndTable.AddRow("public", "table1")
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(schemaAndTable)
				//
				cmdFlags.Set(utils.LEAF_PARTITION_DATA, "true")
				tableRows.AddRow("1", "public.table1")
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(tableRows)
				partitionTables.AddRow("1", "i", "root")
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(partitionTables)
				filterList = []string{"public.table1"}
				defer testhelper.ShouldPanicWithMessage("Cannot filter on public.table1, as it is an intermediate partition table.  Only parent partition tables and leaf partition tables may be specified.")
				backup.ValidateFilterTables(connectionPool, filterList, false)
			})
			It("panics if given an intermediate partition table and --leaf-partition-data is not set", func() {
				// Added to handle call to `quote_ident`
				schemaAndTable.AddRow("public", "table1")
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(schemaAndTable)
				//
				tableRows.AddRow("1", "public.table1")
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(tableRows)
				partitionTables.AddRow("1", "i", "root")
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(partitionTables)
				filterList = []string{"public.table1"}
				defer testhelper.ShouldPanicWithMessage("Cannot filter on public.table1, as it is an intermediate partition table.  Only parent partition tables and leaf partition tables may be specified.")
				backup.ValidateFilterTables(connectionPool, filterList, false)
			})
		})
	})
	Describe("ValidateCompressionLevel", func() {
		It("validates a compression level between 1 and 9", func() {
			compressLevel := 5
			backup.ValidateCompressionLevel(compressLevel)
		})
		It("panics if given a compression level < 1", func() {
			compressLevel := 0
			defer testhelper.ShouldPanicWithMessage("Compression level must be between 1 and 9")
			backup.ValidateCompressionLevel(compressLevel)
		})
		It("panics if given a compression level > 9", func() {
			compressLevel := 11
			defer testhelper.ShouldPanicWithMessage("Compression level must be between 1 and 9")
			backup.ValidateCompressionLevel(compressLevel)
		})
	})
})
