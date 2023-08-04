package backup_test

import (
	"strings"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/options"
	"github.com/spf13/cobra"

	. "github.com/onsi/ginkgo/v2"
)

var _ = Describe("backup/validate tests", func() {
	var filterList []string
	AfterEach(func() {
		filterList = []string{}
	})
	Describe("ValidateSchemasExist", func() {
		It("passes if there are no filter schemas", func() {
			backup.ValidateSchemasExist(connectionPool, filterList, false)
		})
		It("passes if single schema is present in database", func() {
			singleSchemaRow := sqlmock.NewRows([]string{"string"}).
				AddRow("schema1")
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(singleSchemaRow)
			filterList = []string{"schema1"}
			backup.ValidateSchemasExist(connectionPool, filterList, false)
		})
		It("passes if multiple schemas are present in database", func() {
			twoSchemaRows := sqlmock.NewRows([]string{"string"}).
				AddRow("schema1").AddRow("schema2")
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(twoSchemaRows)
			filterList = []string{"schema1", "schema2"}
			backup.ValidateSchemasExist(connectionPool, filterList, false)
		})
		It("panics if schema is not present in database", func() {
			twoSchemaRows := sqlmock.NewRows([]string{"string"}).
				AddRow("schema1")
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(twoSchemaRows)
			filterList = []string{"schema1", "schema2"}
			defer testhelper.ShouldPanicWithMessage("Schema schema2 does not exist")
			backup.ValidateSchemasExist(connectionPool, filterList, false)
		})
		It("panics if all include-schema is not present in database", func() {
			twoSchemaRows := sqlmock.NewRows([]string{"string"})
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(twoSchemaRows)
			filterList = []string{"schema2", "schema3"}
			defer testhelper.ShouldPanicWithMessage("Schema schema2 does not exist")
			backup.ValidateSchemasExist(connectionPool, filterList, false)
		})
		It("does not panic if exclude schema is not present in database", func() {
			_, _, logfile = testhelper.SetupTestLogger()
			twoSchemaRows := sqlmock.NewRows([]string{"string"}).
				AddRow("schema1")
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(twoSchemaRows)
			filterList = []string{"schema1", "schema2"}
			backup.ValidateSchemasExist(connectionPool, filterList, true)
			testhelper.ExpectRegexp(logfile, "[WARNING]:-Excluded schema schema2 does not exist")
		})
	})
	Describe("ValidateTablesExist", func() {
		var tableRows, partitionTables, schemaAndTable, schemaAndTable2 *sqlmock.Rows
		BeforeEach(func() {
			tableRows = sqlmock.NewRows([]string{"oid", "name"})
			schemaAndTable = sqlmock.NewRows([]string{"schemaname", "tablename"})
			schemaAndTable2 = sqlmock.NewRows([]string{"schemaname", "tablename"})
			partitionTables = sqlmock.NewRows([]string{"oid", "level", "rootname"})
		})
		Context("Non-partition tables", func() {
			It("passes if there are no filter tables", func() {
				backup.ValidateTablesExist(connectionPool, filterList, false)
			})
			It("passes if single table is present in database", func() {
				// Added to handle call to `quote_ident`
				schemaAndTable.AddRow("public", "table1")
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(schemaAndTable)
				tableRows.AddRow("1", "public.table1")
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(tableRows)
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(partitionTables)
				filterList = []string{"public.table1"}
				backup.ValidateTablesExist(connectionPool, filterList, false)
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
				backup.ValidateTablesExist(connectionPool, filterList, false)
			})
			It("panics if include table is not present in database", func() {
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
				backup.ValidateTablesExist(connectionPool, filterList, false)
			})
			It("does not panic if exclude table is not present in database", func() {
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
				backup.ValidateTablesExist(connectionPool, filterList, true)
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
				backup.ValidateTablesExist(connectionPool, filterList, false)
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
				_ = cmdFlags.Set(options.LEAF_PARTITION_DATA, "true")
				backup.ValidateTablesExist(connectionPool, filterList, false)
			})
			It("panics if given an intermediate partition table and --leaf-partition-data is set", func() {
				// Added to handle call to `quote_ident`
				schemaAndTable.AddRow("public", "table1")
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(schemaAndTable)
				//
				_ = cmdFlags.Set(options.LEAF_PARTITION_DATA, "true")
				tableRows.AddRow("1", "public.table1")
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(tableRows)
				partitionTables.AddRow("1", "i", "root")
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(partitionTables)
				filterList = []string{"public.table1"}
				defer testhelper.ShouldPanicWithMessage("Cannot filter on public.table1, as it is an intermediate partition table.  Only parent partition tables and leaf partition tables may be specified.")
				backup.ValidateTablesExist(connectionPool, filterList, false)
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
				backup.ValidateTablesExist(connectionPool, filterList, false)
			})
			It("panics if given a leaf partition table and --leaf-partition-data is not set", func() {
				// Added to handle call to `quote_ident`
				schemaAndTable.AddRow("public", "table1")
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(schemaAndTable)
				//
				tableRows.AddRow("1", "public.table1")
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(tableRows)
				partitionTables.AddRow("1", "l", "root")
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(partitionTables)
				filterList = []string{"public.table1"}
				defer testhelper.ShouldPanicWithMessage("--leaf-partition-data flag must be specified to filter on public.table1, as it is a leaf partition table.")
				backup.ValidateTablesExist(connectionPool, filterList, false)
			})
		})
	})
	Describe("Validate various flag combinations that are required or exclusive", func() {
		DescribeTable("Validate various flag combinations that are required or exclusive",
			func(argString string, valid bool) {
				testCmd := &cobra.Command{
					Use:  "flag validation",
					Args: cobra.NoArgs,
					Run: func(cmd *cobra.Command, args []string) {
						backup.DoFlagValidation(cmd)
					}}
				testCmd.SetArgs(strings.Split(argString, " "))
				backup.SetCmdFlags(testCmd.Flags())

				if !valid {
					defer testhelper.ShouldPanicWithMessage("CRITICAL")
				}

				err := testCmd.Execute()
				if err != nil && valid {
					Fail("Valid flag combination failed validation check")
				}
			},
			Entry("--backup-dir combo", "--backup-dir /tmp --plugin-config /tmp/config", false),

			/*
			 * Below are all the different filter combinations
			 */
			// --exclude-schema combinations with other filters
			Entry("--exclude-schema combos", "--exclude-schema schema1 --include-table schema.table2", false),
			Entry("--exclude-schema combos", "--exclude-schema schema1 --include-table-file /tmp/file2", false),
			Entry("--exclude-schema combos", "--exclude-schema schema1 --include-schema schema2", false),
			Entry("--exclude-schema combos", "--exclude-schema schema1 --include-schema-file /tmp/file2", false),
			Entry("--exclude-schema combos", "--exclude-schema schema1 --exclude-table schema.table2", false),
			Entry("--exclude-schema combos", "--exclude-schema schema1 --exclude-table-file /tmp/file2", false),
			Entry("--exclude-schema combos", "--exclude-schema schema1 --exclude-schema schema2", true),
			Entry("--exclude-schema combos", "--exclude-schema schema1 --exclude-schema-file /tmp/file2", false),

			// --exclude-schema-file combinations with other filters
			Entry("--exclude-schema-file combos", "--exclude-schema-file /tmp/file --include-table schema.table2", false),
			Entry("--exclude-schema-file combos", "--exclude-schema-file /tmp/file --include-table-file /tmp/file2", false),
			Entry("--exclude-schema-file combos", "--exclude-schema-file /tmp/file --include-schema schema2", false),
			Entry("--exclude-schema-file combos", "--exclude-schema-file /tmp/file --include-schema-file /tmp/file2", false),
			Entry("--exclude-schema-file combos", "--exclude-schema-file /tmp/file --exclude-table schema.table2", false),
			Entry("--exclude-schema-file combos", "--exclude-schema-file /tmp/file --exclude-table-file /tmp/file2", false),

			// --exclude-table combinations with other filters
			Entry("--exclude-table combos", "--exclude-table schema.table --include-table schema.table2", false),
			Entry("--exclude-table combos", "--exclude-table schema.table --include-table-file /tmp/file2", false),
			Entry("--exclude-table combos", "--exclude-table schema.table --include-schema schema2", true),         // TODO: Verify this.
			Entry("--exclude-table combos", "--exclude-table schema.table --include-schema-file /tmp/file2", true), // TODO: Verify this.
			Entry("--exclude-table combos", "--exclude-table schema.table --exclude-table schema.table2", true),
			Entry("--exclude-table combos", "--exclude-table schema.table --exclude-table-file /tmp/file2", false),

			// --exclude-table-file combinations with other filters
			Entry("--exclude-table-file combos", "--exclude-table-file /tmp/file --include-table schema.table2", false),
			Entry("--exclude-table-file combos", "--exclude-table-file /tmp/file --include-table-file /tmp/file2", false),
			Entry("--exclude-table-file combos", "--exclude-table-file /tmp/file --include-schema schema2", true),         // TODO: Verify this.
			Entry("--exclude-table-file combos", "--exclude-table-file /tmp/file --include-schema-file /tmp/file2", true), // TODO: Verify this.

			// --include-schema combinations with other filters
			Entry("--include-schema combos", "--include-schema schema1 --include-table schema.table2", false),
			Entry("--include-schema combos", "--include-schema schema1 --include-table-file /tmp/file2", false),
			Entry("--include-schema combos", "--include-schema schema1 --include-schema schema2", true),
			Entry("--include-schema combos", "--include-schema schema1 --include-schema-file /tmp/file2", false),

			// --include-schema-file combinations with other filters
			Entry("--include-schema-file combos", "--include-schema-file /tmp/file --include-table schema.table2", false),
			Entry("--include-schema-file combos", "--include-schema-file /tmp/file --include-table-file /tmp/file2", false),

			// --include-table combinations with other filters
			Entry("--include-table combos", "--include-table schema.table --include-table schema.table2", true),
			Entry("--include-table combos", "--include-table schema.table --include-table-file /tmp/file2", false),

			/*
			 * Below are various different incremental combinations
			 */
			Entry("incremental combos", "--incremental", false),
			Entry("incremental combos", "--incremental --leaf-partition-data", true),
			Entry("incremental combos", "--incremental --from-timestamp 20211507152558", false),
			Entry("incremental combos", "--incremental --from-timestamp 20211507152558 --leaf-partition-data", true),
			Entry("incremental combos", "--incremental --leaf-partition-data --data-only", false),
			Entry("incremental combos", "--incremental --leaf-partition-data --metadata-only", false),

			/*
			 * Below are various different jobs combinations
			 */
			Entry("jobs combos", "--jobs 2 --metadata-only", false),
			Entry("jobs combos", "--jobs 2 --single-data-file", false),
			Entry("jobs combos", "--jobs 2 --plugin-config /tmp/file", true),
			Entry("jobs combos", "--jobs 2 --data-only", true),
		)
	})
})
