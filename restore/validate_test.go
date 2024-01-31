package restore_test

import (
	"strings"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/history"
	"github.com/greenplum-db/gpbackup/options"
	"github.com/greenplum-db/gpbackup/restore"
	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/toc"
	"github.com/greenplum-db/gpbackup/utils"
	"github.com/spf13/cobra"

	. "github.com/onsi/ginkgo/v2"
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
		sequence := toc.StatementWithType{ObjectType: toc.OBJ_SEQUENCE, Statement: "CREATE SEQUENCE schema.somesequence"}
		sequenceLen := uint64(len(sequence.Statement))
		table1 := toc.StatementWithType{ObjectType: toc.OBJ_TABLE, Statement: "CREATE TABLE schema1.table1"}
		table1Len := uint64(len(table1.Statement))
		table2 := toc.StatementWithType{ObjectType: toc.OBJ_TABLE, Statement: "CREATE TABLE schema2.table2"}
		table2Len := uint64(len(table2.Statement))
		BeforeEach(func() {
			tocfile, backupfile = testutils.InitializeTestTOC(buffer, "predata")
			backupfile.ByteCount = table1Len
			tocfile.AddMetadataEntry("predata", toc.MetadataEntry{Schema: "schema1", Name: "table1", ObjectType: toc.OBJ_TABLE}, 0, backupfile.ByteCount, []uint32{0, 0})
			tocfile.AddCoordinatorDataEntry("schema1", "table1", 1, "(i)", 0, "", "")
			backupfile.ByteCount += table2Len
			tocfile.AddMetadataEntry("predata", toc.MetadataEntry{Schema: "schema2", Name: "table2", ObjectType: toc.OBJ_TABLE}, table1Len, backupfile.ByteCount, []uint32{0, 0})
			tocfile.AddCoordinatorDataEntry("schema2", "table2", 2, "(j)", 0, "", "")
			backupfile.ByteCount += sequenceLen
			tocfile.AddMetadataEntry("predata", toc.MetadataEntry{Schema: "schema", Name: "somesequence", ObjectType: toc.OBJ_SEQUENCE}, table1Len+table2Len, backupfile.ByteCount, []uint32{0, 0})
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
			tocfile.AddCoordinatorDataEntry("s1", "table1", 1, "(j)", 0, "", "")
			tocfile.AddCoordinatorDataEntry("s1", "table2", 2, "(j)", 0, "", "")
			tocfile.AddCoordinatorDataEntry("s2", "table1", 3, "(j)", 0, "", "")
			tocfile.AddCoordinatorDataEntry("s2", "table2", 4, "(j)", 0, "", "")
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
			tocfile.AddMetadataEntry("predata", toc.MetadataEntry{Schema: "schema1", Name: "table1", ObjectType: toc.OBJ_TABLE}, 0, backupfile.ByteCount, []uint32{0, 0})
			tocfile.AddCoordinatorDataEntry("schema1", "table1", 1, "(i)", 0, "", "")

			tocfile.AddMetadataEntry("predata", toc.MetadataEntry{Schema: "schema2", Name: "table2", ObjectType: toc.OBJ_TABLE}, 0, backupfile.ByteCount, []uint32{0, 0})
			tocfile.AddCoordinatorDataEntry("schema2", "table2", 2, "(j)", 0, "", "")

			tocfile.AddMetadataEntry("predata", toc.MetadataEntry{Schema: "schema1", Name: "somesequence", ObjectType: toc.OBJ_SEQUENCE}, 0, backupfile.ByteCount, []uint32{0, 0})
			tocfile.AddMetadataEntry("predata", toc.MetadataEntry{Schema: "schema1", Name: "someview", ObjectType: toc.OBJ_VIEW}, 0, backupfile.ByteCount, []uint32{0, 0})
			tocfile.AddMetadataEntry("predata", toc.MetadataEntry{Schema: "schema1", Name: "somefunction", ObjectType: toc.OBJ_FUNCTION}, 0, backupfile.ByteCount, []uint32{0, 0})

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
	Describe("Validate various flag combinations that are required or exclusive", func() {
		DescribeTable("Validate various flag combinations that are required or exclusive",
			func(argString string, valid bool) {
				testCmd := &cobra.Command{
					Use:  "flag validation",
					Args: cobra.NoArgs,
					Run: func(cmd *cobra.Command, args []string) {
						restore.ValidateFlagCombinations(cmd.Flags())
					}}
				testCmd.SetArgs(strings.Split(argString, " "))
				restore.SetCmdFlags(testCmd.Flags())

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
			Entry("--exclude-schema combos", "--timestamp=0 --exclude-schema schema1 --include-table schema.table2", false),
			Entry("--exclude-schema combos", "--timestamp=0 --exclude-schema schema1 --include-table-file /tmp/file2", false),
			Entry("--exclude-schema combos", "--timestamp=0 --exclude-schema schema1 --include-schema schema2", false),
			Entry("--exclude-schema combos", "--timestamp=0 --exclude-schema schema1 --include-schema-file /tmp/file2", true), // TODO: Verify this.
			Entry("--exclude-schema combos", "--timestamp=0 --exclude-schema schema1 --exclude-table schema.table2", false),
			Entry("--exclude-schema combos", "--timestamp=0 --exclude-schema schema1 --exclude-table-file /tmp/file2", false),
			Entry("--exclude-schema combos", "--timestamp=0 --exclude-schema schema1 --exclude-schema schema2", true),
			Entry("--exclude-schema combos", "--timestamp=0 --exclude-schema schema1 --exclude-schema-file /tmp/file2", true), // TODO: Verify this.

			// --exclude-schema-file combinations with other filters
			Entry("--exclude-schema-file combos", "--timestamp=0 --exclude-schema-file /tmp/file --include-table schema.table2", true),    // TODO: Verify this.
			Entry("--exclude-schema-file combos", "--timestamp=0 --exclude-schema-file /tmp/file --include-table-file /tmp/file2", true),  // TODO: Verify this.
			Entry("--exclude-schema-file combos", "--timestamp=0 --exclude-schema-file /tmp/file --include-schema schema2", true),         // TODO: Verify this.
			Entry("--exclude-schema-file combos", "--timestamp=0 --exclude-schema-file /tmp/file --include-schema-file /tmp/file2", true), // TODO: Verify this.
			Entry("--exclude-schema-file combos", "--timestamp=0 --exclude-schema-file /tmp/file --exclude-table schema.table2", true),    // TODO: Verify this.
			Entry("--exclude-schema-file combos", "--timestamp=0 --exclude-schema-file /tmp/file --exclude-table-file /tmp/file2", true),  // TODO: Verify this.

			// --exclude-table combinations with other filters
			Entry("--exclude-table combos", "--timestamp=0 --exclude-table schema.table --include-table schema.table2", false),
			Entry("--exclude-table combos", "--timestamp=0 --exclude-table schema.table --include-table-file /tmp/file2", false),
			Entry("--exclude-table combos", "--timestamp=0 --exclude-table schema.table --include-schema schema2", true),
			Entry("--exclude-table combos", "--timestamp=0 --exclude-table schema.table --include-schema-file /tmp/file2", true),
			Entry("--exclude-table combos", "--timestamp=0 --exclude-table schema.table --exclude-table schema.table2", true),
			Entry("--exclude-table combos", "--timestamp=0 --exclude-table schema.table --exclude-table-file /tmp/file2", false),

			// --exclude-table-file combinations with other filters
			Entry("--exclude-table-file combos", "--timestamp=0 --exclude-table-file /tmp/file --include-table schema.table2", false),
			Entry("--exclude-table-file combos", "--timestamp=0 --exclude-table-file /tmp/file --include-table-file /tmp/file2", false),
			Entry("--exclude-table-file combos", "--timestamp=0 --exclude-table-file /tmp/file --include-schema schema2", true),
			Entry("--exclude-table-file combos", "--timestamp=0 --exclude-table-file /tmp/file --include-schema-file /tmp/file2", true),

			// --include-schema combinations with other filters
			Entry("--include-schema combos", "--timestamp=0 --include-schema schema1 --include-table schema.table2", false),
			Entry("--include-schema combos", "--timestamp=0 --include-schema schema1 --include-table-file /tmp/file2", false),
			Entry("--include-schema combos", "--timestamp=0 --include-schema schema1 --include-schema schema2", true),
			Entry("--include-schema combos", "--timestamp=0 --include-schema schema1 --include-schema-file /tmp/file2", true), // TODO: Verify this.

			// --include-schema-file combinations with other filters
			Entry("--include-schema-file combos", "--timestamp=0 --include-schema-file /tmp/file --include-table schema.table2", true),   // TODO: Verify this.
			Entry("--include-schema-file combos", "--timestamp=0 --include-schema-file /tmp/file --include-table-file /tmp/file2", true), // TODO: Verify this.

			// --include-table combinations with other filters
			Entry("--include-table combos", "--timestamp=0 --include-table schema.table --include-table schema.table2", true),
			Entry("--include-table combos", "--timestamp=0 --include-table schema.table --include-table-file /tmp/file2", false),

			/*
			 * Below are various different incremental combinations
			 */
			Entry("incremental combos", "--timestamp=0 --incremental", false),
			Entry("incremental combos", "--timestamp=0 --incremental --data-only", true),

			/*
			 * Below are various different truncate combinations
			 */
			Entry("truncate combos", "--timestamp=0 --truncate-table", false),
			Entry("truncate combos", "--timestamp=0 --truncate-table --include-table schema.table2", true),
			Entry("truncate combos", "--timestamp=0 --truncate-table --include-table-file /tmp/file2", true),
			Entry("truncate combos", "--timestamp=0 --truncate-table --include-table schema.table2 --redirect-db foodb", true),
			Entry("truncate combos", "--timestamp=0 --truncate-table --include-table schema.table2 --redirect-schema schema2", false),

			/*
			 * Below are various different redirect-schema combinations
			 */
			Entry("--redirect-schema combos", "--timestamp=0 --redirect-schema schema1", false),
			Entry("--redirect-schema combos", "--timestamp=0 --redirect-schema schema1 --include-table schema.table2", true),
			Entry("--redirect-schema combos", "--timestamp=0 --redirect-schema schema1 --include-table-file /tmp/file2", true),
			Entry("--redirect-schema combos", "--timestamp=0 --redirect-schema schema1 --include-schema schema2", true),
			Entry("--redirect-schema combos", "--timestamp=0 --redirect-schema schema1 --include-schema-file /tmp/file2", true),
			Entry("--redirect-schema combos", "--timestamp=0 --redirect-schema schema1 --exclude-table schema.table2", false),
			Entry("--redirect-schema combos", "--timestamp=0 --redirect-schema schema1 --exclude-table-file /tmp/file2", false),
			Entry("--redirect-schema combos", "--timestamp=0 --redirect-schema schema1 --exclude-schema schema2", false),
			Entry("--redirect-schema combos", "--timestamp=0 --redirect-schema schema1 --exclude-schema-file /tmp/file2", false),
			Entry("--redirect-schema combos", "--timestamp=0 --redirect-schema schema1 --include-table schema.table2 --metadata-only", true),
			Entry("--redirect-schema combos", "--timestamp=0 --redirect-schema schema1 --include-table schema.table2 --data-only", true),
		)
	})
	Describe("ValidateBackupFlagCombinations", func() {
		It("restore with copy-queue-size should fatal if backup was not taken with single-data-file", func() {
			restore.SetBackupConfig(&history.BackupConfig{SingleDataFile: false})
			testCmd := &cobra.Command{
				Use:  "flag validation",
				Args: cobra.NoArgs,
				Run: func(cmd *cobra.Command, args []string) {
					restore.ValidateBackupFlagCombinations()
				}}
			testCmd.SetArgs([]string{"--copy-queue-size", "4"})
			restore.SetCmdFlags(testCmd.Flags())

			defer testhelper.ShouldPanicWithMessage("CRITICAL")
			err := testCmd.Execute()
			if err == nil {
				Fail("invalid flag combination passed validation check")
			}
		})
	})
})
