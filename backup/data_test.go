package backup_test

import (
	"fmt"
	"regexp"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/history"
	"github.com/greenplum-db/gpbackup/options"
	"github.com/greenplum-db/gpbackup/report"
	"github.com/greenplum-db/gpbackup/toc"
	"github.com/greenplum-db/gpbackup/utils"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup/data tests", func() {
	Describe("ConstructTableAttributesList", func() {
		It("creates an attribute list for a table with one column", func() {
			columnDefs := []backup.ColumnDefinition{{Name: "a"}}
			atts := backup.ConstructTableAttributesList(columnDefs)
			Expect(atts).To(Equal("(a)"))
		})
		It("creates an attribute list for a table with multiple columns", func() {
			columnDefs := []backup.ColumnDefinition{{Name: "a"}, {Name: "b"}}
			atts := backup.ConstructTableAttributesList(columnDefs)
			Expect(atts).To(Equal("(a,b)"))
		})
		It("creates an attribute list for a table with no columns", func() {
			columnDefs := make([]backup.ColumnDefinition, 0)
			atts := backup.ConstructTableAttributesList(columnDefs)
			Expect(atts).To(Equal(""))
		})
	})
	Describe("AddTableDataEntriesToTOC", func() {
		var (
			tocfile        *toc.TOC
			rowsCopiedMaps []map[uint32]int64
			table          backup.Table
		)
		BeforeEach(func() {
			tocfile = &toc.TOC{}
			backup.SetTOC(tocfile)
			rowsCopiedMaps = make([]map[uint32]int64, connectionPool.NumConns)
			columnDefs := []backup.ColumnDefinition{{Oid: 1, Name: "a"}}
			table = backup.Table{
				Relation:        backup.Relation{Oid: 1, Schema: "public", Name: "table"},
				TableDefinition: backup.TableDefinition{ColumnDefs: columnDefs},
			}
		})
		It("adds an entry for a regular table to the TOC", func() {
			tables := []backup.Table{table}
			backup.AddTableDataEntriesToTOC(tables, rowsCopiedMaps)
			expectedDataEntries := []toc.CoordinatorDataEntry{{Schema: "public", Name: "table", Oid: 1, AttributeString: "(a)"}}
			Expect(tocfile.DataEntries).To(Equal(expectedDataEntries))
		})
		It("does not add an entry for an external table to the TOC", func() {
			table.IsExternal = true
			tables := []backup.Table{table}
			backup.AddTableDataEntriesToTOC(tables, rowsCopiedMaps)
			Expect(tocfile.DataEntries).To(BeNil())
		})
		It("does not add an entry for a foreign table to the TOC", func() {
			foreignDef := backup.ForeignTableDefinition{Oid: 23, Options: "", Server: "fs"}
			table.ForeignDef = foreignDef
			tables := []backup.Table{table}
			backup.AddTableDataEntriesToTOC(tables, rowsCopiedMaps)
			Expect(tocfile.DataEntries).To(BeNil())
		})
	})
	Describe("CopyTableOut", func() {
		testTable := backup.Table{Relation: backup.Relation{SchemaOid: 2345, Oid: 3456, Schema: "public", Name: "foo"}}
		It("will back up a table to its own file with gzip compression", func() {
			utils.SetPipeThroughProgram(utils.PipeThroughProgram{Name: "gzip", OutputCommand: "gzip -c -8", InputCommand: "gzip -d -c", Extension: ".gz"})
			execStr := regexp.QuoteMeta("COPY public.foo TO PROGRAM 'gzip -c -8 > <SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_3456.gz' WITH CSV DELIMITER ',' ON SEGMENT IGNORE EXTERNAL PARTITIONS;")
			mock.ExpectExec(execStr).WillReturnResult(sqlmock.NewResult(10, 0))
			filename := "<SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_3456.gz"

			_, err := backup.CopyTableOut(connectionPool, testTable, filename, defaultConnNum)

			Expect(err).ShouldNot(HaveOccurred())
		})
		It("will back up a table to its own file with gzip compression using a plugin", func() {
			_ = cmdFlags.Set(options.PLUGIN_CONFIG, "/tmp/plugin_config")
			pluginConfig := utils.PluginConfig{ExecutablePath: "/tmp/fake-plugin.sh", ConfigPath: "/tmp/plugin_config"}
			backup.SetPluginConfig(&pluginConfig)
			utils.SetPipeThroughProgram(utils.PipeThroughProgram{Name: "gzip", OutputCommand: "gzip -c -8", InputCommand: "gzip -d -c", Extension: ".gz"})
			execStr := regexp.QuoteMeta("COPY public.foo TO PROGRAM 'gzip -c -8 | /tmp/fake-plugin.sh backup_data /tmp/plugin_config <SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_3456' WITH CSV DELIMITER ',' ON SEGMENT IGNORE EXTERNAL PARTITIONS;")
			mock.ExpectExec(execStr).WillReturnResult(sqlmock.NewResult(10, 0))

			filename := "<SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_3456"
			_, err := backup.CopyTableOut(connectionPool, testTable, filename, defaultConnNum)

			Expect(err).ShouldNot(HaveOccurred())
		})
		It("will back up a table to its own file with zstd compression", func() {
			utils.SetPipeThroughProgram(utils.PipeThroughProgram{Name: "zstd", OutputCommand: "zstd --compress -3 -c", InputCommand: "zstd --decompress -c", Extension: ".zst"})
			execStr := regexp.QuoteMeta("COPY public.foo TO PROGRAM 'zstd --compress -3 -c > <SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_3456.zst' WITH CSV DELIMITER ',' ON SEGMENT IGNORE EXTERNAL PARTITIONS;")
			mock.ExpectExec(execStr).WillReturnResult(sqlmock.NewResult(10, 0))
			filename := "<SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_3456.zst"

			_, err := backup.CopyTableOut(connectionPool, testTable, filename, defaultConnNum)

			Expect(err).ShouldNot(HaveOccurred())
		})
		It("will back up a table to its own file with zstd compression using a plugin", func() {
			_ = cmdFlags.Set(options.PLUGIN_CONFIG, "/tmp/plugin_config")
			pluginConfig := utils.PluginConfig{ExecutablePath: "/tmp/fake-plugin.sh", ConfigPath: "/tmp/plugin_config"}
			backup.SetPluginConfig(&pluginConfig)
			utils.SetPipeThroughProgram(utils.PipeThroughProgram{Name: "zstd", OutputCommand: "zstd --compress -3 -c", InputCommand: "zstd --decompress -c", Extension: ".zst"})
			execStr := regexp.QuoteMeta("COPY public.foo TO PROGRAM 'zstd --compress -3 -c | /tmp/fake-plugin.sh backup_data /tmp/plugin_config <SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_3456' WITH CSV DELIMITER ',' ON SEGMENT IGNORE EXTERNAL PARTITIONS;")
			mock.ExpectExec(execStr).WillReturnResult(sqlmock.NewResult(10, 0))

			filename := "<SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_3456"
			_, err := backup.CopyTableOut(connectionPool, testTable, filename, defaultConnNum)

			Expect(err).ShouldNot(HaveOccurred())
		})
		It("will back up a table to its own file without compression", func() {
			utils.SetPipeThroughProgram(utils.PipeThroughProgram{Name: "cat", OutputCommand: "cat -", InputCommand: "cat -", Extension: ""})
			execStr := regexp.QuoteMeta("COPY public.foo TO PROGRAM 'cat - > <SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_3456' WITH CSV DELIMITER ',' ON SEGMENT IGNORE EXTERNAL PARTITIONS;")
			mock.ExpectExec(execStr).WillReturnResult(sqlmock.NewResult(10, 0))
			filename := "<SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_3456"

			_, err := backup.CopyTableOut(connectionPool, testTable, filename, defaultConnNum)

			Expect(err).ShouldNot(HaveOccurred())
		})
		It("will back up a table to its own file without compression using a plugin", func() {
			_ = cmdFlags.Set(options.PLUGIN_CONFIG, "/tmp/plugin_config")
			pluginConfig := utils.PluginConfig{ExecutablePath: "/tmp/fake-plugin.sh", ConfigPath: "/tmp/plugin_config"}
			backup.SetPluginConfig(&pluginConfig)
			utils.SetPipeThroughProgram(utils.PipeThroughProgram{Name: "cat", OutputCommand: "cat -", InputCommand: "cat -", Extension: ""})
			execStr := regexp.QuoteMeta("COPY public.foo TO PROGRAM 'cat - | /tmp/fake-plugin.sh backup_data /tmp/plugin_config <SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_3456' WITH CSV DELIMITER ',' ON SEGMENT IGNORE EXTERNAL PARTITIONS;")
			mock.ExpectExec(execStr).WillReturnResult(sqlmock.NewResult(10, 0))

			filename := "<SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_3456"
			_, err := backup.CopyTableOut(connectionPool, testTable, filename, defaultConnNum)

			Expect(err).ShouldNot(HaveOccurred())
		})
		It("will back up a table to a single file", func() {
			_ = cmdFlags.Set(options.SINGLE_DATA_FILE, "true")
			execStr := regexp.QuoteMeta(`COPY public.foo TO PROGRAM '(test -p "<SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_3456" || (echo "Pipe not found <SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_3456">&2; exit 1)) && cat - > <SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_3456' WITH CSV DELIMITER ',' ON SEGMENT IGNORE EXTERNAL PARTITIONS;`)
			mock.ExpectExec(execStr).WillReturnResult(sqlmock.NewResult(10, 0))
			filename := "<SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_3456"

			_, err := backup.CopyTableOut(connectionPool, testTable, filename, defaultConnNum)

			Expect(err).ShouldNot(HaveOccurred())
		})
	})
	Describe("BackupSingleTableData", func() {
		var (
			testTable     backup.Table
			rowsCopiedMap map[uint32]int64
			progressBars  *utils.MultiProgressBar
			copyFmtStr    = "COPY(.*)%s(.*)"
		)
		BeforeEach(func() {
			testTable = backup.Table{
				Relation:        backup.Relation{Oid: 0, Schema: "public", Name: "testtable"},
				TableDefinition: backup.TableDefinition{IsExternal: false},
			}
			_ = cmdFlags.Set(options.SINGLE_DATA_FILE, "false")
			rowsCopiedMap = make(map[uint32]int64)
			progressBars = utils.NewMultiProgressBar(0, "", 0, false)
		})
		It("backs up a single regular table with single data file", func() {
			_ = cmdFlags.Set(options.SINGLE_DATA_FILE, "true")

			backupFile := fmt.Sprintf("<SEG_DATA_DIR>/gpbackup_<SEGID>_20170101010101_pipe_(.*)_%d", testTable.Oid)
			copyCmd := fmt.Sprintf(copyFmtStr, backupFile)
			mock.ExpectExec(copyCmd).WillReturnResult(sqlmock.NewResult(0, 10))
			err := backup.BackupSingleTableData(testTable, rowsCopiedMap, progressBars, 0)

			Expect(err).ShouldNot(HaveOccurred())
			Expect(rowsCopiedMap[0]).To(Equal(int64(10)))
			Expect(progressBars.TablesBar.GetBar().Get()).To(Equal(int64(1)))
		})
		It("backs up a single regular table without a single data file", func() {
			_ = cmdFlags.Set(options.SINGLE_DATA_FILE, "false")

			backupFile := fmt.Sprintf("<SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_%d", testTable.Oid)
			copyCmd := fmt.Sprintf(copyFmtStr, backupFile)
			mock.ExpectExec(copyCmd).WillReturnResult(sqlmock.NewResult(0, 10))
			err := backup.BackupSingleTableData(testTable, rowsCopiedMap, progressBars, 0)

			Expect(err).ShouldNot(HaveOccurred())
			Expect(rowsCopiedMap[0]).To(Equal(int64(10)))
			Expect(progressBars.TablesBar.GetBar().Get()).To(Equal(int64(1)))
		})
	})
	Describe("GetBackupDataSet", func() {
		config := history.BackupConfig{}
		var testTable backup.Table
		BeforeEach(func() {
			config.Sections = options.Sections{Globals: true, Predata: true, Data: true, Postdata: true}
			backup.SetReport(&report.Report{BackupConfig: config})
			testTable = backup.Table{
				Relation:        backup.Relation{Schema: "public", Name: "testtable"},
				TableDefinition: backup.TableDefinition{},
			}
		})
		It("excludes a foreign table", func() {
			foreignDef := backup.ForeignTableDefinition{Oid: 23, Options: "", Server: "fs"}
			testTable.ForeignDef = foreignDef
			tables := []backup.Table{testTable}

			dataTables, numExtOrForeignTables := backup.GetBackupDataSet(tables)
			Expect(len(dataTables)).To(Equal(0))
			Expect(numExtOrForeignTables).To(Equal(int64(1)))
		})
		It("excludes an external table", func() {
			testTable.IsExternal = true
			tables := []backup.Table{testTable}

			dataTables, numExtOrForeignTables := backup.GetBackupDataSet(tables)
			Expect(len(dataTables)).To(Equal(0))
			Expect(numExtOrForeignTables).To(Equal(int64(1)))
		})
		It("doesn't exclude regular table", func() {
			tables := []backup.Table{testTable}

			dataTables, numExtOrForeignTables := backup.GetBackupDataSet(tables)
			Expect(len(dataTables)).To(Equal(1))
			Expect(numExtOrForeignTables).To(Equal(int64(0)))
		})
		It("returns an empty set, if --metadata-only flag is set and a regular table is given", func() {
			config.Data = false
			backup.SetReport(&report.Report{BackupConfig: config})
			tables := []backup.Table{testTable}

			dataTables, numExtOrForeignTables := backup.GetBackupDataSet(tables)
			Expect(len(dataTables)).To(Equal(0))
			Expect(numExtOrForeignTables).To(Equal(int64(0)))
		})
	})
})
