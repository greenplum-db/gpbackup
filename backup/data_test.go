package backup_test

import (
	"regexp"

	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/utils"

	"fmt"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
	"gopkg.in/cheggaaa/pb.v1"
)

var _ bool = Describe("backup/data tests", func() {
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
			columnDefs := []backup.ColumnDefinition{}
			atts := backup.ConstructTableAttributesList(columnDefs)
			Expect(atts).To(Equal(""))
		})
	})
	Describe("AddTableDataEntriesToTOC", func() {
		var toc *utils.TOC
		var rowsCopiedMaps []map[uint32]int64
		BeforeEach(func() {
			toc = &utils.TOC{}
			backup.SetTOC(toc)
			rowsCopiedMaps = make([]map[uint32]int64, connectionPool.NumConns)
		})
		It("adds an entry for a regular table to the TOC", func() {
			columnDefs := []backup.ColumnDefinition{{Oid: 1, Name: "a"}}
			tableDefs := map[uint32]backup.TableDefinition{1: {ColumnDefs: columnDefs}}
			tables := []backup.Relation{{Oid: 1, Schema: "public", Name: "table"}}
			backup.AddTableDataEntriesToTOC(tables, tableDefs, rowsCopiedMaps)
			expectedDataEntries := []utils.MasterDataEntry{{Schema: "public", Name: "table", Oid: 1, AttributeString: "(a)"}}
			Expect(toc.DataEntries).To(Equal(expectedDataEntries))
		})
		It("does not add an entry for an external table to the TOC", func() {
			columnDefs := []backup.ColumnDefinition{{Oid: 1, Name: "a"}}
			tableDefs := map[uint32]backup.TableDefinition{1: {ColumnDefs: columnDefs, IsExternal: true}}
			tables := []backup.Relation{{Oid: 1, Schema: "public", Name: "table"}}
			backup.AddTableDataEntriesToTOC(tables, tableDefs, rowsCopiedMaps)
			Expect(toc.DataEntries).To(BeNil())
		})
		It("does not add an entry for a foreign table to the TOC", func() {
			columnDefs := []backup.ColumnDefinition{{Oid: 1, Name: "a"}}
			tableDefs := map[uint32]backup.TableDefinition{1: {ColumnDefs: columnDefs, ForeignDef: backup.ForeignTableDefinition{23, "", "fs"}}}
			tables := []backup.Relation{{Oid: 1, Schema: "public", Name: "table"}}
			backup.AddTableDataEntriesToTOC(tables, tableDefs, rowsCopiedMaps)
			Expect(toc.DataEntries).To(BeNil())
		})
	})
	Describe("CopyTableOut", func() {
		It("will back up a table to its own file with compression", func() {
			utils.SetPipeThroughProgram(utils.PipeThroughProgram{Name: "gzip", OutputCommand: "gzip -c -8", InputCommand: "gzip -d -c", Extension: ".gz"})
			testTable := backup.Relation{SchemaOid: 2345, Oid: 3456, Schema: "public", Name: "foo", DependsUpon: nil, Inherits: nil}
			execStr := regexp.QuoteMeta("COPY public.foo TO PROGRAM 'gzip -c -8 > <SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_3456.gz' WITH CSV DELIMITER ',' ON SEGMENT IGNORE EXTERNAL PARTITIONS;")
			mock.ExpectExec(execStr).WillReturnResult(sqlmock.NewResult(10, 0))
			filename := "<SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_3456.gz"

			backup.CopyTableOut(connectionPool, testTable, filename, defaultConnNum)
		})
		It("will back up a table to its own file with compression using a plugin", func() {
			cmdFlags.Set(utils.PLUGIN_CONFIG, "/tmp/plugin_config")
			pluginConfig := utils.PluginConfig{ExecutablePath: "/tmp/fake-plugin.sh", ConfigPath: "/tmp/plugin_config"}
			backup.SetPluginConfig(&pluginConfig)
			utils.SetPipeThroughProgram(utils.PipeThroughProgram{Name: "gzip", OutputCommand: "gzip -c -8", InputCommand: "gzip -d -c", Extension: ".gz"})
			testTable := backup.Relation{SchemaOid: 2345, Oid: 3456, Schema: "public", Name: "foo", DependsUpon: nil, Inherits: nil}
			execStr := regexp.QuoteMeta("COPY public.foo TO PROGRAM 'gzip -c -8 | /tmp/fake-plugin.sh backup_data /tmp/plugin_config <SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_3456' WITH CSV DELIMITER ',' ON SEGMENT IGNORE EXTERNAL PARTITIONS;")
			mock.ExpectExec(execStr).WillReturnResult(sqlmock.NewResult(10, 0))

			filename := "<SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_3456"
			backup.CopyTableOut(connectionPool, testTable, filename, defaultConnNum)
		})
		It("will back up a table to its own file without compression", func() {
			utils.SetPipeThroughProgram(utils.PipeThroughProgram{Name: "cat", OutputCommand: "cat -", InputCommand: "cat -", Extension: ""})
			testTable := backup.Relation{SchemaOid: 2345, Oid: 3456, Schema: "public", Name: "foo", DependsUpon: nil, Inherits: nil}
			execStr := regexp.QuoteMeta("COPY public.foo TO PROGRAM 'cat - > <SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_3456' WITH CSV DELIMITER ',' ON SEGMENT IGNORE EXTERNAL PARTITIONS;")
			mock.ExpectExec(execStr).WillReturnResult(sqlmock.NewResult(10, 0))
			filename := "<SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_3456"

			backup.CopyTableOut(connectionPool, testTable, filename, defaultConnNum)
		})
		It("will back up a table to its own file without compression using a plugin", func() {
			cmdFlags.Set(utils.PLUGIN_CONFIG, "/tmp/plugin_config")
			pluginConfig := utils.PluginConfig{ExecutablePath: "/tmp/fake-plugin.sh", ConfigPath: "/tmp/plugin_config"}
			backup.SetPluginConfig(&pluginConfig)
			utils.SetPipeThroughProgram(utils.PipeThroughProgram{Name: "cat", OutputCommand: "cat -", InputCommand: "cat -", Extension: ""})
			testTable := backup.Relation{SchemaOid: 2345, Oid: 3456, Schema: "public", Name: "foo", DependsUpon: nil, Inherits: nil}
			execStr := regexp.QuoteMeta("COPY public.foo TO PROGRAM 'cat - | /tmp/fake-plugin.sh backup_data /tmp/plugin_config <SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_3456' WITH CSV DELIMITER ',' ON SEGMENT IGNORE EXTERNAL PARTITIONS;")
			mock.ExpectExec(execStr).WillReturnResult(sqlmock.NewResult(10, 0))

			filename := "<SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_3456"
			backup.CopyTableOut(connectionPool, testTable, filename, defaultConnNum)
		})
		It("will back up a table to a single file", func() {
			cmdFlags.Set(utils.SINGLE_DATA_FILE, "true")
			testTable := backup.Relation{SchemaOid: 2345, Oid: 3456, Schema: "public", Name: "foo", DependsUpon: nil, Inherits: nil}
			execStr := regexp.QuoteMeta(`COPY public.foo TO PROGRAM '(test -p "<SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_3456" || (echo "Pipe not found">&2; exit 1)) && cat - > <SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_3456' WITH CSV DELIMITER ',' ON SEGMENT IGNORE EXTERNAL PARTITIONS;`)
			mock.ExpectExec(execStr).WillReturnResult(sqlmock.NewResult(10, 0))
			filename := "<SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_3456"

			backup.CopyTableOut(connectionPool, testTable, filename, defaultConnNum)
		})
	})
	Describe("BackupSingleTableData", func() {
		var (
			tableDef      backup.TableDefinition
			testTable     backup.Relation
			rowsCopiedMap map[uint32]int64
			counters      backup.BackupProgressCounters
			copyFmtStr    = "COPY(.*)%s(.*)"
		)
		BeforeEach(func() {
			tableDef = backup.TableDefinition{IsExternal: false}
			testTable = backup.Relation{Oid: 0, Schema: "public", Name: "testtable"}
			cmdFlags.Set(utils.SINGLE_DATA_FILE, "false")
			rowsCopiedMap = make(map[uint32]int64, 0)
			counters = backup.BackupProgressCounters{NumRegTables: 0, TotalRegTables: 1}
			counters.ProgressBar = utils.NewProgressBar(int(counters.TotalRegTables), "Tables backed up: ", utils.PB_INFO)
			counters.ProgressBar.(*pb.ProgressBar).NotPrint = true
			counters.ProgressBar.Start()
		})
		It("backs up a single regular table with single data file", func() {
			cmdFlags.Set(utils.SINGLE_DATA_FILE, "true")

			backupFile := fmt.Sprintf("<SEG_DATA_DIR>/gpbackup_<SEGID>_20170101010101_pipe_(.*)_%d", testTable.Oid)
			copyCmd := fmt.Sprintf(copyFmtStr, backupFile)
			mock.ExpectExec(copyCmd).WillReturnResult(sqlmock.NewResult(0, 10))
			backup.BackupSingleTableData(tableDef, testTable, rowsCopiedMap, &counters, 0)

			Expect(rowsCopiedMap[0]).To(Equal(int64(10)))
			Expect(counters.NumRegTables).To(Equal(int64(1)))
		})
		It("backs up a single regular table without a single data file", func() {
			cmdFlags.Set(utils.SINGLE_DATA_FILE, "false")

			backupFile := fmt.Sprintf("<SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_%d", testTable.Oid)
			copyCmd := fmt.Sprintf(copyFmtStr, backupFile)
			mock.ExpectExec(copyCmd).WillReturnResult(sqlmock.NewResult(0, 10))
			backup.BackupSingleTableData(tableDef, testTable, rowsCopiedMap, &counters, 0)

			Expect(rowsCopiedMap[0]).To(Equal(int64(10)))
			Expect(counters.NumRegTables).To(Equal(int64(1)))
		})
		It("backs up a single external table", func() {
			cmdFlags.Set(utils.LEAF_PARTITION_DATA, "false")
			tableDef.IsExternal = true
			backup.BackupSingleTableData(tableDef, testTable, rowsCopiedMap, &counters, 0)

			Expect(rowsCopiedMap).To(BeEmpty())
			Expect(counters.NumRegTables).To(Equal(int64(0)))
		})
		It("backs up a single foreign table", func() {
			cmdFlags.Set(utils.LEAF_PARTITION_DATA, "false")
			tableDef.ForeignDef = backup.ForeignTableDefinition{23, "", "fs"}
			backup.BackupSingleTableData(tableDef, testTable, rowsCopiedMap, &counters, 0)

			Expect(rowsCopiedMap).To(BeEmpty())
			Expect(counters.NumRegTables).To(Equal(int64(0)))
		})
	})
	Describe("CheckDBContainsData", func() {
		config := utils.BackupConfig{}
		testTable := []backup.Relation{{Schema: "public", Name: "testtable"}}

		BeforeEach(func() {
			config.MetadataOnly = false
			backup.SetReport(&utils.Report{BackupConfig: config})
		})
		It("changes backup type to metadata if no tables in DB", func() {
			backup.CheckTablesContainData([]backup.Relation{}, map[uint32]backup.TableDefinition{})
			Expect(backup.GetReport().BackupConfig.MetadataOnly).To(BeTrue())
		})
		It("changes backup type to metadata if only external or foreign tables in database", func() {
			tableDef := backup.TableDefinition{IsExternal: true}
			backup.CheckTablesContainData(testTable, map[uint32]backup.TableDefinition{0: tableDef})
			Expect(backup.GetReport().BackupConfig.MetadataOnly).To(BeTrue())
		})
		It("does not change backup type if metadata-only backup", func() {
			config.MetadataOnly = true
			backup.SetReport(&utils.Report{BackupConfig: config})
			backup.CheckTablesContainData([]backup.Relation{}, map[uint32]backup.TableDefinition{})
			Expect(backup.GetReport().BackupConfig.MetadataOnly).To(BeTrue())
		})
		It("does not change backup type if tables present in database", func() {
			backup.CheckTablesContainData(testTable, map[uint32]backup.TableDefinition{0: {}})
			Expect(backup.GetReport().BackupConfig.MetadataOnly).To(BeFalse())
		})
	})
})
