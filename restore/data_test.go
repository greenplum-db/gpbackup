package restore_test

import (
	"regexp"

	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/restore"
	"github.com/greenplum-db/gpbackup/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
)

var _ = Describe("restore/data tests", func() {
	Describe("CopyTableIn", func() {
		BeforeEach(func() {
			utils.SetPipeThroughProgram(utils.PipeThroughProgram{Name: "cat", OutputCommand: "cat -", InputCommand: "cat -", Extension: ""})
			backup.SetPluginConfig(nil)
			cmdFlags.Set(utils.PLUGIN_CONFIG, "")
		})
		It("will restore a table from its own file with compression", func() {
			utils.SetPipeThroughProgram(utils.PipeThroughProgram{Name: "gzip", OutputCommand: "gzip -c -1", InputCommand: "gzip -d -c", Extension: ".gz"})
			execStr := regexp.QuoteMeta("COPY public.foo(i,j) FROM PROGRAM 'cat <SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_3456.gz | gzip -d -c' WITH CSV DELIMITER ',' ON SEGMENT;")
			mock.ExpectExec(execStr).WillReturnResult(sqlmock.NewResult(10, 0))
			filename := "<SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_3456.gz"
			restore.CopyTableIn(connectionPool, "public.foo", "(i,j)", filename, false, 0)
		})
		It("will restore a table from its own file without compression", func() {
			execStr := regexp.QuoteMeta("COPY public.foo(i,j) FROM PROGRAM 'cat <SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_3456 | cat -' WITH CSV DELIMITER ',' ON SEGMENT;")
			mock.ExpectExec(execStr).WillReturnResult(sqlmock.NewResult(10, 0))
			filename := "<SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_3456"
			restore.CopyTableIn(connectionPool, "public.foo", "(i,j)", filename, false, 0)
		})
		It("will restore a table from a single data file", func() {
			execStr := regexp.QuoteMeta("COPY public.foo(i,j) FROM PROGRAM 'cat <SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_pipe_3456 | cat -' WITH CSV DELIMITER ',' ON SEGMENT;")
			mock.ExpectExec(execStr).WillReturnResult(sqlmock.NewResult(10, 0))
			filename := "<SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_pipe_3456"
			restore.CopyTableIn(connectionPool, "public.foo", "(i,j)", filename, true, 0)
		})
		It("will restore a table from its own file with compression using a plugin", func() {
			utils.SetPipeThroughProgram(utils.PipeThroughProgram{Name: "gzip", OutputCommand: "gzip -c -1", InputCommand: "gzip -d -c", Extension: ".gz"})
			cmdFlags.Set(utils.PLUGIN_CONFIG, "/tmp/plugin_config")
			pluginConfig := utils.PluginConfig{ExecutablePath: "/tmp/fake-plugin.sh", ConfigPath: "/tmp/plugin_config"}
			restore.SetPluginConfig(&pluginConfig)
			execStr := regexp.QuoteMeta("COPY public.foo(i,j) FROM PROGRAM '/tmp/fake-plugin.sh restore_data /tmp/plugin_config <SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_pipe_3456.gz | gzip -d -c' WITH CSV DELIMITER ',' ON SEGMENT;")
			mock.ExpectExec(execStr).WillReturnResult(sqlmock.NewResult(10, 0))

			filename := "<SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_pipe_3456.gz"
			restore.CopyTableIn(connectionPool, "public.foo", "(i,j)", filename, false, 0)
		})
		It("will restore a table from its own file without compression using a plugin", func() {
			cmdFlags.Set(utils.PLUGIN_CONFIG, "/tmp/plugin_config")
			pluginConfig := utils.PluginConfig{ExecutablePath: "/tmp/fake-plugin.sh", ConfigPath: "/tmp/plugin_config"}
			restore.SetPluginConfig(&pluginConfig)
			execStr := regexp.QuoteMeta("COPY public.foo(i,j) FROM PROGRAM '/tmp/fake-plugin.sh restore_data /tmp/plugin_config <SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_pipe_3456.gz | cat -' WITH CSV DELIMITER ',' ON SEGMENT;")
			mock.ExpectExec(execStr).WillReturnResult(sqlmock.NewResult(10, 0))

			filename := "<SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_pipe_3456.gz"
			restore.CopyTableIn(connectionPool, "public.foo", "(i,j)", filename, false, 0)
		})
	})
	Describe("CheckRowsRestored", func() {
		var (
			expectedRows int64 = 10
			name               = "public.foo"
		)
		It("does nothing if the number of rows match ", func() {
			err := restore.CheckRowsRestored(10, expectedRows, name)
			Expect(err).ToNot(HaveOccurred())
		})
		It("returns an error if the numbers of rows do not match", func() {
			err := restore.CheckRowsRestored(5, expectedRows, name)
			Expect(err.Error()).To(Equal("Expected to restore 10 rows to table public.foo, but restored 5 instead"))
		})
	})
})
