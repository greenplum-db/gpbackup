package restore_test

import (
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/history"
	"github.com/greenplum-db/gpbackup/options"
	"github.com/greenplum-db/gpbackup/restore"
	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/toc"
	"github.com/greenplum-db/gpbackup/utils"
	"github.com/pkg/errors"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("wrapper tests", func() {
	Describe("SetMaxCsvLineLengthQuery", func() {
		It("returns nothing with a connection version of at least 6.0.0", func() {
			testhelper.SetDBVersion(connectionPool, "6.0.0")
			result := restore.SetMaxCsvLineLengthQuery(connectionPool)
			Expect(result).To(Equal(""))
		})
		It("sets gp_max_csv_line_length to 1GB when connection version is 4.X and at least 4.3.30.0", func() {
			testhelper.SetDBVersion(connectionPool, "4.3.30")
			result := restore.SetMaxCsvLineLengthQuery(connectionPool)
			Expect(result).To(Equal("SET gp_max_csv_line_length = 1073741824;\n"))
		})
		It("sets gp_max_csv_line_length to 1GB when connection version is 5.X and at least 5.11.0", func() {
			testhelper.SetDBVersion(connectionPool, "5.11.0")
			result := restore.SetMaxCsvLineLengthQuery(connectionPool)
			Expect(result).To(Equal("SET gp_max_csv_line_length = 1073741824;\n"))
		})
		It("sets gp_max_csv_line_length to 4MB when connection version is 4.X and before 4.3.30.0", func() {
			testhelper.SetDBVersion(connectionPool, "4.3.29")
			result := restore.SetMaxCsvLineLengthQuery(connectionPool)
			Expect(result).To(Equal("SET gp_max_csv_line_length = 4194304;\n"))
		})
		It("sets gp_max_csv_line_length to 4MB when connection version is 5.X and before 5.11.0", func() {
			testhelper.SetDBVersion(connectionPool, "5.10.999")
			result := restore.SetMaxCsvLineLengthQuery(connectionPool)
			Expect(result).To(Equal("SET gp_max_csv_line_length = 4194304;\n"))
		})
	})
	Describe("RestoreSchemas", func() {
		var (
			ignoredProgressBar utils.ProgressBar
			schemaArray        = []toc.StatementWithType{{Name: "foo", Statement: "create schema foo"}}
		)
		BeforeEach(func() {
			ignoredProgressBar = utils.NewProgressBar(1, "", utils.PB_NONE)
			ignoredProgressBar.Start()
		})
		AfterEach(func() {
			ignoredProgressBar.Finish()
		})
		It("logs nothing if there are no errors", func() {
			expectedResult := sqlmock.NewResult(0, 1)
			mock.ExpectExec("create schema foo").WillReturnResult(expectedResult)

			restore.RestoreSchemas(schemaArray, ignoredProgressBar)

			testhelper.NotExpectRegexp(logfile, "Schema foo already exists")
			testhelper.NotExpectRegexp(logfile, "Error encountered while creating schema foo")
		})
		It("logs warning if schema already exists", func() {
			expectedErr := errors.New(`schema "foo" already exists`)
			mock.ExpectExec("create schema foo").WillReturnError(expectedErr)

			restore.RestoreSchemas(schemaArray, ignoredProgressBar)

			testhelper.ExpectRegexp(logfile, "[WARNING]:-Schema foo already exists")
		})
		It("logs error if --on-error-continue is set", func() {
			_ = cmdFlags.Set(options.ON_ERROR_CONTINUE, "true")
			defer cmdFlags.Set(options.ON_ERROR_CONTINUE, "false")
			expectedErr := errors.New("some other schema error")
			mock.ExpectExec("create schema foo").WillReturnError(expectedErr)

			restore.RestoreSchemas(schemaArray, ignoredProgressBar)

			expectedDebugMsg := "[DEBUG]:-Error encountered while creating schema foo: some other schema error"
			testhelper.ExpectRegexp(logfile, expectedDebugMsg)
			expectedErrMsg := "[ERROR]:-Encountered 1 errors during schema restore; see log file gbytes.Buffer for a list of errors."
			testhelper.ExpectRegexp(logfile, expectedErrMsg)
		})
		It("panics if create schema statement fails", func() {
			expectedErr := errors.New("some other schema error")
			mock.ExpectExec("create schema foo").WillReturnError(expectedErr)
			expectedPanicMsg := "[CRITICAL]:-some other schema error: Error encountered while creating schema foo"
			defer testhelper.ShouldPanicWithMessage(expectedPanicMsg)

			restore.RestoreSchemas(schemaArray, ignoredProgressBar)
		})
	})
	Describe("SetRestorePlanForLegacyBackup", func() {
		legacyBackupConfig := history.BackupConfig{}
		legacyBackupConfig.RestorePlan = nil
		legacyBackupTOC := toc.TOC{
			DataEntries: []toc.CoordinatorDataEntry{
				{Schema: "schema1", Name: "table1"},
				{Schema: "schema2", Name: "table2"},
			},
		}
		legacyBackupTimestamp := "ts0"

		restore.SetRestorePlanForLegacyBackup(&legacyBackupTOC, legacyBackupTimestamp, &legacyBackupConfig)

		Specify("That there should be only one resultant restore plan entry", func() {
			Expect(legacyBackupConfig.RestorePlan).To(HaveLen(1))
		})

		Specify("That the restore plan entry should have the legacy backup's timestamp", func() {
			Expect(legacyBackupConfig.RestorePlan[0].Timestamp).To(Equal(legacyBackupTimestamp))
		})

		Specify("That the restore plan entry should have all table FQNs as in the TOC's DataEntries", func() {
			Expect(legacyBackupConfig.RestorePlan[0].TableFQNs).
				To(Equal([]string{"schema1.table1", "schema2.table2"}))
		})

	})
	Describe("restore history tests", func() {
		sampleConfigContents := `
executablepath: /bin/echo
options:
  hostname: "10.85.20.10"
  storage_unit: "GPDB"
  username: "gpadmin"
  password: "changeme"
  password_encryption:
  directory: "/blah"
  replication: "off"
  remote_hostname: "10.85.20.11"
  remote_storage_unit: "GPDB"
  remote_username: "gpadmin"
  remote_password: "changeme"
  remote_directory: "/blah"
  pgport: 1234
`

		sampleBackupHistConfig1 := history.BackupConfig{
			BackupDir:             "",
			BackupVersion:         "1.11.0+dev.28.g10571fdxs",
			Compressed:            false,
			DatabaseName:          "plugin_test_db",
			DatabaseVersion:       "4.3.99.0+dev.18.gb29642fb22 build dev",
			DataOnly:              false,
			DateDeleted:           "",
			ExcludeRelations:      make([]string, 0),
			ExcludeSchemaFiltered: false,
			ExcludeSchemas:        make([]string, 0),
			ExcludeTableFiltered:  false,
			IncludeRelations:      make([]string, 0),
			IncludeSchemaFiltered: false,
			IncludeSchemas:        make([]string, 0),
			IncludeTableFiltered:  false,
			Incremental:           false,
			LeafPartitionData:     false,
			MetadataOnly:          false,
			Plugin:                "/Users/pivotal/workspace/gp-backup-ddboost-plugin/gpbackup_ddboost_plugin",
			RestorePlan:           []history.RestorePlanEntry{{"20170415154408", []string{"public.test_table"}}},
			SingleDataFile:        false,
			Timestamp:             "20170415154408",
			WithStatistics:        false,
		}
		sampleBackupHistConfig2 := history.BackupConfig{
			BackupDir:             "",
			BackupVersion:         "1.11.0+dev.28.g10571fd",
			Compressed:            false,
			DatabaseName:          "plugin_test_db",
			DatabaseVersion:       "4.3.99.0+dev.18.gb29642fb22 build dev",
			DataOnly:              false,
			DateDeleted:           "",
			ExcludeRelations:      make([]string, 0),
			ExcludeSchemaFiltered: false,
			ExcludeSchemas:        make([]string, 0),
			ExcludeTableFiltered:  false,
			IncludeRelations:      make([]string, 0),
			IncludeSchemaFiltered: false,
			IncludeSchemas:        make([]string, 0),
			IncludeTableFiltered:  false,
			Incremental:           false,
			LeafPartitionData:     false,
			MetadataOnly:          false,
			Plugin:                "/Users/pivotal/workspace/gp-backup-ddboost-plugin/gpbackup_ddboost_plugin",
			PluginVersion:         "99.99.9999",
			RestorePlan:           []history.RestorePlanEntry{{"20180415154238", []string{"public.test_table"}}},
			SingleDataFile:        true,
			Timestamp:             "20180415154238",
			WithStatistics:        false,
		}

		sampleBackupConfig := `
backupdir: ""
backupversion: 1.11.0+dev.28.g10571fd
compressed: false
databasename: plugin_test_db
databaseversion: 4.3.99.0+dev.18.gb29642fb22 build dev
dataonly: false
deleted: false
excluderelations: []
excludeschemafiltered: false
excludeschemas: []
excludetablefiltered: false
includerelations: []
includeschemafiltered: false
includeschemas: []
includetablefiltered: false
incremental: false
leafpartitiondata: false
metadataonly: false
plugin: /Users/pivotal/workspace/gp-backup-ddboost-plugin/gpbackup_ddboost_plugin
pluginversion: "99.99.9999"
restoreplan:
- timestamp: "20180415154238"
tablefqns:
- public.test_table
singledatafile: true
timestamp: "20180415154238"
withstatistics: false
`
		var executor testutils.TestExecutorMultiple
		var testConfigPath = "/tmp/unit_test_plugin_config.yml"
		var oldWd string
		var mdd string
		var tempDir string

		BeforeEach(func() {
			tempDir, _ = ioutil.TempDir("", "temp")

			err := ioutil.WriteFile(testConfigPath, []byte(sampleConfigContents), 0777)
			Expect(err).ToNot(HaveOccurred())
			err = cmdFlags.Set(options.PLUGIN_CONFIG, testConfigPath)
			Expect(err).ToNot(HaveOccurred())

			executor = testutils.TestExecutorMultiple{
				ClusterOutputs: make([]*cluster.RemoteOutput, 2),
			}
			executor.ClusterOutputs[0] = &cluster.RemoteOutput{
				Commands: []cluster.ShellCommand{
					cluster.ShellCommand{Stdout: utils.RequiredPluginVersion},
					cluster.ShellCommand{Stdout: utils.RequiredPluginVersion},
					cluster.ShellCommand{Stdout: utils.RequiredPluginVersion},
				},
			}
			executor.ClusterOutputs[1] = &cluster.RemoteOutput{
				Commands: []cluster.ShellCommand{
					cluster.ShellCommand{Stdout: "myPlugin version 1.2.3"},
					cluster.ShellCommand{Stdout: "myPlugin version 1.2.3"},
					cluster.ShellCommand{Stdout: "myPlugin version 1.2.3"},
				},
			}

			// write history file using test cluster directories
			testCluster := testutils.SetupTestCluster()
			testCluster.Executor = &executor
			oldWd, err = os.Getwd()
			Expect(err).ToNot(HaveOccurred())
			err = os.Chdir(tempDir)
			Expect(err).ToNot(HaveOccurred())
			mdd = filepath.Join(tempDir, testCluster.GetDirForContent(-1))
			err = os.MkdirAll(mdd, 0777)
			Expect(err).ToNot(HaveOccurred())
			historyPath := filepath.Join(mdd, "gpbackup_history.db")
			_ = os.Remove(historyPath) // make sure no previous copy
			db, err := history.InitializeHistoryDatabase(historyPath)
			Expect(err).ToNot(HaveOccurred())
			defer db.Close()
			err = history.StoreBackupHistory(db, &sampleBackupHistConfig1)
			Expect(err).ToNot(HaveOccurred())
			err = history.StoreBackupHistory(db, &sampleBackupHistConfig2)
			Expect(err).ToNot(HaveOccurred())

			// create backup config file
			configDir := filepath.Join(mdd, "backups/20170101/20170101010101/")
			_ = os.MkdirAll(configDir, 0777)
			configPath := filepath.Join(configDir, "gpbackup_20170101010101_config.yaml")
			err = ioutil.WriteFile(configPath, []byte(sampleBackupConfig), 0777)
			Expect(err).ToNot(HaveOccurred())

			restore.SetVersion("1.11.0+dev.28.g10571fd")
		})
		AfterEach(func() {
			_ = os.Chdir(oldWd)
			err := os.RemoveAll(tempDir)
			Expect(err).To(Not(HaveOccurred()))
			_ = os.Remove(testConfigPath)
			confDir := filepath.Dir(testConfigPath)
			confFileName := filepath.Base(testConfigPath)
			files, _ := ioutil.ReadDir(confDir)
			for _, f := range files {
				match, _ := filepath.Match("*"+confFileName+"*", f.Name())
				if match {
					_ = os.Remove(confDir + "/" + f.Name())
				}
			}
		})
		Describe("RecoverMetadataFilesUsingPlugin", func() {
			It("proceed without warning when plugin version is found", func() {
				_ = cmdFlags.Set(options.TIMESTAMP, "20180415154238")
				restore.RecoverMetadataFilesUsingPlugin()
				Expect(string(logfile.Contents())).ToNot(ContainSubstring("cannot recover plugin version"))
			})
			It("logs warning when plugin version not found", func() {
				_ = cmdFlags.Set(options.TIMESTAMP, "20170415154408")
				restore.RecoverMetadataFilesUsingPlugin()
				Expect(string(logfile.Contents())).To(ContainSubstring("cannot recover plugin version"))
			})
		})
		Describe("FindHistoricalPluginVersion", func() {
			It("finds plugin version", func() {
				resultPluginVersion := restore.FindHistoricalPluginVersion("20180415154238")
				Expect(resultPluginVersion).To(Equal("99.99.9999"))
			})
		})
	})
})
