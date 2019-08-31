package restore_test

import (
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/greenplum-db/gpbackup/testutils"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup_history"
	"github.com/greenplum-db/gpbackup/restore"
	"github.com/greenplum-db/gpbackup/utils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
	"github.com/DATA-DOG/go-sqlmock"
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
			schemaArray        = []utils.StatementWithType{{Name: "foo", Statement: "create schema foo"}}
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
			cmdFlags.Set(utils.ON_ERROR_CONTINUE, "true")
			defer cmdFlags.Set(utils.ON_ERROR_CONTINUE, "false")
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
		legacyBackupConfig := backup_history.BackupConfig{}
		legacyBackupConfig.RestorePlan = nil
		legacyBackupTOC := utils.TOC{
			DataEntries: []utils.MasterDataEntry{
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

		sampleBackupHistory := `
backupconfigs:
- backupdir: ""
  backupversion: 1.11.0+dev.28.g10571fd
  compressed: false
  databasename: plugin_test_db
  databaseversion: 5.15.0+dev.18.gb29642fb22 build dev
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
  restoreplan:
  - timestamp: "20170415154408"
    tablefqns:
    - public.test_table
  singledatafile: false
  timestamp: "20170415154408"
  withstatistics: false
- backupdir: ""
  backupversion: 1.11.0+dev.28.g10571fd
  compressed: false
  databasename: plugin_test_db
  databaseversion: 5.15.0+dev.18.gb29642fb22 build dev
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

		sampleBackupConfig := `
backupdir: ""
backupversion: 1.11.0+dev.28.g10571fd
compressed: false
databasename: plugin_test_db
databaseversion: 5.15.0+dev.18.gb29642fb22 build dev
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
			err = cmdFlags.Set(utils.PLUGIN_CONFIG, testConfigPath)
			Expect(err).ToNot(HaveOccurred())

			executor = testutils.TestExecutorMultiple{
				ClusterOutputs: make([]*cluster.RemoteOutput, 2),
			}
			// set up fake command results
			apiResponse := make(map[int]string, 3)
			apiResponse[-1] = utils.RequiredPluginVersion // this is a successful result fpr API version
			apiResponse[0] = utils.RequiredPluginVersion
			apiResponse[1] = utils.RequiredPluginVersion
			executor.ClusterOutputs[0] = &cluster.RemoteOutput{
				Stdouts: apiResponse,
			}

			nativeResponse := make(map[int]string, 3)
			nativeResponse[-1] = "myPlugin version 1.2.3" // this is a successful result for --version
			nativeResponse[0] = "myPlugin version 1.2.3"
			nativeResponse[1] = "myPlugin version 1.2.3"
			executor.ClusterOutputs[1] = &cluster.RemoteOutput{
				Stdouts: nativeResponse,
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
			historyPath := filepath.Join(mdd, "gpbackup_history.yaml")
			_ = os.Remove(historyPath) // make sure no previous copy
			err = ioutil.WriteFile(historyPath, []byte(sampleBackupHistory), 0777)
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
			err := os.RemoveAll(tempDir)
			Expect(err).To(Not(HaveOccurred()))
			_ = os.Chdir(oldWd)
			_ = os.Remove(testConfigPath)
			_ = os.Remove(testConfigPath + "_0")
			_ = os.Remove(testConfigPath + "_1")
		})
		Describe("RecoverMetadataFilesUsingPlugin", func() {
			It("proceed without warning when plugin version is found", func() {
				_ = cmdFlags.Set(utils.TIMESTAMP, "20180415154238")
				restore.RecoverMetadataFilesUsingPlugin()
				Expect(string(logfile.Contents())).ToNot(ContainSubstring("cannot recover plugin version"))
			})
			It("logs warning when plugin version not found", func() {
				_ = cmdFlags.Set(utils.TIMESTAMP, "20170415154408")
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
