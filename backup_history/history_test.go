package backup_history_test

import (
	"os"

	"github.com/greenplum-db/gp-common-go-libs/iohelper"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/greenplum-db/gp-common-go-libs/structmatcher"
	"github.com/greenplum-db/gpbackup/backup_history"
	"github.com/greenplum-db/gpbackup/utils"
	. "github.com/onsi/ginkgo"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"
)

var _ bool = Describe("backup/history tests", func() {
	var testConfig1, testConfig2, testConfig3 utils.BackupConfig
	var historyFilePath = "/tmp/history_file.yaml"

	BeforeEach(func() {
		testConfig1 = utils.BackupConfig{
			DatabaseName:     "testdb1",
			ExcludeRelations: []string{},
			ExcludeSchemas:   []string{},
			IncludeRelations: []string{"testschema.testtable1", "testschema.testtable2"},
			IncludeSchemas:   []string{},
			RestorePlan:      []utils.RestorePlanEntry{},
			Timestamp:        "timestamp1",
		}
		testConfig2 = utils.BackupConfig{
			DatabaseName:     "testdb2",
			ExcludeRelations: []string{},
			ExcludeSchemas:   []string{"public"},
			IncludeRelations: []string{},
			IncludeSchemas:   []string{},
			RestorePlan:      []utils.RestorePlanEntry{},
			Timestamp:        "timestamp2",
		}
		testConfig3 = utils.BackupConfig{
			DatabaseName:     "testdb3",
			ExcludeRelations: []string{},
			ExcludeSchemas:   []string{"public"},
			IncludeRelations: []string{},
			IncludeSchemas:   []string{},
			RestorePlan:      []utils.RestorePlanEntry{},
			Timestamp:        "timestamp3",
		}
	})
	Describe("NewHistory", func() {
		Context("history file doesn't exist", func() {
			It("creates an empty history object", func() {
				operating.System.Stat = func(name string) (os.FileInfo, error) { return nil, errors.New("file does not exist") }

				resultHistory := backup_history.NewHistory(historyFilePath)

				structmatcher.ExpectStructsToMatch(&backup_history.History{BackupConfigs: []utils.BackupConfig{}}, resultHistory)

				operating.System.Stat = os.Stat
			})
		})
		Context("history file exists", func() {
			It("creates a history object with entries from the file", func() {
				historyWithEntries := backup_history.History{
					BackupConfigs: []utils.BackupConfig{testConfig1, testConfig2}}
				historyFileContents, _ := yaml.Marshal(historyWithEntries)
				fileHandle := iohelper.MustOpenFileForWriting(historyFilePath)
				fileHandle.Write(historyFileContents)
				fileHandle.Close()
				defer os.Remove(historyFilePath)

				resultHistory := backup_history.NewHistory(historyFilePath)

				structmatcher.ExpectStructsToMatch(&historyWithEntries, resultHistory)
			})
		})
	})
	Describe("AddBackupConfig", func() {
		It("adds the most recent history entry and keeps the list sorted", func() {
			testHistory := backup_history.History{BackupConfigs: []utils.BackupConfig{testConfig3, testConfig1}}

			testHistory.AddBackupConfig(&testConfig2)

			expectedHistory := backup_history.History{BackupConfigs: []utils.BackupConfig{testConfig3, testConfig2, testConfig1}}
			structmatcher.ExpectStructsToMatch(&expectedHistory, &testHistory)
		})
	})
})
