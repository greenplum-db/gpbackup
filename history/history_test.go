package history_test

import (
	"errors"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/greenplum-db/gp-common-go-libs/structmatcher"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/filepath"
	"github.com/greenplum-db/gpbackup/history"
	"github.com/greenplum-db/gpbackup/report"
	"github.com/greenplum-db/gpbackup/utils"
	"gopkg.in/yaml.v2"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gbytes"
)

var (
	testLogfile *Buffer
)

func TestBackupHistory(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "History Suite")
}

var _ = BeforeSuite(func() {
	_, _, testLogfile = testhelper.SetupTestLogger()
})

var _ = Describe("backup/history tests", func() {
	var testConfig1, testConfig2, testConfig3, testConfigSucceed, testConfigFailed history.BackupConfig
	var historyFilePath = "/tmp/history_file.yaml"

	BeforeEach(func() {
		testConfig1 = history.BackupConfig{
			DatabaseName:     "testdb1",
			ExcludeRelations: []string{},
			ExcludeSchemas:   []string{},
			IncludeRelations: []string{"testschema.testtable1", "testschema.testtable2"},
			IncludeSchemas:   []string{},
			RestorePlan:      []history.RestorePlanEntry{},
			Timestamp:        "timestamp1",
		}
		testConfig2 = history.BackupConfig{
			DatabaseName:     "testdb2",
			ExcludeRelations: []string{},
			ExcludeSchemas:   []string{"public"},
			IncludeRelations: []string{},
			IncludeSchemas:   []string{},
			RestorePlan:      []history.RestorePlanEntry{},
			Timestamp:        "timestamp2",
		}
		testConfig3 = history.BackupConfig{
			DatabaseName:     "testdb3",
			ExcludeRelations: []string{},
			ExcludeSchemas:   []string{"public"},
			IncludeRelations: []string{},
			IncludeSchemas:   []string{},
			RestorePlan:      []history.RestorePlanEntry{},
			Timestamp:        "timestamp3",
		}
		testConfigSucceed = history.BackupConfig{
			DatabaseName:     "testdb3",
			ExcludeRelations: []string{},
			ExcludeSchemas:   []string{"public"},
			IncludeRelations: []string{},
			IncludeSchemas:   []string{},
			RestorePlan:      []history.RestorePlanEntry{},
			Timestamp:        "timestampSucceed",
			Status:           history.BackupStatusSucceed,
		}
		testConfigFailed = history.BackupConfig{
			DatabaseName:     "testdb3",
			ExcludeRelations: []string{},
			ExcludeSchemas:   []string{"public"},
			IncludeRelations: []string{},
			IncludeSchemas:   []string{},
			RestorePlan:      []history.RestorePlanEntry{},
			Timestamp:        "timestampFailed",
			Status:           history.BackupStatusFailed,
		}
		_ = os.Remove(historyFilePath)
	})

	AfterEach(func() {
		_ = os.Remove(historyFilePath)
	})
	Describe("CurrentTimestamp", func() {
		It("returns the current timestamp", func() {
			operating.System.Now = func() time.Time { return time.Date(2017, time.January, 1, 1, 1, 1, 1, time.Local) }
			expected := "20170101010101"
			actual := history.CurrentTimestamp()
			Expect(actual).To(Equal(expected))
		})
	})
	Describe("WriteToFileAndMakeReadOnly", func() {
		var fileInfo os.FileInfo
		var historyWithEntries history.History
		BeforeEach(func() {
			historyWithEntries = history.History{
				BackupConfigs: []history.BackupConfig{testConfig1, testConfig2},
			}
		})
		AfterEach(func() {
			_ = os.Remove(historyFilePath)
		})
		It("makes the file readonly after it is written", func() {
			err := historyWithEntries.WriteToFileAndMakeReadOnly(historyFilePath)
			Expect(err).ToNot(HaveOccurred())

			fileInfo, err = os.Stat(historyFilePath)
			Expect(err).ToNot(HaveOccurred())
			Expect(fileInfo.Mode().Perm()).To(Equal(os.FileMode(0444)))
		})
		It("writes file when file does not exist", func() {
			err := historyWithEntries.WriteToFileAndMakeReadOnly(historyFilePath)
			Expect(err).ToNot(HaveOccurred())

			_, err = os.Stat(historyFilePath)
			Expect(err).ToNot(HaveOccurred())
		})
		It("writes file when file exists and is writeable", func() {
			err := ioutil.WriteFile(historyFilePath, []byte{}, 0644)
			Expect(err).ToNot(HaveOccurred())

			err = historyWithEntries.WriteToFileAndMakeReadOnly(historyFilePath)
			Expect(err).ToNot(HaveOccurred())

			fileHash, err := utils.GetFileHash(historyFilePath)
			Expect(err).ToNot(HaveOccurred())

			resultHistory, historyHash, err := history.NewHistory(historyFilePath)
			Expect(err).ToNot(HaveOccurred())
			Expect(historyWithEntries).To(structmatcher.MatchStruct(resultHistory))
			Expect(fileHash).To(Equal(historyHash))
		})
		It("writes file when file exists and is readonly ", func() {
			err := ioutil.WriteFile(historyFilePath, []byte{}, 0444)
			Expect(err).ToNot(HaveOccurred())

			err = historyWithEntries.WriteToFileAndMakeReadOnly(historyFilePath)
			Expect(err).ToNot(HaveOccurred())

			fileHash, err := utils.GetFileHash(historyFilePath)
			Expect(err).ToNot(HaveOccurred())


			resultHistory, historyHash, err := history.NewHistory(historyFilePath)
			Expect(err).ToNot(HaveOccurred())
			Expect(historyWithEntries).To(structmatcher.MatchStruct(resultHistory))
			Expect(fileHash).To(Equal(historyHash))
		})
	})
	Describe("NewHistory", func() {
		It("creates a history object with entries from the file when history file exists", func() {
			historyWithEntries := history.History{
				BackupConfigs: []history.BackupConfig{testConfig1, testConfig2},
			}
			historyFileContents, _ := yaml.Marshal(historyWithEntries)
			fileHandle, err := utils.OpenFileForWrite(historyFilePath)
			Expect(err).ToNot(HaveOccurred())
			_, err = fileHandle.Write(historyFileContents)
			Expect(err).ToNot(HaveOccurred())
			err = fileHandle.Close()
			Expect(err).ToNot(HaveOccurred())

			fileHash, err := utils.GetFileHash(historyFilePath)
			Expect(err).ToNot(HaveOccurred())

			resultHistory, historyHash, err := history.NewHistory(historyFilePath)
			Expect(err).ToNot(HaveOccurred())
			Expect(historyWithEntries).To(structmatcher.MatchStruct(resultHistory))
			Expect(fileHash).To(Equal(historyHash))
		})
		Context("fatals when", func() {
			BeforeEach(func() {
				operating.System.Stat = func(string) (os.FileInfo, error) { return nil, nil }
				operating.System.OpenFileRead = func(string, int, os.FileMode) (operating.ReadCloserAt, error) { return nil, nil }
			})
			AfterEach(func() {
				operating.System = operating.InitializeSystemFunctions()
			})
			It("gpbackup_history.yaml can't be read", func() {
				operating.System.ReadFile = func(string) ([]byte, error) { return nil, errors.New("read error") }

				_, _, err := history.NewHistory("/tempfile")
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("read error"))
			})
			It("gpbackup_history.yaml is an invalid format", func() {
				operating.System.ReadFile = func(string) ([]byte, error) { return []byte("not yaml"), nil }

				_, _, err := history.NewHistory("/tempfile")
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("not yaml"))
			})
			It("NewHistory returns an empty History", func() {
				backup.SetFPInfo(filepath.FilePathInfo{UserSpecifiedBackupDir: "/tmp", UserSpecifiedSegPrefix: "/test-prefix"})
				backup.SetReport(&report.Report{})
				operating.System.ReadFile = func(string) ([]byte, error) { return []byte(""), nil }

				contents, _, err := history.NewHistory("/tempfile")
				Expect(err).ToNot(HaveOccurred())
				Expect(contents).To(Equal(&history.History{BackupConfigs: make([]history.BackupConfig, 0)}))
			})
		})
	})
	Describe("AddBackupConfig", func() {
		It("adds the most recent history entry and keeps the list sorted", func() {
			testHistory := history.History{
				BackupConfigs: []history.BackupConfig{testConfig3, testConfig1},
			}

			testHistory.AddBackupConfig(&testConfig2)

			expectedHistory := history.History{
				BackupConfigs: []history.BackupConfig{testConfig3, testConfig2, testConfig1},
			}
			Expect(expectedHistory).To(structmatcher.MatchStruct(testHistory))
		})
	})
	Describe("WriteBackupHistory", func() {
		It("appends new config when file exists", func() {
			Expect(testConfig3.EndTime).To(BeEmpty())
			simulatedEndTime := time.Now()
			operating.System.Now = func() time.Time {
				return simulatedEndTime
			}
			historyWithEntries := history.History{
				BackupConfigs: []history.BackupConfig{testConfig2, testConfig1},
			}
			historyFileContents, _ := yaml.Marshal(historyWithEntries)
			fileHandle, err := utils.OpenFileForWrite(historyFilePath)
			Expect(err).ToNot(HaveOccurred())
			_, err = fileHandle.Write(historyFileContents)
			Expect(err).ToNot(HaveOccurred())
			err = fileHandle.Close()
			Expect(err).ToNot(HaveOccurred())
			err = history.WriteBackupHistory(historyFilePath, &testConfig3)
			Expect(err).ToNot(HaveOccurred())

			fileHash, err := utils.GetFileHash(historyFilePath)
			Expect(err).ToNot(HaveOccurred())

			resultHistory, historyHash, err := history.NewHistory(historyFilePath)
			Expect(err).ToNot(HaveOccurred())
			Expect(fileHash).To(Equal(historyHash))

			testConfig3.EndTime = simulatedEndTime.Format("20060102150405")
			expectedHistory := history.History{
				BackupConfigs: []history.BackupConfig{testConfig3, testConfig2, testConfig1},
			}
			Expect(expectedHistory).To(structmatcher.MatchStruct(resultHistory))
		})
		It("writes file with new config when file does not exist", func() {
			Expect(testConfig3.EndTime).To(BeEmpty())
			simulatedEndTime := time.Now()
			operating.System.Now = func() time.Time {
				return simulatedEndTime
			}
			err := history.WriteBackupHistory(historyFilePath, &testConfig3)
			Expect(err).ToNot(HaveOccurred())

			fileHash, err := utils.GetFileHash(historyFilePath)
			Expect(err).ToNot(HaveOccurred())

			resultHistory, historyHash, err := history.NewHistory(historyFilePath)
			Expect(err).ToNot(HaveOccurred())
			Expect(fileHash).To(Equal(historyHash))

			expectedHistory := history.History{BackupConfigs: []history.BackupConfig{testConfig3}}
			Expect(expectedHistory).To(structmatcher.MatchStruct(resultHistory))
			Expect(testLogfile).To(Say("No existing backups found. Creating new backup history file."))
			Expect(testConfig3.EndTime).To(Equal(simulatedEndTime.Format("20060102150405")))
		})
	})
	Describe("FindBackupConfig", func() {
		var resultHistory *history.History
		BeforeEach(func() {
			err := history.WriteBackupHistory(historyFilePath, &testConfig1)
			Expect(err).ToNot(HaveOccurred())
			resultHistory, _, err = history.NewHistory(historyFilePath)
			Expect(err).ToNot(HaveOccurred())
			err = history.WriteBackupHistory(historyFilePath, &testConfig2)
			Expect(err).ToNot(HaveOccurred())
			resultHistory, _, err = history.NewHistory(historyFilePath)
			Expect(err).ToNot(HaveOccurred())
			err = history.WriteBackupHistory(historyFilePath, &testConfig3)
			Expect(err).ToNot(HaveOccurred())
			err = history.WriteBackupHistory(historyFilePath, &testConfigSucceed)
			Expect(err).ToNot(HaveOccurred())
			resultHistory, _, err = history.NewHistory(historyFilePath)
			Expect(err).ToNot(HaveOccurred())
			err = history.WriteBackupHistory(historyFilePath, &testConfigFailed)
			Expect(err).ToNot(HaveOccurred())
			resultHistory, _, err = history.NewHistory(historyFilePath)
			Expect(err).ToNot(HaveOccurred())
		})
		It("finds a backup config for the given timestamp", func() {
			foundConfig := resultHistory.FindBackupConfig("timestamp2")
			Expect(foundConfig).To(Equal(&testConfig2))

			foundConfig = resultHistory.FindBackupConfig("timestampSucceed")
			Expect(foundConfig).To(Equal(&testConfigSucceed))
		})
		It("returns nil when timestamp not found", func() {
			foundConfig := resultHistory.FindBackupConfig("foo")
			Expect(foundConfig).To(BeNil())
		})
		It("returns nil when timestamp is there but status is failed", func() {
			foundConfig := resultHistory.FindBackupConfig("timestampFailed")
			Expect(foundConfig).To(BeNil())
		})
	})
})
