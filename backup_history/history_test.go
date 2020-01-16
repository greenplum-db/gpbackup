package backup_history_test

import (
	"errors"
	"io/ioutil"
	"os"
	"time"

	"github.com/greenplum-db/gp-common-go-libs/iohelper"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/greenplum-db/gp-common-go-libs/structmatcher"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/backup_filepath"
	"github.com/greenplum-db/gpbackup/backup_history"
	"github.com/greenplum-db/gpbackup/report"
	"github.com/onsi/gomega/gbytes"
	"gopkg.in/yaml.v2"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup/history tests", func() {
	var testConfig1, testConfig2, testConfig3 backup_history.BackupConfig
	var historyFilePath = "/tmp/history_file.yaml"

	BeforeEach(func() {
		testConfig1 = backup_history.BackupConfig{
			DatabaseName:     "testdb1",
			ExcludeRelations: []string{},
			ExcludeSchemas:   []string{},
			IncludeRelations: []string{"testschema.testtable1", "testschema.testtable2"},
			IncludeSchemas:   []string{},
			RestorePlan:      []backup_history.RestorePlanEntry{},
			Timestamp:        "timestamp1",
		}
		testConfig2 = backup_history.BackupConfig{
			DatabaseName:     "testdb2",
			ExcludeRelations: []string{},
			ExcludeSchemas:   []string{"public"},
			IncludeRelations: []string{},
			IncludeSchemas:   []string{},
			RestorePlan:      []backup_history.RestorePlanEntry{},
			Timestamp:        "timestamp2",
		}
		testConfig3 = backup_history.BackupConfig{
			DatabaseName:     "testdb3",
			ExcludeRelations: []string{},
			ExcludeSchemas:   []string{"public"},
			IncludeRelations: []string{},
			IncludeSchemas:   []string{},
			RestorePlan:      []backup_history.RestorePlanEntry{},
			Timestamp:        "timestamp3",
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
			actual := backup_history.CurrentTimestamp()
			Expect(actual).To(Equal(expected))
		})
	})
	Describe("WriteToFileAndMakeReadOnly", func() {
		var fileInfo os.FileInfo
		var historyWithEntries backup_history.History
		BeforeEach(func() {
			historyWithEntries = backup_history.History{
				BackupConfigs: []backup_history.BackupConfig{testConfig1, testConfig2},
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

			resultHistory, err := backup_history.NewHistory(historyFilePath)
			Expect(err).ToNot(HaveOccurred())
			structmatcher.ExpectStructsToMatch(&historyWithEntries, resultHistory)
		})
		It("writes file when file exists and is readonly ", func() {
			err := ioutil.WriteFile(historyFilePath, []byte{}, 0444)
			Expect(err).ToNot(HaveOccurred())

			err = historyWithEntries.WriteToFileAndMakeReadOnly(historyFilePath)
			Expect(err).ToNot(HaveOccurred())

			resultHistory, err := backup_history.NewHistory(historyFilePath)
			Expect(err).ToNot(HaveOccurred())
			structmatcher.ExpectStructsToMatch(&historyWithEntries, resultHistory)
		})
	})
	Describe("NewHistory", func() {
		It("creates a history object with entries from the file when history file exists", func() {
			historyWithEntries := backup_history.History{
				BackupConfigs: []backup_history.BackupConfig{testConfig1, testConfig2},
			}
			historyFileContents, _ := yaml.Marshal(historyWithEntries)
			fileHandle := iohelper.MustOpenFileForWriting(historyFilePath)
			_, _ = fileHandle.Write(historyFileContents)
			_ = fileHandle.Close()

			resultHistory, err := backup_history.NewHistory(historyFilePath)
			Expect(err).ToNot(HaveOccurred())

			structmatcher.ExpectStructsToMatch(&historyWithEntries, resultHistory)
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

				_, err := backup_history.NewHistory("/tempfile")
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("read error"))
			})
			It("gpbackup_history.yaml is an invalid format", func() {
				operating.System.ReadFile = func(string) ([]byte, error) { return []byte("not yaml"), nil }

				_, err := backup_history.NewHistory("/tempfile")
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("not yaml"))
			})
			It("NewHistory returns an empty History", func() {
				backup.SetFPInfo(backup_filepath.FilePathInfo{UserSpecifiedBackupDir: "/tmp", UserSpecifiedSegPrefix: "/test-prefix"})
				backup.SetReport(&report.Report{})
				operating.System.ReadFile = func(string) ([]byte, error) { return []byte(""), nil }

				history, err := backup_history.NewHistory("/tempfile")
				Expect(err).ToNot(HaveOccurred())
				Expect(history).To(Equal(&backup_history.History{BackupConfigs: make([]backup_history.BackupConfig, 0)}))
			})
		})
	})
	Describe("AddBackupConfig", func() {
		It("adds the most recent history entry and keeps the list sorted", func() {
			testHistory := backup_history.History{
				BackupConfigs: []backup_history.BackupConfig{testConfig3, testConfig1},
			}

			testHistory.AddBackupConfig(&testConfig2)

			expectedHistory := backup_history.History{
				BackupConfigs: []backup_history.BackupConfig{testConfig3, testConfig2, testConfig1},
			}
			structmatcher.ExpectStructsToMatch(&expectedHistory, &testHistory)
		})
	})
	Describe("WriteBackupHistory", func() {
		It("appends new config when file exists", func() {
			Expect(testConfig3.EndTime).To(BeEmpty())
			simulatedEndTime := time.Now()
			operating.System.Now = func() time.Time {
				return simulatedEndTime
			}
			historyWithEntries := backup_history.History{
				BackupConfigs: []backup_history.BackupConfig{testConfig2, testConfig1},
			}
			historyFileContents, _ := yaml.Marshal(historyWithEntries)
			fileHandle := iohelper.MustOpenFileForWriting(historyFilePath)
			_, _ = fileHandle.Write(historyFileContents)
			_ = fileHandle.Close()

			err := backup_history.WriteBackupHistory(historyFilePath, &testConfig3)
			Expect(err).ToNot(HaveOccurred())

			resultHistory, err := backup_history.NewHistory(historyFilePath)
			Expect(err).ToNot(HaveOccurred())
			testConfig3.EndTime = simulatedEndTime.Format("20060102150405")
			expectedHistory := backup_history.History{
				BackupConfigs: []backup_history.BackupConfig{testConfig3, testConfig2, testConfig1},
			}
			structmatcher.ExpectStructsToMatch(&expectedHistory, resultHistory)
		})
		It("writes file with new config when file does not exist", func() {
			Expect(testConfig3.EndTime).To(BeEmpty())
			simulatedEndTime := time.Now()
			operating.System.Now = func() time.Time {
				return simulatedEndTime
			}
			err := backup_history.WriteBackupHistory(historyFilePath, &testConfig3)
			Expect(err).ToNot(HaveOccurred())

			resultHistory, err := backup_history.NewHistory(historyFilePath)
			Expect(err).ToNot(HaveOccurred())
			expectedHistory := backup_history.History{BackupConfigs: []backup_history.BackupConfig{testConfig3}}
			structmatcher.ExpectStructsToMatch(&expectedHistory, resultHistory)
			Expect(testLogfile).To(gbytes.Say("No existing backups found. Creating new backup history file."))
			Expect(testConfig3.EndTime).To(Equal(simulatedEndTime.Format("20060102150405")))
		})
	})
	Describe("FindBackupConfig", func() {
		var resultHistory *backup_history.History
		BeforeEach(func() {
			err := backup_history.WriteBackupHistory(historyFilePath, &testConfig1)
			Expect(err).ToNot(HaveOccurred())
			resultHistory, err = backup_history.NewHistory(historyFilePath)
			Expect(err).ToNot(HaveOccurred())
			err = backup_history.WriteBackupHistory(historyFilePath, &testConfig2)
			Expect(err).ToNot(HaveOccurred())
			resultHistory, err = backup_history.NewHistory(historyFilePath)
			Expect(err).ToNot(HaveOccurred())
			err = backup_history.WriteBackupHistory(historyFilePath, &testConfig3)
			Expect(err).ToNot(HaveOccurred())
		})
		It("finds a backup config for the given timestamp", func() {
			foundConfig := resultHistory.FindBackupConfig("timestamp2")
			Expect(foundConfig).To(Equal(&testConfig2))
		})
		It("returns nil when timestamp not found", func() {
			foundConfig := resultHistory.FindBackupConfig("foo")
			Expect(foundConfig).To(BeNil())
		})
	})
})
