package backup_test

import (
	"os"

	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/greenplum-db/gp-common-go-libs/structmatcher"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/filepath"
	"github.com/greenplum-db/gpbackup/history"
	"github.com/greenplum-db/gpbackup/options"
	"github.com/greenplum-db/gpbackup/report"
	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/toc"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gbytes"
)

func basicBackupConfig(databaseName string, timestamp string, status string) history.BackupConfig {
	return history.BackupConfig{
		DatabaseName:       databaseName,
		Timestamp:          timestamp,
		Status:             status,
		ExcludeRelations:   []string{},
		ExcludeSchemas:     []string{},
		IncludeRelations:   []string{},
		IncludeSchemas:     []string{},
		RestorePlan:        []history.RestorePlanEntry{},
		Sections:           options.Sections{},
		DeprecatedMetadata: options.DeprecatedMetadata{},
	}
}

var _ = Describe("backup/incremental tests", func() {
	Describe("FilterTablesForIncremental", func() {
		defaultEntry := toc.AOEntry{
			Modcount:         0,
			LastDDLTimestamp: "00000",
		}
		prevTOC := toc.TOC{
			IncrementalMetadata: toc.IncrementalEntries{
				AO: map[string]toc.AOEntry{
					"public.ao_changed_modcount":  defaultEntry,
					"public.ao_changed_timestamp": defaultEntry,
					"public.ao_unchanged":         defaultEntry,
				},
			},
		}

		currTOC := toc.TOC{
			IncrementalMetadata: toc.IncrementalEntries{
				AO: map[string]toc.AOEntry{
					"public.ao_changed_modcount": {
						Modcount:         2,
						LastDDLTimestamp: "00000",
					},
					"public.ao_changed_timestamp": {
						Modcount:         0,
						LastDDLTimestamp: "00001",
					},
					"public.ao_unchanged": defaultEntry,
				},
			},
		}

		tblHeap := backup.Table{Relation: backup.Relation{Schema: "public", Name: "heap"}}
		tblAOChangedModcount := backup.Table{Relation: backup.Relation{Schema: "public", Name: "ao_changed_modcount"}}
		tblAOChangedTS := backup.Table{Relation: backup.Relation{Schema: "public", Name: "ao_changed_timestamp"}}
		tblAOUnchanged := backup.Table{Relation: backup.Relation{Schema: "public", Name: "ao_unchanged"}}
		tables := []backup.Table{
			tblHeap,
			tblAOChangedModcount,
			tblAOChangedTS,
			tblAOUnchanged,
		}

		filteredTables := backup.FilterTablesForIncremental(&prevTOC, &currTOC, tables)

		It("Should include the heap table in the filtered list", func() {
			Expect(filteredTables).To(ContainElement(tblHeap))
		})

		It("Should include the AO table having a modified modcount", func() {
			Expect(filteredTables).To(ContainElement(tblAOChangedModcount))
		})

		It("Should include the AO table having a modified last DDL timestamp", func() {
			Expect(filteredTables).To(ContainElement(tblAOChangedTS))
		})

		It("Should NOT include the unmodified AO table", func() {
			Expect(filteredTables).To(Not(ContainElement(tblAOUnchanged)))
		})
	})

	Describe("GetLatestMatchingBackupConfig", func() {
		historyDBPath := "/tmp/hist.db"
		contents := []history.BackupConfig{
			basicBackupConfig("test2", "timestamp4", history.BackupStatusFailed),
			basicBackupConfig("test1", "timestamp3", history.BackupStatusSucceed),
			basicBackupConfig("test2", "timestamp2", history.BackupStatusSucceed),
			basicBackupConfig("test1", "timestamp1", history.BackupStatusSucceed),
		}
		BeforeEach(func() {
			os.Remove(historyDBPath)
			historyDB, _ := history.InitializeHistoryDatabase(historyDBPath)
			for _, backupConfig := range contents {
				history.StoreBackupHistory(historyDB, &backupConfig)
			}
			historyDB.Close()
		})

		AfterEach(func() {
			os.Remove(historyDBPath)
		})

		It("Should return the latest backup's timestamp with matching Dbname", func() {
			currentBackupConfig := history.BackupConfig{DatabaseName: "test1"}
			latestBackupHistoryEntry := backup.GetLatestMatchingBackupConfig(historyDBPath, &currentBackupConfig)
			// endtime is set dynamically on storage, so force it to match
			contents[1].EndTime = latestBackupHistoryEntry.EndTime
			structmatcher.ExpectStructsToMatch(contents[1], latestBackupHistoryEntry)
		})
		It("Should return the latest matching backup's timestamp that did not fail", func() {
			currentBackupConfig := history.BackupConfig{DatabaseName: "test2"}
			latestBackupHistoryEntry := backup.GetLatestMatchingBackupConfig(historyDBPath, &currentBackupConfig)
			contents[2].EndTime = latestBackupHistoryEntry.EndTime
			structmatcher.ExpectStructsToMatch(contents[2], latestBackupHistoryEntry)
		})
		It("should return nil with no matching Dbname", func() {
			currentBackupConfig := history.BackupConfig{DatabaseName: "test3"}
			latestBackupHistoryEntry := backup.GetLatestMatchingBackupConfig(historyDBPath, &currentBackupConfig)
			Expect(latestBackupHistoryEntry).To(BeNil())
		})
		It("should return nil with an empty history", func() {
			currentBackupConfig := history.BackupConfig{}
			os.Remove(historyDBPath)
			latestBackupHistoryEntry := backup.GetLatestMatchingBackupConfig(historyDBPath, &currentBackupConfig)
			Expect(latestBackupHistoryEntry).To(BeNil())
		})
	})

	Describe("PopulateRestorePlan", func() {
		testCluster := testutils.SetDefaultSegmentConfiguration()
		testFPInfo := filepath.NewFilePathInfo(testCluster, "", "ts0",
			"gpseg", false)
		backup.SetFPInfo(testFPInfo)

		Context("Full backup", func() {
			restorePlan := make([]history.RestorePlanEntry, 0)
			backupSetTables := []backup.Table{
				{Relation: backup.Relation{Schema: "public", Name: "ao1"}},
				{Relation: backup.Relation{Schema: "public", Name: "heap1"}},
			}
			allTables := backupSetTables

			restorePlan = backup.PopulateRestorePlan(backupSetTables, restorePlan, allTables)

			It("Should populate a restore plan with a single entry", func() {
				Expect(restorePlan).To(HaveLen(1))
			})

			Specify("That the single entry should have the latest timestamp", func() {
				Expect(restorePlan[0].Timestamp).To(Equal("ts0"))
			})

			Specify("That the single entry should have the current backup set FQNs", func() {
				expectedTableFQNs := []string{"public.ao1", "public.heap1"}

				Expect(restorePlan[0].TableFQNs).To(Equal(expectedTableFQNs))
			})
		})

		Context("Incremental backup", func() {
			previousRestorePlan := []history.RestorePlanEntry{
				{Timestamp: "ts0", TableFQNs: []string{"public.ao1", "public.ao2"}},
				{Timestamp: "ts1", TableFQNs: []string{"public.heap1"}},
			}
			changedTables := []backup.Table{
				{Relation: backup.Relation{Schema: "public", Name: "ao1"}},
				{Relation: backup.Relation{Schema: "public", Name: "heap1"}},
			}

			Context("Incremental backup with no table drops in between", func() {
				allTables := changedTables

				restorePlan := backup.PopulateRestorePlan(changedTables, previousRestorePlan, allTables)

				It("should append 1 more entry to the previous restore plan", func() {
					Expect(restorePlan[0:2]).To(Equal(previousRestorePlan[0:2]))
					Expect(restorePlan).To(HaveLen(len(previousRestorePlan) + 1))
				})

				Specify("That the added entry should have the current backup set FQNs", func() {
					expectedTableFQNs := []string{"public.ao1", "public.heap1"}

					Expect(restorePlan[2].TableFQNs).To(Equal(expectedTableFQNs))
				})

				Specify("That the previous timestamp entries should NOT have the current backup set FQNs", func() {
					expectedTableFQNs := []string{"public.ao1", "public.heap1"}

					Expect(restorePlan[0].TableFQNs).To(Not(ContainElement(expectedTableFQNs[0])))
					Expect(restorePlan[0].TableFQNs).To(Not(ContainElement(expectedTableFQNs[1])))

					Expect(restorePlan[1].TableFQNs).To(Not(ContainElement(expectedTableFQNs[0])))
					Expect(restorePlan[1].TableFQNs).To(Not(ContainElement(expectedTableFQNs[1])))
				})

			})

			Context("A table was dropped between the last full/incremental and this incremental", func() {
				allTables := changedTables[0:1] // exclude "heap1"
				excludedTableFQN := "public.heap1"

				restorePlan := backup.PopulateRestorePlan(changedTables[0:1], previousRestorePlan, allTables)

				Specify("That the added entry should NOT have the dropped table FQN", func() {
					Expect(restorePlan[2].TableFQNs).To(Not(ContainElement(excludedTableFQN)))
				})

				Specify("That the previous timestamp entries should NOT have the dropped table FQN", func() {
					Expect(restorePlan[0].TableFQNs).To(Not(ContainElement(excludedTableFQN)))
					Expect(restorePlan[1].TableFQNs).To(Not(ContainElement(excludedTableFQN)))
				})

			})
		})

	})
	Describe("GetLatestMatchingBackupTimestamp", func() {
		var log *Buffer
		BeforeEach(func() {
			_, _, log = testhelper.SetupTestLogger()
		})
		AfterEach(func() {
			operating.InitializeSystemFunctions()
		})
		It("fatals when trying to take an incremental backup without a full backup", func() {
			backup.SetFPInfo(filepath.FilePathInfo{UserSpecifiedBackupDir: "/tmp", UserSpecifiedSegPrefix: "/test-prefix"})
			backup.SetReport(&report.Report{})

			Expect(func() { backup.GetLatestMatchingBackupTimestamp() }).Should(Panic())
			Expect(log.Contents()).To(ContainSubstring("There was no matching previous backup found with the flags provided. Please take a full backup."))

		})
	})
})
