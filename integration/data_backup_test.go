package integration

import (
	"fmt"
	"os"
	"path"

	"github.com/blang/semver"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/filepath"
	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/utils"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup integration tests", func() {
	Describe("BackupDataForAllTables", func() {
		var (
			testTables             []backup.Table
			fpInfo                 filepath.FilePathInfo
			origFPInfo             filepath.FilePathInfo
			origPipeThroughProgram utils.PipeThroughProgram
		)
		BeforeEach(func() {
			if connectionPool.Version.Before(backup.SNAPSHOT_GPDB_MIN_VERSION) {
				Skip(fmt.Sprintf("Test only applicable to GPDB %s and above", backup.SNAPSHOT_GPDB_MIN_VERSION))
			}
			if useOldBackupVersion && oldBackupSemVer.LT(semver.MustParse("1.29.0")) {
				Skip(fmt.Sprintf("Test does not apply to gpbackup gpbackup %s", oldBackupSemVer))
			}

			fpInfo = filepath.NewFilePathInfo(testCluster, "", "20170101010101", "gpseg")
			fpInfo.BaseDataDir = "/tmp/backup_data_test"
			os.MkdirAll(path.Join(fpInfo.BaseDataDir, "backups", "20170101", "20170101010101"), 0777)

			backup.SetCluster(testCluster)

			origFPInfo = backup.GetFPInfo()
			backup.SetFPInfo(fpInfo)

			origPipeThroughProgram = utils.GetPipeThroughProgram()

			testhelper.AssertQueryRuns(connectionPool, `CREATE SCHEMA dataTest;`)
		})
		AfterEach(func() {
			testhelper.AssertQueryRuns(connectionPool, `DROP SCHEMA dataTest CASCADE;`)
			os.RemoveAll(fpInfo.BaseDataDir)
			backup.SetFPInfo(origFPInfo)
			utils.SetPipeThroughProgram(origPipeThroughProgram)
		})
		It("backs up multiple tables with valid data", func() {

			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE dataTest.testtable1 (i int) DISTRIBUTED BY (i);`)
			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE dataTest.testtable2 (i int) DISTRIBUTED BY (i);`)
			testTables = []backup.Table{{
				Relation:        backup.Relation{Oid: 0, Schema: "dataTest", Name: "testtable1"},
				TableDefinition: backup.TableDefinition{IsExternal: false},
			},
				{
					Relation:        backup.Relation{Oid: 1, Schema: "dataTest", Name: "testtable2"},
					TableDefinition: backup.TableDefinition{IsExternal: false},
				},
			}
			// set up a backupsnapshot to ensure the code flow we're testing is the intended case
			connectionPool.MustBegin()
			testSnapshot, err := backup.GetSynchronizedSnapshot(connectionPool)
			Expect(err).ToNot(HaveOccurred())
			backup.SetBackupSnapshot(testSnapshot)
			Expect(err).ToNot(HaveOccurred())

			Expect(func() { backup.BackupDataForAllTables(testTables) }).ShouldNot(Panic())

			connectionPool.MustCommit()

			// Assert that at least one segment's worth of files for both tables were written out
			_, err = os.Stat("/tmp/backup_data_test/backups/20170101/20170101010101/gpbackup_0_20170101010101_0")
			Expect(err).ToNot(HaveOccurred())
			_, err = os.Stat("/tmp/backup_data_test/backups/20170101/20170101010101/gpbackup_0_20170101010101_1")
			Expect(err).ToNot(HaveOccurred())

		})
		It("correctly errors if a piped copy command fails", func() {
			// We had a bug for a while where this would result in a permanent hang, instead of an
			// error. This coverage is meant to prevent that from reocurring in future refactors,
			// which that function needs.
			dummyPipeThrough := utils.PipeThroughProgram{
				Name:          "dummy",
				OutputCommand: "doesnotexist",
				InputCommand:  "doesnotexist",
				Extension:     ".dne",
			}
			utils.SetPipeThroughProgram(dummyPipeThrough)

			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE dataTest.test2table1 (i int) DISTRIBUTED BY (i);`)
			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE dataTest.test2table2 (i int) DISTRIBUTED BY (i);`)
			testTables = []backup.Table{{
				Relation:        backup.Relation{Oid: 0, Schema: "dataTest", Name: "test2table1"},
				TableDefinition: backup.TableDefinition{IsExternal: false},
			},
				{
					Relation:        backup.Relation{Oid: 1, Schema: "dataTest", Name: "test2table2"},
					TableDefinition: backup.TableDefinition{IsExternal: false},
				},
			}
			connectionPool.MustBegin()

			testSnapshot, err := backup.GetSynchronizedSnapshot(connectionPool)
			Expect(err).ToNot(HaveOccurred())
			backup.SetBackupSnapshot(testSnapshot)

			Expect(func() { backup.BackupDataForAllTables(testTables) }).Should(Panic())

			connectionPool.MustRollback()

			// Terminate the hanging copy command or it breaks test suite cleanup. We do not need
			// to worry about GPDB5- syntax here because these tests don't apply to that
			cleanupConn := testutils.SetupTestDbConn("testdb")
			defer cleanupConn.Close()
			query := `
			    SELECT pg_terminate_backend(pid)
			    FROM pg_stat_activity
			    WHERE application_name = ''
			    AND query like '%COPY%PROGRAM%doesnotexist%'
                AND pid <> pg_backend_pid();`
			cleanupConn.MustExec(query)

		})

		It("correctly errors if an unexpected error occurs taking a lock", func() {

			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE dataTest.test3table1 (i int) DISTRIBUTED BY (i);`)
			// do not create second table, so taking a lock on it will error
			testTables = []backup.Table{{
				Relation:        backup.Relation{Oid: 0, Schema: "dataTest", Name: "test3table1"},
				TableDefinition: backup.TableDefinition{IsExternal: false},
			},
				{
					Relation:        backup.Relation{Oid: 1, Schema: "dataTest", Name: "test3table2"},
					TableDefinition: backup.TableDefinition{IsExternal: false},
				},
			}
			connectionPool.MustBegin()

			testSnapshot, err := backup.GetSynchronizedSnapshot(connectionPool)
			Expect(err).ToNot(HaveOccurred())
			backup.SetBackupSnapshot(testSnapshot)

			Expect(func() { backup.BackupDataForAllTables(testTables) }).Should(Panic())

			connectionPool.MustRollback()
		})
	})
})
