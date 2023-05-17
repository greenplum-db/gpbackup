package integration

import (
	"fmt"
	"os"
	"path"

	"github.com/blang/semver"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/filepath"
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

			testTables = []backup.Table{{
				Relation:        backup.Relation{Oid: 0, Schema: "dataTest", Name: "testtable1"},
				TableDefinition: backup.TableDefinition{IsExternal: false},
			},
				{
					Relation:        backup.Relation{Oid: 1, Schema: "dataTest", Name: "testtable2"},
					TableDefinition: backup.TableDefinition{IsExternal: false},
				},
			}
			testhelper.AssertQueryRuns(connectionPool, `CREATE SCHEMA dataTest;`)
			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE dataTest.testtable1 (i int) DISTRIBUTED BY (i);`)
			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE dataTest.testtable2 (i int) DISTRIBUTED BY (i);`)
		})
		AfterEach(func() {
			testhelper.AssertQueryRuns(connectionPool, `DROP SCHEMA dataTest CASCADE;`)
			os.RemoveAll(fpInfo.BaseDataDir)
			backup.SetFPInfo(origFPInfo)
			utils.SetPipeThroughProgram(origPipeThroughProgram)
		})
		It("backs up multiple tables with valid data", func() {
			Expect(func() { backup.BackupDataForAllTables(testTables) }).ShouldNot(Panic())

			// Assert that at least one segment's worth of files for both tables were written out
			_, err := os.Stat("/tmp/backup_data_test/backups/20170101/20170101010101/gpbackup_0_20170101010101_0")
			Expect(err).ToNot(HaveOccurred())
			_, err = os.Stat("/tmp/backup_data_test/backups/20170101/20170101010101/gpbackup_0_20170101010101_1")
			Expect(err).ToNot(HaveOccurred())

		})
		It("correctly errors if a piped copy command fails", func() {
			// We had a bug for a while where this would result in a permanent hang, instead of an
			// error. This coverage is meant to prevent that from reocurring in future refactors,
			// which that function needs.  We could consider doing some fancier timeout detection,
			// as waiting around for 10 minutes while this times out is not ideal.  This is good
			// for now.
			dummyPipeThrough := utils.PipeThroughProgram{
				Name:          "dummy",
				OutputCommand: "doesnotexist",
				InputCommand:  "doesnotexist",
				Extension:     ".dne",
			}
			utils.SetPipeThroughProgram(dummyPipeThrough)
			defer testhelper.ShouldPanicWithMessage("doesnotexist")
			backup.BackupDataForAllTables(testTables)
		})
	})
})
