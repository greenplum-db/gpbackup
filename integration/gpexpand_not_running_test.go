package integration

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/iohelper"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/restore"
	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/utils"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("gpexpand_sensor", func() {
	BeforeEach(func() {
		testutils.SkipIfBefore6(connectionPool)
	})

	It("should prevent gprestore from starting when gpexpand is in phase 1", func() {
		coordinatorDataDir, err := dbconn.SelectString(connectionPool, utils.CoordinatorDataDirQuery)
		if err != nil {
			Fail("cannot get coordinator data dir from db")
		}

		// SIMULATE that gpexpand is running by creating its status file
		path := filepath.Join(coordinatorDataDir, utils.GpexpandStatusFilename)
		oidFp := iohelper.MustOpenFileForWriting(path)
		err = oidFp.Close()
		Expect(err).ToNot(HaveOccurred())

		defer func() {
			err = os.Remove(path)
			if err != nil {
				Fail(fmt.Sprintf("IMPORTANT: failed to delete gpexpand.status file, which will prevent any future run of gpbackup! Manually delete file at: %s\n", path))
			}
		}()

		defer testhelper.ShouldPanicWithMessage(`[CRITICAL]:-Greenplum expansion currently in process.  Once expansion is complete, it will be possible to restart gprestore, but please note existing backup sets taken with a different cluster configuration may no longer be compatible with the newly expanded cluster configuration`)
		restore.DoSetup()
	})
	It("should prevent gprestore from starting when gpexpand is in phase 2", func() {
		postgresConn := dbconn.NewDBConnFromEnvironment("postgres")
		postgresConn.MustConnect(1)
		defer postgresConn.Close()

		testhelper.AssertQueryRuns(postgresConn, "CREATE SCHEMA gpexpand")
		defer testhelper.AssertQueryRuns(postgresConn, "DROP SCHEMA gpexpand CASCADE")
		testhelper.AssertQueryRuns(postgresConn, "CREATE TABLE gpexpand.status (status text, updated timestamp)")
		testhelper.AssertQueryRuns(postgresConn, "INSERT INTO gpexpand.status VALUES ('IN PROGRESS', now())")

		defer testhelper.ShouldPanicWithMessage(`[CRITICAL]:-Greenplum expansion currently in process.  Once expansion is complete, it will be possible to restart gprestore, but please note existing backup sets taken with a different cluster configuration may no longer be compatible with the newly expanded cluster configuration`)
		restore.DoSetup()
	})
	It("should prevent gpbackup from starting when gpexpand is in phase 1", func() {
		coordinatorDataDir, err := dbconn.SelectString(connectionPool, utils.CoordinatorDataDirQuery)
		if err != nil {
			Fail("cannot get coordinator data dir from db")
		}

		// SIMULATE that gpexpand is running by creating its status file
		path := filepath.Join(coordinatorDataDir, utils.GpexpandStatusFilename)
		oidFp := iohelper.MustOpenFileForWriting(path)
		err = oidFp.Close()
		Expect(err).ToNot(HaveOccurred())

		defer func() {
			err = os.Remove(path)
			if err != nil {
				Fail(fmt.Sprintf("IMPORTANT: failed to delete gpexpand.status file, which will prevent any future run of gpbackup! Manually delete file at: %s\n", path))
			}
		}()

		defer testhelper.ShouldPanicWithMessage(`[CRITICAL]:-Greenplum expansion currently in process, please re-run gpbackup when the expansion has completed`)
		backup.DoSetup()
	})
	It("should prevent gpbackup from starting when gpexpand is in phase 2", func() {
		postgresConn := dbconn.NewDBConnFromEnvironment("postgres")
		postgresConn.MustConnect(1)
		defer postgresConn.Close()

		testhelper.AssertQueryRuns(postgresConn, "CREATE SCHEMA gpexpand")
		defer testhelper.AssertQueryRuns(postgresConn, "DROP SCHEMA gpexpand CASCADE")
		testhelper.AssertQueryRuns(postgresConn, "CREATE TABLE gpexpand.status (status text, updated timestamp)")
		testhelper.AssertQueryRuns(postgresConn, "INSERT INTO gpexpand.status VALUES ('IN PROGRESS', now())")

		defer testhelper.ShouldPanicWithMessage(`[CRITICAL]:-Greenplum expansion currently in process, please re-run gpbackup when the expansion has completed`)
		backup.DoSetup()
	})
})
