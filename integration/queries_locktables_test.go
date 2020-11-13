package integration

import (
	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/spf13/cobra"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Queries ", func() {
	Describe("BackupJobsLockTables", func() {
		var jobsConnectionPool *dbconn.DBConn
		BeforeEach(func() {
			gplog.SetVerbosity(gplog.LOGERROR) // turn off progress bar in the lock-table routine
			var rootCmd = &cobra.Command{}
			backup.DoInit(rootCmd) // initialize the ObjectCount
			jobsConnectionPool = dbconn.NewDBConnFromEnvironment("testdb")
		})
		AfterEach(func() {
			if jobsConnectionPool != nil {
				jobsConnectionPool.Close()
			}
		})
		It("grab as many access share locks as connections", func() {
			jobsConnectionPool.MustConnect(3)
			for connNum := 0; connNum < jobsConnectionPool.NumConns; connNum++ {
				jobsConnectionPool.MustBegin(connNum)
			}
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.backup_jobs_locktables(i int);")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.backup_jobs_locktables;")

			tableRelations := []backup.Relation {backup.Relation{0, 0, "public", "backup_jobs_locktables"}}
			backup.LockTables(jobsConnectionPool, tableRelations)

			checkLockQuery := `SELECT count(*) FROM pg_locks l, pg_class c, pg_namespace n WHERE l.relation = c.oid AND n.oid = c.relnamespace AND n.nspname = 'public' AND c.relname = 'backup_jobs_locktables' AND l.granted = 't'`
			var lockCount int
			_ = connectionPool.Get(&lockCount, checkLockQuery)
			Expect(lockCount).To(Equal(3))

			for connNum := 0; connNum < jobsConnectionPool.NumConns; connNum++ {
				jobsConnectionPool.MustCommit(connNum)
			}
		})
	})
})
