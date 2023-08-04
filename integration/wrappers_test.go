package integration

import (
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/options"
	"github.com/spf13/cobra"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Wrappers Integration", func() {
	Describe("RetrieveAndProcessTables", func() {
		BeforeEach(func() {
			gplog.SetVerbosity(gplog.LOGERROR) // turn off progress bar in the lock-table routine
			var rootCmd = &cobra.Command{}
			backup.DoInit(rootCmd) // initialize the ObjectCount
		})
		It("returns the data tables that have names with special characters", func() {
			_ = backupCmdFlags.Set(options.INCLUDE_RELATION, "public.foo")
			_ = backupCmdFlags.Set(options.INCLUDE_RELATION, "public.BAR")

			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.foo(i int); INSERT INTO public.foo VALUES (1);")
			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE public."BAR"(i int); INSERT INTO public."BAR" VALUES (1);`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.foo;")
			defer testhelper.AssertQueryRuns(connectionPool, `DROP TABLE public."BAR";`)

			subject, err := options.NewOptions(backupCmdFlags)
			Expect(err).To(Not(HaveOccurred()))
			backup.ValidateAndProcessFilterLists(subject)

			// every backup occurs in a transaction; we are testing a small part of that backup
			connectionPool.MustBegin(0)
			defer connectionPool.MustCommit(0)

			_, dataTables := backup.RetrieveAndProcessTables()
			Expect(len(dataTables)).To(Equal(2))
			Expect(dataTables[0].Name).To(Equal("foo"))
			Expect(dataTables[1].Name).To(Equal(`"BAR"`))
		})
	})
})
