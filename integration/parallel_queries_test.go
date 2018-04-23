package integration

import (
	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/restore"
	"github.com/greenplum-db/gpbackup/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup, utils, and restore integration tests related to parallelism", func() {
	Describe("Connection pooling tests", func() {
		var tempConn *dbconn.DBConn
		BeforeEach(func() {
			tempConn = dbconn.NewDBConnFromEnvironment("testdb")
			tempConn.MustConnect(2)
		})
		AfterEach(func() {
			tempConn.Close()
		})
		It("exhibits session-like behavior when successive queries are executed on the same connection", func() {
			tempConn.Exec("SET client_min_messages TO error;", 1)
			/*
			 * The default value of client_min_messages is "notice", so now connection 1
			 * should have it set to "error" and 0 should still have it set to "notice".
			 */
			notInSession := dbconn.MustSelectString(tempConn, "SELECT setting AS string FROM pg_settings WHERE name = 'client_min_messages';", 0)
			inSession := dbconn.MustSelectString(tempConn, "SELECT setting AS string FROM pg_settings WHERE name = 'client_min_messages';", 1)
			Expect(notInSession).To(Equal("notice"))
			Expect(inSession).To(Equal("error"))
		})
	})
	Describe("Parallel statement execution tests", func() {
		/*
		 * We can't inspect goroutines directly to check for parallelism without
		 * adding runtime hooks to the code, so we test parallelism by executing
		 * statements with varying pg_sleep durations.  In the serial case these
		 * statements will complete in order of execution, while in the parallel
		 * case they will complete in order of increasing sleep duration.
		 *
		 * Because a call to now() will record the timestamp at the start of the
		 * session instead of the timestamp after the pg_sleep call, we must add
		 * the sleep duration to the now() timestamp to get an accurate result.
		 *
		 * Using sleep durations on the order of 0.5 seconds will slow down test
		 * runs slightly, but this is necessary to overcome query execution time
		 * fluctuations.
		 */
		first := "SELECT pg_sleep(0.5); INSERT INTO public.timestamps VALUES (1, now() + '0.5 seconds'::interval);"
		second := "SELECT pg_sleep(1.5); INSERT INTO public.timestamps VALUES (2, now() + '1.5 seconds'::interval);"
		third := "INSERT INTO public.timestamps VALUES (3, now());"
		fourth := "SELECT pg_sleep(1); INSERT INTO public.timestamps VALUES (4, now() + '1 second'::interval);"
		statements := []utils.StatementWithType{
			{ObjectType: "TABLE", Statement: first},
			{ObjectType: "DATABASE", Statement: second},
			{ObjectType: "SEQUENCE", Statement: third},
			{ObjectType: "DATABASE", Statement: fourth},
		}
		/*
		 * We use a separate connection even for serial runs to avoid losing the
		 * configuration of the main connection variable.
		 */
		var tempConn *dbconn.DBConn
		createQuery := "CREATE TABLE public.timestamps(exec_index int, exec_time timestamp);"
		orderQuery := "SELECT exec_index AS string FROM public.timestamps ORDER BY exec_time;"
		BeforeEach(func() {
			restore.SetOnErrorContinue(false)
			tempConn = dbconn.NewDBConnFromEnvironment("testdb")
			restore.SetConnection(tempConn)
		})
		AfterEach(func() {
			testhelper.AssertQueryRuns(tempConn, "DROP TABLE public.timestamps;")
			tempConn.Close()
			tempConn = nil
			restore.SetConnection(connection)
		})
		Context("Serial execution", func() {
			BeforeEach(func() {
				tempConn.MustConnect(1)
				testhelper.AssertQueryRuns(tempConn, "SET ROLE testrole")
				testhelper.AssertQueryRuns(tempConn, createQuery)
			})
			It("can execute all statements in the list serially", func() {
				expectedOrderArray := []string{"1", "2", "3", "4"}
				restore.ExecuteStatementsAndCreateProgressBar(statements, "", utils.PB_NONE, false)
				resultOrderArray := dbconn.MustSelectStringSlice(tempConn, orderQuery)
				Expect(resultOrderArray).To(Equal(expectedOrderArray))
			})
		})
		Context("Parallel execution", func() {
			BeforeEach(func() {
				tempConn.MustConnect(3)
				testhelper.AssertQueryRuns(tempConn, "SET ROLE testrole")
				testhelper.AssertQueryRuns(tempConn, createQuery)
			})
			It("can execute all statements in the list in parallel", func() {
				expectedOrderArray := []string{"3", "1", "4", "2"}
				restore.ExecuteStatementsAndCreateProgressBar(statements, "", utils.PB_NONE, true)
				resultOrderArray := dbconn.MustSelectStringSlice(tempConn, orderQuery)
				Expect(resultOrderArray).To(Equal(expectedOrderArray))
			})
		})
	})
})
