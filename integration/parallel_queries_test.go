package integration

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/options"
	"github.com/greenplum-db/gpbackup/restore"
	"github.com/greenplum-db/gpbackup/toc"
	"github.com/greenplum-db/gpbackup/utils"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gbytes"
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
			tempConn = nil
		})
		It("exhibits session-like behavior when successive queries are executed on the same connection", func() {
			_, _ = tempConn.Exec("SET client_min_messages TO error;", 1)
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
		/*
		 * We use a separate connection even for serial runs to avoid losing the
		 * configuration of the main connection variable.
		 */
		var tempConn *dbconn.DBConn
		orderQuery := "SELECT exec_index AS string FROM public.timestamps ORDER BY exec_time;"
		BeforeEach(func() {
			tempConn = dbconn.NewDBConnFromEnvironment("testdb")
			restore.SetConnection(tempConn)
			tempConn.MustConnect(4)
			testhelper.AssertQueryRuns(tempConn, "SET ROLE testrole")

		})
		AfterEach(func() {
			tempConn.Close()
			tempConn = nil
			restore.SetConnection(connectionPool)
		})
		Context("no errors", func() {
			first := "SELECT pg_sleep(0.5); INSERT INTO public.timestamps VALUES (1, now() + '0.5 seconds'::interval);"
			second := "SELECT pg_sleep(1.5); INSERT INTO public.timestamps VALUES (2, now() + '1.5 seconds'::interval);"
			third := "INSERT INTO public.timestamps VALUES (3, now());"
			fourth := "SELECT pg_sleep(1); INSERT INTO public.timestamps VALUES (4, now() + '1 second'::interval);"
			statements := []toc.StatementWithType{
				{ObjectType: toc.OBJ_TABLE, Statement: first, Tier: []uint32{0, 0}},
				{ObjectType: toc.OBJ_DATABASE, Statement: second, Tier: []uint32{0, 0}},
				{ObjectType: toc.OBJ_SEQUENCE, Statement: third, Tier: []uint32{0, 0}},
				{ObjectType: toc.OBJ_DATABASE, Statement: fourth, Tier: []uint32{0, 0}},
			}
			BeforeEach(func() {
				testhelper.AssertQueryRuns(tempConn, "CREATE TABLE public.timestamps(exec_index int, exec_time timestamp);")
			})
			AfterEach(func() {
				testhelper.AssertQueryRuns(tempConn, "DROP TABLE public.timestamps;")
			})
			It("can execute all statements in the list serially", func() {
				expectedOrderArray := []string{"1", "2", "3", "4"}
				restore.ExecuteStatementsAndCreateProgressBar(statements, "", utils.PB_NONE, false)
				resultOrderArray := dbconn.MustSelectStringSlice(tempConn, orderQuery)
				Expect(resultOrderArray).To(Equal(expectedOrderArray))
			})

			It("can execute all statements in the list in parallel", func() {
				expectedOrderArray := []string{"1", "2", "3", "4"}
				restore.ExecuteStatementsAndCreateProgressBar(statements, "", utils.PB_NONE, true)
				resultOrderArray := dbconn.MustSelectStringSlice(tempConn, orderQuery)
				Expect(resultOrderArray).To(Equal(expectedOrderArray))
			})

		})
		Context("error conditions", func() {
			goodStmt := "SELECT * FROM pg_class LIMIT 1;"
			syntaxError := "BAD SYNTAX;"
			statements := []toc.StatementWithType{
				{ObjectType: toc.OBJ_TABLE, Statement: goodStmt},
				{ObjectType: toc.OBJ_INDEX, Statement: syntaxError},
			}
			Context("on-error-continue is not set", func() {
				It("panics after exiting goroutines when running serially", func() {
					errorMessage := ""
					defer func() {
						if r := recover(); r != nil {
							errorMessage = strings.TrimSpace(fmt.Sprintf("%v", r))
							Expect(logFile).To(Say(regexp.QuoteMeta(`[DEBUG]:-Error encountered when executing statement: BAD SYNTAX; Error was: ERROR: syntax error at or near "BAD"`)))
							Expect(errorMessage).To(ContainSubstring(`[CRITICAL]:-ERROR: syntax error at or near "BAD"`))
							Expect(errorMessage).To(Not(ContainSubstring("goroutine")))
						}
					}()
					restore.ExecuteStatementsAndCreateProgressBar(statements, "", utils.PB_NONE, false)
				})
				It("panics after exiting goroutines when running in parallel", func() {
					errorMessage := ""
					defer func() {
						if r := recover(); r != nil {
							errorMessage = strings.TrimSpace(fmt.Sprintf("%v", r))
							Expect(logFile).To(Say(regexp.QuoteMeta(`[DEBUG]:-Error encountered when executing statement: BAD SYNTAX; Error was: ERROR: syntax error at or near "BAD"`)))
							Expect(errorMessage).To(ContainSubstring(`[CRITICAL]:-ERROR: syntax error at or near "BAD"`))
							Expect(errorMessage).To(Not(ContainSubstring("goroutine")))
						}
					}()
					restore.ExecuteStatementsAndCreateProgressBar(statements, "", utils.PB_NONE, true)
				})
			})
			Context("on-error-continue is set", func() {
				BeforeEach(func() {
					_ = restoreCmdFlags.Set(options.ON_ERROR_CONTINUE, "true")
				})
				It("does not panic, but logs errors when running serially", func() {
					restore.ExecuteStatementsAndCreateProgressBar(statements, "", utils.PB_NONE, false)
					Expect(logFile).To(Say(regexp.QuoteMeta(`[DEBUG]:-Error encountered when executing statement: BAD SYNTAX; Error was: ERROR: syntax error at or near "BAD"`)))
					Expect(stderr).To(Say(regexp.QuoteMeta("[ERROR]:-Encountered 1 errors during metadata restore; see log file gbytes.Buffer for a list of failed statements.")))
					Expect(stderr).To(Not(Say(regexp.QuoteMeta("goroutine"))))
				})
				It("does not panic, but logs errors when running in parallel", func() {
					restore.ExecuteStatementsAndCreateProgressBar(statements, "", utils.PB_NONE, true)
					Expect(logFile).To(Say(regexp.QuoteMeta(`[DEBUG]:-Error encountered when executing statement: BAD SYNTAX; Error was: ERROR: syntax error at or near "BAD"`)))
					Expect(stderr).To(Say(regexp.QuoteMeta("[ERROR]:-Encountered 1 errors during metadata restore; see log file gbytes.Buffer for a list of failed statements.")))
					Expect(stderr).To(Not(Say(regexp.QuoteMeta("goroutine"))))
				})
			})

		})
	})
})
