package end_to_end_test

import (
	"fmt"
	"os/exec"
	"time"

	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Deadlock handling", func() {
	BeforeEach(func() {
		end_to_end_setup()
		testhelper.AssertQueryRuns(backupConn, "CREATE table bigtable(id int unique); INSERT INTO bigtable SELECT generate_series(1,1000000)")
	})
	AfterEach(func() {
		end_to_end_teardown()
		testhelper.AssertQueryRuns(backupConn, "DROP table bigtable")
	})
	It("runs gpbackup with jobs flag and COPY deadlock handling occurs", func() {
		if useOldBackupVersion {
			Skip("This test is not needed for old backup versions")
		}
		// Acquire AccessExclusiveLock on public.foo to block gpbackup when it attempts
		// to grab AccessShareLocks before its metadata dump section.
		backupConn.MustExec("BEGIN; LOCK TABLE public.foo IN ACCESS EXCLUSIVE MODE")

		// Execute gpbackup with --jobs 10 since there are 10 tables to back up
		args := []string{
			"--dbname", "testdb",
			"--backup-dir", backupDir,
			"--jobs", "10",
			"--verbose"}
		cmd := exec.Command(gpbackupPath, args...)
		// Concurrently wait for gpbackup to block when it requests an AccessShareLock on public.foo. Once
		// that happens, acquire an AccessExclusiveLock on pg_catalog.pg_trigger to block gpbackup during its
		// trigger metadata dump. Then release the initial AccessExclusiveLock on public.foo (from the
		// beginning of the test) to unblock gpbackup and let gpbackup move forward to the trigger metadata dump.
		anotherConn := testutils.SetupTestDbConn("testdb")
		defer anotherConn.Close()
		go func() {
			// Query to see if gpbackup's AccessShareLock request on public.foo is blocked
			checkLockQuery := `SELECT count(*) FROM pg_locks l, pg_class c, pg_namespace n WHERE l.relation = c.oid AND n.oid = c.relnamespace AND n.nspname = 'public' AND c.relname = 'foo' AND l.granted = 'f' AND l.mode = 'AccessShareLock'`

			// Wait up to 10 seconds for gpbackup to block
			var gpbackupBlockedLockCount int
			iterations := 100
			for iterations > 0 {
				_ = anotherConn.Get(&gpbackupBlockedLockCount, checkLockQuery)
				if gpbackupBlockedLockCount < 1 {
					time.Sleep(100 * time.Millisecond)
					iterations--
				} else {
					break
				}
			}

			// Queue AccessExclusiveLock request on pg_catalog.pg_trigger to block gpbackup
			// during the trigger metadata dump so that the test can queue a bunch of
			// AccessExclusiveLock requests against the test tables. Afterwards, release the
			// AccessExclusiveLock on public.foo to let gpbackup go to the trigger metadata dump.
			anotherConn.MustExec(`BEGIN; LOCK TABLE pg_catalog.pg_trigger IN ACCESS EXCLUSIVE MODE`)
			backupConn.MustExec("COMMIT")
		}()

		// Concurrently wait for gpbackup to block on the trigger metadata dump section. Once we
		// see gpbackup blocked, request AccessExclusiveLock (to imitate a TRUNCATE or VACUUM
		// FULL) on all the test tables.
		dataTables := []string{`public."FOObar"`, "public.foo", "public.holds", "public.sales", "public.bigtable",
			"schema2.ao1", "schema2.ao2", "schema2.foo2", "schema2.foo3", "schema2.returns"}
		for _, dataTable := range dataTables {
			go func(dataTable string) {
				accessExclusiveLockConn := testutils.SetupTestDbConn("testdb")
				defer accessExclusiveLockConn.Close()

				// Query to see if gpbackup's AccessShareLock request on pg_catalog.pg_trigger is blocked
				checkLockQuery := `SELECT count(*) FROM pg_locks l, pg_class c, pg_namespace n WHERE l.relation = c.oid AND n.oid = c.relnamespace AND n.nspname = 'pg_catalog' AND c.relname = 'pg_trigger' AND l.granted = 'f' AND l.mode = 'AccessShareLock'`

				// Wait up to 10 seconds for gpbackup to block
				var gpbackupBlockedLockCount int
				iterations := 100
				for iterations > 0 {
					_ = accessExclusiveLockConn.Get(&gpbackupBlockedLockCount, checkLockQuery)
					if gpbackupBlockedLockCount < 1 {
						time.Sleep(100 * time.Millisecond)
						iterations--
					} else {
						break
					}
				}

				// Queue an AccessExclusiveLock request on a test table which will later
				// result in a detected deadlock during the gpbackup data dump section.
				accessExclusiveLockConn.MustExec(fmt.Sprintf(`BEGIN; LOCK TABLE %s IN ACCESS EXCLUSIVE MODE; COMMIT`, dataTable))
			}(dataTable)
		}

		// Concurrently wait for all AccessExclusiveLock requests on all 10 test tables to block.
		// Once that happens, release the AccessExclusiveLock on pg_catalog.pg_trigger to unblock
		// gpbackup and let gpbackup move forward to the data dump section.
		var accessExclBlockedLockCount int
		go func() {
			// Query to check for ungranted AccessExclusiveLock requests on our test tables
			checkLockQuery := `SELECT count(*) FROM pg_locks WHERE granted = 'f' AND mode = 'AccessExclusiveLock'`

			// Wait up to 10 seconds
			iterations := 100
			for iterations > 0 {
				_ = backupConn.Get(&accessExclBlockedLockCount, checkLockQuery)
				if accessExclBlockedLockCount < 10 {
					time.Sleep(100 * time.Millisecond)
					iterations--
				} else {
					break
				}
			}

			// Unblock gpbackup by releasing AccessExclusiveLock on pg_catalog.pg_trigger
			anotherConn.MustExec("COMMIT")
		}()

		// gpbackup has finished
		output, _ := cmd.CombinedOutput()
		stdout := string(output)

		// Check that 10 deadlock traps were placed during the test
		Expect(accessExclBlockedLockCount).To(Equal(10))
		// No non-main worker should have been able to run COPY due to deadlock detection
		for i := 1; i < 10; i++ {
			expectedLockString := fmt.Sprintf("[DEBUG]:-Worker %d: LOCK TABLE ", i)
			Expect(stdout).To(ContainSubstring(expectedLockString))

			expectedWarnString := fmt.Sprintf("[WARNING]:-Worker %d could not acquire AccessShareLock for table", i)
			Expect(stdout).To(ContainSubstring(expectedWarnString))

			unexpectedCopyString := fmt.Sprintf("[DEBUG]:-Worker %d: COPY ", i)
			Expect(stdout).ToNot(ContainSubstring(unexpectedCopyString))
		}

		// Only the main worker thread, worker 0, will run COPY on all the test tables
		for _, dataTable := range dataTables {
			expectedString := fmt.Sprintf(`[DEBUG]:-Worker 0: COPY %s`, dataTable)
			Expect(stdout).To(ContainSubstring(expectedString))
		}

		Expect(stdout).To(ContainSubstring("Backup completed successfully"))
	})
	It("runs gpbackup with copy-queue-size flag and COPY deadlock handling occurs", func() {
		if useOldBackupVersion {
			Skip("This test is not needed for old backup versions")
		}
		// Acquire AccessExclusiveLock on public.foo to block gpbackup when it attempts
		// to grab AccessShareLocks before its metadata dump section.
		backupConn.MustExec("BEGIN; LOCK TABLE public.foo IN ACCESS EXCLUSIVE MODE")

		// Execute gpbackup with --copy-queue-size 2
		args := []string{
			"--dbname", "testdb",
			"--backup-dir", backupDir,
			"--single-data-file",
			"--copy-queue-size", "2",
			"--verbose"}
		cmd := exec.Command(gpbackupPath, args...)

		// Concurrently wait for gpbackup to block when it requests an AccessShareLock on public.foo. Once
		// that happens, acquire an AccessExclusiveLock on pg_catalog.pg_trigger to block gpbackup during its
		// trigger metadata dump. Then release the initial AccessExclusiveLock on public.foo (from the
		// beginning of the test) to unblock gpbackup and let gpbackup move forward to the trigger metadata dump.
		anotherConn := testutils.SetupTestDbConn("testdb")
		defer anotherConn.Close()
		go func() {
			// Query to see if gpbackup's AccessShareLock request on public.foo is blocked
			checkLockQuery := `SELECT count(*) FROM pg_locks l, pg_class c, pg_namespace n WHERE l.relation = c.oid AND n.oid = c.relnamespace AND n.nspname = 'public' AND c.relname = 'foo' AND l.granted = 'f' AND l.mode = 'AccessShareLock'`

			// Wait up to 10 seconds for gpbackup to block
			var gpbackupBlockedLockCount int
			iterations := 100
			for iterations > 0 {
				_ = anotherConn.Get(&gpbackupBlockedLockCount, checkLockQuery)
				if gpbackupBlockedLockCount < 1 {
					time.Sleep(100 * time.Millisecond)
					iterations--
				} else {
					break
				}
			}

			// Queue AccessExclusiveLock request on pg_catalog.pg_trigger to block gpbackup
			// during the trigger metadata dump so that the test can queue a bunch of
			// AccessExclusiveLock requests against the test tables. Afterwards, release the
			// AccessExclusiveLock on public.foo to let gpbackup go to the trigger metadata dump.
			anotherConn.MustExec(`BEGIN; LOCK TABLE pg_catalog.pg_trigger IN ACCESS EXCLUSIVE MODE`)
			backupConn.MustExec("COMMIT")
		}()

		// Concurrently wait for gpbackup to block on the trigger metadata dump section. Once we
		// see gpbackup blocked, request AccessExclusiveLock (to imitate a TRUNCATE or VACUUM
		// FULL) on all the test tables.
		dataTables := []string{"public.bigtable", "public.holds", "public.foo", "schema2.foo3", "schema2.ao1",
			"schema2.ao2", `public."FOObar"`, "public.sales", "schema2.foo2", "schema2.returns"}
		for _, dataTable := range dataTables {
			go func(dataTable string) {
				accessExclusiveLockConn := testutils.SetupTestDbConn("testdb")
				defer accessExclusiveLockConn.Close()

				// Query to see if gpbackup's AccessShareLock request on pg_catalog.pg_trigger is blocked
				checkLockQuery := `SELECT count(*) FROM pg_locks l, pg_class c, pg_namespace n WHERE l.relation = c.oid AND n.oid = c.relnamespace AND n.nspname = 'pg_catalog' AND c.relname = 'pg_trigger' AND l.granted = 'f' AND l.mode = 'AccessShareLock'`

				// Wait up to 10 seconds for gpbackup to block
				var gpbackupBlockedLockCount int
				iterations := 100
				for iterations > 0 {
					_ = accessExclusiveLockConn.Get(&gpbackupBlockedLockCount, checkLockQuery)
					if gpbackupBlockedLockCount < 1 {
						time.Sleep(100 * time.Millisecond)
						iterations--
					} else {
						break
					}
				}

				// Queue an AccessExclusiveLock request on a test table which will later
				// result in a detected deadlock during the gpbackup data dump section.
				accessExclusiveLockConn.MustExec(fmt.Sprintf(`BEGIN; LOCK TABLE %s IN ACCESS EXCLUSIVE MODE; COMMIT`, dataTable))
			}(dataTable)
		}

		// Concurrently wait for all AccessExclusiveLock requests on all 10 test tables to block.
		// Once that happens, release the AccessExclusiveLock on pg_catalog.pg_trigger to unblock
		// gpbackup and let gpbackup move forward to the data dump section.
		var accessExclBlockedLockCount int
		go func() {
			// Query to check for ungranted AccessExclusiveLock requests on our test tables
			checkLockQuery := `SELECT count(*) FROM pg_locks WHERE granted = 'f' AND mode = 'AccessExclusiveLock'`

			// Wait up to 10 seconds
			iterations := 100
			for iterations > 0 {
				_ = backupConn.Get(&accessExclBlockedLockCount, checkLockQuery)
				if accessExclBlockedLockCount < 10 {
					time.Sleep(100 * time.Millisecond)
					iterations--
				} else {
					break
				}
			}

			// Unblock gpbackup by releasing AccessExclusiveLock on pg_catalog.pg_trigger
			anotherConn.MustExec("COMMIT")
		}()

		// gpbackup has finished
		output, _ := cmd.CombinedOutput()
		stdout := string(output)

		// Check that 10 deadlock traps were placed during the test
		Expect(accessExclBlockedLockCount).To(Equal(10))
		// No non-main worker should have been able to run COPY due to deadlock detection
		for i := 1; i <= 2; i++ {
			expectedLockString := fmt.Sprintf("[DEBUG]:-Worker %d: LOCK TABLE ", i)
			Expect(stdout).To(ContainSubstring(expectedLockString))

			expectedWarnString := fmt.Sprintf("[WARNING]:-Worker %d could not acquire AccessShareLock for table", i)
			Expect(stdout).To(ContainSubstring(expectedWarnString))

			unexpectedCopyString := fmt.Sprintf("[DEBUG]:-Worker %d: COPY ", i)
			Expect(stdout).ToNot(ContainSubstring(unexpectedCopyString))

			expectedLockString = fmt.Sprintf(`Locks held on table %s`, dataTables[i-1])
			Expect(stdout).To(ContainSubstring(expectedLockString))

			Expect(stdout).To(ContainSubstring(`"Mode":"AccessExclusiveLock"`))
		}

		// Only the main worker thread, worker 0, will run COPY on all the test tables
		for _, dataTable := range dataTables {
			expectedString := fmt.Sprintf(`[DEBUG]:-Worker 0: COPY %s`, dataTable)
			Expect(stdout).To(ContainSubstring(expectedString))
		}

		Expect(stdout).To(ContainSubstring("Backup completed successfully"))
	})
	It("runs gpbackup and defers 2 deadlocked tables to main worker", func() {
		if useOldBackupVersion || backupConn.Version.Before(backup.SNAPSHOT_GPDB_MIN_VERSION) {
			Skip(fmt.Sprintf("This test is not needed for old backup versions or GPDB versions < %s", backup.SNAPSHOT_GPDB_MIN_VERSION))
		}
		// Acquire AccessExclusiveLock on public.foo to block gpbackup when it attempts
		// to grab AccessShareLocks before its metadata dump section.
		backupConn.MustExec("BEGIN; LOCK TABLE public.foo IN ACCESS EXCLUSIVE MODE")

		args := []string{
			"--dbname", "testdb",
			"--backup-dir", backupDir,
			"--jobs", "2",
			"--verbose"}
		cmd := exec.Command(gpbackupPath, args...)
		// Concurrently wait for gpbackup to block when it requests an AccessShareLock on public.foo. Once
		// that happens, acquire an AccessExclusiveLock on pg_catalog.pg_trigger to block gpbackup during its
		// trigger metadata dump. Then release the initial AccessExclusiveLock on public.foo (from the
		// beginning of the test) to unblock gpbackup and let gpbackup move forward to the trigger metadata dump.
		anotherConn := testutils.SetupTestDbConn("testdb")
		defer anotherConn.Close()
		go func() {
			// Query to see if gpbackup's AccessShareLock request on public.foo is blocked
			checkLockQuery := `SELECT count(*) FROM pg_locks l, pg_class c, pg_namespace n WHERE l.relation = c.oid AND n.oid = c.relnamespace AND n.nspname = 'public' AND c.relname = 'foo' AND l.granted = 'f' AND l.mode = 'AccessShareLock'`

			// Wait up to 10 seconds for gpbackup to block
			var gpbackupBlockedLockCount int
			iterations := 100
			for iterations > 0 {
				_ = anotherConn.Get(&gpbackupBlockedLockCount, checkLockQuery)
				if gpbackupBlockedLockCount < 1 {
					time.Sleep(100 * time.Millisecond)
					iterations--
				} else {
					break
				}
			}

			// Queue AccessExclusiveLock request on pg_catalog.pg_trigger to block gpbackup
			// during the trigger metadata dump so that the test can queue a bunch of
			// AccessExclusiveLock requests against the test tables. Afterwards, release the
			// AccessExclusiveLock on public.foo to let gpbackup go to the trigger metadata dump.
			anotherConn.MustExec(`BEGIN; LOCK TABLE pg_catalog.pg_trigger IN ACCESS EXCLUSIVE MODE`)
			backupConn.MustExec("COMMIT")
		}()

		// Concurrently wait for gpbackup to block on the trigger metadata dump section. Once we
		// see gpbackup blocked, request AccessExclusiveLock (to imitate a TRUNCATE or VACUUM
		// FULL) on two of the test tables.
		dataTables := []string{"public.holds", "public.sales", "public.bigtable",
			"schema2.ao1", "schema2.ao2", "schema2.foo2", "schema2.foo3", "schema2.returns"}
		lockedTables := []string{`public."FOObar"`, "public.foo"}
		for _, lockedTable := range lockedTables {
			go func(lockedTable string) {
				accessExclusiveLockConn := testutils.SetupTestDbConn("testdb")
				defer accessExclusiveLockConn.Close()

				// Query to see if gpbackup's AccessShareLock request on pg_catalog.pg_trigger is blocked
				checkLockQuery := `SELECT count(*) FROM pg_locks l, pg_class c, pg_namespace n WHERE l.relation = c.oid AND n.oid = c.relnamespace AND n.nspname = 'pg_catalog' AND c.relname = 'pg_trigger' AND l.granted = 'f' AND l.mode = 'AccessShareLock'`

				// Wait up to 10 seconds for gpbackup to block
				var gpbackupBlockedLockCount int
				iterations := 100
				for iterations > 0 {
					_ = accessExclusiveLockConn.Get(&gpbackupBlockedLockCount, checkLockQuery)
					if gpbackupBlockedLockCount < 1 {
						time.Sleep(100 * time.Millisecond)
						iterations--
					} else {
						break
					}
				}
				// Queue an AccessExclusiveLock request on a test table which will later
				// result in a detected deadlock during the gpbackup data dump section.
				accessExclusiveLockConn.MustExec(fmt.Sprintf(`BEGIN; LOCK TABLE %s IN ACCESS EXCLUSIVE MODE; COMMIT`, lockedTable))
			}(lockedTable)
		}

		// Concurrently wait for all AccessExclusiveLock requests on all 10 test tables to block.
		// Once that happens, release the AccessExclusiveLock on pg_catalog.pg_trigger to unblock
		// gpbackup and let gpbackup move forward to the data dump section.
		var accessExclBlockedLockCount int
		go func() {
			// Query to check for ungranted AccessExclusiveLock requests on our test tables
			checkLockQuery := `SELECT count(*) FROM pg_locks WHERE granted = 'f' AND mode = 'AccessExclusiveLock'`

			// Wait up to 10 seconds
			iterations := 100
			for iterations > 0 {
				_ = backupConn.Get(&accessExclBlockedLockCount, checkLockQuery)
				if accessExclBlockedLockCount < 9 {
					time.Sleep(100 * time.Millisecond)
					iterations--
				} else {
					break
				}
			}

			// Unblock gpbackup by releasing AccessExclusiveLock on pg_catalog.pg_trigger
			anotherConn.MustExec("COMMIT")
		}()

		// gpbackup has finished
		output, _ := cmd.CombinedOutput()
		stdout := string(output)

		// Check that 2 deadlock traps were placed during the test
		Expect(accessExclBlockedLockCount).To(Equal(2))
		// No non-main worker should have been able to run COPY due to deadlock detection
		for i := 1; i < backupConn.NumConns; i++ {
			expectedLockString := fmt.Sprintf("[DEBUG]:-Worker %d: LOCK TABLE ", i)
			Expect(stdout).To(ContainSubstring(expectedLockString))

			expectedWarnString := fmt.Sprintf("[WARNING]:-Worker %d could not acquire AccessShareLock for table", i)
			Expect(stdout).To(ContainSubstring(expectedWarnString))

			unexpectedCopyString := fmt.Sprintf("[DEBUG]:-Worker %d: COPY ", i)
			Expect(stdout).To(ContainSubstring(unexpectedCopyString))
		}

		// Only the main worker thread, worker 0, will run COPY on the 2 locked test tables
		for _, lockedTable := range lockedTables {
			expectedString := fmt.Sprintf(`[DEBUG]:-Worker 0: COPY %s`, lockedTable)
			Expect(stdout).To(ContainSubstring(expectedString))
		}
		for _, dataTable := range dataTables {
			unexpectedString := fmt.Sprintf(`[DEBUG]:-Worker 0: COPY %s`, dataTable)
			Expect(stdout).ToNot(ContainSubstring(unexpectedString))
		}
		Expect(stdout).To(ContainSubstring("Backup completed successfully"))
	})
})
