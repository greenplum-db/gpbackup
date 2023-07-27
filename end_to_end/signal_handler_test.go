package end_to_end_test

import (
	"math/rand"
	"os/exec"
	"time"

	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"golang.org/x/sys/unix"
)

var _ = Describe("Signal handler tests", FlakeAttempts(5), func() {
	BeforeEach(func() {
		end_to_end_setup()
		testhelper.AssertQueryRuns(backupConn, "CREATE table bigtable(id int unique); INSERT INTO bigtable SELECT generate_series(1,10000000)")
	})
	AfterEach(func() {
		end_to_end_teardown()
		testhelper.AssertQueryRuns(backupConn, "DROP TABLE bigtable")
	})
	Context("SIGINT", func() {
		It("runs gpbackup and sends a SIGINT to ensure cleanup functions successfully", func() {
			if useOldBackupVersion {
				Skip("This test is not needed for old backup versions")
			}
			args := []string{"--dbname", "testdb",
				"--backup-dir", backupDir,
				"--single-data-file",
				"--verbose"}
			cmd := exec.Command(gpbackupPath, args...)
			go func() {
				/*
				 * We use a random delay for the sleep in this test (between 1.0s and 1.5s) so that
				 * gpbackup will be interrupted at a different point in the backup process every time to help catch
				 * timing issues with the cleanup.
				 */
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))
				time.Sleep(time.Duration(rng.Intn(1000)+500) * time.Millisecond)
				_ = cmd.Process.Signal(unix.SIGINT)
			}()
			output, _ := cmd.CombinedOutput()
			stdout := string(output)
			timestamp, err := getBackupTimestamp(stdout)
			Expect(err).ToNot(HaveOccurred())
			assertArtifactsCleaned(backupConn, timestamp)
			Expect(stdout).To(ContainSubstring("Received an interrupt signal, aborting backup process"))
			Expect(stdout).To(ContainSubstring("Cleanup complete"))
			Expect(stdout).To(Not(ContainSubstring("CRITICAL")))
		})
		It("runs gpbackup with copy-queue-size and sends a SIGINT to ensure cleanup functions successfully", func() {
			if useOldBackupVersion {
				Skip("This test is not needed for old backup versions")
			}
			args := []string{"--dbname", "testdb",
				"--backup-dir", backupDir,
				"--single-data-file",
				"--copy-queue-size", "4",
				"--verbose"}
			cmd := exec.Command(gpbackupPath, args...)
			go func() {
				/*
				* We use a random delay for the sleep in this test (between
				* 0.5s and 0.8s) so that gpbackup will be interrupted at a
				* different point in the backup process every time to help
				* catch timing issues with the cleanup.
				 */
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))
				time.Sleep(time.Duration(rng.Intn(300)+500) * time.Millisecond)
				_ = cmd.Process.Signal(unix.SIGINT)
			}()
			output, _ := cmd.CombinedOutput()
			stdout := string(output)
			timestamp, err := getBackupTimestamp(stdout)
			Expect(err).ToNot(HaveOccurred())
			assertArtifactsCleaned(backupConn, timestamp)
			Expect(stdout).To(ContainSubstring("Received an interrupt signal, aborting backup process"))
			Expect(stdout).To(ContainSubstring("Cleaning up segment agent processes"))
			Expect(stdout).To(ContainSubstring("Cleanup complete"))
			Expect(stdout).To(Not(ContainSubstring("CRITICAL")))
		})
		It("runs gpbackup and sends a SIGINT to ensure blocked LOCK TABLE query is canceled", func() {
			if useOldBackupVersion {
				Skip("This test is not needed for old backup versions")
			}

			// Query to see if gpbackup lock acquire on schema2.foo2 is blocked
			checkLockQuery := `SELECT count(*) FROM pg_locks l, pg_class c, pg_namespace n WHERE l.relation = c.oid AND n.oid = c.relnamespace AND n.nspname = 'schema2' AND c.relname = 'foo2' AND l.granted = 'f'`

			// Acquire AccessExclusiveLock on schema2.foo2 to prevent gpbackup from acquiring AccessShareLock
			backupConn.MustExec("BEGIN; LOCK TABLE schema2.foo2 IN ACCESS EXCLUSIVE MODE")
			args := []string{
				"--dbname", "testdb",
				"--backup-dir", backupDir,
				"--verbose"}
			cmd := exec.Command(gpbackupPath, args...)

			// Wait up to 5 seconds for gpbackup to block on acquiring AccessShareLock.
			// Once blocked, we send a SIGINT to cancel gpbackup.
			var beforeLockCount int
			go func() {
				iterations := 50
				for iterations > 0 {
					_ = backupConn.Get(&beforeLockCount, checkLockQuery)
					if beforeLockCount < 1 {
						time.Sleep(100 * time.Millisecond)
						iterations--
					} else {
						break
					}
				}
				_ = cmd.Process.Signal(unix.SIGINT)
			}()
			output, _ := cmd.CombinedOutput()
			Expect(beforeLockCount).To(Equal(1))

			// After gpbackup has been canceled, we should no longer see a blocked SQL
			// session trying to acquire AccessShareLock on foo2.
			var afterLockCount int
			_ = backupConn.Get(&afterLockCount, checkLockQuery)
			Expect(afterLockCount).To(Equal(0))
			backupConn.MustExec("ROLLBACK")

			stdout := string(output)
			timestamp, err := getBackupTimestamp(stdout)
			Expect(err).ToNot(HaveOccurred())
			assertArtifactsCleaned(backupConn, timestamp)
			Expect(stdout).To(ContainSubstring("Received an interrupt signal, aborting backup process"))
			Expect(stdout).To(ContainSubstring("Cleanup complete"))
			Expect(stdout).To(Not(ContainSubstring("CRITICAL")))
		})
		It("runs gpbackup with single-data-file and sends a SIGINT to ensure blocked LOCK TABLE query is canceled", func() {
			if useOldBackupVersion {
				Skip("This test is not needed for old backup versions")
			}

			// Query to see if gpbackup lock acquire on schema2.foo2 is blocked
			checkLockQuery := `SELECT count(*) FROM pg_locks l, pg_class c, pg_namespace n WHERE l.relation = c.oid AND n.oid = c.relnamespace AND n.nspname = 'schema2' AND c.relname = 'foo2' AND l.granted = 'f'`

			// Acquire AccessExclusiveLock on schema2.foo2 to prevent gpbackup from acquiring AccessShareLock
			backupConn.MustExec("BEGIN; LOCK TABLE schema2.foo2 IN ACCESS EXCLUSIVE MODE")
			args := []string{
				"--dbname", "testdb",
				"--backup-dir", backupDir,
				"--single-data-file",
				"--verbose"}
			cmd := exec.Command(gpbackupPath, args...)

			// Wait up to 5 seconds for gpbackup to block on acquiring AccessShareLock.
			// Once blocked, we send a SIGINT to cancel gpbackup.
			var beforeLockCount int
			go func() {
				iterations := 50
				for iterations > 0 {
					_ = backupConn.Get(&beforeLockCount, checkLockQuery)
					if beforeLockCount < 1 {
						time.Sleep(100 * time.Millisecond)
						iterations--
					} else {
						break
					}
				}
				_ = cmd.Process.Signal(unix.SIGINT)
			}()
			output, _ := cmd.CombinedOutput()
			Expect(beforeLockCount).To(Equal(1))

			// After gpbackup has been canceled, we should no longer see a blocked SQL
			// session trying to acquire AccessShareLock on foo2.
			var afterLockCount int
			_ = backupConn.Get(&afterLockCount, checkLockQuery)
			Expect(afterLockCount).To(Equal(0))
			backupConn.MustExec("ROLLBACK")

			stdout := string(output)
			timestamp, err := getBackupTimestamp(stdout)
			Expect(err).ToNot(HaveOccurred())
			assertArtifactsCleaned(backupConn, timestamp)
			Expect(stdout).To(ContainSubstring("Received an interrupt signal, aborting backup process"))
			Expect(stdout).To(ContainSubstring("Cleanup complete"))
			Expect(stdout).To(Not(ContainSubstring("CRITICAL")))
		})
		It("runs gprestore and sends a SIGINT to ensure cleanup functions successfully", func() {
			if useOldBackupVersion {
				Skip("This test is not needed for old backup versions")
			}
			timestamp := gpbackup(gpbackupPath, backupHelperPath,
				"--backup-dir", backupDir,
				"--single-data-file")
			args := []string{
				"--timestamp", timestamp,
				"--redirect-db", "restoredb",
				"--backup-dir", backupDir,
				"--verbose"}
			cmd := exec.Command(gprestorePath, args...)
			go func() {
				/*
				* We use a random delay for the sleep in this test (between
				* 1.0s and 1.5s) so that gprestore will be interrupted at a
				* different point in the backup process every time to help
				* catch timing issues with the cleanup.
				 */
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))
				time.Sleep(time.Duration(rng.Intn(1000)+500) * time.Millisecond)
				_ = cmd.Process.Signal(unix.SIGINT)
			}()
			output, _ := cmd.CombinedOutput()
			stdout := string(output)
			Expect(stdout).To(ContainSubstring("Received an interrupt signal, aborting restore process"))
			Expect(stdout).To(ContainSubstring("Cleaning up segment agent processes"))
			Expect(stdout).To(ContainSubstring("Cleanup complete"))
			Expect(stdout).To(Not(ContainSubstring("CRITICAL")))
			assertArtifactsCleaned(restoreConn, timestamp)
		})
		It("runs gprestore with copy-queue-size and sends a SIGINT to ensure cleanup functions successfully", func() {
			if useOldBackupVersion {
				Skip("This test is not needed for old backup versions")
			}
			timestamp := gpbackup(gpbackupPath, backupHelperPath,
				"--backup-dir", backupDir,
				"--single-data-file")
			args := []string{
				"--timestamp", timestamp,
				"--redirect-db", "restoredb",
				"--backup-dir", backupDir,
				"--verbose",
				"--copy-queue-size", "4"}
			cmd := exec.Command(gprestorePath, args...)
			go func() {
				/*
				* We use a random delay for the sleep in this test (between
				* 1.0s and 1.5s) so that gprestore will be interrupted at a
				* different point in the backup process every time to help
				* catch timing issues with the cleanup.
				 */
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))
				time.Sleep(time.Duration(rng.Intn(1000)+500) * time.Millisecond)
				_ = cmd.Process.Signal(unix.SIGINT)
			}()
			output, _ := cmd.CombinedOutput()
			stdout := string(output)
			Expect(stdout).To(ContainSubstring("Received an interrupt signal, aborting restore process"))
			Expect(stdout).To(ContainSubstring("Cleanup complete"))
			Expect(stdout).To(Not(ContainSubstring("CRITICAL")))
			assertArtifactsCleaned(restoreConn, timestamp)
		})
	})
	Context("SIGTERM", func() {
		It("runs gpbackup and sends a SIGTERM to ensure cleanup functions successfully", func() {
			if useOldBackupVersion {
				Skip("This test is not needed for old backup versions")
			}
			args := []string{"--dbname", "testdb",
				"--backup-dir", backupDir,
				"--single-data-file",
				"--verbose"}
			cmd := exec.Command(gpbackupPath, args...)
			go func() {
				/*
				* We use a random delay for the sleep in this test (between
				* 1.0s and 1.5s) so that gpbackup will be interrupted at a
				* different point in the backup process every time to help
				* catch timing issues with the cleanup.
				 */
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))
				time.Sleep(time.Duration(rng.Intn(1000)+500) * time.Millisecond)
				_ = cmd.Process.Signal(unix.SIGTERM)
			}()
			output, _ := cmd.CombinedOutput()
			stdout := string(output)
			timestamp, err := getBackupTimestamp(stdout)
			Expect(err).ToNot(HaveOccurred())
			assertArtifactsCleaned(backupConn, timestamp)
			Expect(stdout).To(ContainSubstring("Received a termination signal, aborting backup process"))
			Expect(stdout).To(ContainSubstring("Cleanup complete"))
			Expect(stdout).To(Not(ContainSubstring("CRITICAL")))
		})
		It("runs gpbackup and sends a SIGTERM to ensure blocked LOCK TABLE query is canceled", func() {
			if useOldBackupVersion {
				Skip("This test is not needed for old backup versions")
			}

			// Query to see if gpbackup lock acquire on schema2.foo2 is blocked
			checkLockQuery := `SELECT count(*) FROM pg_locks l, pg_class c, pg_namespace n WHERE l.relation = c.oid AND n.oid = c.relnamespace AND n.nspname = 'schema2' AND c.relname = 'foo2' AND l.granted = 'f'`

			// Acquire AccessExclusiveLock on schema2.foo2 to prevent gpbackup from acquiring AccessShareLock
			backupConn.MustExec("BEGIN; LOCK TABLE schema2.foo2 IN ACCESS EXCLUSIVE MODE")
			args := []string{
				"--dbname", "testdb",
				"--backup-dir", backupDir,
				"--verbose"}
			cmd := exec.Command(gpbackupPath, args...)

			// Wait up to 5 seconds for gpbackup to block on acquiring AccessShareLock.
			// Once blocked, we send a SIGTERM to cancel gpbackup.
			var beforeLockCount int
			go func() {
				iterations := 50
				for iterations > 0 {
					_ = backupConn.Get(&beforeLockCount, checkLockQuery)
					if beforeLockCount < 1 {
						time.Sleep(100 * time.Millisecond)
						iterations--
					} else {
						break
					}
				}
				_ = cmd.Process.Signal(unix.SIGTERM)
			}()
			output, _ := cmd.CombinedOutput()
			Expect(beforeLockCount).To(Equal(1))

			// After gpbackup has been canceled, we should no longer see a blocked SQL
			// session trying to acquire AccessShareLock on foo2.
			var afterLockCount int
			_ = backupConn.Get(&afterLockCount, checkLockQuery)
			Expect(afterLockCount).To(Equal(0))
			backupConn.MustExec("ROLLBACK")

			stdout := string(output)
			timestamp, err := getBackupTimestamp(stdout)
			Expect(err).ToNot(HaveOccurred())
			assertArtifactsCleaned(backupConn, timestamp)
			Expect(stdout).To(ContainSubstring("Received a termination signal, aborting backup process"))
			Expect(stdout).To(ContainSubstring("Cleanup complete"))
			Expect(stdout).To(Not(ContainSubstring("CRITICAL")))
		})
		It("runs gpbackup with single-data-file and sends a SIGTERM to ensure blocked LOCK TABLE query is canceled", func() {
			if useOldBackupVersion {
				Skip("This test is not needed for old backup versions")
			}

			// Query to see if gpbackup lock acquire on schema2.foo2 is blocked
			checkLockQuery := `SELECT count(*) FROM pg_locks l, pg_class c, pg_namespace n WHERE l.relation = c.oid AND n.oid = c.relnamespace AND n.nspname = 'schema2' AND c.relname = 'foo2' AND l.granted = 'f'`

			// Acquire AccessExclusiveLock on schema2.foo2 to prevent gpbackup from acquiring AccessShareLock
			backupConn.MustExec("BEGIN; LOCK TABLE schema2.foo2 IN ACCESS EXCLUSIVE MODE")
			args := []string{
				"--dbname", "testdb",
				"--backup-dir", backupDir,
				"--single-data-file",
				"--verbose"}
			cmd := exec.Command(gpbackupPath, args...)

			// Wait up to 5 seconds for gpbackup to block on acquiring AccessShareLock.
			// Once blocked, we send a SIGTERM to cancel gpbackup.
			var beforeLockCount int
			go func() {
				iterations := 50
				for iterations > 0 {
					_ = backupConn.Get(&beforeLockCount, checkLockQuery)
					if beforeLockCount < 1 {
						time.Sleep(100 * time.Millisecond)
						iterations--
					} else {
						break
					}
				}
				_ = cmd.Process.Signal(unix.SIGTERM)
			}()
			output, _ := cmd.CombinedOutput()
			Expect(beforeLockCount).To(Equal(1))

			// After gpbackup has been canceled, we should no longer see a blocked SQL
			// session trying to acquire AccessShareLock on foo2.
			var afterLockCount int
			_ = backupConn.Get(&afterLockCount, checkLockQuery)
			Expect(afterLockCount).To(Equal(0))
			backupConn.MustExec("ROLLBACK")

			stdout := string(output)
			timestamp, err := getBackupTimestamp(stdout)
			Expect(err).ToNot(HaveOccurred())
			assertArtifactsCleaned(backupConn, timestamp)
			Expect(stdout).To(ContainSubstring("Received a termination signal, aborting backup process"))
			Expect(stdout).To(ContainSubstring("Cleanup complete"))
			Expect(stdout).To(Not(ContainSubstring("CRITICAL")))
		})
		It("runs gpbackup with copy-queue-size and sends a SIGTERM to ensure cleanup functions successfully", func() {
			if useOldBackupVersion {
				Skip("This test is not needed for old backup versions")
			}
			args := []string{"--dbname", "testdb",
				"--backup-dir", backupDir,
				"--single-data-file",
				"--copy-queue-size", "4",
				"--verbose"}
			cmd := exec.Command(gpbackupPath, args...)
			go func() {
				/*
				* We use a random delay for the sleep in this test (between
				* 1.0s and 1.5s) so that gpbackup will be interrupted at a
				* different point in the backup process every time to help
				* catch timing issues with the cleanup.
				 */
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))
				time.Sleep(time.Duration(rng.Intn(1000)+500) * time.Millisecond)
				_ = cmd.Process.Signal(unix.SIGTERM)
			}()
			output, _ := cmd.CombinedOutput()
			stdout := string(output)
			timestamp, err := getBackupTimestamp(stdout)
			Expect(err).ToNot(HaveOccurred())
			assertArtifactsCleaned(backupConn, timestamp)
			Expect(stdout).To(ContainSubstring("Received a termination signal, aborting backup process"))
			Expect(stdout).To(ContainSubstring("Cleanup complete"))
			Expect(stdout).To(Not(ContainSubstring("CRITICAL")))
		})
		It("runs gprestore and sends a SIGTERM to ensure cleanup functions successfully", func() {
			if useOldBackupVersion {
				Skip("This test is not needed for old backup versions")
			}
			timestamp := gpbackup(gpbackupPath, backupHelperPath,
				"--backup-dir", backupDir,
				"--single-data-file")
			args := []string{
				"--timestamp", timestamp,
				"--redirect-db", "restoredb",
				"--backup-dir", backupDir,
				"--verbose"}
			cmd := exec.Command(gprestorePath, args...)
			go func() {
				/*
				* We use a random delay for the sleep in this test (between
				* 1.0s and 1.5s) so that gprestore will be interrupted at a
				* different point in the backup process every time to help
				* catch timing issues with the cleanup.
				 */
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))
				time.Sleep(time.Duration(rng.Intn(1000)+500) * time.Millisecond)
				_ = cmd.Process.Signal(unix.SIGTERM)
			}()
			output, _ := cmd.CombinedOutput()
			stdout := string(output)
			Expect(stdout).To(ContainSubstring("Received a termination signal, aborting restore process"))
			Expect(stdout).To(ContainSubstring("Cleanup complete"))
			Expect(stdout).To(Not(ContainSubstring("CRITICAL")))
			assertArtifactsCleaned(restoreConn, timestamp)
		})
		It("runs gprestore with copy-queue-size and sends a SIGTERM to ensure cleanup functions successfully", func() {
			if useOldBackupVersion {
				Skip("This test is not needed for old backup versions")
			}
			timestamp := gpbackup(gpbackupPath, backupHelperPath,
				"--backup-dir", backupDir,
				"--single-data-file")
			args := []string{
				"--timestamp", timestamp,
				"--redirect-db", "restoredb",
				"--backup-dir", backupDir,
				"--verbose",
				"--copy-queue-size", "4"}
			cmd := exec.Command(gprestorePath, args...)
			go func() {
				/*
				* We use a random delay for the sleep in this test (between
				* 1.0s and 1.5s) so that gprestore will be interrupted at a
				* different point in the backup process every time to help
				* catch timing issues with the cleanup.
				 */
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))
				time.Sleep(time.Duration(rng.Intn(1000)+500) * time.Millisecond)
				_ = cmd.Process.Signal(unix.SIGTERM)
			}()
			output, _ := cmd.CombinedOutput()
			stdout := string(output)
			Expect(stdout).To(ContainSubstring("Received a termination signal, aborting restore process"))
			Expect(stdout).To(ContainSubstring("Cleanup complete"))
			Expect(stdout).To(Not(ContainSubstring("CRITICAL")))
			assertArtifactsCleaned(restoreConn, timestamp)
		})
	})
})
