package end_to_end_test

import (
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/blang/semver"
	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/iohelper"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup_filepath"
	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/utils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"
)

/* The backup directory must be unique per test. There is test flakiness
 * against Data Domain Boost mounted file systems due to how it handles
 * directory deletion/creation.
 */
var custom_backup_dir string

var useOldBackupVersion bool
var oldBackupSemVer semver.Version

var backupCluster *cluster.Cluster

// This function is run automatically by ginkgo before any tests are run.
func init() {
	flag.StringVar(&custom_backup_dir, "custom_backup_dir", "/tmp", "custom_backup_flag for testing against a configurable directory")
}

/* This function is a helper function to execute gpbackup and return a session
 * to allow checking its output.
 */
func gpbackup(gpbackupPath string, backupHelperPath string, args ...string) string {
	if useOldBackupVersion {
		os.Chdir("..")
		command := exec.Command("make", "install_helper", fmt.Sprintf("helper_path=%s", backupHelperPath))
		mustRunCommand(command)
		os.Chdir("end_to_end")
	}
	args = append([]string{"--verbose", "--dbname", "testdb"}, args...)
	command := exec.Command(gpbackupPath, args...)
	output := mustRunCommand(command)
	r := regexp.MustCompile(`Backup Timestamp = (\d{14})`)
	return r.FindStringSubmatch(fmt.Sprintf("%s", output))[1]
}

func gprestore(gprestorePath string, restoreHelperPath string, timestamp string, args ...string) []byte {
	if useOldBackupVersion {
		os.Chdir("..")
		command := exec.Command("make", "install_helper", fmt.Sprintf("helper_path=%s", restoreHelperPath))
		mustRunCommand(command)
		os.Chdir("end_to_end")
	}
	args = append([]string{"--verbose", "--timestamp", timestamp}, args...)
	command := exec.Command(gprestorePath, args...)
	output := mustRunCommand(command)
	return output
}

func buildAndInstallBinaries() (string, string, string) {
	os.Chdir("..")
	command := exec.Command("make", "build")
	mustRunCommand(command)
	os.Chdir("end_to_end")
	binDir := fmt.Sprintf("%s/go/bin", operating.System.Getenv("HOME"))
	return fmt.Sprintf("%s/gpbackup", binDir), fmt.Sprintf("%s/gpbackup_helper", binDir), fmt.Sprintf("%s/gprestore", binDir)
}

func buildOldBinaries(version string) (string, string) {
	os.Chdir("..")
	command := exec.Command("git", "checkout", version, "-f")
	mustRunCommand(command)
	command = exec.Command("dep", "ensure")
	mustRunCommand(command)
	gpbackupOldPath, err := gexec.Build("github.com/greenplum-db/gpbackup", "-tags", "gpbackup", "-ldflags", fmt.Sprintf("-X github.com/greenplum-db/gpbackup/backup.version=%s", version))
	Expect(err).ShouldNot(HaveOccurred())
	gpbackupHelperOldPath, err := gexec.Build("github.com/greenplum-db/gpbackup", "-tags", "gpbackup_helper", "-ldflags", fmt.Sprintf("-X github.com/greenplum-db/gpbackup/helper.version=%s", version))
	Expect(err).ShouldNot(HaveOccurred())
	command = exec.Command("git", "checkout", "-", "-f")
	mustRunCommand(command)
	command = exec.Command("dep", "ensure")
	mustRunCommand(command)
	os.Chdir("end_to_end")
	return gpbackupOldPath, gpbackupHelperOldPath
}

func assertDataRestored(conn *dbconn.DBConn, tableToTupleCount map[string]int) {
	for name, numTuples := range tableToTupleCount {
		tupleCount := dbconn.MustSelectString(conn, fmt.Sprintf("SELECT count(*) AS string from %s", name))
		Expect(tupleCount).To(Equal(strconv.Itoa(numTuples)))
	}
}

func assertRelationsCreated(conn *dbconn.DBConn, numTables int) {
	countQuery := `SELECT count(*) AS string FROM pg_class c LEFT JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relkind IN ('S','v','r') AND n.nspname IN ('public', 'schema2');`
	tableCount := dbconn.MustSelectString(conn, countQuery)
	Expect(tableCount).To(Equal(strconv.Itoa(numTables)))
}

func assertArtifactsCleaned(conn *dbconn.DBConn, timestamp string) {
	cmdStr := fmt.Sprintf("ps -ef | grep -v grep | grep -E gpbackup_helper.*%s || true", timestamp)
	output := mustRunCommand(exec.Command("bash", "-c", cmdStr))
	Eventually(func() string { return strings.TrimSpace(string(output)) }, 5*time.Second, 100*time.Millisecond).Should(Equal(""))

	fpInfo := backup_filepath.NewFilePathInfo(backupCluster, "", timestamp, backup_filepath.GetSegPrefix(conn))
	description := "Checking if helper files are cleaned up properly"
	cleanupFunc := func(contentID int) string {
		errorFile := fmt.Sprintf("%s_error", fpInfo.GetSegmentPipeFilePath(contentID))
		oidFile := fpInfo.GetSegmentHelperFilePath(contentID, "oid")
		scriptFile := fpInfo.GetSegmentHelperFilePath(contentID, "script")
		pipeFile := fpInfo.GetSegmentPipeFilePath(contentID)

		return fmt.Sprintf("! ls %s && ! ls %s && ! ls %s && ! ls %s*", errorFile, oidFile, scriptFile, pipeFile)
	}
	remoteOutput := backupCluster.GenerateAndExecuteCommand(description, cleanupFunc, cluster.ON_SEGMENTS_AND_MASTER)
	if remoteOutput.NumErrors != 0 {
		Fail(fmt.Sprintf("Helper files found for timestamp %s", timestamp))
	}
}

func mustRunCommand(cmd *exec.Cmd) []byte {
	output, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Printf("%s", output)
		Fail(fmt.Sprintf("%v", err))
	}
	return output
}

func copyPluginToAllHosts(conn *dbconn.DBConn, pluginPath string) {
	hostnameQuery := `SELECT DISTINCT hostname AS string FROM gp_segment_configuration WHERE content != -1`
	hostnames := dbconn.MustSelectStringSlice(conn, hostnameQuery)
	for _, hostname := range hostnames {
		pluginDir, _ := filepath.Split(pluginPath)
		command := exec.Command("ssh", hostname, fmt.Sprintf("mkdir -p %s", pluginDir))
		mustRunCommand(command)
		command = exec.Command("scp", pluginPath, fmt.Sprintf("%s:%s", hostname, pluginPath))
		mustRunCommand(command)
	}
}

func forceMetadataFileDownloadFromPlugin(conn *dbconn.DBConn, timestamp string) {
	fpInfo := backup_filepath.NewFilePathInfo(backupCluster, "", timestamp, backup_filepath.GetSegPrefix(conn))
	remoteOutput := backupCluster.GenerateAndExecuteCommand(fmt.Sprintf("Removing backups on all segments for "+
		"timestamp %s", timestamp), func(contentID int) string {
		return fmt.Sprintf("rm -rf %s", fpInfo.GetDirForContent(contentID))
	}, cluster.ON_SEGMENTS_AND_MASTER)
	if remoteOutput.NumErrors != 0 {
		Fail(fmt.Sprintf("Failed to remove backup directory for timestamp %s", timestamp))
	}
}

func skipIfOldBackupVersionBefore(version string) {
	if useOldBackupVersion && oldBackupSemVer.LT(semver.MustParse(version)) {
		Skip(fmt.Sprintf("Feature not supported in gpbackup %s", oldBackupSemVer))
	}
}

func createGlobalObjects(conn *dbconn.DBConn) {
	if conn.Version.Before("6") {
		testhelper.AssertQueryRuns(conn, "CREATE TABLESPACE test_tablespace FILESPACE test_dir")
	} else {
		testhelper.AssertQueryRuns(conn, "CREATE TABLESPACE test_tablespace LOCATION '/tmp/test_dir';")
	}
	testhelper.AssertQueryRuns(conn, "CREATE RESOURCE QUEUE test_queue WITH (ACTIVE_STATEMENTS=5);")
	testhelper.AssertQueryRuns(conn, "CREATE ROLE global_role RESOURCE QUEUE test_queue;")
	testhelper.AssertQueryRuns(conn, "CREATE ROLE testrole;")
	testhelper.AssertQueryRuns(conn, "GRANT testrole TO global_role;")
	testhelper.AssertQueryRuns(conn, "CREATE DATABASE global_db TABLESPACE test_tablespace;")
	testhelper.AssertQueryRuns(conn, "ALTER DATABASE global_db OWNER TO global_role;")
	testhelper.AssertQueryRuns(conn, "ALTER ROLE global_role SET search_path TO public,pg_catalog;")
	if conn.Version.AtLeast("5") {
		testhelper.AssertQueryRuns(conn, "CREATE RESOURCE GROUP test_group WITH (CPU_RATE_LIMIT=1, MEMORY_LIMIT=1);")
		testhelper.AssertQueryRuns(conn, "ALTER ROLE global_role RESOURCE GROUP test_group;")
	}
}

func dropGlobalObjects(conn *dbconn.DBConn, dbExists bool) {
	if dbExists {
		testhelper.AssertQueryRuns(conn, "DROP DATABASE global_db;")
	}
	testhelper.AssertQueryRuns(conn, "DROP TABLESPACE test_tablespace;")
	testhelper.AssertQueryRuns(conn, "DROP ROLE global_role;")
	testhelper.AssertQueryRuns(conn, "DROP ROLE testrole;")
	testhelper.AssertQueryRuns(conn, "DROP RESOURCE QUEUE test_queue;")
	if conn.Version.AtLeast("5") {
		testhelper.AssertQueryRuns(conn, "DROP RESOURCE GROUP test_group;")
	}
}

func TestEndToEnd(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "EndToEnd Suite")
}

var _ = Describe("backup end to end integration tests", func() {

	var backupConn, restoreConn *dbconn.DBConn
	var gpbackupPath, backupHelperPath, restoreHelperPath, gprestorePath, pluginConfigPath string

	BeforeSuite(func() {

		// This is used to run tests from an older gpbackup version to gprestore latest
		useOldBackupVersion = os.Getenv("OLD_BACKUP_VERSION") != ""
		pluginConfigPath =
			fmt.Sprintf("%s/go/src/github.com/greenplum-db/gpbackup/plugins/example_plugin_config.yaml",
				os.Getenv("HOME"))
		var err error
		testhelper.SetupTestLogger()
		exec.Command("dropdb", "testdb").Run()
		exec.Command("dropdb", "restoredb").Run()

		err = exec.Command("createdb", "testdb").Run()
		if err != nil {
			Fail(fmt.Sprintf("Could not create testdb: %v", err))
		}
		err = exec.Command("createdb", "restoredb").Run()
		if err != nil {
			Fail(fmt.Sprintf("Could not create restoredb: %v", err))
		}
		backupConn = testutils.SetupTestDbConn("testdb")
		restoreConn = testutils.SetupTestDbConn("restoredb")
		testutils.ExecuteSQLFile(backupConn, "test_tables_ddl.sql")
		testutils.ExecuteSQLFile(backupConn, "test_tables_data.sql")
		if useOldBackupVersion {
			oldBackupSemVer = semver.MustParse(os.Getenv("OLD_BACKUP_VERSION"))
			_, restoreHelperPath, gprestorePath = buildAndInstallBinaries()
			gpbackupPath, backupHelperPath = buildOldBinaries(os.Getenv("OLD_BACKUP_VERSION"))
		} else {
			gpbackupPath, backupHelperPath, gprestorePath = buildAndInstallBinaries()
			restoreHelperPath = backupHelperPath
		}
		segConfig := cluster.MustGetSegmentConfiguration(backupConn)
		backupCluster = cluster.NewCluster(segConfig)

		if backupConn.Version.Before("6") {
			testutils.SetupTestFilespace(backupConn, backupCluster)
		} else {
			remoteOutput := backupCluster.GenerateAndExecuteCommand("Creating filespace test directories on all hosts", func(contentID int) string {
				return fmt.Sprintf("mkdir -p /tmp/test_dir && mkdir -p /tmp/test_dir1 && mkdir -p /tmp/test_dir2")
			}, cluster.ON_HOSTS_AND_MASTER)
			if remoteOutput.NumErrors != 0 {
				Fail("Could not create filespace test directory on 1 or more hosts")
			}
		}
	})
	AfterSuite(func() {
		if backupConn.Version.Before("6") {
			testutils.DestroyTestFilespace(backupConn)
		} else {
			remoteOutput := backupCluster.GenerateAndExecuteCommand("Removing /tmp/test_dir* directories on all hosts", func(contentID int) string {
				return fmt.Sprintf("rm -rf /tmp/test_dir*")
			}, cluster.ON_HOSTS_AND_MASTER)
			if remoteOutput.NumErrors != 0 {
				Fail("Could not remove /tmp/testdir* directories on 1 or more hosts")
			}
		}
		if backupConn != nil {
			backupConn.Close()
		}
		if restoreConn != nil {
			restoreConn.Close()
		}
		gexec.CleanupBuildArtifacts()
		err := exec.Command("dropdb", "testdb").Run()
		if err != nil {
			fmt.Printf("Could not drop testdb: %v\n", err)
		}
		err = exec.Command("dropdb", "restoredb").Run()
		if err != nil {
			fmt.Printf("Could not drop restoredb: %v\n", err)
		}
	})

	Describe("end to end gpbackup and gprestore tests", func() {
		var publicSchemaTupleCounts, schema2TupleCounts map[string]int

		BeforeEach(func() {
			testhelper.AssertQueryRuns(restoreConn, "DROP SCHEMA IF EXISTS schema2 CASCADE; DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
			publicSchemaTupleCounts = map[string]int{
				"public.foo":   40000,
				"public.holds": 50000,
				"public.sales": 13,
			}
			schema2TupleCounts = map[string]int{
				"schema2.returns": 6,
				"schema2.foo2":    0,
				"schema2.foo3":    100,
				"schema2.ao1":     1000,
				"schema2.ao2":     1000,
			}
		})
		Describe("Backup include filtering", func() {
			It("runs gpbackup and gprestore with include-schema backup flag and compression level", func() {
				timestamp := gpbackup(gpbackupPath, backupHelperPath, "--include-schema", "public", "--compression-level", "2")
				gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb")

				assertRelationsCreated(restoreConn, 19)
				assertDataRestored(restoreConn, publicSchemaTupleCounts)
				assertArtifactsCleaned(restoreConn, timestamp)
			})
			It("runs gpbackup and gprestore with include-table backup flag", func() {
				skipIfOldBackupVersionBefore("1.4.0")
				timestamp := gpbackup(gpbackupPath, backupHelperPath, "--include-table", "public.foo", "--include-table", "public.sales", "--include-table", "public.myseq1", "--include-table", "public.myview1")
				gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb")

				assertRelationsCreated(restoreConn, 16)
				assertDataRestored(restoreConn, map[string]int{"public.foo": 40000})

				os.Remove("/tmp/include-tables.txt")
			})
			It("runs gpbackup and gprestore with include-table-file backup flag", func() {
				skipIfOldBackupVersionBefore("1.4.0")
				includeFile := iohelper.MustOpenFileForWriting("/tmp/include-tables.txt")
				utils.MustPrintln(includeFile, "public.sales\npublic.foo\npublic.myseq1\npublic.myview1")
				timestamp := gpbackup(gpbackupPath, backupHelperPath, "--include-table-file", "/tmp/include-tables.txt")
				gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb")

				assertRelationsCreated(restoreConn, 16)
				assertDataRestored(restoreConn, map[string]int{"public.sales": 13, "public.foo": 40000})

				os.Remove("/tmp/include-tables.txt")
			})

		})
		Describe("Restore include filtering", func() {
			It("runs gpbackup and gprestore with include-schema restore flag", func() {
				backupdir := filepath.Join(custom_backup_dir, "include_schema") // Must be unique
				timestamp := gpbackup(gpbackupPath, backupHelperPath, "--backup-dir", backupdir)
				gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb", "--backup-dir", backupdir, "--include-schema", "schema2")

				assertRelationsCreated(restoreConn, 17)
				assertDataRestored(restoreConn, schema2TupleCounts)

				os.RemoveAll(backupdir)
			})
			It("runs gpbackup and gprestore with include-table restore flag", func() {
				timestamp := gpbackup(gpbackupPath, backupHelperPath)
				gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb", "--include-table", "public.foo", "--include-table", "public.sales", "--include-table", "public.myseq1", "--include-table", "public.myview1")

				assertRelationsCreated(restoreConn, 16)
				assertDataRestored(restoreConn, map[string]int{"public.sales": 13, "public.foo": 40000})
			})
			It("runs gpbackup and gprestore with include-table-file restore flag", func() {
				includeFile := iohelper.MustOpenFileForWriting("/tmp/include-tables.txt")
				utils.MustPrintln(includeFile, "public.sales\npublic.foo\npublic.myseq1\npublic.myview1")
				backupdir := filepath.Join(custom_backup_dir, "include_table_file") // Must be unique
				timestamp := gpbackup(gpbackupPath, backupHelperPath, "--backup-dir", backupdir)
				gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb", "--backup-dir", backupdir, "--include-table-file", "/tmp/include-tables.txt")

				assertRelationsCreated(restoreConn, 16)
				assertDataRestored(restoreConn, map[string]int{"public.sales": 13, "public.foo": 40000})

				os.RemoveAll(backupdir)
				os.Remove("/tmp/include-tables.txt")
			})
			It("runs gpbackup and gprestore with include-table restore flag against a leaf partition", func() {
				skipIfOldBackupVersionBefore("1.7.2")
				timestamp := gpbackup(gpbackupPath, backupHelperPath, "--leaf-partition-data")
				gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb", "--include-table", "public.sales_1_prt_jan17")

				assertRelationsCreated(restoreConn, 13)
				assertDataRestored(restoreConn, map[string]int{"public.sales": 1, "public.sales_1_prt_jan17": 1})
			})
		})
		Describe("Backup exclude filtering", func() {
			It("runs gpbackup and gprestore with exclude-schema backup flag", func() {
				timestamp := gpbackup(gpbackupPath, backupHelperPath, "--exclude-schema", "public")
				gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb")

				assertRelationsCreated(restoreConn, 17)
				assertDataRestored(restoreConn, schema2TupleCounts)
			})
			It("runs gpbackup and gprestore with exclude-table backup flag", func() {
				skipIfOldBackupVersionBefore("1.4.0")
				timestamp := gpbackup(gpbackupPath, backupHelperPath, "--exclude-table", "schema2.foo2", "--exclude-table", "schema2.returns", "--exclude-table", "public.myseq2", "--exclude-table", "public.myview2")
				gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb")

				assertRelationsCreated(restoreConn, 20)
				assertDataRestored(restoreConn, map[string]int{"schema2.foo3": 100, "public.foo": 40000, "public.holds": 50000, "public.sales": 13})

				os.Remove("/tmp/exclude-tables.txt")
			})
			It("runs gpbackup and gprestore with exclude-table-file backup flag", func() {
				skipIfOldBackupVersionBefore("1.4.0")
				excludeFile := iohelper.MustOpenFileForWriting("/tmp/exclude-tables.txt")
				utils.MustPrintln(excludeFile, "schema2.foo2\nschema2.returns\npublic.sales\npublic.myseq2\npublic.myview2")
				timestamp := gpbackup(gpbackupPath, backupHelperPath, "--exclude-table-file", "/tmp/exclude-tables.txt")
				gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb")

				assertRelationsCreated(restoreConn, 7)
				assertDataRestored(restoreConn, map[string]int{"schema2.foo3": 100, "public.foo": 40000, "public.holds": 50000})

				os.Remove("/tmp/exclude-tables.txt")
			})
		})
		Describe("Restore exclude filtering", func() {
			It("runs gpbackup and gprestore with exclude-schema restore flag", func() {
				timestamp := gpbackup(gpbackupPath, backupHelperPath)
				gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb", "--exclude-schema", "public")

				assertRelationsCreated(restoreConn, 17)
				assertDataRestored(restoreConn, schema2TupleCounts)
			})
			It("runs gpbackup and gprestore with exclude-table restore flag", func() {
				timestamp := gpbackup(gpbackupPath, backupHelperPath)
				gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb", "--exclude-table", "schema2.foo2", "--exclude-table", "schema2.returns", "--exclude-table", "public.myseq2", "--exclude-table", "public.myview2")

				assertRelationsCreated(restoreConn, 20)
				assertDataRestored(restoreConn, map[string]int{"schema2.foo3": 100, "public.foo": 40000, "public.holds": 50000, "public.sales": 13})

				os.Remove("/tmp/exclude-tables.txt")
			})
			It("runs gpbackup and gprestore with exclude-table-file restore flag", func() {
				includeFile := iohelper.MustOpenFileForWriting("/tmp/exclude-tables.txt")
				utils.MustPrintln(includeFile, "schema2.foo2\nschema2.returns\npublic.myseq2\npublic.myview2")
				backupdir := filepath.Join(custom_backup_dir, "exclude_table_file") // Must be unique
				timestamp := gpbackup(gpbackupPath, backupHelperPath, "--backup-dir", backupdir)
				gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb", "--backup-dir", backupdir, "--exclude-table-file", "/tmp/exclude-tables.txt")

				assertRelationsCreated(restoreConn, 20)
				assertDataRestored(restoreConn, map[string]int{"public.sales": 13, "public.foo": 40000})

				os.RemoveAll(backupdir)
				os.Remove("/tmp/exclude-tables.txt")
			})
		})
		Describe("Single data file", func() {
			It("runs gpbackup and gprestore with single-data-file flag", func() {
				backupdir := filepath.Join(custom_backup_dir, "single_data_file") // Must be unique
				timestamp := gpbackup(gpbackupPath, backupHelperPath, "--single-data-file", "--backup-dir", backupdir)
				gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb", "--backup-dir", backupdir)

				assertRelationsCreated(restoreConn, 36)
				assertDataRestored(restoreConn, publicSchemaTupleCounts)
				assertDataRestored(restoreConn, schema2TupleCounts)
				assertArtifactsCleaned(restoreConn, timestamp)

				os.RemoveAll(backupdir)
			})
			It("runs gpbackup and gprestore with single-data-file flag without compression", func() {
				backupdir := filepath.Join(custom_backup_dir, "single_data_file_no_compression") // Must be unique
				timestamp := gpbackup(gpbackupPath, backupHelperPath, "--single-data-file", "--backup-dir", backupdir, "--no-compression")
				gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb", "--backup-dir", backupdir)

				assertRelationsCreated(restoreConn, 36)
				assertDataRestored(restoreConn, publicSchemaTupleCounts)
				assertDataRestored(restoreConn, schema2TupleCounts)
				assertArtifactsCleaned(restoreConn, timestamp)

				os.RemoveAll(backupdir)
			})
			It("runs gpbackup and gprestore on database with all objects", func() {
				testhelper.AssertQueryRuns(backupConn, "DROP SCHEMA IF EXISTS schema2 CASCADE; DROP SCHEMA public CASCADE; CREATE SCHEMA public; DROP PROCEDURAL LANGUAGE IF EXISTS plpythonu;")
				defer testutils.ExecuteSQLFile(backupConn, "test_tables_data.sql")
				defer testutils.ExecuteSQLFile(backupConn, "test_tables_ddl.sql")
				defer testhelper.AssertQueryRuns(backupConn, "DROP SCHEMA IF EXISTS schema2 CASCADE; DROP SCHEMA public CASCADE; CREATE SCHEMA public; DROP PROCEDURAL LANGUAGE IF EXISTS plpythonu;")
				defer testhelper.AssertQueryRuns(restoreConn, "DROP SCHEMA IF EXISTS schema2 CASCADE; DROP SCHEMA public CASCADE; CREATE SCHEMA public; DROP PROCEDURAL LANGUAGE IF EXISTS plpythonu;")
				testhelper.AssertQueryRuns(backupConn, "CREATE ROLE testrole SUPERUSER")
				defer testhelper.AssertQueryRuns(backupConn, "DROP ROLE testrole")
				testutils.ExecuteSQLFile(backupConn, "gpdb4_objects.sql")
				if backupConn.Version.AtLeast("5") {
					testutils.ExecuteSQLFile(backupConn, "gpdb5_objects.sql")
				}
				if backupConn.Version.AtLeast("6") {
					testutils.ExecuteSQLFile(backupConn, "gpdb6_objects.sql")
					defer testhelper.AssertQueryRuns(backupConn, "DROP FOREIGN DATA WRAPPER fdw CASCADE;")
					defer testhelper.AssertQueryRuns(restoreConn, "DROP FOREIGN DATA WRAPPER fdw CASCADE;")
				}
				timestamp := gpbackup(gpbackupPath, backupHelperPath, "--leaf-partition-data", "--single-data-file")
				gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb")
				assertArtifactsCleaned(restoreConn, timestamp)
			})

			Context("with include filtering on restore", func() {
				It("runs gpbackup and gprestore with include-table-file restore flag with a single data file", func() {
					includeFile := iohelper.MustOpenFileForWriting("/tmp/include-tables.txt")
					utils.MustPrintln(includeFile, "public.sales\npublic.foo\npublic.myseq1\npublic.myview1")
					backupdir := filepath.Join(custom_backup_dir, "include_table_file_single_data_file") // Must be unique
					timestamp := gpbackup(gpbackupPath, backupHelperPath, "--backup-dir", backupdir, "--single-data-file")
					gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb", "--backup-dir", backupdir, "--include-table-file", "/tmp/include-tables.txt")
					assertRelationsCreated(restoreConn, 16)
					assertDataRestored(restoreConn, map[string]int{"public.sales": 13, "public.foo": 40000})
					assertArtifactsCleaned(restoreConn, timestamp)

					os.RemoveAll(backupdir)
					os.Remove("/tmp/include-tables.txt")
				})
				It("runs gpbackup and gprestore with include-schema restore flag with a single data file", func() {
					backupdir := filepath.Join(custom_backup_dir, "include_schema_single_data_file") // Must be unique
					timestamp := gpbackup(gpbackupPath, backupHelperPath, "--backup-dir", backupdir, "--single-data-file")
					gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb", "--backup-dir", backupdir, "--include-schema", "schema2")

					assertRelationsCreated(restoreConn, 17)
					assertDataRestored(restoreConn, schema2TupleCounts)
					assertArtifactsCleaned(restoreConn, timestamp)

					os.RemoveAll(backupdir)
				})
			})

			Context("with plugin", func() {
				BeforeEach(func() {
					skipIfOldBackupVersionBefore("1.7.0")
				})
				It("runs gpbackup and gprestore with plugin, single-data-file, and no-compression", func() {
					pluginDir := "/tmp/plugin_dest"
					pluginExecutablePath := fmt.Sprintf("%s/go/src/github.com/greenplum-db/gpbackup/plugins/example_plugin.sh", os.Getenv("HOME"))
					copyPluginToAllHosts(backupConn, pluginExecutablePath)

					timestamp := gpbackup(gpbackupPath, backupHelperPath, "--single-data-file", "--no-compression", "--plugin-config", pluginConfigPath)
					forceMetadataFileDownloadFromPlugin(backupConn, timestamp)

					gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb", "--plugin-config", pluginConfigPath)

					assertRelationsCreated(restoreConn, 36)
					assertDataRestored(restoreConn, publicSchemaTupleCounts)
					assertDataRestored(restoreConn, schema2TupleCounts)
					assertArtifactsCleaned(restoreConn, timestamp)

					os.RemoveAll(pluginDir)
				})
				It("runs gpbackup and gprestore with plugin and single-data-file", func() {
					pluginDir := "/tmp/plugin_dest"
					pluginExecutablePath := fmt.Sprintf("%s/go/src/github.com/greenplum-db/gpbackup/plugins/example_plugin.sh", os.Getenv("HOME"))
					copyPluginToAllHosts(backupConn, pluginExecutablePath)

					timestamp := gpbackup(gpbackupPath, backupHelperPath, "--single-data-file", "--plugin-config", pluginConfigPath)
					forceMetadataFileDownloadFromPlugin(backupConn, timestamp)

					gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb", "--plugin-config", pluginConfigPath)

					assertRelationsCreated(restoreConn, 36)
					assertDataRestored(restoreConn, publicSchemaTupleCounts)
					assertDataRestored(restoreConn, schema2TupleCounts)
					assertArtifactsCleaned(restoreConn, timestamp)

					os.RemoveAll(pluginDir)
				})
				It("runs gpbackup and gprestore with plugin and metadata-only", func() {
					pluginDir := "/tmp/plugin_dest"
					pluginExecutablePath := fmt.Sprintf("%s/go/src/github.com/greenplum-db/gpbackup/plugins/example_plugin.sh", os.Getenv("HOME"))
					copyPluginToAllHosts(backupConn, pluginExecutablePath)

					timestamp := gpbackup(gpbackupPath, backupHelperPath, "--metadata-only", "--plugin-config", pluginConfigPath)
					forceMetadataFileDownloadFromPlugin(backupConn, timestamp)

					gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb", "--plugin-config", pluginConfigPath)

					assertRelationsCreated(restoreConn, 36)
					assertArtifactsCleaned(restoreConn, timestamp)

					os.RemoveAll(pluginDir)
				})
			})
		})
		Describe("Multi-file Plugin", func() {
			It("runs gpbackup and gprestore with plugin and no-compression", func() {
				skipIfOldBackupVersionBefore("1.7.0")
				pluginDir := "/tmp/plugin_dest"
				pluginExecutablePath := fmt.Sprintf("%s/go/src/github.com/greenplum-db/gpbackup/plugins/example_plugin.sh", os.Getenv("HOME"))
				copyPluginToAllHosts(backupConn, pluginExecutablePath)

				timestamp := gpbackup(gpbackupPath, backupHelperPath, "--no-compression", "--plugin-config", pluginConfigPath)
				forceMetadataFileDownloadFromPlugin(backupConn, timestamp)

				gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb", "--plugin-config", pluginConfigPath)

				assertRelationsCreated(restoreConn, 36)
				assertDataRestored(restoreConn, publicSchemaTupleCounts)
				assertDataRestored(restoreConn, schema2TupleCounts)

				os.RemoveAll(pluginDir)
			})
			It("runs gpbackup and gprestore with plugin and compression", func() {
				skipIfOldBackupVersionBefore("1.7.0")
				pluginDir := "/tmp/plugin_dest"
				pluginExecutablePath := fmt.Sprintf("%s/go/src/github.com/greenplum-db/gpbackup/plugins/example_plugin.sh", os.Getenv("HOME"))
				copyPluginToAllHosts(backupConn, pluginExecutablePath)

				timestamp := gpbackup(gpbackupPath, backupHelperPath, "--plugin-config", pluginConfigPath)
				forceMetadataFileDownloadFromPlugin(backupConn, timestamp)

				gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb", "--plugin-config", pluginConfigPath)

				assertRelationsCreated(restoreConn, 36)
				assertDataRestored(restoreConn, publicSchemaTupleCounts)
				assertDataRestored(restoreConn, schema2TupleCounts)

				os.RemoveAll(pluginDir)
			})
		})
		Describe("Incremental", func() {
			BeforeEach(func() {
				skipIfOldBackupVersionBefore("1.7.0")
			})
			It("restores from an incremental backup specified with a timestamp", func() {
				fullBackupTimestamp := gpbackup(gpbackupPath, backupHelperPath, "--leaf-partition-data")

				testhelper.AssertQueryRuns(backupConn, "INSERT into schema2.ao1 values(1001)")
				defer testhelper.AssertQueryRuns(backupConn, "DELETE from schema2.ao1 where i=1001")
				incremental1Timestamp := gpbackup(gpbackupPath, backupHelperPath,
					"--incremental", "--leaf-partition-data", "--from-timestamp", fullBackupTimestamp)

				testhelper.AssertQueryRuns(backupConn, "INSERT into schema2.ao1 values(1002)")
				defer testhelper.AssertQueryRuns(backupConn, "DELETE from schema2.ao1 where i=1002")
				incremental2Timestamp := gpbackup(gpbackupPath, backupHelperPath,
					"--incremental", "--leaf-partition-data", "--from-timestamp", incremental1Timestamp)

				gprestore(gprestorePath, restoreHelperPath, incremental2Timestamp, "--redirect-db", "restoredb")

				assertRelationsCreated(restoreConn, 36)
				assertDataRestored(restoreConn, publicSchemaTupleCounts)
				schema2TupleCounts["schema2.ao1"] = 1002
				assertDataRestored(restoreConn, schema2TupleCounts)
			})
			Context("Without a timestamp", func() {
				It("restores from a incremental backup specified with a backup directory", func() {
					backupdir := filepath.Join(custom_backup_dir, "test_incremental") // Must be unique
					_ = gpbackup(gpbackupPath, backupHelperPath, "--leaf-partition-data", "--backup-dir", backupdir)

					testhelper.AssertQueryRuns(backupConn, "INSERT into schema2.ao1 values(1001)")
					defer testhelper.AssertQueryRuns(backupConn, "DELETE from schema2.ao1 where i=1001")
					_ = gpbackup(gpbackupPath, backupHelperPath,
						"--incremental", "--leaf-partition-data", "--backup-dir", backupdir)

					testhelper.AssertQueryRuns(backupConn, "INSERT into schema2.ao1 values(1002)")
					defer testhelper.AssertQueryRuns(backupConn, "DELETE from schema2.ao1 where i=1002")
					incremental2Timestamp := gpbackup(gpbackupPath, backupHelperPath,
						"--incremental", "--leaf-partition-data", "--backup-dir", backupdir)

					gprestore(gprestorePath, restoreHelperPath, incremental2Timestamp, "--redirect-db", "restoredb", "--backup-dir", backupdir)

					assertRelationsCreated(restoreConn, 36)
					assertDataRestored(restoreConn, publicSchemaTupleCounts)
					schema2TupleCounts["schema2.ao1"] = 1002
					assertDataRestored(restoreConn, schema2TupleCounts)

					os.Remove(backupdir)
				})
				It("restores from a filtered incremental backup with partition tables", func() {
					_ = gpbackup(gpbackupPath, backupHelperPath, "--leaf-partition-data", "--include-table", "public.sales")

					testhelper.AssertQueryRuns(backupConn, "INSERT into sales VALUES(19, '2017-02-15'::date, 100)")
					defer testhelper.AssertQueryRuns(backupConn, "DELETE from sales where id=19")
					_ = gpbackup(gpbackupPath, backupHelperPath,
						"--incremental", "--leaf-partition-data", "--include-table", "public.sales")

					testhelper.AssertQueryRuns(backupConn, "INSERT into sales VALUES(20, '2017-03-15'::date, 100)")
					defer testhelper.AssertQueryRuns(backupConn, "DELETE from sales where id=20")
					incremental2Timestamp := gpbackup(gpbackupPath, backupHelperPath,
						"--incremental", "--leaf-partition-data", "--include-table", "public.sales")

					gprestore(gprestorePath, restoreHelperPath, incremental2Timestamp, "--redirect-db", "restoredb")

					assertDataRestored(restoreConn, map[string]int{
						"public.sales":             15,
						"public.sales_1_prt_feb17": 2,
						"public.sales_1_prt_mar17": 2,
					})
				})
				It("restores from full incremental backup with partition tables with restore table filtering", func() {
					skipIfOldBackupVersionBefore("1.7.2")
					testhelper.AssertQueryRuns(backupConn, "INSERT into sales VALUES(19, '2017-02-15'::date, 100)")
					defer testhelper.AssertQueryRuns(backupConn, "DELETE from sales where id=19")
					_ = gpbackup(gpbackupPath, backupHelperPath, "--leaf-partition-data")

					incremental1Timestamp := gpbackup(gpbackupPath, backupHelperPath, "--incremental", "--leaf-partition-data")

					gprestore(gprestorePath, restoreHelperPath, incremental1Timestamp, "--redirect-db", "restoredb", "--include-table", "public.sales_1_prt_feb17")

					assertDataRestored(restoreConn, map[string]int{
						"public.sales":             2,
						"public.sales_1_prt_feb17": 2,
					})
				})
				It("restores from a new incremental backup", func() {
					if !useOldBackupVersion {
						Skip("This test is only needed for old backup versions")
					}
					_ = gpbackup(gpbackupPath, backupHelperPath, "--leaf-partition-data")

					testhelper.AssertQueryRuns(backupConn, "INSERT into schema2.ao1 values(1001)")
					defer testhelper.AssertQueryRuns(backupConn, "DELETE from schema2.ao1 where i=1001")
					_ = gpbackup(gpbackupPath, backupHelperPath,
						"--incremental", "--leaf-partition-data")

					gpbackupPath, backupHelperPath, _ = buildAndInstallBinaries()

					testhelper.AssertQueryRuns(backupConn, "INSERT into schema2.ao1 values(1002)")
					defer testhelper.AssertQueryRuns(backupConn, "DELETE from schema2.ao1 where i=1002")
					incremental2Timestamp := gpbackup(gpbackupPath, backupHelperPath,
						"--incremental", "--leaf-partition-data")

					gpbackupPath, backupHelperPath = buildOldBinaries(os.Getenv("OLD_BACKUP_VERSION"))

					gprestore(gprestorePath, restoreHelperPath, incremental2Timestamp, "--redirect-db", "restoredb")

					assertRelationsCreated(restoreConn, 36)
					assertDataRestored(restoreConn, publicSchemaTupleCounts)
					schema2TupleCounts["schema2.ao1"] = 1002
					assertDataRestored(restoreConn, schema2TupleCounts)

				})
			})
			Context("With a plugin", func() {
				var pluginDir string
				BeforeEach(func() {
					pluginDir = "/tmp/plugin_dest"
					pluginExecutablePath := fmt.Sprintf("%s/go/src/github.com/greenplum-db/gpbackup/plugins/example_plugin.sh", os.Getenv("HOME"))
					copyPluginToAllHosts(backupConn, pluginExecutablePath)
				})
				AfterEach(func() {
					os.RemoveAll(pluginDir)
				})
				It("Restores from an incremental backup based on a from-timestamp incremental", func() {
					fullBackupTimestamp := gpbackup(gpbackupPath, backupHelperPath,
						"--leaf-partition-data", "--single-data-file", "--plugin-config", pluginConfigPath)
					forceMetadataFileDownloadFromPlugin(backupConn, fullBackupTimestamp)
					testhelper.AssertQueryRuns(backupConn, "INSERT into schema2.ao1 values(1001)")

					defer testhelper.AssertQueryRuns(backupConn, "DELETE from schema2.ao1 where i=1001")
					incremental1Timestamp := gpbackup(gpbackupPath, backupHelperPath,
						"--incremental", "--leaf-partition-data", "--single-data-file", "--from-timestamp",
						fullBackupTimestamp, "--plugin-config", pluginConfigPath)
					forceMetadataFileDownloadFromPlugin(backupConn, incremental1Timestamp)

					testhelper.AssertQueryRuns(backupConn, "INSERT into schema2.ao1 values(1002)")
					defer testhelper.AssertQueryRuns(backupConn, "DELETE from schema2.ao1 where i=1002")
					incremental2Timestamp := gpbackup(gpbackupPath, backupHelperPath,
						"--incremental", "--leaf-partition-data", "--single-data-file", "--plugin-config",
						pluginConfigPath)
					forceMetadataFileDownloadFromPlugin(backupConn, incremental2Timestamp)

					gprestore(gprestorePath, restoreHelperPath, incremental2Timestamp,
						"--redirect-db", "restoredb", "--plugin-config", pluginConfigPath)

					assertRelationsCreated(restoreConn, 36)
					assertDataRestored(restoreConn, publicSchemaTupleCounts)
					schema2TupleCounts["schema2.ao1"] = 1002
					assertDataRestored(restoreConn, schema2TupleCounts)
					assertArtifactsCleaned(restoreConn, fullBackupTimestamp)
					assertArtifactsCleaned(restoreConn, incremental1Timestamp)
					assertArtifactsCleaned(restoreConn, incremental2Timestamp)
				})
			})
		})
		Describe("globals tests", func() {
			It("runs gpbackup and gprestore with --with-globals", func() {
				skipIfOldBackupVersionBefore("1.8.2")
				createGlobalObjects(backupConn)

				timestamp := gpbackup(gpbackupPath, backupHelperPath)

				dropGlobalObjects(backupConn, true)
				defer dropGlobalObjects(backupConn, false)

				gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb", "--with-globals")
			})
			It("runs gpbackup and gprestore with --with-globals and --create-db", func() {
				skipIfOldBackupVersionBefore("1.8.2")
				createGlobalObjects(backupConn)
				if backupConn.Version.AtLeast("6") {
					testhelper.AssertQueryRuns(backupConn, "ALTER ROLE global_role IN DATABASE global_db SET search_path TO public,pg_catalog;")
				}

				timestamp := gpbackup(gpbackupPath, backupHelperPath)

				dropGlobalObjects(backupConn, true)
				defer dropGlobalObjects(backupConn, true)

				gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "global_db", "--with-globals", "--create-db")
			})
		})
		It("runs gpbackup and gprestore without redirecting restore to another db", func() {
			timestamp := gpbackup(gpbackupPath, backupHelperPath)
			backupConn.Close()
			err := exec.Command("dropdb", "testdb").Run()
			if err != nil {
				Fail(fmt.Sprintf("%v", err))
			}
			gprestore(gprestorePath, restoreHelperPath, timestamp, "--create-db")
			backupConn = testutils.SetupTestDbConn("testdb")
		})
		It("runs gpbackup and gprestore with redirecting restore to another db containing special capital letters", func() {
			timestamp := gpbackup(gpbackupPath, backupHelperPath)
			gprestore(gprestorePath, restoreHelperPath, timestamp, "--create-db", "--redirect-db", "CAPS")
			err := exec.Command("dropdb", `CAPS`).Run()
			if err != nil {
				Fail(fmt.Sprintf("%v", err))
			}
		})
		It("runs basic gpbackup and gprestore with metadata and data-only flags", func() {
			timestamp := gpbackup(gpbackupPath, backupHelperPath, "--metadata-only")
			timestamp2 := gpbackup(gpbackupPath, backupHelperPath, "--data-only")
			gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb")
			assertDataRestored(restoreConn, map[string]int{"public.foo": 0, "schema2.foo3": 0})
			assertRelationsCreated(restoreConn, 36)
			gprestore(gprestorePath, restoreHelperPath, timestamp2, "--redirect-db", "restoredb")

			assertDataRestored(restoreConn, publicSchemaTupleCounts)
			assertDataRestored(restoreConn, schema2TupleCounts)
		})
		It("runs gpbackup and gprestore with metadata-only backup flag", func() {
			timestamp := gpbackup(gpbackupPath, backupHelperPath, "--metadata-only")
			gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb")

			assertDataRestored(restoreConn, map[string]int{"public.foo": 0, "schema2.foo3": 0})
			assertRelationsCreated(restoreConn, 36)
		})
		It("runs gpbackup and gprestore with data-only backup flag", func() {
			testutils.ExecuteSQLFile(restoreConn, "test_tables_ddl.sql")

			timestamp := gpbackup(gpbackupPath, backupHelperPath, "--data-only")
			gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb")

			assertDataRestored(restoreConn, publicSchemaTupleCounts)
			assertDataRestored(restoreConn, schema2TupleCounts)
		})

		It("runs gpbackup and gprestore with the data-only restore flag", func() {
			testutils.ExecuteSQLFile(restoreConn, "test_tables_ddl.sql")
			timestamp := gpbackup(gpbackupPath, backupHelperPath)
			gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb", "--data-only")

			assertDataRestored(restoreConn, publicSchemaTupleCounts)
			assertDataRestored(restoreConn, schema2TupleCounts)
		})
		It("runs gpbackup and gprestore with the metadata-only restore flag", func() {
			timestamp := gpbackup(gpbackupPath, backupHelperPath)
			gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb", "--metadata-only")

			assertDataRestored(restoreConn, map[string]int{"public.foo": 0, "schema2.foo3": 0})
			assertRelationsCreated(restoreConn, 36)
		})
		It("runs gpbackup and gprestore with leaf-partition-data and backupdir flags", func() {
			backupdir := filepath.Join(custom_backup_dir, "leaf_partition_data") // Must be unique
			timestamp := gpbackup(gpbackupPath, backupHelperPath, "--leaf-partition-data", "--backup-dir", backupdir)
			output := gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb", "--backup-dir", backupdir)
			Expect(string(output)).To(ContainSubstring("table 30 of 30"))

			assertDataRestored(restoreConn, publicSchemaTupleCounts)
			assertDataRestored(restoreConn, schema2TupleCounts)

			os.RemoveAll(backupdir)
		})
		It("runs gpbackup and gprestore with no-compression flag", func() {
			backupdir := filepath.Join(custom_backup_dir, "no_compression") // Must be unique
			timestamp := gpbackup(gpbackupPath, backupHelperPath, "--no-compression", "--backup-dir", backupdir)
			gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb", "--backup-dir", backupdir)
			configFile, _ := filepath.Glob(filepath.Join(backupdir, "*-1/backups/*", timestamp, "*config.yaml"))
			contents, _ := ioutil.ReadFile(configFile[0])

			Expect(string(contents)).To(ContainSubstring("compressed: false"))
			assertRelationsCreated(restoreConn, 36)
			assertDataRestored(restoreConn, publicSchemaTupleCounts)
			assertDataRestored(restoreConn, schema2TupleCounts)

			os.RemoveAll(backupdir)
		})
		It("runs gpbackup and gprestore with with-stats flag", func() {
			backupdir := filepath.Join(custom_backup_dir, "with_stats") // Must be unique
			timestamp := gpbackup(gpbackupPath, backupHelperPath, "--with-stats", "--backup-dir", backupdir)
			files, _ := filepath.Glob(filepath.Join(backupdir, "*-1/backups/*", timestamp, "*statistics.sql"))

			Expect(files).To(HaveLen(1))
			output := gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb", "--with-stats", "--backup-dir", backupdir)

			Expect(string(output)).To(ContainSubstring("Query planner statistics restore complete"))
			assertDataRestored(restoreConn, publicSchemaTupleCounts)
			assertDataRestored(restoreConn, schema2TupleCounts)

			os.RemoveAll(backupdir)
		})
		It("runs gpbackup and gprestore with jobs flag", func() {
			skipIfOldBackupVersionBefore("1.3.0")
			backupdir := filepath.Join(custom_backup_dir, "parallel") // Must be unique
			timestamp := gpbackup(gpbackupPath, backupHelperPath, "--backup-dir", backupdir, "--jobs", "4")
			gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb", "--backup-dir", backupdir, "--jobs", "4")

			assertRelationsCreated(restoreConn, 36)
			assertDataRestored(restoreConn, schema2TupleCounts)
			assertDataRestored(restoreConn, publicSchemaTupleCounts)

			os.RemoveAll(backupdir)
		})
		It("runs gpbackup and sends a SIGINT to ensure cleanup functions successfully", func() {
			if useOldBackupVersion {
				Skip("This test is not needed for old backup versions")
			}
			backupdir := filepath.Join(custom_backup_dir, "backup_signals") // Must be unique
			args := []string{"--dbname", "testdb", "--backup-dir", backupdir, "--single-data-file", "--verbose"}
			cmd := exec.Command(gpbackupPath, args...)
			go func() {
				/*
				 * We use a random delay for the sleep in this test (between
				 * 0.5s and 1.5s) so that gpbackup will be interrupted at a
				 * different point in the backup process every time to help
				 * catch timing issues with the cleanup.
				 */
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))
				time.Sleep(time.Duration(rng.Intn(1000)+500) * time.Millisecond)
				cmd.Process.Signal(os.Interrupt)
			}()
			output, _ := cmd.CombinedOutput()
			stdout := string(output)

			Expect(stdout).To(ContainSubstring("Received a termination signal, aborting backup process"))
			Expect(stdout).To(ContainSubstring("Cleanup complete"))
			Expect(stdout).To(Not(ContainSubstring("CRITICAL")))

			os.RemoveAll(backupdir)
		})
		It("runs gprestore and sends a SIGINT to ensure cleanup functions successfully", func() {
			if useOldBackupVersion {
				Skip("This test is not needed for old backup versions")
			}
			backupdir := filepath.Join(custom_backup_dir, "restore_signals") // Must be unique
			timestamp := gpbackup(gpbackupPath, backupHelperPath, "--backup-dir", backupdir, "--single-data-file")
			args := []string{"--timestamp", timestamp, "--redirect-db", "restoredb", "--backup-dir", backupdir, "--include-schema", "schema2", "--verbose"}
			cmd := exec.Command(gprestorePath, args...)
			go func() {
				/*
				 * We use a random delay for the sleep in this test (between
				 * 0.5s and 1.5s) so that gprestore will be interrupted at a
				 * different point in the backup process every time to help
				 * catch timing issues with the cleanup.
				 */
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))
				time.Sleep(time.Duration(rng.Intn(1000)+500) * time.Millisecond)
				cmd.Process.Signal(os.Interrupt)
			}()
			output, _ := cmd.CombinedOutput()
			stdout := string(output)

			Expect(stdout).To(ContainSubstring("Received a termination signal, aborting restore process"))
			Expect(stdout).To(ContainSubstring("Cleanup complete"))
			Expect(stdout).To(Not(ContainSubstring("CRITICAL")))
			assertArtifactsCleaned(restoreConn, timestamp)

			os.RemoveAll(backupdir)
		})
		It("runs example_plugin.sh with plugin_test_bench", func() {
			skipIfOldBackupVersionBefore("1.7.0")
			pluginsDir := fmt.Sprintf("%s/go/src/github.com/greenplum-db/gpbackup/plugins", os.Getenv("HOME"))
			copyPluginToAllHosts(backupConn, fmt.Sprintf("%s/example_plugin.sh", pluginsDir))
			command := exec.Command("bash", "-c", fmt.Sprintf("%s/plugin_test_bench.sh %s/example_plugin.sh %s/example_plugin_config.yaml", pluginsDir, pluginsDir, pluginsDir))
			mustRunCommand(command)

			os.RemoveAll("/tmp/plugin_dest")
		})
		It("runs gpbackup with --version flag", func() {
			if useOldBackupVersion {
				Skip("This test is not needed for old backup versions")
			}
			command := exec.Command(gpbackupPath, "--version")
			output := mustRunCommand(command)
			Expect(string(output)).To(MatchRegexp(`gpbackup version \w+`))
		})
		It("runs gprestore with --version flag", func() {
			command := exec.Command(gprestorePath, "--version")
			output := mustRunCommand(command)
			Expect(string(output)).To(MatchRegexp(`gprestore version \w+`))
		})

	})
})
