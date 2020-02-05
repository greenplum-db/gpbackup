package end_to_end_test

import (
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"os/exec"
	path "path/filepath"
	"regexp"
	"sort"
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
	"github.com/greenplum-db/gpbackup/filepath"
	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gexec"
)

/* The backup directory must be unique per test. There is test flakiness
 * against Data Domain Boost mounted file systems due to how it handles
 * directory deletion/creation.
 */
var customBackupDir string

var useOldBackupVersion bool
var oldBackupSemVer semver.Version

var backupCluster *cluster.Cluster
var historyFilePath string
var saveHistoryFilePath = "/tmp/end_to_end_save_history_file.yaml"

// This function is run automatically by ginkgo before any tests are run.
func init() {
	flag.StringVar(&customBackupDir, "custom_backup_dir", "/tmp", "custom_backup_flag for testing against a configurable directory")
}

/* This function is a helper function to execute gpbackup and return a session
 * to allow checking its output.
 */
func gpbackup(gpbackupPath string, backupHelperPath string, args ...string) string {
	if useOldBackupVersion {
		_ = os.Chdir("..")
		command := exec.Command("make", "install", fmt.Sprintf("helper_path=%s", backupHelperPath))
		mustRunCommand(command)
		_ = os.Chdir("end_to_end")
	}
	args = append([]string{"--verbose", "--dbname", "testdb"}, args...)
	command := exec.Command(gpbackupPath, args...)
	output := mustRunCommand(command)
	r := regexp.MustCompile(`Backup Timestamp = (\d{14})`)
	return r.FindStringSubmatch(fmt.Sprintf("%s", output))[1]
}

func gprestore(gprestorePath string, restoreHelperPath string, timestamp string, args ...string) []byte {
	if useOldBackupVersion {
		_ = os.Chdir("..")
		command := exec.Command("make", "install", fmt.Sprintf("helper_path=%s", restoreHelperPath))
		mustRunCommand(command)
		_ = os.Chdir("end_to_end")
	}
	args = append([]string{"--verbose", "--timestamp", timestamp}, args...)
	command := exec.Command(gprestorePath, args...)
	output := mustRunCommand(command)
	return output
}

func buildAndInstallBinaries() (string, string, string) {
	_ = os.Chdir("..")
	command := exec.Command("make", "build")
	mustRunCommand(command)
	_ = os.Chdir("end_to_end")
	binDir := fmt.Sprintf("%s/go/bin", operating.System.Getenv("HOME"))
	return fmt.Sprintf("%s/gpbackup", binDir), fmt.Sprintf("%s/gpbackup_helper", binDir), fmt.Sprintf("%s/gprestore", binDir)
}

func buildOldBinaries(version string) (string, string) {
	_ = os.Chdir("..")
	command := exec.Command("git", "checkout", version, "-f")
	mustRunCommand(command)
	command = exec.Command("dep", "ensure")
	mustRunCommand(command)
	gpbackupOldPath, err := Build("github.com/greenplum-db/gpbackup", "-tags", "gpbackup", "-ldflags", fmt.Sprintf("-X github.com/greenplum-db/gpbackup/backup.version=%s", version))
	Expect(err).ShouldNot(HaveOccurred())
	gpbackupHelperOldPath, err := Build("github.com/greenplum-db/gpbackup", "-tags", "gpbackup_helper", "-ldflags", fmt.Sprintf("-X github.com/greenplum-db/gpbackup/helper.version=%s", version))
	Expect(err).ShouldNot(HaveOccurred())
	command = exec.Command("git", "checkout", "-", "-f")
	mustRunCommand(command)
	command = exec.Command("dep", "ensure")
	mustRunCommand(command)
	_ = os.Chdir("end_to_end")
	return gpbackupOldPath, gpbackupHelperOldPath
}

func assertDataRestored(conn *dbconn.DBConn, tableToTupleCount map[string]int) {
	for tableName, expectedNumTuples := range tableToTupleCount {
		actualTupleCount := dbconn.MustSelectString(conn, fmt.Sprintf("SELECT count(*) AS string from %s", tableName))
		if strconv.Itoa(expectedNumTuples) != actualTupleCount {
			Fail(fmt.Sprintf("Expected:\n\t%s rows to have been restored into table %s\nActual:\n\t%s rows were restored", strconv.Itoa(expectedNumTuples), tableName, actualTupleCount))
		}
	}
}

func assertSchemasExist(conn *dbconn.DBConn, expectedNumSchemas int) {
	countQuery := `SELECT COUNT(n.nspname) FROM pg_catalog.pg_namespace n WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' ORDER BY 1;`
	actualSchemaCount := dbconn.MustSelectString(conn, countQuery)
	if strconv.Itoa(expectedNumSchemas) != actualSchemaCount {
		Fail(fmt.Sprintf("Expected:\n\t%s schemas to exist in the DB\nActual:\n\t%s schemas are in the DB", strconv.Itoa(expectedNumSchemas), actualSchemaCount))
	}
}

func assertRelationsCreated(conn *dbconn.DBConn, expectedNumTables int) {
	countQuery := `SELECT count(*) AS string FROM pg_class c LEFT JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relkind IN ('S','v','r') AND n.nspname IN ('public', 'schema2');`
	actualTableCount := dbconn.MustSelectString(conn, countQuery)
	if strconv.Itoa(expectedNumTables) != actualTableCount {
		Fail(fmt.Sprintf("Expected:\n\t%s relations to have been created\nActual:\n\t%s relations were created", strconv.Itoa(expectedNumTables), actualTableCount))
	}
}

func assertRelationsExistForIncremental(conn *dbconn.DBConn, expectedNumTables int) {
	countQuery := `SELECT count(*) AS string FROM pg_class c LEFT JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relkind IN ('S','v','r') AND n.nspname IN ('old_schema', 'new_schema');`
	actualTableCount := dbconn.MustSelectString(conn, countQuery)
	if strconv.Itoa(expectedNumTables) != actualTableCount {
		Fail(fmt.Sprintf("Expected:\n\t%s relations to exist in old_schema and new_schema\nActual:\n\t%s relations are present", strconv.Itoa(expectedNumTables), actualTableCount))
	}
}

func assertArtifactsCleaned(conn *dbconn.DBConn, timestamp string) {
	cmdStr := fmt.Sprintf("ps -ef | grep -v grep | grep -E gpbackup_helper.*%s || true", timestamp)
	output := mustRunCommand(exec.Command("bash", "-c", cmdStr))
	Eventually(func() string { return strings.TrimSpace(string(output)) }, 5*time.Second, 100*time.Millisecond).Should(Equal(""))

	fpInfo := filepath.NewFilePathInfo(backupCluster, "", timestamp, filepath.GetSegPrefix(conn))
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
		pluginDir, _ := path.Split(pluginPath)
		command := exec.Command("ssh", hostname, fmt.Sprintf("mkdir -p %s", pluginDir))
		mustRunCommand(command)
		command = exec.Command("scp", pluginPath, fmt.Sprintf("%s:%s", hostname, pluginPath))
		mustRunCommand(command)
	}
}

func forceMetadataFileDownloadFromPlugin(conn *dbconn.DBConn, timestamp string) {
	fpInfo := filepath.NewFilePathInfo(backupCluster, "", timestamp, filepath.GetSegPrefix(conn))
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

var backupConn, restoreConn *dbconn.DBConn
var _ = Describe("backup end to end integration tests", func() {

	const (
		TOTAL_RELATIONS               = 37
		TOTAL_RELATIONS_AFTER_EXCLUDE = 21
		TOTAL_CREATE_STATEMENTS       = 9
	)

	var gpbackupPath, backupHelperPath, restoreHelperPath, gprestorePath, pluginConfigPath string

	BeforeSuite(func() {
		// This is used to run tests from an older gpbackup version to gprestore latest
		useOldBackupVersion = os.Getenv("OLD_BACKUP_VERSION") != ""
		pluginConfigPath =
			fmt.Sprintf("%s/go/src/github.com/greenplum-db/gpbackup/plugins/example_plugin_config.yaml",
				os.Getenv("HOME"))
		var err error
		testhelper.SetupTestLogger()
		_ = exec.Command("dropdb", "testdb").Run()
		_ = exec.Command("dropdb", "restoredb").Run()
		_ = exec.Command("psql", "postgres", "-c", "DROP RESOURCE QUEUE test_queue").Run()

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
			oldBackupVersionStr := os.Getenv("OLD_BACKUP_VERSION")

			_, restoreHelperPath, gprestorePath = buildAndInstallBinaries()

			// Precompiled binaries will exist when running the ci job, `backward-compatibility`
			if _, err := os.Stat(fmt.Sprintf("/tmp/%s", oldBackupVersionStr)); err == nil {
				gpbackupPath = path.Join("/tmp", oldBackupVersionStr, "gpbackup")
				backupHelperPath = path.Join("/tmp", oldBackupVersionStr, "gpbackup_helper")
			} else {
				gpbackupPath, backupHelperPath = buildOldBinaries(oldBackupVersionStr)
			}
		} else {
			// Check if gpbackup binary has been installed using gppkg
			gpHomeDir := operating.System.Getenv("GPHOME")
			binDir := fmt.Sprintf("%s/go/bin", operating.System.Getenv("HOME"))
			if _, err := os.Stat(fmt.Sprintf("%s/bin/gpbackup", gpHomeDir)); err == nil {
				binDir = fmt.Sprintf("%s/bin", gpHomeDir)
			}

			gpbackupPath = fmt.Sprintf("%s/gpbackup", binDir)
			gprestorePath = fmt.Sprintf("%s/gprestore", binDir)
			backupHelperPath = fmt.Sprintf("%s/gpbackup_helper", binDir)
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

		saveHistory(backupCluster)

		// Flag validation
		_, err = os.Stat(customBackupDir)
		if os.IsNotExist(err) {
			Fail(fmt.Sprintf("Custom backup directory %s does not exist.", customBackupDir))
		}

	})
	AfterSuite(func() {
		_ = utils.CopyFile(saveHistoryFilePath, historyFilePath)

		if backupConn.Version.Before("6") {
			testutils.DestroyTestFilespace(backupConn)
		} else {
			_ = exec.Command("psql", "postgres", "-c", "DROP RESOURCE QUEUE test_queue").Run()
			_ = exec.Command("psql", "postgres", "-c", "DROP TABLESPACE test_tablespace").Run()
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
		CleanupBuildArtifacts()
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
		var backupDir string

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

			// note that BeforeSuite has saved off history file, in case of running on workstation where we want to retain normal (non-test?) history
			// we remove in order to work around an old common-library bug in closing a file after writing, and truncating when opening to write, both of which manifest as a broken history file in old code
			_ = os.Remove(historyFilePath)

			// Assign a unique directory for each test
			backupDir, _ = ioutil.TempDir(customBackupDir, "temp")
		})
		AfterEach(func() {
			_ = os.RemoveAll(backupDir)
		})
		Describe("Backup include filtering", func() {
			It("runs gpbackup and gprestore with include-schema backup flag and compression level", func() {
				timestamp := gpbackup(gpbackupPath, backupHelperPath, "--include-schema", "public", "--compression-level", "2")
				gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb")

				assertRelationsCreated(restoreConn, 20)
				assertDataRestored(restoreConn, publicSchemaTupleCounts)
				assertArtifactsCleaned(restoreConn, timestamp)
			})
			It("runs gpbackup and gprestore with include-table backup flag", func() {
				skipIfOldBackupVersionBefore("1.4.0")
				timestamp := gpbackup(gpbackupPath, backupHelperPath, "--include-table", "public.foo", "--include-table", "public.sales", "--include-table", "public.myseq1", "--include-table", "public.myview1")
				gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb")

				assertRelationsCreated(restoreConn, 16)
				assertDataRestored(restoreConn, map[string]int{"public.foo": 40000})
			})
			It("runs gpbackup and gprestore with include-table-file backup flag", func() {
				skipIfOldBackupVersionBefore("1.4.0")
				includeFile := iohelper.MustOpenFileForWriting("/tmp/include-tables.txt")
				utils.MustPrintln(includeFile, "public.sales\npublic.foo\npublic.myseq1\npublic.myview1")
				timestamp := gpbackup(gpbackupPath, backupHelperPath, "--include-table-file", "/tmp/include-tables.txt")
				gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb")

				assertRelationsCreated(restoreConn, 16)
				assertDataRestored(restoreConn, map[string]int{"public.sales": 13, "public.foo": 40000})

				_ = os.Remove("/tmp/include-tables.txt")
			})
			It("runs gpbackup and gprestore with include-schema-file backup flag", func() {
				skipIfOldBackupVersionBefore("1.17.0")
				includeFile := iohelper.MustOpenFileForWriting("/tmp/include-schema.txt")
				utils.MustPrintln(includeFile, "public")
				timestamp := gpbackup(gpbackupPath, backupHelperPath, "--include-schema-file", "/tmp/include-schema.txt")
				gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb")

				assertRelationsCreated(restoreConn, 20)

				_ = os.Remove("/tmp/include-schema.txt")
			})

		})
		Describe("Restore include filtering", func() {
			It("runs gpbackup and gprestore with include-schema restore flag", func() {
				timestamp := gpbackup(gpbackupPath, backupHelperPath, "--backup-dir", backupDir)
				gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb", "--backup-dir", backupDir, "--include-schema", "schema2")

				assertRelationsCreated(restoreConn, 17)
				assertDataRestored(restoreConn, schema2TupleCounts)

			})
			It("runs gpbackup and gprestore with include-schema-file restore flag", func() {
				includeFile := iohelper.MustOpenFileForWriting("/tmp/include-schema.txt")
				utils.MustPrintln(includeFile, "schema2")
				utils.MustPrintln(includeFile, "public")
				timestamp := gpbackup(gpbackupPath, backupHelperPath, "--backup-dir", backupDir)
				gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb", "--backup-dir", backupDir, "--include-schema-file", "/tmp/include-schema.txt")

				assertRelationsCreated(restoreConn, 37)
				assertDataRestored(restoreConn, schema2TupleCounts)

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
				timestamp := gpbackup(gpbackupPath, backupHelperPath, "--backup-dir", backupDir)
				gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb", "--backup-dir", backupDir, "--include-table-file", "/tmp/include-tables.txt")

				assertRelationsCreated(restoreConn, 16)
				assertDataRestored(restoreConn, map[string]int{"public.sales": 13, "public.foo": 40000})

				_ = os.Remove("/tmp/include-tables.txt")
			})
			It("runs gpbackup and gprestore with include-table restore flag against a leaf partition", func() {
				skipIfOldBackupVersionBefore("1.7.2")
				timestamp := gpbackup(gpbackupPath, backupHelperPath, "--leaf-partition-data")
				gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb", "--include-table", "public.sales_1_prt_jan17")

				assertRelationsCreated(restoreConn, 13)
				assertDataRestored(restoreConn, map[string]int{"public.sales": 1, "public.sales_1_prt_jan17": 1})
			})
			It("runs gpbackup and gprestore with include-table restore flag which implicitly filters schema restore list", func() {
				timestamp := gpbackup(gpbackupPath, backupHelperPath, "--backup-dir", backupDir)
				gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb", "--backup-dir", backupDir, "--include-table", "schema2.foo3")
				assertRelationsCreated(restoreConn, 1)
				assertDataRestored(restoreConn, map[string]int{"schema2.foo3": 100})
			})
		})
		Describe("Backup exclude filtering", func() {
			It("runs gpbackup and gprestore with exclude-schema backup flag", func() {
				timestamp := gpbackup(gpbackupPath, backupHelperPath, "--exclude-schema", "public")
				gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb")

				assertRelationsCreated(restoreConn, 17)
				assertDataRestored(restoreConn, schema2TupleCounts)
			})
			It("runs gpbackup and gprestore with exclude-schema-file backup flag", func() {
				skipIfOldBackupVersionBefore("1.17.0")
				excludeFile := iohelper.MustOpenFileForWriting("/tmp/exclude-schema.txt")
				utils.MustPrintln(excludeFile, "public")
				timestamp := gpbackup(gpbackupPath, backupHelperPath, "--exclude-schema-file", "/tmp/exclude-schema.txt")
				gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb")

				assertRelationsCreated(restoreConn, 17)
				assertDataRestored(restoreConn, schema2TupleCounts)

				_ = os.Remove("/tmp/exclude-schema.txt")
			})
			It("runs gpbackup and gprestore with exclude-table backup flag", func() {
				skipIfOldBackupVersionBefore("1.4.0")
				timestamp := gpbackup(gpbackupPath, backupHelperPath, "--exclude-table", "schema2.foo2", "--exclude-table", "schema2.returns", "--exclude-table", "public.myseq2", "--exclude-table", "public.myview2")
				gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb")

				assertRelationsCreated(restoreConn, TOTAL_RELATIONS_AFTER_EXCLUDE)
				assertDataRestored(restoreConn, map[string]int{"schema2.foo3": 100, "public.foo": 40000, "public.holds": 50000, "public.sales": 13})
			})
			It("runs gpbackup and gprestore with exclude-table-file backup flag", func() {
				skipIfOldBackupVersionBefore("1.4.0")
				excludeFile := iohelper.MustOpenFileForWriting("/tmp/exclude-tables.txt")
				utils.MustPrintln(excludeFile, "schema2.foo2\nschema2.returns\npublic.sales\npublic.myseq2\npublic.myview2")
				timestamp := gpbackup(gpbackupPath, backupHelperPath, "--exclude-table-file", "/tmp/exclude-tables.txt")
				gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb")

				assertRelationsCreated(restoreConn, 8)
				assertDataRestored(restoreConn, map[string]int{"schema2.foo3": 100, "public.foo": 40000, "public.holds": 50000})

				_ = os.Remove("/tmp/exclude-tables.txt")
			})
		})
		Describe("Restore exclude filtering", func() {
			It("runs gpbackup and gprestore with exclude-schema restore flag", func() {
				timestamp := gpbackup(gpbackupPath, backupHelperPath)
				gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb", "--exclude-schema", "public")

				assertRelationsCreated(restoreConn, 17)
				assertDataRestored(restoreConn, schema2TupleCounts)
			})
			It("runs gpbackup and gprestore with exclude-schema-file restore flag", func() {
				includeFile := iohelper.MustOpenFileForWriting("/tmp/exclude-schema.txt")
				utils.MustPrintln(includeFile, "public")
				timestamp := gpbackup(gpbackupPath, backupHelperPath, "--backup-dir", backupDir)
				gprestore(gprestorePath, restoreHelperPath, timestamp, "--backup-dir", backupDir, "--redirect-db", "restoredb", "--exclude-schema-file", "/tmp/exclude-schema.txt")

				assertRelationsCreated(restoreConn, 17)
				assertDataRestored(restoreConn, schema2TupleCounts)

				_ = os.Remove("/tmp/exclude-schema.txt")
			})
			It("runs gpbackup and gprestore with exclude-table restore flag", func() {
				timestamp := gpbackup(gpbackupPath, backupHelperPath)
				gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb", "--exclude-table", "schema2.foo2", "--exclude-table", "schema2.returns", "--exclude-table", "public.myseq2", "--exclude-table", "public.myview2")

				assertRelationsCreated(restoreConn, TOTAL_RELATIONS_AFTER_EXCLUDE)
				assertDataRestored(restoreConn, map[string]int{"schema2.foo3": 100, "public.foo": 40000, "public.holds": 50000, "public.sales": 13})
			})
			It("runs gpbackup and gprestore with exclude-table-file restore flag", func() {
				includeFile := iohelper.MustOpenFileForWriting("/tmp/exclude-tables.txt")
				utils.MustPrintln(includeFile, "schema2.foo2\nschema2.returns\npublic.myseq2\npublic.myview2")
				timestamp := gpbackup(gpbackupPath, backupHelperPath, "--backup-dir", backupDir)
				gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb", "--backup-dir", backupDir, "--exclude-table-file", "/tmp/exclude-tables.txt")

				assertRelationsCreated(restoreConn, TOTAL_RELATIONS_AFTER_EXCLUDE)
				assertDataRestored(restoreConn, map[string]int{"public.sales": 13, "public.foo": 40000})

				_ = os.Remove("/tmp/exclude-tables.txt")
			})
		})
		Describe("Single data file", func() {
			It("runs gpbackup and gprestore with single-data-file flag", func() {
				timestamp := gpbackup(gpbackupPath, backupHelperPath, "--single-data-file", "--backup-dir", backupDir)
				gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb", "--backup-dir", backupDir)

				assertRelationsCreated(restoreConn, TOTAL_RELATIONS)
				assertDataRestored(restoreConn, publicSchemaTupleCounts)
				assertDataRestored(restoreConn, schema2TupleCounts)
				assertArtifactsCleaned(restoreConn, timestamp)

			})
			It("runs gpbackup and gprestore with single-data-file flag without compression", func() {
				timestamp := gpbackup(gpbackupPath, backupHelperPath, "--single-data-file", "--backup-dir", backupDir, "--no-compression")
				gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb", "--backup-dir", backupDir)

				assertRelationsCreated(restoreConn, TOTAL_RELATIONS)
				assertDataRestored(restoreConn, publicSchemaTupleCounts)
				assertDataRestored(restoreConn, schema2TupleCounts)
				assertArtifactsCleaned(restoreConn, timestamp)
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
				if backupConn.Version.AtLeast("6.2") {
					testhelper.AssertQueryRuns(backupConn, "CREATE TABLE mview_table1(i int, j text);")
					defer testhelper.AssertQueryRuns(restoreConn, "DROP TABLE mview_table1;")
					testhelper.AssertQueryRuns(backupConn, "CREATE MATERIALIZED VIEW mview1 (i2) as select i from mview_table1;")
					defer testhelper.AssertQueryRuns(restoreConn, "DROP MATERIALIZED VIEW mview1;")
					testhelper.AssertQueryRuns(backupConn, "CREATE MATERIALIZED VIEW mview2 as select * from mview1;")
					defer testhelper.AssertQueryRuns(restoreConn, "DROP MATERIALIZED VIEW mview2;")
				}

				timestamp := gpbackup(gpbackupPath, backupHelperPath, "--leaf-partition-data", "--single-data-file")
				gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb")
				assertArtifactsCleaned(restoreConn, timestamp)
			})

			Context("with include filtering on restore", func() {
				It("runs gpbackup and gprestore with include-table-file restore flag with a single data file", func() {
					includeFile := iohelper.MustOpenFileForWriting("/tmp/include-tables.txt")
					utils.MustPrintln(includeFile, "public.sales\npublic.foo\npublic.myseq1\npublic.myview1")
					timestamp := gpbackup(gpbackupPath, backupHelperPath, "--backup-dir", backupDir, "--single-data-file")
					gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb", "--backup-dir", backupDir, "--include-table-file", "/tmp/include-tables.txt")
					assertRelationsCreated(restoreConn, 16)
					assertDataRestored(restoreConn, map[string]int{"public.sales": 13, "public.foo": 40000})
					assertArtifactsCleaned(restoreConn, timestamp)

					_ = os.Remove("/tmp/include-tables.txt")
				})
				It("runs gpbackup and gprestore with include-schema restore flag with a single data file", func() {
					timestamp := gpbackup(gpbackupPath, backupHelperPath, "--backup-dir", backupDir, "--single-data-file")
					gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb", "--backup-dir", backupDir, "--include-schema", "schema2")

					assertRelationsCreated(restoreConn, 17)
					assertDataRestored(restoreConn, schema2TupleCounts)
					assertArtifactsCleaned(restoreConn, timestamp)
				})
			})

			Context("with plugin", func() {
				BeforeEach(func() {
					skipIfOldBackupVersionBefore("1.7.0")
					// FIXME: we are temporarily disabling these tests because we will be altering our backwards compatibility logic.
					if useOldBackupVersion {
						Skip("This test is only needed for the most recent backup versions")
					}

				})
				It("runs gpbackup and gprestore with plugin, single-data-file, and no-compression", func() {
					pluginExecutablePath := fmt.Sprintf("%s/go/src/github.com/greenplum-db/gpbackup/plugins/example_plugin.bash", os.Getenv("HOME"))
					copyPluginToAllHosts(backupConn, pluginExecutablePath)

					timestamp := gpbackup(gpbackupPath, backupHelperPath, "--single-data-file", "--no-compression", "--plugin-config", pluginConfigPath)
					forceMetadataFileDownloadFromPlugin(backupConn, timestamp)

					gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb", "--plugin-config", pluginConfigPath)

					assertRelationsCreated(restoreConn, TOTAL_RELATIONS)
					assertDataRestored(restoreConn, publicSchemaTupleCounts)
					assertDataRestored(restoreConn, schema2TupleCounts)
					assertArtifactsCleaned(restoreConn, timestamp)
				})
				It("runs gpbackup and gprestore with plugin and single-data-file", func() {
					pluginExecutablePath := fmt.Sprintf("%s/go/src/github.com/greenplum-db/gpbackup/plugins/example_plugin.bash", os.Getenv("HOME"))
					copyPluginToAllHosts(backupConn, pluginExecutablePath)

					timestamp := gpbackup(gpbackupPath, backupHelperPath, "--single-data-file", "--plugin-config", pluginConfigPath)
					forceMetadataFileDownloadFromPlugin(backupConn, timestamp)

					gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb", "--plugin-config", pluginConfigPath)

					assertRelationsCreated(restoreConn, TOTAL_RELATIONS)
					assertDataRestored(restoreConn, publicSchemaTupleCounts)
					assertDataRestored(restoreConn, schema2TupleCounts)
					assertArtifactsCleaned(restoreConn, timestamp)
				})
				It("runs gpbackup and gprestore with plugin and metadata-only", func() {
					pluginExecutablePath := fmt.Sprintf("%s/go/src/github.com/greenplum-db/gpbackup/plugins/example_plugin.bash", os.Getenv("HOME"))
					copyPluginToAllHosts(backupConn, pluginExecutablePath)

					timestamp := gpbackup(gpbackupPath, backupHelperPath, "--metadata-only", "--plugin-config", pluginConfigPath)
					forceMetadataFileDownloadFromPlugin(backupConn, timestamp)

					gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb", "--plugin-config", pluginConfigPath)

					assertRelationsCreated(restoreConn, TOTAL_RELATIONS)
					assertArtifactsCleaned(restoreConn, timestamp)
				})
			})
		})
		Describe("Multi-file Plugin", func() {
			It("runs gpbackup and gprestore with plugin and no-compression", func() {
				skipIfOldBackupVersionBefore("1.7.0")
				// FIXME: we are temporarily disabling these tests because we will be altering our backwards compatibility logic.
				if useOldBackupVersion {
					Skip("This test is only needed for the most recent backup versions")
				}
				pluginExecutablePath := fmt.Sprintf("%s/go/src/github.com/greenplum-db/gpbackup/plugins/example_plugin.bash", os.Getenv("HOME"))
				copyPluginToAllHosts(backupConn, pluginExecutablePath)

				timestamp := gpbackup(gpbackupPath, backupHelperPath, "--no-compression", "--plugin-config", pluginConfigPath)
				forceMetadataFileDownloadFromPlugin(backupConn, timestamp)

				gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb", "--plugin-config", pluginConfigPath)

				assertRelationsCreated(restoreConn, TOTAL_RELATIONS)
				assertDataRestored(restoreConn, publicSchemaTupleCounts)
				assertDataRestored(restoreConn, schema2TupleCounts)
			})
			It("runs gpbackup and gprestore with plugin and compression", func() {
				skipIfOldBackupVersionBefore("1.7.0")
				// FIXME: we are temporarily disabling these tests because we will be altering our backwards compatibility logic.
				if useOldBackupVersion {
					Skip("This test is only needed for the most recent backup versions")
				}
				pluginExecutablePath := fmt.Sprintf("%s/go/src/github.com/greenplum-db/gpbackup/plugins/example_plugin.bash", os.Getenv("HOME"))
				copyPluginToAllHosts(backupConn, pluginExecutablePath)

				timestamp := gpbackup(gpbackupPath, backupHelperPath, "--plugin-config", pluginConfigPath)
				forceMetadataFileDownloadFromPlugin(backupConn, timestamp)

				gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb", "--plugin-config", pluginConfigPath)

				assertRelationsCreated(restoreConn, TOTAL_RELATIONS)
				assertDataRestored(restoreConn, publicSchemaTupleCounts)
				assertDataRestored(restoreConn, schema2TupleCounts)
			})
		})
		Describe("Incremental backup", func() {
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

				assertRelationsCreated(restoreConn, TOTAL_RELATIONS)
				assertDataRestored(restoreConn, publicSchemaTupleCounts)
				schema2TupleCounts["schema2.ao1"] = 1002
				assertDataRestored(restoreConn, schema2TupleCounts)
			})
			It("restores from an incremental backup with AO Table consisting of multiple segment files", func() {
				// Versions before 1.13.0 incorrectly handle AO table inserts involving multiple seg files
				skipIfOldBackupVersionBefore("1.13.0")

				testhelper.AssertQueryRuns(backupConn, "CREATE TABLE foobar WITH (appendonly=true) AS SELECT i FROM generate_series(1,5) i")
				defer testhelper.AssertQueryRuns(backupConn, "DROP TABLE foobar")
				testhelper.AssertQueryRuns(backupConn, "VACUUM foobar")
				entriesInTable := dbconn.MustSelectString(backupConn, "SELECT count(*) FROM foobar")
				Expect(entriesInTable).To(Equal(strconv.Itoa(5)))

				fullBackupTimestamp := gpbackup(gpbackupPath, backupHelperPath, "--leaf-partition-data")

				testhelper.AssertQueryRuns(backupConn, "INSERT INTO foobar VALUES (1)")

				// Ensure two distinct seg files contain 'foobar' data
				var numRows string
				if backupConn.Version.Before("6") {
					numRows = dbconn.MustSelectString(backupConn, "SELECT count(*) FROM gp_toolkit.__gp_aoseg_name('foobar')")
				} else {
					numRows = dbconn.MustSelectString(backupConn, "SELECT count(*) FROM gp_toolkit.__gp_aoseg('foobar'::regclass)")
				}
				Expect(numRows).To(Equal(strconv.Itoa(2)))

				entriesInTable = dbconn.MustSelectString(backupConn, "SELECT count(*) FROM foobar")
				Expect(entriesInTable).To(Equal(strconv.Itoa(6)))

				incremental1Timestamp := gpbackup(gpbackupPath, backupHelperPath,
					"--incremental", "--leaf-partition-data", "--from-timestamp", fullBackupTimestamp)

				gprestore(gprestorePath, restoreHelperPath, incremental1Timestamp, "--redirect-db", "restoredb")

				// The insertion should have been recorded in the incremental backup
				entriesInTable = dbconn.MustSelectString(restoreConn, "SELECT count(*) FROM foobar")
				Expect(entriesInTable).To(Equal(strconv.Itoa(6)))
			})
			It("can restore from an old backup with an incremental taken from new binaries with --include-table", func() {
				if !useOldBackupVersion {
					Skip("This test is only needed for old backup versions")
				}
				_ = gpbackup(gpbackupPath, backupHelperPath, "--leaf-partition-data", "--include-table=public.sales")
				testhelper.AssertQueryRuns(backupConn, "INSERT into sales values(1, '2017-01-01', 99.99)")
				defer testhelper.AssertQueryRuns(backupConn, "DELETE from sales where amt=99.99")
				_ = gpbackup(gpbackupPath, backupHelperPath,
					"--incremental", "--leaf-partition-data", "--include-table=public.sales")

				gpbackupPath, backupHelperPath, _ = buildAndInstallBinaries()

				testhelper.AssertQueryRuns(backupConn, "INSERT into sales values(2, '2017-02-01', 88.88)")
				defer testhelper.AssertQueryRuns(backupConn, "DELETE from sales where amt=88.88")
				incremental2Timestamp := gpbackup(gpbackupPath, backupHelperPath,
					"--incremental", "--leaf-partition-data", "--include-table=public.sales")

				gprestore(gprestorePath, restoreHelperPath, incremental2Timestamp, "--redirect-db", "restoredb")

				localTupleCounts := map[string]int{
					"public.sales": 15,
				}
				assertRelationsCreated(restoreConn, 13)
				assertDataRestored(restoreConn, localTupleCounts)
			})
			Context("Without a timestamp", func() {
				It("restores from a incremental backup specified with a backup directory", func() {
					_ = gpbackup(gpbackupPath, backupHelperPath, "--leaf-partition-data", "--backup-dir", backupDir)

					testhelper.AssertQueryRuns(backupConn, "INSERT into schema2.ao1 values(1001)")
					defer testhelper.AssertQueryRuns(backupConn, "DELETE from schema2.ao1 where i=1001")
					_ = gpbackup(gpbackupPath, backupHelperPath,
						"--incremental", "--leaf-partition-data", "--backup-dir", backupDir)

					testhelper.AssertQueryRuns(backupConn, "INSERT into schema2.ao1 values(1002)")
					defer testhelper.AssertQueryRuns(backupConn, "DELETE from schema2.ao1 where i=1002")
					incremental2Timestamp := gpbackup(gpbackupPath, backupHelperPath,
						"--incremental", "--leaf-partition-data", "--backup-dir", backupDir)

					gprestore(gprestorePath, restoreHelperPath, incremental2Timestamp, "--redirect-db", "restoredb", "--backup-dir", backupDir)

					assertRelationsCreated(restoreConn, TOTAL_RELATIONS)
					assertDataRestored(restoreConn, publicSchemaTupleCounts)
					schema2TupleCounts["schema2.ao1"] = 1002
					assertDataRestored(restoreConn, schema2TupleCounts)

					_ = os.Remove(backupDir)
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
				Context("old binaries", func() {
					It("can restore from a backup with an incremental taken from new binaries", func() {
						if !useOldBackupVersion {
							Skip("This test is only needed for old backup versions")
						}
						_ = gpbackup(gpbackupPath, backupHelperPath, "--leaf-partition-data")

						testhelper.AssertQueryRuns(backupConn, "INSERT into schema2.ao1 values(1001)")
						defer testhelper.AssertQueryRuns(backupConn, "DELETE from schema2.ao1 where i=1001")
						_ = gpbackup(gpbackupPath, backupHelperPath,
							"--incremental", "--leaf-partition-data")

						gpbackupPathOld, backupHelperPathOld := gpbackupPath, backupHelperPath
						gpbackupPath, backupHelperPath, _ = buildAndInstallBinaries()

						testhelper.AssertQueryRuns(backupConn, "INSERT into schema2.ao1 values(1002)")
						defer testhelper.AssertQueryRuns(backupConn, "DELETE from schema2.ao1 where i=1002")
						incremental2Timestamp := gpbackup(gpbackupPath, backupHelperPath,
							"--incremental", "--leaf-partition-data")
						gpbackupPath, backupHelperPath = gpbackupPathOld, backupHelperPathOld

						gprestore(gprestorePath, restoreHelperPath, incremental2Timestamp, "--redirect-db", "restoredb")

						assertRelationsCreated(restoreConn, TOTAL_RELATIONS)
						assertDataRestored(restoreConn, publicSchemaTupleCounts)
						schema2TupleCounts["schema2.ao1"] = 1002
						assertDataRestored(restoreConn, schema2TupleCounts)
					})
				})
			})
			Context("With a plugin", func() {
				BeforeEach(func() {
					// FIXME: we are temporarily disabling these tests because we will be altering our backwards compatibility logic.
					if useOldBackupVersion {
						Skip("This test is only needed for the most recent backup versions")
					}
					pluginExecutablePath := fmt.Sprintf("%s/go/src/github.com/greenplum-db/gpbackup/plugins/example_plugin.bash", os.Getenv("HOME"))
					copyPluginToAllHosts(backupConn, pluginExecutablePath)
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

					assertRelationsCreated(restoreConn, TOTAL_RELATIONS)
					assertDataRestored(restoreConn, publicSchemaTupleCounts)
					schema2TupleCounts["schema2.ao1"] = 1002
					assertDataRestored(restoreConn, schema2TupleCounts)
					assertArtifactsCleaned(restoreConn, fullBackupTimestamp)
					assertArtifactsCleaned(restoreConn, incremental1Timestamp)
					assertArtifactsCleaned(restoreConn, incremental2Timestamp)
				})
			})
		})
		Describe("Incremental restore", func() {
			var oldSchemaTupleCounts, newSchemaTupleCounts map[string]int
			BeforeEach(func() {
				skipIfOldBackupVersionBefore("1.16.0")
			})
			Context("No DDL no partitioning", func() {
				BeforeEach(func() {
					testhelper.AssertQueryRuns(backupConn, "DROP SCHEMA IF EXISTS new_schema CASCADE; DROP SCHEMA IF EXISTS old_schema CASCADE; CREATE SCHEMA old_schema;")
					testhelper.AssertQueryRuns(backupConn, "CREATE TABLE old_schema.old_table0 (mydata int) WITH (appendonly=true) DISTRIBUTED BY (mydata);")
					testhelper.AssertQueryRuns(backupConn, "CREATE TABLE old_schema.old_table1 (mydata int) WITH (appendonly=true) DISTRIBUTED BY (mydata);")
					testhelper.AssertQueryRuns(backupConn, "CREATE TABLE old_schema.old_table2 (mydata int) WITH (appendonly=true) DISTRIBUTED BY (mydata);")
					testhelper.AssertQueryRuns(backupConn, "INSERT INTO old_schema.old_table0 SELECT generate_series(1, 5);")
					testhelper.AssertQueryRuns(backupConn, "INSERT INTO old_schema.old_table1 SELECT generate_series(1, 10);")
					testhelper.AssertQueryRuns(backupConn, "INSERT INTO old_schema.old_table2 SELECT generate_series(1, 15);")

					oldSchemaTupleCounts = map[string]int{
						"old_schema.old_table0": 5,
						"old_schema.old_table1": 10,
						"old_schema.old_table2": 15,
					}
					newSchemaTupleCounts = map[string]int{}
					baseTimestamp := gpbackup(gpbackupPath, backupHelperPath, "--leaf-partition-data")

					testhelper.AssertQueryRuns(restoreConn, "DROP SCHEMA IF EXISTS new_schema CASCADE; DROP SCHEMA IF EXISTS old_schema CASCADE;")
					gprestore(gprestorePath, restoreHelperPath, baseTimestamp, "--redirect-db", "restoredb")
				})
				AfterEach(func() {
					testhelper.AssertQueryRuns(backupConn, "DROP SCHEMA IF EXISTS new_schema CASCADE; DROP SCHEMA IF EXISTS old_schema CASCADE;")
					testhelper.AssertQueryRuns(restoreConn, "DROP SCHEMA IF EXISTS new_schema CASCADE; DROP SCHEMA IF EXISTS old_schema CASCADE;")
				})
				Context("Only new schemas and tables", func() {
					var timestamp string
					BeforeEach(func() {
						testhelper.AssertQueryRuns(backupConn, "CREATE SCHEMA new_schema;")
						testhelper.AssertQueryRuns(backupConn, "CREATE TABLE new_schema.new_table1 (mydata int) WITH (appendonly=true) DISTRIBUTED BY (mydata);")
						testhelper.AssertQueryRuns(backupConn, "CREATE TABLE new_schema.new_table2 (mydata int) WITH (appendonly=true) DISTRIBUTED BY (mydata);")
						testhelper.AssertQueryRuns(backupConn, "INSERT INTO new_schema.new_table1 SELECT generate_series(1, 30);")
						testhelper.AssertQueryRuns(backupConn, "INSERT INTO new_schema.new_table2 SELECT generate_series(1, 35);")
						timestamp = gpbackup(gpbackupPath, backupHelperPath, "--leaf-partition-data", "--incremental")
					})
					AfterEach(func() {
						testhelper.AssertQueryRuns(backupConn, "DROP SCHEMA IF EXISTS new_schema CASCADE;")
						testhelper.AssertQueryRuns(restoreConn, "DROP SCHEMA IF EXISTS new_schema CASCADE;")
						newSchemaTupleCounts = map[string]int{}
						assertArtifactsCleaned(restoreConn, timestamp)
					})
					It("Restores new schemas and tables", func() {
						gprestore(gprestorePath, restoreHelperPath, timestamp, "--incremental", "--redirect-db", "restoredb")
						newSchemaTupleCounts["new_schema.new_table1"] = 30
						newSchemaTupleCounts["new_schema.new_table2"] = 35
						assertSchemasExist(restoreConn, 5)
						assertRelationsExistForIncremental(restoreConn, 5)
						assertDataRestored(restoreConn, oldSchemaTupleCounts)
						assertDataRestored(restoreConn, newSchemaTupleCounts)
					})
					It("Restores only tables included by use if user input is provided", func() {
						gprestore(gprestorePath, restoreHelperPath, timestamp, "--incremental", "--include-table", "new_schema.new_table1", "--redirect-db", "restoredb")
						newSchemaTupleCounts["new_schema.new_table1"] = 30
						assertSchemasExist(restoreConn, 5)
						assertRelationsExistForIncremental(restoreConn, 4)
						assertDataRestored(restoreConn, oldSchemaTupleCounts)
						assertDataRestored(restoreConn, newSchemaTupleCounts)
					})
					It("Does not restore tables excluded by user if user input is provided", func() {
						gprestore(gprestorePath, restoreHelperPath, timestamp, "--incremental", "--exclude-table", "new_schema.new_table1", "--redirect-db", "restoredb")
						newSchemaTupleCounts["new_schema.new_table2"] = 35
						assertSchemasExist(restoreConn, 5)
						assertRelationsExistForIncremental(restoreConn, 4)
						assertDataRestored(restoreConn, oldSchemaTupleCounts)
						assertDataRestored(restoreConn, newSchemaTupleCounts)
					})
					It("Restores only schemas included by user if user input is provided", func() {
						gprestore(gprestorePath, restoreHelperPath, timestamp, "--incremental", "--include-schema", "old_schema", "--redirect-db", "restoredb")
						assertSchemasExist(restoreConn, 4)
						assertRelationsExistForIncremental(restoreConn, 3)
						assertDataRestored(restoreConn, oldSchemaTupleCounts)
						assertDataRestored(restoreConn, newSchemaTupleCounts)
					})
					It("Does not restore schemas excluded by user if user input is provided", func() {
						gprestore(gprestorePath, restoreHelperPath, timestamp, "--incremental", "--exclude-schema", "new_schema", "--redirect-db", "restoredb")
						assertSchemasExist(restoreConn, 4)
						assertRelationsExistForIncremental(restoreConn, 3)
						assertDataRestored(restoreConn, oldSchemaTupleCounts)
						assertDataRestored(restoreConn, newSchemaTupleCounts)
					})
				})
				Context("New tables in existing schemas", func() {
					var timestamp string
					BeforeEach(func() {
						testhelper.AssertQueryRuns(backupConn, "CREATE TABLE old_schema.new_table1 (mydata int) WITH (appendonly=true) DISTRIBUTED BY (mydata);")
						testhelper.AssertQueryRuns(backupConn, "CREATE TABLE old_schema.new_table2 (mydata int) WITH (appendonly=true) DISTRIBUTED BY (mydata);")
						testhelper.AssertQueryRuns(backupConn, "INSERT INTO old_schema.new_table1 SELECT generate_series(1, 20);")
						testhelper.AssertQueryRuns(backupConn, "INSERT INTO old_schema.new_table2 SELECT generate_series(1, 25);")
						timestamp = gpbackup(gpbackupPath, backupHelperPath, "--leaf-partition-data", "--incremental")
					})
					AfterEach(func() {
						testhelper.AssertQueryRuns(backupConn, "DROP TABLE IF EXISTS old_schema.new_table1 CASCADE;")
						testhelper.AssertQueryRuns(backupConn, "DROP TABLE IF EXISTS old_schema.new_table2 CASCADE;")
						testhelper.AssertQueryRuns(restoreConn, "DROP TABLE IF EXISTS old_schema.new_table1 CASCADE;")
						testhelper.AssertQueryRuns(restoreConn, "DROP TABLE IF EXISTS old_schema.new_table2 CASCADE;")
						oldSchemaTupleCounts = map[string]int{}
						newSchemaTupleCounts = map[string]int{}
						assertArtifactsCleaned(restoreConn, timestamp)
					})
					It("Restores new tables", func() {
						gprestore(gprestorePath, restoreHelperPath, timestamp, "--incremental", "--redirect-db", "restoredb")
						oldSchemaTupleCounts["old_schema.new_table1"] = 20
						oldSchemaTupleCounts["old_schema.new_table2"] = 25
						assertSchemasExist(restoreConn, 4)
						assertRelationsExistForIncremental(restoreConn, 5)
						assertDataRestored(restoreConn, oldSchemaTupleCounts)
						assertDataRestored(restoreConn, newSchemaTupleCounts)
					})
					It("Restores only tables included by use if user input is provided", func() {
						gprestore(gprestorePath, restoreHelperPath, timestamp, "--incremental", "--include-table", "old_schema.new_table1", "--redirect-db", "restoredb")
						oldSchemaTupleCounts["old_schema.new_table1"] = 20
						assertSchemasExist(restoreConn, 4)
						assertRelationsExistForIncremental(restoreConn, 4)
						assertDataRestored(restoreConn, oldSchemaTupleCounts)
						assertDataRestored(restoreConn, newSchemaTupleCounts)
					})
					It("Does not restore tables excluded by user if user input is provided", func() {
						gprestore(gprestorePath, restoreHelperPath, timestamp, "--incremental", "--exclude-table", "old_schema.new_table1", "--redirect-db", "restoredb")
						oldSchemaTupleCounts["old_schema.new_table2"] = 25
						assertSchemasExist(restoreConn, 4)
						assertRelationsExistForIncremental(restoreConn, 4)
						assertDataRestored(restoreConn, oldSchemaTupleCounts)
						assertDataRestored(restoreConn, newSchemaTupleCounts)
					})
					It("Does not restore anything if user excluded all new tables", func() {
						gprestore(gprestorePath, restoreHelperPath, timestamp, "--incremental", "--exclude-table", "old_schema.new_table1", "--exclude-table", "old_schema.new_table2", "--redirect-db", "restoredb")
						assertSchemasExist(restoreConn, 4)
						assertRelationsExistForIncremental(restoreConn, 3)
						assertDataRestored(restoreConn, oldSchemaTupleCounts)
						assertDataRestored(restoreConn, newSchemaTupleCounts)
					})
					It("Does not restore tables if user input is provided and schema is not included", func() {
						gprestore(gprestorePath, restoreHelperPath, timestamp, "--incremental", "--include-schema", "public", "--redirect-db", "restoredb")
						assertSchemasExist(restoreConn, 4)
						assertRelationsExistForIncremental(restoreConn, 3)
						assertDataRestored(restoreConn, oldSchemaTupleCounts)
						assertDataRestored(restoreConn, newSchemaTupleCounts)
					})
					It("Does not restore tables if user input is provide and schema is excluded", func() {
						gprestore(gprestorePath, restoreHelperPath, timestamp, "--incremental", "--exclude-schema", "old_schema", "--redirect-db", "restoredb")
						assertSchemasExist(restoreConn, 4)
						assertRelationsExistForIncremental(restoreConn, 3)
						assertDataRestored(restoreConn, oldSchemaTupleCounts)
						assertDataRestored(restoreConn, newSchemaTupleCounts)
					})
				})
				Context("Existing tables in existing schemas were updated", func() {
					var timestamp string
					BeforeEach(func() {
						testhelper.AssertQueryRuns(backupConn, "INSERT INTO old_schema.old_table1 SELECT generate_series(11, 20);")
						testhelper.AssertQueryRuns(backupConn, "INSERT INTO old_schema.old_table2 SELECT generate_series(16, 30);")
						timestamp = gpbackup(gpbackupPath, backupHelperPath, "--leaf-partition-data", "--incremental")
					})
					AfterEach(func() {
						testhelper.AssertQueryRuns(backupConn, "DELETE FROM old_schema.old_table1 where mydata>10;")
						testhelper.AssertQueryRuns(backupConn, "DELETE FROM old_schema.old_table2 where mydata>15;")
						oldSchemaTupleCounts["old_schema.old_table1"] = 10
						oldSchemaTupleCounts["old_schema.old_table2"] = 15
						testhelper.AssertQueryRuns(restoreConn, "DELETE FROM old_schema.old_table1 where mydata>10;")
						testhelper.AssertQueryRuns(restoreConn, "DELETE FROM old_schema.old_table2 where mydata>15;")
						assertArtifactsCleaned(restoreConn, timestamp)
					})
					It("Updates data in existing tables", func() {
						gprestore(gprestorePath, restoreHelperPath, timestamp, "--incremental", "--redirect-db", "restoredb")
						oldSchemaTupleCounts["old_schema.old_table1"] = 20
						oldSchemaTupleCounts["old_schema.old_table2"] = 30
						assertSchemasExist(restoreConn, 4)
						assertRelationsExistForIncremental(restoreConn, 3)
						assertDataRestored(restoreConn, oldSchemaTupleCounts)
						assertDataRestored(restoreConn, newSchemaTupleCounts)
					})
					It("Updates only tables included by user if user input is provided", func() {
						gprestore(gprestorePath, restoreHelperPath, timestamp, "--incremental", "--include-table", "old_schema.old_table1", "--redirect-db", "restoredb")
						oldSchemaTupleCounts["old_schema.old_table1"] = 20
						assertSchemasExist(restoreConn, 4)
						assertRelationsExistForIncremental(restoreConn, 3)
						assertDataRestored(restoreConn, oldSchemaTupleCounts)
						assertDataRestored(restoreConn, newSchemaTupleCounts)
					})
					It("Does not update tables excluded by user if user input is provided", func() {
						gprestore(gprestorePath, restoreHelperPath, timestamp, "--incremental", "--exclude-table", "old_schema.old_table1", "--redirect-db", "restoredb")
						oldSchemaTupleCounts["old_schema.old_table2"] = 30
						assertSchemasExist(restoreConn, 4)
						assertRelationsExistForIncremental(restoreConn, 3)
						assertDataRestored(restoreConn, oldSchemaTupleCounts)
						assertDataRestored(restoreConn, newSchemaTupleCounts)
					})
					It("Does not update anything if user excluded all tables", func() {
						gprestore(gprestorePath, restoreHelperPath, timestamp, "--incremental", "--exclude-table", "old_schema.old_table1", "--exclude-table", "old_schema.old_table2", "--redirect-db", "restoredb")
						assertSchemasExist(restoreConn, 4)
						assertRelationsExistForIncremental(restoreConn, 3)
						assertDataRestored(restoreConn, oldSchemaTupleCounts)
						assertDataRestored(restoreConn, newSchemaTupleCounts)
					})
					It("Does not update tables if user input is provided and schema is not included", func() {
						gprestore(gprestorePath, restoreHelperPath, timestamp, "--incremental", "--include-schema", "public", "--redirect-db", "restoredb")
						assertSchemasExist(restoreConn, 4)
						assertRelationsExistForIncremental(restoreConn, 3)
						assertDataRestored(restoreConn, oldSchemaTupleCounts)
						assertDataRestored(restoreConn, newSchemaTupleCounts)
					})
					It("Does not restore tables if user input is provide and schema is excluded by user", func() {
						gprestore(gprestorePath, restoreHelperPath, timestamp, "--incremental", "--exclude-schema", "old_schema", "--redirect-db", "restoredb")
						assertSchemasExist(restoreConn, 4)
						assertRelationsExistForIncremental(restoreConn, 3)
						assertDataRestored(restoreConn, oldSchemaTupleCounts)
						assertDataRestored(restoreConn, newSchemaTupleCounts)
					})
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
			err := exec.Command("createdb", "recreateme").Run()
			if err != nil {
				Fail(fmt.Sprintf("%v", err))
			}

			// Specifying the recreateme database will override the default DB, testdb
			timestamp := gpbackup(gpbackupPath, backupHelperPath, "--dbname", "recreateme")

			err = exec.Command("dropdb", "recreateme").Run()
			if err != nil {
				Fail(fmt.Sprintf("%v", err))
			}

			gprestore(gprestorePath, restoreHelperPath, timestamp, "--create-db")
			recreatemeConn := testutils.SetupTestDbConn("recreateme")
			recreatemeConn.Close()

			err = exec.Command("dropdb", "recreateme").Run()
			if err != nil {
				Fail(fmt.Sprintf("%v", err))
			}
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
			assertRelationsCreated(restoreConn, TOTAL_RELATIONS)
			gprestore(gprestorePath, restoreHelperPath, timestamp2, "--redirect-db", "restoredb")

			assertDataRestored(restoreConn, publicSchemaTupleCounts)
			assertDataRestored(restoreConn, schema2TupleCounts)
		})
		It("runs gpbackup and gprestore with metadata-only backup flag", func() {
			timestamp := gpbackup(gpbackupPath, backupHelperPath, "--metadata-only")
			gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb")

			assertDataRestored(restoreConn, map[string]int{"public.foo": 0, "schema2.foo3": 0})
			assertRelationsCreated(restoreConn, TOTAL_RELATIONS)
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
			assertRelationsCreated(restoreConn, TOTAL_RELATIONS)
		})
		It("runs gpbackup and gprestore with leaf-partition-data and backupDir flags", func() {
			timestamp := gpbackup(gpbackupPath, backupHelperPath, "--leaf-partition-data", "--backup-dir", backupDir)
			output := gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb", "--backup-dir", backupDir)
			Expect(string(output)).To(ContainSubstring("table 31 of 31"))

			assertDataRestored(restoreConn, publicSchemaTupleCounts)
			assertDataRestored(restoreConn, schema2TupleCounts)

		})
		It("runs gpbackup and gprestore with no-compression flag", func() {
			timestamp := gpbackup(gpbackupPath, backupHelperPath, "--no-compression", "--backup-dir", backupDir)
			gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb", "--backup-dir", backupDir)
			configFile, err := path.Glob(path.Join(backupDir, "*-1/backups/*", timestamp, "*config.yaml"))
			Expect(err).ToNot(HaveOccurred())
			Expect(configFile).To(HaveLen(1))

			contents, err := ioutil.ReadFile(configFile[0])
			Expect(err).ToNot(HaveOccurred())

			Expect(string(contents)).To(ContainSubstring("compressed: false"))
			assertRelationsCreated(restoreConn, TOTAL_RELATIONS)
			assertDataRestored(restoreConn, publicSchemaTupleCounts)
			assertDataRestored(restoreConn, schema2TupleCounts)

		})
		It("runs gpbackup and gprestore with with-stats flag", func() {
			timestamp := gpbackup(gpbackupPath, backupHelperPath, "--with-stats", "--backup-dir", backupDir)
			files, err := path.Glob(path.Join(backupDir, "*-1/backups/*", timestamp, "*statistics.sql"))
			Expect(err).ToNot(HaveOccurred())
			Expect(files).To(HaveLen(1))

			output := gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb", "--with-stats", "--backup-dir", backupDir)

			Expect(string(output)).To(ContainSubstring("Query planner statistics restore complete"))
			assertDataRestored(restoreConn, publicSchemaTupleCounts)
			assertDataRestored(restoreConn, schema2TupleCounts)

		})
		It("runs gprestore with --include-table flag to only restore tables specified ", func() {
			testhelper.AssertQueryRuns(backupConn, "CREATE TABLE public.table_to_include_with_stats(i int)")
			defer testhelper.AssertQueryRuns(backupConn, "DROP TABLE public.table_to_include_with_stats")
			testhelper.AssertQueryRuns(backupConn, "INSERT INTO public.table_to_include_with_stats VALUES (1)")
			timestamp := gpbackup(gpbackupPath, backupHelperPath, "--backup-dir", backupDir)
			gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb", "--backup-dir", backupDir, "--include-table", "public.table_to_include_with_stats")

			assertRelationsCreated(restoreConn, 1)

			localSchemaTupleCounts := map[string]int{
				`public."table_to_include_with_stats"`: 1,
			}
			assertDataRestored(restoreConn, localSchemaTupleCounts)
			assertArtifactsCleaned(restoreConn, timestamp)
		})
		It("restores statistics only for tables specified in --include-table flag when runs gprestore with with-stats flag", func() {
			testhelper.AssertQueryRuns(backupConn, "CREATE TABLE public.table_to_include_with_stats(i int)")
			defer testhelper.AssertQueryRuns(backupConn, "DROP TABLE public.table_to_include_with_stats")
			timestamp := gpbackup(gpbackupPath, backupHelperPath, "--with-stats", "--backup-dir", backupDir)
			statFiles, err := path.Glob(path.Join(backupDir, "*-1/backups/*", timestamp, "*statistics.sql"))
			Expect(err).ToNot(HaveOccurred())
			Expect(statFiles).To(HaveLen(1))

			gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb", "--with-stats", "--backup-dir", backupDir, "--include-table", "public.table_to_include_with_stats")

			rawCount := dbconn.MustSelectString(restoreConn, "SELECT COUNT(*) FROM pg_stat_all_tables WHERE schemaname='public'")
			Expect(rawCount).To(Equal(strconv.Itoa(1)))
		})
		It("runs gpbackup and gprestore with jobs flag", func() {
			skipIfOldBackupVersionBefore("1.3.0")
			timestamp := gpbackup(gpbackupPath, backupHelperPath, "--backup-dir", backupDir, "--jobs", "4")
			output := gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb", "--backup-dir", backupDir, "--jobs", "4", "--verbose")

			expectedString := fmt.Sprintf("table %d of %d", TOTAL_CREATE_STATEMENTS, TOTAL_CREATE_STATEMENTS)
			Expect(string(output)).To(ContainSubstring(expectedString))
			assertRelationsCreated(restoreConn, TOTAL_RELATIONS)
			assertDataRestored(restoreConn, schema2TupleCounts)
			assertDataRestored(restoreConn, publicSchemaTupleCounts)

		})
		It("runs gpbackup and sends a SIGINT to ensure cleanup functions successfully", func() {
			if useOldBackupVersion {
				Skip("This test is not needed for old backup versions")
			}
			args := []string{"--dbname", "testdb", "--backup-dir", backupDir, "--single-data-file", "--verbose"}
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
				_ = cmd.Process.Signal(os.Interrupt)
			}()
			output, _ := cmd.CombinedOutput()
			stdout := string(output)

			Expect(stdout).To(ContainSubstring("Received a termination signal, aborting backup process"))
			Expect(stdout).To(ContainSubstring("Cleanup complete"))
			Expect(stdout).To(Not(ContainSubstring("CRITICAL")))

		})
		It("runs gprestore and sends a SIGINT to ensure cleanup functions successfully", func() {
			if useOldBackupVersion {
				Skip("This test is not needed for old backup versions")
			}
			timestamp := gpbackup(gpbackupPath, backupHelperPath, "--backup-dir", backupDir, "--single-data-file")
			args := []string{"--timestamp", timestamp, "--redirect-db", "restoredb", "--backup-dir", backupDir, "--include-schema", "schema2", "--verbose"}
			cmd := exec.Command(gprestorePath, args...)
			go func() {
				/*
				 * We use a random delay for the sleep in this test (between
				 * 0.5s and 0.8s) so that gprestore will be interrupted at a
				 * different point in the backup process every time to help
				 * catch timing issues with the cleanup.
				 */
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))
				time.Sleep(time.Duration(rng.Intn(300)+500) * time.Millisecond)
				_ = cmd.Process.Signal(os.Interrupt)
			}()
			output, _ := cmd.CombinedOutput()
			stdout := string(output)

			Expect(stdout).To(ContainSubstring("Received a termination signal, aborting restore process"))
			Expect(stdout).To(ContainSubstring("Cleanup complete"))
			Expect(stdout).To(Not(ContainSubstring("CRITICAL")))
			assertArtifactsCleaned(restoreConn, timestamp)

		})
		It("runs gpbackup and sends a SIGINT to ensure blocked LOCK TABLE query is canceled", func() {
			if useOldBackupVersion {
				Skip("This test is not needed for old backup versions")
			}

			// Query to see if gpbackup lock acquire on schema2.foo2 is blocked
			checkLockQuery := `SELECT count(*) FROM pg_locks l, pg_class c, pg_namespace n WHERE l.relation = c.oid AND n.oid = c.relnamespace AND n.nspname = 'schema2' AND c.relname = 'foo2' AND l.granted = 'f'`

			// Acquire AccessExclusiveLock on schema2.foo2 to prevent gpbackup from acquiring AccessShareLock
			backupConn.MustExec("BEGIN; LOCK TABLE schema2.foo2 IN ACCESS EXCLUSIVE MODE")
			args := []string{"--dbname", "testdb", "--backup-dir", backupDir, "--verbose"}
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
				_ = cmd.Process.Signal(os.Interrupt)
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
			Expect(stdout).To(ContainSubstring("Received a termination signal, aborting backup process"))
			Expect(stdout).To(ContainSubstring("Cleanup complete"))
			Expect(stdout).To(Not(ContainSubstring("CRITICAL")))
		})
		It("runs example_plugin.bash with plugin_test_bench", func() {
			if useOldBackupVersion {
				Skip("This test is only needed for the latest backup version")
			}
			pluginsDir := fmt.Sprintf("%s/go/src/github.com/greenplum-db/gpbackup/plugins", os.Getenv("HOME"))
			copyPluginToAllHosts(backupConn, fmt.Sprintf("%s/example_plugin.bash", pluginsDir))
			command := exec.Command("bash", "-c", fmt.Sprintf("%s/plugin_test_bench.sh %s/example_plugin.bash %s/example_plugin_config.yaml", pluginsDir, pluginsDir, pluginsDir))
			mustRunCommand(command)

			_ = os.RemoveAll("/tmp/plugin_dest")
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

		It("runs gpbackup with --include-table flag with CAPS special characters", func() {
			skipIfOldBackupVersionBefore("1.9.1")
			timestamp := gpbackup(gpbackupPath, backupHelperPath, "--backup-dir", backupDir, "--include-table", `public.FOObar`)
			gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb", "--backup-dir", backupDir)

			assertRelationsCreated(restoreConn, 1)

			localSchemaTupleCounts := map[string]int{
				`public."FOObar"`: 1,
			}
			assertDataRestored(restoreConn, localSchemaTupleCounts)
			assertArtifactsCleaned(restoreConn, timestamp)
		})
		It("runs gpbackup with --include-table flag with partitions (non-special chars)", func() {
			testhelper.AssertQueryRuns(backupConn, `CREATE TABLE public.testparent (id int, rank int, year int, gender
char(1), count int )
DISTRIBUTED BY (id)
PARTITION BY LIST (gender)
( PARTITION girls VALUES ('F'),
  PARTITION boys VALUES ('M'),
  DEFAULT PARTITION other );
			`)
			defer testhelper.AssertQueryRuns(backupConn, `DROP TABLE public.testparent`)

			testhelper.AssertQueryRuns(backupConn, `insert into public.testparent values (1,1,1,'M',1)`)
			testhelper.AssertQueryRuns(backupConn, `insert into public.testparent values (0,0,0,'F',1)`)

			timestamp := gpbackup(gpbackupPath, backupHelperPath, "--backup-dir", backupDir, "--include-table", `public.testparent_1_prt_girls`, "--leaf-partition-data")
			gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb", "--backup-dir", backupDir)

			assertRelationsCreated(restoreConn, 4)

			localSchemaTupleCounts := map[string]int{
				`public.testparent_1_prt_girls`: 1,
				`public.testparent`:             1,
			}
			assertDataRestored(restoreConn, localSchemaTupleCounts)
			assertArtifactsCleaned(restoreConn, timestamp)
		})
		It("runs gpbackup with --include-table flag with partitions with special chars", func() {
			skipIfOldBackupVersionBefore("1.9.1")
			testhelper.AssertQueryRuns(backupConn, `CREATE TABLE public."CAPparent" (id int, rank int, year int, gender
char(1), count int )
DISTRIBUTED BY (id)
PARTITION BY LIST (gender)
( PARTITION girls VALUES ('F'),
  PARTITION boys VALUES ('M'),
  DEFAULT PARTITION other );
			`)
			defer testhelper.AssertQueryRuns(backupConn, `DROP TABLE public."CAPparent"`)

			testhelper.AssertQueryRuns(backupConn, `insert into public."CAPparent" values (1,1,1,'M',1)`)
			testhelper.AssertQueryRuns(backupConn, `insert into public."CAPparent" values (0,0,0,'F',1)`)

			timestamp := gpbackup(gpbackupPath, backupHelperPath, "--backup-dir", backupDir, "--include-table", `public.CAPparent_1_prt_girls`, "--leaf-partition-data")
			gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb", "--backup-dir", backupDir)

			assertRelationsCreated(restoreConn, 4)

			localSchemaTupleCounts := map[string]int{
				`public."CAPparent_1_prt_girls"`: 1,
				`public."CAPparent"`:             1,
			}
			assertDataRestored(restoreConn, localSchemaTupleCounts)
			assertArtifactsCleaned(restoreConn, timestamp)
		})
		It(`gpbackup runs with table name including special chars ~#$%^&*()_-+[]{}><|;:/?!\tC`, func() {
			var err error
			allChars := []rune{' ', '`', '~', '#', '$', '%', '^', '&', '*', '(', ')', '-', '+', '[', ']', '{', '}', '>', '<', '\\', '|', ';', ':', '/', '?', ',', '!', 'C'}
			var includeTableArgs []string
			includeTableArgs = append(includeTableArgs, "--dbname")
			includeTableArgs = append(includeTableArgs, "testdb")
			for _, char := range allChars {
				tableName := fmt.Sprintf(`foo%sbar`, string(char))
				testhelper.AssertQueryRuns(backupConn, fmt.Sprintf(`CREATE TABLE public."%s" ();`, tableName))
				defer testhelper.AssertQueryRuns(backupConn, fmt.Sprintf(`DROP TABLE public."%s";`, tableName))
				includeTableArgs = append(includeTableArgs, "--include-table")
				includeTableArgs = append(includeTableArgs, fmt.Sprintf(`public.%s`, tableName))
			}
			cmd := exec.Command("gpbackup", includeTableArgs...)
			_, err = cmd.CombinedOutput()
			Expect(err).ToNot(HaveOccurred())
		})
		It(`successfully backs up precise real data types`, func() {
			// Versions before 1.13.0 do not set the extra_float_digits GUC
			skipIfOldBackupVersionBefore("1.13.0")

			tableName := "public.test_real_precision"
			testhelper.AssertQueryRuns(backupConn, fmt.Sprintf(`CREATE TABLE %s (val real)`, tableName))
			defer testhelper.AssertQueryRuns(backupConn, fmt.Sprintf(`DROP TABLE %s`, tableName))
			testhelper.AssertQueryRuns(backupConn, fmt.Sprintf(`INSERT INTO %s VALUES (0.100001216)`, tableName))
			timestamp := gpbackup(gpbackupPath, backupHelperPath, "--backup-dir", backupDir, "--dbname", "testdb", "--include-table", fmt.Sprintf("%s", tableName))
			gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb", "--backup-dir", backupDir)
			tableCount := dbconn.MustSelectString(restoreConn, fmt.Sprintf("SELECT count(*) FROM %s WHERE val = 0.100001216::real", tableName))
			Expect(tableCount).To(Equal(strconv.Itoa(1)))
		})
		It(`gprestore continues when encountering errors during data load with --single-data-file and --on-error-continue`, func() {
			// This backup is corrupt because the data for a single row on
			// segment0 was changed so that the value stored in the row is
			// 9 instead of 1.  This will cause an issue when COPY FROM
			// attempts to restore this data because it will error out
			// stating it belongs to a different segment. This backup was
			// taken with gpbackup version 1.12.1 and GPDB version 4.3.33.2

			command := exec.Command("tar", "-xzf", "resources/corrupt-db.tar.gz", "-C", backupDir)
			mustRunCommand(command)

			gprestoreCmd := exec.Command(gprestorePath, "--timestamp", "20190809230424", "--redirect-db", "restoredb", "--backup-dir", path.Join(backupDir, "corrupt-db"), "--on-error-continue")
			_, err := gprestoreCmd.CombinedOutput()
			Expect(err).To(HaveOccurred())

			assertRelationsCreated(restoreConn, 3)
			// Expect corrupt_table to have 0 tuples because data load should have failed due violation of distribution key constraint.
			assertDataRestored(restoreConn, map[string]int{"public.corrupt_table": 0, "public.good_table1": 10, "public.good_table2": 10})

		})

		It("backup and restore all data when NOT VALID option on constraints is specified", func() {
			testutils.SkipIfBefore6(backupConn)
			testhelper.AssertQueryRuns(backupConn, "CREATE TABLE legacy_table_violate_constraints (a int)")
			defer testhelper.AssertQueryRuns(backupConn, "DROP TABLE legacy_table_violate_constraints")
			testhelper.AssertQueryRuns(backupConn, "INSERT INTO legacy_table_violate_constraints values (0), (1), (2), (3), (4), (5), (6), (7)")

			testhelper.AssertQueryRuns(backupConn, "ALTER TABLE legacy_table_violate_constraints ADD CONSTRAINT new_constraint_not_valid CHECK (a > 4) NOT VALID")
			defer testhelper.AssertQueryRuns(backupConn, "ALTER TABLE legacy_table_violate_constraints DROP CONSTRAINT new_constraint_not_valid")

			timestamp := gpbackup(gpbackupPath, backupHelperPath, "--backup-dir", backupDir)
			gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb", "--backup-dir", backupDir)

			legacySchemaTupleCounts := map[string]int{
				`public."legacy_table_violate_constraints"`: 8,
			}
			assertDataRestored(restoreConn, legacySchemaTupleCounts)

			isConstraintHere := dbconn.MustSelectString(restoreConn, "SELECT COUNT(*) FROM pg_constraint WHERE conname='new_constraint_not_valid'")
			Expect(isConstraintHere).To(Equal(strconv.Itoa(1)))

			_, err := restoreConn.Exec("INSERT INTO legacy_table_violate_constraints VALUES (1)")
			Expect(err).To(HaveOccurred())

			assertArtifactsCleaned(restoreConn, timestamp)
		})

		It(`ensure gprestore on corrupt backup with --on-error-continue logs error tables`, func() {
			command := exec.Command("tar", "-xzf", "resources/corrupt-db.tar.gz", "-C", backupDir)
			mustRunCommand(command)

			// Restore command with data error
			// Metadata errors due to invalid alter ownership
			expectedErrorTablesData := []string{"public.corrupt_table"}
			expectedErrorTablesMetadata := []string{"public.corrupt_table", "public.good_table1", "public.good_table2"}
			gprestoreCmd := exec.Command(gprestorePath, "--timestamp", "20190809230424", "--redirect-db", "restoredb", "--backup-dir", path.Join(backupDir, "corrupt-db"), "--on-error-continue")
			_, _ = gprestoreCmd.CombinedOutput()

			files, _ := path.Glob(path.Join(backupDir, "/corrupt-db/", "*-1/backups/*", "20190809230424", "*error_tables*"))
			Expect(files).To(HaveLen(2))

			Expect(files[0]).To(HaveSuffix("_data"))
			contents, err := ioutil.ReadFile(files[0])
			Expect(err).ToNot(HaveOccurred())
			tables := strings.Split(string(contents), "\n")
			Expect(tables).To(Equal(expectedErrorTablesData))
			_ = os.Remove(files[0])

			Expect(files).To(HaveLen(2))
			Expect(files[1]).To(HaveSuffix("_metadata"))
			contents, err = ioutil.ReadFile(files[1])
			Expect(err).ToNot(HaveOccurred())
			tables = strings.Split(string(contents), "\n")
			sort.Strings(tables)
			Expect(tables).To(Equal(expectedErrorTablesMetadata))
			_ = os.Remove(files[1])

			// Restore command with tables containing multiple metadata errors
			// This test is to ensure we don't have tables with multiple errors show up twice
			gprestoreCmd = exec.Command(gprestorePath, "--timestamp", "20190809230424", "--redirect-db", "restoredb", "--backup-dir", path.Join(backupDir, "corrupt-db"), "--metadata-only", "--on-error-continue")
			_, _ = gprestoreCmd.CombinedOutput()
			expectedErrorTablesMetadata = []string{"public.corrupt_table", "public.good_table1", "public.good_table2"}
			files, _ = path.Glob(path.Join(backupDir, "/corrupt-db/", "*-1/backups/*", "20190809230424", "*error_tables*"))
			Expect(files).To(HaveLen(1))
			Expect(files[0]).To(HaveSuffix("_metadata"))
			contents, err = ioutil.ReadFile(files[0])
			Expect(err).ToNot(HaveOccurred())
			tables = strings.Split(string(contents), "\n")
			sort.Strings(tables)
			Expect(tables).To(HaveLen(len(expectedErrorTablesMetadata)))
			_ = os.Remove(files[0])
		})

		It(`ensure successful gprestore with --on-error-continue does not log error tables`, func() {
			// Ensure no error tables with successful restore
			timestamp := gpbackup(gpbackupPath, backupHelperPath, "--no-compression", "--backup-dir", backupDir)
			gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb", "--backup-dir", backupDir)
			files, err := path.Glob(path.Join(backupDir, "*-1/backups/*", timestamp, "_error_tables*"))
			Expect(err).ToNot(HaveOccurred())
			Expect(files).To(HaveLen(0))
		})
	})
})

func saveHistory(myCluster *cluster.Cluster) {
	// move history file out of the way, and replace in "after". This is because the history file might have newer backups, with more attributes, and thus the newer history could be a longer file than when read and rewritten by the old history code (the history code reads in history, inserts a new config at top, and writes the entire file). We have known bugs in the underlying common library about closing a file after reading, and also a bug with not using OS_TRUNC when opening a file for writing.

	mdd := myCluster.GetDirForContent(-1)
	historyFilePath = path.Join(mdd, "gpbackup_history.yaml")
	_ = utils.CopyFile(historyFilePath, saveHistoryFilePath)
}
