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

	"github.com/pkg/errors"
	"github.com/blang/semver"
	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/iohelper"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/greenplum-db/gp-common-go-libs/structmatcher"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/filepath"
	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/toc"
	"github.com/greenplum-db/gpbackup/utils"
	"github.com/spf13/pflag"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gexec"

	"gopkg.in/yaml.v2"
)

/* The backup directory must be unique per test. There is test flakiness
 * against Data Domain Boost mounted file systems due to how it handles
 * directory deletion/creation.
 */
var (
	customBackupDir string

	useOldBackupVersion bool
	oldBackupSemVer     semver.Version

	backupCluster           *cluster.Cluster
	historyFilePath         string
	saveHistoryFilePath     = "/tmp/end_to_end_save_history_file.yaml"
	testFailure             bool
	backupConn              *dbconn.DBConn
	restoreConn             *dbconn.DBConn
	gpbackupPath            string
	backupHelperPath        string
	restoreHelperPath       string
	gprestorePath           string
	pluginConfigPath        string
	publicSchemaTupleCounts map[string]int
	schema2TupleCounts      map[string]int
	backupDir               string
)

const (
	TOTAL_RELATIONS               = 37
	TOTAL_RELATIONS_AFTER_EXCLUDE = 21
	TOTAL_CREATE_STATEMENTS       = 9
)

// This function is run automatically by ginkgo before any tests are run.
func init() {
	flag.StringVar(&customBackupDir, "custom_backup_dir", "/tmp",
		"custom_backup_flag for testing against a configurable directory")
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
		command := exec.Command("make", "install",
			fmt.Sprintf("helper_path=%s", restoreHelperPath))
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
	gpbackupOldPath, err := Build("github.com/greenplum-db/gpbackup",
		"-tags", "gpbackup", "-ldflags",
		fmt.Sprintf("-X github.com/greenplum-db/gpbackup/backup.version=%s", version))
	Expect(err).ShouldNot(HaveOccurred())
	gpbackupHelperOldPath, err := Build("github.com/greenplum-db/gpbackup",
		"-tags", "gpbackup_helper", "-ldflags",
		fmt.Sprintf("-X github.com/greenplum-db/gpbackup/helper.version=%s", version))
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
		actualTupleCount := dbconn.MustSelectString(conn, fmt.Sprintf("SELECT count(*) AS string FROM %s", tableName))
		if strconv.Itoa(expectedNumTuples) != actualTupleCount {
			Fail(fmt.Sprintf("Expected:\n\t%s rows to have been restored into table %s\nActual:\n\t%s rows were restored", strconv.Itoa(expectedNumTuples), tableName, actualTupleCount))
		}
	}
}

type PGClassStats struct {
	Relpages  int
	Reltuples float32
}

func assertPGClassStatsRestored(backupConn *dbconn.DBConn, restoreConn *dbconn.DBConn, tableToTupleCount map[string]int) {
	for tableName, _ := range tableToTupleCount {
		backupStats := make([]PGClassStats, 0)
		restoreStats := make([]PGClassStats, 0)
		pgClassStatsQuery := fmt.Sprintf("SELECT relpages, reltuples FROM pg_class WHERE oid='%s'::regclass::oid", tableName)
		backupErr := backupConn.Select(&backupStats, pgClassStatsQuery)
		restoreErr := restoreConn.Select(&restoreStats, pgClassStatsQuery)
		if backupErr != nil {
			Fail(fmt.Sprintf("Unable to get pg_class stats for table '%s' on the backup database", tableName))
		} else if restoreErr != nil {
			Fail(fmt.Sprintf("Unable to get pg_class stats for table '%s' on the restore database: %s", tableName, restoreErr))
		}

		if backupStats[0].Relpages != restoreStats[0].Relpages && backupStats[0].Reltuples != restoreStats[0].Reltuples {
			Fail(fmt.Sprintf("The pg_class stats for table '%s' do not match: %v != %v", tableName, backupStats, restoreStats))
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

func assertRelationsCreatedInSchema(conn *dbconn.DBConn, schema string, expectedNumTables int) {
	countQuery := fmt.Sprintf(`SELECT count(*) AS string FROM pg_class c LEFT JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relkind IN ('S','v','r') AND n.nspname = '%s'`, schema)
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
	remoteOutput := backupCluster.GenerateAndExecuteCommand(description, cluster.ON_SEGMENTS|cluster.INCLUDE_MASTER, cleanupFunc)
	if remoteOutput.NumErrors != 0 {
		Fail(fmt.Sprintf("Helper files found for timestamp %s", timestamp))
	}
}

func mustRunCommand(cmd *exec.Cmd) []byte {
	output, err := cmd.CombinedOutput()
	if err != nil {
		testFailure = true
		fmt.Printf("%s", output)
		Fail(fmt.Sprintf("%v", err))
	}
	return output
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

// fileSuffix should be config.yaml, metadata.sql, or toc.yaml, or report
func getMetdataFileContents(backupDir string, timestamp string, fileSuffix string) []byte {
	file, err := path.Glob(path.Join(backupDir, "*-1/backups", timestamp[:8], timestamp, fmt.Sprintf("gpbackup_%s_%s", timestamp, fileSuffix)))
	Expect(err).ToNot(HaveOccurred())
	fileContentBytes, err := ioutil.ReadFile(file[0])
	Expect(err).ToNot(HaveOccurred())

	return fileContentBytes
}

func saveHistory(myCluster *cluster.Cluster) {
	// move history file out of the way, and replace in "after". This is because the
	// history file might have newer backups, with more attributes, and thus the newer
	// history could be a longer file than when read and rewritten by the old history
	// code (the history code reads in history, inserts a new config at top, and writes
	// the entire file). We have known bugs in the underlying common library about
	// closing a file after reading, and also a bug with not using OS_TRUNC when opening
	// a file for writing.

	mdd := myCluster.GetDirForContent(-1)
	historyFilePath = path.Join(mdd, "gpbackup_history.yaml")
	_ = utils.CopyFile(historyFilePath, saveHistoryFilePath)
}

func TestEndToEnd(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "EndToEnd Suite")
}

var _ = BeforeSuite(func() {
	// This is used to run tests from an older gpbackup version to gprestore latest
	useOldBackupVersion = os.Getenv("OLD_BACKUP_VERSION") != ""
	pluginConfigPath =
		fmt.Sprintf("%s/src/github.com/greenplum-db/gpbackup/plugins/example_plugin_config.yaml",
			os.Getenv("GOPATH"))
	var err error
	testhelper.SetupTestLogger()
	_ = exec.Command("dropdb", "testdb").Run()
	_ = exec.Command("dropdb", "restoredb").Run()
	_ = exec.Command("psql", "postgres",
		"-c", "DROP RESOURCE QUEUE test_queue").Run()

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
	backupCmdFlags := pflag.NewFlagSet("gpbackup", pflag.ExitOnError)
	backup.SetCmdFlags(backupCmdFlags)
	backup.InitializeMetadataParams(backupConn)
	backup.SetFilterRelationClause("")
	testutils.ExecuteSQLFile(backupConn, "resources/test_tables_ddl.sql")
	testutils.ExecuteSQLFile(backupConn, "resources/test_tables_data.sql")
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
		remoteOutput := backupCluster.GenerateAndExecuteCommand(
			"Creating filespace test directories on all hosts",
			cluster.ON_HOSTS|cluster.INCLUDE_MASTER,
			func(contentID int) string {
				return fmt.Sprintf("mkdir -p /tmp/test_dir && mkdir -p /tmp/test_dir1 && mkdir -p /tmp/test_dir2")
			})
		if remoteOutput.NumErrors != 0 {
			Fail("Could not create filespace test directory on 1 or more hosts")
		}
	}

	saveHistory(backupCluster)

	err = os.MkdirAll(customBackupDir, 0777)
	if err != nil {
		Fail(fmt.Sprintf("Failed to create directory: %s. Error: %s", customBackupDir, err.Error()))
	}
	// Flag validation
	_, err = os.Stat(customBackupDir)
	if os.IsNotExist(err) {
		Fail(fmt.Sprintf("Custom backup directory %s does not exist.", customBackupDir))
	}

})

var _ = AfterSuite(func() {
	if testFailure {
		return
	}
	_ = utils.CopyFile(saveHistoryFilePath, historyFilePath)

	if backupConn.Version.Before("6") {
		testutils.DestroyTestFilespace(backupConn)
	} else {
		_ = exec.Command("psql", "postgres",
			"-c", "DROP RESOURCE QUEUE test_queue").Run()
		_ = exec.Command("psql", "postgres",
			"-c", "DROP TABLESPACE test_tablespace").Run()
		remoteOutput := backupCluster.GenerateAndExecuteCommand(
			"Removing /tmp/test_dir* directories on all hosts",
			cluster.ON_HOSTS|cluster.INCLUDE_MASTER,
			func(contentID int) string {
				return fmt.Sprintf("rm -rf /tmp/test_dir*")
			})
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

func end_to_end_setup() {
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

	// note that BeforeSuite has saved off history file, in case of running on
	// workstation where we want to retain normal (non-test?) history
	// we remove in order to work around an old common-library bug in closing a
	// file after writing, and truncating when opening to write, both of which
	// manifest as a broken history file in old code
	_ = os.Remove(historyFilePath)

	// Assign a unique directory for each test
	backupDir, _ = ioutil.TempDir(customBackupDir, "temp")
}

func end_to_end_teardown() {
	_ = os.RemoveAll(backupDir)
}

var _ = Describe("backup and restore end to end tests", func() {
	BeforeEach(func() {
		end_to_end_setup()
	})
	AfterEach(func() {
		end_to_end_teardown()
	})

	Describe("globals tests", func() {
		It("runs gpbackup and gprestore with --with-globals", func() {
			skipIfOldBackupVersionBefore("1.8.2")
			createGlobalObjects(backupConn)

			timestamp := gpbackup(gpbackupPath, backupHelperPath)

			dropGlobalObjects(backupConn, true)
			defer dropGlobalObjects(backupConn, false)

			gprestore(gprestorePath, restoreHelperPath, timestamp,
				"--redirect-db", "restoredb",
				"--with-globals")
		})
		It("runs gpbackup and gprestore with --with-globals and --create-db", func() {
			skipIfOldBackupVersionBefore("1.8.2")
			createGlobalObjects(backupConn)
			if backupConn.Version.AtLeast("6") {
				testhelper.AssertQueryRuns(backupConn,
					"ALTER ROLE global_role IN DATABASE global_db SET search_path TO public,pg_catalog;")
			}

			timestamp := gpbackup(gpbackupPath, backupHelperPath)
			dropGlobalObjects(backupConn, true)
			defer dropGlobalObjects(backupConn, true)
			gprestore(gprestorePath, restoreHelperPath, timestamp,
				"--redirect-db", "global_db",
				"--with-globals",
				"--create-db")
		})
		It("runs gpbackup with --without-globals", func() {
			skipIfOldBackupVersionBefore("1.18.0")
			createGlobalObjects(backupConn)
			defer dropGlobalObjects(backupConn, true)

			timestamp := gpbackup(gpbackupPath, backupHelperPath, "--backup-dir", backupDir, "--without-globals")

			configFileContents := getMetdataFileContents(backupDir, timestamp, "config.yaml")
			Expect(string(configFileContents)).To(ContainSubstring("withoutglobals: true"))

			metadataFileContents := getMetdataFileContents(backupDir, timestamp, "metadata.sql")
			Expect(string(metadataFileContents)).ToNot(ContainSubstring("CREATE ROLE testrole"))

			tocFileContents := getMetdataFileContents(backupDir, timestamp, "toc.yaml")
			tocStruct := &toc.TOC{}
			err := yaml.Unmarshal(tocFileContents, tocStruct)
			Expect(err).ToNot(HaveOccurred())
			Expect(len(tocStruct.GlobalEntries)).To(Equal(1))
			Expect(tocStruct.GlobalEntries[0].ObjectType).To(Equal("SESSION GUCS"))
		})
		It("runs gpbackup with --without-globals and --metadata-only", func() {
			skipIfOldBackupVersionBefore("1.18.0")
			createGlobalObjects(backupConn)
			defer dropGlobalObjects(backupConn, true)

			timestamp := gpbackup(gpbackupPath, backupHelperPath, "--backup-dir", backupDir, "--without-globals", "--metadata-only")

			configFileContents := getMetdataFileContents(backupDir, timestamp, "config.yaml")
			Expect(string(configFileContents)).To(ContainSubstring("withoutglobals: true"))

			metadataFileContents := getMetdataFileContents(backupDir, timestamp, "metadata.sql")
			Expect(string(metadataFileContents)).ToNot(ContainSubstring("CREATE ROLE testrole"))

			tocFileContents := getMetdataFileContents(backupDir, timestamp, "toc.yaml")
			tocStruct := &toc.TOC{}
			err := yaml.Unmarshal(tocFileContents, tocStruct)
			Expect(err).ToNot(HaveOccurred())
			Expect(len(tocStruct.GlobalEntries)).To(Equal(1))
			Expect(tocStruct.GlobalEntries[0].ObjectType).To(Equal("SESSION GUCS"))
		})
	})
	Describe("SIGINT tests", func() {
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
	})
	Describe(`On Error Continue`, func() {
		It(`gprestore continues when encountering errors during data load with --single-data-file and --on-error-continue`, func() {
			// This backup is corrupt because the data for a single row on
			// segment0 was changed so that the value stored in the row is
			// 9 instead of 1.  This will cause an issue when COPY FROM
			// attempts to restore this data because it will error out
			// stating it belongs to a different segment. This backup was
			// taken with gpbackup version 1.12.1 and GPDB version 4.3.33.2

			command := exec.Command("tar", "-xzf", "resources/corrupt-db.tar.gz", "-C", backupDir)
			mustRunCommand(command)

			gprestoreCmd := exec.Command(gprestorePath,
				"--timestamp", "20190809230424",
				"--redirect-db", "restoredb",
				"--backup-dir", path.Join(backupDir, "corrupt-db"),
				"--on-error-continue")
			_, err := gprestoreCmd.CombinedOutput()
			Expect(err).To(HaveOccurred())

			assertRelationsCreated(restoreConn, 3)
			// Expect corrupt_table to have 0 tuples because data load should have failed due violation of distribution key constraint.
			assertDataRestored(restoreConn, map[string]int{
				"public.corrupt_table": 0,
				"public.good_table1":   10,
				"public.good_table2":   10})
		})
		It(`Creates skip file on segments for corrupted table for helpers to discover the file and skip it with --single-data-file and --on-error-continue`, func(){
			if useOldBackupVersion {
				Skip("This test is not needed for old backup versions")
			} else if restoreConn.Version.Before("6") {
				Skip("This test does not apply to GPDB versions before 6X")
			}

			command := exec.Command("tar", "-xzf", "resources/corrupt-db.tar.gz", "-C", backupDir)
			mustRunCommand(command)

			testhelper.AssertQueryRuns(restoreConn,
				"CREATE TABLE public.corrupt_table (i integer);")
			defer testhelper.AssertQueryRuns(restoreConn,
				"DROP TABLE public.corrupt_table")

			// we know that broken value goes to seg2, so seg1 should be
			// ok. Connect in utility mode to seg1.
			segmentOne := backupCluster.ByContent[1]
			port := segmentOne[0].Port
			segConn := testutils.SetupTestDBConnSegment("restoredb", port)
			defer segConn.Close()

			// Take ACCESS EXCLUSIVE LOCK on public.corrupt_table which will
			// make COPY on seg1 block until the lock is released. By that
			// time, COPY on seg2 will fail and gprestore will create a skip
			// file for public.corrupt_table. When the lock is released on seg1,
			// the restore helper should discover the file and skip the table.
			segConn.Begin(0)
			segConn.Exec("LOCK TABLE public.corrupt_table IN ACCESS EXCLUSIVE MODE;")

			gprestoreCmd := exec.Command(gprestorePath,
				"--timestamp", "20190809230424",
				"--redirect-db", "restoredb",
				"--backup-dir", path.Join(backupDir, "corrupt-db"),
				"--data-only", "--on-error-continue",
				"--include-table", "public.corrupt_table")
			_, err := gprestoreCmd.CombinedOutput()
			Expect(err).To(HaveOccurred())

			segConn.Commit(0)
			homeDir := os.Getenv("HOME")
			helperLogs, _ := path.Glob(path.Join(homeDir, "gpAdminLogs/gpbackup_helper*"))
			cmdStr := fmt.Sprintf("tail -n 40 %s | grep \"Skip file has been discovered for entry\" || true", helperLogs[len(helperLogs)-1])

			attemts := 1000
			err = errors.New("Timeout to discover skip file")
			for attemts > 0 {
				output := mustRunCommand(exec.Command("bash", "-c", cmdStr))
				if strings.TrimSpace(string(output)) == "" {
					time.Sleep(5 * time.Millisecond)
					attemts--
				} else {
					err = nil
					break
				}
			}
			Expect(err).NotTo(HaveOccurred())
		})
		It(`ensure gprestore on corrupt backup with --on-error-continue logs error tables`, func() {
			command := exec.Command("tar", "-xzf",
				"resources/corrupt-db.tar.gz", "-C", backupDir)
			mustRunCommand(command)

			// Restore command with data error
			// Metadata errors due to invalid alter ownership
			expectedErrorTablesData := []string{"public.corrupt_table"}
			expectedErrorTablesMetadata := []string{
				"public.corrupt_table", "public.good_table1", "public.good_table2"}
			gprestoreCmd := exec.Command(gprestorePath,
				"--timestamp", "20190809230424",
				"--redirect-db", "restoredb",
				"--backup-dir", path.Join(backupDir, "corrupt-db"),
				"--on-error-continue")
			_, _ = gprestoreCmd.CombinedOutput()

			files, _ := path.Glob(path.Join(backupDir, "/corrupt-db/", "*-1/backups/*",
				"20190809230424", "*error_tables*"))
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
			gprestoreCmd = exec.Command(gprestorePath,
				"--timestamp", "20190809230424",
				"--redirect-db", "restoredb",
				"--backup-dir", path.Join(backupDir, "corrupt-db"),
				"--metadata-only",
				"--on-error-continue")
			_, _ = gprestoreCmd.CombinedOutput()
			expectedErrorTablesMetadata = []string{
				"public.corrupt_table", "public.good_table1", "public.good_table2"}
			files, _ = path.Glob(path.Join(backupDir, "/corrupt-db/",
				"*-1/backups/*", "20190809230424", "*error_tables*"))
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
			timestamp := gpbackup(gpbackupPath, backupHelperPath,
				"--no-compression",
				"--backup-dir", backupDir)
			gprestore(gprestorePath, restoreHelperPath, timestamp,
				"--redirect-db", "restoredb",
				"--backup-dir", backupDir,
				"--on-error-continue")
			files, err := path.Glob(path.Join(backupDir, "*-1/backups/*", timestamp, "_error_tables*"))
			Expect(err).ToNot(HaveOccurred())
			Expect(files).To(HaveLen(0))
		})
	})
	Describe("Redirect Schema", func() {
		It("runs gprestore with --redirect-schema restoring data and statistics to the new schema", func() {
			skipIfOldBackupVersionBefore("1.17.0")
			testhelper.AssertQueryRuns(restoreConn,
				"DROP SCHEMA IF EXISTS schema3 CASCADE; CREATE SCHEMA schema3;")
			defer testhelper.AssertQueryRuns(restoreConn,
				"DROP SCHEMA schema3 CASCADE")
			testhelper.AssertQueryRuns(backupConn,
				"CREATE INDEX foo3_idx1 ON schema2.foo3(i)")
			defer testhelper.AssertQueryRuns(backupConn,
				"DROP INDEX schema2.foo3_idx1")
			testhelper.AssertQueryRuns(backupConn,
				"ANALYZE schema2.foo3")
			timestamp := gpbackup(gpbackupPath, backupHelperPath,
				"--with-stats")
			gprestore(gprestorePath, restoreHelperPath, timestamp,
				"--redirect-db", "restoredb",
				"--include-table", "schema2.foo3",
				"--redirect-schema", "schema3",
				"--with-stats")

			schema3TupleCounts := map[string]int{
				"schema3.foo3": 100,
			}
			assertDataRestored(restoreConn, schema3TupleCounts)
			assertPGClassStatsRestored(restoreConn, restoreConn, schema3TupleCounts)

			actualIndexCount := dbconn.MustSelectString(restoreConn,
				`SELECT count(*) AS string FROM pg_indexes WHERE schemaname='schema3' AND indexname='foo3_idx1';`)
			Expect(actualIndexCount).To(Equal("1"))

			actualStatisticCount := dbconn.MustSelectString(restoreConn,
				`SELECT count(*) FROM pg_statistic WHERE starelid='schema3.foo3'::regclass::oid;`)
			Expect(actualStatisticCount).To(Equal("1"))
		})
		It("runs gprestore with --redirect-schema to redirect data back to the original database which still contain the original tables", func() {
			skipIfOldBackupVersionBefore("1.17.0")
			testhelper.AssertQueryRuns(backupConn,
				"DROP SCHEMA IF EXISTS schema3 CASCADE; CREATE SCHEMA schema3;")
			defer testhelper.AssertQueryRuns(backupConn,
				"DROP SCHEMA schema3 CASCADE")
			testhelper.AssertQueryRuns(backupConn,
				"CREATE INDEX foo3_idx1 ON schema2.foo3(i)")
			defer testhelper.AssertQueryRuns(backupConn,
				"DROP INDEX schema2.foo3_idx1")
			testhelper.AssertQueryRuns(backupConn,
				"ANALYZE schema2.foo3")
			timestamp := gpbackup(gpbackupPath, backupHelperPath,
				"--with-stats")
			gprestore(gprestorePath, restoreHelperPath, timestamp,
				"--include-table", "schema2.foo3",
				"--redirect-schema", "schema3",
				"--with-stats")

			schema3TupleCounts := map[string]int{
				"schema3.foo3": 100,
			}
			assertDataRestored(backupConn, schema3TupleCounts)
			assertPGClassStatsRestored(backupConn, backupConn, schema3TupleCounts)

			actualIndexCount := dbconn.MustSelectString(backupConn,
				`SELECT count(*) AS string FROM pg_indexes WHERE schemaname='schema3' AND indexname='foo3_idx1';`)
			Expect(actualIndexCount).To(Equal("1"))

			actualStatisticCount := dbconn.MustSelectString(backupConn,
				`SELECT count(*) FROM pg_statistic WHERE starelid='schema3.foo3'::regclass::oid;`)
			Expect(actualStatisticCount).To(Equal("1"))
		})
		It("runs gprestore with --redirect-schema and multiple included schemas", func() {
			skipIfOldBackupVersionBefore("1.17.0")
			testhelper.AssertQueryRuns(restoreConn,
				"DROP SCHEMA IF EXISTS schema3 CASCADE; CREATE SCHEMA schema3;")
			defer testhelper.AssertQueryRuns(restoreConn,
				"DROP SCHEMA schema3 CASCADE")
			testhelper.AssertQueryRuns(backupConn,
				"CREATE SCHEMA \"FOO\"")
			defer testhelper.AssertQueryRuns(backupConn,
				"DROP SCHEMA \"FOO\" CASCADE")
			testhelper.AssertQueryRuns(backupConn,
				"CREATE TABLE \"FOO\".bar(i int)")

			tableFile := path.Join(backupDir, "test-table-file.txt")
			includeFile := iohelper.MustOpenFileForWriting(tableFile)
			utils.MustPrintln(includeFile,
				"public.sales\nschema2.foo2\nschema2.ao1")
			utils.MustPrintln(includeFile,
				"public.sales\nschema2.foo2\nschema2.ao1\nFOO.bar")
			timestamp := gpbackup(gpbackupPath, backupHelperPath)

			gprestore(gprestorePath, restoreHelperPath, timestamp,
				"--include-table-file", tableFile,
				"--redirect-db", "restoredb",
				"--redirect-schema", "schema3")

			schema3TupleCounts := map[string]int{
				"schema3.foo2":  0,
				"schema3.ao1":   1000,
				"schema3.sales": 13,
				"schema3.bar":   0,
			}
			assertDataRestored(restoreConn, schema3TupleCounts)
			assertRelationsCreatedInSchema(restoreConn, "schema2", 0)
		})
		It("runs --redirect-schema with --matadata-only", func() {
			skipIfOldBackupVersionBefore("1.17.0")
			testhelper.AssertQueryRuns(restoreConn,
				"DROP SCHEMA IF EXISTS schema_to_redirect CASCADE; CREATE SCHEMA \"schema_to_redirect\";")
			defer testhelper.AssertQueryRuns(restoreConn,
				"DROP SCHEMA schema_to_redirect CASCADE")
			testhelper.AssertQueryRuns(backupConn,
				"CREATE SCHEMA schema_to_test")
			defer testhelper.AssertQueryRuns(backupConn,
				"DROP SCHEMA schema_to_test CASCADE")
			testhelper.AssertQueryRuns(backupConn,
				"CREATE TABLE schema_to_test.table_metadata_only AS SELECT generate_series(1,10)")
			timestamp := gpbackup(gpbackupPath, backupHelperPath, "--metadata-only", "--include-schema", "schema_to_test")
			gprestore(gprestorePath, restoreHelperPath, timestamp,
				"--redirect-db", "restoredb",
				"--redirect-schema", "schema_to_redirect",
				"--include-table", "schema_to_test.table_metadata_only",
				"--metadata-only")
			assertRelationsCreatedInSchema(restoreConn, "schema_to_redirect", 1)
			assertDataRestored(restoreConn, map[string]int{"schema_to_redirect.table_metadata_only": 0})
		})
		It("runs --redirect-schema with --include-schema and --include-schema-file", func() {
			skipIfOldBackupVersionBefore("1.17.0")
			testhelper.AssertQueryRuns(restoreConn,
				"DROP SCHEMA IF EXISTS schema3 CASCADE; CREATE SCHEMA schema3;")
			defer testhelper.AssertQueryRuns(restoreConn,
				"DROP SCHEMA schema3 CASCADE")
			testhelper.AssertQueryRuns(backupConn,
				"CREATE SCHEMA fooschema")
			defer testhelper.AssertQueryRuns(backupConn,
				"DROP SCHEMA fooschema CASCADE")
			testhelper.AssertQueryRuns(backupConn,
				"CREATE TABLE fooschema.redirected_table(i int)")

			schemaFile := path.Join(backupDir, "test-schema-file.txt")
			includeSchemaFd := iohelper.MustOpenFileForWriting(schemaFile)
			utils.MustPrintln(includeSchemaFd, "fooschema")

			timestamp := gpbackup(gpbackupPath, backupHelperPath)

			gprestore(gprestorePath, restoreHelperPath, timestamp,
				"--include-schema-file", schemaFile,
				"--include-schema", "schema2",
				"--redirect-db", "restoredb",
				"--redirect-schema", "schema3")

			expectedSchema3TupleCounts := map[string]int{
				"schema3.returns": 6,
				"schema3.foo2":    0,
				"schema3.foo3":    100,
				"schema3.ao1":     1000,
				"schema3.ao2":     1000,
				"schema3.redirected_table": 0,
			}
			assertDataRestored(restoreConn, expectedSchema3TupleCounts)
			assertRelationsCreatedInSchema(restoreConn, "public", 0)
			assertRelationsCreatedInSchema(restoreConn, "schema2", 0)
			assertRelationsCreatedInSchema(restoreConn, "fooschema", 0)
		})
	})
	Describe("ACLs for extensions", func() {
		It("runs gpbackup and gprestores any user defined ACLs on extensions", func() {
			testutils.SkipIfBefore5(backupConn)
			skipIfOldBackupVersionBefore("1.17.0")
			currentUser := os.Getenv("USER")
			testhelper.AssertQueryRuns(backupConn, "CREATE ROLE testrole")
			defer testhelper.AssertQueryRuns(backupConn,
				"DROP ROLE testrole")
			testhelper.AssertQueryRuns(backupConn, "CREATE EXTENSION pgcrypto")
			defer testhelper.AssertQueryRuns(backupConn,
				"DROP EXTENSION pgcrypto")
			// Create a grant on a function that belongs to the extension
			testhelper.AssertQueryRuns(backupConn,
				"GRANT EXECUTE ON FUNCTION gen_random_bytes(integer) to testrole WITH GRANT OPTION")

			timestamp := gpbackup(gpbackupPath, backupHelperPath,
				"--metadata-only")
			gprestore(gprestorePath, restoreHelperPath, timestamp,
				"--redirect-db", "restoredb")

			extensionMetadata := backup.ObjectMetadata{
				ObjectType: "FUNCTION", Privileges: []backup.ACL{
					{Grantee: "", Execute: true},
					{Grantee: currentUser, Execute: true},
					{Grantee: "testrole", ExecuteWithGrant: true},
				}, Owner: currentUser}

			// Check for the corresponding grants in restored database
			uniqueID := testutils.UniqueIDFromObjectName(restoreConn,
				"public", "gen_random_bytes", backup.TYPE_FUNCTION)
			resultMetadataMap := backup.GetMetadataForObjectType(restoreConn, backup.TYPE_FUNCTION)

			Expect(resultMetadataMap).To(Not(BeEmpty()))
			resultMetadata := resultMetadataMap[uniqueID]
			match, err := structmatcher.MatchStruct(&extensionMetadata).Match(&resultMetadata)
			Expect(err).To(Not(HaveOccurred()))
			Expect(match).To(BeTrue())
			// Following statement is needed in order to drop testrole
			testhelper.AssertQueryRuns(restoreConn, "DROP EXTENSION pgcrypto")
			assertArtifactsCleaned(restoreConn, timestamp)
		})
	})
	Describe("Restore with truncate-table", func() {
		It("runs gpbackup and gprestore with truncate-table and include-table flags", func() {
			timestamp := gpbackup(gpbackupPath, backupHelperPath)
			gprestore(gprestorePath, restoreHelperPath, timestamp,
				"--redirect-db", "restoredb",
				"--include-table", "public.sales")
			assertDataRestored(restoreConn, map[string]int{
				"public.sales": 13})

			testhelper.AssertQueryRuns(restoreConn,
				"INSERT into sales values(1, '2017-01-01', 109.99)")
			time.Sleep(1 * time.Second)

			gprestore(gprestorePath, restoreHelperPath, timestamp,
				"--redirect-db", "restoredb",
				"--include-table", "public.sales",
				"--truncate-table", "--data-only")
			assertDataRestored(restoreConn, map[string]int{
				"public.sales": 13})
		})
		It("runs gpbackup and gprestore with truncate-table and include-table-file flags", func() {
			includeFile := iohelper.MustOpenFileForWriting("/tmp/include-tables.txt")
			utils.MustPrintln(includeFile, "public.sales")
			timestamp := gpbackup(gpbackupPath, backupHelperPath,
				"--backup-dir", backupDir)
			gprestore(gprestorePath, restoreHelperPath, timestamp,
				"--redirect-db", "restoredb",
				"--backup-dir", backupDir,
				"--include-table-file", "/tmp/include-tables.txt")
			assertDataRestored(restoreConn, map[string]int{
				"public.sales": 13})

			testhelper.AssertQueryRuns(restoreConn,
				"INSERT into sales values(1, '2017-01-01', 99.99)")
			time.Sleep(1 * time.Second)

			gprestore(gprestorePath, restoreHelperPath, timestamp,
				"--redirect-db", "restoredb",
				"--backup-dir", backupDir,
				"--include-table-file", "/tmp/include-tables.txt",
				"--truncate-table", "--data-only")
			assertDataRestored(restoreConn, map[string]int{
				"public.sales": 13})

			_ = os.Remove("/tmp/include-tables.txt")
		})
		It("runs gpbackup and gprestore with truncate-table flag against a leaf partition", func() {
			skipIfOldBackupVersionBefore("1.7.2")
			timestamp := gpbackup(gpbackupPath, backupHelperPath,
				"--leaf-partition-data")
			gprestore(gprestorePath, restoreHelperPath, timestamp,
				"--redirect-db", "restoredb",
				"--include-table", "public.sales_1_prt_jan17")

			testhelper.AssertQueryRuns(restoreConn,
				"INSERT into public.sales_1_prt_jan17 values(1, '2017-01-01', 99.99)")
			time.Sleep(1 * time.Second)

			gprestore(gprestorePath, restoreHelperPath, timestamp,
				"--redirect-db", "restoredb",
				"--include-table", "public.sales_1_prt_jan17",
				"--truncate-table", "--data-only")
			assertDataRestored(restoreConn, map[string]int{
				"public.sales": 1, "public.sales_1_prt_jan17": 1})
		})
	})
	Describe("Restore with --run-analyze", func() {
		It("runs gprestore without --run-analyze", func() {
			timestamp := gpbackup(gpbackupPath, backupHelperPath,
				"--include-table", "public.sales")
			gprestore(gprestorePath, restoreHelperPath, timestamp,
				"--redirect-db", "restoredb")

			// Since --run-analyze was not used, there should be no statistics
			actualStatisticCount := dbconn.MustSelectString(restoreConn,
				`SELECT count(*) FROM pg_statistic WHERE starelid='public.sales'::regclass::oid`)
			Expect(actualStatisticCount).To(Equal("0"))
		})
		It("runs gprestore with --run-analyze", func() {
			timestamp := gpbackup(gpbackupPath, backupHelperPath,
				"--include-table", "public.sales")
			gprestore(gprestorePath, restoreHelperPath, timestamp,
				"--redirect-db", "restoredb",
				"--run-analyze")

			// Since --run-analyze was used, there should be stats
			// for all 3 columns of the sales partition table
			actualStatisticCount := dbconn.MustSelectString(restoreConn,
				`SELECT count(*) FROM pg_statistic WHERE starelid='public.sales'::regclass::oid`)
			Expect(actualStatisticCount).To(Equal("3"))
		})
		It("runs gprestore with --run-analyze and --redirect-schema", func() {
			skipIfOldBackupVersionBefore("1.17.0")
			testhelper.AssertQueryRuns(restoreConn, "CREATE SCHEMA fooschema")
			defer testhelper.AssertQueryRuns(restoreConn, "DROP SCHEMA fooschema CASCADE")
			timestamp := gpbackup(gpbackupPath, backupHelperPath,
				"--include-table", "public.sales")
			gprestore(gprestorePath, restoreHelperPath, timestamp,
				"--redirect-db", "restoredb",
				"--include-table", "public.sales",
				"--redirect-schema", "fooschema",
				"--run-analyze")

			// Since --run-analyze was used, there should be stats
			// for all 3 columns of the sales partition table.
			actualStatisticCount := dbconn.MustSelectString(restoreConn,
				`SELECT count(*) FROM pg_statistic WHERE starelid='fooschema.sales'::regclass::oid`)
			Expect(actualStatisticCount).To(Equal("3"))
		})
		It("runs gpbackup with --leaf-partition-data and gprestore with --run-analyze", func() {
			timestamp := gpbackup(gpbackupPath, backupHelperPath,
				"--include-table", "public.sales", "--leaf-partition-data")
			gprestore(gprestorePath, restoreHelperPath, timestamp,
				"--redirect-db", "restoredb",
				"--run-analyze")

			// Since --run-analyze was used, there should be stats
			// for all 3 columns of the sales partition table. The
			// leaf partition stats should merge up to the root
			// partition.
			actualStatisticCount := dbconn.MustSelectString(restoreConn,
				`SELECT count(*) FROM pg_statistic WHERE starelid='public.sales'::regclass::oid`)
			Expect(actualStatisticCount).To(Equal("3"))
		})
	})
	It("runs gpbackup and gprestore without redirecting restore to another db", func() {
		err := exec.Command("createdb", "recreateme").Run()
		if err != nil {
			Fail(fmt.Sprintf("%v", err))
		}

		// Specifying the recreateme database will override the default DB, testdb
		timestamp := gpbackup(gpbackupPath, backupHelperPath,
			"--dbname", "recreateme")

		err = exec.Command("dropdb", "recreateme").Run()
		if err != nil {
			Fail(fmt.Sprintf("%v", err))
		}

		gprestore(gprestorePath, restoreHelperPath, timestamp,
			"--create-db")
		recreatemeConn := testutils.SetupTestDbConn("recreateme")
		recreatemeConn.Close()

		err = exec.Command("dropdb", "recreateme").Run()
		if err != nil {
			Fail(fmt.Sprintf("%v", err))
		}
	})
	It("runs basic gpbackup and gprestore with metadata and data-only flags", func() {
		timestamp := gpbackup(gpbackupPath, backupHelperPath,
			"--metadata-only")
		timestamp2 := gpbackup(gpbackupPath, backupHelperPath,
			"--data-only")
		gprestore(gprestorePath, restoreHelperPath, timestamp,
			"--redirect-db", "restoredb")
		assertDataRestored(restoreConn, map[string]int{
			"public.foo": 0, "schema2.foo3": 0})
		assertRelationsCreated(restoreConn, TOTAL_RELATIONS)
		gprestore(gprestorePath, restoreHelperPath, timestamp2,
			"--redirect-db", "restoredb")

		assertDataRestored(restoreConn, publicSchemaTupleCounts)
		assertDataRestored(restoreConn, schema2TupleCounts)
	})
	It("runs gpbackup and gprestore with metadata-only backup flag", func() {
		timestamp := gpbackup(gpbackupPath, backupHelperPath,
			"--metadata-only")
		gprestore(gprestorePath, restoreHelperPath, timestamp,
			"--redirect-db", "restoredb")

		assertDataRestored(restoreConn, map[string]int{
			"public.foo": 0, "schema2.foo3": 0})
		assertRelationsCreated(restoreConn, TOTAL_RELATIONS)
	})
	It("runs gpbackup and gprestore with data-only backup flag", func() {
		testutils.ExecuteSQLFile(restoreConn, "resources/test_tables_ddl.sql")

		timestamp := gpbackup(gpbackupPath, backupHelperPath,
			"--data-only")
		gprestore(gprestorePath, restoreHelperPath, timestamp,
			"--redirect-db", "restoredb")

		assertDataRestored(restoreConn, publicSchemaTupleCounts)
		assertDataRestored(restoreConn, schema2TupleCounts)
	})
	It("runs gpbackup and gprestore with the data-only restore flag", func() {
		testutils.ExecuteSQLFile(restoreConn, "resources/test_tables_ddl.sql")
		testhelper.AssertQueryRuns(backupConn, "SELECT pg_catalog.setval('public.myseq2', 8888, false)")
		defer testhelper.AssertQueryRuns(backupConn, "SELECT pg_catalog.setval('public.myseq2', 100, false)")

		timestamp := gpbackup(gpbackupPath, backupHelperPath)
		output := gprestore(gprestorePath, restoreHelperPath, timestamp,
			"--redirect-db", "restoredb",
			"--data-only")

		assertDataRestored(restoreConn, publicSchemaTupleCounts)
		assertDataRestored(restoreConn, schema2TupleCounts)

		// Assert that sequence values have been properly
		// updated as part of special sequence handling during
		// gprestore --data-only calls
		restoreSequenceValue := dbconn.MustSelectString(restoreConn,
			`SELECT last_value FROM public.myseq2`)
		Expect(restoreSequenceValue).To(Equal("8888"))
		Expect(string(output)).To(ContainSubstring("Restoring sequence values"))
	})
	It("runs gpbackup and gprestore with the metadata-only restore flag", func() {
		timestamp := gpbackup(gpbackupPath, backupHelperPath)
		gprestore(gprestorePath, restoreHelperPath, timestamp,
			"--redirect-db", "restoredb",
			"--metadata-only")

		assertDataRestored(restoreConn, map[string]int{
			"public.foo": 0, "schema2.foo3": 0})
		assertRelationsCreated(restoreConn, TOTAL_RELATIONS)
	})
	It("runs gpbackup and gprestore with leaf-partition-data and backupDir flags", func() {
		timestamp := gpbackup(gpbackupPath, backupHelperPath,
			"--leaf-partition-data",
			"--backup-dir", backupDir)
		output := gprestore(gprestorePath, restoreHelperPath, timestamp,
			"--redirect-db", "restoredb",
			"--backup-dir", backupDir)
		Expect(string(output)).To(ContainSubstring("table 31 of 31"))

		assertDataRestored(restoreConn, publicSchemaTupleCounts)
		assertDataRestored(restoreConn, schema2TupleCounts)
	})
	It("runs gpbackup and gprestore with no-compression flag", func() {
		timestamp := gpbackup(gpbackupPath, backupHelperPath,
			"--no-compression",
			"--backup-dir", backupDir)
		gprestore(gprestorePath, restoreHelperPath, timestamp,
			"--redirect-db", "restoredb",
			"--backup-dir", backupDir)
		configFile, err := path.Glob(path.Join(backupDir, "*-1/backups/*",
			timestamp, "*config.yaml"))
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
		// gpbackup before version 1.18.0 does not dump pg_class statistics correctly
		skipIfOldBackupVersionBefore("1.18.0")

		timestamp := gpbackup(gpbackupPath, backupHelperPath,
			"--with-stats",
			"--backup-dir", backupDir)
		files, err := path.Glob(path.Join(backupDir, "*-1/backups/*",
			timestamp, "*statistics.sql"))
		Expect(err).ToNot(HaveOccurred())
		Expect(files).To(HaveLen(1))

		output := gprestore(gprestorePath, restoreHelperPath, timestamp,
			"--redirect-db", "restoredb",
			"--with-stats",
			"--backup-dir", backupDir)

		Expect(string(output)).To(ContainSubstring("Query planner statistics restore complete"))
		assertDataRestored(restoreConn, publicSchemaTupleCounts)
		assertDataRestored(restoreConn, schema2TupleCounts)
		assertPGClassStatsRestored(backupConn, restoreConn, publicSchemaTupleCounts)
		assertPGClassStatsRestored(backupConn, restoreConn, schema2TupleCounts)

		backupStatisticCount := dbconn.MustSelectString(backupConn,
			`SELECT count(*) AS string FROM pg_statistic;`)
		restoredStatisticsCount := dbconn.MustSelectString(restoreConn,
			`SELECT count(*) AS string FROM pg_statistic;`)
		Expect(backupStatisticCount).To(Equal(restoredStatisticsCount))

		restoredTablesAnalyzed := dbconn.MustSelectString(restoreConn,
			`SELECT count(*) FROM pg_stat_last_operation WHERE objid IN ('public.foo'::regclass::oid, 'public.holds'::regclass::oid, 'public.sales'::regclass::oid, 'schema2.returns'::regclass::oid, 'schema2.foo2'::regclass::oid, 'schema2.foo3'::regclass::oid, 'schema2.ao1'::regclass::oid, 'schema2.ao2'::regclass::oid) AND staactionname='ANALYZE';`)
		Expect(restoredTablesAnalyzed).To(Equal("0"))
	})
	It("restores statistics only for tables specified in --include-table flag when runs gprestore with with-stats flag", func() {
		// gpbackup before version 1.18.0 does not dump pg_class statistics correctly
		skipIfOldBackupVersionBefore("1.18.0")

		testhelper.AssertQueryRuns(backupConn,
			"CREATE TABLE public.table_to_include_with_stats(i int)")
		testhelper.AssertQueryRuns(backupConn,
			"INSERT INTO public.table_to_include_with_stats SELECT generate_series(0,9);")
		defer testhelper.AssertQueryRuns(backupConn,
			"DROP TABLE public.table_to_include_with_stats")
		timestamp := gpbackup(gpbackupPath, backupHelperPath,
			"--with-stats",
			"--backup-dir", backupDir)
		statFiles, err := path.Glob(path.Join(backupDir, "*-1/backups/*",
			timestamp, "*statistics.sql"))
		Expect(err).ToNot(HaveOccurred())
		Expect(statFiles).To(HaveLen(1))

		gprestore(gprestorePath, restoreHelperPath, timestamp,
			"--redirect-db", "restoredb",
			"--with-stats",
			"--backup-dir", backupDir,
			"--include-table", "public.table_to_include_with_stats")

		includeTableTupleCounts := map[string]int{
			"public.table_to_include_with_stats": 10,
		}
		assertDataRestored(backupConn, includeTableTupleCounts)
		assertPGClassStatsRestored(backupConn, restoreConn, includeTableTupleCounts)

		rawCount := dbconn.MustSelectString(restoreConn,
			"SELECT count(*) FROM pg_statistic WHERE starelid = 'public.table_to_include_with_stats'::regclass::oid;")
		Expect(rawCount).To(Equal(strconv.Itoa(1)))

		restoreTableCount := dbconn.MustSelectString(restoreConn,
			"SELECT count(*) FROM pg_class WHERE oid >= 16384 AND relnamespace in (SELECT oid from pg_namespace WHERE nspname in ('public', 'schema2'));")
		Expect(restoreTableCount).To(Equal(strconv.Itoa(1)))
	})
	It("runs gpbackup and gprestore with jobs flag", func() {
		skipIfOldBackupVersionBefore("1.3.0")
		timestamp := gpbackup(gpbackupPath, backupHelperPath,
			"--backup-dir", backupDir,
			"--jobs", "4")
		output := gprestore(gprestorePath, restoreHelperPath, timestamp,
			"--redirect-db", "restoredb",
			"--backup-dir", backupDir,
			"--jobs", "4",
			"--verbose")

		expectedString := fmt.Sprintf("table %d of %d", TOTAL_CREATE_STATEMENTS, TOTAL_CREATE_STATEMENTS)
		Expect(string(output)).To(ContainSubstring(expectedString))
		assertRelationsCreated(restoreConn, TOTAL_RELATIONS)
		assertDataRestored(restoreConn, schema2TupleCounts)
		assertDataRestored(restoreConn, publicSchemaTupleCounts)
	})
	It("runs gpbackup with jobs flag and COPY deadlock handling occurs", func() {
		if useOldBackupVersion {
			Skip("This test is not needed for old backup versions")
		}

		// Acquire AccessExclusiveLock on public.foo to block gpbackup when it attempts
		// to grab AccessShareLocks before its metadata dump section.
		backupConn.MustExec("BEGIN; LOCK TABLE public.foo IN ACCESS EXCLUSIVE MODE")

		// Execute gpbackup with --jobs 9 since there are 9 tables to back up
		args := []string{
			"--dbname", "testdb",
			"--backup-dir", backupDir,
			"--jobs", "9",
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
		dataTables := []string{`public."FOObar"`, "public.foo", "public.holds", "public.sales",
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

		// Concurrently wait for all AccessExclusiveLock requests on all 9 test tables to block.
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

		// Sanity check that 9 deadlock traps were placed during the test
		Expect(accessExclBlockedLockCount).To(Equal(9))

		// No non-main worker should have been able to run COPY due to deadlock detection
		for i := 1; i < 9; i++ {
			expectedLockString := fmt.Sprintf("[DEBUG]:-Worker %d: LOCK TABLE ", i)
			Expect(stdout).To(ContainSubstring(expectedLockString))

			expectedWarnString := fmt.Sprintf("[WARNING]:-Worker %d could not acquire AccessShareLock for table", i)
			Expect(stdout).To(ContainSubstring(expectedWarnString))

			unexpectedCopyString := fmt.Sprintf("[DEBUG]:-Worker %d: COPY ", i)
			Expect(stdout).ToNot(ContainSubstring(unexpectedCopyString))
		}

		// Only the main worker thread, worker 0, will run COPY on all the test tables
		for _, dataTable := range dataTables {
			expectedString := fmt.Sprintf(`[DEBUG]:-Worker 0: COPY %s TO PROGRAM `, dataTable)
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
		dataTables := []string{`public."FOObar"`, "public.foo", "public.holds", "public.sales",
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

		// Concurrently wait for all AccessExclusiveLock requests on all 9 test tables to block.
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

		// Sanity check that 9 deadlock traps were placed during the test
		Expect(accessExclBlockedLockCount).To(Equal(9))
		Expect(stdout).To(ContainSubstring("All copy queue workers terminated due to lock issues. Falling back to single main worker."))
		// No non-main worker should have been able to run COPY due to deadlock detection
		for i := 1; i < 2; i++ {
			expectedLockString := fmt.Sprintf("[DEBUG]:-Worker %d: LOCK TABLE ", i)
			Expect(stdout).To(ContainSubstring(expectedLockString))

			expectedWarnString := fmt.Sprintf("[WARNING]:-Worker %d could not acquire AccessShareLock for table", i)
			Expect(stdout).To(ContainSubstring(expectedWarnString))

			unexpectedCopyString := fmt.Sprintf("[DEBUG]:-Worker %d: COPY ", i)
			Expect(stdout).ToNot(ContainSubstring(unexpectedCopyString))

			expectedLockString = fmt.Sprintf(`Locks held on table %s`, dataTables[i])
			Expect(stdout).To(ContainSubstring(expectedLockString))

			Expect(stdout).To(ContainSubstring(`"Mode":"AccessExclusiveLock"`))
		}

		// Only the main worker thread, worker 0, will run COPY on all the test tables
		for _, dataTable := range dataTables {
			expectedString := fmt.Sprintf(`[DEBUG]:-Worker 0: COPY %s TO PROGRAM `, dataTable)
			Expect(stdout).To(ContainSubstring(expectedString))
		}

		Expect(stdout).To(ContainSubstring("Backup completed successfully"))
	})
	It("successfully backs up foreign tables", func() {
		if backupConn.Version.Before("6") {
			Skip("Test does not apply for GPDB versions before 6")
		}
		testhelper.AssertQueryRuns(backupConn,
			"CREATE FOREIGN DATA WRAPPER foreigndatawrapper;")
		defer testhelper.AssertQueryRuns(backupConn,
			"DROP FOREIGN DATA WRAPPER foreigndatawrapper CASCADE;")
		testhelper.AssertQueryRuns(backupConn,
			"CREATE SERVER sc FOREIGN DATA WRAPPER foreigndatawrapper;")
		testhelper.AssertQueryRuns(backupConn,
			"CREATE FOREIGN TABLE public.ft1 (field1 text) SERVER sc")
		testhelper.AssertQueryRuns(backupConn,
			"CREATE FOREIGN TABLE public.ft2 (field1 text) SERVER sc")
		args := []string{
			"--dbname", "testdb",
			"--backup-dir", backupDir,
			"--jobs", "4",
			"--verbose"}
		cmd := exec.Command(gpbackupPath, args...)
		output, _ := cmd.CombinedOutput()
		stdout := string(output)
		Expect(stdout).To(Not(ContainSubstring("CRITICAL")))
		Expect(stdout).To(ContainSubstring("Skipping data backup of table public.ft1"))
		Expect(stdout).To(ContainSubstring("Skipping data backup of table public.ft2"))
		Expect(stdout).To(ContainSubstring("Skipped data backup of 2 external/foreign table(s)"))
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
	It(`successfully backs up precise real data types`, func() {
		// Versions before 1.13.0 do not set the extra_float_digits GUC
		skipIfOldBackupVersionBefore("1.13.0")

		tableName := "public.test_real_precision"
		tableNameCopy := "public.test_real_precision_copy"
		testhelper.AssertQueryRuns(backupConn, fmt.Sprintf(`CREATE TABLE %s (val real)`, tableName))
		defer testhelper.AssertQueryRuns(backupConn, fmt.Sprintf(`DROP TABLE %s`, tableName))
		testhelper.AssertQueryRuns(backupConn, fmt.Sprintf(`INSERT INTO %s VALUES (0.100001216)`, tableName))
		testhelper.AssertQueryRuns(backupConn, fmt.Sprintf(`CREATE TABLE %s AS SELECT * FROM %s`, tableNameCopy, tableName))
		defer testhelper.AssertQueryRuns(backupConn, fmt.Sprintf(`DROP TABLE %s`, tableNameCopy))

		// We use --jobs flag to make sure all parallel connections have the GUC set properly
		timestamp := gpbackup(gpbackupPath, backupHelperPath,
			"--backup-dir", backupDir,
			"--dbname", "testdb", "--jobs", "2",
			"--include-table", fmt.Sprintf("%s", tableName),
			"--include-table", fmt.Sprintf("%s", tableNameCopy))
		gprestore(gprestorePath, restoreHelperPath, timestamp,
			"--redirect-db", "restoredb",
			"--backup-dir", backupDir)
		tableCount := dbconn.MustSelectString(restoreConn, fmt.Sprintf("SELECT count(*) FROM %s WHERE val = 0.100001216::real", tableName))
		Expect(tableCount).To(Equal(strconv.Itoa(1)))
		tableCopyCount := dbconn.MustSelectString(restoreConn, fmt.Sprintf("SELECT count(*) FROM %s WHERE val = 0.100001216::real", tableNameCopy))
		Expect(tableCopyCount).To(Equal(strconv.Itoa(1)))
	})
	It("backup and restore all data when NOT VALID option on constraints is specified", func() {
		testutils.SkipIfBefore6(backupConn)
		testhelper.AssertQueryRuns(backupConn,
			"CREATE TABLE legacy_table_violate_constraints (a int)")
		defer testhelper.AssertQueryRuns(backupConn,
			"DROP TABLE legacy_table_violate_constraints")
		testhelper.AssertQueryRuns(backupConn,
			"INSERT INTO legacy_table_violate_constraints values (0), (1), (2), (3), (4), (5), (6), (7)")
		testhelper.AssertQueryRuns(backupConn,
			"ALTER TABLE legacy_table_violate_constraints ADD CONSTRAINT new_constraint_not_valid CHECK (a > 4) NOT VALID")
		defer testhelper.AssertQueryRuns(backupConn,
			"ALTER TABLE legacy_table_violate_constraints DROP CONSTRAINT new_constraint_not_valid")

		timestamp := gpbackup(gpbackupPath, backupHelperPath,
			"--backup-dir", backupDir)
		gprestore(gprestorePath, restoreHelperPath, timestamp,
			"--redirect-db", "restoredb",
			"--backup-dir", backupDir)

		legacySchemaTupleCounts := map[string]int{
			`public."legacy_table_violate_constraints"`: 8,
		}
		assertDataRestored(restoreConn, legacySchemaTupleCounts)

		isConstraintHere := dbconn.MustSelectString(restoreConn,
			"SELECT count(*) FROM pg_constraint WHERE conname='new_constraint_not_valid'")
		Expect(isConstraintHere).To(Equal(strconv.Itoa(1)))

		_, err := restoreConn.Exec("INSERT INTO legacy_table_violate_constraints VALUES (1)")
		Expect(err).To(HaveOccurred())
		assertArtifactsCleaned(restoreConn, timestamp)
	})
	It("runs gpbackup and gprestore to backup tables depending on functions", func() {
		skipIfOldBackupVersionBefore("1.19.0")
		testhelper.AssertQueryRuns(backupConn, "CREATE FUNCTION func1(val integer) RETURNS integer AS $$ BEGIN RETURN val + 1; END; $$ LANGUAGE PLPGSQL;;")
		defer testhelper.AssertQueryRuns(backupConn, "DROP FUNCTION func1(val integer);")

		testhelper.AssertQueryRuns(backupConn, "CREATE TABLE test_depends_on_function (id integer, claim_id character varying(20) DEFAULT ('WC-'::text || func1(10)::text)) DISTRIBUTED BY (id);")
		defer testhelper.AssertQueryRuns(backupConn, "DROP TABLE test_depends_on_function;")
		testhelper.AssertQueryRuns(backupConn, "INSERT INTO  test_depends_on_function values (1);")
		testhelper.AssertQueryRuns(backupConn, "INSERT INTO  test_depends_on_function values (2);")

		timestamp := gpbackup(gpbackupPath, backupHelperPath)
		gprestore(gprestorePath, restoreHelperPath, timestamp,
			"--redirect-db", "restoredb")

		assertRelationsCreated(restoreConn, TOTAL_RELATIONS+1) // for new table
		assertDataRestored(restoreConn, schema2TupleCounts)
		assertDataRestored(restoreConn, map[string]int{
			"public.foo":                      40000,
			"public.holds":                    50000,
			"public.sales":                    13,
			"public.test_depends_on_function": 2})
		assertArtifactsCleaned(restoreConn, timestamp)
	})
	It("runs gpbackup and gprestore to backup functions depending on tables", func() {
		skipIfOldBackupVersionBefore("1.19.0")

		testhelper.AssertQueryRuns(backupConn, "CREATE TABLE to_use_for_function (n int);")
		defer testhelper.AssertQueryRuns(backupConn, "DROP TABLE to_use_for_function;")

		testhelper.AssertQueryRuns(backupConn, "INSERT INTO  to_use_for_function values (1);")
		testhelper.AssertQueryRuns(backupConn, "CREATE FUNCTION func1(val integer) RETURNS integer AS $$ BEGIN RETURN val + (SELECT n FROM to_use_for_function); END; $$ LANGUAGE PLPGSQL;;")

		defer testhelper.AssertQueryRuns(backupConn, "DROP FUNCTION func1(val integer);")

		testhelper.AssertQueryRuns(backupConn, "CREATE TABLE test_depends_on_function (id integer, claim_id character varying(20) DEFAULT ('WC-'::text || func1(10)::text)) DISTRIBUTED BY (id);")
		defer testhelper.AssertQueryRuns(backupConn, "DROP TABLE test_depends_on_function;")
		testhelper.AssertQueryRuns(backupConn, "INSERT INTO  test_depends_on_function values (1);")

		timestamp := gpbackup(gpbackupPath, backupHelperPath)
		gprestore(gprestorePath, restoreHelperPath, timestamp,
			"--redirect-db", "restoredb")

		assertRelationsCreated(restoreConn, TOTAL_RELATIONS+2) // for 2 new tables
		assertDataRestored(restoreConn, schema2TupleCounts)
		assertDataRestored(restoreConn, map[string]int{
			"public.foo":                      40000,
			"public.holds":                    50000,
			"public.sales":                    13,
			"public.to_use_for_function":      1,
			"public.test_depends_on_function": 1})

		assertArtifactsCleaned(restoreConn, timestamp)
	})
	It("runs gprestore with --include-schema and --exclude-table flag", func() {
		timestamp := gpbackup(gpbackupPath, backupHelperPath,
			"--metadata-only")
		gprestore(gprestorePath, restoreHelperPath, timestamp,
			"--redirect-db", "restoredb",
			"--include-schema", "schema2",
			"--exclude-table", "schema2.returns",
			"--metadata-only")
		assertRelationsCreated(restoreConn, 4)
	})
	It("runs gprestore with jobs flag and postdata has metadata", func() {
		if useOldBackupVersion {
			Skip("This test is not needed for old backup versions")
		}

		if backupConn.Version.Before("6") {
			testhelper.AssertQueryRuns(backupConn, "CREATE TABLESPACE test_tablespace FILESPACE test_dir")
		} else {
			testhelper.AssertQueryRuns(backupConn, "CREATE TABLESPACE test_tablespace LOCATION '/tmp/test_dir';")
		}
		defer testhelper.AssertQueryRuns(backupConn, "DROP TABLESPACE test_tablespace;")

		// Store everything in this test schema for easy test cleanup.
		testhelper.AssertQueryRuns(backupConn, "CREATE SCHEMA postdata_metadata;")
		defer testhelper.AssertQueryRuns(backupConn, "DROP SCHEMA postdata_metadata CASCADE;")
		defer testhelper.AssertQueryRuns(restoreConn, "DROP SCHEMA postdata_metadata CASCADE;")

		// Create a table and indexes. Currently for indexes, there are 4 possible pieces
		// of metadata: TABLESPACE, CLUSTER, REPLICA IDENTITY, and COMMENT.
		testhelper.AssertQueryRuns(backupConn, "CREATE TABLE postdata_metadata.foobar (a int NOT NULL);")
		testhelper.AssertQueryRuns(backupConn, "CREATE INDEX fooidx1 ON postdata_metadata.foobar USING btree(a) TABLESPACE test_tablespace;")
		testhelper.AssertQueryRuns(backupConn, "CREATE INDEX fooidx2 ON postdata_metadata.foobar USING btree(a) TABLESPACE test_tablespace;")
		testhelper.AssertQueryRuns(backupConn, "CREATE UNIQUE INDEX fooidx3 ON postdata_metadata.foobar USING btree(a) TABLESPACE test_tablespace;")
		testhelper.AssertQueryRuns(backupConn, "COMMENT ON INDEX postdata_metadata.fooidx1 IS 'hello';")
		testhelper.AssertQueryRuns(backupConn, "COMMENT ON INDEX postdata_metadata.fooidx2 IS 'hello';")
		testhelper.AssertQueryRuns(backupConn, "COMMENT ON INDEX postdata_metadata.fooidx3 IS 'hello';")
		testhelper.AssertQueryRuns(backupConn, "ALTER TABLE postdata_metadata.foobar CLUSTER ON fooidx3;")
		if backupConn.Version.AtLeast("6") {
			testhelper.AssertQueryRuns(backupConn, "ALTER TABLE postdata_metadata.foobar REPLICA IDENTITY USING INDEX fooidx3")
		}

		// Create a rule. Currently for rules, the only metadata is COMMENT.
		testhelper.AssertQueryRuns(backupConn, "CREATE RULE postdata_rule AS ON UPDATE TO postdata_metadata.foobar DO SELECT * FROM postdata_metadata.foobar;")
		testhelper.AssertQueryRuns(backupConn, "COMMENT ON RULE postdata_rule IS 'hello';")

		// Create a trigger. Currently for triggers, the only metadata is COMMENT.
		testhelper.AssertQueryRuns(backupConn, `CREATE TRIGGER postdata_trigger AFTER INSERT OR DELETE OR UPDATE ON postdata_metadata.foobar FOR EACH STATEMENT EXECUTE PROCEDURE pg_catalog."RI_FKey_check_ins"();`)
		testhelper.AssertQueryRuns(backupConn, "COMMENT ON TRIGGER postdata_trigger ON postdata_metadata.foobar IS 'hello';")

		// Create an event trigger. Currently for event triggers, there are 2 possible
		// pieces of metadata: ENABLE and COMMENT.
		if backupConn.Version.AtLeast("6") {
			testhelper.AssertQueryRuns(backupConn, "CREATE OR REPLACE FUNCTION postdata_metadata.postdata_eventtrigger_func() RETURNS event_trigger AS $$ BEGIN END $$ LANGUAGE plpgsql;")
			testhelper.AssertQueryRuns(backupConn, "CREATE EVENT TRIGGER postdata_eventtrigger ON sql_drop EXECUTE PROCEDURE postdata_metadata.postdata_eventtrigger_func();")
			testhelper.AssertQueryRuns(backupConn, "ALTER EVENT TRIGGER postdata_eventtrigger DISABLE;")
			testhelper.AssertQueryRuns(backupConn, "COMMENT ON EVENT TRIGGER postdata_eventtrigger IS 'hello'")
		}

		timestamp := gpbackup(gpbackupPath, backupHelperPath,
			"--metadata-only")
		output := gprestore(gprestorePath, restoreHelperPath, timestamp,
			"--redirect-db", "restoredb", "--jobs", "8", "--verbose")

		// The gprestore parallel postdata restore should have succeeded without a CRITICAL error.
		stdout := string(output)
		Expect(stdout).To(Not(ContainSubstring("CRITICAL")))
		Expect(stdout).To(Not(ContainSubstring("Error encountered when executing statement")))
	})
	It("Can restore xml with xmloption set to document", func() {
		testutils.SkipIfBefore6(backupConn)
		// Set up the XML table that contains XML content
		testhelper.AssertQueryRuns(backupConn, "CREATE TABLE xml_test AS SELECT xml 'fooxml'")
		defer testhelper.AssertQueryRuns(backupConn, "DROP TABLE xml_test")

		// Set up database that has xmloption default to document instead of content
		testhelper.AssertQueryRuns(backupConn, "CREATE DATABASE document_db")
		defer testhelper.AssertQueryRuns(backupConn, "DROP DATABASE document_db")
		testhelper.AssertQueryRuns(backupConn, "ALTER DATABASE document_db SET xmloption TO document")

		timestamp := gpbackup(gpbackupPath, backupHelperPath, "--include-table", "public.xml_test")

		gprestore(gprestorePath, restoreHelperPath, timestamp,
			"--redirect-db", "document_db")
	})
	It("does not hold lock on gp_segment_configuration when backup is in progress", func() {
		if useOldBackupVersion {
			Skip("This test is not needed for old backup versions")
		}
		// Block on pg_trigger, which gpbackup queries after gp_segment_configuration
		backupConn.MustExec("BEGIN; LOCK TABLE pg_trigger IN ACCESS EXCLUSIVE MODE")

		args := []string{
			"--dbname", "testdb",
			"--backup-dir", backupDir,
			"--verbose"}
		cmd := exec.Command(gpbackupPath, args...)

		backupConn.MustExec("COMMIT")
		anotherConn := testutils.SetupTestDbConn("testdb")
		defer anotherConn.Close()
		var lockCount int
		go func() {
			gpSegConfigQuery := `SELECT * FROM pg_locks l, pg_class c, pg_namespace n WHERE l.relation = c.oid AND n.oid = c.relnamespace AND c.relname = 'gp_segment_configuration';`
			_ = anotherConn.Get(&lockCount, gpSegConfigQuery)
		}()

		Expect(lockCount).To(Equal(0))

		output, _ := cmd.CombinedOutput()
		stdout := string(output)
		Expect(stdout).To(ContainSubstring("Backup completed successfully"))
	})
})
