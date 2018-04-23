package end_to_end_test

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/utils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"

	"testing"
)

/* This function is a helper function to execute gpbackup and return a session
 * to allow checking its output.
 */
func gpbackup(gpbackupPath string, args ...string) string {
	args = append([]string{"-dbname", "testdb"}, args...)
	command := exec.Command(gpbackupPath, args...)
	output, err := command.CombinedOutput()
	if err != nil {
		fmt.Printf("%s", output)
		Fail(fmt.Sprintf("%v", err))
	}
	r := regexp.MustCompile(`Backup Timestamp = (\d{14})`)
	return r.FindStringSubmatch(fmt.Sprintf("%s", output))[1]
}

func gprestore(gprestorePath string, timestamp string, args ...string) []byte {
	args = append([]string{"-timestamp", timestamp}, args...)
	command := exec.Command(gprestorePath, args...)
	output, err := command.CombinedOutput()
	if err != nil {
		fmt.Printf("%s", output)
		Fail(fmt.Sprintf("%v", err))
	}
	return output
}

func buildAndInstallBinaries() (string, string) {
	os.Chdir("..")
	command := exec.Command("make", "build")
	output, err := command.CombinedOutput()
	if err != nil {
		fmt.Printf("%s", output)
		Fail(fmt.Sprintf("%v", err))
	}
	os.Chdir("end_to_end")
	binDir := fmt.Sprintf("%s/go/bin", operating.System.Getenv("HOME"))
	return fmt.Sprintf("%s/gpbackup", binDir), fmt.Sprintf("%s/gprestore", binDir)
}

func assertDataRestored(conn *dbconn.DBConn, tableToTupleCount map[string]int) {
	for name, numTuples := range tableToTupleCount {
		tupleCount := dbconn.MustSelectString(conn, fmt.Sprintf("SELECT count(*) AS string from %s", name))
		Expect(tupleCount).To(Equal(strconv.Itoa(numTuples)))
	}
}

func assertTablesCreated(conn *dbconn.DBConn, numTables int) {
	countQuery := `SELECT count(*) AS string FROM pg_tables WHERE schemaname IN ('public','schema2')`
	tableCount := dbconn.MustSelectString(conn, countQuery)
	Expect(tableCount).To(Equal(strconv.Itoa(numTables)))
}

func copyPluginToAllHosts(conn *dbconn.DBConn, pluginPath string) {
	hostnameQuery := `SELECT DISTINCT hostname AS string FROM gp_segment_configuration WHERE content != -1`
	hostnames := dbconn.MustSelectStringSlice(conn, hostnameQuery)
	for _, hostname := range hostnames {
		pluginDir, _ := filepath.Split(pluginPath)
		output, err := exec.Command("ssh", hostname, fmt.Sprintf("mkdir -p %s", pluginDir)).CombinedOutput()
		if err != nil {
			fmt.Printf("%s", output)
			Fail(fmt.Sprintf("%v", err))
		}
		output, err = exec.Command("scp", pluginPath, fmt.Sprintf("%s:%s", hostname, pluginPath)).CombinedOutput()
		if err != nil {
			fmt.Printf("%s", output)
			Fail(fmt.Sprintf("%v", err))
		}
	}
}

func TestEndToEnd(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "EndToEnd Suite")
}

var _ = Describe("backup end to end integration tests", func() {

	var backupConn, restoreConn *dbconn.DBConn
	var gpbackupPath, gprestorePath string
	BeforeSuite(func() {
		var err error
		testhelper.SetupTestLogger()
		exec.Command("dropdb", "testdb").Run()
		err = exec.Command("createdb", "testdb").Run()
		if err != nil {
			Fail(fmt.Sprintf("Could not create testdb: %v", err))
		}
		err = exec.Command("createdb", "restoredb").Run()
		if err != nil {
			Fail(fmt.Sprintf("Could not create restoredb: %v", err))
		}
		backupConn = dbconn.NewDBConnFromEnvironment("testdb")
		backupConn.MustConnect(1)
		restoreConn = dbconn.NewDBConnFromEnvironment("restoredb")
		restoreConn.MustConnect(1)
		testutils.ExecuteSQLFile(backupConn, "test_tables.sql")
		gpbackupPath, gprestorePath = buildAndInstallBinaries()
	})
	AfterSuite(func() {
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
		publicSchemaTupleCounts := map[string]int{"public.foo": 40000, "public.holds": 50000, "public.sales": 13}
		schema2TupleCounts := map[string]int{"schema2.returns": 6, "schema2.foo2": 0, "schema2.foo3": 100}
		BeforeEach(func() {
			testhelper.AssertQueryRuns(restoreConn, "DROP SCHEMA IF EXISTS schema2 CASCADE; DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
		})
		It("runs gpbackup and gprestore without redirecting restore to another db", func() {
			timestamp := gpbackup(gpbackupPath)
			backupConn.Close()
			err := exec.Command("dropdb", "testdb").Run()
			if err != nil {
				Fail(fmt.Sprintf("%v", err))
			}
			gprestore(gprestorePath, timestamp, "-create-db")
			backupConn = dbconn.NewDBConnFromEnvironment("testdb")
			backupConn.MustConnect(1)
		})
		It("runs basic gpbackup and gprestore with metadata and data-only flags", func() {
			timestamp := gpbackup(gpbackupPath, "-metadata-only")
			timestamp2 := gpbackup(gpbackupPath, "-data-only")
			gprestore(gprestorePath, timestamp, "-redirect-db", "restoredb")
			assertDataRestored(restoreConn, map[string]int{"public.foo": 0, "schema2.foo3": 0})
			assertTablesCreated(restoreConn, 30)
			gprestore(gprestorePath, timestamp2, "-redirect-db", "restoredb")

			assertDataRestored(restoreConn, publicSchemaTupleCounts)
			assertDataRestored(restoreConn, schema2TupleCounts)
		})
		It("runs gpbackup and gprestore with exclude-schema backup flag", func() {
			timestamp := gpbackup(gpbackupPath, "-exclude-schema", "public")
			gprestore(gprestorePath, timestamp, "-redirect-db", "restoredb")

			assertTablesCreated(restoreConn, 15)
			assertDataRestored(restoreConn, schema2TupleCounts)
		})
		It("runs gpbackup and gprestore with exclude-schema restore flag", func() {
			timestamp := gpbackup(gpbackupPath)
			gprestore(gprestorePath, timestamp, "-redirect-db", "restoredb", "-exclude-schema", "public")

			assertTablesCreated(restoreConn, 15)
			assertDataRestored(restoreConn, schema2TupleCounts)
		})
		It("runs gpbackup and gprestore with include-schema flag and compression level", func() {
			timestamp := gpbackup(gpbackupPath, "-include-schema", "public", "-compression-level", "2")
			gprestore(gprestorePath, timestamp, "-redirect-db", "restoredb")

			assertTablesCreated(restoreConn, 15)
			assertDataRestored(restoreConn, publicSchemaTupleCounts)
		})
		It("runs gpbackup and gprestore with exclude-table backup flag", func() {
			timestamp := gpbackup(gpbackupPath, "-exclude-table", "schema2.foo2", "-exclude-table", "schema2.returns")
			gprestore(gprestorePath, timestamp, "-redirect-db", "restoredb")

			assertTablesCreated(restoreConn, 16)
			assertDataRestored(restoreConn, map[string]int{"schema2.foo3": 100, "public.foo": 40000, "public.holds": 50000, "public.sales": 13})

			os.Remove("/tmp/exclude-tables.txt")
		})
		It("runs gpbackup and gprestore with include-table backup flag", func() {
			timestamp := gpbackup(gpbackupPath, "-include-table", "public.foo", "-include-table", "public.sales")
			gprestore(gprestorePath, timestamp, "-redirect-db", "restoredb")

			assertTablesCreated(restoreConn, 14)
			assertDataRestored(restoreConn, map[string]int{"public.sales": 13, "public.foo": 40000})

			os.Remove("/tmp/include-tables.txt")
		})
		It("runs gpbackup and gprestore with exclude-table restore flag", func() {
			timestamp := gpbackup(gpbackupPath)
			gprestore(gprestorePath, timestamp, "-redirect-db", "restoredb", "-exclude-table", "schema2.foo2", "-exclude-table", "schema2.returns")

			assertTablesCreated(restoreConn, 16)
			assertDataRestored(restoreConn, map[string]int{"schema2.foo3": 100, "public.foo": 40000, "public.holds": 50000, "public.sales": 13})

			os.Remove("/tmp/exclude-tables.txt")
		})
		It("runs gpbackup and gprestore with include-table restore flag", func() {
			timestamp := gpbackup(gpbackupPath)
			gprestore(gprestorePath, timestamp, "-redirect-db", "restoredb", "-include-table", "public.foo", "-include-table", "public.sales")

			assertTablesCreated(restoreConn, 14)
			assertDataRestored(restoreConn, map[string]int{"public.sales": 13, "public.foo": 40000})

			os.Remove("/tmp/include-tables.txt")
		})
		It("runs gpbackup and gprestore with exclude-table-file flag", func() {
			excludeFile := utils.MustOpenFileForWriting("/tmp/exclude-tables.txt")
			utils.MustPrintln(excludeFile, "schema2.foo2\nschema2.returns\npublic.sales")
			timestamp := gpbackup(gpbackupPath, "-exclude-table-file", "/tmp/exclude-tables.txt")
			gprestore(gprestorePath, timestamp, "-redirect-db", "restoredb")

			assertTablesCreated(restoreConn, 3)
			assertDataRestored(restoreConn, map[string]int{"schema2.foo3": 100, "public.foo": 40000, "public.holds": 50000})

			os.Remove("/tmp/exclude-tables.txt")
		})
		It("runs gpbackup and gprestore with include-table-file backup flag", func() {
			includeFile := utils.MustOpenFileForWriting("/tmp/include-tables.txt")
			utils.MustPrintln(includeFile, "public.sales\npublic.foo")
			timestamp := gpbackup(gpbackupPath, "-include-table-file", "/tmp/include-tables.txt")
			gprestore(gprestorePath, timestamp, "-redirect-db", "restoredb")

			assertTablesCreated(restoreConn, 14)
			assertDataRestored(restoreConn, map[string]int{"public.sales": 13, "public.foo": 40000})

			os.Remove("/tmp/include-tables.txt")
		})
		It("runs gpbackup and gprestore with leaf-partition-data and backupdir flags", func() {
			backupdir := "/tmp/leaf_partition_data"
			timestamp := gpbackup(gpbackupPath, "-leaf-partition-data", "-backup-dir", backupdir)
			output := gprestore(gprestorePath, timestamp, "-redirect-db", "restoredb", "-backup-dir", backupdir)
			Expect(strings.Contains(string(output), "Tables restored:  28 / 28")).To(BeTrue())

			assertDataRestored(restoreConn, publicSchemaTupleCounts)
			assertDataRestored(restoreConn, schema2TupleCounts)

			os.RemoveAll(backupdir)
		})
		It("runs gpbackup and gprestore with no-compression flag", func() {
			backupdir := "/tmp/no_compression"
			timestamp := gpbackup(gpbackupPath, "-no-compression", "-backup-dir", backupdir)
			gprestore(gprestorePath, timestamp, "-redirect-db", "restoredb", "-backup-dir", backupdir)
			configFile, _ := filepath.Glob(filepath.Join(backupdir, "*-1/backups/*", timestamp, "*config.yaml"))
			contents, _ := ioutil.ReadFile(configFile[0])

			Expect(strings.Contains(string(contents), "compressed: false")).To(BeTrue())
			assertTablesCreated(restoreConn, 30)
			assertDataRestored(restoreConn, publicSchemaTupleCounts)
			assertDataRestored(restoreConn, schema2TupleCounts)

			os.RemoveAll(backupdir)
		})
		It("runs gpbackup and gprestore with with-stats flag", func() {
			backupdir := "/tmp/with_stats"
			timestamp := gpbackup(gpbackupPath, "-with-stats", "-backup-dir", backupdir)
			files, _ := filepath.Glob(filepath.Join(backupdir, "*-1/backups/*", timestamp, "*statistics.sql"))

			Expect(len(files)).To(Equal(1))
			output := gprestore(gprestorePath, timestamp, "-redirect-db", "restoredb", "-with-stats", "-backup-dir", backupdir)

			Expect(strings.Contains(string(output), "Query planner statistics restore complete")).To(BeTrue())
			assertDataRestored(restoreConn, publicSchemaTupleCounts)
			assertDataRestored(restoreConn, schema2TupleCounts)

			os.RemoveAll(backupdir)
		})
		It("runs gpbackup and gprestore with single-data-file flag", func() {
			backupdir := "/tmp/single_data_file"
			timestamp := gpbackup(gpbackupPath, "-single-data-file", "-backup-dir", backupdir)
			gprestore(gprestorePath, timestamp, "-redirect-db", "restoredb", "-backup-dir", backupdir)

			assertTablesCreated(restoreConn, 30)
			assertDataRestored(restoreConn, publicSchemaTupleCounts)
			assertDataRestored(restoreConn, schema2TupleCounts)

			os.RemoveAll(backupdir)
		})

		It("runs gpbackup and gprestore with single-data-file flag without compression", func() {
			backupdir := "/tmp/single_data_file"
			timestamp := gpbackup(gpbackupPath, "-single-data-file", "-backup-dir", backupdir, "-no-compression")
			gprestore(gprestorePath, timestamp, "-redirect-db", "restoredb", "-backup-dir", backupdir)

			assertTablesCreated(restoreConn, 30)
			assertDataRestored(restoreConn, publicSchemaTupleCounts)
			assertDataRestored(restoreConn, schema2TupleCounts)

			os.RemoveAll(backupdir)
		})

		It("runs gpbackup and gprestore with plugin, single-data-file, and no-compression", func() {
			pluginDir := "/tmp/plugin_dest"
			pluginExecutablePath := fmt.Sprintf("%s/go/src/github.com/greenplum-db/gpbackup/plugins/example_plugin.sh", os.Getenv("HOME"))
			copyPluginToAllHosts(backupConn, pluginExecutablePath)
			pluginConfigPath := fmt.Sprintf("%s/go/src/github.com/greenplum-db/gpbackup/plugins/example_plugin_config.yaml", os.Getenv("HOME"))

			timestamp := gpbackup(gpbackupPath, "-single-data-file", "-no-compression", "-plugin-config", pluginConfigPath)
			gprestore(gprestorePath, timestamp, "-redirect-db", "restoredb", "-plugin-config", pluginConfigPath)

			assertTablesCreated(restoreConn, 30)
			assertDataRestored(restoreConn, publicSchemaTupleCounts)
			assertDataRestored(restoreConn, schema2TupleCounts)

			os.RemoveAll(pluginDir)
		})

		It("runs gpbackup and gprestore with plugin and single-data-file", func() {
			pluginDir := "/tmp/plugin_dest"
			pluginExecutablePath := fmt.Sprintf("%s/go/src/github.com/greenplum-db/gpbackup/plugins/example_plugin.sh", os.Getenv("HOME"))
			copyPluginToAllHosts(backupConn, pluginExecutablePath)
			pluginConfigPath := fmt.Sprintf("%s/go/src/github.com/greenplum-db/gpbackup/plugins/example_plugin_config.yaml", os.Getenv("HOME"))

			timestamp := gpbackup(gpbackupPath, "-single-data-file", "-plugin-config", pluginConfigPath)
			gprestore(gprestorePath, timestamp, "-redirect-db", "restoredb", "-plugin-config", pluginConfigPath)

			assertTablesCreated(restoreConn, 30)
			assertDataRestored(restoreConn, publicSchemaTupleCounts)
			assertDataRestored(restoreConn, schema2TupleCounts)

			os.RemoveAll(pluginDir)
		})

		It("runs gpbackup and gprestore with include-table-file restore flag", func() {
			includeFile := utils.MustOpenFileForWriting("/tmp/include-tables.txt")
			utils.MustPrintln(includeFile, "public.sales\npublic.foo")
			backupdir := "/tmp/include_table_file"
			timestamp := gpbackup(gpbackupPath, "-backup-dir", backupdir)
			gprestore(gprestorePath, timestamp, "-redirect-db", "restoredb", "-backup-dir", backupdir, "-include-table-file", "/tmp/include-tables.txt")

			assertTablesCreated(restoreConn, 14)
			assertDataRestored(restoreConn, map[string]int{"public.sales": 13, "public.foo": 40000})

			os.RemoveAll(backupdir)
			os.Remove("/tmp/include-tables.txt")
		})

		It("runs gpbackup and gprestore with include-table-file restore flag with a single data file", func() {
			includeFile := utils.MustOpenFileForWriting("/tmp/include-tables.txt")
			utils.MustPrintln(includeFile, "public.sales\npublic.foo")
			backupdir := "/tmp/include_table_file"
			timestamp := gpbackup(gpbackupPath, "-backup-dir", backupdir, "-single-data-file")
			gprestore(gprestorePath, timestamp, "-redirect-db", "restoredb", "-backup-dir", backupdir, "-include-table-file", "/tmp/include-tables.txt")
			assertTablesCreated(restoreConn, 14)
			assertDataRestored(restoreConn, map[string]int{"public.sales": 13, "public.foo": 40000})

			os.RemoveAll(backupdir)
			os.Remove("/tmp/include-tables.txt")
		})

		It("runs gpbackup and gprestore with include-schema restore flag", func() {
			backupdir := "/tmp/include_schema"
			timestamp := gpbackup(gpbackupPath, "-backup-dir", backupdir)
			gprestore(gprestorePath, timestamp, "-redirect-db", "restoredb", "-backup-dir", backupdir, "-include-schema", "schema2")

			assertTablesCreated(restoreConn, 15)
			assertDataRestored(restoreConn, schema2TupleCounts)

			os.RemoveAll(backupdir)
		})
		It("runs gpbackup and gprestore with jobs flag", func() {
			backupdir := "/tmp/parallel"
			timestamp := gpbackup(gpbackupPath, "-backup-dir", backupdir)
			gprestore(gprestorePath, timestamp, "-redirect-db", "restoredb", "-backup-dir", backupdir, "-jobs", "4")

			assertTablesCreated(restoreConn, 30)
			assertDataRestored(restoreConn, schema2TupleCounts)
			assertDataRestored(restoreConn, publicSchemaTupleCounts)

			os.RemoveAll(backupdir)
		})
		It("runs gpbackup and gprestore with include-schema restore flag with a single data file", func() {
			backupdir := "/tmp/include_schema"
			timestamp := gpbackup(gpbackupPath, "-backup-dir", backupdir, "-single-data-file")
			gprestore(gprestorePath, timestamp, "-redirect-db", "restoredb", "-backup-dir", backupdir, "-include-schema", "schema2")

			assertTablesCreated(restoreConn, 15)
			assertDataRestored(restoreConn, schema2TupleCounts)

			os.RemoveAll(backupdir)
		})
		It("runs gpbackup and gprestore on database with all objects", func() {
			testhelper.AssertQueryRuns(backupConn, "DROP SCHEMA IF EXISTS schema2 CASCADE; DROP SCHEMA public CASCADE; CREATE SCHEMA public; DROP PROCEDURAL LANGUAGE IF EXISTS plpythonu;")
			defer testutils.ExecuteSQLFile(backupConn, "test_tables.sql")
			defer testhelper.AssertQueryRuns(backupConn, "DROP SCHEMA IF EXISTS schema2 CASCADE; DROP SCHEMA public CASCADE; CREATE SCHEMA public; DROP PROCEDURAL LANGUAGE IF EXISTS plpythonu;")
			testutils.ExecuteSQLFile(backupConn, "gpdb4_objects.sql")
			if backupConn.Version.AtLeast("5") {
				testutils.ExecuteSQLFile(backupConn, "gpdb5_objects.sql")
			}
			if backupConn.Version.AtLeast("6") {
				testutils.ExecuteSQLFile(backupConn, "gpdb6_objects.sql")
			}
			timestamp := gpbackup(gpbackupPath, "-leaf-partition-data")
			gprestore(gprestorePath, timestamp, "-redirect-db", "restoredb")

		})
		It("runs gpbackup and sends a SIGINT to ensure cleanup functions successfully", func() {
			backupdir := "/tmp/signals"
			args := []string{"-dbname", "testdb", "-backup-dir", backupdir, "-single-data-file", "-verbose"}
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
			backupdir := "/tmp/signals"
			timestamp := gpbackup(gpbackupPath, "-backup-dir", backupdir, "-single-data-file")
			args := []string{"-timestamp", timestamp, "-redirect-db", "restoredb", "-backup-dir", backupdir, "-include-schema", "schema2", "-verbose"}
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

			os.RemoveAll(backupdir)
		})

		It("runs example_plugin.sh with plugin_test_bench", func() {
			pluginsDir := fmt.Sprintf("%s/go/src/github.com/greenplum-db/gpbackup/plugins", os.Getenv("HOME"))
			copyPluginToAllHosts(backupConn, fmt.Sprintf("%s/example_plugin.sh", pluginsDir))
			output, err := exec.Command("bash", "-c", fmt.Sprintf("%s/plugin_test_bench.sh %s/example_plugin.sh %s/example_plugin_config.yaml", pluginsDir, pluginsDir, pluginsDir)).CombinedOutput()
			if err != nil {
				fmt.Printf("%s", output)
				Fail(fmt.Sprintf("%v", err))
			}

			os.RemoveAll("/tmp/plugin_dest")
		})

	})
})
