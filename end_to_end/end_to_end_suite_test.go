package end_to_end_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

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

func assertDataRestored(conn *utils.DBConn, tableToTupleCount map[string]int) {
	for name, numTuples := range tableToTupleCount {
		tupleCount := utils.SelectString(conn, fmt.Sprintf("SELECT count(*) AS string from %s", name))
		Expect(tupleCount).To(Equal(strconv.Itoa(numTuples)))
	}
}

func assertTablesCreated(conn *utils.DBConn, numTables int) {
	countQuery := `SELECT count(*) AS string FROM pg_tables WHERE schemaname IN ('public','schema2')`
	tableCount := utils.SelectString(conn, countQuery)
	Expect(tableCount).To(Equal(strconv.Itoa(numTables)))
}

func TestEndToEnd(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "EndToEnd Suite")
}

var _ = Describe("backup end to end integration tests", func() {

	var gpbackupPath, gprestorePath string
	var backupConn, restoreConn *utils.DBConn
	BeforeSuite(func() {
		var err error
		testutils.SetupTestLogger()
		gpbackupPath, err = gexec.Build("github.com/greenplum-db/gpbackup", "-tags", "gpbackup", "-ldflags", "-X github.com/greenplum-db/gpbackup/backup.version=0.5.0")
		Expect(err).ShouldNot(HaveOccurred())
		gprestorePath, err = gexec.Build("github.com/greenplum-db/gpbackup", "-tags", "gprestore", "-ldflags", "-X github.com/greenplum-db/gpbackup/restore.version=0.5.0")
		Expect(err).ShouldNot(HaveOccurred())
		exec.Command("dropdb", "testdb").Run()
		err = exec.Command("createdb", "testdb").Run()
		if err != nil {
			Fail(fmt.Sprintf("%v", err))
		}
		err = exec.Command("createdb", "restoredb").Run()
		if err != nil {
			Fail(fmt.Sprintf("%v", err))
		}
		backupConn = utils.NewDBConn("testdb")
		backupConn.Connect(1)
		restoreConn = utils.NewDBConn("restoredb")
		restoreConn.Connect(1)
		testutils.ExecuteSQLFile(backupConn, "test_tables.sql")
	})
	AfterSuite(func() {
		backupConn.Close()
		restoreConn.Close()
		gexec.CleanupBuildArtifacts()
		exec.Command("dropdb", "testdb").Run()
		exec.Command("dropdb", "restoredb").Run()
	})

	Describe("end to end gpbackup and gprestore tests", func() {
		publicSchemaTupleCounts := map[string]int{"public.foo": 40000, "public.holds": 50000, "public.sales": 13}
		schema2TupleCounts := map[string]int{"schema2.returns": 6, "schema2.foo2": 0, "schema2.foo3": 100}
		BeforeEach(func() {
			testutils.AssertQueryRuns(restoreConn, "DROP SCHEMA IF EXISTS schema2 CASCADE; DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
		})
		It("runs gpbackup and gprestore without redirecting restore to another db", func() {
			timestamp := gpbackup(gpbackupPath)
			backupConn.Close()
			err := exec.Command("dropdb", "testdb").Run()
			if err != nil {
				Fail(fmt.Sprintf("%v", err))
			}
			gprestore(gprestorePath, timestamp, "-createdb")
			backupConn = utils.NewDBConn("testdb")
			backupConn.Connect(1)
		})
		It("runs basic gpbackup and gprestore with metadata and data-only flags", func() {
			timestamp := gpbackup(gpbackupPath, "-metadata-only")
			timestamp2 := gpbackup(gpbackupPath, "-data-only")
			gprestore(gprestorePath, timestamp, "-redirect", "restoredb")
			assertDataRestored(restoreConn, map[string]int{"public.foo": 0, "schema2.foo3": 0})
			assertTablesCreated(restoreConn, 30)
			gprestore(gprestorePath, timestamp2, "-redirect", "restoredb")

			assertDataRestored(restoreConn, publicSchemaTupleCounts)
			assertDataRestored(restoreConn, schema2TupleCounts)
		})
		It("runs gpbackup and gprestore with exclude-schema flag", func() {
			timestamp := gpbackup(gpbackupPath, "-exclude-schema", "public")
			gprestore(gprestorePath, timestamp, "-redirect", "restoredb")

			assertTablesCreated(restoreConn, 15)
			assertDataRestored(restoreConn, schema2TupleCounts)
		})
		It("runs gpbackup and gprestore with include-schema flag and compression level", func() {
			timestamp := gpbackup(gpbackupPath, "-include-schema", "public", "-compression-level", "2")
			gprestore(gprestorePath, timestamp, "-redirect", "restoredb")

			assertTablesCreated(restoreConn, 15)
			assertDataRestored(restoreConn, publicSchemaTupleCounts)
		})
		It("runs gpbackup and gprestore with exclude-table-file flag", func() {
			excludeFile := utils.MustOpenFileForWriting("/tmp/exclude-tables.txt")
			utils.MustPrintln(excludeFile, "schema2.foo2\nschema2.foo3\nschema2.returns\npublic.sales")
			timestamp := gpbackup(gpbackupPath, "-exclude-table-file", "/tmp/exclude-tables.txt")
			gprestore(gprestorePath, timestamp, "-redirect", "restoredb")

			assertTablesCreated(restoreConn, 2)
			assertDataRestored(restoreConn, map[string]int{"public.foo": 40000, "public.holds": 50000})

			os.Remove("/tmp/exclude-tables.txt")
		})
		It("runs gpbackup and gprestore with include-table-file flag", func() {
			includeFile := utils.MustOpenFileForWriting("/tmp/include-tables.txt")
			utils.MustPrintln(includeFile, "public.sales\npublic.foo")
			timestamp := gpbackup(gpbackupPath, "-include-table-file", "/tmp/include-tables.txt")
			gprestore(gprestorePath, timestamp, "-redirect", "restoredb")

			assertTablesCreated(restoreConn, 14)
			assertDataRestored(restoreConn, map[string]int{"public.sales": 13, "public.foo": 40000})

			os.Remove("/tmp/include-tables.txt")
		})
		It("runs gpbackup and gprestore with leaf-partition-data and backupdir flags", func() {
			backupdir := "/tmp/leaf_partition_data"
			timestamp := gpbackup(gpbackupPath, "-leaf-partition-data", "-backupdir", backupdir)
			output := gprestore(gprestorePath, timestamp, "-redirect", "restoredb", "-backupdir", backupdir)
			Expect(strings.Contains(string(output), "Tables restored:  28 / 28")).To(BeTrue())

			assertDataRestored(restoreConn, publicSchemaTupleCounts)
			assertDataRestored(restoreConn, schema2TupleCounts)

			os.RemoveAll(backupdir)
		})
		It("runs gpbackup and gprestore with no-compression flag", func() {
			backupdir := "/tmp/no_compression"
			timestamp := gpbackup(gpbackupPath, "-no-compression", "-backupdir", backupdir)
			gprestore(gprestorePath, timestamp, "-redirect", "restoredb", "-backupdir", backupdir)
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
			timestamp := gpbackup(gpbackupPath, "-with-stats", "-backupdir", backupdir)
			files, _ := filepath.Glob(filepath.Join(backupdir, "*-1/backups/*", timestamp, "*statistics.sql"))

			Expect(len(files)).To(Equal(1))
			output := gprestore(gprestorePath, timestamp, "-redirect", "restoredb", "-with-stats", "-backupdir", backupdir)

			Expect(strings.Contains(string(output), "Query planner statistics restore complete")).To(BeTrue())
			assertDataRestored(restoreConn, publicSchemaTupleCounts)
			assertDataRestored(restoreConn, schema2TupleCounts)

			os.RemoveAll(backupdir)
		})
		It("runs gpbackup and gprestore with single-data-file flag", func() {
			backupdir := "/tmp/single_data_file"
			timestamp := gpbackup(gpbackupPath, "-single-data-file", "-backupdir", backupdir)
			gprestore(gprestorePath, timestamp, "-redirect", "restoredb", "-backupdir", backupdir)

			assertTablesCreated(restoreConn, 30)
			assertDataRestored(restoreConn, publicSchemaTupleCounts)
			assertDataRestored(restoreConn, schema2TupleCounts)

			os.RemoveAll(backupdir)
		})

		It("runs gpbackup and gprestore with single-data-file flag without compression", func() {
			backupdir := "/tmp/single_data_file"
			timestamp := gpbackup(gpbackupPath, "-single-data-file", "-backupdir", backupdir, "-no-compression")
			gprestore(gprestorePath, timestamp, "-redirect", "restoredb", "-backupdir", backupdir)

			os.RemoveAll(backupdir)
		})

		It("runs gpbackup and gprestore with include-table-file restore flag", func() {
			includeFile := utils.MustOpenFileForWriting("/tmp/include-tables.txt")
			utils.MustPrintln(includeFile, "public.sales\npublic.foo")
			backupdir := "/tmp/include_table_file"
			timestamp := gpbackup(gpbackupPath, "-backupdir", backupdir)
			gprestore(gprestorePath, timestamp, "-redirect", "restoredb", "-backupdir", backupdir, "-include-table-file", "/tmp/include-tables.txt")

			assertTablesCreated(restoreConn, 14)
			assertDataRestored(restoreConn, map[string]int{"public.sales": 13, "public.foo": 40000})

			os.RemoveAll(backupdir)
			os.Remove("/tmp/include-tables.txt")
		})

		It("runs gpbackup and gprestore with include-table-file restore flag with a single data file", func() {
			includeFile := utils.MustOpenFileForWriting("/tmp/include-tables.txt")
			utils.MustPrintln(includeFile, "public.sales\npublic.foo")
			backupdir := "/tmp/include_table_file"
			timestamp := gpbackup(gpbackupPath, "-backupdir", backupdir, "-single-data-file")
			gprestore(gprestorePath, timestamp, "-redirect", "restoredb", "-backupdir", backupdir, "-include-table-file", "/tmp/include-tables.txt")
			assertTablesCreated(restoreConn, 14)
			assertDataRestored(restoreConn, map[string]int{"public.sales": 13, "public.foo": 40000})

			os.RemoveAll(backupdir)
			os.Remove("/tmp/include-tables.txt")
		})

		It("runs gpbackup and gprestore with include-schema restore flag", func() {
			backupdir := "/tmp/include_schema"
			timestamp := gpbackup(gpbackupPath, "-backupdir", backupdir)
			gprestore(gprestorePath, timestamp, "-redirect", "restoredb", "-backupdir", backupdir, "-include-schema", "schema2")

			assertTablesCreated(restoreConn, 15)
			assertDataRestored(restoreConn, schema2TupleCounts)

			os.RemoveAll(backupdir)
		})
		It("runs gpbackup and gprestore with jobs flag", func() {
			backupdir := "/tmp/parallel"
			timestamp := gpbackup(gpbackupPath, "-backupdir", backupdir)
			gprestore(gprestorePath, timestamp, "-redirect", "restoredb", "-backupdir", backupdir, "-jobs", "4")

			assertTablesCreated(restoreConn, 30)
			assertDataRestored(restoreConn, schema2TupleCounts)
			assertDataRestored(restoreConn, publicSchemaTupleCounts)

			os.RemoveAll(backupdir)
		})
		It("runs gpbackup and gprestore with include-schema restore flag with a single data file", func() {
			backupdir := "/tmp/include_schema"
			timestamp := gpbackup(gpbackupPath, "-backupdir", backupdir, "-single-data-file")
			gprestore(gprestorePath, timestamp, "-redirect", "restoredb", "-backupdir", backupdir, "-include-schema", "schema2")

			assertTablesCreated(restoreConn, 15)
			assertDataRestored(restoreConn, schema2TupleCounts)

			os.RemoveAll(backupdir)
		})
		It("runs gpbackup and gprestore on database with all objects", func() {
			testutils.AssertQueryRuns(backupConn, "DROP SCHEMA IF EXISTS schema2 CASCADE; DROP SCHEMA public CASCADE; CREATE SCHEMA public; DROP PROCEDURAL LANGUAGE IF EXISTS plpythonu;")
			/* We do not check the error code since there are some objects we
			 * expect to fail during creation between DB versions
			 */
			exec.Command("psql", "-d", "testdb", "-f", "all_objects.sql", "-q")
			timestamp := gpbackup(gpbackupPath)
			gprestore(gprestorePath, timestamp, "-redirect", "restoredb")

			testutils.AssertQueryRuns(backupConn, "DROP SCHEMA IF EXISTS schema2 CASCADE; DROP SCHEMA public CASCADE; CREATE SCHEMA public; DROP PROCEDURAL LANGUAGE IF EXISTS plpythonu;")
			testutils.ExecuteSQLFile(backupConn, "test_tables.sql")
		})
	})
})
