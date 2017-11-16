package end_to_end_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/greenplum-db/gpbackup/backup"
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
		backupConn = utils.NewDBConn("testdb")
		backupConn.Connect()
		utils.ExecuteSQLFile(backupConn, "test_tables.sql")
	})
	AfterSuite(func() {
		backupConn.Close()
		restoreConn.Close()
		gexec.CleanupBuildArtifacts()
		exec.Command("dropdb", "testdb").Run()
		exec.Command("dropdb", "restoredb").Run()
	})

	Describe("end to end gpbackup and gprestore tests", func() {
		countQuery := `SELECT count(*) AS string FROM pg_tables WHERE schemaname IN ('public','schema2')`
		BeforeEach(func() {
			if restoreConn != nil {
				restoreConn.Close()
			}
			exec.Command("dropdb", "restoredb").Run()
			err := exec.Command("createdb", "restoredb").Run()
			if err != nil {
				Fail(fmt.Sprintf("%v", err))
			}
			restoreConn = utils.NewDBConn("restoredb")
			restoreConn.Connect()
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
			backupConn.Connect()
		})
		It("runs basic gpbackup and gprestore with metadata and data-only flags", func() {
			timestamp := gpbackup(gpbackupPath, "-metadata-only")
			timestamp2 := gpbackup(gpbackupPath, "-data-only")
			gprestore(gprestorePath, timestamp, "-redirect", "restoredb")
			gprestore(gprestorePath, timestamp2, "-redirect", "restoredb")
		})
		It("runs gpbackup and gprestore with exclude-schema flag", func() {
			timestamp := gpbackup(gpbackupPath, "-exclude-schema", "public")
			gprestore(gprestorePath, timestamp, "-redirect", "restoredb")
			tableCount := backup.SelectString(restoreConn, countQuery)
			Expect(tableCount).To(Equal("14"))
		})
		It("runs gpbackup and gprestore with include-schema flag", func() {
			timestamp := gpbackup(gpbackupPath, "-include-schema", "public")
			gprestore(gprestorePath, timestamp, "-redirect", "restoredb")
			tableCount := backup.SelectString(restoreConn, countQuery)
			Expect(tableCount).To(Equal("15"))
		})
		It("runs gpbackup and gprestore with exclude-table-file flag", func() {
			excludeFile := utils.MustOpenFileForWriting("/tmp/exclude-tables.txt")
			utils.MustPrintln(excludeFile, "schema2.foo2\nschema2.returns\npublic.sales")
			timestamp := gpbackup(gpbackupPath, "-exclude-table-file", "/tmp/exclude-tables.txt")
			gprestore(gprestorePath, timestamp, "-redirect", "restoredb")
			tableCount := backup.SelectString(restoreConn, countQuery)
			Expect(tableCount).To(Equal("2"))
			os.Remove("/tmp/exclude-tables.txt")
		})
		It("runs gpbackup and gprestore with include-table-file flag", func() {
			includeFile := utils.MustOpenFileForWriting("/tmp/include-tables.txt")
			utils.MustPrintln(includeFile, "public.sales")
			timestamp := gpbackup(gpbackupPath, "-include-table-file", "/tmp/include-tables.txt")
			gprestore(gprestorePath, timestamp, "-redirect", "restoredb")
			tableCount := backup.SelectString(restoreConn, countQuery)
			Expect(tableCount).To(Equal("13"))
			os.Remove("/tmp/include-tables.txt")
		})
		It("runs gpbackup and gprestore with leaf-partition-data and backupdir flags", func() {
			backupdir := "/tmp/leaf_partition_data"
			timestamp := gpbackup(gpbackupPath, "-leaf-partition-data", "-backupdir", backupdir)
			output := gprestore(gprestorePath, timestamp, "-redirect", "restoredb", "-backupdir", backupdir)
			Expect(strings.Contains(string(output), "Tables restored:  27 / 27")).To(BeTrue())
			os.RemoveAll(backupdir)
		})
		It("runs gpbackup and gprestore with no-compression flag", func() {
			backupdir := "/tmp/no_compression"
			timestamp := gpbackup(gpbackupPath, "-no-compression", "-backupdir", backupdir)
			gprestore(gprestorePath, timestamp, "-redirect", "restoredb", "-backupdir", backupdir)
			configFile, _ := filepath.Glob(filepath.Join(backupdir, "*-1/backups/*", timestamp, "*config.yaml"))
			contents, _ := ioutil.ReadFile(configFile[0])

			Expect(strings.Contains(string(contents), "compressed: false")).To(BeTrue())
			os.RemoveAll(backupdir)
		})
		It("runs gpbackup and gprestore with with-stats flag", func() {
			backupdir := "/tmp/with_stats"
			timestamp := gpbackup(gpbackupPath, "-with-stats", "-backupdir", backupdir)
			files, _ := filepath.Glob(filepath.Join(backupdir, "*-1/backups/*", timestamp, "*statistics.sql"))

			Expect(len(files)).To(Equal(1))
			output := gprestore(gprestorePath, timestamp, "-redirect", "restoredb", "-with-stats", "-backupdir", backupdir)

			Expect(strings.Contains(string(output), "Query planner statistics restore complete")).To(BeTrue())
			os.RemoveAll(backupdir)
		})
		It("runs gpbackup and gprestore on database with all objects", func() {
			backupConn.Close()
			exec.Command("dropdb", "testdb").Run()
			err := exec.Command("createdb", "testdb").Run()
			if err != nil {
				Fail(fmt.Sprintf("%v", err))
			}
			connStr := []string{
				"-d", "testdb",
				"-f", "all_objects.sql",
				"-q",
			}
			exec.Command("psql", connStr...)

			timestamp := gpbackup(gpbackupPath)
			gprestore(gprestorePath, timestamp, "-redirect", "restoredb")

			exec.Command("dropdb", "testdb").Run()
			err = exec.Command("createdb", "testdb").Run()
			if err != nil {
				Fail(fmt.Sprintf("%v", err))
			}
			backupConn = utils.NewDBConn("testdb")
			backupConn.Connect()
			utils.ExecuteSQLFile(backupConn, "test_tables.sql")
		})
		It("runs gpbackup and gprestore on a database with a 3 level partition table with an external partition", func() {
			testutils.AssertQueryRuns(backupConn, `
CREATE TABLE part_tbl (id int, year int, month int, day int, region text)
DISTRIBUTED BY (id)
PARTITION BY RANGE (year)
    SUBPARTITION BY RANGE (month)
       SUBPARTITION TEMPLATE (
        START (1) END (13) EVERY (1),
        DEFAULT SUBPARTITION other_months )
           SUBPARTITION BY LIST (region)
             SUBPARTITION TEMPLATE (
               SUBPARTITION usa VALUES ('usa'),
               SUBPARTITION europe VALUES ('europe'),
               SUBPARTITION asia VALUES ('asia'),
               DEFAULT SUBPARTITION other_regions )
( START (2002) END (2012) EVERY (1),
  DEFAULT PARTITION outlying_years );
`)
			testutils.AssertQueryRuns(backupConn, `CREATE EXTERNAL TABLE part_tbl_ext_part_ (like part_tbl_1_prt_10_2_prt_5_3_prt_europe) LOCATION ('gpfdist://127.0.0.1/apj') FORMAT 'text';`)
			testutils.AssertQueryRuns(backupConn, `ALTER TABLE public.part_tbl ALTER PARTITION FOR (RANK(9)) ALTER PARTITION FOR (RANK(4)) EXCHANGE PARTITION europe WITH TABLE public.part_tbl_ext_part_ WITHOUT VALIDATION;`)

			timestamp := gpbackup(gpbackupPath)
			gprestore(gprestorePath, timestamp, "-redirect", "restoredb")
			storageType := backup.SelectString(restoreConn, "SELECT relstorage AS string FROM pg_class WHERE relname='part_tbl_1_prt_10_2_prt_5_3_prt_europe'")
			Expect(storageType).To(Equal("x"))
		})
	})
})
