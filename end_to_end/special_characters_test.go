package end_to_end_test

import (
	"fmt"
	"os/exec"

	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("End to End Special Character tests", func() {
	BeforeEach(func() {
		end_to_end_setup()
	})
	AfterEach(func() {
		end_to_end_teardown()
	})

	It("runs gpbackup and gprestore with redirecting restore to another db containing special capital letters", func() {
		output := gpbackup(gpbackupPath, backupHelperPath)
		timestamp := getBackupTimestamp(string(output))

		gprestore(gprestorePath, restoreHelperPath, timestamp,
			"--create-db",
			"--redirect-db", "CAPS")
		err := exec.Command("dropdb", "CAPS").Run()
		if err != nil {
			Fail(fmt.Sprintf("%v", err))
		}
	})

	It("runs gpbackup with --include-table flag with CAPS special characters", func() {
		skipIfOldBackupVersionBefore("1.9.1")
		output := gpbackup(gpbackupPath, backupHelperPath,
			"--backup-dir", backupDir,
			"--include-table", "public.FOObar")
		timestamp := getBackupTimestamp(string(output))

		gprestore(gprestorePath, restoreHelperPath, timestamp,
			"--redirect-db", "restoredb",
			"--backup-dir", backupDir)

		assertRelationsCreated(restoreConn, 1)
		localSchemaTupleCounts := map[string]int{
			`public."FOObar"`: 1,
		}
		assertDataRestored(restoreConn, localSchemaTupleCounts)
		assertArtifactsCleaned(timestamp)
	})
	It("runs gpbackup with --include-table flag with partitions with special chars", func() {
		skipIfOldBackupVersionBefore("1.9.1")
		testhelper.AssertQueryRuns(backupConn,
			`CREATE TABLE public."CAPparent" (id int, rank int, year int, gender
char(1), count int )
DISTRIBUTED BY (id)
PARTITION BY LIST (gender)
( PARTITION girls VALUES ('F'),
  PARTITION boys VALUES ('M'),
  DEFAULT PARTITION other );
			`)
		defer testhelper.AssertQueryRuns(backupConn, `DROP TABLE public."CAPparent"`)

		testhelper.AssertQueryRuns(backupConn,
			`insert into public."CAPparent" values (1,1,1,'M',1)`)
		testhelper.AssertQueryRuns(backupConn,
			`insert into public."CAPparent" values (0,0,0,'F',1)`)
		output := gpbackup(gpbackupPath, backupHelperPath,
			"--backup-dir", backupDir,
			"--include-table", `public.CAPparent_1_prt_girls`,
			"--leaf-partition-data")
		timestamp := getBackupTimestamp(string(output))

		gprestore(gprestorePath, restoreHelperPath, timestamp,
			"--redirect-db", "restoredb",
			"--backup-dir", backupDir)

		// When running against GPDB 7+, only the root partition and the included leaf partition
		// will be created due to the new flexible GPDB 7+ partitioning logic. For versions
		// before GPDB 7, there is only one big DDL for the entire partition table.
		if backupConn.Version.AtLeast("7") {
			assertRelationsCreated(restoreConn, 2)
		} else {
			assertRelationsCreated(restoreConn, 4)
		}

		localSchemaTupleCounts := map[string]int{
			`public."CAPparent_1_prt_girls"`: 1,
			`public."CAPparent"`:             1,
		}
		assertDataRestored(restoreConn, localSchemaTupleCounts)
		assertArtifactsCleaned(timestamp)
	})
	It(`gpbackup runs with table name including special chars ~#$%^&*()_-+[]{}><|;:/?!\tC`, func() {
		allChars := []string{" ", "`", "~", "#", "$", "%", "^", "&", "*", "(", ")", "-", "+", "[", "]", "{", "}", ">", "<", "\\", "|", ";", ":", "/", "?", ",", "!", "C", "\t", "'", "1", "\\n", "\\t", "\""}
		var includeTableArgs []string
		includeTableArgs = append(includeTableArgs, "--dbname")
		includeTableArgs = append(includeTableArgs, "testdb")
		for _, char := range allChars {
			// Table names containing a double quote (") need to be escaped by doubling the double quote ("")
			if char == "\"" {
				testhelper.AssertQueryRuns(backupConn,
					`CREATE TABLE public."foo""bar" ();`)
				defer testhelper.AssertQueryRuns(backupConn,
					`DROP TABLE public."foo""bar";`)
				continue
			}
			tableName := fmt.Sprintf("foo%sbar", char)
			testhelper.AssertQueryRuns(backupConn, fmt.Sprintf(`CREATE TABLE public."%s" ();`, tableName))
			defer testhelper.AssertQueryRuns(backupConn, fmt.Sprintf(`DROP TABLE public."%s";`, tableName))
			includeTableArgs = append(includeTableArgs, "--include-table")
			includeTableArgs = append(includeTableArgs, fmt.Sprintf(`public.%s`, tableName))
		}

		cmd := exec.Command("gpbackup", includeTableArgs...)
		_, err := cmd.CombinedOutput()
		Expect(err).ToNot(HaveOccurred())
	})
})
