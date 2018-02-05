package restore_test

import (
	"os/user"
	"regexp"

	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/restore"
	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
)

var _ = Describe("restore/data tests", func() {
	masterSeg := utils.SegConfig{ContentID: -1, Hostname: "localhost", DataDir: "/data/gpseg-1"}
	localSegOne := utils.SegConfig{ContentID: 0, Hostname: "localhost", DataDir: "/data/gpseg0"}
	remoteSegOne := utils.SegConfig{ContentID: 1, Hostname: "remotehost1", DataDir: "/data/gpseg1"}
	var (
		testCluster  utils.Cluster
		testExecutor *testutils.TestExecutor
	)
	Describe("CopyTableIn", func() {
		It("will restore a table from its own file with compression", func() {
			utils.SetCompressionParameters(true, utils.Compression{Name: "gzip", CompressCommand: "gzip -c -1", DecompressCommand: "gzip -d -c", Extension: ".gz"})
			execStr := regexp.QuoteMeta("COPY public.foo(i,j) FROM PROGRAM 'gzip -d -c < <SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_3456.gz' WITH CSV DELIMITER ',' ON SEGMENT;")
			mock.ExpectExec(execStr).WillReturnResult(sqlmock.NewResult(10, 0))
			filename := "<SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_3456.gz"
			restore.CopyTableIn(connection, "public.foo", "(i,j)", filename, false, 0, 3456)
		})
		It("will restore a table from its own file without compression", func() {
			utils.SetCompressionParameters(false, utils.Compression{})
			execStr := regexp.QuoteMeta("COPY public.foo(i,j) FROM '<SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_3456' WITH CSV DELIMITER ',' ON SEGMENT;")
			mock.ExpectExec(execStr).WillReturnResult(sqlmock.NewResult(10, 0))
			filename := "<SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_3456"
			restore.CopyTableIn(connection, "public.foo", "(i,j)", filename, false, 0, 3456)
		})
		It("will restore a table from a single data file with compression", func() {
			utils.SetCompressionParameters(true, utils.Compression{Name: "gzip", CompressCommand: "gzip -c -1", DecompressCommand: "gzip -d -c", Extension: ".gz"})
			execStr := regexp.QuoteMeta("COPY public.foo(i,j) FROM PROGRAM 'cat <SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_pipe_3456' WITH CSV DELIMITER ',' ON SEGMENT;")
			mock.ExpectExec(execStr).WillReturnResult(sqlmock.NewResult(10, 0))
			filename := "<SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_pipe"
			restore.CopyTableIn(connection, "public.foo", "(i,j)", filename, true, 0, 3456)
		})
		It("will restore a table from a single data file without compression", func() {
			utils.SetCompressionParameters(false, utils.Compression{})
			execStr := regexp.QuoteMeta("COPY public.foo(i,j) FROM PROGRAM 'cat <SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_pipe_3456' WITH CSV DELIMITER ',' ON SEGMENT;")
			mock.ExpectExec(execStr).WillReturnResult(sqlmock.NewResult(10, 0))
			filename := "<SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_pipe"
			restore.CopyTableIn(connection, "public.foo", "(i,j)", filename, true, 0, 3456)
		})
	})
	Describe("CheckRowsRestored", func() {
		var (
			expectedRows int64 = 10
			name               = "public.foo"
		)
		BeforeEach(func() {
			operating.System.CurrentUser = func() (*user.User, error) { return &user.User{Username: "testUser", HomeDir: "testDir"}, nil }
			operating.System.Hostname = func() (string, error) { return "testHost", nil }
			testExecutor = &testutils.TestExecutor{}
			testCluster = utils.NewCluster([]utils.SegConfig{masterSeg, localSegOne, remoteSegOne}, "", "20170101010101", "gpseg")
			testCluster.Executor = testExecutor
		})
		AfterEach(func() {
			restore.SetOnErrorContinue(false)
		})
		It("does nothing if the number of rows match ", func() {
			restore.CheckRowsRestored(10, expectedRows, name)
		})
		It("panics if the numbers of rows do not match and there is an error with a segment agent", func() {
			restore.SetOnErrorContinue(false)
			testExecutor.ClusterOutput = &utils.RemoteOutput{
				Stdouts: map[int]string{
					1: "error",
				},
			}
			testCluster.Executor = testExecutor
			restore.SetCluster(testCluster)
			defer func() {
				Expect(stderr).To(gbytes.Say("Expected to restore 10 rows to table public.foo, but restored 5 instead"))
			}()
			defer testhelper.ShouldPanicWithMessage("Encountered errors with 1 restore agent(s).  See gbytes.Buffer for a complete list of segments with errors, and see testDir/gpAdminLogs/gpbackup_helper_20170101.log on the corresponding hosts for detailed error messages.")
			restore.CheckRowsRestored(5, expectedRows, name)
		})
		It("panics if the numbers of rows do not match and there is no error with a segment agent", func() {
			restore.SetOnErrorContinue(false)
			testExecutor.ClusterOutput = &utils.RemoteOutput{
				Stdouts: map[int]string{
					1: "",
				},
			}
			testCluster.Executor = testExecutor
			restore.SetCluster(testCluster)
			defer testhelper.ShouldPanicWithMessage("Expected to restore 10 rows to table public.foo, but restored 5 instead")
			restore.CheckRowsRestored(5, expectedRows, name)
		})
		It("prints an error if the numbers of rows do not match and onErrorContinue is set", func() {
			restore.SetOnErrorContinue(true)
			testExecutor.ClusterOutput = &utils.RemoteOutput{
				Stdouts: map[int]string{
					1: "",
				},
			}
			testCluster.Executor = testExecutor
			restore.SetCluster(testCluster)
			restore.CheckRowsRestored(5, expectedRows, name)
			Expect(stderr).To(gbytes.Say(regexp.QuoteMeta("[ERROR]:-Expected to restore 10 rows to table public.foo, but restored 5 instead")))

			testExecutor.ClusterOutput = &utils.RemoteOutput{
				Stdouts: map[int]string{
					1: "error",
				},
			}
			testCluster.Executor = testExecutor
			restore.SetCluster(testCluster)
			restore.CheckRowsRestored(5, expectedRows, name)
			Expect(stderr).To(gbytes.Say(regexp.QuoteMeta("[ERROR]:-Expected to restore 10 rows to table public.foo, but restored 5 instead")))
		})
	})
})
