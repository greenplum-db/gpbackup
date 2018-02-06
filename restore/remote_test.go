package restore_test

import (
	"os/user"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/restore"
	"github.com/greenplum-db/gpbackup/utils"
	"github.com/pkg/errors"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("restore/remote tests", func() {
	masterSeg := cluster.SegConfig{ContentID: -1, Hostname: "localhost", DataDir: "/data/gpseg-1"}
	localSegOne := cluster.SegConfig{ContentID: 0, Hostname: "localhost", DataDir: "/data/gpseg0"}
	remoteSegOne := cluster.SegConfig{ContentID: 1, Hostname: "remotehost1", DataDir: "/data/gpseg1"}
	var (
		testCluster  cluster.Cluster
		testExecutor *testhelper.TestExecutor
		testFPInfo   utils.FilePathInfo
	)

	BeforeEach(func() {
		operating.System.CurrentUser = func() (*user.User, error) { return &user.User{Username: "testUser", HomeDir: "testDir"}, nil }
		operating.System.Hostname = func() (string, error) { return "testHost", nil }
		testExecutor = &testhelper.TestExecutor{}
		testCluster = cluster.NewCluster([]cluster.SegConfig{masterSeg, localSegOne, remoteSegOne})
		testCluster.Executor = testExecutor
		testFPInfo = utils.NewFilePathInfo(testCluster.SegDirMap, "", "20170101010101", "gpseg")
		restore.SetFPInfo(testFPInfo)
		cluster.SetLogger(logger)
	})
	Describe("VerifyBackupFileCountOnSegments", func() {
		It("successfully verifies all backup file counts", func() {
			testExecutor.ClusterOutput = &cluster.RemoteOutput{
				NumErrors: 0,
			}
			testCluster.Executor = testExecutor
			restore.SetCluster(testCluster)
			restore.VerifyBackupFileCountOnSegments(2)
			Expect((*testExecutor).NumExecutions).To(Equal(1))
		})
		It("panics if backup file counts do not match on all segments", func() {
			testExecutor.ClusterOutput = &cluster.RemoteOutput{
				Stdouts: map[int]string{
					0: "1",
					1: "1",
				},
			}
			testCluster.Executor = testExecutor
			restore.SetCluster(testCluster)
			defer testhelper.ShouldPanicWithMessage("Found incorrect number of backup files on 2 segments")
			restore.VerifyBackupFileCountOnSegments(2)
		})
		It("panics if backup file counts do not match on some segments", func() {
			testExecutor.ClusterOutput = &cluster.RemoteOutput{
				Stdouts: map[int]string{
					1: "1",
				},
			}
			testCluster.Executor = testExecutor
			restore.SetCluster(testCluster)
			defer testhelper.ShouldPanicWithMessage("Found incorrect number of backup files on 1 segment")
			restore.VerifyBackupFileCountOnSegments(2)
		})
		It("panics if it cannot verify some backup file counts", func() {
			testExecutor.ClusterOutput = &cluster.RemoteOutput{
				NumErrors: 1,
				Errors: map[int]error{
					1: errors.Errorf("exit status 1"),
				},
			}
			testCluster.Executor = testExecutor
			restore.SetCluster(testCluster)
			defer testhelper.ShouldPanicWithMessage("Could not verify backup file count on 1 segment")
			restore.VerifyBackupFileCountOnSegments(2)
		})
	})
	Describe("VerifyBackupDirectoriesExistOnAllHosts", func() {
		It("successfully verifies all directories", func() {
			testExecutor.ClusterOutput = &cluster.RemoteOutput{
				NumErrors: 0,
			}
			testCluster.Executor = testExecutor
			restore.SetCluster(testCluster)
			restore.VerifyBackupDirectoriesExistOnAllHosts()
			Expect((*testExecutor).NumExecutions).To(Equal(1))
		})
		It("panics if it cannot verify all directories", func() {
			testExecutor.ClusterOutput = &cluster.RemoteOutput{
				NumErrors: 2,
				Errors: map[int]error{
					0: errors.Errorf("exit status 1"),
					1: errors.Errorf("exit status 1"),
				},
			}
			testCluster.Executor = testExecutor
			restore.SetCluster(testCluster)
			defer testhelper.ShouldPanicWithMessage("Backup directories missing or inaccessible on 2 segments")
			restore.VerifyBackupDirectoriesExistOnAllHosts()
		})
		It("panics if it cannot verify some directories", func() {
			testExecutor.ClusterOutput = &cluster.RemoteOutput{
				NumErrors: 1,
				Errors: map[int]error{
					1: errors.Errorf("exit status 1"),
				},
			}
			testCluster.Executor = testExecutor
			restore.SetCluster(testCluster)
			defer testhelper.ShouldPanicWithMessage("Backup directories missing or inaccessible on 1 segment")
			restore.VerifyBackupDirectoriesExistOnAllHosts()
		})
	})
})
