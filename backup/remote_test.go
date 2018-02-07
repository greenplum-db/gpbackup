package backup_test

import (
	"os/user"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/utils"
	"github.com/pkg/errors"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup/remote tests", func() {
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
		backup.SetCluster(testCluster)
		backup.SetFPInfo(testFPInfo)
	})
	Describe("CreateBackupDirectoriesOnAllHosts", func() {
		It("successfully creates all directories", func() {
			testExecutor.ClusterOutput = &cluster.RemoteOutput{
				NumErrors: 0,
			}
			backup.CreateBackupDirectoriesOnAllHosts()
			Expect((*testExecutor).NumExecutions).To(Equal(1))
		})
		It("panics if it cannot create all directories", func() {
			testExecutor.ClusterOutput = &cluster.RemoteOutput{
				NumErrors: 2,
				Errors: map[int]error{
					0: errors.Errorf("exit status 1"),
					1: errors.Errorf("exit status 1"),
				},
			}
			testCluster.Executor = testExecutor
			defer testhelper.ShouldPanicWithMessage("Unable to create backup directories on 2 segments")
			backup.CreateBackupDirectoriesOnAllHosts()
		})
		It("panics if it cannot create some directories", func() {
			testExecutor.ClusterOutput = &cluster.RemoteOutput{
				NumErrors: 1,
				Errors: map[int]error{
					1: errors.Errorf("exit status 1"),
				},
			}
			testCluster.Executor = testExecutor
			defer testhelper.ShouldPanicWithMessage("Unable to create backup directories on 1 segment")
			backup.CreateBackupDirectoriesOnAllHosts()
		})
	})
})
