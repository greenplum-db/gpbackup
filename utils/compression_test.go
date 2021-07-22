package utils_test

import (
	"os/user"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/greenplum-db/gp-common-go-libs/structmatcher"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/utils"

	. "github.com/onsi/ginkgo"
)

var _ = Describe("utils/compression tests", func() {
	masterSeg := cluster.SegConfig{ContentID: -1, Hostname: "localhost", DataDir: "/data/gpseg-1"}
	localSegOne := cluster.SegConfig{ContentID: 0, Hostname: "localhost", DataDir: "/data/gpseg0"}
	remoteSegOne := cluster.SegConfig{ContentID: 1, Hostname: "remotehost1", DataDir: "/data/gpseg1"}
	var (
		testCluster  *cluster.Cluster
		testExecutor *testhelper.TestExecutor
	)

	BeforeEach(func() {
		operating.System.CurrentUser = func() (*user.User, error) { return &user.User{Username: "testUser", HomeDir: "testDir"}, nil }
		operating.System.Hostname = func() (string, error) { return "testHost", nil }
		testExecutor = &testhelper.TestExecutor{}
		testCluster = cluster.NewCluster([]cluster.SegConfig{masterSeg, localSegOne, remoteSegOne})
		testCluster.Executor = testExecutor
	})

	Describe("InitializePipeThroughParameters", func() {
		It("initializes to use cat when passed no compression", func() {
			originalProgram := utils.GetPipeThroughProgram()
			defer utils.SetPipeThroughProgram(originalProgram)
			expectedProgram := utils.PipeThroughProgram{
				Name:          "cat",
				OutputCommand: "cat -",
				InputCommand:  "cat -",
				Extension:     "",
			}
			utils.InitializePipeThroughParameters(false, "", 3)
			resultProgram := utils.GetPipeThroughProgram()
			structmatcher.ExpectStructsToMatch(&expectedProgram, &resultProgram)
		})
		It("initializes to use gzip when passed compression and a level", func() {
			originalProgram := utils.GetPipeThroughProgram()
			defer utils.SetPipeThroughProgram(originalProgram)
			expectedProgram := utils.PipeThroughProgram{
				Name:          "gzip",
				OutputCommand: "gzip -c -7",
				InputCommand:  "gzip -d -c",
				Extension:     ".gz",
			}
			utils.InitializePipeThroughParameters(true, "gzip", 7)
			resultProgram := utils.GetPipeThroughProgram()
			structmatcher.ExpectStructsToMatch(&expectedProgram, &resultProgram)
		})
	})
})
