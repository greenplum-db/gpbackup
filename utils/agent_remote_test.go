package utils_test

import (
	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup_filepath"
	"github.com/greenplum-db/gpbackup/utils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
)

var _ = Describe("agent remote", func() {
	var (
		testCluster  *cluster.Cluster
		testExecutor *testhelper.TestExecutor
	)
	BeforeEach(func() {
		masterSeg := cluster.SegConfig{ContentID: -1, Hostname: "localhost", DataDir: "/data/gpseg-1"}
		localSegOne := cluster.SegConfig{ContentID: 0, Hostname: "localhost", DataDir: "/data/gpseg0"}
		remoteSegOne := cluster.SegConfig{ContentID: 1, Hostname: "remotehost1", DataDir: "/data/gpseg1"}

		testExecutor = &testhelper.TestExecutor{
			ClusterOutput: &cluster.RemoteOutput{},
		}
		testCluster = cluster.NewCluster([]cluster.SegConfig{masterSeg, localSegOne, remoteSegOne})
		testCluster.Executor = testExecutor

	})
	It("calls the correct command to copy the OID list", func() {

		oidList := []string{"1", "2", "3"}
		filePath := backup_filepath.NewFilePathInfo(testCluster, "my_dir", "20190102030405", "my_user_seg_prefix")

		utils.WriteOidListToSegments(oidList, testCluster, filePath)

		Expect(testExecutor.ClusterCommands).To(HaveLen(1))
		Expect(testExecutor.ClusterCommands[0][0][4]).To(ContainSubstring("echo \"1\n2\n3\" > /data/gpseg0/gpbackup_0_20190102030405_oid_"))
	})
	It("it panics and prints error message to log when execution fails", func() {
		failingExecutor := &testhelper.TestExecutor{
			ClusterOutput: &cluster.RemoteOutput{
				NumErrors: 1,
				Errors:    map[int]error{0: errors.New("test error")},
			},
		}
		emptyCluster := cluster.Cluster{
			Segments: map[int]cluster.SegConfig{},
			Executor: failingExecutor,
		}

		oidList := []string{"1", "2", "3"}
		filePath := backup_filepath.NewFilePathInfo(&emptyCluster, "my_dir", "20190102030405", "my_user_seg_prefix")

		Expect(func() { utils.WriteOidListToSegments(oidList, &emptyCluster, filePath) }).To(Panic())
		Expect(string(logfile.Contents())).To(ContainSubstring(`-Unable to write oid list to segments on 1 segment`))
	})
})
