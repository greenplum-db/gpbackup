package restore_test

import (
	"os/user"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/filepath"
	"github.com/greenplum-db/gpbackup/history"
	"github.com/greenplum-db/gpbackup/options"
	"github.com/greenplum-db/gpbackup/restore"
	"github.com/greenplum-db/gpbackup/toc"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("restore/remote tests", func() {
	coordinatorSeg := cluster.SegConfig{ContentID: -1, Hostname: "localhost", DataDir: "/data/gpseg-1"}
	localSegOne := cluster.SegConfig{ContentID: 0, Hostname: "localhost", DataDir: "/data/gpseg0"}
	remoteSegOne := cluster.SegConfig{ContentID: 1, Hostname: "remotehost1", DataDir: "/data/gpseg1"}
	var (
		testCluster  *cluster.Cluster
		testExecutor *testhelper.TestExecutor
		testFPInfo   filepath.FilePathInfo
	)

	BeforeEach(func() {
		operating.System.CurrentUser = func() (*user.User, error) { return &user.User{Username: "testUser", HomeDir: "testDir"}, nil }
		operating.System.Hostname = func() (string, error) { return "testHost", nil }
		testExecutor = &testhelper.TestExecutor{}
		testCluster = cluster.NewCluster([]cluster.SegConfig{coordinatorSeg, localSegOne, remoteSegOne})
		testCluster.Executor = testExecutor
		testFPInfo = filepath.NewFilePathInfo(testCluster, "", "20170101010101", "gpseg", false)
		restore.SetFPInfo(testFPInfo)
	})
	Describe("VerifyBackupFileCountOnSegments", func() {
		BeforeEach(func() {
			restore.SetBackupConfig(&history.BackupConfig{SingleDataFile: true})

			dataEntryOne := toc.CoordinatorDataEntry{}
			dataEntryTwo := toc.CoordinatorDataEntry{}
			globalDataEntries := []toc.CoordinatorDataEntry{dataEntryOne, dataEntryTwo}
			restore.SetTOC(&toc.TOC{DataEntries: globalDataEntries})
		})
		It("successfully verifies that all backup file counts", func() {
			testExecutor.ClusterOutput = &cluster.RemoteOutput{
				NumErrors: 0,
			}
			testCluster.Executor = testExecutor
			restore.SetCluster(testCluster)
			restore.VerifyBackupFileCountOnSegments()
			Expect((*testExecutor).NumExecutions).To(Equal(1))
		})
		It("panics if backup file counts do not match on all segments with single-data-file", func() {
			testExecutor.ClusterOutput = &cluster.RemoteOutput{
				Commands: []cluster.ShellCommand{
					cluster.ShellCommand{Stdout: "1"},
					cluster.ShellCommand{Stdout: "1"},
				},
			}
			testCluster.Executor = testExecutor
			restore.SetCluster(testCluster)
			defer testhelper.ShouldPanicWithMessage("Found incorrect number of backup files on 2 segments")
			restore.VerifyBackupFileCountOnSegments()
		})
		It("panics if backup file counts do not match on all segments without single-data-file", func() {
			testExecutor.ClusterOutput = &cluster.RemoteOutput{
				Commands: []cluster.ShellCommand{
					cluster.ShellCommand{Stdout: "1"},
					cluster.ShellCommand{Stdout: "1"},
				},
			}
			restore.SetBackupConfig(&history.BackupConfig{SingleDataFile: false})
			testCluster.Executor = testExecutor
			restore.SetCluster(testCluster)
			defer testhelper.ShouldPanicWithMessage("Found incorrect number of backup files on 2 segments")
			restore.VerifyBackupFileCountOnSegments()
		})
		It("panics if backup file counts do not match on some segments", func() {
			testExecutor.ClusterOutput = &cluster.RemoteOutput{
				Commands: []cluster.ShellCommand{
					cluster.ShellCommand{Stdout: "1"},
				},
			}
			testCluster.Executor = testExecutor
			restore.SetCluster(testCluster)
			defer testhelper.ShouldPanicWithMessage("Found incorrect number of backup files on 1 segment")
			restore.VerifyBackupFileCountOnSegments()
		})
		It("panics if it cannot verify some backup file counts", func() {
			testExecutor.ClusterOutput = &cluster.RemoteOutput{
				NumErrors: 1,
				FailedCommands: []*cluster.ShellCommand{
					&cluster.ShellCommand{Content: 1, Stdout: "1"},
				},
			}
			testCluster.Executor = testExecutor
			restore.SetCluster(testCluster)
			defer testhelper.ShouldPanicWithMessage("Could not verify backup file count on 1 segment")
			restore.VerifyBackupFileCountOnSegments()
		})
		It("verifies backup file counts match on all segments with resize-cluster", func() {
			testExecutor.ClusterOutput = &cluster.RemoteOutput{
				Commands: []cluster.ShellCommand{
					cluster.ShellCommand{Stdout: "4"},
					cluster.ShellCommand{Stdout: "2"},
				},
			}
			testCluster.Executor = testExecutor
			restore.SetCluster(testCluster)
			restore.SetBackupConfig(&history.BackupConfig{SingleDataFile: true, SegmentCount: 3})
			cmdFlags.Set(options.RESIZE_CLUSTER, "true")
			// LogFatalError in VerifyBackupFileCountOnSegments will fail test if values are wrong
			// Expect is implied, does not need to be explicitly called here
			restore.VerifyBackupFileCountOnSegments()
		})
	})
})
