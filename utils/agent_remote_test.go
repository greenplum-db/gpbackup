package utils_test

import (
	"fmt"
	"io"
	"os"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/filepath"
	"github.com/greenplum-db/gpbackup/utils"
	"github.com/pkg/errors"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("agent remote", func() {
	var (
		oidList      []string
		fpInfo       filepath.FilePathInfo
		testCluster  *cluster.Cluster
		testExecutor *testhelper.TestExecutor
		remoteOutput *cluster.RemoteOutput
	)
	BeforeEach(func() {
		oidList = []string{"1", "2", "3"}
		operating.System.OpenFileWrite = func(name string, flag int, perm os.FileMode) (io.WriteCloser, error) {
			return buffer, nil
		}

		// Setup test cluster
		coordinatorSeg := cluster.SegConfig{ContentID: -1, Hostname: "localhost", DataDir: "/data/gpseg-1"}
		localSegOne := cluster.SegConfig{ContentID: 0, Hostname: "localhost", DataDir: "/data/gpseg0"}
		remoteSegOne := cluster.SegConfig{ContentID: 1, Hostname: "remotehost1", DataDir: "/data/gpseg1"}

		testExecutor = &testhelper.TestExecutor{}
		remoteOutput = &cluster.RemoteOutput{}
		testExecutor.ClusterOutput = remoteOutput

		testCluster = cluster.NewCluster([]cluster.SegConfig{coordinatorSeg, localSegOne, remoteSegOne})
		testCluster.Executor = testExecutor

		fpInfo = filepath.NewFilePathInfo(testCluster, "", "11112233445566", "", false)
	})
	// note: technically the file system is written to during the call `operating.System.TempFile`
	//			this file is not used throughout the unit tests below, and it is cleaned up with the method: `operating.System.Remove`
	Describe("WriteOidListToSegments()", func() {
		It("generates the correct rsync commands to copy oid file to segments", func() {
			utils.WriteOidListToSegments(oidList, testCluster, fpInfo, "oid")

			Expect(testExecutor.NumExecutions).To(Equal(1))
			cc := testExecutor.ClusterCommands[0]
			Expect(len(cc)).To(Equal(2))
			Expect(cc[0].CommandString).To(MatchRegexp("rsync -e ssh .*/gpbackup-oids.* localhost:/data/gpseg0/gpbackup_0_11112233445566_oid_.*"))
			Expect(cc[1].CommandString).To(MatchRegexp("rsync -e ssh .*/gpbackup-oids.* remotehost1:/data/gpseg1/gpbackup_1_11112233445566_oid_.*"))
		})
		It("panics if any rsync commands fail and outputs correct err messages", func() {
			testExecutor.ErrorOnExecNum = 1
			remoteOutput.NumErrors = 1
			remoteOutput.Scope = cluster.ON_LOCAL & cluster.ON_SEGMENTS
			remoteOutput.Commands = []cluster.ShellCommand{
				cluster.ShellCommand{Content: -1},
				cluster.ShellCommand{Content: 0},
				cluster.ShellCommand{
					Content:       1,
					CommandString: "rsync -e ssh fake_coordinator fake_host",
					Stderr:        "stderr content 1",
					Error:         errors.New("test error 1"),
				},
			}

			Expect(func() { utils.WriteOidListToSegments(oidList, testCluster, fpInfo, "oid") }).To(Panic())

			Expect(testExecutor.NumExecutions).To(Equal(1))
			Expect(string(logfile.Contents())).To(ContainSubstring(`[CRITICAL]:-Failed to rsync oid file on 1 segment. See gbytes.Buffer for a complete list of errors.`))
		})
	})
	Describe("WriteOidsToFile()", func() {
		It("writes oid list, delimited by newline characters", func() {
			utils.WriteOidsToFile("myFilename", oidList)

			Expect(string(buffer.Contents())).To(Equal("1\n2\n3\n"))
		})
		It("panics and prints when it cannot open local oid file", func() {
			operating.System.OpenFileWrite = func(name string, flag int, perm os.FileMode) (io.WriteCloser, error) {
				return nil, errors.New("cannot open local oid file")
			}

			Expect(func() { utils.WriteOidsToFile("filename", oidList) }).To(Panic())
			Expect(string(logfile.Contents())).To(ContainSubstring(`cannot open local oid file`))
		})
		It("panics and prints when it cannot close local oid file", func() {
			operating.System.OpenFileWrite = func(name string, flag int, perm os.FileMode) (io.WriteCloser, error) {
				return testWriter{CloseErr: errors.New("cannot close local oid file")}, nil
			}

			Expect(func() { utils.WriteOidsToFile("filename", oidList) }).To(Panic())
			Expect(string(logfile.Contents())).To(ContainSubstring(`cannot close local oid file`))
		})
		It("panics and prints when WriteOids returns an error", func() {
			operating.System.OpenFileWrite = func(name string, flag int, perm os.FileMode) (io.WriteCloser, error) {
				return testWriter{WriteErr: errors.New("WriteOids returned err")}, nil
			}

			Expect(func() { utils.WriteOidsToFile("filename", oidList) }).To(Panic())
			Expect(string(logfile.Contents())).To(ContainSubstring("WriteOids returned err"))
		})
	})
	Describe("WriteOids()", func() {
		It("writes oid list, delimited by newline characters", func() {
			err := utils.WriteOids(buffer, oidList)
			Expect(err).ToNot(HaveOccurred())

			Expect(string(buffer.Contents())).To(Equal("1\n2\n3\n"))
		})
		It("returns an error if it fails to write an oid", func() {
			tw := testWriter{}
			tw.WriteErr = errors.New("fail oid write")

			err := utils.WriteOids(tw, oidList)
			Expect(err).To(Equal(tw.WriteErr))
		})
	})
	Describe("StartGpbackupHelpers()", func() {
		It("Correctly propagates --on-error-continue flag to gpbackup_helper", func() {
			wasTerminated := false
			utils.StartGpbackupHelpers(testCluster, fpInfo, "operation", "/tmp/pluginConfigFile.yml", " compressStr", true, false, &wasTerminated, 1, true, false, 0, 0)

			cc := testExecutor.ClusterCommands[0]
			Expect(cc[1].CommandString).To(ContainSubstring(" --on-error-continue"))
		})
		It("Correctly propagates --copy-queue-size value to gpbackup_helper", func() {
			wasTerminated := false
			utils.StartGpbackupHelpers(testCluster, fpInfo, "operation", "/tmp/pluginConfigFile.yml", " compressStr", false, false, &wasTerminated, 4, true, false, 0, 0)

			cc := testExecutor.ClusterCommands[0]
			Expect(cc[1].CommandString).To(ContainSubstring(" --copy-queue-size 4"))
		})
	})
	Describe("CheckAgentErrorsOnSegments", func() {
		It("constructs the correct ssh call to check for the existance of an error file on each segment", func() {
			err := utils.CheckAgentErrorsOnSegments(testCluster, fpInfo)
			Expect(err).ToNot(HaveOccurred())

			cc := testExecutor.ClusterCommands[0]
			errorFile0 := fmt.Sprintf(`/data/gpseg0/gpbackup_0_11112233445566_pipe_%d_error`, fpInfo.PID)
			expectedCmd0 := fmt.Sprintf(`if [[ -f %[1]s ]]; then echo 'error'; fi; rm -f %[1]s`, errorFile0)
			Expect(cc[0].CommandString).To(ContainSubstring(expectedCmd0))

			errorFile1 := fmt.Sprintf(`/data/gpseg1/gpbackup_1_11112233445566_pipe_%d_error`, fpInfo.PID)
			expectedCmd1 := fmt.Sprintf(`if [[ -f %[1]s ]]; then echo 'error'; fi; rm -f %[1]s`, errorFile1)
			Expect(cc[1].CommandString).To(ContainSubstring(expectedCmd1))
		})

	})
})

type testWriter struct {
	WriteErr error
	CloseErr error
}

func (f testWriter) Write(p []byte) (n int, err error) {
	_ = p
	return 0, f.WriteErr
}
func (f testWriter) Close() error {
	return f.CloseErr
}
