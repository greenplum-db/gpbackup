package utils_test

import (
	"io"
	"os"

	"github.com/greenplum-db/gpbackup/utils"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup_filepath"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
)

var _ = Describe("agent remote", func() {
	var (
		oidList []string
	)
	BeforeEach(func() {
		oidList = []string{"1", "2", "3"}
		operating.System.OpenFileWrite = func(name string, flag int, perm os.FileMode) (io.WriteCloser, error) {
			return buffer, nil
		}
		operating.System.Remove = func(name string) error {
			return nil
		}
	})
	Describe("WriteOidListToSegments()", func() {
		var (
			filePath     backup_filepath.FilePathInfo
			testCluster  *cluster.Cluster
			testExecutor *testhelper.TestExecutor
		)
		BeforeEach(func() {
			masterSeg := cluster.SegConfig{ContentID: -1, Hostname: "localhost", DataDir: "/data/gpseg-1"}
			localSegOne := cluster.SegConfig{ContentID: 0, Hostname: "localhost", DataDir: "/data/gpseg0"}
			remoteSegOne := cluster.SegConfig{ContentID: 1, Hostname: "remotehost1", DataDir: "/data/gpseg1"}

			testExecutor = &testhelper.TestExecutor{
				ErrorOnExecNum: -1,
			}
			testCluster = cluster.NewCluster([]cluster.SegConfig{masterSeg, localSegOne, remoteSegOne})
			testCluster.Executor = testExecutor
			filePath = backup_filepath.NewFilePathInfo(testCluster, "my_dir", "20190102030405", "my_user_seg_prefix")
		})
		It("writes oid list, delimited by newline characters, to temp file", func() {
			utils.WriteOidListToSegments(oidList, testCluster, filePath)

			Expect(string(buffer.Contents())).To(Equal("1\n2\n3\n"))
		})
		It("generates the correct command to copy the OID list", func() {
			utils.WriteOidListToSegments(oidList, testCluster, filePath)

			// one command per segment
			Expect(testExecutor.LocalCommands).To(HaveLen(2))
			Expect(testExecutor.LocalCommands[0]).To(MatchRegexp(`^scp /tmp/gpbackup-oids\d+ localhost:/data/gpseg0/gpbackup_0_20190102030405_oid_\d+$`))
			Expect(testExecutor.LocalCommands[1]).To(MatchRegexp(`^scp /tmp/gpbackup-oids\d+ remotehost1:/data/gpseg1/gpbackup_1_20190102030405_oid_\d+$`))
		})
		It("panics and prints when command execution fails", func() {
			failingExecutor := &testhelper.TestExecutor{
				LocalError: errors.New("command execution error"),
			}
			testCluster.Executor = failingExecutor

			Expect(func() { utils.WriteOidListToSegments(oidList, testCluster, filePath) }).To(Panic())
			Expect(string(logfile.Contents())).To(MatchRegexp(`.*command execution error.*`))
		})
		It("logs a warning when temp file cannot be removed", func() {
			operating.System.Remove = func(name string) error {
				return errors.New("failed to remove oid temp file")
			}

			utils.WriteOidListToSegments(oidList, testCluster, filePath)
			Expect(string(logfile.Contents())).To(MatchRegexp(`.*\[WARNING\]:-.*failed to remove oid temp file.*`))
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
})

type testWriter struct {
	WriteErr error
	CloseErr error
}

func (f testWriter) Write(p []byte) (n int, err error) {
	return 0, f.WriteErr
}
func (f testWriter) Close() error {
	return f.CloseErr
}
