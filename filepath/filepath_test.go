package filepath_test

import (
	"os"
	path "path/filepath"
	"testing"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"

	. "github.com/greenplum-db/gpbackup/filepath"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestBackupFilepath(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Filepath Suite")
}

var _ = BeforeSuite(func() {
	_, _, _ = testhelper.SetupTestLogger()
	operating.System = operating.InitializeSystemFunctions()
})

var _ = Describe("filepath tests", func() {
	coordinatorDir := "/data/gpseg-1"
	standbyDir := "/data/gpseg_mirror-1"
	segDirOne := "/data/gpseg0"
	segDirTwo := "/data/gpseg1"
	mirrorDirOne := "/data/gpseg_mirror0"
	mirrorDirTwo := "/data/gpseg_mirror1"

	var c *cluster.Cluster
	BeforeEach(func() {
		c = cluster.NewCluster([]cluster.SegConfig{
			{ContentID: -1, DataDir: coordinatorDir},
		})
	})
	Describe("Backup Filepath setup and accessors", func() {
		It("returns content dir for a single-host, single-segment nodes", func() {
			c = cluster.NewCluster([]cluster.SegConfig{
				{ContentID: -1, DataDir: coordinatorDir},
				{ContentID: 0, DataDir: segDirOne},
			})
			fpInfo := NewFilePathInfo(c, "", "20170101010101", "gpseg", false)
			Expect(fpInfo.SegDirMap).To(HaveLen(2))
			Expect(fpInfo.GetDirForContent(-1)).To(Equal("/data/gpseg-1/backups/20170101/20170101010101"))
			Expect(fpInfo.GetDirForContent(0)).To(Equal("/data/gpseg0/backups/20170101/20170101010101"))
		})
		It("sets up the configuration for a single-host, multi-segment fpInfo", func() {
			c = cluster.NewCluster([]cluster.SegConfig{
				{ContentID: -1, DataDir: coordinatorDir},
				{ContentID: 0, DataDir: segDirOne},
				{ContentID: 1, DataDir: segDirTwo},
			})
			fpInfo := NewFilePathInfo(c, "", "20170101010101", "gpseg", false)
			Expect(fpInfo.SegDirMap).To(HaveLen(3))
			Expect(fpInfo.GetDirForContent(-1)).To(Equal("/data/gpseg-1/backups/20170101/20170101010101"))
			Expect(fpInfo.GetDirForContent(0)).To(Equal("/data/gpseg0/backups/20170101/20170101010101"))
			Expect(fpInfo.GetDirForContent(1)).To(Equal("/data/gpseg1/backups/20170101/20170101010101"))
		})
		It("sets up the configuration for a single-host, multi-segment fpInfo using primaries when the cluster has mirrors", func() {
			c = cluster.NewCluster([]cluster.SegConfig{
				{ContentID: -1, Role: "p", DataDir: coordinatorDir},
				{ContentID: 0, Role: "p", DataDir: segDirOne},
				{ContentID: 1, Role: "p", DataDir: segDirTwo},
				{ContentID: 0, Role: "m", DataDir: mirrorDirOne},
				{ContentID: 1, Role: "m", DataDir: mirrorDirTwo},
				{ContentID: -1, Role: "m", DataDir: standbyDir},
			})
			fpInfo := NewFilePathInfo(c, "", "20170101010101", "gpseg", false)
			Expect(fpInfo.SegDirMap).To(HaveLen(3))
			Expect(fpInfo.GetDirForContent(-1)).To(Equal("/data/gpseg-1/backups/20170101/20170101010101"))
			Expect(fpInfo.GetDirForContent(0)).To(Equal("/data/gpseg0/backups/20170101/20170101010101"))
			Expect(fpInfo.GetDirForContent(1)).To(Equal("/data/gpseg1/backups/20170101/20170101010101"))
		})
		It("sets up the configuration for a single-host, multi-segment fpInfo using mirrors", func() {
			c = cluster.NewCluster([]cluster.SegConfig{
				{ContentID: -1, Role: "p", DataDir: coordinatorDir},
				{ContentID: 0, Role: "p", DataDir: segDirOne},
				{ContentID: 1, Role: "p", DataDir: segDirTwo},
				{ContentID: 0, Role: "m", DataDir: mirrorDirOne},
				{ContentID: 1, Role: "m", DataDir: mirrorDirTwo},
				{ContentID: -1, Role: "m", DataDir: standbyDir},
			})
			fpInfo := NewFilePathInfo(c, "", "20170101010101", "gpseg", false, true)
			Expect(fpInfo.SegDirMap).To(HaveLen(3))
			Expect(fpInfo.GetDirForContent(-1)).To(Equal("/data/gpseg_mirror-1/backups/20170101/20170101010101"))
			Expect(fpInfo.GetDirForContent(0)).To(Equal("/data/gpseg_mirror0/backups/20170101/20170101010101"))
			Expect(fpInfo.GetDirForContent(1)).To(Equal("/data/gpseg_mirror1/backups/20170101/20170101010101"))
		})
		It("returns the content directory based on the user specified path", func() {
			fpInfo := NewFilePathInfo(c, "/foo/bar", "20170101010101", "gpseg", false)
			Expect(fpInfo.SegDirMap).To(HaveLen(1))
			Expect(fpInfo.GetDirForContent(-1)).To(Equal("/foo/bar/gpseg-1/backups/20170101/20170101010101"))
		})
	})
	Describe("GetTableBackupFilePathForCopyCommand()", func() {
		It("returns table file path for copy command", func() {
			fpInfo := NewFilePathInfo(c, "", "20170101010101", "gpseg", false)
			Expect(fpInfo.GetTableBackupFilePathForCopyCommand(1234, "", false)).To(Equal("<SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_1234"))
		})
		It("returns table file path for copy command based on user specified path", func() {
			fpInfo := NewFilePathInfo(c, "/foo/bar", "20170101010101", "gpseg", false)
			Expect(fpInfo.GetTableBackupFilePathForCopyCommand(1234, "", false)).To(Equal("/foo/bar/gpseg<SEGID>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_1234"))
		})
		It("returns table file path for copy command in single-file mode", func() {
			fpInfo := NewFilePathInfo(c, "", "20170101010101", "gpseg", false)
			Expect(fpInfo.GetTableBackupFilePathForCopyCommand(1234, "", true)).To(Equal("<SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101"))
		})
		It("returns table file path for copy command based on user specified path in single-file mode", func() {
			fpInfo := NewFilePathInfo(c, "/foo/bar", "20170101010101", "gpseg", false)
			Expect(fpInfo.GetTableBackupFilePathForCopyCommand(1234, "", true)).To(Equal("/foo/bar/gpseg<SEGID>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101"))
		})
		It("returns table file path for copy command with extension", func() {
			fpInfo := NewFilePathInfo(c, "", "20170101010101", "gpseg", false)
			Expect(fpInfo.GetTableBackupFilePathForCopyCommand(1234, ".gzip", false)).To(Equal("<SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_1234.gzip"))
		})
		It("returns table file path for copy command based on user specified path with extension", func() {
			fpInfo := NewFilePathInfo(c, "/foo/bar", "20170101010101", "gpseg", false)
			Expect(fpInfo.GetTableBackupFilePathForCopyCommand(1234, ".gzip", false)).To(Equal("/foo/bar/gpseg<SEGID>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_1234.gzip"))
		})
		It("returns table file path for copy command in single-file mode with extension", func() {
			fpInfo := NewFilePathInfo(c, "", "20170101010101", "gpseg", false)
			Expect(fpInfo.GetTableBackupFilePathForCopyCommand(1234, ".gzip", true)).To(Equal("<SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101.gzip"))
		})
		It("returns table file path for copy command based on user specified path in single-file mode with extension", func() {
			fpInfo := NewFilePathInfo(c, "/foo/bar", "20170101010101", "gpseg", false)
			Expect(fpInfo.GetTableBackupFilePathForCopyCommand(1234, ".gzip", true)).To(Equal("/foo/bar/gpseg<SEGID>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101.gzip"))
		})
	})
	Describe("GetReportFilePath", func() {
		It("returns report file path", func() {
			fpInfo := NewFilePathInfo(c, "", "20170101010101", "gpseg", false)
			Expect(fpInfo.GetBackupReportFilePath()).To(Equal("/data/gpseg-1/backups/20170101/20170101010101/gpbackup_20170101010101_report"))
		})
		It("returns report file path based on user specified path", func() {
			fpInfo := NewFilePathInfo(c, "/foo/bar", "20170101010101", "gpseg", false)
			Expect(fpInfo.GetBackupReportFilePath()).To(Equal("/foo/bar/gpseg-1/backups/20170101/20170101010101/gpbackup_20170101010101_report"))
		})
		It("returns report file path for restore command", func() {
			fpInfo := NewFilePathInfo(c, "", "20170101010101", "gpseg", false)
			Expect(fpInfo.GetRestoreReportFilePath("20200101010101")).To(Equal("/data/gpseg-1/backups/20170101/20170101010101/gprestore_20170101010101_20200101010101_report"))
		})
		It("returns report file path based on user specified path for restore command", func() {
			fpInfo := NewFilePathInfo(c, "/foo/bar", "20170101010101", "gpseg", false)
			Expect(fpInfo.GetRestoreReportFilePath("20200101010101")).To(Equal("/foo/bar/gpseg-1/backups/20170101/20170101010101/gprestore_20170101010101_20200101010101_report"))
		})
		It("returns different report file paths based on user specified report path for backup and restore command", func() {
			fpInfo := NewFilePathInfo(c, "/foo/bar", "20170101010101", "gpseg", false)
			fpInfo.SingleBackupDir = true
			fpInfo.UserSpecifiedReportDir = "/bar/foo"
			Expect(fpInfo.GetBackupReportFilePath()).To(Equal("/foo/bar/gpseg-1/backups/20170101/20170101010101/gpbackup_20170101010101_report"))
			Expect(fpInfo.GetRestoreReportFilePath("20200101010101")).To(Equal("/bar/foo/gprestore_20170101010101_20200101010101_report"))
			fpInfo.SingleBackupDir = false
			Expect(fpInfo.GetRestoreReportFilePath("20200101010101")).To(Equal("/bar/foo/gpseg-1/backups/20170101/20170101010101/gprestore_20170101010101_20200101010101_report"))
		})
	})
	Describe("GetTableBackupFilePath", func() {
		It("returns table file path", func() {
			fpInfo := NewFilePathInfo(c, "", "20170101010101", "gpseg", false)
			Expect(fpInfo.GetTableBackupFilePath(-1, 1234, "", false)).To(Equal("/data/gpseg-1/backups/20170101/20170101010101/gpbackup_-1_20170101010101_1234"))
		})
		It("returns table file path based on user specified path", func() {
			fpInfo := NewFilePathInfo(c, "/foo/bar", "20170101010101", "gpseg", false)
			Expect(fpInfo.GetTableBackupFilePath(-1, 1234, "", false)).To(Equal("/foo/bar/gpseg-1/backups/20170101/20170101010101/gpbackup_-1_20170101010101_1234"))
		})
		It("returns single data file path", func() {
			fpInfo := NewFilePathInfo(c, "", "20170101010101", "gpseg", false)
			Expect(fpInfo.GetTableBackupFilePath(-1, 1234, "", true)).To(Equal("/data/gpseg-1/backups/20170101/20170101010101/gpbackup_-1_20170101010101"))
		})
		It("returns single data file path based on user specified path", func() {
			fpInfo := NewFilePathInfo(c, "/foo/bar", "20170101010101", "gpseg", false)
			Expect(fpInfo.GetTableBackupFilePath(-1, 1234, "", true)).To(Equal("/foo/bar/gpseg-1/backups/20170101/20170101010101/gpbackup_-1_20170101010101"))
		})
	})
	Describe("ParseSegPrefix", func() {
		AfterEach(func() {
			operating.System.Glob = path.Glob
		})
		It("returns segment prefix from directory path if coordinator backup directory exists", func() {
			operating.System.Glob = func(pattern string) (matches []string, err error) {
				return []string{"/tmp/foo/gpseg-1/backups"}, nil
			}
			res, _, err := ParseSegPrefix("/tmp/foo", "00000000000000")
			Expect(err).ToNot(HaveOccurred())
			Expect(res).To(Equal("gpseg"))
		})
		It("returns empty string if backup directory is empty", func() {
			res, _, err := ParseSegPrefix("", "00000000000000")
			Expect(err).ToNot(HaveOccurred())
			Expect(res).To(Equal(""))
		})
		It("returns an error when coordinator backup directory does not exist", func() {
			operating.System.Glob = func(pattern string) (matches []string, err error) { return []string{}, nil }
			_, _, err := ParseSegPrefix("/tmp/foo", "00000000000000")
			Expect(err.Error()).To(Equal("Backup directory in /tmp/foo missing"))
		})
		It("returns an error when there is an error accessing coordinator backup directory", func() {
			operating.System.Glob = func(pattern string) (matches []string, err error) {
				return []string{""}, os.ErrPermission
			}
			_, _, err := ParseSegPrefix("/tmp/foo", "00000000000000")
			Expect(err.Error()).To(Equal("Failure while trying to locate backup directory in /tmp/foo. Error: permission denied"))
		})
		It("returns an error when multiple coordinator backup directories", func() {
			operating.System.Glob = func(pattern string) (matches []string, err error) {
				if pattern == "/tmp/foo/*-1/backups" {
					return []string{"/tmp/foo/foo-1/backups", "/tmp/foo/foo-1/backups"}, nil
				} else {
					return []string{}, nil
				}
			}
			_, _, err := ParseSegPrefix("/tmp/foo", "00000000000000")
			Expect(err.Error()).To(Equal("Multiple backup directories in /tmp/foo"))
		})
		Describe("IsValidTimestamp", func() {
			It("allows a valid timestamp", func() {
				timestamp := "20170101010101"
				isValid := IsValidTimestamp(timestamp)
				Expect(isValid).To(BeTrue())
			})
			It("invalidates a non-numeric timestamp", func() {
				timestamp := "2017ababababab"
				isValid := IsValidTimestamp(timestamp)
				Expect(isValid).To(BeFalse())
			})
			It("invalidates a timestamp that is too short", func() {
				timestamp := "201701010101"
				isValid := IsValidTimestamp(timestamp)
				Expect(isValid).To(BeFalse())
			})
			It("invalidates a timestamp that is too long", func() {
				timestamp := "2017010101010101"
				isValid := IsValidTimestamp(timestamp)
				Expect(isValid).To(BeFalse())
			})
		})
	})
})
