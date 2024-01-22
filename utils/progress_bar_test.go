package utils_test

import (
	"io"
	"os"
	"os/user"
	"time"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/utils"
	"gopkg.in/cheggaaa/pb.v1"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("utils/log tests", func() {
	var (
		fakeInfo os.FileInfo
	)

	BeforeEach(func() {
		err := operating.System.MkdirAll("/tmp/log_dir", 0755)
		Expect(err).ToNot(HaveOccurred())
		fakeInfo, err = os.Stat("/tmp/log_dir")
		Expect(err).ToNot(HaveOccurred())

		operating.System.OpenFileWrite = func(name string, flag int, perm os.FileMode) (io.WriteCloser, error) { return buffer, nil }
		operating.System.CurrentUser = func() (*user.User, error) { return &user.User{Username: "testUser", HomeDir: "testDir"}, nil }
		operating.System.Getpid = func() int { return 0 }
		operating.System.Hostname = func() (string, error) { return "testHost", nil }
		operating.System.IsNotExist = func(err error) bool { return false }
		operating.System.Now = func() time.Time { return time.Date(2017, time.January, 1, 1, 1, 1, 1, time.Local) }
		operating.System.Stat = func(name string) (os.FileInfo, error) { return fakeInfo, nil }
	})
	AfterEach(func() {
		operating.System = operating.InitializeSystemFunctions()
	})

	Describe("NewProgressBar", func() {
		Context("PB_NONE", func() {
			It("will not print when passed a none value", func() {
				progressBar := utils.NewProgressBar(10, "test progress bar", utils.PB_NONE)
				infoPb, ok := progressBar.(*pb.ProgressBar)
				Expect(ok).To(BeTrue())
				Expect(infoPb.NotPrint).To(Equal(true))
			})
		})
		Context("PB_INFO", func() {
			It("will create a pb.ProgressBar when passed an info value", func() {
				progressBar := utils.NewProgressBar(10, "test progress bar", utils.PB_INFO)
				_, ok := progressBar.(*pb.ProgressBar)
				Expect(ok).To(BeTrue())
			})
			It("will not print with verbosity LOGERROR", func() {
				gplog.SetVerbosity(gplog.LOGERROR)
				progressBar := utils.NewProgressBar(10, "test progress bar", utils.PB_INFO)
				infoPb, _ := progressBar.(*pb.ProgressBar)
				Expect(infoPb.NotPrint).To(Equal(true))
			})
			It("will print with verbosity LOGINFO", func() {
				gplog.SetVerbosity(gplog.LOGINFO)
				progressBar := utils.NewProgressBar(10, "test progress bar", utils.PB_INFO)
				infoPb, _ := progressBar.(*pb.ProgressBar)
				Expect(infoPb.NotPrint).To(Equal(false))
			})
			It("will not print with verbosity LOGVERBOSE", func() {
				gplog.SetVerbosity(gplog.LOGVERBOSE)
				progressBar := utils.NewProgressBar(10, "test progress bar", utils.PB_INFO)
				infoPb, _ := progressBar.(*pb.ProgressBar)
				Expect(infoPb.NotPrint).To(Equal(true))
			})
		})
		Context("PB_VERBOSE", func() {
			It("will create a verboseProgressBar when passed a verbose value", func() {
				progressBar := utils.NewProgressBar(10, "test progress bar", utils.PB_VERBOSE)
				_, ok := progressBar.(*utils.VerboseProgressBar)
				Expect(ok).To(BeTrue())
			})
			It("verboseProgressBar's infoPb will not print with verbosity LOGERROR", func() {
				gplog.SetVerbosity(gplog.LOGERROR)
				progressBar := utils.NewProgressBar(10, "test progress bar", utils.PB_VERBOSE)
				vPb, _ := progressBar.(*utils.VerboseProgressBar)
				Expect(vPb.ProgressBar.NotPrint).To(Equal(true))
			})
			It("verboseProgressBar's infoPb will print with verbosity LOGINFO", func() {
				gplog.SetVerbosity(gplog.LOGINFO)
				progressBar := utils.NewProgressBar(10, "test progress bar", utils.PB_VERBOSE)
				vPb, _ := progressBar.(*utils.VerboseProgressBar)
				Expect(vPb.ProgressBar.NotPrint).To(Equal(false))
			})
			It("verboseProgressBar's infoPb will not print with verbosity LOGVERBOSE", func() {
				gplog.SetVerbosity(gplog.LOGVERBOSE)
				progressBar := utils.NewProgressBar(10, "test progress bar", utils.PB_VERBOSE)
				vPb, _ := progressBar.(*utils.VerboseProgressBar)
				Expect(vPb.ProgressBar.NotPrint).To(Equal(true))
			})
		})
	})
	Describe("Increment", func() {
		var vPb *utils.VerboseProgressBar
		BeforeEach(func() {
			progressBar := utils.NewProgressBar(10, "test progress bar:", utils.PB_VERBOSE)
			vPb, _ = progressBar.(*utils.VerboseProgressBar)
		})
		It("writes to the log file at 10% increments", func() {
			progressBar := utils.NewProgressBar(10, "test progress bar:", utils.PB_VERBOSE)
			vPb, _ = progressBar.(*utils.VerboseProgressBar)
			vPb.Increment()
			expectedMessage := "test progress bar: 10% (1/10)"
			testhelper.ExpectRegexp(logfile, expectedMessage)
			vPb.Increment()
			expectedMessage = "test progress bar: 20% (2/10)"
			testhelper.ExpectRegexp(logfile, expectedMessage)
		})
		It("only logs when it hits a new % marker", func() {
			progressBar := utils.NewProgressBar(20, "test progress bar:", utils.PB_VERBOSE)
			vPb, _ = progressBar.(*utils.VerboseProgressBar)

			expectedMessage := "test progress bar: 10% (2/20)"
			vPb.Increment()
			testhelper.NotExpectRegexp(logfile, expectedMessage)
			vPb.Increment()
			testhelper.ExpectRegexp(logfile, expectedMessage)
			expectedMessage = "test progress bar: 20% (4/20)"
			vPb.Increment()
			testhelper.NotExpectRegexp(logfile, expectedMessage)
			vPb.Increment()
			testhelper.ExpectRegexp(logfile, expectedMessage)
		})
		It("writes accurate percentages if < 10 items", func() {
			progressBar := utils.NewProgressBar(5, "test progress bar:", utils.PB_VERBOSE)
			vPb, _ = progressBar.(*utils.VerboseProgressBar)
			vPb.Increment()
			expectedMessage := "test progress bar: 20% (1/5)"
			testhelper.ExpectRegexp(logfile, expectedMessage)
			vPb.Increment()
			expectedMessage = "test progress bar: 40% (2/5)"
			testhelper.ExpectRegexp(logfile, expectedMessage)
		})
		It("does not log if called again after hitting 100%", func() {
			progressBar := utils.NewProgressBar(1, "test progress bar:", utils.PB_VERBOSE)
			vPb, _ = progressBar.(*utils.VerboseProgressBar)
			vPb.Increment()
			expectedMessage := "test progress bar: 100% (1/1)"
			testhelper.ExpectRegexp(logfile, expectedMessage)
			vPb.Increment()
			testhelper.NotExpectRegexp(logfile, expectedMessage)
		})
	})
})
