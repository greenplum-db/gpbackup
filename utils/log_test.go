package utils_test

import (
	"fmt"
	"io"
	"os"
	"os/user"
	"reflect"
	"time"

	pb "gopkg.in/cheggaaa/pb.v1"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
)

var _ = Describe("utils/log tests", func() {
	var (
		sampleLogger *gplog.Logger
		fakeInfo     os.FileInfo
	)

	BeforeEach(func() {
		err := utils.System.MkdirAll("/tmp/log_dir", 0755)
		Expect(err).ToNot(HaveOccurred())
		fakeInfo, err = os.Stat("/tmp/log_dir")
		Expect(err).ToNot(HaveOccurred())

		utils.System.OpenFileWrite = func(name string, flag int, perm os.FileMode) (io.WriteCloser, error) { return buffer, nil }
		utils.System.CurrentUser = func() (*user.User, error) { return &user.User{Username: "testUser", HomeDir: "testDir"}, nil }
		utils.System.Getpid = func() int { return 0 }
		utils.System.Hostname = func() (string, error) { return "testHost", nil }
		utils.System.IsNotExist = func(err error) bool { return false }
		utils.System.Now = func() time.Time { return time.Date(2017, time.January, 1, 1, 1, 1, 1, time.Local) }
		utils.System.Stat = func(name string) (os.FileInfo, error) { return fakeInfo, nil }
	})
	AfterEach(func() {
		utils.System = utils.InitializeSystemFunctions()
		utils.SetLogger(logger)
	})

	Describe("InitializeLogging", func() {
		BeforeEach(func() {
			sampleLogger = gplog.NewLogger(os.Stdout, os.Stderr, buffer, "testDir/gpAdminLogs/testProgram_20170101.log",
				gplog.LOGINFO, "testProgram")
		})
		Context("Logger initialized with default log directory and Info log level", func() {
			It("creates a new logger writing to gpAdminLogs and sets utils.logger to this new logger", func() {
				newLogger := utils.InitializeLogging("testProgram", "")
				if !reflect.DeepEqual(newLogger, sampleLogger) {
					Fail(fmt.Sprintf("Created logger does not match sample logger:\n%v\n%v", newLogger, sampleLogger))
				}
			})
		})
		Context("Logger initialized with a specified log directory and Info log level", func() {
			It("creates a new logger writing to the specified log directory and sets utils.logger to this new logger", func() {
				sampleLogger = gplog.NewLogger(os.Stdout, os.Stderr, buffer, "/tmp/log_dir/testProgram_20170101.log",
					gplog.LOGINFO, "testProgram")
				newLogger := utils.InitializeLogging("testProgram", "/tmp/log_dir")
				if !reflect.DeepEqual(newLogger, sampleLogger) {
					Fail(fmt.Sprintf("Created logger does not match sample logger:\n%v\n%v", newLogger, sampleLogger))
				}
			})
		})
		Context("Directory or log file does not exist or is not writable", func() {
			It("creates a log directory if given a nonexistent log directory", func() {
				calledWith := ""
				utils.System.IsNotExist = func(err error) bool { return true }
				utils.System.Stat = func(name string) (os.FileInfo, error) {
					calledWith = name
					return fakeInfo, errors.New("file does not exist")
				}
				utils.InitializeLogging("testProgram", "/tmp/log_dir")
				Expect(calledWith).To(Equal("/tmp/log_dir"))
			})
			It("creates a log file if given a nonexistent log file", func() {
				calledWith := ""
				utils.System.OpenFileWrite = func(name string, flag int, perm os.FileMode) (io.WriteCloser, error) {
					calledWith = name
					return buffer, nil
				}
				utils.System.IsNotExist = func(err error) bool { return true }
				utils.System.Stat = func(name string) (os.FileInfo, error) { return fakeInfo, errors.New("file does not exist") }
				utils.InitializeLogging("testProgram", "/tmp/log_dir")
				Expect(calledWith).To(Equal("/tmp/log_dir/testProgram_20170101.log"))
			})
			It("panics if given a non-writable log directory", func() {
				utils.System.Stat = func(name string) (os.FileInfo, error) { return fakeInfo, errors.New("permission denied") }
				defer testutils.ShouldPanicWithMessage("permission denied")
				utils.InitializeLogging("testProgram", "/tmp/log_dir")
			})
			It("panics if given a non-writable log file", func() {
				utils.System.OpenFileWrite = func(name string, flag int, perm os.FileMode) (io.WriteCloser, error) {
					return nil, errors.New("permission denied")
				}
				defer testutils.ShouldPanicWithMessage("permission denied")
				utils.InitializeLogging("testProgram", "/tmp/log_dir")
			})
		})
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
				logger.SetVerbosity(gplog.LOGERROR)
				progressBar := utils.NewProgressBar(10, "test progress bar", utils.PB_INFO)
				infoPb, _ := progressBar.(*pb.ProgressBar)
				Expect(infoPb.NotPrint).To(Equal(true))
			})
			It("will print with verbosity LOGINFO", func() {
				logger.SetVerbosity(gplog.LOGINFO)
				progressBar := utils.NewProgressBar(10, "test progress bar", utils.PB_INFO)
				infoPb, _ := progressBar.(*pb.ProgressBar)
				Expect(infoPb.NotPrint).To(Equal(false))
			})
			It("will not print with verbosity LOGVERBOSE", func() {
				logger.SetVerbosity(gplog.LOGVERBOSE)
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
				logger.SetVerbosity(gplog.LOGERROR)
				progressBar := utils.NewProgressBar(10, "test progress bar", utils.PB_VERBOSE)
				vPb, _ := progressBar.(*utils.VerboseProgressBar)
				Expect(vPb.ProgressBar.NotPrint).To(Equal(true))
			})
			It("verboseProgressBar's infoPb will print with verbosity LOGINFO", func() {
				logger.SetVerbosity(gplog.LOGINFO)
				progressBar := utils.NewProgressBar(10, "test progress bar", utils.PB_VERBOSE)
				vPb, _ := progressBar.(*utils.VerboseProgressBar)
				Expect(vPb.ProgressBar.NotPrint).To(Equal(false))
			})
			It("verboseProgressBar's infoPb will not print with verbosity LOGVERBOSE", func() {
				logger.SetVerbosity(gplog.LOGVERBOSE)
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
			testutils.ExpectRegexp(logfile, expectedMessage)
			vPb.Increment()
			expectedMessage = "test progress bar: 20% (2/10)"
			testutils.ExpectRegexp(logfile, expectedMessage)
		})
		It("only logs when it hits a new % marker", func() {
			progressBar := utils.NewProgressBar(20, "test progress bar:", utils.PB_VERBOSE)
			vPb, _ = progressBar.(*utils.VerboseProgressBar)

			expectedMessage := "test progress bar: 10% (2/20)"
			vPb.Increment()
			testutils.NotExpectRegexp(logfile, expectedMessage)
			vPb.Increment()
			testutils.ExpectRegexp(logfile, expectedMessage)
			expectedMessage = "test progress bar: 20% (4/20)"
			vPb.Increment()
			testutils.NotExpectRegexp(logfile, expectedMessage)
			vPb.Increment()
			testutils.ExpectRegexp(logfile, expectedMessage)
		})
		It("writes accurate percentages if < 10 items", func() {
			progressBar := utils.NewProgressBar(5, "test progress bar:", utils.PB_VERBOSE)
			vPb, _ = progressBar.(*utils.VerboseProgressBar)
			vPb.Increment()
			expectedMessage := "test progress bar: 20% (1/5)"
			testutils.ExpectRegexp(logfile, expectedMessage)
			vPb.Increment()
			expectedMessage = "test progress bar: 40% (2/5)"
			testutils.ExpectRegexp(logfile, expectedMessage)
		})
		It("does not log if called again after hitting 100%", func() {
			progressBar := utils.NewProgressBar(1, "test progress bar:", utils.PB_VERBOSE)
			vPb, _ = progressBar.(*utils.VerboseProgressBar)
			vPb.Increment()
			expectedMessage := "test progress bar: 100% (1/1)"
			testutils.ExpectRegexp(logfile, expectedMessage)
			vPb.Increment()
			testutils.NotExpectRegexp(logfile, expectedMessage)
		})
	})
})
