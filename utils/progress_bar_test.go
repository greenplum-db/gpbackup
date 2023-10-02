package utils_test

import (
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/utils"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("utils/log tests", func() {
	Describe("NewProgressBar", func() {
		Context("PB_NONE", func() {
			It("will not print when passed a none value", func() {
				infoPb := utils.NewProgressBar(10, "test progress bar", utils.PB_NONE)
				Expect(infoPb.GetBar().NotPrint).To(Equal(true))
			})
		})
		Context("PB_INFO", func() {
			It("will create an InfoProgressBar when passed an info value", func() {
				progressBar := utils.NewProgressBar(10, "test progress bar", utils.PB_INFO)
				_, ok := progressBar.(*utils.InfoProgressBar)
				Expect(ok).To(BeTrue())
			})
			It("will not print with verbosity LOGERROR", func() {
				gplog.SetVerbosity(gplog.LOGERROR)
				infoPb := utils.NewProgressBar(10, "test progress bar", utils.PB_INFO)
				Expect(infoPb.GetBar().NotPrint).To(Equal(true))
			})
			It("will print with verbosity LOGINFO", func() {
				gplog.SetVerbosity(gplog.LOGINFO)
				infoPb := utils.NewProgressBar(10, "test progress bar", utils.PB_INFO)
				Expect(infoPb.GetBar().NotPrint).To(Equal(false))
			})
			It("will not print with verbosity LOGVERBOSE", func() {
				gplog.SetVerbosity(gplog.LOGVERBOSE)
				infoPb := utils.NewProgressBar(10, "test progress bar", utils.PB_INFO)
				Expect(infoPb.GetBar().NotPrint).To(Equal(true))
			})
		})
		Context("PB_VERBOSE", func() {
			It("will create a VerboseProgressBar when passed a verbose value", func() {
				progressBar := utils.NewProgressBar(10, "test progress bar", utils.PB_VERBOSE)
				_, ok := progressBar.(*utils.VerboseProgressBar)
				Expect(ok).To(BeTrue())
			})
			It("verboseProgressBar's infoPb will not print with verbosity LOGERROR", func() {
				gplog.SetVerbosity(gplog.LOGERROR)
				vPb := utils.NewProgressBar(10, "test progress bar", utils.PB_VERBOSE)
				Expect(vPb.GetBar().NotPrint).To(Equal(true))
			})
			It("verboseProgressBar's infoPb will print with verbosity LOGINFO", func() {
				gplog.SetVerbosity(gplog.LOGINFO)
				vPb := utils.NewProgressBar(10, "test progress bar", utils.PB_VERBOSE)
				Expect(vPb.GetBar().NotPrint).To(Equal(false))
			})
			It("verboseProgressBar's infoPb will not print with verbosity LOGVERBOSE", func() {
				gplog.SetVerbosity(gplog.LOGVERBOSE)
				vPb := utils.NewProgressBar(10, "test progress bar", utils.PB_VERBOSE)
				Expect(vPb.GetBar().NotPrint).To(Equal(true))
			})
		})
	})
	Describe("Increment", func() {
		It("writes to the log file at 10% increments", func() {
			vPb := utils.NewProgressBar(10, "test progress bar:", utils.PB_VERBOSE)
			vPb.Increment()
			expectedMessage := "test progress bar: 10% (1/10)"
			testhelper.ExpectRegexp(logfile, expectedMessage)
			vPb.Increment()
			expectedMessage = "test progress bar: 20% (2/10)"
			testhelper.ExpectRegexp(logfile, expectedMessage)
		})
		It("only logs when it hits a new % marker", func() {
			vPb := utils.NewProgressBar(20, "test progress bar:", utils.PB_VERBOSE)

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
			vPb := utils.NewProgressBar(5, "test progress bar:", utils.PB_VERBOSE)
			vPb.Increment()
			expectedMessage := "test progress bar: 20% (1/5)"
			testhelper.ExpectRegexp(logfile, expectedMessage)
			vPb.Increment()
			expectedMessage = "test progress bar: 40% (2/5)"
			testhelper.ExpectRegexp(logfile, expectedMessage)
		})
		It("does not log if called again after hitting 100%", func() {
			vPb := utils.NewProgressBar(1, "test progress bar:", utils.PB_VERBOSE)
			vPb.Increment()
			expectedMessage := "test progress bar: 100% (1/1)"
			testhelper.ExpectRegexp(logfile, expectedMessage)
			vPb.Increment()
			testhelper.NotExpectRegexp(logfile, expectedMessage)
		})
		It("does not log the count if ShouldPrintCount is false", func() {
			vPb := utils.NewProgressBar(1, "test progress bar:", utils.PB_VERBOSE)
			vPb.(*utils.VerboseProgressBar).ShouldPrintCount = false
			vPb.Increment()
			expectedMessage := "test progress bar: 100%"
			testhelper.ExpectRegexp(logfile, expectedMessage)
		})
	})
	Describe("NewMultiProgressBar", func() {
		It("can create a MultiProgressBar with info verbosity", func() {
			multiPb := utils.NewMultiProgressBar(10, "test multi progress bar", 2, false)

			Expect(multiPb.TablesBar).ToNot(BeNil())
			Expect(multiPb.TuplesBars).ToNot(BeNil())
			Expect(len(multiPb.TuplesBars)).To(Equal(2))
			infoTablesBar, ok := multiPb.TablesBar.(*utils.InfoProgressBar)
			Expect(ok).To(BeTrue())
			Expect(infoTablesBar.GetBar().NotPrint).To(BeFalse())
			infoTuplesBar1, ok := multiPb.TuplesBars[0].(*utils.InfoProgressBar)
			Expect(ok).To(BeTrue())
			Expect(infoTuplesBar1.GetBar().NotPrint).To(BeFalse())
			infoTuplesBar2, ok := multiPb.TuplesBars[1].(*utils.InfoProgressBar)
			Expect(ok).To(BeTrue())
			Expect(infoTuplesBar2.GetBar().NotPrint).To(BeFalse())

			Expect(multiPb.TablesBar.GetBar().Get()).To(Equal(int64(0)))
			Expect(multiPb.Verbosity).To(Equal(utils.PB_INFO))
			Expect(multiPb.ProgressPool).To(BeNil())
		})
		It("can create a MultiProgressBar with verbose verbosity", func() {
			multiPb := utils.NewMultiProgressBar(10, "test multi progress bar", 2, true)

			Expect(multiPb.TablesBar).ToNot(BeNil())
			Expect(multiPb.TuplesBars).ToNot(BeNil())
			Expect(len(multiPb.TuplesBars)).To(Equal(2))
			verboseTablesBar, ok := multiPb.TablesBar.(*utils.VerboseProgressBar)
			Expect(ok).To(BeTrue())
			Expect(verboseTablesBar.GetBar().NotPrint).To(BeTrue())
			verboseTuplesBar1, ok := multiPb.TuplesBars[0].(*utils.VerboseProgressBar)
			Expect(ok).To(BeTrue())
			Expect(verboseTuplesBar1.GetBar().NotPrint).To(BeTrue())
			verboseTuplesBar2, ok := multiPb.TuplesBars[1].(*utils.VerboseProgressBar)
			Expect(ok).To(BeTrue())
			Expect(verboseTuplesBar2.GetBar().NotPrint).To(BeTrue())

			Expect(multiPb.TablesBar.GetBar().Get()).To(Equal(int64(0)))
			Expect(multiPb.Verbosity).To(Equal(utils.PB_VERBOSE))
			Expect(multiPb.ProgressPool).To(BeNil())
		})
		It("can handle a MultiProgressBar without tuple progress bars", func() {
			multiPb := utils.NewMultiProgressBar(10, "test multi progress bar", 0, false)

			Expect(multiPb.TablesBar).ToNot(BeNil())
			Expect(multiPb.TuplesBars).To(BeNil())
			_, ok := multiPb.TablesBar.(*utils.InfoProgressBar)
			Expect(ok).To(BeTrue())

			Expect(multiPb.TablesBar.GetBar().Get()).To(Equal(int64(0)))
			Expect(multiPb.ProgressPool).To(BeNil())
		})
	})
	Describe("GetBar", func() {
		It("returns a modifiable pb.ProgressBar struct", func() {
			infoPb := utils.NewProgressBar(10, "test progress bar", utils.PB_NONE)
			Expect(infoPb.GetBar().NotPrint).To(BeTrue())
			infoPb.GetBar().NotPrint = false
			Expect(infoPb.GetBar().NotPrint).To(BeFalse())
		})
	})
	Describe("TrackCopyProgress", func() {
		var (
			row10       *sqlmock.Rows
			row20       *sqlmock.Rows
			row30       *sqlmock.Rows
			row40       *sqlmock.Rows
			row50       *sqlmock.Rows
			rowNegative *sqlmock.Rows
			rowEmpty    *sqlmock.Rows
			mpb         *utils.MultiProgressBar
			done        chan bool
		)
		BeforeEach(func() {
			row10 = sqlmock.NewRows([]string{"tuples_processed"}).AddRow("10")
			row20 = sqlmock.NewRows([]string{"tuples_processed"}).AddRow("20")
			row30 = sqlmock.NewRows([]string{"tuples_processed"}).AddRow("30")
			row40 = sqlmock.NewRows([]string{"tuples_processed"}).AddRow("40")
			row50 = sqlmock.NewRows([]string{"tuples_processed"}).AddRow("50")
			rowNegative = sqlmock.NewRows([]string{"tuples_processed"}).AddRow("-25")
			rowEmpty = sqlmock.NewRows([]string{"tuples_processed"})
			mpb = utils.NewMultiProgressBar(0, "test multi progress bar", 1, true)
			done = make(chan bool, 1)
		})
		It("tracks a table's progress from start to finish", func() {
			mock.ExpectQuery("SELECT .*").WillReturnRows(row10)
			mock.ExpectQuery("SELECT .*").WillReturnRows(row20)
			mock.ExpectQuery("SELECT .*").WillReturnRows(row30)
			mock.ExpectQuery("SELECT .*").WillReturnRows(row40)
			mock.ExpectQuery("SELECT .*").WillReturnRows(row50)

			go mpb.TrackCopyProgress("public.foo", 1, 50, connectionPool, 0, 0, done)
			time.Sleep(time.Second)
			done <- true
			time.Sleep(time.Second)
			close(done)

			tupleBar := mpb.TuplesBars[0].GetBar()
			Expect(tupleBar.Total).To(Equal(int64(50)))
			Expect(tupleBar.Get()).To(Equal(int64(50)))
			testhelper.ExpectRegexp(logfile, "public.foo:  20%")
			testhelper.ExpectRegexp(logfile, "public.foo:  40%")
			testhelper.ExpectRegexp(logfile, "public.foo:  60%")
			testhelper.ExpectRegexp(logfile, "public.foo:  80%")
			testhelper.ExpectRegexp(logfile, "public.foo:  100%")
		})
		It("handles a table so small it never appears in gp_stat_progress_copy_summary", func() {
			go mpb.TrackCopyProgress("public.foo", 1, 50, connectionPool, 0, 0, done)
			done <- true
			time.Sleep(time.Second)
			close(done)

			tupleBar := mpb.TuplesBars[0].GetBar()
			Expect(tupleBar.Total).To(Equal(int64(50)))
			Expect(tupleBar.Get()).To(Equal(int64(50)))
			testhelper.NotExpectRegexp(logfile, "public.foo:")
		})
		It("handles an instance of the database returning a negative tuple count", func() {
			mock.ExpectQuery("SELECT .*").WillReturnRows(row10)
			mock.ExpectQuery("SELECT .*").WillReturnRows(row20)
			mock.ExpectQuery("SELECT .*").WillReturnRows(rowNegative)
			mock.ExpectQuery("SELECT .*").WillReturnRows(row30)
			mock.ExpectQuery("SELECT .*").WillReturnRows(row40)
			mock.ExpectQuery("SELECT .*").WillReturnRows(row50)

			go mpb.TrackCopyProgress("public.foo", 1, 50, connectionPool, 0, 0, done)
			time.Sleep(time.Second)
			done <- true
			time.Sleep(time.Second)
			close(done)

			tupleBar := mpb.TuplesBars[0].GetBar()
			Expect(tupleBar.Total).To(Equal(int64(50)))
			Expect(tupleBar.Get()).To(Equal(int64(50)))
			testhelper.ExpectRegexp(logfile, "public.foo:  20%")
			testhelper.ExpectRegexp(logfile, "public.foo:  40%")
			testhelper.ExpectRegexp(logfile, "public.foo:  60%")
			testhelper.ExpectRegexp(logfile, "public.foo:  80%")
			testhelper.ExpectRegexp(logfile, "public.foo:  100%")
		})
		It("handles instances of the table temporarily not appearing in gp_stat_progress_copy_summary", func() {
			mock.ExpectQuery("SELECT tuples_processed .*").WillReturnRows(row10)
			mock.ExpectQuery("SELECT tuples_processed .*").WillReturnRows(row20)
			mock.ExpectQuery("SELECT tuples_processed .*").WillReturnRows(rowEmpty)
			mock.ExpectQuery("SELECT tuples_processed .*").WillReturnRows(rowEmpty)
			mock.ExpectQuery("SELECT tuples_processed .*").WillReturnRows(row30)
			mock.ExpectQuery("SELECT tuples_processed .*").WillReturnRows(row40)

			go mpb.TrackCopyProgress("public.foo", 1, 50, connectionPool, 0, 0, done)
			time.Sleep(time.Second)
			done <- true
			time.Sleep(time.Second)
			close(done)

			tupleBar := mpb.TuplesBars[0].GetBar()
			Expect(tupleBar.Total).To(Equal(int64(50)))
			Expect(tupleBar.Get()).To(Equal(int64(50)))
			testhelper.ExpectRegexp(logfile, "public.foo:  20%")
			testhelper.ExpectRegexp(logfile, "public.foo:  40%")
			testhelper.ExpectRegexp(logfile, "public.foo:  60%")
			testhelper.ExpectRegexp(logfile, "public.foo:  80%")
		})
		It("handles a COPY failure", func() {
			mock.ExpectQuery("SELECT tuples_processed .*").WillReturnRows(row10)
			mock.ExpectQuery("SELECT tuples_processed .*").WillReturnRows(row20)
			mock.ExpectQuery("SELECT tuples_processed .*").WillReturnRows(row30)

			go mpb.TrackCopyProgress("public.foo", 1, 50, connectionPool, 0, 0, done)
			time.Sleep(time.Second)
			close(done)

			tupleBar := mpb.TuplesBars[0].GetBar()
			Expect(tupleBar.Total).To(Equal(int64(50)))
			Expect(tupleBar.Get()).To(Equal(int64(30)))
			testhelper.ExpectRegexp(logfile, "public.foo:  20%")
			testhelper.ExpectRegexp(logfile, "public.foo:  40%")
			testhelper.ExpectRegexp(logfile, "public.foo:  60%")
		})
	})
})
