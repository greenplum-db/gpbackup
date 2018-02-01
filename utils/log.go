package utils

/*
 * This file contains structs and functions related to logging.
 */

import (
	"fmt"
	"os"
	"time"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	pb "gopkg.in/cheggaaa/pb.v1"
)

var (
	logger        *gplog.Logger
	defaultLogDir = "gpAdminLogs"
)

/*
 * This function creates a logger, sets it to global so it's usable in utils/,
 * and then returns it so the same logger can be used in backup/ and restore/.
 */
func InitializeLogging(program string, logdir string) *gplog.Logger {

	// Create a temporary logger to start in case there are fatal errors during initialization
	nullFile, _ := os.Open("/dev/null")
	tempLogger := gplog.NewLogger(os.Stdout, os.Stderr, nullFile, "/dev/null", gplog.LOGINFO, program)
	SetLogger(tempLogger)

	currentUser, _ := System.CurrentUser()
	if logdir == "" {
		logdir = fmt.Sprintf("%s/%s", currentUser.HomeDir, defaultLogDir)
	}

	CreateDirectoryOnMaster(logdir)

	logfile := fmt.Sprintf("%s/%s_%s.log", logdir, program, CurrentTimestamp()[0:8])
	logFileHandle := MustOpenFileForWriting(logfile, true)

	logger := gplog.NewLogger(os.Stdout, os.Stderr, logFileHandle, logfile, gplog.LOGINFO, program)
	SetLogger(logger)
	return logger
}

func SetLogger(log *gplog.Logger) {
	logger = log
}

/*
 * Progress bar functions
 */

/*
 * The following constants are used for determining when to display a progress bar
 *
 * PB_INFO only shows in info mode because some methods have a different way of
 * logging in verbose mode and we don't want them to conflict
 * PB_VERBOSE show a progress bar in INFO and VERBOSE mode
 *
 * A simple incremental progress tracker will be shown in info mode and
 * in verbose mode we will log progress at increments of 10%
 */
const (
	PB_NONE = iota
	PB_INFO
	PB_VERBOSE

	//Verbose progress bar logs every 10 percent
	INCR_PERCENT = 10
)

func NewProgressBar(count int, prefix string, showProgressBar int) ProgressBar {
	progressBar := pb.New(count).Prefix(prefix)
	progressBar.ShowTimeLeft = false
	progressBar.SetMaxWidth(100)
	progressBar.SetRefreshRate(time.Millisecond * 200)
	progressBar.NotPrint = !(showProgressBar >= PB_INFO && count > 0 && logger.GetVerbosity() == gplog.LOGINFO)
	if showProgressBar == PB_VERBOSE {
		verboseProgressBar := NewVerboseProgressBar(count, prefix)
		verboseProgressBar.ProgressBar = progressBar
		return verboseProgressBar
	}
	return progressBar
}

type ProgressBar interface {
	Start() *pb.ProgressBar
	Finish()
	Increment() int
}

type VerboseProgressBar struct {
	current            int
	total              int
	prefix             string
	nextPercentToPrint int
	*pb.ProgressBar
}

func NewVerboseProgressBar(count int, prefix string) *VerboseProgressBar {
	newPb := VerboseProgressBar{total: count, prefix: prefix, nextPercentToPrint: INCR_PERCENT}
	return &newPb
}

func (vpb *VerboseProgressBar) Increment() int {
	vpb.ProgressBar.Increment()
	if vpb.current < vpb.total {
		vpb.current++
		vpb.checkPercent()
	}
	return vpb.current
}

/*
 * If progress bar reaches a percentage that is a multiple of 10, log a message to stdout
 * We increment nextPercentToPrint so the same percentage will not be printed multiple times
 */
func (vpb *VerboseProgressBar) checkPercent() {
	currPercent := int(float64(vpb.current) / float64(vpb.total) * 100)
	//closestMult is the nearest percentage <= currPercent that is a multiple of 10
	closestMult := currPercent / INCR_PERCENT * INCR_PERCENT
	if closestMult >= vpb.nextPercentToPrint {
		vpb.nextPercentToPrint = closestMult
		logger.Verbose("%s %d%% (%d/%d)", vpb.prefix, vpb.nextPercentToPrint, vpb.current, vpb.total)
		vpb.nextPercentToPrint += INCR_PERCENT
	}
}
