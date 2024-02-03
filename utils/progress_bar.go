package utils

/*
 * This file contains structs and functions related to logging.
 */

import (
	"sync"
	"time"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"gopkg.in/cheggaaa/pb.v1"
)

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
	progressBar.NotPrint = !(showProgressBar >= PB_INFO && count > 0 && gplog.GetVerbosity() == gplog.LOGINFO)
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
	Add(int) int
}

type VerboseProgressBar struct {
	current            int
	total              int
	prefix             string
	nextPercentToPrint int
	*pb.ProgressBar
	mu sync.Mutex
}

func NewVerboseProgressBar(count int, prefix string) *VerboseProgressBar {
	newPb := VerboseProgressBar{total: count, prefix: prefix, nextPercentToPrint: INCR_PERCENT}
	return &newPb
}

func (vpb *VerboseProgressBar) Increment() int {
	vpb.mu.Lock()
	defer vpb.mu.Unlock()
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
		gplog.Verbose("%s %d%% (%d/%d)", vpb.prefix, vpb.nextPercentToPrint, vpb.current, vpb.total)
		vpb.nextPercentToPrint += INCR_PERCENT
	}
}
