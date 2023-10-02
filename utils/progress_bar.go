package utils

/*
 * This file contains structs and functions related to logging.
 */

import (
	"fmt"
	"time"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
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
	return &InfoProgressBar{ProgressBar: progressBar}
}

type ProgressBar interface {
	Start() *pb.ProgressBar
	Finish()
	Prefix(string) *pb.ProgressBar
	Increment() int
	Add(int) int
	Set(int) *pb.ProgressBar
	Reset(int) *pb.ProgressBar
	GetBar() *pb.ProgressBar
}

type InfoProgressBar struct {
	*pb.ProgressBar
}

func (ipb *InfoProgressBar) GetBar() *pb.ProgressBar {
	return ipb.ProgressBar
}

type VerboseProgressBar struct {
	current            int
	total              int
	prefix             string
	nextPercentToPrint int
	ShouldPrintCount   bool
	*pb.ProgressBar
}

func NewVerboseProgressBar(count int, prefix string) *VerboseProgressBar {
	newPb := VerboseProgressBar{total: count, prefix: prefix, nextPercentToPrint: INCR_PERCENT, ShouldPrintCount: true}
	return &newPb
}

func (vpb *VerboseProgressBar) GetBar() *pb.ProgressBar {
	return vpb.ProgressBar
}

func (vpb *VerboseProgressBar) Increment() int {
	if vpb.current < vpb.total {
		vpb.current++
		vpb.checkPercent()
	}
	return vpb.ProgressBar.Increment()
}

func (vpb *VerboseProgressBar) Add(add int) int {
	if vpb.current < vpb.total {
		vpb.current += add
		vpb.checkPercent()
	}
	return vpb.ProgressBar.Add(add)
}

func (vpb *VerboseProgressBar) Set(current int) *pb.ProgressBar {
	vpb.current = current
	return vpb.ProgressBar.Set(current)
}

func (vpb *VerboseProgressBar) Reset(total int) *pb.ProgressBar {
	vpb.current = 0
	vpb.total = total
	return vpb.ProgressBar.Reset(total)
}

func (vpb *VerboseProgressBar) Prefix(prefix string) *pb.ProgressBar {
	vpb.prefix = prefix
	return vpb.ProgressBar.Prefix(prefix)
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
		message := fmt.Sprintf("%s %d%%%%", vpb.prefix, vpb.nextPercentToPrint)
		if vpb.ShouldPrintCount {
			message += fmt.Sprintf(" (%d/%d)", vpb.current, vpb.total)
		}
		gplog.Verbose(message)
		vpb.nextPercentToPrint += INCR_PERCENT
	}
}

type MultiProgressBar struct {
	TablesBar    ProgressBar
	TuplesBars   []ProgressBar
	Verbosity    int
	ProgressPool *pb.Pool
}

func NewMultiProgressBar(tablesBarTotal int, tablesBarLabel string, numTuplesBars int, verbose bool) *MultiProgressBar {
	verbosity := PB_INFO
	if verbose {
		verbosity = PB_VERBOSE
	}
	tablesBar := NewProgressBar(tablesBarTotal, tablesBarLabel, verbosity)
	tablesBar.GetBar().ShowFinalTime = false
	tablesBar.GetBar().NotPrint = verbose
	progressBars := &MultiProgressBar{
		TablesBar: tablesBar,
		Verbosity: verbosity,
	}
	if numTuplesBars > 0 {
		progressBars.TuplesBars = make([]ProgressBar, numTuplesBars)
		// In order to get all of the progress bars to play nicely with one another we have to
		// create and start all of them in a single pool upfront, then edit their prefixes and
		// totals later, instead of starting them one by one when each COPY is ready, so we don't
		// initialize prefixes or totals here.
		for i := 0; i < numTuplesBars; i++ {
			tupleBar := NewProgressBar(0, "", verbosity)
			tupleBar.GetBar().ShowFinalTime = false
			tupleBar.GetBar().NotPrint = verbose
			if verbose {
				tupleBar.(*VerboseProgressBar).ShouldPrintCount = false
			}
			progressBars.TuplesBars[i] = tupleBar
		}
	}
	return progressBars
}

func (mpb *MultiProgressBar) Start() error {
	if mpb.Verbosity == PB_VERBOSE {
		// We're not printing the bars, so this is a no-op
		return nil
	}
	bars := []*pb.ProgressBar{mpb.TablesBar.GetBar()}
	for _, bar := range mpb.TuplesBars {
		bars = append(bars, bar.GetBar())
	}
	pool, err := pb.StartPool(bars...)
	if err != nil {
		return err
	}
	mpb.ProgressPool = pool
	// Calling Start on a bar with a 0 total disables printing percentages, so we need
	// to explicitly reenable that here.  Also, we don't want to print tuple counts, as
	// on the backup side we're only using estimated tuple counts and don't want users
	// to think we're backing up the wrong number of tuples, so we disable the built-in
	// counters on these bars.
	for _, bar := range mpb.TuplesBars {
		bar.GetBar().ShowPercent = true
		bar.GetBar().ShowCounters = false
	}
	return nil
}

func (mpb *MultiProgressBar) Finish() {
	if mpb.Verbosity == PB_VERBOSE {
		// We're not printing the bars, so this is a no-op
		return
	}
	mpb.TablesBar.Finish()
	for _, bar := range mpb.TuplesBars {
		bar.Finish()
	}
	mpb.ProgressPool.Stop()
}

func (mpb *MultiProgressBar) TrackCopyProgress(tablename string, oid uint32, numTuples int, conn *dbconn.DBConn, whichConn int, tableNum int, done chan bool) {

	progressBar := mpb.TuplesBars[tableNum]
	progressBar.Prefix(fmt.Sprintf("%s: ", tablename))

	if oid == 0 {
		intOid, err := dbconn.SelectInt(conn, fmt.Sprintf("SELECT '%s'::regclass::oid", EscapeSingleQuotes(tablename)), whichConn)
		if err != nil {
			// Don't block the COPY for an error here, just note it and skip the progress bar for this table
			gplog.Verbose("Could not retrieve oid for table %s, not printing COPY progress: %v", tablename, err)
			return
		}
		oid = uint32(intOid)
	}

	// For performance reasons, we only get an estimate of tuple count; reltuples will only be populated
	// if the table has been analyzed or there is an index on the table, but we can safely assume that
	// will be true in any real backup scenario.
	if numTuples < 0 {
		var err error
		numTuples, err = dbconn.SelectInt(conn, fmt.Sprintf("SELECT reltuples::bigint FROM pg_class WHERE oid = %d", oid), whichConn)
		if err != nil {
			// Don't block the COPY for an error here, just note it and skip the progress bar for this table
			gplog.Verbose("Could not retrieve tuple count for table %s, not printing COPY progress: %v", tablename, err)
			return
		}
	}

	// We call Finish and Reset at the beginning instead of the end because we start the whole process
	// with empty progress bars that need to be set and there's no need to special-case those.
	progressBar.Finish()
	progressBar.Reset(numTuples)

	processed := 0
	for {
		select {
		case succeeded := <-done:
			if succeeded {
				// Manually set the progress to maximum if COPY succeeded, as we won't be able to get the last few tuples
				// from the view (or any tuples, for especially small tables) and we don't want users to worry that any
				// tuples were missed.
				progressBar.Set(numTuples)
			}
			return
		default:
			// Ignore any error on this query: if there's a hiccup we'll get the number on the next pass, and if there's
			// a real connection problem the COPY itself should error out elsewhere.
			tuplesProcessed, _ := dbconn.SelectInt(conn, fmt.Sprintf("SELECT tuples_processed FROM gp_stat_progress_copy_summary WHERE relid = %d", oid), whichConn)
			if tuplesProcessed == 0 { // Don't stop tracking yet, sometimes the table hiccups and has no rows for a bit
				break
			}
			delta := int(tuplesProcessed) - processed
			// TODO: There's a server bug that underreports tuples so it looks like the number processed went down.
			// For now, if we see a negative delta, simply don't update the progress bar and wait for the next loop.
			if delta < 0 {
				break
			}
			progressBar.Add(delta)
			processed += delta
		}
		time.Sleep(100 * time.Millisecond)
	}
}
