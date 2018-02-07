package restore

/*
 * This file contains functions related to executing multiple SQL statements in parallel.
 */

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpbackup/utils"
	"github.com/pkg/errors"
)

/*
 * The return value for this function is the number of errors encountered, not
 * an error code.
 */
func executeStatement(statement utils.StatementWithType, showProgressBar int, shouldExecute *utils.FilterSet, whichConn int) uint32 {
	whichConn = connection.ValidateConnNum(whichConn)
	if shouldExecute.MatchesFilter(statement.ObjectType) {
		_, err := connection.Exec(statement.Statement, whichConn)
		if err != nil {
			if showProgressBar >= utils.PB_INFO && gplog.GetVerbosity() == gplog.LOGINFO {
				fmt.Println() // Move error message to its own line, since the cursor is currently at the end of the progress bar
			}
			gplog.Verbose("Error encountered when executing statement: %s Error was: %s", strings.TrimSpace(statement.Statement), err.Error())
			if *onErrorContinue {
				return 1
			}
			gplog.Fatal(errors.Errorf("%s; see log file %s for details.", err.Error(), gplog.GetLogFilePath()), "Failed to execute statement")
		}
	}
	return 0
}

/*
 * This function creates a worker pool of N goroutines to be able to execute up
 * to N statements in parallel.
 */
func ExecuteStatements(statements []utils.StatementWithType, progressBar utils.ProgressBar, showProgressBar int, shouldExecute *utils.FilterSet, executeInParallel bool, whichConn ...int) {
	var numErrors uint32
	if !executeInParallel {
		connNum := connection.ValidateConnNum(whichConn...)
		for _, statement := range statements {
			numErrors += executeStatement(statement, showProgressBar, shouldExecute, connNum)
			progressBar.Increment()
		}
	} else {
		tasks := make(chan utils.StatementWithType, len(statements))
		var workerPool sync.WaitGroup
		for i := 0; i < connection.NumConns; i++ {
			workerPool.Add(1)
			go func(whichConn int) {
				for statement := range tasks {
					atomic.AddUint32(&numErrors, executeStatement(statement, showProgressBar, shouldExecute, whichConn))
					progressBar.Increment()
				}
				workerPool.Done()
			}(i)
		}
		for _, statement := range statements {
			tasks <- statement
		}
		close(tasks)
		workerPool.Wait()
	}
	if numErrors > 0 {
		gplog.Error("Encountered %d errors during metadata restore; see log file %s for a list of failed statements.", numErrors, gplog.GetLogFilePath())
	}
}

func ExecuteStatementsAndCreateProgressBar(statements []utils.StatementWithType, objectsTitle string, showProgressBar int, shouldExecute *utils.FilterSet, executeInParallel bool, whichConn ...int) {
	progressBar := utils.NewProgressBar(len(statements), fmt.Sprintf("%s restored: ", objectsTitle), showProgressBar)
	progressBar.Start()
	ExecuteStatements(statements, progressBar, showProgressBar, shouldExecute, executeInParallel, whichConn...)
	progressBar.Finish()
}

/*
 *   There is an existing bug in Greenplum where creating indexes in parallel
 *   on an AO table that didn't have any indexes previously can cause
 *   deadlock.
 *
 *   We work around this issue by restoring post data objects in
 *   two batches. The first batch takes one index from each table and
 *   restores them in parallel (which has no possibility of deadlock) and
 *   then the second restores all other postdata objects in parallel. After
 *   each table has at least one index, there is no more risk of deadlock.
 */
func BatchPostdataStatements(statements []utils.StatementWithType) ([]utils.StatementWithType, []utils.StatementWithType) {
	indexMap := make(map[string]bool, 0)
	firstBatch := make([]utils.StatementWithType, 0)
	secondBatch := make([]utils.StatementWithType, 0)
	for _, statement := range statements {
		_, tableIndexPresent := indexMap[statement.ReferenceObject]
		if statement.ObjectType == "INDEX" && !tableIndexPresent {
			indexMap[statement.ReferenceObject] = true
			firstBatch = append(firstBatch, statement)
		} else {
			secondBatch = append(secondBatch, statement)
		}
	}
	return firstBatch, secondBatch
}
