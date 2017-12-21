package restore

/*
 * This file contains functions related to executing multiple SQL statements in parallel.
 */

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

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
			if showProgressBar >= utils.PB_INFO && logger.GetVerbosity() == utils.LOGINFO {
				fmt.Println() // Move error message to its own line, since the cursor is currently at the end of the progress bar
			}
			logger.Verbose("Error encountered when executing statement: %s Error was: %s", strings.TrimSpace(statement.Statement), err.Error())
			if *onErrorContinue {
				return 1
			}
			logger.Fatal(errors.Errorf("%s; see log file %s for details.", err.Error(), logger.GetLogFilePath()), "Failed to execute statement")
		}
	}
	return 0
}

/*
 * This function creates a worker pool of N goroutines to be able to execute up
 * to N statements in parallel.
 */
func ExecuteStatements(statements []utils.StatementWithType, objectsTitle string, showProgressBar int, shouldExecute *utils.FilterSet, executeInParallel bool, whichConn ...int) {
	var numErrors uint32
	progressBar := utils.NewProgressBar(len(statements), fmt.Sprintf("%s restored: ", objectsTitle), showProgressBar)
	progressBar.Start()
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
			/*
			 * Attempting to execute certain statements such as CREATE INDEX on the same table
			 * at the same time can cause a deadlock due to conflicting Access Exclusive locks,
			 * so we add a small delay between statements to avoid the issue.
			 */
			time.Sleep(20 * time.Millisecond)
		}
		close(tasks)
		workerPool.Wait()
	}
	progressBar.Finish()
	if numErrors > 0 {
		logger.Error("Encountered %d errors during metadata restore; see log file %s for a list of failed statements.", numErrors, logger.GetLogFilePath())
	}
}
