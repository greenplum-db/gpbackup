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
)

func executeStatementsForConn(statements chan utils.StatementWithType, fatalErr *error, numErrors *int32, progressBar utils.ProgressBar, whichConn int) {
	var mutex = &sync.Mutex{}
	for statement := range statements {
		if wasTerminated || *fatalErr != nil {
			return
		}
		_, err := connectionPool.Exec(statement.Statement, whichConn)
		if err != nil {
			gplog.Verbose("Error encountered when executing statement: %s Error was: %s", strings.TrimSpace(statement.Statement), err.Error())
			if MustGetFlagBool(utils.ON_ERROR_CONTINUE) {
				atomic.AddInt32(numErrors, 1)
				mutex.Lock()
				errorTablesMetadata[statement.Schema + "." + statement.Name] = Empty{}
				mutex.Unlock()
			} else {
				*fatalErr = err
			}
		}
		progressBar.Increment()
	}
}

/*
 * This function creates a worker pool of N goroutines to be able to execute up
 * to N statements in parallel.
 */
func ExecuteStatements(statements []utils.StatementWithType, progressBar utils.ProgressBar, executeInParallel bool, whichConn ...int) {
	var workerPool sync.WaitGroup
	var fatalErr error
	var numErrors int32
	tasks := make(chan utils.StatementWithType, len(statements))
	for _, statement := range statements {
		tasks <- statement
	}
	close(tasks)

	if !executeInParallel {
		connNum := connectionPool.ValidateConnNum(whichConn...)
		executeStatementsForConn(tasks, &fatalErr, &numErrors, progressBar, connNum)
	} else {
		for i := 0; i < connectionPool.NumConns; i++ {
			workerPool.Add(1)
			go func(connNum int) {
				defer workerPool.Done()
				connNum = connectionPool.ValidateConnNum(connNum)
				executeStatementsForConn(tasks, &fatalErr, &numErrors, progressBar, connNum)
			}(i)
		}
		workerPool.Wait()
	}
	if fatalErr != nil {
		fmt.Println("")
		gplog.Fatal(fatalErr, "")
	} else if numErrors > 0 {
		fmt.Println("")
		gplog.Error("Encountered %d errors during metadata restore; see log file %s for a list of failed statements.", numErrors, gplog.GetLogFilePath())
	}
}

func ExecuteStatementsAndCreateProgressBar(statements []utils.StatementWithType, objectsTitle string, showProgressBar int, executeInParallel bool, whichConn ...int) {
	progressBar := utils.NewProgressBar(len(statements), fmt.Sprintf("%s restored: ", objectsTitle), showProgressBar)
	progressBar.Start()
	ExecuteStatements(statements, progressBar, executeInParallel, whichConn...)
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
	indexMap := make(map[string]bool)
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
