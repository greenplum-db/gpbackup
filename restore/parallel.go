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
	"github.com/greenplum-db/gpbackup/options"
	"github.com/greenplum-db/gpbackup/toc"
	"github.com/greenplum-db/gpbackup/utils"
)

var (
	mutex   = &sync.Mutex{}
	txMutex = &sync.Mutex{}
)

func executeStatementsForConn(statements chan toc.StatementWithType, fatalErr *error, numErrors *int32, progressBar utils.ProgressBar, whichConn int, executeInParallel bool) {
	for statement := range statements {
		if wasTerminated || *fatalErr != nil {
			if executeInParallel {
				gplog.Error("Error detected on connection %d. Terminating transactions.", whichConn)
				txMutex.Lock()
				if connectionPool.Tx[whichConn] != nil {
					connectionPool.Rollback(whichConn)
				}
				txMutex.Unlock()
			}
			return
		}

		gplog.Debug("Executing statement: %s on connection: %d", strings.TrimSpace(statement.Statement), whichConn)
		_, err := connectionPool.Exec(statement.Statement, whichConn)
		if err != nil {
			gplog.Verbose("Error encountered when executing statement: %s Error was: %s", strings.TrimSpace(statement.Statement), err.Error())
			if MustGetFlagBool(options.ON_ERROR_CONTINUE) {
				if executeInParallel {
					atomic.AddInt32(numErrors, 1)
					if statement.ObjectType == toc.OBJ_TABLE {
						mutex.Lock()
						errorTablesMetadata[statement.Schema+"."+statement.Name] = Empty{}
						mutex.Unlock()
					}
				} else {
					*numErrors = *numErrors + 1
					if statement.ObjectType == toc.OBJ_TABLE {
						errorTablesMetadata[statement.Schema+"."+statement.Name] = Empty{}
					}
				}
			} else {
				*fatalErr = err
			}
		}
		if progressBar != nil {
			progressBar.Increment()
		}
	}

	if executeInParallel {
		txMutex.Lock()
		if connectionPool.Tx[whichConn] != nil {
			connectionPool.Commit(whichConn)
		}
		txMutex.Unlock()
	}
}

func findLeastBusyConn(connStatementCounts map[int]int) int {
	minStatements := connStatementCounts[1]
	leastBusyConn := 1
	for conn, count := range connStatementCounts {
		if count < minStatements {
			leastBusyConn = conn
			minStatements = count
		}
	}
	return leastBusyConn
}

func scheduleStatementsOnWorkers(statements []toc.StatementWithType, numConns int) map[int][]toc.StatementWithType {
	splitStatements := make(map[int][]toc.StatementWithType, numConns)
	cohortConnAssignments := make(map[uint32]int)

	connStatementCounts := make(map[int]int, numConns)
	for i := 0; i < numConns; i++ {
		connStatementCounts[i] = 0
	}
	// During backup, we track objects into "cohorts" that must be backed up in the same
	// transaction to avoid lock contention and/or deadlocks. Split up statements to
	// connections by cohort.
	for i := 0; i < len(statements); i++ {
		var cohort uint32
		if len(statements[i].Tier) == 0 {
			// older backups will not have a Tier value
			cohort = 0
		} else {
			cohort = statements[i].Tier[1]
		}
		keyConn, alreadyPinned := cohortConnAssignments[cohort]
		if alreadyPinned {
			splitStatements[keyConn] = append(splitStatements[keyConn], statements[i])
			connStatementCounts[keyConn] = connStatementCounts[keyConn] + 1
		} else {
			leastBusyConn := findLeastBusyConn(connStatementCounts)
			cohortConnAssignments[cohort] = leastBusyConn
			splitStatements[leastBusyConn] = append(splitStatements[leastBusyConn], statements[i])
			connStatementCounts[leastBusyConn] = connStatementCounts[leastBusyConn] + 1
		}
	}
	return splitStatements
}

/*
 * This function creates a worker pool of N goroutines to be able to execute up
 * to N statements in parallel.
 */
func ExecutePredataStatements(statements []toc.StatementWithType, progressBar utils.ProgressBar, executeInParallel bool, whichConn ...int) int32 {
	var workerPool sync.WaitGroup
	var fatalErr error
	var numErrors int32

	if len(statements) == 0 {
		return 0
	}
	if !executeInParallel {
		tasks := make(chan toc.StatementWithType, len(statements))
		for _, statement := range statements {
			tasks <- statement
		}
		close(tasks)
		connNum := connectionPool.ValidateConnNum(whichConn...)
		executeStatementsForConn(tasks, &fatalErr, &numErrors, progressBar, connNum, executeInParallel)
	} else {
		panicChan := make(chan error)
		splitStatements := scheduleStatementsOnWorkers(statements, connectionPool.NumConns)
		chanMap := make(map[int]chan toc.StatementWithType, connectionPool.NumConns)
		// preinitialize channels to prevent concurrent read and write
		for i := 0; i < connectionPool.NumConns; i++ {
			chanMap[i] = make(chan toc.StatementWithType, len(splitStatements[i]))
			for _, statement := range splitStatements[i] {
				chanMap[i] <- statement
			}
			close(chanMap[i])
		}
		for i := 0; i < connectionPool.NumConns; i++ {
			workerPool.Add(1)
			go func(connNum int) {
				defer func() {
					if panicErr := recover(); panicErr != nil {
						panicChan <- fmt.Errorf("%v", panicErr)
					}
				}()
				defer workerPool.Done()
				connNum = connectionPool.ValidateConnNum(connNum)
				executeStatementsForConn(chanMap[connNum], &fatalErr, &numErrors, progressBar, connNum, executeInParallel)
			}(i)
		}
		workerPool.Wait()
		// Allow panics to crash from the main process, invoking DoCleanup
		select {
		case err := <-panicChan:
			gplog.Fatal(err, "")
		default:
			// no panic, nothing to do
		}
	}
	if fatalErr != nil {
		fmt.Println("")
		gplog.Fatal(fatalErr, "")
	} else if numErrors > 0 {
		fmt.Println("")
		gplog.Error("Encountered %d errors during metadata restore; see log file %s for a list of failed statements.", numErrors, gplog.GetLogFilePath())
	}

	return numErrors
}

func ExecuteStatements(statements []toc.StatementWithType, progressBar utils.ProgressBar, executeInParallel bool, whichConn ...int) int32 {
	var workerPool sync.WaitGroup
	var fatalErr error
	var numErrors int32
	tasks := make(chan toc.StatementWithType, len(statements))
	for _, statement := range statements {
		tasks <- statement
	}
	close(tasks)

	if !executeInParallel {
		connNum := connectionPool.ValidateConnNum(whichConn...)
		executeStatementsForConn(tasks, &fatalErr, &numErrors, progressBar, connNum, executeInParallel)
	} else {
		panicChan := make(chan error)
		for i := 0; i < connectionPool.NumConns; i++ {
			workerPool.Add(1)
			go func(connNum int) {
				defer func() {
					if panicErr := recover(); panicErr != nil {
						panicChan <- fmt.Errorf("%v", panicErr)
					}
				}()
				defer workerPool.Done()
				connNum = connectionPool.ValidateConnNum(connNum)
				executeStatementsForConn(tasks, &fatalErr, &numErrors, progressBar, connNum, executeInParallel)
			}(i)
		}
		workerPool.Wait()
		// Allow panics to crash from the main process, invoking DoCleanup
		select {
		case err := <-panicChan:
			gplog.Fatal(err, "")
		default:
			// no panic, nothing to do
		}
	}
	if fatalErr != nil {
		fmt.Println("")
		gplog.Fatal(fatalErr, "")
	} else if numErrors > 0 {
		fmt.Println("")
		gplog.Error("Encountered %d errors during metadata restore; see log file %s for a list of failed statements.", numErrors, gplog.GetLogFilePath())
	}

	return numErrors
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
 *
 *   A third batch is created specifically for postdata metadata
 *   (e.g. ALTER INDEX, ALTER EVENT TRIGGER, COMMENT ON). These
 *   statements cannot be concurrently run with batch two since that
 *   is where the dependent postdata objects are being created.
 */
func BatchPostdataStatements(statements []toc.StatementWithType) ([]toc.StatementWithType, []toc.StatementWithType, []toc.StatementWithType) {
	indexMap := make(map[string]bool)
	firstBatch := make([]toc.StatementWithType, 0)
	secondBatch := make([]toc.StatementWithType, 0)
	thirdBatch := make([]toc.StatementWithType, 0)
	for _, statement := range statements {
		_, tableIndexPresent := indexMap[statement.ReferenceObject]
		if statement.ObjectType == toc.OBJ_INDEX && !tableIndexPresent {
			indexMap[statement.ReferenceObject] = true
			firstBatch = append(firstBatch, statement)
		} else if strings.Contains(statement.ObjectType, " METADATA") {
			thirdBatch = append(thirdBatch, statement)
		} else {
			secondBatch = append(secondBatch, statement)
		}
	}
	return firstBatch, secondBatch, thirdBatch
}

func BatchPredataStatements(statements []toc.StatementWithType) ([]toc.StatementWithType, map[uint32][]toc.StatementWithType, []toc.StatementWithType) {
	foundNumberedTier := false
	firstTierZero := make([]toc.StatementWithType, 0)
	numberedTiers := make(map[uint32][]toc.StatementWithType)
	secondTierZero := make([]toc.StatementWithType, 0)

	for _, statement := range statements {
		// For backwards compatibility, assume restores without Tier are always 0,0.  This is also
		// used for some object types like Schemas where we haven't yet gone through the work of
		// tiering them out for parallel execution.
		if len(statement.Tier) == 0 {
			statement.Tier = []uint32{0, 0}
		}

		if statement.Tier[0] > 0 {
			foundNumberedTier = true
			numberedTiers[statement.Tier[0]] = append(numberedTiers[statement.Tier[0]], statement)
		} else if statement.Tier[0] == 0 {
			if !foundNumberedTier {
				// insert into firstTierZero
				firstTierZero = append(firstTierZero, statement)
			} else {
				// insert into secondTierZero
				secondTierZero = append(secondTierZero, statement)
			}
		}
	}
	return firstTierZero, numberedTiers, secondTierZero
}
