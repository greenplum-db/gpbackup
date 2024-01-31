package backup

/*
 * This file contains structs and functions related to backing up data on the segments.
 */

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpbackup/options"
	"github.com/greenplum-db/gpbackup/utils"
	"github.com/jackc/pgconn"
)

var (
	tableDelim   = ","
	progressBars *utils.MultiProgressBar
)

func ConstructTableAttributesList(columnDefs []ColumnDefinition) string {
	// this attribute list used ONLY by CopyTableIn on the restore side
	// columns where data should not be copied out and back in are excluded from this string.
	names := make([]string, 0)
	for _, col := range columnDefs {
		// data in generated columns should not be backed up or restored.
		if col.AttGenerated == "" {
			names = append(names, col.Name)
		}
	}
	if len(names) > 0 {
		return fmt.Sprintf("(%s)", strings.Join(names, ","))
	}
	return ""
}

func AddTableDataEntriesToTOC(tables []Table, rowsCopiedMaps []map[uint32]int64) {
	for _, table := range tables {
		if !table.SkipDataBackup() {
			var rowsCopied int64
			for _, rowsCopiedMap := range rowsCopiedMaps {
				if val, ok := rowsCopiedMap[table.Oid]; ok {
					rowsCopied = val
					break
				}
			}
			attributes := ConstructTableAttributesList(table.ColumnDefs)
			globalTOC.AddCoordinatorDataEntry(table.Schema, table.Name, table.Oid, attributes, rowsCopied, table.PartitionLevelInfo.RootName, table.DistPolicy)
		}
	}
}

func CopyTableOut(connectionPool *dbconn.DBConn, table Table, destinationToWrite string, connNum int) (int64, error) {
	checkPipeExistsCommand := ""
	customPipeThroughCommand := utils.GetPipeThroughProgram().OutputCommand
	sendToDestinationCommand := ">"
	if MustGetFlagBool(options.SINGLE_DATA_FILE) {
		/*
		 * The segment TOC files are always written to the segment data directory for
		 * performance reasons, in case the user-specified directory is on a mounted
		 * drive.  It will be copied to a user-specified directory, if any, once all
		 * of the data is backed up.
		 */
		checkPipeExistsCommand = fmt.Sprintf("(test -p \"%s\" || (echo \"Pipe not found %s\">&2; exit 1)) && ", destinationToWrite, destinationToWrite)
		customPipeThroughCommand = "cat -"
	} else if MustGetFlagString(options.PLUGIN_CONFIG) != "" {
		sendToDestinationCommand = fmt.Sprintf("| %s backup_data %s", pluginConfig.ExecutablePath, pluginConfig.ConfigPath)
	}

	copyCommand := fmt.Sprintf("PROGRAM '%s%s %s %s'", checkPipeExistsCommand, customPipeThroughCommand, sendToDestinationCommand, destinationToWrite)

	columnNames := ""
	if connectionPool.Version.AtLeast("7") {
		// process column names to exclude generated columns from data copy out
		columnNames = ConstructTableAttributesList(table.ColumnDefs)
	}

	query := fmt.Sprintf("COPY %s%s TO %s WITH CSV DELIMITER '%s' ON SEGMENT IGNORE EXTERNAL PARTITIONS;", table.FQN(), columnNames, copyCommand, tableDelim)
	gplog.Verbose("Worker %d: %s", connNum, query)

	var done chan bool
	var trackerTimer *time.Timer
	var endTime time.Time
	shouldTrackProgress := connectionPool.Version.AtLeast("7") && progressBars != nil

	var tableNum int
	if shouldTrackProgress {
		// A true on this channel means the COPY succeeded.  We explicitly signal this on a success, instead
		// of just closing the channel, to allow the progress goroutine to handle cleanup properly based on
		// success or failure.  We don't give the channel a size because we want the send to block, so that
		// we don't stop printing any progress bars before all goroutines are done and possibly make it look
		// like we failed to back up some number of tuples.
		done = make(chan bool, 1)
		defer close(done)

		// Normally, we pass connNum-1 for tableNum because we index into progressBars.TuplesBars using
		// that value (progressBars.TablesBar effectively corresponds to connection 0, so it's off by 1)
		// and use connection 0 to query the copy progress table; however, when dealing with deferred
		// tables, those *must* use connection 0, so we use connection 1 for the query in that case.
		whichConn := 0
		tableNum = connNum - 1
		if connNum == 0 {
			whichConn = 1
			tableNum = 0
		}

		// To prevent undue overhead when backing up many small tables, we only begin tracking
		// progress after 5 seconds have elapsed. If the copy finishes before then, we stop the
		// timer and move on.
		trackerDelay := 5 * time.Second
		trackerTimer = time.AfterFunc(trackerDelay, func() {
			progressBars.TrackCopyProgress(table.FQN(), table.Oid, -1, connectionPool, whichConn, tableNum, done)
		})
		endTime = time.Now().Add(trackerDelay)
	}

	result, err := connectionPool.Exec(query, connNum)
	if err != nil {
		return 0, err
	}
	numRows, _ := result.RowsAffected()

	if shouldTrackProgress {
		if time.Now().Before(endTime) {
			trackerTimer.Stop()
		}
		// send signal to channel whether tracking or not, just to avoid race condition weirdness
		done <- true

		// Manually set the progress to maximum if COPY succeeded, as we won't be able to get the last few tuples
		// from the view (or any tuples, for especially small tables) and we don't want users to worry that any
		// tuples were missed.
		progressBar := progressBars.TuplesBars[tableNum]
		progressBar.Set(progressBars.TuplesCounts[tableNum])
	}
	return numRows, nil
}

func BackupSingleTableData(table Table, rowsCopiedMap map[uint32]int64, progressBars *utils.MultiProgressBar, whichConn int) error {
	logMessage := fmt.Sprintf("Worker %d: Writing data for table %s to file", whichConn, table.FQN())
	// Avoid race condition by incrementing progressBars in call to sprintf
	current := progressBars.TablesBar.GetBar().Get()
	tableCount := fmt.Sprintf(" (table %d of %d)", atomic.AddInt64(&current, 1), progressBars.TablesBar.GetBar().Total)
	if gplog.GetVerbosity() > gplog.LOGINFO {
		// No progress bar at this log level, so we note table count here
		gplog.Verbose(logMessage + tableCount)
	} else {
		gplog.Verbose(logMessage)
	}

	destinationToWrite := ""
	if MustGetFlagBool(options.SINGLE_DATA_FILE) {
		destinationToWrite = fmt.Sprintf("%s_%d", globalFPInfo.GetSegmentPipePathForCopyCommand(), table.Oid)
	} else {
		destinationToWrite = globalFPInfo.GetTableBackupFilePathForCopyCommand(table.Oid, utils.GetPipeThroughProgram().Extension, false)
	}
	rowsCopied, err := CopyTableOut(connectionPool, table, destinationToWrite, whichConn)
	if err != nil {
		return err
	}
	rowsCopiedMap[table.Oid] = rowsCopied
	progressBars.TablesBar.Increment()
	return nil
}

/*
* Iterate through tables, backup data from each table in set.
* If supported by the database, a synchronized database snapshot is used to sync
* all parallel workers' view of the database and ensure data consistency across workers.
* In each worker, we try to acquire a ACCESS SHARE lock on each table in NOWAIT mode,
* to avoid potential deadlocks with queued DDL operations that requested ACCESS EXCLUSIVE
* locks on the same tables (for eg concurrent ALTER TABLE operations). If worker is unable to
* acquire a lock, defer table to worker 0, which holds all ACCESS SHARE locks for the backup set.
* If synchronized snapshot is not supported and worker is unable to acquire a lock, the
* worker must be terminated because the session no longer has a valid distributed snapshot
*
* FIXME: Simplify BackupDataForAllTables by having one function for snapshot workflow and
* another without, then extract common portions into their own functions.
 */
func BackupDataForAllTables(tables []Table) []map[uint32]int64 {
	numTuplesBars := 0
	if connectionPool.Version.AtLeast("7") {
		numTuplesBars = connectionPool.NumConns - 1
	}
	progressBars = utils.NewMultiProgressBar(len(tables), "Tables backed up: ", numTuplesBars, MustGetFlagBool(options.VERBOSE))
	defer progressBars.Finish()
	err := progressBars.Start()
	gplog.FatalOnError(err)

	rowsCopiedMaps := make([]map[uint32]int64, connectionPool.NumConns)
	/*
	 * We break when an interrupt is received and rely on
	 * TerminateHangingCopySessions to halt any COPY statements
	 * in progress if they don't finish on their own.
	 */
	tasks := make(chan Table, len(tables))
	var oidMap sync.Map
	var isErroredBackup atomic.Bool
	var workerPool sync.WaitGroup
	// Record and track tables in a hashmap of oids and table states (preloaded with value Unknown).
	// The tables are loaded into the tasks channel for the subsequent goroutines to work on.
	for _, table := range tables {
		oidMap.Store(table.Oid, Unknown)
		tasks <- table
	}

	/*
	 * Worker 0 is a special database connection that
	 * 	1) Exports the database snapshot if the feature is supported
	 * 	2) Does not have tables pre-assigned to it.
	 * 	3) Processes tables only in the event that the other workers encounter locking issues.
	 * Worker 0 already has all locks on the tables so it will not run into locking issues.
	 */
	panicChan := make(chan error)
	rowsCopiedMaps[0] = make(map[uint32]int64)
	for connNum := 1; connNum < connectionPool.NumConns; connNum++ {
		rowsCopiedMaps[connNum] = make(map[uint32]int64)
		workerPool.Add(1)
		go func(whichConn int) {
			defer func() {
				if panicErr := recover(); panicErr != nil {
					panicChan <- fmt.Errorf("%v", panicErr)
				}
			}()
			defer workerPool.Done()
			/* If the --leaf-partition-data flag is not set, the parent and all leaf
			 * partition data are treated as a single table and will be assigned to a single worker.
			 * Large partition hierarchies result in a large number of locks being held until the
			 * transaction commits and the locks are released.
			 */
			for table := range tasks {
				if wasTerminated || isErroredBackup.Load() {
					progressBars.TablesBar.GetBar().NotPrint = true
					return
				}
				if backupSnapshot != "" && connectionPool.Tx[whichConn] == nil {
					err := SetSynchronizedSnapshot(connectionPool, whichConn, backupSnapshot)
					gplog.FatalOnError(err)
				}
				// If a random external SQL command had queued an AccessExclusiveLock acquisition
				// request against this next table, the --job worker thread would deadlock on the
				// COPY attempt. To prevent gpbackup from hanging, we attempt to acquire an
				// AccessShareLock on the relation with the NOWAIT option before we run COPY. If
				// the LOCK TABLE NOWAIT call fails, we catch the error and defer the table to the
				// main worker thread, worker 0. Afterwards, if we are in a local transaction
				// instead of a distributed snapshot, we break early and terminate the worker since
				// its transaction is now in an aborted state. We do not need to do this with the
				// main worker thread because it has already acquired AccessShareLocks on all
				// tables before the metadata dumping part.
				err := LockTableNoWait(table, whichConn)
				if err != nil {
					if pgErr, ok := err.(*pgconn.PgError); ok && pgErr.Code != PG_LOCK_NOT_AVAILABLE {
						isErroredBackup.Store(true)
						err = connectionPool.Rollback(whichConn)
						if err != nil {
							gplog.Warn("Worker %d: %s", whichConn, err)
						}
						gplog.Fatal(fmt.Errorf("Unexpectedly unable to take lock on table %s, %s", table.FQN(), pgErr.Error()), "")
					}
					if gplog.GetVerbosity() < gplog.LOGVERBOSE {
						// Add a newline to interrupt the progress bar so that
						// the following WARN message is nicely outputted.
						fmt.Printf("\n")
					}
					gplog.Warn("Worker %d could not acquire AccessShareLock for table %s.", whichConn, table.FQN())
					logTableLocks(table, whichConn)
					// rollback transaction and defer table
					err = connectionPool.Rollback(whichConn)
					if err != nil {
						gplog.Warn("Worker %d: %s", whichConn, err)
					}
					oidMap.Store(table.Oid, Deferred)
					// If have backup snapshot, continue to next table, else terminate worker.
					if backupSnapshot != "" {
						continue
					} else {
						gplog.Warn("Terminating worker %d and deferring table %s to main worker thread.", whichConn, table.FQN())
						break
					}
				}
				err = BackupSingleTableData(table, rowsCopiedMaps[whichConn], progressBars, whichConn)
				if err != nil {
					// if copy isn't working, skip remaining backups, and let downstream panic
					// handling deal with it
					progressBars.TablesBar.GetBar().NotPrint = true
					isErroredBackup.Store(true)
					gplog.Fatal(err, "")
				} else {
					oidMap.Store(table.Oid, Complete)
				}
				if backupSnapshot != "" {
					err = connectionPool.Commit(whichConn)
					if err != nil {
						gplog.Warn("Worker %d: %s", whichConn, err)
					}
				}
			}
		}(connNum)
	}

	// Special goroutine to handle deferred tables
	// Handle all tables deferred by the deadlock detection. This can only be
	// done with the main worker thread, worker 0, because it has
	// AccessShareLocks on all the tables already.
	deferredWorkerDone := make(chan bool)
	go func() {
		defer func() {
			if panicErr := recover(); panicErr != nil {
				panicChan <- fmt.Errorf("%v", panicErr)
			}
		}()
		for _, table := range tables {
			for {
				if wasTerminated || isErroredBackup.Load() {
					return
				}
				state, _ := oidMap.Load(table.Oid)
				if state.(int) == Unknown {
					time.Sleep(time.Millisecond * 50)
				} else if state.(int) == Deferred {
					err := BackupSingleTableData(table, rowsCopiedMaps[0], progressBars, 0)
					if err != nil {
						isErroredBackup.Store(true)
						gplog.Fatal(err, "")
					}
					oidMap.Store(table.Oid, Complete)
					break
				} else if state.(int) == Complete {
					break
				} else {
					gplog.Fatal(errors.New("Encountered unknown table state"), "")
				}
			}
		}
		deferredWorkerDone <- true
	}()

	close(tasks)
	workerPool.Wait()

	// Allow panics to crash from the main process, invoking DoCleanup
	select {
	case err := <-panicChan:
		gplog.Fatal(err, "")
	default:
		// no panic, nothing to do
	}

	// If not using synchronized snapshots,
	// check if all workers were terminated due to lock issues.
	if backupSnapshot == "" {
		allWorkersTerminatedLogged := false
		for _, table := range tables {
			if wasTerminated || isErroredBackup.Load() {
				progressBars.TablesBar.GetBar().NotPrint = true
				break
			}
			state, _ := oidMap.Load(table.Oid)
			if state == Unknown {
				if !allWorkersTerminatedLogged {
					gplog.Warn("All workers terminated due to lock issues. Falling back to single main worker.")
					allWorkersTerminatedLogged = true
				}
				oidMap.Store(table.Oid, Deferred)
			}
		}
	}

	// Main goroutine waits for deferred worker 0 by waiting on this channel
	<-deferredWorkerDone
	agentErr := utils.CheckAgentErrorsOnSegments(globalCluster, globalFPInfo)
	if agentErr != nil {
		gplog.Fatal(agentErr, "")
	}

	return rowsCopiedMaps
}

func printDataBackupWarnings(numExtTables int64) {
	if numExtTables > 0 {
		gplog.Info("Skipped data backup of %d external/foreign table(s).", numExtTables)
		gplog.Info("See %s for a complete list of skipped tables.", gplog.GetLogFilePath())
	}
}

// Remove external/foreign tables from the data backup set
func GetBackupDataSet(tables []Table) ([]Table, int64) {
	var backupDataSet []Table
	var numExtOrForeignTables int64

	if !backupReport.MetadataOnly {
		for _, table := range tables {
			if !table.SkipDataBackup() {
				backupDataSet = append(backupDataSet, table)
			} else {
				gplog.Verbose("Skipping data backup of table %s because it is either an external or foreign table.", table.FQN())
				numExtOrForeignTables++
			}
		}
	}
	return backupDataSet, numExtOrForeignTables
}

// Acquire AccessShareLock on a table with NOWAIT option. If we are unable to acquire
// the lock, the call will fail instead of block. Return the failure for handling.
func LockTableNoWait(dataTable Table, connNum int) error {
	var lockMode string
	if connectionPool.Version.AtLeast("7") {
		lockMode = `IN ACCESS SHARE MODE NOWAIT COORDINATOR ONLY`
	} else if connectionPool.Version.AtLeast("6.21.0") {
		lockMode = `IN ACCESS SHARE MODE NOWAIT MASTER ONLY`
	} else {
		lockMode = `IN ACCESS SHARE MODE NOWAIT`
	}
	query := fmt.Sprintf("LOCK TABLE %s %s;", dataTable.FQN(), lockMode)
	gplog.Verbose("Worker %d: %s", connNum, query)
	_, err := connectionPool.Exec(query, connNum)
	if err != nil {
		return err
	}
	return nil
}
