package restore

/*
 * This file contains structs and functions related to backing up data on the segments.
 */

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpbackup/filepath"
	"github.com/greenplum-db/gpbackup/options"
	"github.com/greenplum-db/gpbackup/toc"
	"github.com/greenplum-db/gpbackup/utils"
	"github.com/jackc/pgconn"
	"github.com/pkg/errors"
)

var (
	tableDelim = ","
)

func CopyTableIn(connectionPool *dbconn.DBConn, tableName string, entry toc.CoordinatorDataEntry, destinationToRead string, singleDataFile bool, whichConn int, dataProgressBar *utils.MultiProgressBar) (int64, error) {
	whichConn = connectionPool.ValidateConnNum(whichConn)
	copyCommand := ""
	readFromDestinationCommand := "cat"
	customPipeThroughCommand := utils.GetPipeThroughProgram().InputCommand
	origSize, destSize, resizeCluster := GetResizeClusterInfo()

	if singleDataFile || resizeCluster {
		//helper.go handles compression, so we don't want to set it here
		customPipeThroughCommand = "cat -"
	} else if MustGetFlagString(options.PLUGIN_CONFIG) != "" {
		readFromDestinationCommand = fmt.Sprintf("%s restore_data %s", pluginConfig.ExecutablePath, pluginConfig.ConfigPath)
	}

	copyCommand = fmt.Sprintf("PROGRAM '%s %s | %s'", readFromDestinationCommand, destinationToRead, customPipeThroughCommand)

	query := fmt.Sprintf("COPY %s%s FROM %s WITH CSV DELIMITER '%s' ON SEGMENT;", tableName, entry.AttributeString, copyCommand, tableDelim)

	var done chan bool
	shouldTrackProgress := connectionPool.Version.AtLeast("7") && dataProgressBar != nil

	if shouldTrackProgress {
		done = make(chan bool)
		defer close(done)
		oid, err := dbconn.SelectInt(connectionPool, fmt.Sprintf("SELECT '%s'::regclass::oid", tableName), whichConn)
		if err != nil {
			// Don't block the COPY for an error here, just note it and skip the progress bar for this table
			gplog.Verbose("Could not retrieve oid for table %s, not printing COPY progress: %v", tableName, err)
		} else {
			dataProgressBar.TrackCopyProgress(tableName, uint32(oid), int(entry.RowsCopied), connectionPool, 0, whichConn-1, done)
		}
	}

	var numRows int64
	var err error

	// During a larger-to-smaller restore, we need multiple COPY passes to load all the data.
	// One pass is sufficient for smaller-to-larger and normal restores.
	batches := 1
	if resizeCluster && origSize > destSize {
		batches = origSize / destSize
		if origSize%destSize != 0 {
			batches += 1
		}
	}
	for i := 0; i < batches; i++ {
		if connectionPool.Version.AtLeast("7") {
			gplog.Verbose(`Executing "%s" on coordinator`, query)
		} else {
			gplog.Verbose(`Executing "%s" on master`, query)
		}
		result, err := connectionPool.Exec(query, whichConn)
		if err != nil {
			errStr := fmt.Sprintf("Error loading data into table %s", tableName)

			// The COPY ON SEGMENT error might contain useful CONTEXT output
			if pgErr, ok := err.(*pgconn.PgError); ok && pgErr.Where != "" {
				errStr = fmt.Sprintf("%s: %s", errStr, pgErr.Where)
			}

			return 0, errors.Wrap(err, errStr)
		}
		rowsLoaded, _ := result.RowsAffected()
		numRows += rowsLoaded
	}

	if shouldTrackProgress {
		done <- true
	}
	return numRows, err
}

func restoreSingleTableData(fpInfo *filepath.FilePathInfo, entry toc.CoordinatorDataEntry, tableName string, whichConn int, origSize int, destSize int, dataProgressBar *utils.MultiProgressBar) error {
	resizeCluster := MustGetFlagBool(options.RESIZE_CLUSTER)
	destinationToRead := ""
	if backupConfig.SingleDataFile || resizeCluster {
		destinationToRead = fmt.Sprintf("%s_%d", fpInfo.GetSegmentPipePathForCopyCommand(), entry.Oid)
	} else {
		destinationToRead = fpInfo.GetTableBackupFilePathForCopyCommand(entry.Oid, utils.GetPipeThroughProgram().Extension, backupConfig.SingleDataFile)
	}
	gplog.Debug("Reading from %s", destinationToRead)
	numRowsRestored, err := CopyTableIn(connectionPool, tableName, entry, destinationToRead, backupConfig.SingleDataFile, whichConn, dataProgressBar)
	if err != nil {
		return err
	}
	numRowsBackedUp := entry.RowsCopied

	// For replicated tables, we don't restore second and subsequent batches of data in the larger-to-smaller case,
	// as that would duplicate data, so we have to "scale down" the values to determine whether the correct number
	// of rows was restored
	if entry.IsReplicated && origSize > destSize {
		numRowsBackedUp /= int64(origSize)
		numRowsRestored /= int64(destSize)
	}

	err = CheckRowsRestored(numRowsRestored, numRowsBackedUp, tableName)
	if err != nil {
		return err
	}

	if resizeCluster {
		// replicated tables cannot be redistributed, so instead expand them if needed
		if entry.IsReplicated && (origSize < destSize) {
			err = ExpandReplicatedTable(origSize, tableName, whichConn)
		} else {
			err = RedistributeTableData(tableName, whichConn)
		}
		if err != nil {
			return err
		}
	}
	return err
}

func ExpandReplicatedTable(origSize int, tableName string, whichConn int) error {
	// Replicated tables will only be initially restored to the segments backup was run from, and
	// redistributing does not cause the data to be replicated to the new segments.
	// To work around this, update the distribution policy entry for those tables to the original cluster size
	// and then explicitly expand them to cause the data to be replicated to all new segments.
	gplog.Debug("Distributing replicated data for %s", tableName)
	alterDistPolQuery := fmt.Sprintf("UPDATE gp_distribution_policy SET numsegments=%d WHERE localoid = '%s'::regclass::oid", origSize, tableName)
	_, err := connectionPool.Exec(alterDistPolQuery, whichConn)
	if err != nil {
		return err
	}

	expandTableQuery := fmt.Sprintf("ALTER TABLE %s EXPAND TABLE;", tableName)
	_, err = connectionPool.Exec(expandTableQuery, whichConn)
	if err != nil {
		return err
	}

	return nil
}

func CheckRowsRestored(rowsRestored int64, rowsBackedUp int64, tableName string) error {
	if rowsRestored != rowsBackedUp {
		rowsErrMsg := fmt.Sprintf("Expected to restore %d rows to table %s, but restored %d instead", rowsBackedUp, tableName, rowsRestored)
		return errors.New(rowsErrMsg)
	}
	return nil
}

func RedistributeTableData(tableName string, whichConn int) error {
	gplog.Debug("Redistributing data for %s", tableName)
	query := fmt.Sprintf("ALTER TABLE %s SET WITH (REORGANIZE=true)", tableName)
	_, err := connectionPool.Exec(query, whichConn)
	return err
}

func restoreDataFromTimestamp(fpInfo filepath.FilePathInfo, dataEntries []toc.CoordinatorDataEntry,
	gucStatements []toc.StatementWithType, dataProgressBar *utils.MultiProgressBar) int32 {
	totalTables := len(dataEntries)
	if totalTables == 0 {
		gplog.Verbose("No data to restore for timestamp = %s", fpInfo.Timestamp)
		return 0
	}

	origSize, destSize, resizeCluster := GetResizeClusterInfo()
	if backupConfig.SingleDataFile || resizeCluster {
		msg := ""
		if backupConfig.SingleDataFile {
			msg += "single data file "
		}
		if resizeCluster {
			msg += "resize "
		}
		gplog.Verbose("Initializing pipes and gpbackup_helper on segments for %srestore", msg)
		utils.VerifyHelperVersionOnSegments(version, globalCluster)
		oidList := make([]string, totalTables)
		replicatedOidList := make([]string, 0)
		for i, entry := range dataEntries {
			oidString := fmt.Sprintf("%d", entry.Oid)
			oidList[i] = oidString
			if entry.IsReplicated {
				replicatedOidList = append(replicatedOidList, oidString)
			}
		}
		utils.WriteOidListToSegments(oidList, globalCluster, fpInfo, "oid")
		if len(replicatedOidList) > 0 {
			utils.WriteOidListToSegments(replicatedOidList, globalCluster, fpInfo, "replicated_oid")
		}
		initialPipes := CreateInitialSegmentPipes(oidList, globalCluster, connectionPool, fpInfo)
		if wasTerminated {
			return 0
		}
		isFilter := false
		if len(opts.IncludedRelations) > 0 || len(opts.ExcludedRelations) > 0 || len(opts.IncludedSchemas) > 0 || len(opts.ExcludedSchemas) > 0 {
			isFilter = true
		}
		compressStr := ""
		if backupConfig.Compressed {
			compressStr = fmt.Sprintf(" --compression-type %s ", utils.GetPipeThroughProgram().Name)
		}
		utils.StartGpbackupHelpers(globalCluster, fpInfo, "--restore-agent", MustGetFlagString(options.PLUGIN_CONFIG), compressStr, MustGetFlagBool(options.ON_ERROR_CONTINUE), isFilter, &wasTerminated, initialPipes, backupConfig.SingleDataFile, resizeCluster, origSize, destSize)
	}
	/*
	 * We break when an interrupt is received and rely on
	 * TerminateHangingCopySessions to stop any COPY
	 * statements in progress if they don't finish on their own.
	 */
	var tableNum int64 = 0
	tasks := make(chan toc.CoordinatorDataEntry, totalTables)
	var workerPool sync.WaitGroup
	var numErrors int32
	var mutex = &sync.Mutex{}
	panicChan := make(chan error)

	for i := 1; i < connectionPool.NumConns; i++ {
		workerPool.Add(1)
		go func(whichConn int) {
			defer func() {
				if panicErr := recover(); panicErr != nil {
					panicChan <- fmt.Errorf("%v", panicErr)
				}
			}()
			defer workerPool.Done()

			setGUCsForConnection(gucStatements, whichConn)
			for entry := range tasks {
				if wasTerminated {
					dataProgressBar.TablesBar.GetBar().NotPrint = true
					return
				}
				tableName := utils.MakeFQN(entry.Schema, entry.Name)
				if opts.RedirectSchema != "" {
					tableName = utils.MakeFQN(opts.RedirectSchema, entry.Name)
				}
				// Truncate table before restore, if needed
				var err error
				if MustGetFlagBool(options.INCREMENTAL) || MustGetFlagBool(options.TRUNCATE_TABLE) {
					err = TruncateTable(tableName, whichConn)
				}
				if err == nil {
					err = restoreSingleTableData(&fpInfo, entry, tableName, whichConn, origSize, destSize, dataProgressBar)

					if gplog.GetVerbosity() > gplog.LOGINFO {
						// No progress bar at this log level, so we note table count here
						gplog.Verbose("Restored data to table %s from file (table %d of %d)", tableName, atomic.AddInt64(&tableNum, 1), totalTables)
					} else {
						gplog.Verbose("Restored data to table %s from file", tableName)
					}
				}

				if err != nil {
					gplog.Error(err.Error())
					atomic.AddInt32(&numErrors, 1)
					if !MustGetFlagBool(options.ON_ERROR_CONTINUE) {
						dataProgressBar.TablesBar.GetBar().NotPrint = true
						return
					} else if connectionPool.Version.AtLeast("6") && backupConfig.SingleDataFile {
						// inform segment helpers to skip this entry
						utils.CreateSkipFileOnSegments(fmt.Sprintf("%d", entry.Oid), tableName, globalCluster, globalFPInfo)
					}
					mutex.Lock()
					errorTablesData[tableName] = Empty{}
					mutex.Unlock()
				}

				if backupConfig.SingleDataFile {
					agentErr := utils.CheckAgentErrorsOnSegments(globalCluster, globalFPInfo)
					if agentErr != nil {
						gplog.Error(agentErr.Error())
						return
					}
				}

				dataProgressBar.TablesBar.Increment()
			}
		}(i)
	}
	for _, entry := range dataEntries {
		tasks <- entry
	}
	close(tasks)
	workerPool.Wait()
	// Allow panics to crash from the main process, invoking DoCleanup
	select {
	case err := <-panicChan:
		gplog.Fatal(err, "")
	default:
		// no panic, nothing to do
	}

	if numErrors > 0 {
		fmt.Println("")
		gplog.Error("Encountered %d error(s) during table data restore; see log file %s for a list of table errors.", numErrors, gplog.GetLogFilePath())
	}

	return numErrors
}

func CreateInitialSegmentPipes(oidList []string, c *cluster.Cluster, connectionPool *dbconn.DBConn, fpInfo filepath.FilePathInfo) int {
	// Create min(connections, tables) segment pipes on each host
	var maxPipes int
	if connectionPool.NumConns < len(oidList) {
		maxPipes = connectionPool.NumConns
	} else {
		maxPipes = len(oidList)
	}
	for i := 0; i < maxPipes; i++ {
		utils.CreateSegmentPipeOnAllHosts(oidList[i], c, fpInfo)
	}
	return maxPipes
}
