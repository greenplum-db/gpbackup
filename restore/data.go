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
	"gopkg.in/cheggaaa/pb.v1"
)

var (
	tableDelim       = ","
	resizeBatchMap   map[int]map[int]int
	resizeContentMap map[int]int
)

func CopyTableIn(connectionPool *dbconn.DBConn, tableName string, tableAttributes string, destinationToRead string, singleDataFile bool, whichConn int) (int64, error) {
	whichConn = connectionPool.ValidateConnNum(whichConn)
	copyCommand := ""
	readFromDestinationCommand := "cat"
	customPipeThroughCommand := utils.GetPipeThroughProgram().InputCommand

	resizeCluster := MustGetFlagBool(options.RESIZE_CLUSTER)

	if singleDataFile || resizeCluster {
		//helper.go handles compression, so we don't want to set it here
		customPipeThroughCommand = "cat -"
	} else if MustGetFlagString(options.PLUGIN_CONFIG) != "" {
		readFromDestinationCommand = fmt.Sprintf("%s restore_data %s", pluginConfig.ExecutablePath, pluginConfig.ConfigPath)
	}

	copyCommand = fmt.Sprintf("PROGRAM '%s %s | %s'", readFromDestinationCommand, destinationToRead, customPipeThroughCommand)

	query := fmt.Sprintf("COPY %s%s FROM %s WITH CSV DELIMITER '%s' ON SEGMENT;", tableName, tableAttributes, copyCommand, tableDelim)

	var numRows int64
	var err error

	// During a larger-to-smaller restore, we need multiple COPY passes to load all the data.
	// One pass is sufficient for smaller-to-larger and normal restores.
	batches := 1
	destSize := len(globalCluster.ContentIDs) - 1
	origSize := backupConfig.SegmentCount
	if resizeCluster && origSize > destSize {
		batches = origSize / destSize
		if origSize%destSize != 0 {
			batches += 1
		}
	}
	for i := 0; i < batches; i++ {
		gplog.Verbose(`Executing "%s" on master`, query)
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

	return numRows, err
}

func restoreSingleTableData(fpInfo *filepath.FilePathInfo, entry toc.MasterDataEntry, tableName string, whichConn int) error {
	resizeCluster := MustGetFlagBool(options.RESIZE_CLUSTER)
	destinationToRead := ""
	if backupConfig.SingleDataFile || resizeCluster {
		destinationToRead = fmt.Sprintf("%s_%d", fpInfo.GetSegmentPipePathForCopyCommand(), entry.Oid)
	} else {
		destinationToRead = fpInfo.GetTableBackupFilePathForCopyCommand(entry.Oid, utils.GetPipeThroughProgram().Extension, backupConfig.SingleDataFile)
	}
	gplog.Debug("Reading from %s", destinationToRead)
	numRowsRestored, err := CopyTableIn(connectionPool, tableName, entry.AttributeString, destinationToRead, backupConfig.SingleDataFile, whichConn)
	if err != nil {
		return err
	}
	numRowsBackedUp := entry.RowsCopied
	err = CheckRowsRestored(numRowsRestored, numRowsBackedUp, tableName)
	if err != nil {
		return err
	}
	if resizeCluster {
		gplog.Debug("Redistributing data for %s", tableName)
		err = RedistributeTableData(tableName, whichConn)
	}
	return err
}

func CheckRowsRestored(rowsRestored int64, rowsBackedUp int64, tableName string) error {
	if rowsRestored != rowsBackedUp {
		rowsErrMsg := fmt.Sprintf("Expected to restore %d rows to table %s, but restored %d instead", rowsBackedUp, tableName, rowsRestored)
		return errors.New(rowsErrMsg)
	}
	return nil
}

func RedistributeTableData(tableName string, whichConn int) error {
	query := fmt.Sprintf("ALTER TABLE %s SET WITH (REORGANIZE=true)", tableName)
	_, err := connectionPool.Exec(query, whichConn)
	return err
}

func restoreDataFromTimestamp(fpInfo filepath.FilePathInfo, dataEntries []toc.MasterDataEntry,
	gucStatements []toc.StatementWithType, dataProgressBar utils.ProgressBar) int32 {
	totalTables := len(dataEntries)
	if totalTables == 0 {
		gplog.Verbose("No data to restore for timestamp = %s", fpInfo.Timestamp)
		return 0
	}

	resizeCluster := MustGetFlagBool(options.RESIZE_CLUSTER)
	if backupConfig.SingleDataFile || resizeCluster {
		msg := ""
		if backupConfig.SingleDataFile {
			msg += "single data file "
		}
		origSize := 0
		destSize := 0
		if resizeCluster {
			msg += "resize "
			origSize = backupConfig.SegmentCount
			destSize = len(globalCluster.ContentIDs) - 1
		}
		gplog.Verbose("Initializing pipes and gpbackup_helper on segments for %srestore", msg)
		utils.VerifyHelperVersionOnSegments(version, globalCluster)
		oidList := make([]string, totalTables)
		for i, entry := range dataEntries {
			oidList[i] = fmt.Sprintf("%d", entry.Oid)
		}
		utils.WriteOidListToSegments(oidList, globalCluster, fpInfo)
		initialPipes := CreateInitialSegmentPipes(oidList, globalCluster, connectionPool, fpInfo)
		if wasTerminated {
			return 0
		}
		isFilter := false
		if len(opts.IncludedRelations) > 0 || len(opts.ExcludedRelations) > 0 || len(opts.IncludedSchemas) > 0 || len(opts.ExcludedSchemas) > 0 {
			isFilter = true
		}
		compressStr := " --compression-type %s "
		if !backupConfig.Compressed {
			compressStr = fmt.Sprintf(compressStr, "none")
		} else {
			compressStr = fmt.Sprintf(compressStr, utils.GetPipeThroughProgram().Name)
		}
		utils.StartGpbackupHelpers(globalCluster, fpInfo, "--restore-agent", MustGetFlagString(options.PLUGIN_CONFIG), compressStr, MustGetFlagBool(options.ON_ERROR_CONTINUE), isFilter, &wasTerminated, initialPipes, backupConfig.SingleDataFile, origSize, destSize)
	}
	/*
	 * We break when an interrupt is received and rely on
	 * TerminateHangingCopySessions to kill any COPY
	 * statements in progress if they don't finish on their own.
	 */
	var tableNum int64 = 0
	tasks := make(chan toc.MasterDataEntry, totalTables)
	var workerPool sync.WaitGroup
	var numErrors int32
	var mutex = &sync.Mutex{}

	for i := 0; i < connectionPool.NumConns; i++ {
		workerPool.Add(1)
		go func(whichConn int) {
			defer workerPool.Done()

			setGUCsForConnection(gucStatements, whichConn)
			for entry := range tasks {
				if wasTerminated {
					dataProgressBar.(*pb.ProgressBar).NotPrint = true
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
					err = restoreSingleTableData(&fpInfo, entry, tableName, whichConn)

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
						dataProgressBar.(*pb.ProgressBar).NotPrint = true
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

				dataProgressBar.Increment()
			}
		}(i)
	}
	for _, entry := range dataEntries {
		tasks <- entry
	}
	close(tasks)
	workerPool.Wait()

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
