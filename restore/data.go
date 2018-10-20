package restore

/*
 * This file contains structs and functions related to backing up data on the segments.
 */

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpbackup/utils"
	"github.com/pkg/errors"
	pb "gopkg.in/cheggaaa/pb.v1"
)

var (
	tableDelim = ","
)

func CopyTableIn(connectionPool *dbconn.DBConn, tableName string, tableAttributes string, destinationToRead string, singleDataFile bool, whichConn int) (int64, error) {
	whichConn = connectionPool.ValidateConnNum(whichConn)
	copyCommand := ""
	readFromDestinationCommand := "cat"
	customPipeThroughCommand := utils.GetPipeThroughProgram().InputCommand

	if singleDataFile {
		//helper.go handles compression, so we don't want to set it here
		customPipeThroughCommand = "cat -"
	} else if MustGetFlagString(utils.PLUGIN_CONFIG) != "" {
		readFromDestinationCommand = fmt.Sprintf("%s restore_data %s", pluginConfig.ExecutablePath, pluginConfig.ConfigPath)
	}

	copyCommand = fmt.Sprintf("PROGRAM '%s %s | %s'", readFromDestinationCommand, destinationToRead, customPipeThroughCommand)

	query := fmt.Sprintf("COPY %s%s FROM %s WITH CSV DELIMITER '%s' ON SEGMENT;", tableName, tableAttributes, copyCommand, tableDelim)
	result, err := connectionPool.Exec(query, whichConn)
	if err != nil {
		return 0, errors.Wrap(err, fmt.Sprintf("Error loading data into table %s", tableName))
	}
	numRows, _ := result.RowsAffected()
	return numRows, err
}

func restoreSingleTableData(fpInfo *utils.FilePathInfo, entry utils.MasterDataEntry, tableNum uint32, totalTables int, whichConn int) error {
	name := utils.MakeFQN(entry.Schema, entry.Name)
	if gplog.GetVerbosity() > gplog.LOGINFO {
		// No progress bar at this log level, so we note table count here
		gplog.Verbose("Reading data for table %s from file (table %d of %d)", name, tableNum, totalTables)
	} else {
		gplog.Verbose("Reading data for table %s from file", name)
	}
	destinationToRead := ""
	if backupConfig.SingleDataFile {
		destinationToRead = fmt.Sprintf("%s_%d", fpInfo.GetSegmentPipePathForCopyCommand(), entry.Oid)
	} else {
		destinationToRead = fpInfo.GetTableBackupFilePathForCopyCommand(entry.Oid, backupConfig.SingleDataFile)
	}
	numRowsRestored, err := CopyTableIn(connectionPool, name, entry.AttributeString, destinationToRead, backupConfig.SingleDataFile, whichConn)
	if err != nil {
		return err
	}
	numRowsBackedUp := entry.RowsCopied
	err = CheckRowsRestored(numRowsRestored, numRowsBackedUp, name)
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

func restoreDataFromTimestamp(fpInfo utils.FilePathInfo, dataEntries []utils.MasterDataEntry,
	gucStatements []utils.StatementWithType, dataProgressBar utils.ProgressBar) {
	if len(dataEntries) == 0 {
		gplog.Verbose("No data to restore for timestamp = %s", fpInfo.Timestamp)
		return
	}

	if backupConfig.SingleDataFile {
		gplog.Verbose("Initializing pipes and gpbackup_helper on segments for single data file restore")
		utils.VerifyHelperVersionOnSegments(version, globalCluster)
		filteredOids := make([]string, len(dataEntries))
		for i, entry := range dataEntries {
			filteredOids[i] = fmt.Sprintf("%d", entry.Oid)
		}
		utils.WriteOidListToSegments(filteredOids, globalCluster, fpInfo)
		firstOid := fmt.Sprintf("%d", dataEntries[0].Oid)
		utils.CreateFirstSegmentPipeOnAllHosts(firstOid, globalCluster, fpInfo)
		if wasTerminated {
			return
		}
		utils.StartAgent(globalCluster, fpInfo, "--restore-agent", MustGetFlagString(utils.PLUGIN_CONFIG), "")
	}
	/*
	 * We break when an interrupt is received and rely on
	 * TerminateHangingCopySessions to kill any COPY
	 * statements in progress if they don't finish on their own.
	 */
	var tableNum uint32 = 1
	tasks := make(chan utils.MasterDataEntry, len(dataEntries))
	var workerPool sync.WaitGroup
	var fatalErr error
	var numErrors int32
	for i := 0; i < connectionPool.NumConns; i++ {
		workerPool.Add(1)
		go func(whichConn int) {
			defer workerPool.Done()
			setGUCsForConnection(gucStatements, whichConn)
			for entry := range tasks {
				if wasTerminated || fatalErr != nil {
					dataProgressBar.(*pb.ProgressBar).NotPrint = true
					return
				}
				err := restoreSingleTableData(&fpInfo, entry, tableNum, len(dataEntries), whichConn)
				if err != nil {
					if MustGetFlagBool(utils.ON_ERROR_CONTINUE) {
						gplog.Verbose(err.Error())
						atomic.AddInt32(&numErrors, 1)
					} else {
						fatalErr = err
					}
				}
				atomic.AddUint32(&tableNum, 1)
				dataProgressBar.Increment()
			}
		}(i)
	}
	for _, entry := range dataEntries {
		tasks <- entry
	}
	close(tasks)
	workerPool.Wait()
	if fatalErr != nil {
		gplog.Fatal(fatalErr, "")
	} else if numErrors > 0 {
		gplog.Error("Encountered %d errors during table data restore; see log file %s for a list of table errors.", numErrors, gplog.GetLogFilePath())
	}
	err := utils.CheckAgentErrorsOnSegments(globalCluster, globalFPInfo)
	if err != nil {
		errMsg := "Error restoring data for one or more tables"
		if MustGetFlagBool(utils.ON_ERROR_CONTINUE) {
			gplog.Error("%s: %v", errMsg, err)
		} else {
			gplog.Fatal(err, errMsg)
		}
	}
}
