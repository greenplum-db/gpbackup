package backup

/*
 * This file contains structs and functions related to backing up data on the segments.
 */

import (
	"fmt"
	"strings"

	"sync"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpbackup/utils"
	"gopkg.in/cheggaaa/pb.v1"
)

var (
	tableDelim = ","
)

func ConstructTableAttributesList(columnDefs []ColumnDefinition) string {
	names := make([]string, 0)
	for _, col := range columnDefs {
		names = append(names, col.Name)
	}
	if len(names) > 0 {
		return fmt.Sprintf("(%s)", strings.Join(names, ","))
	}
	return ""
}

func AddTableDataEntriesToTOC(tables []Relation, tableDefs map[uint32]TableDefinition, rowsCopiedMaps []map[uint32]int64) {
	for _, table := range tables {
		if !tableDefs[table.Oid].IsExternal {
			var rowsCopied int64
			for _, rowsCopiedMap := range rowsCopiedMaps {
				if val, ok := rowsCopiedMap[table.Oid]; ok {
					rowsCopied = val
					break
				}
			}
			attributes := ConstructTableAttributesList(tableDefs[table.Oid].ColumnDefs)
			globalTOC.AddMasterDataEntry(table.Schema, table.Name, table.Oid, attributes, rowsCopied)
		}
	}
}

type BackupProgressCounters struct {
	NumRegTables   int64
	TotalRegTables int64
	mutex          sync.Mutex
	ProgressBar    utils.ProgressBar
}

func CopyTableOut(connectionPool *dbconn.DBConn, table Relation, destinationToWrite string, connNum int) (int64, error) {
	checkPipeExistsCommand := ""
	customPipeThroughCommand := utils.GetPipeThroughProgram().OutputCommand
	sendToDestinationCommand := ">"
	if MustGetFlagBool(utils.SINGLE_DATA_FILE) {
		/*
		 * The segment TOC files are always written to the segment data directory for
		 * performance reasons, in case the user-specified directory is on a mounted
		 * drive.  It will be copied to a user-specified directory, if any, once all
		 * of the data is backed up.
		 */
		checkPipeExistsCommand = fmt.Sprintf("(test -p \"%s\" || (echo \"Pipe not found %s\">&2; exit 1)) && ", destinationToWrite, destinationToWrite)
		customPipeThroughCommand = "cat -"
	} else if MustGetFlagString(utils.PLUGIN_CONFIG) != "" {
		sendToDestinationCommand = fmt.Sprintf("| %s backup_data %s", pluginConfig.ExecutablePath, pluginConfig.ConfigPath)
	}

	copyCommand := fmt.Sprintf("PROGRAM '%s%s %s %s'", checkPipeExistsCommand, customPipeThroughCommand, sendToDestinationCommand, destinationToWrite)

	query := fmt.Sprintf("COPY %s TO %s WITH CSV DELIMITER '%s' ON SEGMENT IGNORE EXTERNAL PARTITIONS;", table.ToString(), copyCommand, tableDelim)
	result, err := connectionPool.Exec(query, connNum)
	if err != nil {
		return 0, err
	}
	numRows, _ := result.RowsAffected()
	return numRows, nil
}

func BackupSingleTableData(tableDef TableDefinition, table Relation, rowsCopiedMap map[uint32]int64, counters *BackupProgressCounters, whichConn int) error {
	if !tableDef.IsExternal {
		counters.mutex.Lock()
		counters.NumRegTables++
		numTables := counters.NumRegTables //We save this so it won't be modified before we log it
		counters.mutex.Unlock()
		if gplog.GetVerbosity() > gplog.LOGINFO {
			// No progress bar at this log level, so we note table count here
			gplog.Verbose("Writing data for table %s to file (table %d of %d)", table.ToString(), numTables, counters.TotalRegTables)
		} else {
			gplog.Verbose("Writing data for table %s to file", table.ToString())
		}

		destinationToWrite := ""
		if MustGetFlagBool(utils.SINGLE_DATA_FILE) {
			destinationToWrite = fmt.Sprintf("%s_%d", globalFPInfo.GetSegmentPipePathForCopyCommand(), table.Oid)
		} else {
			destinationToWrite = globalFPInfo.GetTableBackupFilePathForCopyCommand(table.Oid, false)
		}
		rowsCopied, err := CopyTableOut(connectionPool, table, destinationToWrite, whichConn)
		if err != nil {
			return err
		}
		rowsCopiedMap[table.Oid] = rowsCopied
		counters.ProgressBar.Increment()
	} else {
		gplog.Verbose("Skipping data backup of table %s because it is an external table.", table.ToString())
	}
	return nil
}

func BackupDataForAllTables(tables []Relation, tableDefs map[uint32]TableDefinition) []map[uint32]int64 {
	var totalExtTables int64
	for _, table := range tables {
		if tableDefs[table.Oid].IsExternal {
			totalExtTables++
		}
	}
	counters := BackupProgressCounters{NumRegTables: 0, TotalRegTables: int64(len(tables)) - totalExtTables}
	counters.ProgressBar = utils.NewProgressBar(int(counters.TotalRegTables), "Tables backed up: ", utils.PB_INFO)
	counters.ProgressBar.Start()
	rowsCopiedMaps := make([]map[uint32]int64, connectionPool.NumConns)
	/*
	 * We break when an interrupt is received and rely on
	 * TerminateHangingCopySessions to kill any COPY statements
	 * in progress if they don't finish on their own.
	 */
	tasks := make(chan Relation, len(tables))
	var workerPool sync.WaitGroup
	var copyErr error
	for connNum := 0; connNum < connectionPool.NumConns; connNum++ {
		rowsCopiedMaps[connNum] = make(map[uint32]int64, 0)
		workerPool.Add(1)
		go func(whichConn int) {
			defer workerPool.Done()
			for table := range tasks {
				if wasTerminated || copyErr != nil {
					counters.ProgressBar.(*pb.ProgressBar).NotPrint = true
					return
				}
				err := BackupSingleTableData(tableDefs[table.Oid], table, rowsCopiedMaps[whichConn], &counters, whichConn)
				if err != nil {
					copyErr = err
				}
			}
		}(connNum)
	}
	for _, table := range tables {
		tasks <- table
	}
	close(tasks)
	workerPool.Wait()
	if copyErr != nil {
		errStr := ""
		if MustGetFlagBool(utils.SINGLE_DATA_FILE) {
			helperLogName := globalFPInfo.GetHelperLogPath()
			errStr = fmt.Sprintf("Check %s on the affected segment host for more info.", helperLogName)
		}
		gplog.Fatal(copyErr, errStr)
	}
	counters.ProgressBar.Finish()

	printDataBackupWarnings(totalExtTables)
	return rowsCopiedMaps
}

func printDataBackupWarnings(numExtTables int64) {
	if numExtTables > 0 {
		s := ""
		if numExtTables > 1 {
			s = "s"
		}
		gplog.Info("Skipped data backup of %d external table%s.", numExtTables, s)
	}
	if numExtTables > 0 {
		gplog.Info("See %s for a complete list of skipped tables.", gplog.GetLogFilePath())
	}
}

func CheckTablesContainData(tables []Relation, tableDefs map[uint32]TableDefinition) {
	if !backupReport.MetadataOnly {
		for _, table := range tables {
			if !tableDefs[table.Oid].IsExternal {
				return
			}
		}
		gplog.Warn("No tables in backup set contain data. Performing metadata-only backup instead.")
		backupReport.MetadataOnly = true
	}
}
