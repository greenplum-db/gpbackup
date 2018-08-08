package restore

/*
 * This file contains structs and functions related to backing up data on the segments.
 */

import (
	"fmt"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpbackup/utils"
	"github.com/pkg/errors"
)

var (
	tableDelim = ","
)

func CopyTableIn(connection *dbconn.DBConn, tableName string, tableAttributes string, backupFile string, singleDataFile bool, whichConn int) int64 {
	whichConn = connection.ValidateConnNum(whichConn)
	usingCompression, compressionProgram := utils.GetCompressionParameters()
	copyCommand := ""
	if singleDataFile {
		copyCommand = fmt.Sprintf("PROGRAM 'cat %s'", backupFile)
	} else if usingCompression && !singleDataFile {
		copyCommand = fmt.Sprintf("PROGRAM '%s < %s'", compressionProgram.DecompressCommand, backupFile)
	} else {
		copyCommand = fmt.Sprintf("'%s'", backupFile)
	}
	query := fmt.Sprintf("COPY %s%s FROM %s WITH CSV DELIMITER '%s' ON SEGMENT;", tableName, tableAttributes, copyCommand, tableDelim)
	result, err := connection.Exec(query, whichConn)
	if err != nil {
		errMsg := fmt.Sprintf("Error loading data into table %s", tableName)
		if MustGetFlagBool(utils.ON_ERROR_CONTINUE) {
			gplog.Error("%s: %v", errMsg, err)
		} else {
			gplog.Fatal(err, errMsg)
		}
	}
	numRows, err := result.RowsAffected()
	gplog.FatalOnError(err)
	return numRows
}

func restoreSingleTableData(fpInfo *utils.FilePathInfo, entry utils.MasterDataEntry, tableNum uint32, totalTables int, whichConn int) {
	name := utils.MakeFQN(entry.Schema, entry.Name)
	if gplog.GetVerbosity() > gplog.LOGINFO {
		// No progress bar at this log level, so we note table count here
		gplog.Verbose("Reading data for table %s from file (table %d of %d)", name, tableNum, totalTables)
	} else {
		gplog.Verbose("Reading data for table %s from file", name)
	}
	backupFile := ""
	if backupConfig.SingleDataFile {
		backupFile = fmt.Sprintf("%s_%d", fpInfo.GetSegmentPipePathForCopyCommand(), entry.Oid)
	} else {
		backupFile = fpInfo.GetTableBackupFilePathForCopyCommand(entry.Oid, backupConfig.SingleDataFile)
	}
	numRowsRestored := CopyTableIn(connectionPool, name, entry.AttributeString, backupFile, backupConfig.SingleDataFile, whichConn)
	numRowsBackedUp := entry.RowsCopied
	CheckRowsRestored(numRowsRestored, numRowsBackedUp, name)
}

func CheckRowsRestored(rowsRestored int64, rowsBackedUp int64, tableName string) {
	if rowsRestored != rowsBackedUp {
		rowsErrMsg := fmt.Sprintf("Expected to restore %d rows to table %s, but restored %d instead", rowsBackedUp, tableName, rowsRestored)
		if MustGetFlagBool(utils.ON_ERROR_CONTINUE) {
			gplog.Error(rowsErrMsg)
		} else {
			agentErr := CheckAgentErrorsOnSegments()
			if agentErr != nil {
				gplog.Error(rowsErrMsg)
				gplog.Fatal(agentErr, "")
			}
			gplog.Fatal(errors.Errorf("%s", rowsErrMsg), "")
		}
	}
}
