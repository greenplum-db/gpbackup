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

func CopyTableIn(connection *dbconn.DBConn, tableName string, tableAttributes string, destinationToRead string, singleDataFile bool, whichConn int) (int64, error) {
	whichConn = connection.ValidateConnNum(whichConn)
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
	result, err := connection.Exec(query, whichConn)
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
	CheckRowsRestored(numRowsRestored, numRowsBackedUp, name)
	return nil
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
