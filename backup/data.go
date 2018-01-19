package backup

/*
 * This file contains structs and functions related to backing up data on the segments.
 */

import (
	"fmt"
	"strings"

	"github.com/greenplum-db/gpbackup/utils"
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

func AddTableDataEntriesToTOC(tables []Relation, tableDefs map[uint32]TableDefinition) {
	for _, table := range tables {
		if !tableDefs[table.Oid].IsExternal {
			attributes := ConstructTableAttributesList(tableDefs[table.Oid].ColumnDefs)
			globalTOC.AddMasterDataEntry(table.Schema, table.Name, table.Oid, attributes)
		}
	}
}

func CopyTableOut(connection *utils.DBConn, table Relation, backupFile string) {
	usingCompression, compressionProgram := utils.GetCompressionParameters()
	copyCommand := ""
	if *singleDataFile {
		/*
		 * The segment TOC files are always written to the segment data directory for
		 * performance reasons, in case the user-specified directory is on a mounted
		 * drive.  It will be copied to a user-specified directory, if any, once all
		 * of the data is backed up.
		 */
		tocFile := globalCluster.GetSegmentTOCFilePath("<SEG_DATA_DIR>", "<SEGID>")
		helperCommand := fmt.Sprintf("$GPHOME/bin/gpbackup_helper --oid=%d --toc-file=%s --content=<SEGID>", table.Oid, tocFile)
		checkPipeExistsCommand := fmt.Sprintf("([[ -p %s ]] || (echo \"Pipe not found\">&2; exit 1))", backupFile)
		copyCommand = fmt.Sprintf("PROGRAM '%s && %s >> %s'", checkPipeExistsCommand, helperCommand, backupFile)
	} else if usingCompression {
		copyCommand = fmt.Sprintf("PROGRAM '%s > %s'", compressionProgram.CompressCommand, backupFile)
	} else {
		copyCommand = fmt.Sprintf("'%s'", backupFile)
	}
	query := fmt.Sprintf("COPY %s TO %s WITH CSV DELIMITER '%s' ON SEGMENT IGNORE EXTERNAL PARTITIONS;", table.ToString(), copyCommand, tableDelim)
	connection.MustExec(query)
}

func BackupDataForAllTables(tables []Relation, tableDefs map[uint32]TableDefinition) {
	numExtTables := 0
	numRegTables := 1
	totalExtTables := 0
	for _, table := range tables {
		if tableDefs[table.Oid].IsExternal {
			totalExtTables++
		}
	}
	totalRegTables := len(tables) - totalExtTables
	dataProgressBar := utils.NewProgressBar(totalRegTables, "Tables backed up: ", utils.PB_INFO)
	dataProgressBar.Start()
	backupFile := ""
	if *singleDataFile {
		backupFile = globalCluster.GetSegmentPipePathForCopyCommand()
	}

	for _, table := range tables {
		tableDef := tableDefs[table.Oid]
		isExternal := tableDef.IsExternal
		if !isExternal {
			if logger.GetVerbosity() > utils.LOGINFO {
				// No progress bar at this log level, so we note table count here
				logger.Verbose("Writing data for table %s to file (table %d of %d)", table.ToString(), numRegTables, totalRegTables)
			} else {
				logger.Verbose("Writing data for table %s to file", table.ToString())
			}
			if !*singleDataFile {
				backupFile = globalCluster.GetTableBackupFilePathForCopyCommand(table.Oid, false)
			}
			CopyTableOut(connection, table, backupFile)
			numRegTables++
			dataProgressBar.Increment()
		} else if *leafPartitionData || tableDef.PartitionType != "l" {
			logger.Verbose("Skipping data backup of table %s because it is an external table.", table.ToString())
			numExtTables++
		}
	}
	dataProgressBar.Finish()
	printDataBackupWarnings(numExtTables)
}

func printDataBackupWarnings(numExtTables int) {
	if numExtTables > 0 {
		s := ""
		if numExtTables > 1 {
			s = "s"
		}
		logger.Warn("Skipped data backup of %d external table%s.", numExtTables, s)
	}
	if numExtTables > 0 {
		logger.Warn("See %s for a complete list of skipped tables.", logger.GetLogFilePath())
	}
}

func CheckTablesContainData(tables []Relation, tableDefs map[uint32]TableDefinition) {
	if !backupReport.MetadataOnly {
		for _, table := range tables {
			if !tableDefs[table.Oid].IsExternal {
				return
			}
		}
		logger.Warn("No tables in backup set contain data. Performing metadata-only backup instead.")
		backupReport.MetadataOnly = true
	}
}
