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
	/*
	 * The segment TOC files are always written to the segment data directory for
	 * performance reasons, in case the user-specified directory is on a mounted
	 * drive.  It will be copied to a user-specified directory, if any, once all
	 * of the data is backed up.
	 */
	tocFile := globalCluster.GetSegmentTOCFilePath("<SEG_DATA_DIR>", "<SEGID>")
	helperCommand := fmt.Sprintf("$GPHOME/bin/gpbackup_helper --oid=%d --toc-file=%s", table.Oid, tocFile)
	if *singleDataFile && usingCompression {
		copyCommand = fmt.Sprintf("PROGRAM 'set -o pipefail; %s | %s >> %s'", helperCommand, compressionProgram.CompressCommand, backupFile)
	} else if *singleDataFile {
		copyCommand = fmt.Sprintf("PROGRAM '%s >> %s'", helperCommand, backupFile)
	} else if usingCompression {
		copyCommand = fmt.Sprintf("PROGRAM '%s > %s'", compressionProgram.CompressCommand, backupFile)
	} else {
		copyCommand = fmt.Sprintf("'%s'", backupFile)
	}
	query := fmt.Sprintf("COPY %s TO %s WITH CSV DELIMITER '%s' ON SEGMENT;", table.ToString(), copyCommand, tableDelim)
	_, err := connection.Exec(query)
	utils.CheckError(err)
}
