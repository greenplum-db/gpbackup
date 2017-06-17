package backup

/*
 * This file contains structs and functions related to dumping data on the segments.
 */

import (
	"fmt"

	"github.com/greenplum-db/gpbackup/utils"
)

var (
	tableDelim = ","
)

func GetTableMapFilePath() string {
	return fmt.Sprintf("%s/gpbackup_%s_table_map", utils.GetDirForContent(-1), utils.DumpTimestamp)
}

func GetTableDumpFilePath(table utils.Relation) string {
	return fmt.Sprintf("%s/gpbackup_<SEGID>_%s_%d", utils.GetGenericSegDir(), utils.DumpTimestamp, table.RelationOid)
}

func WriteTableMapFile(tables []utils.Relation) {
	tableMapFile := utils.MustOpenFile(GetTableMapFilePath())
	for _, table := range tables {
		utils.MustPrintf(tableMapFile, "%s: %d\n", table.ToString(), table.RelationOid)
	}
}

func CopyTableOut(connection *utils.DBConn, table utils.Relation, dumpFile string) {
	query := fmt.Sprintf("COPY %s TO '%s' WITH CSV DELIMITER '%s' ON SEGMENT;", table.ToString(), dumpFile, tableDelim)
	_, err := connection.Exec(query)
	utils.CheckError(err)
}
