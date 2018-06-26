package backup

import (
	"fmt"
	"github.com/greenplum-db/gpbackup/utils"
)

func FilterTablesForIncremental(lastBackupTOC, currentTOC *utils.TOC, tables []Relation) []Relation {
	var filteredTables []Relation
	for _, table := range tables {
		currentAOEntry, isAOTable := currentTOC.IncrementalMetadata.AO[table.ToString()]
		if !isAOTable {
			filteredTables = append(filteredTables, table)
			fmt.Println(table.ToString(), "is a heap table")
			continue
		}
		previousAOEntry := lastBackupTOC.IncrementalMetadata.AO[table.ToString()]

		if previousAOEntry.Modcount != currentAOEntry.Modcount || previousAOEntry.LastDDLTimestamp != currentAOEntry.LastDDLTimestamp {
			filteredTables = append(filteredTables, table)
			fmt.Println(table.ToString(), "has changed")
		} else {
			fmt.Println(table.ToString(), "has not changed")
		}
	}

	return filteredTables
}

func GetLastBackupTOC() *utils.TOC {
	// TODO: get last backup timestamp and pass to NewFilePathInfo
	lastBackupFPInfo := utils.NewFilePathInfo(globalCluster, globalFPInfo.UserSpecifiedBackupDir,
		*incremental, globalFPInfo.UserSpecifiedSegPrefix)
	lastBackupTOCFilePath := lastBackupFPInfo.GetTOCFilePath()
	lastBackupTOC := utils.NewTOC(lastBackupTOCFilePath)

	return lastBackupTOC
}
