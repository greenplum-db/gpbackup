package backup

import (
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gp-common-go-libs/iohelper"
	"github.com/greenplum-db/gpbackup/backup_history"
	"github.com/greenplum-db/gpbackup/utils"
	"github.com/pkg/errors"
)

func FilterTablesForIncremental(lastBackupTOC, currentTOC *utils.TOC, tables []Table) []Table {
	var filteredTables []Table
	for _, table := range tables {
		currentAOEntry, isAOTable := currentTOC.IncrementalMetadata.AO[table.FQN()]
		if !isAOTable {
			filteredTables = append(filteredTables, table)
			continue
		}
		previousAOEntry := lastBackupTOC.IncrementalMetadata.AO[table.FQN()]

		if previousAOEntry.Modcount != currentAOEntry.Modcount || previousAOEntry.LastDDLTimestamp != currentAOEntry.LastDDLTimestamp {
			filteredTables = append(filteredTables, table)
		}
	}

	return filteredTables
}

func GetTargetBackupTimestamp() string {
	targetTimestamp := ""
	if fromTimestamp := MustGetFlagString(utils.FROM_TIMESTAMP); fromTimestamp != "" {
		ValidateFromTimestamp(fromTimestamp)
		targetTimestamp = fromTimestamp
	} else {
		targetTimestamp = GetLatestMatchingBackupTimestamp()
	}
	return targetTimestamp
}

func GetLatestMatchingBackupTimestamp() string {
	var history *backup_history.History
	var latestMatchingBackupHistoryEntry *backup_history.BackupConfig
	var err error
	if iohelper.FileExistsAndIsReadable(globalFPInfo.GetBackupHistoryFilePath()) {
		history, err = backup_history.NewHistory(globalFPInfo.GetBackupHistoryFilePath())
		gplog.FatalOnError(err)
		latestMatchingBackupHistoryEntry = GetLatestMatchingBackupConfig(history, &backupReport.BackupConfig)
	}

	if latestMatchingBackupHistoryEntry == nil {
		gplog.FatalOnError(errors.Errorf("There was no matching previous backup found with the flags provided. " +
			"Please take a full backup."))
	}

	return latestMatchingBackupHistoryEntry.Timestamp
}

func GetLatestMatchingBackupConfig(history *backup_history.History, currentBackupConfig *backup_history.BackupConfig) *backup_history.BackupConfig {
	for _, backupConfig := range history.BackupConfigs {
		if MatchesIncrementalFlags(&backupConfig, currentBackupConfig) {
			return &backupConfig
		}
	}

	return nil
}

func MatchesIncrementalFlags(backupConfig *backup_history.BackupConfig, currentBackupConfig *backup_history.BackupConfig) bool {
	return backupConfig.BackupDir == MustGetFlagString(utils.BACKUP_DIR) &&
		backupConfig.DatabaseName == currentBackupConfig.DatabaseName &&
		backupConfig.LeafPartitionData == MustGetFlagBool(utils.LEAF_PARTITION_DATA) &&
		backupConfig.Plugin == currentBackupConfig.Plugin &&
		backupConfig.SingleDataFile == MustGetFlagBool(utils.SINGLE_DATA_FILE) &&
		backupConfig.Compressed == currentBackupConfig.Compressed &&
		// Expanding of the include list happens before this now so we must compare again current backup config
		utils.NewIncludeSet(backupConfig.IncludeRelations).Equals(utils.NewIncludeSet(currentBackupConfig.IncludeRelations)) &&
		utils.NewIncludeSet(backupConfig.IncludeSchemas).Equals(utils.NewIncludeSet(MustGetFlagStringSlice(utils.INCLUDE_SCHEMA))) &&
		utils.NewIncludeSet(backupConfig.ExcludeRelations).Equals(utils.NewIncludeSet(MustGetFlagStringSlice(utils.EXCLUDE_RELATION))) &&
		utils.NewIncludeSet(backupConfig.ExcludeSchemas).Equals(utils.NewIncludeSet(MustGetFlagStringSlice(utils.EXCLUDE_SCHEMA)))
}

func PopulateRestorePlan(changedTables []Table,
	restorePlan []backup_history.RestorePlanEntry, allTables []Table) []backup_history.RestorePlanEntry {
	currBackupRestorePlanEntry := backup_history.RestorePlanEntry{
		Timestamp: globalFPInfo.Timestamp,
		TableFQNs: make([]string, 0, len(changedTables)),
	}

	for _, changedTable := range changedTables {
		changedTableFQN := changedTable.FQN()
		currBackupRestorePlanEntry.TableFQNs = append(currBackupRestorePlanEntry.TableFQNs, changedTableFQN)
	}

	changedTableFQNs := make(map[string]bool)
	for _, changedTable := range changedTables {
		changedTableFQN := changedTable.FQN()
		changedTableFQNs[changedTableFQN] = true
	}

	allTableFQNs := make(map[string]bool)
	for _, table := range allTables {
		tableFQN := table.FQN()
		allTableFQNs[tableFQN] = true
	}

	// Removing filtered table FQNs for the current backup from entries with previous timestamps
	for i, restorePlanEntry := range restorePlan {
		tableFQNs := make([]string, 0)
		for _, tableFQN := range restorePlanEntry.TableFQNs {
			if !changedTableFQNs[tableFQN] && allTableFQNs[tableFQN] {
				tableFQNs = append(tableFQNs, tableFQN)
			}
		}
		restorePlan[i].TableFQNs = tableFQNs
	}
	restorePlan = append(restorePlan, currBackupRestorePlanEntry)

	return restorePlan
}
