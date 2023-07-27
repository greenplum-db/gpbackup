package backup

import (
	"fmt"
	"path"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/greenplum-db/gpbackup/history"
	"github.com/greenplum-db/gpbackup/options"
	"github.com/greenplum-db/gpbackup/toc"
	"github.com/greenplum-db/gpbackup/utils"
	"github.com/pkg/errors"
)

func FilterTablesForIncremental(lastBackupTOC, currentTOC *toc.TOC, tables []Table) []Table {
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
	if fromTimestamp := MustGetFlagString(options.FROM_TIMESTAMP); fromTimestamp != "" {
		validateFromTimestamp(fromTimestamp)
		targetTimestamp = fromTimestamp
	} else {
		targetTimestamp = GetLatestMatchingBackupTimestamp()
	}
	return targetTimestamp
}

func GetLatestMatchingBackupTimestamp() string {
	latestTimestamp := ""
	var latestMatchingBackupHistoryEntry *history.BackupConfig

	historyDBPath := globalFPInfo.GetBackupHistoryDatabasePath()
	_, err := operating.System.Stat(historyDBPath)
	if err == nil {
		latestMatchingBackupHistoryEntry = GetLatestMatchingBackupConfig(historyDBPath, &backupReport.BackupConfig)
	}

	if latestMatchingBackupHistoryEntry == nil {
		gplog.FatalOnError(errors.Errorf("There was no matching previous backup found with the flags provided. " +
			"Please take a full backup."))
	} else {
		latestTimestamp = latestMatchingBackupHistoryEntry.Timestamp
	}

	return latestTimestamp
}

func GetLatestMatchingBackupConfig(historyDBPath string, currentBackupConfig *history.BackupConfig) *history.BackupConfig {
	// get list of timestamps for backups that match filterable flags, most recent first, then
	// iterate through them querying and checking one at a time. this is necessary due to the
	// impracticality of checking the include and exclude sets directly in a query

	historyDB, _ := history.InitializeHistoryDatabase(historyDBPath)

	whereClause := fmt.Sprintf(`backup_dir = '%s' AND database_name = '%s' AND leaf_partition_data = %v
		AND plugin = '%s' AND single_data_file = %v AND compressed = %v AND date_deleted = '' AND status = '%s'`,
		MustGetFlagString(options.BACKUP_DIR),
		currentBackupConfig.DatabaseName,
		MustGetFlagBool(options.LEAF_PARTITION_DATA),
		currentBackupConfig.Plugin,
		MustGetFlagBool(options.SINGLE_DATA_FILE),
		currentBackupConfig.Compressed,
        history.BackupStatusSucceed)

	getBackupTimetampsQuery := fmt.Sprintf(`
		SELECT timestamp
		FROM backups
		WHERE %s
		ORDER BY timestamp DESC`, whereClause)
	timestampRows, err := historyDB.Query(getBackupTimetampsQuery)
	if err != nil {
		gplog.Error(err.Error())
		return nil
	}
	defer timestampRows.Close()

	timestamps := make([]string, 0)
	for timestampRows.Next() {
		var timestamp string
		err = timestampRows.Scan(&timestamp)
		if err != nil {
			gplog.Error(err.Error())
			return nil
		}
		timestamps = append(timestamps, timestamp)
	}

	for _, ts := range timestamps {
		backupConfig, err := history.GetBackupConfig(ts, historyDB)
		if err != nil {
			gplog.Error(err.Error())
			return nil
		}
		if !backupConfig.Failed() && matchesIncrementalFlags(backupConfig, currentBackupConfig) {
			return backupConfig
		}
	}

	return nil
}

func matchesIncrementalFlags(backupConfig *history.BackupConfig, currentBackupConfig *history.BackupConfig) bool {
	_, pluginBinaryName := path.Split(backupConfig.Plugin)
	return backupConfig.BackupDir == MustGetFlagString(options.BACKUP_DIR) &&
		backupConfig.DatabaseName == currentBackupConfig.DatabaseName &&
		backupConfig.LeafPartitionData == MustGetFlagBool(options.LEAF_PARTITION_DATA) &&
		pluginBinaryName == currentBackupConfig.Plugin &&
		backupConfig.SingleDataFile == MustGetFlagBool(options.SINGLE_DATA_FILE) &&
		backupConfig.Compressed == currentBackupConfig.Compressed &&
		// Expanding of the include list happens before this now so we must compare again current backup config
		utils.NewIncludeSet(backupConfig.IncludeRelations).Equals(utils.NewIncludeSet(currentBackupConfig.IncludeRelations)) &&
		utils.NewIncludeSet(backupConfig.IncludeSchemas).Equals(utils.NewIncludeSet(MustGetFlagStringArray(options.INCLUDE_SCHEMA))) &&
		utils.NewIncludeSet(backupConfig.ExcludeRelations).Equals(utils.NewIncludeSet(MustGetFlagStringArray(options.EXCLUDE_RELATION))) &&
		utils.NewIncludeSet(backupConfig.ExcludeSchemas).Equals(utils.NewIncludeSet(MustGetFlagStringArray(options.EXCLUDE_SCHEMA)))
}

func PopulateRestorePlan(changedTables []Table,
	restorePlan []history.RestorePlanEntry, allTables []Table) []history.RestorePlanEntry {
	currBackupRestorePlanEntry := history.RestorePlanEntry{
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
