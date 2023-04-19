package history

import (
	"database/sql"
	"errors"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/greenplum-db/gpbackup/utils"
	_ "github.com/mattn/go-sqlite3"
	"gopkg.in/yaml.v2"
)

type RestorePlanEntry struct {
	Timestamp string
	TableFQNs []string
}

const (
	BackupStatusSucceed = "Success"
	BackupStatusFailed  = "Failure"
)

type BackupConfig struct {
	BackupDir             string
	BackupVersion         string
	Compressed            bool
	CompressionType       string
	DatabaseName          string
	DatabaseVersion       string
	SegmentCount          int
	DataOnly              bool
	DateDeleted           string
	ExcludeRelations      []string
	ExcludeSchemaFiltered bool
	ExcludeSchemas        []string
	ExcludeTableFiltered  bool
	IncludeRelations      []string
	IncludeSchemaFiltered bool
	IncludeSchemas        []string
	IncludeTableFiltered  bool
	Incremental           bool
	LeafPartitionData     bool
	MetadataOnly          bool
	Plugin                string
	PluginVersion         string
	RestorePlan           []RestorePlanEntry
	SingleDataFile        bool
	Timestamp             string
	EndTime               string
	WithoutGlobals        bool
	WithStatistics        bool
	Status                string
}

func (backup *BackupConfig) Failed() bool {
	return backup.Status == BackupStatusFailed
}

func ReadConfigFile(filename string) *BackupConfig {
	config := &BackupConfig{}
	contents, err := ioutil.ReadFile(filename)
	gplog.FatalOnError(err)
	err = yaml.Unmarshal(contents, config)
	gplog.FatalOnError(err)
	return config
}

func WriteConfigFile(config *BackupConfig, configFilename string) {
	configContents, err := yaml.Marshal(config)
	gplog.FatalOnError(err)
	_ = utils.WriteToFileAndMakeReadOnly(configFilename, configContents)
}

func InitializeHistoryDatabase(historyDBPath string) (*sql.DB, error) {
	// Create and set up backup history database if it does not exist, and return a connection to it
	// It is the caller's responsibility to close the returned connection when done with it.

	// These setup statements are idempotent. Attempting to run them if the db already exists is a
	// no-op. This approach allows us to avoid risky race conditions around creating the database
	// only if not present, at the overhead cost of a few no-op DDL queries when we connect.

	// Create db file if one does not exist
	fd, err := os.OpenFile(historyDBPath, os.O_CREATE|os.O_EXCL, 0666) // TODO -- change to 0644?
	if err != nil && !errors.Is(err, os.ErrExist) {
		return nil, err
	} else if err == nil {
		// We don't want an fd handle to it, so close it
		fd.Close()
	}

	db, err := sql.Open("sqlite3", historyDBPath)
	if err != nil {
		return nil, err
	}

	// PRAGMAs must be set before the transaction begins to be enforced throughout
	_, err = db.Exec("PRAGMA foreign_keys = ON;")
	if err != nil {
		return nil, err
	}

	tx, _ := db.Begin()

	createBackupsTable := `
		CREATE TABLE IF NOT EXISTS backups (
            timestamp TEXT NOT NULL PRIMARY KEY,
			backup_dir TEXT,
            backup_version TEXT,
            compressed INT CHECK (compressed in (0,1)),
            compression_type TEXT,
            database_name TEXT,
            database_version TEXT,
            segment_count INT,
            data_only INT CHECK (data_only in (0,1)),
            date_deleted TEXT,
            exclude_schema_filtered INT CHECK (exclude_schema_filtered in (0,1)),
            exclude_table_filtered INT CHECK (exclude_table_filtered in (0,1)),
            include_schema_filtered INT CHECK (include_schema_filtered in (0,1)),
            include_table_filtered INT CHECK (include_table_filtered in (0,1)),
            incremental INT CHECK (incremental in (0,1)),
            leaf_partition_data INT CHECK (leaf_partition_data in (0,1)),
            metadata_only INT CHECK (metadata_only in (0,1)),
            plugin TEXT,
            plugin_version TEXT,
            single_data_file INT CHECK (single_data_file in (0,1)),
            end_time TEXT,
            without_globals INT CHECK (without_globals in (0,1)),
            with_statistics INT CHECK (with_statistics in (0,1)),
            status TEXT
		);`
	_, err = tx.Exec(createBackupsTable)
	if err != nil {
		tx.Rollback()
		db.Close()
		return nil, err
	}

	createAuxTableQuery := `
		CREATE TABLE IF NOT EXISTS %s (
			timestamp TEXT NOT NULL,
			name TEXT NOT NULL,
			FOREIGN KEY(timestamp) REFERENCES backups(timestamp)
		);`

	auxTables := []string{"exclude_relations", "exclude_schemas", "include_relations", "include_schemas"}
	for _, auxTable := range auxTables {
		_, err = tx.Exec(fmt.Sprintf(createAuxTableQuery, auxTable))
		if err != nil {
			tx.Rollback()
			db.Close()
			return nil, err
		}
	}

	// TODO -- consider warning if restore_plan_timestamp references a backup timestamp not present
	// in historyDB? This scenario may be caused by lack of migration of legacy files, and may cause
	// future backup-manager functionality such as CASCADE or cleanup to perform in unexpected ways.
	createRestorePlansTable := `
		CREATE TABLE IF NOT EXISTS restore_plans (
			timestamp TEXT NOT NULL,
			restore_plan_timestamp TEXT NOT NULL,
			FOREIGN KEY(timestamp) REFERENCES backups(timestamp)
		);`
	_, err = tx.Exec(createRestorePlansTable)
	if err != nil {
		tx.Rollback()
		db.Close()
		return nil, err
	}

	createRestorePlanTablesTable := `
		CREATE TABLE IF NOT EXISTS restore_plan_tables (
			timestamp TEXT NOT NULL,
			restore_plan_timestamp TEXT NOT NULL,
			table_fqn TEXT NOT NULL,
			FOREIGN KEY(timestamp) REFERENCES backups(timestamp)
		);`
	_, err = tx.Exec(createRestorePlanTablesTable)
	if err != nil {
		tx.Rollback()
		db.Close()
		return nil, err
	}

	err = tx.Commit()
	if err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func CurrentTimestamp() string {
	return operating.System.Now().Format("20060102150405")
}

func storeAuxTable(tx *sql.Tx, arrayValues []string, tablename string, timestamp string) error {
	for _, arrayVal := range arrayValues {
		auxTableInsert := fmt.Sprintf("INSERT INTO %s VALUES (?, ?);", tablename)
		_, err := tx.Exec(auxTableInsert, timestamp, arrayVal)
		if err != nil {
			return err
		}
	}
	return nil
}

func StoreBackupHistory(db *sql.DB, currentBackupConfig *BackupConfig) error {
	if currentBackupConfig.EndTime == "" {
		// If we're migrating in prior backup records, we don't want to overwrite pre-existing EndTime values
		currentBackupConfig.EndTime = CurrentTimestamp()
	}
	tx, _ := db.Begin()

	_, err := tx.Exec(`INSERT INTO backups (
			timestamp, backup_dir, backup_version, compressed, compression_type, database_name,
			database_version, segment_count, data_only, date_deleted, exclude_schema_filtered,
			exclude_table_filtered, include_schema_filtered, include_table_filtered, incremental,
			leaf_partition_data, metadata_only, plugin, plugin_version, single_data_file, end_time,
			without_globals, with_statistics, status
			)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`,
		currentBackupConfig.Timestamp, currentBackupConfig.BackupDir,
		currentBackupConfig.BackupVersion, currentBackupConfig.Compressed,
		currentBackupConfig.CompressionType, currentBackupConfig.DatabaseName,
		currentBackupConfig.DatabaseVersion, currentBackupConfig.SegmentCount,
		currentBackupConfig.DataOnly, currentBackupConfig.DateDeleted,
		currentBackupConfig.ExcludeSchemaFiltered, currentBackupConfig.ExcludeTableFiltered,
		currentBackupConfig.IncludeSchemaFiltered, currentBackupConfig.IncludeTableFiltered,
		currentBackupConfig.Incremental, currentBackupConfig.LeafPartitionData,
		currentBackupConfig.MetadataOnly, currentBackupConfig.Plugin,
		currentBackupConfig.PluginVersion, currentBackupConfig.SingleDataFile,
		currentBackupConfig.EndTime, currentBackupConfig.WithoutGlobals,
		currentBackupConfig.WithStatistics, currentBackupConfig.Status)
	if err != nil {
		goto CleanupError
	}

	err = storeAuxTable(tx, currentBackupConfig.ExcludeRelations, "exclude_relations", currentBackupConfig.Timestamp)
	if err != nil {
		goto CleanupError
	}

	err = storeAuxTable(tx, currentBackupConfig.ExcludeSchemas, "exclude_schemas", currentBackupConfig.Timestamp)
	if err != nil {
		goto CleanupError
	}

	err = storeAuxTable(tx, currentBackupConfig.IncludeRelations, "include_relations", currentBackupConfig.Timestamp)
	if err != nil {
		goto CleanupError
	}

	err = storeAuxTable(tx, currentBackupConfig.IncludeSchemas, "include_schemas", currentBackupConfig.Timestamp)
	if err != nil {
		goto CleanupError
	}

	// unpack and store restore plan entries
	for _, restorePlan := range currentBackupConfig.RestorePlan {
		_, err = tx.Exec("INSERT INTO restore_plans VALUES (?, ?);",
			currentBackupConfig.Timestamp, restorePlan.Timestamp)
		if err != nil {
			goto CleanupError
		}

		for _, tableFQN := range restorePlan.TableFQNs {
			_, err = tx.Exec("INSERT INTO restore_plan_tables VALUES (?, ?, ?);",
				currentBackupConfig.Timestamp, restorePlan.Timestamp, tableFQN)
			if err != nil {
				goto CleanupError
			}
		}
	}

	err = tx.Commit()
	return err

CleanupError:
	tx.Rollback()
	return err
}

func GetMainBackupInfo(timestamp string, historyDB *sql.DB) (BackupConfig, error) {
	// Retreive main backups information. SQLite doesn't have booleans so convert from ints
	// TODO -- consider passing in a tx instead so that aux tables are coherent with main backups
	// table. Need to confirm this is possible with sqlite. Unclear if we ever pull in and use aux
	// table info, so it may not be needed.
	backupQuery := fmt.Sprintf(`
		SELECT timestamp, backup_dir, backup_version, compressed, compression_type, database_name,
			database_version, segment_count, data_only, date_deleted, exclude_schema_filtered,
			exclude_table_filtered, include_schema_filtered, include_table_filtered, incremental,
			leaf_partition_data, metadata_only, plugin, plugin_version, single_data_file, end_time,
			without_globals, with_statistics, status
		FROM backups WHERE timestamp = '%s'`,
		timestamp)
	backupRow := historyDB.QueryRow(backupQuery)

	var backupConfig BackupConfig
	var isCompressed int
	var isDataOnly int
	var isExclSchemaFiltered int
	var isExclTableFiltered int
	var isInclSchemaFiltered int
	var isInclTableFiltered int
	var isIncremental int
	var isLeafPartition int
	var isMetadataOnly int
	var isSingleDataFile int
	var isWithoutGlobals int
	var isWithStatistics int
	err := backupRow.Scan(
		&backupConfig.Timestamp, &backupConfig.BackupDir, &backupConfig.BackupVersion,
		&isCompressed, &backupConfig.CompressionType, &backupConfig.DatabaseName,
		&backupConfig.DatabaseVersion, &backupConfig.SegmentCount, &isDataOnly,
		&backupConfig.DateDeleted, &isExclSchemaFiltered, &isExclTableFiltered,
		&isInclSchemaFiltered, &isInclTableFiltered, &isIncremental, &isLeafPartition,
		&isMetadataOnly, &backupConfig.Plugin, &backupConfig.PluginVersion, &isSingleDataFile,
		&backupConfig.EndTime, &isWithoutGlobals, &isWithStatistics, &backupConfig.Status)
	if err == sql.ErrNoRows {
		return backupConfig, errors.New("timestamp doesn't match any existing backups")
	} else if err != nil {
		return backupConfig, err
	}

	backupConfig.Compressed = isCompressed == 1
	backupConfig.DataOnly = isDataOnly == 1
	backupConfig.ExcludeSchemaFiltered = isExclSchemaFiltered == 1
	backupConfig.ExcludeTableFiltered = isExclTableFiltered == 1
	backupConfig.IncludeSchemaFiltered = isInclSchemaFiltered == 1
	backupConfig.IncludeTableFiltered = isInclTableFiltered == 1
	backupConfig.Incremental = isIncremental == 1
	backupConfig.LeafPartitionData = isLeafPartition == 1
	backupConfig.MetadataOnly = isMetadataOnly == 1
	backupConfig.SingleDataFile = isSingleDataFile == 1
	backupConfig.WithoutGlobals = isWithoutGlobals == 1
	backupConfig.WithStatistics = isWithStatistics == 1

	return backupConfig, err
}

func getAuxTable(db *sql.DB, timestamp, tableName string) ([]string, error) {
	getAuxTableQuery := fmt.Sprintf("SELECT name FROM %s WHERE timestamp = '%s'", tableName, timestamp)
	auxTableRows, err := db.Query(getAuxTableQuery)
	if err != nil {
		return nil, err
	}
	defer auxTableRows.Close()

	auxTableSlice := make([]string, 0)
	for auxTableRows.Next() {
		var auxTableValue string
		err = auxTableRows.Scan(&auxTableValue)
		if err != nil {
			return nil, err
		}
		auxTableSlice = append(auxTableSlice, auxTableValue)
	}

	return auxTableSlice, nil
}

func GetBackupConfig(timestamp string, historyDB *sql.DB) (*BackupConfig, error) {

	backupConfig, err := GetMainBackupInfo(timestamp, historyDB)
	if err != nil {
		return nil, err
	}

	backupConfig.ExcludeSchemas, err = getAuxTable(historyDB, timestamp, "exclude_schemas")
	if err != nil {
		return nil, err
	}

	backupConfig.ExcludeRelations, err = getAuxTable(historyDB, timestamp, "exclude_relations")
	if err != nil {
		return nil, err
	}

	backupConfig.IncludeSchemas, err = getAuxTable(historyDB, timestamp, "include_schemas")
	if err != nil {
		return nil, err
	}

	backupConfig.IncludeRelations, err = getAuxTable(historyDB, timestamp, "include_relations")
	if err != nil {
		return nil, err
	}

	// Retrieve restore plan information
	restorePlanQuery := fmt.Sprintf("SELECT DISTINCT restore_plan_timestamp FROM restore_plans WHERE timestamp = '%s' ORDER BY restore_plan_timestamp", timestamp)
	restorePlanRows, err := historyDB.Query(restorePlanQuery)
	if err != nil {
		return nil, err
	}
	defer restorePlanRows.Close()

	backupConfig.RestorePlan = make([]RestorePlanEntry, 0)
	for restorePlanRows.Next() {

		var restorePlanTimestamp string
		restorePlan := RestorePlanEntry{}
		restorePlan.TableFQNs = make([]string, 0)

		err = restorePlanRows.Scan(&restorePlanTimestamp)
		if err != nil {
			return nil, err
		}
		restorePlan.Timestamp = restorePlanTimestamp

		restorePlanTablesQuery := fmt.Sprintf("SELECT table_fqn FROM restore_plan_tables WHERE timestamp = '%s' and restore_plan_timestamp = '%s'", timestamp, restorePlanTimestamp)
		restorePlanTableRows, err := historyDB.Query(restorePlanTablesQuery)
		if err != nil {
			return nil, err
		}
		defer restorePlanTableRows.Close()

		for restorePlanTableRows.Next() {
			var tableFQN string
			err = restorePlanTableRows.Scan(&tableFQN)
			if err != nil {
				return nil, err
			}
			restorePlan.TableFQNs = append(restorePlan.TableFQNs, tableFQN)
		}

		backupConfig.RestorePlan = append(backupConfig.RestorePlan, restorePlan)
	}

	return &backupConfig, err
}
