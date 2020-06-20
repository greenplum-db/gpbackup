package history

import (
	"io/ioutil"
	"os"
	"sort"
	"time"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/greenplum-db/gpbackup/utils"
	"github.com/nightlyone/lockfile"
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
	DatabaseName          string
	DatabaseVersion       string
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

type History struct {
	BackupConfigs []BackupConfig
}

func NewHistory(filename string) (*History, error) {
	history := &History{BackupConfigs: make([]BackupConfig, 0)}
	contents, err := operating.System.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	err = yaml.Unmarshal(contents, history)
	if err != nil {
		return nil, err
	}
	return history, nil
}

func (history *History) AddBackupConfig(backupConfig *BackupConfig) {
	history.BackupConfigs = append(history.BackupConfigs, *backupConfig)
	sort.Slice(history.BackupConfigs, func(i, j int) bool {
		return history.BackupConfigs[i].Timestamp > history.BackupConfigs[j].Timestamp
	})
}

func CurrentTimestamp() string {
	return operating.System.Now().Format("20060102150405")
}

func WriteBackupHistory(historyFilePath string, currentBackupConfig *BackupConfig) error {
	lock := lockHistoryFile()
	defer func() {
		_ = lock.Unlock()
	}()

	history := &History{BackupConfigs: make([]BackupConfig, 0)}
	_, err := os.Stat(historyFilePath)
	fileExists := err == nil
	if fileExists {
		err = os.Chmod(historyFilePath, 0644)
		gplog.FatalOnError(err)
		history, err = NewHistory(historyFilePath)
		gplog.FatalOnError(err)
	}
	if len(history.BackupConfigs) == 0 {
		gplog.Verbose("No existing backups found. Creating new backup history file.")
	}

	currentBackupConfig.EndTime = CurrentTimestamp()

	history.AddBackupConfig(currentBackupConfig)
	return history.WriteToFileAndMakeReadOnly(historyFilePath)
}

func (history *History) RewriteHistoryFile(historyFilePath string) error {
	lock := lockHistoryFile()
	defer func() {
		_ = lock.Unlock()
	}()

	err := history.WriteToFileAndMakeReadOnly(historyFilePath)
	return err
}

func lockHistoryFile() lockfile.Lockfile {
	lock, err := lockfile.New("/tmp/gpbackup_history.yaml.lck")
	gplog.FatalOnError(err)
	err = lock.TryLock()
	for err != nil {
		time.Sleep(50 * time.Millisecond)
		err = lock.TryLock()
	}
	return lock
}

func (history *History) WriteToFileAndMakeReadOnly(filename string) error {
	_, err := operating.System.Stat(filename)
	fileExists := err == nil
	if fileExists {
		err = operating.System.Chmod(filename, 0644)
		if err != nil {
			return err
		}
	}
	historyFileContents, err := yaml.Marshal(history)

	if err != nil {
		return err
	}
	return utils.WriteToFileAndMakeReadOnly(filename, historyFileContents)
}

func (history *History) FindBackupConfig(timestamp string) *BackupConfig {
	for _, backupConfig := range history.BackupConfigs {
		if backupConfig.Timestamp == timestamp && !backupConfig.Failed() {
			return &backupConfig
		}
	}
	return nil
}
