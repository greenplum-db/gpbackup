package backup_history

//TODO: change package name to conform to Go standards

import (
	"sort"
	"time"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gp-common-go-libs/iohelper"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/nightlyone/lockfile"
	"gopkg.in/yaml.v2"
)

type RestorePlanEntry struct {
	Timestamp string
	TableFQNs []string
}

type BackupConfig struct {
	BackupDir             string
	BackupVersion         string
	Compressed            bool
	DatabaseName          string
	DatabaseVersion       string
	DataOnly              bool
	Deleted               bool
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
	RestorePlan           []RestorePlanEntry
	SingleDataFile        bool
	Timestamp             string
	WithStatistics        bool
}

func ReadConfigFile(filename string) *BackupConfig {
	config := &BackupConfig{}
	contents, err := operating.System.ReadFile(filename)
	gplog.FatalOnError(err)
	err = yaml.Unmarshal(contents, config)
	gplog.FatalOnError(err)
	return config
}

func WriteConfigFile(config *BackupConfig, configFilename string) {
	configFile := iohelper.MustOpenFileForWriting(configFilename)
	configContents, _ := yaml.Marshal(config)
	_, err := configFile.Write(configContents)
	gplog.FatalOnError(err)
	err = operating.System.Chmod(configFilename, 0444)
	gplog.FatalOnError(err)
}

type History struct {
	BackupConfigs []BackupConfig
}

func NewHistory(filename string) *History {
	history := &History{BackupConfigs: make([]BackupConfig, 0)}
	if historyFileExists := iohelper.FileExistsAndIsReadable(filename); historyFileExists {
		contents, err := operating.System.ReadFile(filename)

		gplog.FatalOnError(err)
		err = yaml.Unmarshal(contents, history)
		gplog.FatalOnError(err)
	}
	return history
}

func (history *History) AddBackupConfig(backupConfig *BackupConfig) {
	history.BackupConfigs = append(history.BackupConfigs, *backupConfig)
	sort.Slice(history.BackupConfigs, func(i, j int) bool {
		return history.BackupConfigs[i].Timestamp > history.BackupConfigs[j].Timestamp
	})
}

func WriteBackupHistory(historyFilePath string, currentBackupConfig *BackupConfig) {
	lock := lockHistoryFile()
	defer func() {
		_ = lock.Unlock()
	}()

	history := NewHistory(historyFilePath)
	if len(history.BackupConfigs) == 0 {
		gplog.Verbose("No existing backup history file could be found. Creating new backup history file.")
	}
	history.AddBackupConfig(currentBackupConfig)
	history.writeToFileAndMakeReadOnly(historyFilePath)
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

func (history *History) writeToFileAndMakeReadOnly(filename string) {
	_, err := operating.System.Stat(filename)
	fileExists := err == nil
	if fileExists {
		err = operating.System.Chmod(filename, 0644)
		gplog.FatalOnError(err)
	}
	historyFile := iohelper.MustOpenFileForWriting(filename)
	historyFileContents, err := yaml.Marshal(history)
	gplog.FatalOnError(err)
	_, err = historyFile.Write(historyFileContents)
	gplog.FatalOnError(err)
	err = operating.System.Chmod(filename, 0444)
	gplog.FatalOnError(err)
}
