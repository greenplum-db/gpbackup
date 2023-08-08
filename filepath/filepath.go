package filepath

/*
 * This file contains structs and functions used in both backup and restore
 * related to interacting with files and directories, both locally and
 * remotely over SSH.
 */

import (
	"fmt"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/operating"
)

type FilePathInfo struct {
	PID                    int
	SegDirMap              map[int]string
	Timestamp              string
	UserSpecifiedBackupDir string
	UserSpecifiedSegPrefix string
	BaseDataDir            string
	UserSpecifiedReportDir string
}

func NewFilePathInfo(c *cluster.Cluster, userSpecifiedBackupDir string, timestamp string, userSegPrefix string, useMirrors ...bool) FilePathInfo {
	backupFPInfo := FilePathInfo{}
	backupFPInfo.PID = os.Getpid()
	backupFPInfo.UserSpecifiedBackupDir = userSpecifiedBackupDir
	// We only need the segment prefix to support restoring the legacy backup file format.
	// New backups should not set it, and the existence of a segment prefix can be used
	// as a proxy to determine whether a backup set is in the old or new format.
	backupFPInfo.UserSpecifiedSegPrefix = userSegPrefix
	backupFPInfo.UserSpecifiedReportDir = ""
	backupFPInfo.Timestamp = timestamp
	backupFPInfo.SegDirMap = make(map[int]string)
	backupFPInfo.BaseDataDir = "<SEG_DATA_DIR>"

	// While gpbackup doesn't care about mirrors, gpbackup_manager uses FilePathInfo and needs
	// to record mirror information for deleting backups, so we add that functionality here.
	role := "p"
	if len(useMirrors) == 1 && useMirrors[0] {
		role = "m"
	}
	for _, content := range c.ContentIDs {
		backupFPInfo.SegDirMap[content] = c.GetDirForContent(content, role)
	}

	return backupFPInfo
}

/*
 * Set user specified dir for report.
 * Currently used for restore only.
 */
func (backupFPInfo *FilePathInfo) SetReportDir(userSpecifiedReportDir string) {
	backupFPInfo.UserSpecifiedReportDir = userSpecifiedReportDir
}

/*
 * Restoring a future-dated backup is allowed (e.g. the backup was taken in a
 * different time zone that is ahead of the restore time zone), so only check
 * format, not whether the timestamp is earlier than the current time.
 */
func IsValidTimestamp(timestamp string) bool {
	timestampFormat := regexp.MustCompile(`^([0-9]{14})$`)
	return timestampFormat.MatchString(timestamp)
}

func (backupFPInfo *FilePathInfo) IsUserSpecifiedBackupDir() bool {
	return backupFPInfo.UserSpecifiedBackupDir != ""
}

// When using the current directory format, the backup directory is identical for all contents;
// we keep this function around solely for backwards compatibility.
func (backupFPInfo *FilePathInfo) GetDirForContent(contentID int) string {
	baseDir := backupFPInfo.SegDirMap[contentID]
	if backupFPInfo.IsUserSpecifiedBackupDir() {
		baseDir = backupFPInfo.UserSpecifiedBackupDir
		if backupFPInfo.UserSpecifiedSegPrefix != "" {
			segDir := fmt.Sprintf("%s%d", backupFPInfo.UserSpecifiedSegPrefix, contentID)
			baseDir = path.Join(baseDir, segDir)
		}
	}
	return path.Join(baseDir, "backups", backupFPInfo.Timestamp[0:8], backupFPInfo.Timestamp)
}

func (backupFPInfo *FilePathInfo) GetDirForReport(contentID int) string {
	if backupFPInfo.UserSpecifiedReportDir != "" {
		segDir := fmt.Sprintf("%s%d", backupFPInfo.UserSpecifiedSegPrefix, contentID)
		return path.Join(backupFPInfo.UserSpecifiedReportDir, segDir, "backups", backupFPInfo.Timestamp[0:8], backupFPInfo.Timestamp)
	}
	return backupFPInfo.GetDirForContent(contentID);
}

func (backupFPInfo *FilePathInfo) replaceCopyFormatStringsInPath(templateFilePath string, contentID int) string {
	filePath := strings.Replace(templateFilePath, "<SEG_DATA_DIR>", backupFPInfo.SegDirMap[contentID], -1)
	return strings.Replace(filePath, "<SEGID>", strconv.Itoa(contentID), -1)
}

func (backupFPInfo *FilePathInfo) GetSegmentPipeFilePath(contentID int) string {
	templateFilePath := backupFPInfo.GetSegmentPipePathForCopyCommand()
	return backupFPInfo.replaceCopyFormatStringsInPath(templateFilePath, contentID)
}

func (backupFPInfo *FilePathInfo) GetSegmentPipePathForCopyCommand() string {
	return fmt.Sprintf("<SEG_DATA_DIR>/gpbackup_<SEGID>_%s_pipe_%d", backupFPInfo.Timestamp, backupFPInfo.PID)
}

func (backupFPInfo *FilePathInfo) GetTableBackupFilePath(contentID int, tableOid uint32, extension string, singleDataFile bool) string {
	templateFilePath := backupFPInfo.GetTableBackupFilePathForCopyCommand(tableOid, extension, singleDataFile)
	return backupFPInfo.replaceCopyFormatStringsInPath(templateFilePath, contentID)
}

func (backupFPInfo *FilePathInfo) GetTableBackupFilePathForCopyCommand(tableOid uint32, extension string, singleDataFile bool) string {
	backupFilePath := fmt.Sprintf("gpbackup_<SEGID>_%s", backupFPInfo.Timestamp)
	if !singleDataFile {
		backupFilePath += fmt.Sprintf("_%d", tableOid)
	}

	backupFilePath += extension
	baseDir := backupFPInfo.BaseDataDir
	if backupFPInfo.IsUserSpecifiedBackupDir() {
		baseDir = backupFPInfo.UserSpecifiedBackupDir
		if backupFPInfo.UserSpecifiedSegPrefix != "" {
			baseDir = path.Join(baseDir, fmt.Sprintf("%s<SEGID>", backupFPInfo.UserSpecifiedSegPrefix))
		}
	}
	return path.Join(baseDir, "backups", backupFPInfo.Timestamp[0:8], backupFPInfo.Timestamp, backupFilePath)
}

var metadataFilenameMap = map[string]string{
	"config":                "config.yaml",
	"metadata":              "metadata.sql",
	"statistics":            "statistics.sql",
	"table of contents":     "toc.yaml",
	"report":                "report",
	"plugin_config":         "plugin_config.yaml",
	"error_tables_metadata": "error_tables_metadata",
	"error_tables_data":     "error_tables_data",
}

func (backupFPInfo *FilePathInfo) GetBackupFilePath(filetype string) string {
	return path.Join(backupFPInfo.GetDirForContent(-1), fmt.Sprintf("gpbackup_%s_%s", backupFPInfo.Timestamp, metadataFilenameMap[filetype]))
}

func (backupFPInfo *FilePathInfo) GetBackupHistoryFilePath() string {
	coordinatorDataDirectoryPath := backupFPInfo.SegDirMap[-1]
	return path.Join(coordinatorDataDirectoryPath, "gpbackup_history.yaml")
}

func (backupFPInfo *FilePathInfo) GetBackupHistoryDatabasePath() string {
	coordinatorDataDirectoryPath := backupFPInfo.SegDirMap[-1]
	return path.Join(coordinatorDataDirectoryPath, "gpbackup_history.db")
}

func (backupFPInfo *FilePathInfo) GetMetadataFilePath() string {
	return backupFPInfo.GetBackupFilePath("metadata")
}

func (backupFPInfo *FilePathInfo) GetStatisticsFilePath() string {
	return backupFPInfo.GetBackupFilePath("statistics")
}

func (backupFPInfo *FilePathInfo) GetTOCFilePath() string {
	return backupFPInfo.GetBackupFilePath("table of contents")
}

func (backupFPInfo *FilePathInfo) GetBackupReportFilePath() string {
	return backupFPInfo.GetBackupFilePath("report")
}

func (backupFPInfo *FilePathInfo) GetRestoreFilePath(restoreTimestamp string, filetype string) string {
	return path.Join(backupFPInfo.GetDirForReport(-1), fmt.Sprintf("gprestore_%s_%s_%s", backupFPInfo.Timestamp, restoreTimestamp, metadataFilenameMap[filetype]))
}

func (backupFPInfo *FilePathInfo) GetRestoreReportFilePath(restoreTimestamp string) string {
	return backupFPInfo.GetRestoreFilePath(restoreTimestamp, "report")
}

func (backupFPInfo *FilePathInfo) GetErrorTablesMetadataFilePath(restoreTimestamp string) string {
	return backupFPInfo.GetRestoreFilePath(restoreTimestamp, "error_tables_metadata")
}

func (backupFPInfo *FilePathInfo) GetErrorTablesDataFilePath(restoreTimestamp string) string {
	return backupFPInfo.GetRestoreFilePath(restoreTimestamp, "error_tables_data")
}

func (backupFPInfo *FilePathInfo) GetConfigFilePath() string {
	return backupFPInfo.GetBackupFilePath("config")
}

func (backupFPInfo *FilePathInfo) GetSegmentTOCFilePath(contentID int) string {
	return fmt.Sprintf("%s/gpbackup_%d_%s_toc.yaml", backupFPInfo.GetDirForContent(contentID), contentID, backupFPInfo.Timestamp)
}

func (backupFPInfo *FilePathInfo) GetPluginConfigPath() string {
	return backupFPInfo.GetBackupFilePath("plugin_config")
}

func (backupFPInfo *FilePathInfo) GetSegmentHelperFilePath(contentID int, suffix string) string {
	return path.Join(backupFPInfo.SegDirMap[contentID], fmt.Sprintf("gpbackup_%d_%s_%s_%d", contentID, backupFPInfo.Timestamp, suffix, backupFPInfo.PID))
}

func (backupFPInfo *FilePathInfo) GetHelperLogPath() string {
	currentUser, _ := operating.System.CurrentUser()
	homeDir := currentUser.HomeDir
	return fmt.Sprintf("%s/gpAdminLogs/gpbackup_helper_%s.log", homeDir, backupFPInfo.Timestamp[0:8])
}

/*
 * Helper functions
 */

func ParseSegPrefix(backupDir string) (string, error) {
	if backupDir == "" {
		return "", nil
	}

	_, err := operating.System.Stat(fmt.Sprintf("%s/backups", backupDir))
	if err == nil {
		// We're using the current directory format, there's no prefix to parse
		return "", nil
	}
	if err != nil && !os.IsNotExist(err) {
		return "", fmt.Errorf("Failure while trying to locate backup directory in %s. Error: %s", backupDir, err.Error())
	}

	// We're using the legacy directory format, try to find a prefix
	backupDirForCoordinator, err := operating.System.Glob(fmt.Sprintf("%s/*-1/backups", backupDir))
	if err != nil {
		return "", fmt.Errorf("Failure while trying to locate backup directory in %s. Error: %s", backupDir, err.Error())
	}
	if len(backupDirForCoordinator) == 0 {
		return "", fmt.Errorf("Backup directory in %s missing", backupDir)
	}
	if len(backupDirForCoordinator) != 1 {
		return "", fmt.Errorf("Multiple backup directories in %s", backupDir)
	}
	indexOfBackupsSubstr := strings.LastIndex(backupDirForCoordinator[0], "-1/backups")
	_, segPrefix := path.Split(backupDirForCoordinator[0][:indexOfBackupsSubstr])

	return segPrefix, nil
}
