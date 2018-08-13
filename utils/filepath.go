package utils

/*
 * This file contains structs and functions used in both backup and restore
 * related to interacting with files and directories, both locally and
 * remotely over SSH.
 */

import (
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gp-common-go-libs/operating"
)

type FilePathInfo struct {
	PID                    int
	SegDirMap              map[int]string
	Timestamp              string
	UserSpecifiedBackupDir string
	UserSpecifiedSegPrefix string
}

func NewFilePathInfo(c *cluster.Cluster, userSpecifiedBackupDir string, timestamp string, userSegPrefix string) FilePathInfo {
	backupFPInfo := FilePathInfo{}
	backupFPInfo.PID = os.Getpid()
	backupFPInfo.UserSpecifiedBackupDir = userSpecifiedBackupDir
	backupFPInfo.UserSpecifiedSegPrefix = userSegPrefix
	backupFPInfo.Timestamp = timestamp
	backupFPInfo.SegDirMap = make(map[int]string)
	for content, segment := range c.Segments {
		backupFPInfo.SegDirMap[content] = segment.DataDir
	}
	return backupFPInfo
}

func (backupFPInfo *FilePathInfo) IsUserSpecifiedBackupDir() bool {
	return backupFPInfo.UserSpecifiedBackupDir != ""
}

func (backupFPInfo *FilePathInfo) GetDirForContent(contentID int) string {
	if backupFPInfo.IsUserSpecifiedBackupDir() {
		segDir := fmt.Sprintf("%s%d", backupFPInfo.UserSpecifiedSegPrefix, contentID)
		return path.Join(backupFPInfo.UserSpecifiedBackupDir, segDir, "backups", backupFPInfo.Timestamp[0:8], backupFPInfo.Timestamp)
	}
	return path.Join(backupFPInfo.SegDirMap[contentID], "backups", backupFPInfo.Timestamp[0:8], backupFPInfo.Timestamp)
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

func (backupFPInfo *FilePathInfo) GetTableBackupFilePath(contentID int, tableOid uint32, singleDataFile bool) string {
	templateFilePath := backupFPInfo.GetTableBackupFilePathForCopyCommand(tableOid, singleDataFile)
	return backupFPInfo.replaceCopyFormatStringsInPath(templateFilePath, contentID)
}

func (backupFPInfo *FilePathInfo) GetTableBackupFilePathForCopyCommand(tableOid uint32, singleDataFile bool) string {
	backupFilePath := fmt.Sprintf("gpbackup_<SEGID>_%s", backupFPInfo.Timestamp)
	if !singleDataFile {
		backupFilePath += fmt.Sprintf("_%d", tableOid)
	}

	backupFilePath += GetCompressionProgram().Extension
	baseDir := "<SEG_DATA_DIR>"
	if backupFPInfo.IsUserSpecifiedBackupDir() {
		baseDir = path.Join(backupFPInfo.UserSpecifiedBackupDir, fmt.Sprintf("%s<SEGID>", backupFPInfo.UserSpecifiedSegPrefix))
	}
	return path.Join(baseDir, "backups", backupFPInfo.Timestamp[0:8], backupFPInfo.Timestamp, backupFilePath)
}

var metadataFilenameMap = map[string]string{
	"config":            "config.yaml",
	"metadata":          "metadata.sql",
	"statistics":        "statistics.sql",
	"table of contents": "toc.yaml",
	"report":            "report",
}

func (backupFPInfo *FilePathInfo) GetBackupFilePath(filetype string) string {
	return path.Join(backupFPInfo.GetDirForContent(-1), fmt.Sprintf("gpbackup_%s_%s", backupFPInfo.Timestamp, metadataFilenameMap[filetype]))
}

func (backupFPInfo *FilePathInfo) GetBackupHistoryFilePath() string {
	masterDataDirectoryPath := backupFPInfo.SegDirMap[-1]
	return path.Join(masterDataDirectoryPath, "gpbackup_history.yaml")
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

func (backupFPInfo *FilePathInfo) GetRestoreReportFilePath(restoreTimestamp string) string {
	return path.Join(backupFPInfo.GetDirForContent(-1), fmt.Sprintf("gprestore_%s_%s_report", backupFPInfo.Timestamp, restoreTimestamp))
}

func (backupFPInfo *FilePathInfo) GetConfigFilePath() string {
	return backupFPInfo.GetBackupFilePath("config")
}

func (backupFPInfo *FilePathInfo) GetSegmentTOCFilePath(contentID int) string {
	return fmt.Sprintf("%s/gpbackup_%d_%s_toc.yaml", backupFPInfo.GetDirForContent(contentID), contentID, backupFPInfo.Timestamp)
}

func (backupFPInfo *FilePathInfo) GetSegmentHelperFilePath(contentID int, suffix string) string {
	return path.Join(backupFPInfo.SegDirMap[contentID], fmt.Sprintf("gpbackup_%d_%s_%s_%d", contentID, backupFPInfo.Timestamp, suffix, backupFPInfo.PID))
}

func (backupFPInfo *FilePathInfo) GetHelperLogPath() string {
	_, homeDir, _ := GetUserAndHostInfo()
	return fmt.Sprintf("%s/gpAdminLogs/gpbackup_helper_%s.log", homeDir, backupFPInfo.Timestamp[0:8])
}

/*
 * Helper functions
 */

func GetSegPrefix(connection *dbconn.DBConn) string {
	query := ""
	if connection.Version.Before("6") {
		query = "SELECT fselocation FROM pg_filespace_entry WHERE fsedbid = 1;"
	} else {
		query = "SELECT datadir FROM gp_segment_configuration WHERE dbid = 1;"
	}
	result := ""
	err := connection.Get(&result, query)
	gplog.FatalOnError(err)
	_, segPrefix := path.Split(result)
	segPrefix = segPrefix[:len(segPrefix)-2] // Remove "-1" segment ID from string
	return segPrefix
}

func ParseSegPrefix(backupDir string, timestamp string) string {
	segPrefix := ""
	if len(backupDir) > 0 {
		backupDirForTimestamp, err := operating.System.Glob(fmt.Sprintf("%s/*-1/backups/*/%s", backupDir, timestamp))
		if err != nil || len(backupDirForTimestamp) == 0 {
			gplog.Fatal(err, "Master backup directory in %s missing or inaccessible", backupDir)
		}
		indexOfBackupsSubstr := strings.LastIndex(backupDirForTimestamp[0], "-1/backups/")
		_, segPrefix = path.Split(backupDirForTimestamp[0][:indexOfBackupsSubstr])
	}
	return segPrefix
}
