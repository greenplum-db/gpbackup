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

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gp-common-go-libs/operating"
)

type FilePathInfo struct {
	PID                    int
	BackupDirMap           map[int]string
	Timestamp              string
	UserSpecifiedBackupDir string
	UserSpecifiedSegPrefix string
}

func NewFilePathInfo(segDirMap map[int]string, userSpecifiedBackupDir string, timestamp string, userSegPrefix string) FilePathInfo {
	backupFPInfo := FilePathInfo{}
	backupFPInfo.PID = os.Getpid()
	backupFPInfo.UserSpecifiedBackupDir = userSpecifiedBackupDir
	backupFPInfo.UserSpecifiedSegPrefix = userSegPrefix
	backupFPInfo.Timestamp = timestamp
	backupFPInfo.BackupDirMap = make(map[int]string)
	for k, v := range segDirMap {
		backupFPInfo.BackupDirMap[k] = v
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
	return path.Join(backupFPInfo.BackupDirMap[contentID], "backups", backupFPInfo.Timestamp[0:8], backupFPInfo.Timestamp)
}

func (backupFPInfo *FilePathInfo) replaceCopyFormatStringsInPath(templateFilePath string, contentID int) string {
	filePath := strings.Replace(templateFilePath, "<SEG_DATA_DIR>", backupFPInfo.BackupDirMap[contentID], -1)
	return strings.Replace(filePath, "<SEGID>", strconv.Itoa(contentID), -1)
}

func (backupFPInfo *FilePathInfo) GetSegmentPipeFilePath(contentID int) string {
	templateFilePath := backupFPInfo.GetSegmentPipePathForCopyCommand()
	return backupFPInfo.replaceCopyFormatStringsInPath(templateFilePath, contentID)
}

func (backupFPInfo *FilePathInfo) GetSegmentPipeFilePathWithPID(contentID int) string {
	return fmt.Sprintf("%s_%d", backupFPInfo.GetSegmentPipeFilePath(contentID), backupFPInfo.PID)
}

func (backupFPInfo *FilePathInfo) GetSegmentPipePathForCopyCommand() string {
	return fmt.Sprintf("<SEG_DATA_DIR>/gpbackup_<SEGID>_%s_pipe", backupFPInfo.Timestamp)
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
	if usingCompression {
		backupFilePath += compressionProgram.Extension
	}
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

func (backupFPInfo *FilePathInfo) GetMetadataFilePath() string {
	return backupFPInfo.GetBackupFilePath("metadata")
}

func (backupFPInfo *FilePathInfo) GetStatisticsFilePath() string {
	return backupFPInfo.GetBackupFilePath("statistics")
}

func (backupFPInfo *FilePathInfo) GetTOCFilePath() string {
	return backupFPInfo.GetBackupFilePath("table of contents")
}

func (backupFPInfo *FilePathInfo) GetReportFilePath() string {
	return backupFPInfo.GetBackupFilePath("report")
}

func (backupFPInfo *FilePathInfo) GetConfigFilePath() string {
	return backupFPInfo.GetBackupFilePath("config")
}

/*
 * This is the temporary location of the segment TOC file before it is moved
 * to its final location in the actual backup directory
 */
func (backupFPInfo *FilePathInfo) GetSegmentTOCFilePath(topDir string, contentStr string) string {
	return path.Join(topDir, fmt.Sprintf("gpbackup_%s_%s_toc.yaml", contentStr, backupFPInfo.Timestamp))
}

func (backupFPInfo *FilePathInfo) GetSegmentTOCFilePathWithPID(topDir string, contentStr string) string {
	return path.Join(topDir, fmt.Sprintf("gpbackup_%s_%s_toc_%d.yaml", contentStr, backupFPInfo.Timestamp, backupFPInfo.PID))
}

func (backupFPInfo *FilePathInfo) GetSegmentHelperFilePath(contentID int, suffix string) string {
	return path.Join(backupFPInfo.BackupDirMap[contentID], fmt.Sprintf("gpbackup_%d_%s_%s_%d", contentID, backupFPInfo.Timestamp, suffix, backupFPInfo.PID))
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

func ParseSegPrefix(backupDir string) string {
	segPrefix := ""
	if len(backupDir) > 0 {
		masterDir, err := operating.System.Glob(fmt.Sprintf("%s/*-1", backupDir))
		if err != nil || len(masterDir) == 0 {
			gplog.Fatal(err, "Master backup directory in %s missing or inaccessible", backupDir)
		}
		_, segPrefix = path.Split(masterDir[0])
		segPrefix = segPrefix[:len(segPrefix)-2] // Remove "-1" segment ID from string
	}
	return segPrefix
}
