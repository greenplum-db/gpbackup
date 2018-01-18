package utils

/*
 * This file contains structs and functions used in both backup and restore
 * related to interacting with files and directories, both locally and
 * remotely over SSH.
 */

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

type Executor interface {
	ExecuteLocalCommand(commandStr string) error
	ExecuteClusterCommand(commandMap map[int][]string) *RemoteOutput
}

// This type only exists to allow us to mock Execute[...]Command functions for testing
type GPDBExecutor struct{}

type Cluster struct {
	ContentIDs             []int
	PID                    int
	SegDirMap              map[int]string
	SegHostMap             map[int]string
	UserSpecifiedBackupDir string
	UserSpecifiedSegPrefix string
	Timestamp              string
	Executor
}

type SegConfig struct {
	ContentID int
	Hostname  string
	DataDir   string
}

type RemoteOutput struct {
	NumErrors int
	Stdouts   map[int]string
	Stderrs   map[int]string
	Errors    map[int]error
}

/*
 * Base cluster functions
 */

func NewCluster(segConfigs []SegConfig, userSpecifiedBackupDir string, timestamp string, userSegPrefix string) Cluster {
	cluster := Cluster{}
	cluster.SegHostMap = make(map[int]string, 0)
	cluster.SegDirMap = make(map[int]string, 0)
	cluster.UserSpecifiedBackupDir = userSpecifiedBackupDir
	cluster.UserSpecifiedSegPrefix = userSegPrefix
	cluster.Timestamp = timestamp
	cluster.PID = os.Getpid()
	for _, seg := range segConfigs {
		cluster.ContentIDs = append(cluster.ContentIDs, seg.ContentID)
		cluster.SegDirMap[seg.ContentID] = seg.DataDir
		cluster.SegHostMap[seg.ContentID] = seg.Hostname
	}
	cluster.Executor = &GPDBExecutor{}
	return cluster
}

func (cluster *Cluster) GenerateSSHCommandMap(includeMaster bool, generateCommand func(int) string) map[int][]string {
	commandMap := make(map[int][]string, len(cluster.ContentIDs))
	for _, contentID := range cluster.ContentIDs {
		if contentID == -1 && !includeMaster {
			continue
		}
		host := cluster.GetHostForContent(contentID)
		cmdStr := generateCommand(contentID)
		if contentID == -1 {
			commandMap[contentID] = []string{"bash", "-c", cmdStr}
		} else {
			commandMap[contentID] = ConstructSSHCommand(host, cmdStr)
		}
	}
	return commandMap
}

func (cluster *Cluster) GenerateSSHCommandMapForCluster(generateCommand func(int) string) map[int][]string {
	return cluster.GenerateSSHCommandMap(true, generateCommand)
}

func (cluster *Cluster) GenerateSSHCommandMapForSegments(generateCommand func(int) string) map[int][]string {
	return cluster.GenerateSSHCommandMap(false, generateCommand)
}

func (executor *GPDBExecutor) ExecuteLocalCommand(commandStr string) error {
	_, err := exec.Command("bash", "-c", commandStr).CombinedOutput()
	return err
}

func newRemoteOutput(numIDs int) *RemoteOutput {
	stdout := make(map[int]string, numIDs)
	stderr := make(map[int]string, numIDs)
	err := make(map[int]error, numIDs)
	return &RemoteOutput{NumErrors: 0, Stdouts: stdout, Stderrs: stderr, Errors: err}
}

func (executor *GPDBExecutor) ExecuteClusterCommand(commandMap map[int][]string) *RemoteOutput {
	length := len(commandMap)
	finished := make(chan int)
	contentIDs := make([]int, length)
	i := 0
	for key := range commandMap {
		contentIDs[i] = key
		i++
	}
	output := newRemoteOutput(length)
	stdouts := make([]string, length)
	stderrs := make([]string, length)
	errors := make([]error, length)
	for i, contentID := range contentIDs {
		go func(index int, segCommand []string) {
			var stderr bytes.Buffer
			cmd := exec.Command(segCommand[0], segCommand[1:]...)
			cmd.Stderr = &stderr
			out, err := cmd.Output()
			stdouts[index] = string(out)
			stderrs[index] = stderr.String()
			errors[index] = err
			finished <- index
		}(i, commandMap[contentID])
	}
	for i := 0; i < length; i++ {
		index := <-finished
		id := contentIDs[index]
		output.Stdouts[id] = stdouts[index]
		output.Stderrs[id] = stderrs[index]
		output.Errors[id] = errors[index]
		if output.Errors[id] != nil {
			output.NumErrors++
		}
	}
	return output
}

/*
 * GenerateAndExecuteCommand and CheckClusterError are generic wrapper functions
 * to simplify execution of shell commands on remote hosts.
 */
func (cluster *Cluster) GenerateAndExecuteCommand(verboseMsg string, execFunc func(contentID int) string, entireCluster ...bool) *RemoteOutput {
	logger.Verbose(verboseMsg)
	var commandMap map[int][]string
	if len(entireCluster) == 1 && entireCluster[0] == true {
		commandMap = cluster.GenerateSSHCommandMapForCluster(execFunc)
	} else {
		commandMap = cluster.GenerateSSHCommandMapForSegments(execFunc)
	}
	return cluster.ExecuteClusterCommand(commandMap)
}

func (cluster *Cluster) CheckClusterError(remoteOutput *RemoteOutput, finalErrMsg string, messageFunc func(contentID int) string, noFatal ...bool) {
	if remoteOutput.NumErrors == 0 {
		return
	}
	for contentID, err := range remoteOutput.Errors {
		if err != nil {
			logger.Verbose("%s on segment %d on host %s with error %s", messageFunc(contentID), contentID, cluster.GetHostForContent(contentID), err)
		}
	}
	if len(noFatal) == 1 && noFatal[0] == true {
		logger.Error(finalErrMsg)
	} else {
		LogFatalClusterError(finalErrMsg, remoteOutput.NumErrors)
	}
}

func LogFatalClusterError(errMessage string, numErrors int) {
	s := ""
	if numErrors != 1 {
		s = "s"
	}
	logger.Fatal(errors.Errorf("%s on %d segment%s. See %s for a complete list of segments with errors.", errMessage, numErrors, s, logger.GetLogFilePath()), "")
}

/*
 * Shared cluster functions that are used in both backup and restore
 */

func (cluster *Cluster) GetContentList() []int {
	return cluster.ContentIDs
}

func (cluster *Cluster) GetHostForContent(contentID int) string {
	return cluster.SegHostMap[contentID]
}

func (cluster *Cluster) IsUserSpecifiedBackupDir() bool {
	return cluster.UserSpecifiedBackupDir != ""
}

func (cluster *Cluster) GetDirForContent(contentID int) string {
	if cluster.IsUserSpecifiedBackupDir() {
		segDir := fmt.Sprintf("%s%d", cluster.UserSpecifiedSegPrefix, contentID)
		return path.Join(cluster.UserSpecifiedBackupDir, segDir, "backups", cluster.Timestamp[0:8], cluster.Timestamp)
	}
	return path.Join(cluster.SegDirMap[contentID], "backups", cluster.Timestamp[0:8], cluster.Timestamp)
}

func (cluster *Cluster) replaceCopyFormatStringsInPath(templateFilePath string, contentID int) string {
	filePath := strings.Replace(templateFilePath, "<SEG_DATA_DIR>", cluster.SegDirMap[contentID], -1)
	return strings.Replace(filePath, "<SEGID>", strconv.Itoa(contentID), -1)
}

func (cluster *Cluster) GetSegmentPipeFilePath(contentID int) string {
	templateFilePath := cluster.GetSegmentPipePathForCopyCommand()
	return cluster.replaceCopyFormatStringsInPath(templateFilePath, contentID)
}

func (cluster *Cluster) GetSegmentPipeFilePathWithPID(contentID int) string {
	return fmt.Sprintf("%s_%d", cluster.GetSegmentPipeFilePath(contentID), cluster.PID)
}

func (cluster *Cluster) GetSegmentPipePathForCopyCommand() string {
	return fmt.Sprintf("<SEG_DATA_DIR>/gpbackup_<SEGID>_%s_pipe", cluster.Timestamp)
}

func (cluster *Cluster) GetTableBackupFilePath(contentID int, tableOid uint32, singleDataFile bool) string {
	templateFilePath := cluster.GetTableBackupFilePathForCopyCommand(tableOid, singleDataFile)
	return cluster.replaceCopyFormatStringsInPath(templateFilePath, contentID)
}

func (cluster *Cluster) GetTableBackupFilePathForCopyCommand(tableOid uint32, singleDataFile bool) string {
	backupFilePath := fmt.Sprintf("gpbackup_<SEGID>_%s", cluster.Timestamp)
	if !singleDataFile {
		backupFilePath += fmt.Sprintf("_%d", tableOid)
	}
	if usingCompression {
		backupFilePath += compressionProgram.Extension
	}
	baseDir := "<SEG_DATA_DIR>"
	if cluster.IsUserSpecifiedBackupDir() {
		baseDir = path.Join(cluster.UserSpecifiedBackupDir, fmt.Sprintf("%s<SEGID>", cluster.UserSpecifiedSegPrefix))
	}
	return path.Join(baseDir, "backups", cluster.Timestamp[0:8], cluster.Timestamp, backupFilePath)
}

var metadataFilenameMap = map[string]string{
	"config":            "config.yaml",
	"metadata":          "metadata.sql",
	"statistics":        "statistics.sql",
	"table of contents": "toc.yaml",
	"report":            "report",
}

func (cluster *Cluster) GetBackupFilePath(filetype string) string {
	return path.Join(cluster.GetDirForContent(-1), fmt.Sprintf("gpbackup_%s_%s", cluster.Timestamp, metadataFilenameMap[filetype]))
}

func (cluster *Cluster) GetMetadataFilePath() string {
	return cluster.GetBackupFilePath("metadata")
}

func (cluster *Cluster) GetStatisticsFilePath() string {
	return cluster.GetBackupFilePath("statistics")
}

func (cluster *Cluster) GetTOCFilePath() string {
	return cluster.GetBackupFilePath("table of contents")
}

func (cluster *Cluster) GetReportFilePath() string {
	return cluster.GetBackupFilePath("report")
}

func (cluster *Cluster) GetConfigFilePath() string {
	return cluster.GetBackupFilePath("config")
}

/*
 * This is the temporary location of the segment TOC file before it is moved
 * to its final location in the actual backup directory
 */
func (cluster *Cluster) GetSegmentTOCFilePath(topDir string, contentStr string) string {
	return path.Join(topDir, fmt.Sprintf("gpbackup_%s_%s_toc.yaml", contentStr, cluster.Timestamp))
}

func (cluster *Cluster) GetSegmentTOCFilePathWithPID(topDir string, contentStr string) string {
	return path.Join(topDir, fmt.Sprintf("gpbackup_%s_%s_toc_%d.yaml", contentStr, cluster.Timestamp, cluster.PID))
}

func (cluster *Cluster) GetSegmentHelperFilePath(contentID int, suffix string) string {
	return path.Join(cluster.SegDirMap[contentID], fmt.Sprintf("gpbackup_%d_%s_%s_%d", contentID, cluster.Timestamp, suffix, cluster.PID))
}

/*
 * Helper functions
 */

func GetSegmentConfiguration(connection *DBConn) []SegConfig {
	query := ""
	if connection.Version.Before("6") {
		query = `
SELECT
	s.content as contentid,
	s.hostname,
	e.fselocation as datadir
FROM gp_segment_configuration s
JOIN pg_filespace_entry e ON s.dbid = e.fsedbid
JOIN pg_filespace f ON e.fsefsoid = f.oid
WHERE s.role = 'p' AND f.fsname = 'pg_system'
ORDER BY s.content;`
	} else {
		query = `
SELECT
	content as contentid,
	hostname,
	datadir
FROM gp_segment_configuration
WHERE role = 'p'
ORDER BY content;`
	}

	results := make([]SegConfig, 0)
	err := connection.Select(&results, query)
	CheckError(err)
	return results
}

func GetSegPrefix(connection *DBConn) string {
	query := ""
	if connection.Version.Before("6") {
		query = "SELECT fselocation FROM pg_filespace_entry WHERE fsedbid = 1;"
	} else {
		query = "SELECT datadir FROM gp_segment_configuration WHERE dbid = 1;"
	}
	result := ""
	err := connection.Get(&result, query)
	CheckError(err)
	_, segPrefix := path.Split(result)
	segPrefix = segPrefix[:len(segPrefix)-2] // Remove "-1" segment ID from string
	return segPrefix
}

func ParseSegPrefix(backupDir string) string {
	segPrefix := ""
	if len(backupDir) > 0 {
		masterDir, err := System.Glob(fmt.Sprintf("%s/*-1", backupDir))
		if err != nil || len(masterDir) == 0 {
			logger.Fatal(err, "Master backup directory in %s missing or inaccessible", backupDir)
		}
		_, segPrefix = path.Split(masterDir[0])
		segPrefix = segPrefix[:len(segPrefix)-2] // Remove "-1" segment ID from string
	}
	return segPrefix
}

func ConstructSSHCommand(host string, cmd string) []string {
	currentUser, _, _ := GetUserAndHostInfo()
	return []string{"ssh", "-o", "StrictHostKeyChecking=no", fmt.Sprintf("%s@%s", currentUser, host), cmd}
}
