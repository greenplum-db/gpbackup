package utils

/*
 * This file contains structs and functions related to interacting with files
 * and directories, both locally and remotely over SSH.
 */

import (
	"fmt"
	"os/exec"
	"path"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

var (
	usingCompression   = true
	compressionProgram Compression
)

type Compression struct {
	Name              string
	CompressCommand   string
	DecompressCommand string
	Extension         string
}

func InitializeCompressionParameters(compress bool, compressionLevel int) {
	usingCompression = compress
	compressCommand := ""
	if compressionLevel == 0 {
		compressCommand = "gzip -c -1"
	} else {
		compressCommand = fmt.Sprintf("gzip -c -%d", compressionLevel)
	}
	compressionProgram = Compression{Name: "gzip", CompressCommand: compressCommand, DecompressCommand: "gzip -d -c", Extension: ".gz"}
}

func GetCompressionParameters() (bool, Compression) {
	return usingCompression, compressionProgram
}

func SetCompressionParameters(compress bool, compression Compression) {
	usingCompression = compress
	compressionProgram = compression
}

type Executor interface {
	ExecuteLocalCommand(commandStr string) error
	ExecuteClusterCommand(commandMap map[int][]string) map[int]error
}

// This type only exists to allow us to mock Execute[...]Command functions for testing
type GPDBExecutor struct{}

type Cluster struct {
	ContentIDs             []int
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

/*
 * Cluster functions
 */

func (cluster *Cluster) IsUserSpecifiedBackupDir() bool {
	return cluster.UserSpecifiedBackupDir != ""
}

func NewCluster(segConfigs []SegConfig, userSpecifiedBackupDir string, timestamp string, userSegPrefix string) Cluster {
	cluster := Cluster{}
	cluster.SegHostMap = make(map[int]string, 0)
	cluster.SegDirMap = make(map[int]string, 0)
	cluster.UserSpecifiedBackupDir = userSpecifiedBackupDir
	cluster.UserSpecifiedSegPrefix = userSegPrefix
	cluster.Timestamp = timestamp
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

func (cluster *Cluster) GenerateFileVerificationCommandMap(fileCount int) map[int][]string {
	commandMap := cluster.GenerateSSHCommandMapForSegments(func(contentID int) string {
		return fmt.Sprintf("find %s -type f | wc -l | grep %d", cluster.GetDirForContent(contentID), fileCount)
	})
	return commandMap
}

func (executor *GPDBExecutor) ExecuteLocalCommand(commandStr string) error {
	_, err := exec.Command("bash", "-c", commandStr).CombinedOutput()
	return err
}

func (executor *GPDBExecutor) ExecuteClusterCommand(commandMap map[int][]string) map[int]error {
	errMap := make(map[int]error)
	finished := make(chan int)
	contentIDs := make([]int, 0)
	for key := range commandMap {
		contentIDs = append(contentIDs, key)
	}
	errorList := make([]error, len(contentIDs))
	for i, contentID := range contentIDs {
		go func(index int, segCommand []string) {
			_, errorList[index] = exec.Command(segCommand[0], segCommand[1:]...).CombinedOutput()
			finished <- index
		}(i, commandMap[contentID])
	}
	for i := 0; i < len(contentIDs); i++ {
		index := <-finished
		hostErr := errorList[index]
		if hostErr != nil {
			errMap[contentIDs[index]] = hostErr
		}
	}
	return errMap
}

func (cluster *Cluster) VerifyBackupFileCountOnSegments(fileCount int) {
	commandMap := cluster.GenerateFileVerificationCommandMap(fileCount)
	errMap := cluster.ExecuteClusterCommand(commandMap)
	numErrors := len(errMap)
	if numErrors == 0 {
		return
	}
	s := ""
	if fileCount != 1 {
		s = "s"
	}
	for contentID := range errMap {
		logger.Verbose("Expected to see %d backup file%s on segment %d, but some were missing.", fileCount, s, contentID)
	}
	cluster.LogFatalError("Backup files missing", numErrors)
}

func (cluster *Cluster) VerifyBackupDirectoriesExistOnAllHosts() {
	commandMap := cluster.GenerateSSHCommandMapForCluster(func(contentID int) string {
		return fmt.Sprintf("test -d %s", cluster.GetDirForContent(contentID))
	})
	errMap := cluster.ExecuteClusterCommand(commandMap)
	numErrors := len(errMap)
	if numErrors == 0 {
		return
	}
	for contentID := range errMap {
		logger.Verbose("Directory %s missing or inaccessible for segment %d on host %s", cluster.GetDirForContent(contentID), contentID, cluster.GetHostForContent(contentID))
	}
	cluster.LogFatalError("Directories missing or inaccessible", numErrors)
}

func (cluster *Cluster) CreateBackupDirectoriesOnAllHosts() {
	logger.Verbose("Creating backup directories")
	commandMap := cluster.GenerateSSHCommandMapForCluster(func(contentID int) string {
		return fmt.Sprintf("mkdir -p %s", cluster.GetDirForContent(contentID))
	})
	errMap := cluster.ExecuteClusterCommand(commandMap)
	numErrors := len(errMap)
	if numErrors == 0 {
		return
	}
	for contentID := range errMap {
		logger.Verbose("Unable to create directory %s for segment %d on host %s", cluster.GetDirForContent(contentID), contentID, cluster.GetHostForContent(contentID))
	}
	cluster.LogFatalError("Unable to create directories", numErrors)
}

func (cluster *Cluster) CreateSegmentPipesOnAllHosts() {
	logger.Verbose("Creating segment data pipes")
	commandMap := cluster.GenerateSSHCommandMapForSegments(func(contentID int) string {
		return fmt.Sprintf("mkfifo %s", cluster.GetSegmentPipeFilePath(contentID))
	})
	errMap := cluster.ExecuteClusterCommand(commandMap)
	numErrors := len(errMap)
	if numErrors == 0 {
		return
	}
	for contentID := range errMap {
		logger.Verbose("Unable to create segment data pipe for segment %d on host %s", contentID, cluster.GetHostForContent(contentID))
	}
	cluster.LogFatalError("Unable to create segment data pipes", numErrors)
}

func (cluster *Cluster) CleanUpSegmentPipesOnAllHosts() {
	logger.Verbose("Cleaning up segment data pipes")
	commandMap := cluster.GenerateSSHCommandMapForSegments(func(contentID int) string {
		pipePath := cluster.GetSegmentPipeFilePath(contentID)
		// This cleans up both the pipe itself as well as any gpbackup_helper process associated with it
		return fmt.Sprintf("set -o pipefail; rm -f %s* && ps ux | grep %s | grep -v grep | awk '{print $2}' | xargs kill -9 || true", pipePath, pipePath)
	})
	errMap := cluster.ExecuteClusterCommand(commandMap)
	numErrors := len(errMap)
	if numErrors == 0 {
		return
	}
	for contentID := range errMap {
		logger.Verbose("Unable to clean up segment data pipe for segment %d on host %s", contentID, cluster.GetHostForContent(contentID))
	}
	cluster.LogFatalError("Unable to clean up segment data pipes", numErrors)
}

func (cluster *Cluster) ReadFromSegmentPipes() {
	logger.Verbose("Reading from segment data pipes")
	usingCompression, compressionProgram := GetCompressionParameters()
	commandMap := cluster.GenerateSSHCommandMapForSegments(func(contentID int) string {
		pipeFile := cluster.GetSegmentPipeFilePath(contentID)
		compress := compressionProgram.CompressCommand
		backupFile := cluster.GetTableBackupFilePath(contentID, 0, true)
		if usingCompression {
			return fmt.Sprintf("set -o pipefail; nohup tail -n +1 -f %s | %s > %s &", pipeFile, compress, backupFile)
		}
		return fmt.Sprintf("nohup tail -n +1 -f %s > %s &", pipeFile, backupFile)
	})
	errMap := cluster.ExecuteClusterCommand(commandMap)
	numErrors := len(errMap)
	if numErrors == 0 {
		return
	}
	for contentID := range errMap {
		logger.Verbose("Unable to read from data pipe for segment %d on host %s", contentID, cluster.GetHostForContent(contentID))
	}
	cluster.LogFatalError("Unable to read from segment data pipes", numErrors)
}

func (cluster *Cluster) CleanUpSegmentTailProcesses() {
	logger.Verbose("Cleaning up segment tail processes")
	commandMap := cluster.GenerateSSHCommandMapForSegments(func(contentID int) string {
		filePattern := fmt.Sprintf("gpbackup_%d_%s", contentID, cluster.Timestamp) // Matches pipe name for backup and file name for restore
		return fmt.Sprintf(`ps ux | grep "tail -n +1 -f" | grep "%s" | grep -v "grep" | awk '{print $2}' | xargs kill -9`, filePattern)
	})
	errMap := cluster.ExecuteClusterCommand(commandMap)
	numErrors := len(errMap)
	if numErrors == 0 {
		return
	}
	for contentID, err := range errMap {
		logger.Verbose("Unable to clean up tail process for segment %d on host %s: %s", contentID, cluster.GetHostForContent(contentID), err.Error())
	}
	cluster.LogFatalError("Unable to clean up tail processes", numErrors)
}

func (cluster *Cluster) MoveSegmentTOCsAndMakeReadOnly() {
	logger.Verbose("Setting permissions on segment table of contents files")
	logger.Verbose("Moving segment table of contents files to user-specified backup directory")
	var commandMap map[int][]string
	commandMap = cluster.GenerateSSHCommandMapForSegments(func(contentID int) string {
		tocFile := cluster.GetSegmentTOCFilePath(cluster.SegDirMap[contentID], fmt.Sprintf("%d", contentID))
		str := fmt.Sprintf("chmod 444 %s; mv %s %s/.", tocFile, tocFile, cluster.GetDirForContent(contentID))
		return str
	})
	errMap := cluster.ExecuteClusterCommand(commandMap)
	numErrors := len(errMap)
	if numErrors == 0 {
		return
	}
	for contentID := range errMap {
		tocFile := cluster.GetSegmentTOCFilePath(cluster.SegDirMap[contentID], fmt.Sprintf("%d", contentID))
		logger.Verbose("Unable to set permissions on or copy file %s on segment %d on host %s", tocFile, contentID, cluster.GetHostForContent(contentID))
	}
	cluster.LogFatalError("Unable to set permissions on or copy segment table of contents files", numErrors)
}

func (cluster *Cluster) CopySegmentTOCs() {
	logger.Verbose("Copying segment table of contents files from backup directories")
	var commandMap map[int][]string
	commandMap = cluster.GenerateSSHCommandMapForSegments(func(contentID int) string {
		tocFile := cluster.GetSegmentTOCFilePath(cluster.SegDirMap[contentID], fmt.Sprintf("%d", contentID))
		tocFilename := fmt.Sprintf("gpbackup_%d_%s_toc.yaml", contentID, cluster.Timestamp)
		str := fmt.Sprintf("cp -f %s/%s %s", cluster.GetDirForContent(contentID), tocFilename, tocFile)
		return str
	})
	errMap := cluster.ExecuteClusterCommand(commandMap)
	numErrors := len(errMap)
	if numErrors == 0 {
		return
	}
	for contentID := range errMap {
		tocFile := cluster.GetSegmentTOCFilePath(cluster.SegDirMap[contentID], fmt.Sprintf("%d", contentID))
		logger.Verbose("Unable to copy file %s on segment %d on host %s", tocFile, contentID, cluster.GetHostForContent(contentID))
	}
	cluster.LogFatalError("Unable to copy segment table of contents files from backup directories", numErrors)
}

func (cluster *Cluster) CleanUpSegmentTOCs() {
	logger.Verbose("Removing segment table of contents files from segment data directories")
	var commandMap map[int][]string
	commandMap = cluster.GenerateSSHCommandMapForSegments(func(contentID int) string {
		tocFile := cluster.GetSegmentTOCFilePath(cluster.SegDirMap[contentID], fmt.Sprintf("%d", contentID))
		str := fmt.Sprintf("rm %s", tocFile)
		return str
	})
	errMap := cluster.ExecuteClusterCommand(commandMap)
	numErrors := len(errMap)
	if numErrors == 0 {
		return
	}
	for contentID := range errMap {
		tocFile := cluster.GetSegmentTOCFilePath(cluster.SegDirMap[contentID], fmt.Sprintf("%d", contentID))
		logger.Verbose("Unable to remove file %s on segment %d on host %s", tocFile, contentID, cluster.GetHostForContent(contentID))
	}
	logger.Error("Unable to remove segment table of contents file(s). See %s for a complete list of segments with errors and remove manually.", logger.GetLogFilePath())
}

func (cluster *Cluster) LogFatalError(errMessage string, numErrors int) {
	s := ""
	if numErrors != 1 {
		s = "s"
	}
	logger.Fatal(errors.Errorf("%s on %d segment%s. See %s for a complete list of segments with errors.", errMessage, numErrors, s, logger.GetLogFilePath()), "")
}

func (cluster *Cluster) GetContentList() []int {
	return cluster.ContentIDs
}

func (cluster *Cluster) GetHostForContent(contentID int) string {
	return cluster.SegHostMap[contentID]
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

/*
 * Backup and restore filename functions
 */

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

func (cluster *Cluster) VerifyMetadataFilePaths(dataOnly bool, withStats bool, tableFiltered bool) {
	filetypes := []string{"config", "table of contents"}
	if !dataOnly {
		filetypes = append(filetypes, "metadata")
	}
	missing := false
	for _, filetype := range filetypes {
		filepath := cluster.GetBackupFilePath(filetype)
		if !FileExistsAndIsReadable(filepath) {
			missing = true
			logger.Error("Cannot access %s file %s", filetype, filepath)
		}
	}
	if withStats {
		filepath := cluster.GetStatisticsFilePath()
		if !FileExistsAndIsReadable(filepath) {
			missing = true
			logger.Error("Cannot access statistics file %s", filepath)
			logger.Error(`Note that the "-with-stats" flag must be passed to gpbackup to generate a statistics file.`)
		}
	}
	if missing {
		logger.Fatal(errors.Errorf("One or more metadata files do not exist or are not readable."), "Cannot proceed with restore")
	}
}

/*
 * Helper functions
 */

func GetSegmentConfiguration(connection *DBConn) []SegConfig {
	query := `
SELECT
	s.content as contentid,
	s.hostname,
	e.fselocation as datadir
FROM gp_segment_configuration s
JOIN pg_filespace_entry e ON s.dbid = e.fsedbid
JOIN pg_filespace f ON e.fsefsoid = f.oid
WHERE s.role = 'p' AND f.fsname = 'pg_system'
ORDER BY s.content;`

	results := make([]SegConfig, 0)
	err := connection.Select(&results, query)
	CheckError(err)
	return results
}

func GetSegPrefix(connection *DBConn) string {
	query := "SELECT fselocation FROM pg_filespace_entry WHERE fsedbid = 1;"
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
