package utils

/*
 * This file contains structs and functions related to interacting with files
 * and directories, both locally and remotely over SSH.
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

func NewRemoteOutput(numIDs int) *RemoteOutput {
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
	output := NewRemoteOutput(length)
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
		cluster.LogFatalError(finalErrMsg, remoteOutput.NumErrors)
	}
}

func (cluster *Cluster) VerifyBackupDirectoriesExistOnAllHosts() {
	remoteOutput := cluster.GenerateAndExecuteCommand("Verifying backup directories exist", func(contentID int) string {
		return fmt.Sprintf("test -d %s", cluster.GetDirForContent(contentID))
	}, true)
	cluster.CheckClusterError(remoteOutput, "Backup directories missing or inaccessible", func(contentID int) string {
		return fmt.Sprintf("Backup directory %s missing or inaccessible", cluster.GetDirForContent(contentID))
	})
}

func (cluster *Cluster) CreateBackupDirectoriesOnAllHosts() {
	remoteOutput := cluster.GenerateAndExecuteCommand("Creating backup directories", func(contentID int) string {
		return fmt.Sprintf("mkdir -p %s", cluster.GetDirForContent(contentID))
	}, true)
	cluster.CheckClusterError(remoteOutput, "Unable to create backup directories", func(contentID int) string {
		return fmt.Sprintf("Unable to create backup directory %s", cluster.GetDirForContent(contentID))
	})
}

/*
 * This is used to create a pipe file during backup without an oid in the name
 */
func (cluster *Cluster) CreateSegmentPipesOnAllHostsForBackup() {
	remoteOutput := cluster.GenerateAndExecuteCommand("Creating segment data pipes", func(contentID int) string {
		pipeName := cluster.GetSegmentPipeFilePath(contentID)
		return fmt.Sprintf("mkfifo %s", pipeName)
	})
	cluster.CheckClusterError(remoteOutput, "Unable to create segment data pipes", func(contentID int) string {
		return "Unable to create segment data pipe"
	})
}

/*
 * This is used to create the first table pipe for the restore agent with an oid in the name.
 */
func (cluster *Cluster) CreateSegmentPipesOnAllHostsForRestore(oid uint32) {
	remoteOutput := cluster.GenerateAndExecuteCommand("Creating segment data pipes", func(contentID int) string {
		pipeName := cluster.GetSegmentPipeFilePathWithPID(contentID)
		pipeName = fmt.Sprintf("%s_%d", pipeName, oid)
		return fmt.Sprintf("mkfifo %s", pipeName)
	})
	cluster.CheckClusterError(remoteOutput, "Unable to create segment data pipes", func(contentID int) string {
		return "Unable to create segment data pipe"
	})
}

func (cluster *Cluster) CleanUpSegmentPipesOnAllHosts() {
	remoteOutput := cluster.GenerateAndExecuteCommand("Cleaning up segment data pipes", func(contentID int) string {
		pipePath := cluster.GetSegmentPipeFilePath(contentID)
		scriptPath := cluster.GetSegmentHelperFilePath(contentID, "script")
		// This cleans up both the pipe itself as well as any gpbackup_helper process associated with it
		return fmt.Sprintf("set -o pipefail; rm -f %s* && ps ux | grep %s | grep -v grep | awk '{print $2}' | xargs kill -9 || true", pipePath, scriptPath)
	})
	cluster.CheckClusterError(remoteOutput, "Unable to clean up segment data pipes", func(contentID int) string {
		return "Unable to clean up segment data pipe"
	})
}

func (cluster *Cluster) ReadFromSegmentPipes() {
	remoteOutput := cluster.GenerateAndExecuteCommand("Reading from segment data pipes", func(contentID int) string {
		usingCompression, compressionProgram := GetCompressionParameters()
		pipeFile := cluster.GetSegmentPipeFilePath(contentID)
		compress := compressionProgram.CompressCommand
		backupFile := cluster.GetTableBackupFilePath(contentID, 0, true)
		if usingCompression {
			return fmt.Sprintf("set -o pipefail; nohup tail -n +1 -f %s | %s > %s &", pipeFile, compress, backupFile)
		}
		return fmt.Sprintf("nohup tail -n +1 -f %s > %s &", pipeFile, backupFile)
	})
	cluster.CheckClusterError(remoteOutput, "Unable to read from segment data pipes", func(contentID int) string {
		return "Unable to read from segment data pipe"
	})
}

func (cluster *Cluster) CleanUpSegmentTailProcesses() {
	remoteOutput := cluster.GenerateAndExecuteCommand("Cleaning up segment tail processes", func(contentID int) string {
		filePattern := fmt.Sprintf("gpbackup_%d_%s", contentID, cluster.Timestamp) // Matches pipe name for backup and file name for restore
		return fmt.Sprintf(`ps ux | grep "tail -n +1 -f" | grep "%s" | grep -v "grep" | awk '{print $2}' | xargs kill -9`, filePattern)
	})
	cluster.CheckClusterError(remoteOutput, "Unable to clean up tail processes", func(contentID int) string {
		return "Unable to clean up tail process"
	})
}

func (cluster *Cluster) WriteToSegmentPipes() {
	remoteOutput := cluster.GenerateAndExecuteCommand("Writing to segment data pipes", func(contentID int) string {
		tocFile := cluster.GetSegmentTOCFilePathWithPID(cluster.SegDirMap[contentID], fmt.Sprintf("%d", contentID))
		oidFile := cluster.GetSegmentHelperFilePath(contentID, "oid")
		scriptFile := cluster.GetSegmentHelperFilePath(contentID, "script")
		pipeFile := cluster.GetSegmentPipeFilePathWithPID(contentID)
		backupFile := cluster.GetTableBackupFilePath(contentID, 0, true)
		gphomePath := System.Getenv("GPHOME")
		return fmt.Sprintf(`cat << HEREDOC > %s
#!/bin/bash
%s/bin/gpbackup_helper --restore-agent --toc-file %s --oid-file %s --pipe-file %s --data-file %s --content %d
HEREDOC

chmod +x %s; (nohup %s > /dev/null 2>&1 &) &`, scriptFile, gphomePath, tocFile, oidFile, pipeFile, backupFile, contentID, scriptFile, scriptFile)
	})
	cluster.CheckClusterError(remoteOutput, "Unable to write to segment data pipes", func(contentID int) string {
		return fmt.Sprintf("Unable to write to data pipe for segment %d on host %s", contentID, cluster.GetHostForContent(contentID))
	})
}

func (cluster *Cluster) WriteOidListToSegments(filteredEntries []MasterDataEntry) {
	filteredOids := make([]string, len(filteredEntries))
	for i, entry := range filteredEntries {
		filteredOids[i] = fmt.Sprintf("%d", entry.Oid)
	}
	oidStr := strings.Join(filteredOids, "\n")
	remoteOutput := cluster.GenerateAndExecuteCommand("Writing filtered oid list to segments", func(contentID int) string {
		oidFile := cluster.GetSegmentHelperFilePath(contentID, "oid")
		return fmt.Sprintf(`echo "%s" > %s`, oidStr, oidFile)
	})
	cluster.CheckClusterError(remoteOutput, "Unable to write oid list to segments", func(contentID int) string {
		return fmt.Sprintf("Unable to write oid list for segment %d on host %s", contentID, cluster.GetHostForContent(contentID))
	})
}

func (cluster *Cluster) CleanUpHelperFilesOnAllHosts() {
	remoteOutput := cluster.GenerateAndExecuteCommand("Removing oid list and helper script files from segment data directories", func(contentID int) string {
		oidFile := cluster.GetSegmentHelperFilePath(contentID, "oid")
		scriptFile := cluster.GetSegmentHelperFilePath(contentID, "script")
		return fmt.Sprintf("rm %s && rm %s", oidFile, scriptFile)
	})
	errMsg := fmt.Sprintf("Unable to remove segment helper file(s). See %s for a complete list of segments with errors and remove manually.",
		logger.GetLogFilePath())
	cluster.CheckClusterError(remoteOutput, errMsg, func(contentID int) string {
		tocFile := cluster.GetSegmentTOCFilePathWithPID(cluster.SegDirMap[contentID], fmt.Sprintf("%d", contentID))
		return fmt.Sprintf("Unable to remove helper file %s on segment %d on host %s", tocFile, contentID, cluster.GetHostForContent(contentID))
	}, true)
}

func (cluster *Cluster) MoveSegmentTOCsAndMakeReadOnly() {
	remoteOutput := cluster.GenerateAndExecuteCommand("Setting permissions on segment table of contents files and moving to backup directories", func(contentID int) string {
		tocFile := cluster.GetSegmentTOCFilePath(cluster.SegDirMap[contentID], fmt.Sprintf("%d", contentID))
		return fmt.Sprintf("chmod 444 %s; mv %s %s/.", tocFile, tocFile, cluster.GetDirForContent(contentID))
	})
	cluster.CheckClusterError(remoteOutput, "Unable to set permissions on or move segment table of contents files", func(contentID int) string {
		return fmt.Sprintf("Unable to set permissions on or move file %s", cluster.GetSegmentTOCFilePath(cluster.SegDirMap[contentID], fmt.Sprintf("%d", contentID)))
	})
}

func (cluster *Cluster) CopySegmentTOCs() {
	remoteOutput := cluster.GenerateAndExecuteCommand("Copying segment table of contents files from backup directories", func(contentID int) string {
		tocFile := cluster.GetSegmentTOCFilePathWithPID(cluster.SegDirMap[contentID], fmt.Sprintf("%d", contentID))
		tocFilename := fmt.Sprintf("gpbackup_%d_%s_toc.yaml", contentID, cluster.Timestamp)
		return fmt.Sprintf("cp -f %s/%s %s", cluster.GetDirForContent(contentID), tocFilename, tocFile)
	})
	cluster.CheckClusterError(remoteOutput, "Unable to copy segment table of contents files from backup directories", func(contentID int) string {
		return fmt.Sprintf("Unable to copy segment table of contents file to %s", cluster.GetSegmentTOCFilePathWithPID(cluster.SegDirMap[contentID], fmt.Sprintf("%d", contentID)))
	})
}

func (cluster *Cluster) VerifyBackupFileCountOnSegments(fileCount int) {
	remoteOutput := cluster.GenerateAndExecuteCommand("Verifying backup file count", func(contentID int) string {
		return fmt.Sprintf("find %s -type f | wc -l", cluster.GetDirForContent(contentID))
	})
	cluster.CheckClusterError(remoteOutput, "Could not verify backup file count", func(contentID int) string {
		return "Could not verify backup file count"
	})

	s := ""
	if fileCount != 1 {
		s = "s"
	}
	numIncorrect := 0
	for contentID := range remoteOutput.Stdouts {
		numFound, _ := strconv.Atoi(strings.TrimSpace(remoteOutput.Stdouts[contentID]))
		if numFound != fileCount {
			logger.Verbose("Expected to find %d file%s on segment %d on host %s, but found %d instead.", fileCount, s, contentID, cluster.GetHostForContent(contentID), numFound)
			numIncorrect++
		}
	}
	if numIncorrect > 0 {
		cluster.LogFatalError("Found incorrect number of backup files", numIncorrect)
	}
}

func (cluster *Cluster) VerifyHelperVersionOnSegments(version string) {
	remoteOutput := cluster.GenerateAndExecuteCommand("Verifying gpbackup_helper version", func(contentID int) string {
		gphome := System.Getenv("GPHOME")
		return fmt.Sprintf("%s/bin/gpbackup_helper --version", gphome)
	})
	cluster.CheckClusterError(remoteOutput, "Could not verify gpbackup_helper version", func(contentID int) string {
		return "Could not verify gpbackup_helper version"
	})

	numIncorrect := 0
	for contentID := range remoteOutput.Stdouts {
		segVersion := strings.TrimSpace(remoteOutput.Stdouts[contentID])
		segVersion = strings.Split(segVersion, " ")[1] // Format is "gpbackup_helper [version string]"
		if segVersion != version {
			logger.Verbose("Version mismatch for gpbackup_helper on segment %d on host %s: Expected version %s, found version %s.", contentID, cluster.GetHostForContent(contentID), version, segVersion)
			numIncorrect++
		}
	}
	if numIncorrect > 0 {
		cluster.LogFatalError("The version of gpbackup_helper must match the version of gprestore, but found gpbackup_helper binaries with invalid version", numIncorrect)
	}
}

func (cluster *Cluster) CleanUpSegmentTOCs() {
	remoteOutput := cluster.GenerateAndExecuteCommand("Removing segment table of contents files from segment data directories", func(contentID int) string {
		tocFile := cluster.GetSegmentTOCFilePathWithPID(cluster.SegDirMap[contentID], fmt.Sprintf("%d", contentID))
		return fmt.Sprintf("rm %s", tocFile)
	})
	errMsg := fmt.Sprintf("Unable to remove segment table of contents file(s). See %s for a complete list of segments with errors and remove manually.",
		logger.GetLogFilePath())
	cluster.CheckClusterError(remoteOutput, errMsg, func(contentID int) string {
		tocFile := cluster.GetSegmentTOCFilePathWithPID(cluster.SegDirMap[contentID], fmt.Sprintf("%d", contentID))
		return fmt.Sprintf("Unable to remove table of contents file %s on segment %d on host %s", tocFile, contentID, cluster.GetHostForContent(contentID))
	}, true)
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

func (cluster *Cluster) GetSegmentTOCFilePathWithPID(topDir string, contentStr string) string {
	return path.Join(topDir, fmt.Sprintf("gpbackup_%s_%s_toc_%d.yaml", contentStr, cluster.Timestamp, cluster.PID))
}

func (cluster *Cluster) GetSegmentHelperFilePath(contentID int, suffix string) string {
	return path.Join(cluster.SegDirMap[contentID], fmt.Sprintf("gpbackup_%d_%s_%s_%d", contentID, cluster.Timestamp, suffix, cluster.PID))
}

func (cluster *Cluster) VerifyMetadataFilePaths(dataOnly bool, withStats bool) {
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
