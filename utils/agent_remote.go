package utils

import (
	"fmt"
	"io"
	path "path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gp-common-go-libs/iohelper"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/greenplum-db/gpbackup/filepath"
	"github.com/pkg/errors"
)

var helperMutex sync.Mutex

/*
 * Functions to run commands on entire cluster during both backup and restore
 */

/*
 * The reason that gprestore is in charge of creating the first pipe to ensure
 * that the first pipe is created before the first COPY FROM is issued.  If
 * gpbackup_helper was in charge of creating the first pipe, there is a
 * possibility that the COPY FROM commands start before gpbackup_helper is done
 * starting up and setting up the first pipe.
 */
func CreateSegmentPipeOnAllHosts(oid string, c *cluster.Cluster, fpInfo filepath.FilePathInfo) {
	remoteOutput := c.GenerateAndExecuteCommand("Creating segment data pipes", cluster.ON_SEGMENTS, func(contentID int) string {
		pipeName := fpInfo.GetSegmentPipeFilePath(contentID)
		pipeName = fmt.Sprintf("%s_%s", pipeName, oid)
		gplog.Debug("Creating pipe %s", pipeName)
		return fmt.Sprintf("mkfifo -m 0700 %s", pipeName)
	})
	c.CheckClusterError(remoteOutput, "Unable to create segment data pipes", func(contentID int) string {
		return "Unable to create segment data pipe"
	})
}

func WriteOidListToSegments(oidList []string, c *cluster.Cluster, fpInfo filepath.FilePathInfo, fileSuffix string) {
	rsync_exists := CommandExists("rsync")
	if !rsync_exists {
		gplog.Fatal(errors.New("Failed to find rsync on PATH. Please ensure rsync is installed."), "")
	}

	localOidFile, err := operating.System.TempFile("", "gpbackup-oids")
	gplog.FatalOnError(err, "Cannot open temporary file to write oids")
	defer func() {
		err = operating.System.Remove(localOidFile.Name())
		if err != nil {
			gplog.Warn("Cannot remove temporary oid file: %s, Err: %s", localOidFile.Name(), err.Error())
		}
	}()

	WriteOidsToFile(localOidFile.Name(), oidList)

	generateScpCmd := func(contentID int) string {
		sourceFile := localOidFile.Name()
		hostname := c.GetHostForContent(contentID)
		dest := fpInfo.GetSegmentHelperFilePath(contentID, fileSuffix)

		return fmt.Sprintf(`rsync -e ssh %s %s:%s`, sourceFile, hostname, dest)
	}
	remoteOutput := c.GenerateAndExecuteCommand("rsync oid file to segments", cluster.ON_LOCAL|cluster.ON_SEGMENTS, generateScpCmd)

	errMsg := "Failed to rsync oid file"
	errFunc := func(contentID int) string {
		return "Failed to run rsync"
	}
	c.CheckClusterError(remoteOutput, errMsg, errFunc, false)
}

func WriteOidsToFile(filename string, oidList []string) {
	oidFp, err := iohelper.OpenFileForWriting(filename)
	gplog.FatalOnError(err, filename)
	defer func() {
		err = oidFp.Close()
		gplog.FatalOnError(err, filename)
	}()

	err = WriteOids(oidFp, oidList)
	gplog.FatalOnError(err, filename)
}

func WriteOids(writer io.Writer, oidList []string) error {
	var err error
	for _, oid := range oidList {
		_, err = writer.Write([]byte(oid + "\n"))
		if err != nil {
			// error logging handled in calling functions
			return err
		}
	}

	return nil
}

func VerifyHelperVersionOnSegments(version string, c *cluster.Cluster) {
	remoteOutput := c.GenerateAndExecuteCommand("Verifying gpbackup_helper version", cluster.ON_HOSTS, func(contentID int) string {
		gphome := operating.System.Getenv("GPHOME")
		return fmt.Sprintf("%s/bin/gpbackup_helper --version", gphome)
	})
	c.CheckClusterError(remoteOutput, "Could not verify gpbackup_helper version", func(contentID int) string {
		return "Could not verify gpbackup_helper version"
	})

	numIncorrect := 0
	for contentID, cmd := range remoteOutput.Commands {
		parsedSegVersion := ""
		segVersion := strings.TrimSpace(cmd.Stdout) // Expected format is "gpbackup_helper version [version string]"
		splitSegVersion := strings.Split(segVersion, " ")
		if len(splitSegVersion) == 3 {
			// Array access placed inside a length guard to keep error messages nice, instead of
			// spilling panics to the log
			parsedSegVersion = splitSegVersion[2]
		}
		if parsedSegVersion != version {
			gplog.Verbose("Version mismatch for gpbackup_helper on segment %d on host %s: Expected version %s, found version %s.", contentID, c.GetHostForContent(contentID), version, parsedSegVersion)
			numIncorrect++
		}
	}
	if numIncorrect > 0 {
		cluster.LogFatalClusterError("The version of gpbackup_helper must match the version of gpbackup/gprestore, but found gpbackup_helper binaries with invalid version", cluster.ON_HOSTS, numIncorrect)
	}
}

func StartGpbackupHelpers(c *cluster.Cluster, fpInfo filepath.FilePathInfo, operation string, pluginConfigFile string, compressStr string, onErrorContinue bool, isFilter bool, wasTerminated *bool, copyQueue int, isSingleDataFile bool, resizeCluster bool, origSize int, destSize int) {
	// A mutex lock for cleaning up and starting gpbackup helpers prevents a
	// race condition that causes gpbackup_helpers to be orphaned if
	// gpbackup_helper cleanup happens before they are started.
	helperMutex.Lock()
	if *wasTerminated {
		helperMutex.Unlock()
		select {} // Pause forever and wait for cleanup to exit program.
	}
	defer helperMutex.Unlock()

	gphomePath := operating.System.Getenv("GPHOME")
	pluginStr := ""
	if pluginConfigFile != "" {
		_, configFilename := path.Split(pluginConfigFile)
		pluginStr = fmt.Sprintf(" --plugin-config /tmp/%s", configFilename)
	}
	onErrorContinueStr := ""
	if onErrorContinue {
		onErrorContinueStr = " --on-error-continue"
	}
	filterStr := ""
	if isFilter {
		filterStr = " --with-filters"
	}
	singleDataFileStr := ""
	if isSingleDataFile {
		singleDataFileStr = " --single-data-file"
	}
	resizeStr := ""
	if resizeCluster {
		resizeStr = fmt.Sprintf(" --resize-cluster --orig-seg-count %d --dest-seg-count %d", origSize, destSize)
	}
	remoteOutput := c.GenerateAndExecuteCommand("Starting gpbackup_helper agent", cluster.ON_SEGMENTS, func(contentID int) string {
		tocFile := fpInfo.GetSegmentTOCFilePath(contentID)
		oidFile := fpInfo.GetSegmentHelperFilePath(contentID, "oid")
		scriptFile := fpInfo.GetSegmentHelperFilePath(contentID, "script")
		pipeFile := fpInfo.GetSegmentPipeFilePath(contentID)
		backupFile := fpInfo.GetTableBackupFilePath(contentID, 0, GetPipeThroughProgram().Extension, true)
		replicatedOidFile := fpInfo.GetSegmentHelperFilePath(contentID, "replicated_oid")
		helperCmdStr := fmt.Sprintf(`gpbackup_helper %s --toc-file %s --oid-file %s --pipe-file %s --data-file "%s" --content %d%s%s%s%s%s%s --copy-queue-size %d --replication-file %s`,
			operation, tocFile, oidFile, pipeFile, backupFile, contentID, pluginStr, compressStr, onErrorContinueStr, filterStr, singleDataFileStr, resizeStr, copyQueue, replicatedOidFile)
		// we run these commands in sequence to ensure that any failure is critical; the last command ensures the agent process was successfully started
		return fmt.Sprintf(`cat << HEREDOC > %[1]s && chmod +x %[1]s && ( nohup %[1]s &> /dev/null &)
#!/bin/bash
source %[2]s/greenplum_path.sh
%[2]s/bin/%s

HEREDOC

`, scriptFile, gphomePath, helperCmdStr)
	})
	c.CheckClusterError(remoteOutput, "Error starting gpbackup_helper agent", func(contentID int) string {
		return "Error starting gpbackup_helper agent"
	})
}

func findCommandStr(c *cluster.Cluster, fpInfo filepath.FilePathInfo, fileType string, contentID int) string {
	return fmt.Sprintf(`find %s -type %s -regextype posix-extended -regex ".*gpbackup_%d_%s_(oid|script|replicated_oid|pipe)_%d.*"`,
		c.GetDirForContent(contentID), fileType, contentID, fpInfo.Timestamp, fpInfo.PID)
}

func GetHelperFileCount(c *cluster.Cluster, fpInfo filepath.FilePathInfo) int {
	totalFiles := 0
	remoteOutput := c.GenerateAndExecuteCommand("Checking for leftover gpbackup_helper files on segments", cluster.ON_SEGMENTS, func(contentID int) string {
		return fmt.Sprintf("%s %s", findCommandStr(c, fpInfo, "f", contentID), "| wc -l")
	})

	if remoteOutput.NumErrors > 0 {
		gplog.Error("Unable to check for leftover gpbackup_helper files on segments")
	} else {
		for _, cmd := range remoteOutput.Commands {
			numFiles, _ := strconv.Atoi(strings.TrimSuffix(cmd.Stdout, "\n"))
			if numFiles > 0 {
				totalFiles = totalFiles + numFiles
			}
		}
	}
	return totalFiles
}

// Removes all gpbackup_helper files from the segment data directories.
// It's expected that gpbackup_helper cleans up the files on exit, but that is not guaranteed,
// so this function is used to clean up any leftover files.
func RemoveHelperFiles(c *cluster.Cluster, fpInfo filepath.FilePathInfo) {
	errMsg := fmt.Sprintf("Unable to remove gpbackup_helper file(s). See %s for a complete list of segments with errors and remove manually.",
		gplog.GetLogFilePath())

	remoteOutput := c.GenerateAndExecuteCommand("Removing gpbackup_helper files from segment data directories", cluster.ON_SEGMENTS, func(contentID int) string {
		return fmt.Sprintf("%s %s", findCommandStr(c, fpInfo, "f", contentID), `-exec rm -f {} \;`)
	})

	c.CheckClusterError(remoteOutput, errMsg, func(contentID int) string {
		return fmt.Sprintf("Unable to remove gpbackup_helper file(s)\n\t%s", findCommandStr(c, fpInfo, "f", contentID))
	}, true)
}

func CleanUpHelperFilesOnAllHosts(c *cluster.Cluster, fpInfo filepath.FilePathInfo, timeout time.Duration) {
	var fileCount int
	helperMutex.Lock()
	defer helperMutex.Unlock()
	tickerCleanup := time.NewTicker(1 * time.Second)

	for {
		fileCount = GetHelperFileCount(c, fpInfo)
		select {
		case <-tickerCleanup.C:
			if fileCount == 0 {
				return
			}
			RemoveHelperFiles(c, fpInfo)
		case <-time.After(timeout):
			gplog.Warn("Timeout of %ds reached while waiting for %d gpbackup_helper file(s) to be removed.", int(timeout.Seconds()), fileCount)
			return
		}
	}
}

func GetHelperPipeCount(c *cluster.Cluster, fpInfo filepath.FilePathInfo) int {
	totalPipes := 0
	remoteOutput := c.GenerateAndExecuteCommand("Checking for leftover gpbackup_helper data pipes", cluster.ON_SEGMENTS, func(contentID int) string {
		return fmt.Sprintf("%s %s", findCommandStr(c, fpInfo, "p", contentID), "| wc -l")
	})
	for contentID, cmd := range remoteOutput.Commands {
		numPipes, _ := strconv.Atoi(cmd.Stdout)
		if numPipes > 0 {
			gplog.Debug("Found %d leftover segment data pipes on segment %d on host %s",
				numPipes, contentID, c.GetHostForContent(contentID))
			totalPipes += numPipes
		}
	}
	return totalPipes
}

// Removes all gpbackup_helper pipes from the segment data directories.
// It's expected that gpbackup_helper cleans up the pipes on exit, but that is not guaranteed,
// so this function is used to clean up any leftover pipes.
func RemoveHelperPipes(c *cluster.Cluster, fpInfo filepath.FilePathInfo) {
	errMsg := fmt.Sprintf("Unable to remove gpbackup_helper pipe(s). See %s for a complete list of segments with errors and remove manually.",
		gplog.GetLogFilePath())

	remoteOutput := c.GenerateAndExecuteCommand("Removing gpbackup_helper pipes from segment data directories", cluster.ON_SEGMENTS, func(contentID int) string {
		return fmt.Sprintf("%s %s", findCommandStr(c, fpInfo, "p", contentID), `-exec rm -f {} \;`)
	})

	c.CheckClusterError(remoteOutput, errMsg, func(contentID int) string {
		return fmt.Sprintf("Unable to remove gpbackup_helper pipe(s)\n\t%s", findCommandStr(c, fpInfo, "p", contentID))
	}, true)
}

func CleanUpPipesOnAllHosts(c *cluster.Cluster, fpInfo filepath.FilePathInfo, timeout time.Duration) {
	var pipeCount int
	helperMutex.Lock()
	defer helperMutex.Unlock()
	tickerCleanup := time.NewTicker(1 * time.Second)

	for {
		select {
		case <-tickerCleanup.C:
			pipeCount = GetHelperPipeCount(c, fpInfo)
			if pipeCount == 0 {
				return
			}
			RemoveHelperFiles(c, fpInfo)
		case <-time.After(timeout):
			gplog.Warn("Timeout of %ds reached while waiting for %d gpbackup_helper file(s) to be removed.", int(timeout.Seconds()), pipeCount)
			return
		}
	}
}

// Checks for gpbackup_helper processes on segments.
// Returns a boolean indicating whether any PIDs were found and a map of hostnames to PIDs.
func CheckHelperPids(c *cluster.Cluster, fpInfo filepath.FilePathInfo, operation string) (map[string][]int, bool) {
	var foundPid bool = false
	remoteOutput := c.GenerateAndExecuteCommand("Checking for leftover gpbackup_helper processes", cluster.ON_SEGMENTS, func(contentID int) string {
		tocFile := fpInfo.GetSegmentTOCFilePath(contentID)
		procPattern := fmt.Sprintf("gpbackup_helper --%s-agent --toc-file %s*", operation, tocFile)
		return fmt.Sprintf(`ps ux | grep "%s" | grep -v grep | awk '{print $2}'`, procPattern)
	})
	pidMap := make(map[string][]int)
	for _, cmd := range remoteOutput.Commands {
		pids := strings.Split(strings.TrimSpace(cmd.Stdout), "\n")
		host := c.GetHostForContent(cmd.Content)
		for _, pid := range pids {
			if pid != "" {
				foundPid = true
				gplog.Debug("Found gpbackup_helper process %s for segment %d on host %s", pid, cmd.Content, host)
				pidInt, _ := strconv.Atoi(pid)
				pidMap[host] = append(pidMap[host], pidInt)
			}
		}
	}
	return pidMap, foundPid
}

// Checks for gpbackup_helper processes on segments and sends a USR1 signal to them to terminate.
// If the processes are not terminated within the timeout, a warning is logged.
// Ideally, the termination requests would only be sent to the segment hosts reported by CheckHelperPids,
// but the current design of cluster.GenerateAndExecuteCommand does not allow for that, so we a
// command to check for pids and send the termination signal to all segments if CheckHelperPids reports
// that there are still processes running.
func CleanUpSegmentHelperProcesses(c *cluster.Cluster, fpInfo filepath.FilePathInfo, operation string, timeout time.Duration) {
	helperMutex.Lock()
	defer helperMutex.Unlock()
	tickerCleanup := time.NewTicker(1 * time.Second)

	for {
		helperPids, found := CheckHelperPids(c, fpInfo, operation)
		select {
		case <-tickerCleanup.C:
			if !found {
				return
			}
			c.GenerateAndExecuteCommand("Cleaning up gpbackup_helper processes", cluster.ON_SEGMENTS, func(contentID int) string {
				tocFile := fpInfo.GetSegmentTOCFilePath(contentID)
				procPattern := fmt.Sprintf("gpbackup_helper --%s-agent --toc-file %s", operation, tocFile)
				return fmt.Sprintf("PIDS=`ps ux | grep \"%s\" | grep -v grep | awk '{print $2}'`; if [[ ! -z \"$PIDS\" ]]; then kill -USR1 $PIDS; fi", procPattern)
			})
		case <-time.After(timeout):
			for host, pids := range helperPids {
				if len(pids) > 0 {
					gplog.Warn("Unable to terminate segment helper process(es) on host %s. ", host)
				}
			}
			gplog.Warn("See %s for a complete list of segments with errors and terminate manually.", gplog.GetLogFilePath())
			return
		}
	}
}

func CheckAgentErrorsOnSegments(c *cluster.Cluster, fpInfo filepath.FilePathInfo) error {
	remoteOutput := c.GenerateAndExecuteCommand("Checking whether segment agents had errors", cluster.ON_SEGMENTS, func(contentID int) string {
		errorFile := fmt.Sprintf("%s_error", fpInfo.GetSegmentPipeFilePath(contentID))
		/*
		 * If an error file exists we want to indicate an error, as that means
		 * the agent errored out.  If no file exists, the agent was successful.
		 */
		return fmt.Sprintf("if [[ -f %s ]]; then echo 'error'; fi; rm -f %s", errorFile, errorFile)
	})

	numErrors := 0
	for contentID, cmd := range remoteOutput.Commands {
		if strings.TrimSpace(cmd.Stdout) == "error" {
			gplog.Verbose("Error occurred with helper agent on segment %d on host %s.", contentID, c.GetHostForContent(contentID))
			numErrors++
		}
	}
	if numErrors > 0 {
		helperLogName := fpInfo.GetHelperLogPath()
		return errors.Errorf("Encountered errors with %d helper agent(s).  See %s for a complete list of segments with errors, and see %s on the corresponding hosts for detailed error messages.",
			numErrors, gplog.GetLogFilePath(), helperLogName)
	}
	return nil
}

func CreateSkipFileOnSegments(oid string, tableName string, c *cluster.Cluster, fpInfo filepath.FilePathInfo) {
	createSkipFileLogMsg := fmt.Sprintf("Creating skip file on segments for restore entry %s (%s)", oid, tableName)
	remoteOutput := c.GenerateAndExecuteCommand(createSkipFileLogMsg, cluster.ON_SEGMENTS, func(contentID int) string {
		return fmt.Sprintf("touch %s_skip_%s", fpInfo.GetSegmentPipeFilePath(contentID), oid)
	})
	c.CheckClusterError(remoteOutput, "Error while creating skip file on segments", func(contentID int) string {
		return fmt.Sprintf("Could not create skip file %s_skip_%s on segments", fpInfo.GetSegmentPipeFilePath(contentID), oid)
	})
}
