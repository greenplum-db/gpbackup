package utils

import (
	"fmt"
	"io"
	path "path/filepath"
	"strings"
	"sync"

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
		return fmt.Sprintf("mkfifo %s", pipeName)
	})
	c.CheckClusterError(remoteOutput, "Unable to create segment data pipes", func(contentID int) string {
		return "Unable to create segment data pipe"
	})
}

func WriteOidListToSegments(oidList []string, c *cluster.Cluster, fpInfo filepath.FilePathInfo) {
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
		dest := fpInfo.GetSegmentHelperFilePath(contentID, "oid")

		return fmt.Sprintf(`scp %s %s:%s`, sourceFile, hostname, dest)
	}
	remoteOutput := c.GenerateAndExecuteCommand("Scp oid file to segments", cluster.ON_LOCAL|cluster.ON_SEGMENTS, generateScpCmd)

	errMsg := "Failed to scp oid file"
	errFunc := func(contentID int) string {
		return "Failed to run scp"
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
		segVersion := strings.TrimSpace(cmd.Stdout)
		segVersion = strings.Split(segVersion, " ")[2] // Format is "gpbackup_helper version [version string]"
		if segVersion != version {
			gplog.Verbose("Version mismatch for gpbackup_helper on segment %d on host %s: Expected version %s, found version %s.", contentID, c.GetHostForContent(contentID), version, segVersion)
			numIncorrect++
		}
	}
	if numIncorrect > 0 {
		cluster.LogFatalClusterError("The version of gpbackup_helper must match the version of gpbackup/gprestore, but found gpbackup_helper binaries with invalid version", cluster.ON_HOSTS, numIncorrect)
	}
}

func StartGpbackupHelpers(c *cluster.Cluster, fpInfo filepath.FilePathInfo, operation string, pluginConfigFile string, compressStr string, onErrorContinue bool, isFilter bool, wasTerminated *bool, copyQueue int) {
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
	remoteOutput := c.GenerateAndExecuteCommand("Starting gpbackup_helper agent", cluster.ON_SEGMENTS, func(contentID int) string {
		tocFile := fpInfo.GetSegmentTOCFilePath(contentID)
		oidFile := fpInfo.GetSegmentHelperFilePath(contentID, "oid")
		scriptFile := fpInfo.GetSegmentHelperFilePath(contentID, "script")
		pipeFile := fpInfo.GetSegmentPipeFilePath(contentID)
		backupFile := fpInfo.GetTableBackupFilePath(contentID, 0, GetPipeThroughProgram().Extension, true)
		helperCmdStr := fmt.Sprintf("gpbackup_helper %s --toc-file %s --oid-file %s --pipe-file %s --data-file %s --content %d%s%s%s%s --copy-queue-size %d", operation, tocFile, oidFile, pipeFile, backupFile, contentID, pluginStr, compressStr, onErrorContinueStr, filterStr, copyQueue)
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

func CleanUpHelperFilesOnAllHosts(c *cluster.Cluster, fpInfo filepath.FilePathInfo) {
	remoteOutput := c.GenerateAndExecuteCommand("Removing oid list and helper script files from segment data directories", cluster.ON_SEGMENTS, func(contentID int) string {
		errorFile := fmt.Sprintf("%s_error", fpInfo.GetSegmentPipeFilePath(contentID))
		oidFile := fpInfo.GetSegmentHelperFilePath(contentID, "oid")
		scriptFile := fpInfo.GetSegmentHelperFilePath(contentID, "script")
		return fmt.Sprintf("rm -f %s && rm -f %s && rm -f %s", errorFile, oidFile, scriptFile)
	})
	errMsg := fmt.Sprintf("Unable to remove segment helper file(s). See %s for a complete list of segments with errors and remove manually.",
		gplog.GetLogFilePath())
	c.CheckClusterError(remoteOutput, errMsg, func(contentID int) string {
		errorFile := fmt.Sprintf("%s_error", fpInfo.GetSegmentPipeFilePath(contentID))
		return fmt.Sprintf("Unable to remove helper file %s on segment %d on host %s", errorFile, contentID, c.GetHostForContent(contentID))
	}, true)
}

func CleanUpSegmentHelperProcesses(c *cluster.Cluster, fpInfo filepath.FilePathInfo, operation string) {
	helperMutex.Lock()
	defer helperMutex.Unlock()

	remoteOutput := c.GenerateAndExecuteCommand("Cleaning up segment agent processes", cluster.ON_SEGMENTS, func(contentID int) string {
		tocFile := fpInfo.GetSegmentTOCFilePath(contentID)
		procPattern := fmt.Sprintf("gpbackup_helper --%s-agent --toc-file %s", operation, tocFile)
		/*
		 * We try to avoid erroring out if no gpbackup_helper processes are found,
		 * as it's possible that all gpbackup_helper processes have finished by
		 * the time DoCleanup is called.
		 */
		return fmt.Sprintf("PIDS=`ps ux | grep \"%s\" | grep -v grep | awk '{print $2}'`; if [[ ! -z \"$PIDS\" ]]; then kill -USR1 $PIDS; fi", procPattern)
	})
	c.CheckClusterError(remoteOutput, "Unable to clean up agent processes", func(contentID int) string {
		return "Unable to clean up agent process"
	})
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
