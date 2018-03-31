package backup

import (
	"fmt"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/greenplum-db/gpbackup/utils"
	"github.com/pkg/errors"
)

/*
 * Functions to run commands on entire cluster during backup
 */

func CreateBackupDirectoriesOnAllHosts() {
	remoteOutput := globalCluster.GenerateAndExecuteCommand("Creating backup directories", func(contentID int) string {
		return fmt.Sprintf("mkdir -p %s", globalFPInfo.GetDirForContent(contentID))
	}, cluster.ON_SEGMENTS_AND_MASTER)
	globalCluster.CheckClusterError(remoteOutput, "Unable to create backup directories", func(contentID int) string {
		return fmt.Sprintf("Unable to create backup directory %s", globalFPInfo.GetDirForContent(contentID))
	})
}

func CreateSegmentPipesOnAllHostsForBackup(oid uint32) {
	remoteOutput := globalCluster.GenerateAndExecuteCommand("Creating segment data pipes", func(contentID int) string {
		pipeName := globalFPInfo.GetSegmentPipeFilePath(contentID)
		pipeName = fmt.Sprintf("%s_%d", pipeName, oid)
		return fmt.Sprintf("mkfifo %s", pipeName)
	}, cluster.ON_SEGMENTS)
	globalCluster.CheckClusterError(remoteOutput, "Unable to create segment data pipes", func(contentID int) string {
		return "Unable to create segment data pipe"
	})
}

func ReadFromSegmentPipes() {
	remoteOutput := globalCluster.GenerateAndExecuteCommand("Reading from segment data pipes", func(contentID int) string {
		tocFile := globalFPInfo.GetSegmentTOCFilePath(globalFPInfo.GetDirForContent(contentID), fmt.Sprintf("%d", contentID))
		oidFile := globalFPInfo.GetSegmentHelperFilePathForBackup(contentID, "oid")
		scriptFile := globalFPInfo.GetSegmentHelperFilePathForBackup(contentID, "script")
		pipeFile := globalFPInfo.GetSegmentPipeFilePath(contentID)
		backupFile := globalFPInfo.GetTableBackupFilePath(contentID, 0, true)
		gphomePath := operating.System.Getenv("GPHOME")
		compressLevel := *compressionLevel
		if !*noCompression && *compressionLevel == 0 {
			compressLevel = 1
		}
		pluginStr := ""
		if *pluginConfigFile != "" {
			pluginStr = fmt.Sprintf(" --plugin-config %s", *pluginConfigFile)
		}
		return fmt.Sprintf(`cat << HEREDOC > %s
#!/bin/bash
%s/bin/gpbackup_helper --backup-agent --toc-file %s --oid-file %s --pipe-file %s --data-file %s --compression-level %d --content %d%s

HEREDOC

chmod +x %s; (nohup %s > /dev/null 2>&1 &) &`, scriptFile, gphomePath, tocFile, oidFile, pipeFile, backupFile, compressLevel, contentID, pluginStr, scriptFile, scriptFile)
	}, cluster.ON_SEGMENTS)
	globalCluster.CheckClusterError(remoteOutput, "Unable to read from segment data pipes", func(contentID int) string {
		return "Unable to read from segment data pipe"
	})
}

func WriteOidListToSegments(oidList []string) {
	oidStr := strings.Join(oidList, "\n")
	remoteOutput := globalCluster.GenerateAndExecuteCommand("Writing filtered oid list to segments", func(contentID int) string {
		oidFile := globalFPInfo.GetSegmentHelperFilePathForBackup(contentID, "oid")
		return fmt.Sprintf(`echo "%s" > %s`, oidStr, oidFile)
	}, cluster.ON_SEGMENTS)
	globalCluster.CheckClusterError(remoteOutput, "Unable to write oid list to segments", func(contentID int) string {
		return fmt.Sprintf("Unable to write oid list for segment %d on host %s", contentID, globalCluster.GetHostForContent(contentID))
	})
}

func VerifyHelperVersionOnSegments(version string) {
	remoteOutput := globalCluster.GenerateAndExecuteCommand("Verifying gpbackup_helper version", func(contentID int) string {
		gphome := operating.System.Getenv("GPHOME")
		return fmt.Sprintf("%s/bin/gpbackup_helper --version", gphome)
	}, cluster.ON_HOSTS)
	globalCluster.CheckClusterError(remoteOutput, "Could not verify gpbackup_helper version", func(contentID int) string {
		return "Could not verify gpbackup_helper version"
	})

	numIncorrect := 0
	for contentID := range remoteOutput.Stdouts {
		segVersion := strings.TrimSpace(remoteOutput.Stdouts[contentID])
		segVersion = strings.Split(segVersion, " ")[1] // Format is "gpbackup_helper [version string]"
		if segVersion != version {
			gplog.Verbose("Version mismatch for gpbackup_helper on segment %d on host %s: Expected version %s, found version %s.", contentID, globalCluster.GetHostForContent(contentID), version, segVersion)
			numIncorrect++
		}
	}
	if numIncorrect > 0 {
		cluster.LogFatalClusterError("The version of gpbackup_helper must match the version of gprestore, but found gpbackup_helper binaries with invalid version", cluster.ON_HOSTS, numIncorrect)
	}
}

func CleanUpHelperFilesOnAllHosts() {
	remoteOutput := globalCluster.GenerateAndExecuteCommand("Removing oid list and helper script files from segment data directories", func(contentID int) string {
		errorFile := fmt.Sprintf("%s_error", globalFPInfo.GetSegmentPipeFilePath(contentID))
		oidFile := globalFPInfo.GetSegmentHelperFilePathForBackup(contentID, "oid")
		scriptFile := globalFPInfo.GetSegmentHelperFilePathForBackup(contentID, "script")
		return fmt.Sprintf("rm -f %s && rm -f %s && rm -f %s", errorFile, oidFile, scriptFile)
	}, cluster.ON_SEGMENTS)
	errMsg := fmt.Sprintf("Unable to remove segment helper file(s). See %s for a complete list of segments with errors and remove manually.",
		gplog.GetLogFilePath())
	globalCluster.CheckClusterError(remoteOutput, errMsg, func(contentID int) string {
		errorFile := fmt.Sprintf("%s_error", globalFPInfo.GetSegmentPipeFilePathWithPID(contentID))
		return fmt.Sprintf("Unable to remove helper file %s on segment %d on host %s", errorFile, contentID, globalCluster.GetHostForContent(contentID))
	}, true)
}

func CleanUpSegmentHelperProcesses() {
	remoteOutput := globalCluster.GenerateAndExecuteCommand("Cleaning up segment backup agent processes", func(contentID int) string {
		tocFile := globalFPInfo.GetSegmentTOCFilePath(globalFPInfo.GetDirForContent(contentID), fmt.Sprintf("%d", contentID))
		procPattern := fmt.Sprintf("gpbackup_helper --backup-agent --toc-file %s", tocFile)
		/*
		 * We try to avoid erroring out if no gpbackup_helper processes are found,
		 * as it's possible that all gpbackup_helper processes have finished by
		 * the time DoCleanup is called.
		 */
		return fmt.Sprintf("PIDS=`ps ux | grep \"%s\" | grep -v grep | awk '{print $2}'`; if [[ ! -z \"$PIDS\" ]]; then kill $PIDS; fi", procPattern)
	}, cluster.ON_SEGMENTS)
	globalCluster.CheckClusterError(remoteOutput, "Unable to clean up backup agent processes", func(contentID int) string {
		return "Unable to clean up backup agent process"
	})
}

func CheckAgentErrorsOnSegments() error {
	remoteOutput := globalCluster.GenerateAndExecuteCommand("Checking whether segment agents had errors during backup", func(contentID int) string {
		errorFile := fmt.Sprintf("%s_error", globalFPInfo.GetSegmentPipeFilePath(contentID))
		/*
		 * If an error file exists we want to indicate an error, as that means
		 * the agent errored out.  If no file exists, the agent was successful.
		 */
		return fmt.Sprintf("if [[ -f %s ]]; then echo 'error'; fi; rm -f %s", errorFile, errorFile)
	}, cluster.ON_SEGMENTS)

	numErrors := 0
	for contentID := range remoteOutput.Stdouts {
		if strings.TrimSpace(remoteOutput.Stdouts[contentID]) == "error" {
			gplog.Verbose("Error occurred with backup agent on segment %d on host %s.", contentID, globalCluster.GetHostForContent(contentID))
			numErrors++
		}
	}
	if numErrors > 0 {
		_, homeDir, _ := utils.GetUserAndHostInfo()
		helperLogName := fmt.Sprintf("%s/gpAdminLogs/gpbackup_helper_%s.log", homeDir, globalFPInfo.Timestamp[0:8])
		return errors.Errorf("Encountered errors with %d backup agent(s).  See %s for a complete list of segments with errors, and see %s on the corresponding hosts for detailed error messages.",
			numErrors, gplog.GetLogFilePath(), helperLogName)
	}
	return nil
}
