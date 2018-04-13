package utils

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gp-common-go-libs/operating"
)

/*
 * Functions to run commands on entire cluster during both backup and restore
 */

func CreateFirstSegmentPipeOnAllHosts(oid uint32, c cluster.Cluster, fpInfo FilePathInfo) {
	remoteOutput := c.GenerateAndExecuteCommand("Creating segment data pipes", func(contentID int) string {
		pipeName := fpInfo.GetSegmentPipeFilePath(contentID)
		pipeName = fmt.Sprintf("%s_%d", pipeName, oid)
		return fmt.Sprintf("mkfifo %s", pipeName)
	}, cluster.ON_SEGMENTS)
	c.CheckClusterError(remoteOutput, "Unable to create segment data pipes", func(contentID int) string {
		return "Unable to create segment data pipe"
	})
}

func WriteOidListToSegments(oidList []string, c cluster.Cluster, fpInfo FilePathInfo) {
	oidStr := strings.Join(oidList, "\n")
	remoteOutput := c.GenerateAndExecuteCommand("Writing filtered oid list to segments", func(contentID int) string {
		oidFile := fpInfo.GetSegmentHelperFilePath(contentID, "oid")
		return fmt.Sprintf(`echo "%s" > %s`, oidStr, oidFile)
	}, cluster.ON_SEGMENTS)
	c.CheckClusterError(remoteOutput, "Unable to write oid list to segments", func(contentID int) string {
		return fmt.Sprintf("Unable to write oid list for segment %d on host %s", contentID, c.GetHostForContent(contentID))
	})
}

func VerifyHelperVersionOnSegments(version string, c cluster.Cluster) {
	remoteOutput := c.GenerateAndExecuteCommand("Verifying gpbackup_helper version", func(contentID int) string {
		gphome := operating.System.Getenv("GPHOME")
		return fmt.Sprintf("%s/bin/gpbackup_helper --version", gphome)
	}, cluster.ON_HOSTS)
	c.CheckClusterError(remoteOutput, "Could not verify gpbackup_helper version", func(contentID int) string {
		return "Could not verify gpbackup_helper version"
	})

	numIncorrect := 0
	for contentID := range remoteOutput.Stdouts {
		segVersion := strings.TrimSpace(remoteOutput.Stdouts[contentID])
		segVersion = strings.Split(segVersion, " ")[1] // Format is "gpbackup_helper [version string]"
		if segVersion != version {
			gplog.Verbose("Version mismatch for gpbackup_helper on segment %d on host %s: Expected version %s, found version %s.", contentID, c.GetHostForContent(contentID), version, segVersion)
			numIncorrect++
		}
	}
	if numIncorrect > 0 {
		cluster.LogFatalClusterError("The version of gpbackup_helper must match the version of gprestore, but found gpbackup_helper binaries with invalid version", cluster.ON_HOSTS, numIncorrect)
	}
}

func StartAgent(c cluster.Cluster, fpInfo FilePathInfo, operation string, pluginConfigFile string, compressStr string) {
	remoteOutput := c.GenerateAndExecuteCommand("Starting gpbackup_helper agent", func(contentID int) string {
		tocFile := fpInfo.GetSegmentTOCFilePath(contentID)
		oidFile := fpInfo.GetSegmentHelperFilePath(contentID, "oid")
		scriptFile := fpInfo.GetSegmentHelperFilePath(contentID, "script")
		pipeFile := fpInfo.GetSegmentPipeFilePath(contentID)
		backupFile := fpInfo.GetTableBackupFilePath(contentID, 0, true)
		gphomePath := operating.System.Getenv("GPHOME")
		pluginStr := ""
		if pluginConfigFile != "" {
			_, configFilename := filepath.Split(pluginConfigFile)
			pluginStr = fmt.Sprintf(" --plugin-config /tmp/%s", configFilename)
		}
		helperCmdStr := fmt.Sprintf("gpbackup_helper %s --toc-file %s --oid-file %s --pipe-file %s --data-file %s --content %d%s%s", operation, tocFile, oidFile, pipeFile, backupFile, contentID, pluginStr, compressStr)
		return fmt.Sprintf(`cat << HEREDOC > %s
#!/bin/bash
%s/bin/%s

HEREDOC

chmod +x %s; (nohup %s > /dev/null 2>&1 &) &`, scriptFile, gphomePath, helperCmdStr, scriptFile, scriptFile)
	}, cluster.ON_SEGMENTS)
	c.CheckClusterError(remoteOutput, "Error starting gpbackup_helper agent", func(contentID int) string {
		return "Error starting gpbackup_helper agent"
	})
}

func CleanUpHelperFilesOnAllHosts(c cluster.Cluster, fpInfo FilePathInfo) {
	remoteOutput := c.GenerateAndExecuteCommand("Removing oid list and helper script files from segment data directories", func(contentID int) string {
		errorFile := fmt.Sprintf("%s_error", fpInfo.GetSegmentPipeFilePath(contentID))
		oidFile := fpInfo.GetSegmentHelperFilePath(contentID, "oid")
		scriptFile := fpInfo.GetSegmentHelperFilePath(contentID, "script")
		return fmt.Sprintf("rm -f %s && rm -f %s && rm -f %s", errorFile, oidFile, scriptFile)
	}, cluster.ON_SEGMENTS)
	errMsg := fmt.Sprintf("Unable to remove segment helper file(s). See %s for a complete list of segments with errors and remove manually.",
		gplog.GetLogFilePath())
	c.CheckClusterError(remoteOutput, errMsg, func(contentID int) string {
		errorFile := fmt.Sprintf("%s_error", fpInfo.GetSegmentPipeFilePath(contentID))
		return fmt.Sprintf("Unable to remove helper file %s on segment %d on host %s", errorFile, contentID, c.GetHostForContent(contentID))
	}, true)
}

func CleanUpSegmentHelperProcesses(c cluster.Cluster, fpInfo FilePathInfo, operation string) {
	remoteOutput := c.GenerateAndExecuteCommand("Cleaning up segment agent processes", func(contentID int) string {
		tocFile := fpInfo.GetSegmentTOCFilePath(contentID)
		procPattern := fmt.Sprintf("gpbackup_helper --%s-agent --toc-file %s", operation, tocFile)
		/*
		 * We try to avoid erroring out if no gpbackup_helper processes are found,
		 * as it's possible that all gpbackup_helper processes have finished by
		 * the time DoCleanup is called.
		 */
		return fmt.Sprintf("PIDS=`ps ux | grep \"%s\" | grep -v grep | awk '{print $2}'`; if [[ ! -z \"$PIDS\" ]]; then kill $PIDS; fi", procPattern)
	}, cluster.ON_SEGMENTS)
	c.CheckClusterError(remoteOutput, "Unable to clean up agent processes", func(contentID int) string {
		return "Unable to clean up agent process"
	})
}
