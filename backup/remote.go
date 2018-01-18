package backup

import (
	"fmt"

	"github.com/greenplum-db/gpbackup/utils"
)

/*
 * Functions to run commands on entire cluster during backup
 */

func CreateBackupDirectoriesOnAllHosts(cluster utils.Cluster) {
	remoteOutput := cluster.GenerateAndExecuteCommand("Creating backup directories", func(contentID int) string {
		return fmt.Sprintf("mkdir -p %s", cluster.GetDirForContent(contentID))
	}, true)
	cluster.CheckClusterError(remoteOutput, "Unable to create backup directories", func(contentID int) string {
		return fmt.Sprintf("Unable to create backup directory %s", cluster.GetDirForContent(contentID))
	})
}

func CreateSegmentPipesOnAllHostsForBackup(cluster utils.Cluster) {
	remoteOutput := cluster.GenerateAndExecuteCommand("Creating segment data pipes", func(contentID int) string {
		pipeName := cluster.GetSegmentPipeFilePath(contentID)
		return fmt.Sprintf("mkfifo %s", pipeName)
	})
	cluster.CheckClusterError(remoteOutput, "Unable to create segment data pipes", func(contentID int) string {
		return "Unable to create segment data pipe"
	})
}

func CleanUpSegmentPipesOnAllHosts(cluster utils.Cluster) {
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

func ReadFromSegmentPipes(cluster utils.Cluster) {
	remoteOutput := cluster.GenerateAndExecuteCommand("Reading from segment data pipes", func(contentID int) string {
		usingCompression, compressionProgram := utils.GetCompressionParameters()
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

func CleanUpSegmentTailProcesses(cluster utils.Cluster) {
	remoteOutput := cluster.GenerateAndExecuteCommand("Cleaning up segment tail processes", func(contentID int) string {
		filePattern := fmt.Sprintf("gpbackup_%d_%s", contentID, cluster.Timestamp) // Matches pipe name for backup and file name for restore
		return fmt.Sprintf(`ps ux | grep "tail -n +1 -f" | grep "%s" | grep -v "grep" | awk '{print $2}' | xargs kill -9`, filePattern)
	})
	cluster.CheckClusterError(remoteOutput, "Unable to clean up tail processes", func(contentID int) string {
		return "Unable to clean up tail process"
	})
}

func MoveSegmentTOCsAndMakeReadOnly(cluster utils.Cluster) {
	remoteOutput := cluster.GenerateAndExecuteCommand("Setting permissions on segment table of contents files and moving to backup directories", func(contentID int) string {
		tocFile := cluster.GetSegmentTOCFilePath(cluster.SegDirMap[contentID], fmt.Sprintf("%d", contentID))
		return fmt.Sprintf("chmod 444 %s; mv %s %s/.", tocFile, tocFile, cluster.GetDirForContent(contentID))
	})
	cluster.CheckClusterError(remoteOutput, "Unable to set permissions on or move segment table of contents files", func(contentID int) string {
		return fmt.Sprintf("Unable to set permissions on or move file %s", cluster.GetSegmentTOCFilePath(cluster.SegDirMap[contentID], fmt.Sprintf("%d", contentID)))
	})
}
