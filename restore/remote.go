package restore

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/greenplum-db/gpbackup/utils"
	"github.com/pkg/errors"
)

/*
 * Functions to run commands on entire cluster during restore
 */

func VerifyBackupDirectoriesExistOnAllHosts(cluster utils.Cluster) {
	remoteOutput := cluster.GenerateAndExecuteCommand("Verifying backup directories exist", func(contentID int) string {
		return fmt.Sprintf("test -d %s", cluster.GetDirForContent(contentID))
	}, true)
	cluster.CheckClusterError(remoteOutput, "Backup directories missing or inaccessible", func(contentID int) string {
		return fmt.Sprintf("Backup directory %s missing or inaccessible", cluster.GetDirForContent(contentID))
	})
}

func CreateSegmentPipesOnAllHostsForRestore(cluster utils.Cluster, oid uint32) {
	remoteOutput := cluster.GenerateAndExecuteCommand("Creating segment data pipes", func(contentID int) string {
		pipeName := cluster.GetSegmentPipeFilePathWithPID(contentID)
		pipeName = fmt.Sprintf("%s_%d", pipeName, oid)
		return fmt.Sprintf("mkfifo %s", pipeName)
	})
	cluster.CheckClusterError(remoteOutput, "Unable to create segment data pipes", func(contentID int) string {
		return "Unable to create segment data pipe"
	})
}

func WriteToSegmentPipes(cluster utils.Cluster) {
	remoteOutput := cluster.GenerateAndExecuteCommand("Writing to segment data pipes", func(contentID int) string {
		tocFile := cluster.GetSegmentTOCFilePathWithPID(cluster.SegDirMap[contentID], fmt.Sprintf("%d", contentID))
		oidFile := cluster.GetSegmentHelperFilePath(contentID, "oid")
		scriptFile := cluster.GetSegmentHelperFilePath(contentID, "script")
		pipeFile := cluster.GetSegmentPipeFilePathWithPID(contentID)
		backupFile := cluster.GetTableBackupFilePath(contentID, 0, true)
		gphomePath := utils.System.Getenv("GPHOME")
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

func WriteOidListToSegments(cluster utils.Cluster, filteredEntries []utils.MasterDataEntry) {
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

func CleanUpHelperFilesOnAllHosts(cluster utils.Cluster) {
	remoteOutput := cluster.GenerateAndExecuteCommand("Removing oid list and helper script files from segment data directories", func(contentID int) string {
		oidFile := cluster.GetSegmentHelperFilePath(contentID, "oid")
		scriptFile := cluster.GetSegmentHelperFilePath(contentID, "script")
		return fmt.Sprintf("rm -f %s && rm -f %s", oidFile, scriptFile)
	})
	errMsg := fmt.Sprintf("Unable to remove segment helper file(s). See %s for a complete list of segments with errors and remove manually.",
		logger.GetLogFilePath())
	cluster.CheckClusterError(remoteOutput, errMsg, func(contentID int) string {
		tocFile := cluster.GetSegmentTOCFilePathWithPID(cluster.SegDirMap[contentID], fmt.Sprintf("%d", contentID))
		return fmt.Sprintf("Unable to remove helper file %s on segment %d on host %s", tocFile, contentID, cluster.GetHostForContent(contentID))
	}, true)
}

func CopySegmentTOCs(cluster utils.Cluster) {
	remoteOutput := cluster.GenerateAndExecuteCommand("Copying segment table of contents files from backup directories", func(contentID int) string {
		tocFile := cluster.GetSegmentTOCFilePathWithPID(cluster.SegDirMap[contentID], fmt.Sprintf("%d", contentID))
		tocFilename := fmt.Sprintf("gpbackup_%d_%s_toc.yaml", contentID, cluster.Timestamp)
		return fmt.Sprintf("cp -f %s/%s %s", cluster.GetDirForContent(contentID), tocFilename, tocFile)
	})
	cluster.CheckClusterError(remoteOutput, "Unable to copy segment table of contents files from backup directories", func(contentID int) string {
		return fmt.Sprintf("Unable to copy segment table of contents file to %s", cluster.GetSegmentTOCFilePathWithPID(cluster.SegDirMap[contentID], fmt.Sprintf("%d", contentID)))
	})
}

func VerifyBackupFileCountOnSegments(cluster utils.Cluster, fileCount int) {
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
		utils.LogFatalClusterError("Found incorrect number of backup files", numIncorrect)
	}
}

func VerifyHelperVersionOnSegments(cluster utils.Cluster, version string) {
	remoteOutput := cluster.GenerateAndExecuteCommand("Verifying gpbackup_helper version", func(contentID int) string {
		gphome := utils.System.Getenv("GPHOME")
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
		utils.LogFatalClusterError("The version of gpbackup_helper must match the version of gprestore, but found gpbackup_helper binaries with invalid version", numIncorrect)
	}
}

func CleanUpSegmentHelperProcesses(cluster utils.Cluster) {
	remoteOutput := cluster.GenerateAndExecuteCommand("Cleaning up segment restore agent processes", func(contentID int) string {
		tocFile := cluster.GetSegmentTOCFilePathWithPID(cluster.SegDirMap[contentID], fmt.Sprintf("%d", contentID))
		procPattern := fmt.Sprintf("gpbackup_helper --restore-agent --toc-file %s", tocFile)
		/*
		 * We try to avoid erroring out if no gpbackup_helper processes are found,
		 * as it's possible that all gpbackup_helper processes have finished by
		 * the time DoCleanup is called.
		 */
		return fmt.Sprintf("PIDS=`ps ux | grep \"%s\" | grep \"%s\" | grep -v grep | awk '{print $2}'`; if [[ ! -z \"$PIDS\" ]]; then kill -9 $PIDS; fi", procPattern)
	})
	cluster.CheckClusterError(remoteOutput, "Unable to clean up restore agent processes", func(contentID int) string {
		return "Unable to clean up restore agent process"
	})
}

func CleanUpSegmentTOCs(cluster utils.Cluster) {
	remoteOutput := cluster.GenerateAndExecuteCommand("Removing segment table of contents files from segment data directories", func(contentID int) string {
		tocFile := cluster.GetSegmentTOCFilePathWithPID(cluster.SegDirMap[contentID], fmt.Sprintf("%d", contentID))
		return fmt.Sprintf("rm -f %s", tocFile)
	})
	errMsg := fmt.Sprintf("Unable to remove segment table of contents file(s). See %s for a complete list of segments with errors and remove manually.",
		logger.GetLogFilePath())
	cluster.CheckClusterError(remoteOutput, errMsg, func(contentID int) string {
		tocFile := cluster.GetSegmentTOCFilePathWithPID(cluster.SegDirMap[contentID], fmt.Sprintf("%d", contentID))
		return fmt.Sprintf("Unable to remove table of contents file %s on segment %d on host %s", tocFile, contentID, cluster.GetHostForContent(contentID))
	}, true)
}

func VerifyMetadataFilePaths(cluster utils.Cluster, withStats bool) {
	filetypes := []string{"config", "table of contents", "metadata"}
	missing := false
	for _, filetype := range filetypes {
		filepath := cluster.GetBackupFilePath(filetype)
		if !utils.FileExistsAndIsReadable(filepath) {
			missing = true
			logger.Error("Cannot access %s file %s", filetype, filepath)
		}
	}
	if withStats {
		filepath := cluster.GetStatisticsFilePath()
		if !utils.FileExistsAndIsReadable(filepath) {
			missing = true
			logger.Error("Cannot access statistics file %s", filepath)
			logger.Error(`Note that the "-with-stats" flag must be passed to gpbackup to generate a statistics file.`)
		}
	}
	if missing {
		logger.Fatal(errors.Errorf("One or more metadata files do not exist or are not readable."), "Cannot proceed with restore")
	}
}
