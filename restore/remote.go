package restore

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/greenplum-db/gpbackup/utils"
	"github.com/pkg/errors"
)

/*
 * Functions to run commands on entire cluster during restore
 */

func VerifyBackupDirectoriesExistOnAllHosts() {
	remoteOutput := globalCluster.GenerateAndExecuteCommand("Verifying backup directories exist", func(contentID int) string {
		return fmt.Sprintf("test -d %s", globalFPInfo.GetDirForContent(contentID))
	}, true)
	globalCluster.CheckClusterError(remoteOutput, "Backup directories missing or inaccessible", func(contentID int) string {
		return fmt.Sprintf("Backup directory %s missing or inaccessible", globalFPInfo.GetDirForContent(contentID))
	})
}

func CreateSegmentPipesOnAllHostsForRestore(oid uint32) {
	remoteOutput := globalCluster.GenerateAndExecuteCommand("Creating segment data pipes", func(contentID int) string {
		pipeName := globalFPInfo.GetSegmentPipeFilePathWithPID(contentID)
		pipeName = fmt.Sprintf("%s_%d", pipeName, oid)
		return fmt.Sprintf("mkfifo %s", pipeName)
	})
	globalCluster.CheckClusterError(remoteOutput, "Unable to create segment data pipes", func(contentID int) string {
		return "Unable to create segment data pipe"
	})
}

func WriteToSegmentPipes() {
	remoteOutput := globalCluster.GenerateAndExecuteCommand("Writing to segment data pipes", func(contentID int) string {
		tocFile := globalFPInfo.GetSegmentTOCFilePathWithPID(globalFPInfo.BackupDirMap[contentID], fmt.Sprintf("%d", contentID))
		oidFile := globalFPInfo.GetSegmentHelperFilePath(contentID, "oid")
		scriptFile := globalFPInfo.GetSegmentHelperFilePath(contentID, "script")
		pipeFile := globalFPInfo.GetSegmentPipeFilePathWithPID(contentID)
		backupFile := globalFPInfo.GetTableBackupFilePath(contentID, 0, true)
		gphomePath := operating.System.Getenv("GPHOME")
		return fmt.Sprintf(`cat << HEREDOC > %s
#!/bin/bash
%s/bin/gpbackup_helper --restore-agent --toc-file %s --oid-file %s --pipe-file %s --data-file %s --content %d
HEREDOC

chmod +x %s; (nohup %s > /dev/null 2>&1 &) &`, scriptFile, gphomePath, tocFile, oidFile, pipeFile, backupFile, contentID, scriptFile, scriptFile)
	})
	globalCluster.CheckClusterError(remoteOutput, "Unable to write to segment data pipes", func(contentID int) string {
		return fmt.Sprintf("Unable to write to data pipe for segment %d on host %s", contentID, globalCluster.GetHostForContent(contentID))
	})
}

func WriteOidListToSegments(filteredEntries []utils.MasterDataEntry) {
	filteredOids := make([]string, len(filteredEntries))
	for i, entry := range filteredEntries {
		filteredOids[i] = fmt.Sprintf("%d", entry.Oid)
	}
	oidStr := strings.Join(filteredOids, "\n")
	remoteOutput := globalCluster.GenerateAndExecuteCommand("Writing filtered oid list to segments", func(contentID int) string {
		oidFile := globalFPInfo.GetSegmentHelperFilePath(contentID, "oid")
		return fmt.Sprintf(`echo "%s" > %s`, oidStr, oidFile)
	})
	globalCluster.CheckClusterError(remoteOutput, "Unable to write oid list to segments", func(contentID int) string {
		return fmt.Sprintf("Unable to write oid list for segment %d on host %s", contentID, globalCluster.GetHostForContent(contentID))
	})
}

func CleanUpHelperFilesOnAllHosts() {
	remoteOutput := globalCluster.GenerateAndExecuteCommand("Removing oid list and helper script files from segment data directories", func(contentID int) string {
		errorFile := fmt.Sprintf("%s_error", globalFPInfo.GetSegmentPipeFilePathWithPID(contentID))
		oidFile := globalFPInfo.GetSegmentHelperFilePath(contentID, "oid")
		scriptFile := globalFPInfo.GetSegmentHelperFilePath(contentID, "script")
		return fmt.Sprintf("rm -f %s && rm -f %s && rm -f %s", errorFile, oidFile, scriptFile)
	})
	errMsg := fmt.Sprintf("Unable to remove segment helper file(s). See %s for a complete list of segments with errors and remove manually.",
		gplog.GetLogFilePath())
	globalCluster.CheckClusterError(remoteOutput, errMsg, func(contentID int) string {
		tocFile := globalFPInfo.GetSegmentTOCFilePathWithPID(globalFPInfo.BackupDirMap[contentID], fmt.Sprintf("%d", contentID))
		return fmt.Sprintf("Unable to remove helper file %s on segment %d on host %s", tocFile, contentID, globalCluster.GetHostForContent(contentID))
	}, true)
}

func CopySegmentTOCs() {
	remoteOutput := globalCluster.GenerateAndExecuteCommand("Copying segment table of contents files from backup directories", func(contentID int) string {
		tocFile := globalFPInfo.GetSegmentTOCFilePathWithPID(globalFPInfo.BackupDirMap[contentID], fmt.Sprintf("%d", contentID))
		tocFilename := fmt.Sprintf("gpbackup_%d_%s_toc.yaml", contentID, globalFPInfo.Timestamp)
		return fmt.Sprintf("cp -f %s/%s %s", globalFPInfo.GetDirForContent(contentID), tocFilename, tocFile)
	})
	globalCluster.CheckClusterError(remoteOutput, "Unable to copy segment table of contents files from backup directories", func(contentID int) string {
		return fmt.Sprintf("Unable to copy segment table of contents file to %s", globalFPInfo.GetSegmentTOCFilePathWithPID(globalFPInfo.BackupDirMap[contentID], fmt.Sprintf("%d", contentID)))
	})
}

func VerifyBackupFileCountOnSegments(fileCount int) {
	remoteOutput := globalCluster.GenerateAndExecuteCommand("Verifying backup file count", func(contentID int) string {
		return fmt.Sprintf("find %s -type f | wc -l", globalFPInfo.GetDirForContent(contentID))
	})
	globalCluster.CheckClusterError(remoteOutput, "Could not verify backup file count", func(contentID int) string {
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
			gplog.Verbose("Expected to find %d file%s on segment %d on host %s, but found %d instead.", fileCount, s, contentID, globalCluster.GetHostForContent(contentID), numFound)
			numIncorrect++
		}
	}
	if numIncorrect > 0 {
		cluster.LogFatalClusterError("Found incorrect number of backup files", numIncorrect)
	}
}

func VerifyHelperVersionOnSegments(version string) {
	remoteOutput := globalCluster.GenerateAndExecuteCommand("Verifying gpbackup_helper version", func(contentID int) string {
		gphome := operating.System.Getenv("GPHOME")
		return fmt.Sprintf("%s/bin/gpbackup_helper --version", gphome)
	})
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
		cluster.LogFatalClusterError("The version of gpbackup_helper must match the version of gprestore, but found gpbackup_helper binaries with invalid version", numIncorrect)
	}
}

func CleanUpSegmentHelperProcesses() {
	remoteOutput := globalCluster.GenerateAndExecuteCommand("Cleaning up segment restore agent processes", func(contentID int) string {
		tocFile := globalFPInfo.GetSegmentTOCFilePathWithPID(globalFPInfo.BackupDirMap[contentID], fmt.Sprintf("%d", contentID))
		procPattern := fmt.Sprintf("gpbackup_helper --restore-agent --toc-file %s", tocFile)
		/*
		 * We try to avoid erroring out if no gpbackup_helper processes are found,
		 * as it's possible that all gpbackup_helper processes have finished by
		 * the time DoCleanup is called.
		 */
		return fmt.Sprintf("PIDS=`ps ux | grep \"%s\" | grep -v grep | awk '{print $2}'`; if [[ ! -z \"$PIDS\" ]]; then kill $PIDS; fi", procPattern)
	})
	globalCluster.CheckClusterError(remoteOutput, "Unable to clean up restore agent processes", func(contentID int) string {
		return "Unable to clean up restore agent process"
	})
}

func CleanUpSegmentTOCs() {
	remoteOutput := globalCluster.GenerateAndExecuteCommand("Removing segment table of contents files from segment data directories", func(contentID int) string {
		tocFile := globalFPInfo.GetSegmentTOCFilePathWithPID(globalFPInfo.BackupDirMap[contentID], fmt.Sprintf("%d", contentID))
		return fmt.Sprintf("rm -f %s", tocFile)
	})
	errMsg := fmt.Sprintf("Unable to remove segment table of contents file(s). See %s for a complete list of segments with errors and remove manually.",
		gplog.GetLogFilePath())
	globalCluster.CheckClusterError(remoteOutput, errMsg, func(contentID int) string {
		tocFile := globalFPInfo.GetSegmentTOCFilePathWithPID(globalFPInfo.BackupDirMap[contentID], fmt.Sprintf("%d", contentID))
		return fmt.Sprintf("Unable to remove table of contents file %s on segment %d on host %s", tocFile, contentID, globalCluster.GetHostForContent(contentID))
	}, true)
}

func VerifyMetadataFilePaths(withStats bool) {
	filetypes := []string{"config", "table of contents", "metadata"}
	missing := false
	for _, filetype := range filetypes {
		filepath := globalFPInfo.GetBackupFilePath(filetype)
		if !utils.FileExistsAndIsReadable(filepath) {
			missing = true
			gplog.Error("Cannot access %s file %s", filetype, filepath)
		}
	}
	if withStats {
		filepath := globalFPInfo.GetStatisticsFilePath()
		if !utils.FileExistsAndIsReadable(filepath) {
			missing = true
			gplog.Error("Cannot access statistics file %s", filepath)
			gplog.Error(`Note that the "-with-stats" flag must be passed to gpbackup to generate a statistics file.`)
		}
	}
	if missing {
		gplog.Fatal(errors.Errorf("One or more metadata files do not exist or are not readable."), "Cannot proceed with restore")
	}
}

func CheckAgentErrorsOnSegments() error {
	remoteOutput := globalCluster.GenerateAndExecuteCommand("Checking whether segment agents had errors during restore", func(contentID int) string {
		errorFile := fmt.Sprintf("%s_error", globalFPInfo.GetSegmentPipeFilePathWithPID(contentID))
		/*
		 * If an error file exists we want to indicate an error, as that means
		 * the agent errored out.  If no file exists, the agent was successful.
		 */
		return fmt.Sprintf("if [[ -f %s ]]; then echo 'error'; fi; rm -f %s", errorFile, errorFile)
	})

	numErrors := 0
	for contentID := range remoteOutput.Stdouts {
		if strings.TrimSpace(remoteOutput.Stdouts[contentID]) == "error" {
			gplog.Verbose("Error occurred with restore agent on segment %d on host %s.", contentID, globalCluster.GetHostForContent(contentID))
			numErrors++
		}
	}
	if numErrors > 0 {
		_, homeDir, _ := utils.GetUserAndHostInfo()
		helperLogName := fmt.Sprintf("%s/gpAdminLogs/gpbackup_helper_%s.log", homeDir, globalFPInfo.Timestamp[0:8])
		return errors.Errorf("Encountered errors with %d restore agent(s).  See %s for a complete list of segments with errors, and see %s on the corresponding hosts for detailed error messages.",
			numErrors, gplog.GetLogFilePath(), helperLogName)
	}
	return nil
}
