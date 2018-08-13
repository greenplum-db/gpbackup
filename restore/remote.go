package restore

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gp-common-go-libs/iohelper"
	"github.com/greenplum-db/gpbackup/utils"
	"github.com/pkg/errors"
)

/*
 * Functions to run commands on entire cluster during restore
 */

func VerifyBackupDirectoriesExistOnAllHosts() {
	_, err := globalCluster.ExecuteLocalCommand(fmt.Sprintf("test -d %s", globalFPInfo.GetDirForContent(-1)))
	gplog.FatalOnError(err, "Backup directory %s missing or inaccessible", globalFPInfo.GetDirForContent(-1))
	if MustGetFlagString(utils.PLUGIN_CONFIG) == "" || backupConfig.SingleDataFile {
		remoteOutput := globalCluster.GenerateAndExecuteCommand("Verifying backup directories exist", func(contentID int) string {
			return fmt.Sprintf("test -d %s", globalFPInfo.GetDirForContent(contentID))
		}, cluster.ON_SEGMENTS)
		globalCluster.CheckClusterError(remoteOutput, "Backup directories missing or inaccessible", func(contentID int) string {
			return fmt.Sprintf("Backup directory %s missing or inaccessible", globalFPInfo.GetDirForContent(contentID))
		})
	}
}

func VerifyBackupFileCountOnSegments(fileCount int) {
	remoteOutput := globalCluster.GenerateAndExecuteCommand("Verifying backup file count", func(contentID int) string {
		return fmt.Sprintf("find %s -type f | wc -l", globalFPInfo.GetDirForContent(contentID))
	}, cluster.ON_SEGMENTS)
	globalCluster.CheckClusterError(remoteOutput, "Could not verify backup file count", func(contentID int) string {
		return "Could not verify backup file count"
	})

	numIncorrect := 0
	for contentID := range remoteOutput.Stdouts {
		numFound, _ := strconv.Atoi(strings.TrimSpace(remoteOutput.Stdouts[contentID]))
		if numFound != fileCount {
			gplog.Verbose("Expected to find %d file(s) on segment %d on host %s, but found %d instead.", fileCount, contentID, globalCluster.GetHostForContent(contentID), numFound)
			numIncorrect++
		}
	}
	if numIncorrect > 0 {
		cluster.LogFatalClusterError("Found incorrect number of backup files", cluster.ON_SEGMENTS, numIncorrect)
	}
}

func VerifyMetadataFilePaths(withStats bool) {
	filetypes := []string{"config", "table of contents", "metadata"}
	missing := false
	for _, filetype := range filetypes {
		filepath := globalFPInfo.GetBackupFilePath(filetype)
		if !iohelper.FileExistsAndIsReadable(filepath) {
			missing = true
			gplog.Error("Cannot access %s file %s", filetype, filepath)
		}
	}
	if withStats {
		filepath := globalFPInfo.GetStatisticsFilePath()
		if !iohelper.FileExistsAndIsReadable(filepath) {
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
			gplog.Verbose("Error occurred with restore agent on segment %d on host %s.", contentID, globalCluster.GetHostForContent(contentID))
			numErrors++
		}
	}
	if numErrors > 0 {
		helperLogName := globalFPInfo.GetHelperLogPath()
		return errors.Errorf("Encountered errors with %d restore agent(s).  See %s for a complete list of segments with errors, and see %s on the corresponding hosts for detailed error messages.",
			numErrors, gplog.GetLogFilePath(), helperLogName)
	}
	return nil
}
