package restore

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gp-common-go-libs/iohelper"
	"github.com/greenplum-db/gpbackup/options"
	"github.com/pkg/errors"
)

/*
 * Functions to run commands on entire cluster during restore
 */

func VerifyBackupDirectoriesExistOnAllHosts() {
	_, err := globalCluster.ExecuteLocalCommand(fmt.Sprintf("test -d %s", globalFPInfo.GetDirForContent(-1)))
	gplog.FatalOnError(err, "Backup directory %s missing or inaccessible", globalFPInfo.GetDirForContent(-1))
	if MustGetFlagString(options.PLUGIN_CONFIG) == "" || backupConfig.SingleDataFile {
		origSize, destSize, isResizeRestore := GetResizeClusterInfo()

		remoteOutput := globalCluster.GenerateAndExecuteCommand("Verifying backup directories exist", cluster.ON_SEGMENTS, func(contentID int) string {
			if isResizeRestore { // Map origin content to destination content to find where the original files have been placed
				if contentID >= origSize { // Don't check for directories for contents that aren't part of the backup set
					return ""
				}
				contentID = contentID % destSize
			}
			return fmt.Sprintf("test -d %s", globalFPInfo.GetDirForContent(contentID))
		})
		globalCluster.CheckClusterError(remoteOutput, "Backup directories missing or inaccessible", func(contentID int) string {
			return fmt.Sprintf("Backup directory %s missing or inaccessible", globalFPInfo.GetDirForContent(contentID))
		})
	}
}

func VerifyBackupFileCountOnSegments() {
	// In the current backup directory format, all content IDs are intermingled in one directory, so we need to get a list of which contents
	// correspond to the content ID we're going to check in order to provide a useful count to the user in the case of an error.
	origSize, destSize, isResizeRestore := GetResizeClusterInfo()
	contentMap := make(map[int][]string, destSize) // []string instead of []int so we can join them later
	for i := 0; i < origSize; i++ {
		contentMap[i%destSize] = append(contentMap[i%destSize], fmt.Sprintf("%d", i))
	}

	remoteOutput := globalCluster.GenerateAndExecuteCommand("Verifying backup file count", cluster.ON_SEGMENTS, func(contentID int) string {
		// Coordinator backup files (and any gprestore report files) will be mixed in with segment backup files on a single-node cluster,
		// so we explicitly look for filenames in the segment filename format.  In a smaller-to-larger restore, the contents list for a segment
		// outside the destination array will be "[]", which the find command can handle safely in this context.
		contentsList := fmt.Sprintf("(%s)", strings.Join(contentMap[contentID], "|"))
		cmdString := fmt.Sprintf(`find %s -type f -regextype posix-extended -regex ".*gpbackup_%s_%s.*" | wc -l`, globalFPInfo.GetDirForContent(contentID), contentsList, globalFPInfo.Timestamp)
		return cmdString
	})
	globalCluster.CheckClusterError(remoteOutput, "Could not verify backup file count", func(contentID int) string {
		return "Could not verify backup file count"
	})

	// these are the file counts for non-resize restores.
	fileCount := 2 // 1 for the actual data file, 1 for the segment TOC file
	if !backupConfig.SingleDataFile {
		fileCount = len(globalTOC.DataEntries)
	}

	batchMap := make(map[int]int, len(remoteOutput.Commands))
	for i := 0; i < origSize; i++ {
		batchMap[i%destSize] += fileCount
	}

	numIncorrect := 0
	for contentID, cmd := range remoteOutput.Commands {
		numFound, _ := strconv.Atoi(strings.TrimSpace(cmd.Stdout))
		if isResizeRestore {
			fileCount = batchMap[contentID]
		}
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

func GetResizeClusterInfo() (int, int, bool) {
	isResizeCluster := MustGetFlagBool(options.RESIZE_CLUSTER)
	origSize := backupConfig.SegmentCount
	destSize := len(globalCluster.ContentIDs) - 1
	if !isResizeCluster && origSize == 0 { // Backup taken with version <1.26, no SegmentCount stored
		origSize = destSize
	}
	return origSize, destSize, isResizeCluster
}
