package helper

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/sys/unix"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/greenplum-db/gpbackup/utils"
)

/*
 * Non-flag variables
 */

var (
	CleanupGroup  *sync.WaitGroup
	errBuf        bytes.Buffer
	version       string
	wasTerminated bool
	writeHandle   *os.File
	writer        *bufio.Writer
	pipesMap      map[string]bool
)

/*
 * Command-line flags
 */
var (
	backupAgent      *bool
	compressionLevel *int
	compressionType  *string
	content          *int
	dataFile         *string
	oidFile          *string
	onErrorContinue  *bool
	pipeFile         *string
	pluginConfigFile *string
	printVersion     *bool
	restoreAgent     *bool
	tocFile          *string
	isFiltered       *bool
	copyQueue        *int
	singleDataFile   *bool
	isResizeRestore  *bool
	origSize         *int
	destSize         *int
	replicationFile  *string
)

func DoHelper() {
	var err error
	defer func() {
		if wasTerminated {
			CleanupGroup.Wait()
			return
		}
		DoCleanup()
		os.Exit(gplog.GetErrorCode())
	}()

	InitializeGlobals()
	go InitializeSignalHandler()

	if *backupAgent {
		err = doBackupAgent()
	} else if *restoreAgent {
		err = doRestoreAgent()
	}
	if err != nil {
		// error logging handled in doBackupAgent and doRestoreAgent
		handle, _ := utils.OpenFileForWrite(fmt.Sprintf("%s_error", *pipeFile))
		_ = handle.Close()
	}
}

func InitializeGlobals() {
	CleanupGroup = &sync.WaitGroup{}
	CleanupGroup.Add(1)
	gplog.InitializeLogging("gpbackup_helper", "")

	backupAgent = flag.Bool("backup-agent", false, "Use gpbackup_helper as an agent for backup")
	content = flag.Int("content", -2, "Content ID of the corresponding segment")
	compressionLevel = flag.Int("compression-level", 0, "The level of compression. O indicates no compression. Range of valid values depends on compression type")
	compressionType = flag.String("compression-type", "gzip", "The type of compression. Valid values are 'gzip' and 'zstd'")
	dataFile = flag.String("data-file", "", "Absolute path to the data file")
	oidFile = flag.String("oid-file", "", "Absolute path to the file containing a list of oids to restore")
	onErrorContinue = flag.Bool("on-error-continue", false, "Continue restore even when encountering an error")
	pipeFile = flag.String("pipe-file", "", "Absolute path to the pipe file")
	pluginConfigFile = flag.String("plugin-config", "", "The configuration file to use for a plugin")
	printVersion = flag.Bool("version", false, "Print version number and exit")
	restoreAgent = flag.Bool("restore-agent", false, "Use gpbackup_helper as an agent for restore")
	tocFile = flag.String("toc-file", "", "Absolute path to the table of contents file")
	isFiltered = flag.Bool("with-filters", false, "Used with table/schema filters")
	copyQueue = flag.Int("copy-queue-size", 1, "Used to know how many COPIES are being queued up")
	singleDataFile = flag.Bool("single-data-file", false, "Used with single data file restore.")
	isResizeRestore = flag.Bool("resize-cluster", false, "Used with resize cluster restore.")
	origSize = flag.Int("orig-seg-count", 0, "Used with resize restore.  Gives the segment count of the backup.")
	destSize = flag.Int("dest-seg-count", 0, "Used with resize restore.  Gives the segment count of the current cluster.")
	replicationFile = flag.String("replication-file", "", "Used with resize restore.  Gives the list of replicated tables.")

	if *onErrorContinue && !*restoreAgent {
		fmt.Printf("--on-error-continue flag can only be used with --restore-agent flag")
		os.Exit(1)
	}
	if (*origSize > 0 && *destSize == 0) || (*destSize > 0 && *origSize == 0) {
		fmt.Printf("Both --orig-seg-count and --dest-seg-count must be used during a resize restore")
		os.Exit(1)
	}
	flag.Parse()
	if *printVersion {
		fmt.Printf("gpbackup_helper version %s\n", version)
		os.Exit(0)
	}
	operating.InitializeSystemFunctions()

	pipesMap = make(map[string]bool, 0)
}

func InitializeSignalHandler() {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, unix.SIGINT, unix.SIGTERM, unix.SIGPIPE, unix.SIGUSR1)
	terminatedChan := make(chan bool, 1)
	for {
		go func() {
			sig := <-signalChan
			fmt.Println() // Add newline after "^C" is printed
			switch sig {
			case unix.SIGINT:
				gplog.Warn("Received an interrupt signal on segment %d: aborting", *content)
				terminatedChan <- true
			case unix.SIGTERM:
				gplog.Warn("Received a termination signal on segment %d: aborting", *content)
				terminatedChan <- true
			case unix.SIGPIPE:
				if *onErrorContinue {
					gplog.Warn("Received a broken pipe signal on segment %d: on-error-continue set, continuing", *content)
					terminatedChan <- false
				} else {
					gplog.Warn("Received a broken pipe signal on segment %d: aborting", *content)
					terminatedChan <- true
				}
			case unix.SIGUSR1:
				gplog.Warn("Received shutdown request on segment %d: beginning cleanup", *content)
				terminatedChan <- true
			}
		}()
		wasTerminated = <-terminatedChan
		if wasTerminated {
			DoCleanup()
			os.Exit(2)
		} else {
			continue
		}
	}
}

/*
 * Shared functions
 */

func createPipe(pipe string) error {
	err := unix.Mkfifo(pipe, 0700)
	if err != nil {
		return err
	}

	pipesMap[pipe] = true
	return nil
}

func deletePipe(pipe string) error {
	err := utils.RemoveFileIfExists(pipe)
	if err != nil {
		return err
	}

	delete(pipesMap, pipe)
	return nil
}

// Gpbackup creates the first n pipes. Record these pipes.
func preloadCreatedPipes(oidList []int, queuedPipeCount int) {
	for i := 0; i < queuedPipeCount; i++ {
		pipeName := fmt.Sprintf("%s_%d", *pipeFile, oidList[i])
		pipesMap[pipeName] = true
	}
}

func getOidListFromFile(oidFileName string) ([]int, error) {
	oidStr, err := operating.System.ReadFile(oidFileName)
	if err != nil {
		logError(fmt.Sprintf("Error encountered reading oid list from file: %v", err))
		return nil, err
	}
	oidStrList := strings.Split(strings.TrimSpace(fmt.Sprintf("%s", oidStr)), "\n")
	oidList := make([]int, len(oidStrList))
	for i, oid := range oidStrList {
		num, _ := strconv.Atoi(oid)
		oidList[i] = num
	}
	sort.Ints(oidList)
	return oidList, nil
}

func flushAndCloseRestoreWriter(pipeName string, oid int) error {
	if writer != nil {
		err := writer.Flush()
		if err != nil {
			logError("Oid %d: Failed to flush pipe %s", oid, pipeName)
			return err
		}
		writer = nil
		log("Oid %d: Successfully flushed pipe %s", oid, pipeName)
	}
	if writeHandle != nil {
		err := writeHandle.Close()
		if err != nil {
			logError("Oid %d: Failed to close pipe handle", oid)
			return err
		}
		writeHandle = nil
		log("Oid %d: Successfully closed pipe handle", oid)
	}
	return nil
}

/*
 * Shared helper functions
 */

func DoCleanup() {
	defer CleanupGroup.Done()
	if wasTerminated {
		/*
		 * If the agent dies during the last table copy, it can still report
		 * success, so we create an error file and check for its presence in
		 * gprestore after the COPYs are finished.
		 */
		handle, _ := utils.OpenFileForWrite(fmt.Sprintf("%s_error", *pipeFile))
		_ = handle.Close()
	}
	err := flushAndCloseRestoreWriter("Current writer pipe on cleanup", 0)
	if err != nil {
		log("Encountered error during cleanup: %v", err)
	}

	for pipeName, _ := range pipesMap {
		log("Removing pipe %s", pipeName)
		err = deletePipe(pipeName)
		if err != nil {
			log("Encountered error removing pipe %s: %v", pipeName, err)
		}
	}

	skipFiles, _ := filepath.Glob(fmt.Sprintf("%s_skip_*", *pipeFile))
	for _, skipFile := range skipFiles {
		err = utils.RemoveFileIfExists(skipFile)
		if err != nil {
			log("Encountered error during cleanup skip files: %v", err)
		}
	}
	log("Cleanup complete")
}

func log(s string, v ...interface{}) {
	s = fmt.Sprintf("Segment %d: %s", *content, s)
	gplog.Verbose(s, v...)
}

func logError(s string, v ...interface{}) {
	s = fmt.Sprintf("Segment %d: %s", *content, s)
	gplog.Error(s, v...)
}
