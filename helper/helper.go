package helper

import (
	"bufio"
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpbackup/utils"
)

var ( // Shared globals
	content      *int
	dataFile     *string
	logger       *gplog.Logger
	oid          *uint
	oidFile      *string
	pipeFile     *string
	printVersion *bool
	restoreAgent *bool
	tocFile      *string
	version      string
)

var ( // Globals for restore only
	CleanupGroup  *sync.WaitGroup
	currentPipe   string
	lastPipe      string
	nextPipe      string
	wasTerminated bool
	writer        *bufio.Writer
	writeHandle   *os.File
)

/*
 * Shared functions
 */

func DoHelper() {
	defer DoTeardown()
	InitializeGlobals()
	utils.InitializeSignalHandler(DoCleanup, fmt.Sprintf("restore agent on segment %d", *content), &wasTerminated)
	if *restoreAgent {
		doRestoreAgent()
	} else {
		doBackupHelper()
	}
}

func InitializeGlobals() {
	CleanupGroup = &sync.WaitGroup{}
	CleanupGroup.Add(1)
	content = flag.Int("content", -2, "Content ID of the corresponding segment")
	dataFile = flag.String("data-file", "", "Absolute path to the data file")
	logger = utils.InitializeLogging("gpbackup_helper", "")
	oid = flag.Uint("oid", 0, "Oid of the table being processed")
	oidFile = flag.String("oid-file", "", "Absolute path to the file containing a list of oids to restore")
	pipeFile = flag.String("pipe-file", "", "Absolute path to the pipe file")
	printVersion = flag.Bool("version", false, "Print version number and exit")
	restoreAgent = flag.Bool("restore-agent", false, "Use gpbackup_helper as an agent for restore")
	tocFile = flag.String("toc-file", "", "Absolute path to the table of contents file")
	flag.Parse()
	if *printVersion {
		fmt.Printf("gpbackup_helper %s\n", version)
		os.Exit(0)
	}
	utils.InitializeSystemFunctions()
}

func SetContent(id int) {
	content = &id
}

func SetFilename(name string) {
	tocFile = &name
}

func SetLogger(log *gplog.Logger) {
	logger = log
}

func SetOid(newoid uint) {
	oid = &newoid
}

/*
 * Backup helper functions
 */

func doBackupHelper() {
	toc, lastRead := ReadOrCreateTOC()
	numBytes := ReadAndCountBytes()
	lastProcessed := lastRead + numBytes
	toc.AddSegmentDataEntry(*oid, lastRead, lastProcessed)
	toc.LastByteRead = lastProcessed
	toc.WriteToFile(*tocFile)
}

func ReadOrCreateTOC() (*utils.SegmentTOC, uint64) {
	var toc *utils.SegmentTOC
	var lastRead uint64
	if utils.FileExistsAndIsReadable(*tocFile) {
		toc = utils.NewSegmentTOC(*tocFile)
		lastRead = toc.LastByteRead
	} else {
		toc = &utils.SegmentTOC{}
		toc.DataEntries = make(map[uint]utils.SegmentDataEntry, 1)
		lastRead = 0
	}
	return toc, lastRead
}

func ReadAndCountBytes() uint64 {
	reader := bufio.NewReader(utils.System.Stdin)
	numBytes, _ := io.Copy(utils.System.Stdout, reader)
	return uint64(numBytes)
}

/*
 * Restore helper functions
 */

func getOidListFromFile() []int {
	oidStr, err := utils.System.ReadFile(*oidFile)
	utils.CheckError(err)
	oidStrList := strings.Split(strings.TrimSpace(fmt.Sprintf("%s", oidStr)), "\n")
	oidList := make([]int, len(oidStrList))
	for i, oid := range oidStrList {
		num, _ := strconv.ParseInt(oid, 10, 32)
		oidList[i] = int(num)
	}
	sort.Ints(oidList)
	return oidList
}

func doRestoreAgent() {
	tocEntries := utils.NewSegmentTOC(*tocFile).DataEntries
	lastByte := uint64(0)
	oidList := getOidListFromFile()
	reader := getPipeReader()

	lastPipe = ""
	currentPipe = ""
	nextPipe = fmt.Sprintf("%s_%d", *pipeFile, oidList[0])
	for i, oid := range oidList {
		log(fmt.Sprintf("Restoring table with oid %d", oid))
		lastPipe = currentPipe
		currentPipe = nextPipe
		if i < len(oidList)-1 {
			nextPipe = fmt.Sprintf("%s_%d", *pipeFile, oidList[i+1])
			createNextPipe()
		} else {
			nextPipe = ""
		}
		removeFileIfExists(lastPipe)

		log(fmt.Sprintf("Opening pipe for oid %d", oid))
		writer, writeHandle = getPipeWriter(currentPipe)
		start := tocEntries[uint(oid)].StartByte
		end := tocEntries[uint(oid)].EndByte
		log(fmt.Sprintf("Start Byte: %d; End Byte: %d; Last Byte: %d", start, end, lastByte))
		reader.Discard(int(start - lastByte))
		log(fmt.Sprintf("Discarded %d bytes", start-lastByte))
		bytesRead, err := io.CopyN(writer, reader, int64(end-start))
		log(fmt.Sprintf("Read %d bytes", bytesRead))
		utils.CheckError(err)
		log(fmt.Sprintf("Closing pipe for oid %d", oid))
		flushAndCloseWriter()
		lastByte = end
	}
}

func createNextPipe() {
	err := syscall.Mkfifo(nextPipe, 0777)
	utils.CheckError(err)
}

func getPipeReader() *bufio.Reader {
	readHandle, err := os.Open(*dataFile)
	utils.CheckError(err)
	var reader *bufio.Reader
	if strings.HasSuffix(*dataFile, ".gz") {
		gzipReader, err := gzip.NewReader(readHandle)
		utils.CheckError(err)
		reader = bufio.NewReader(gzipReader)
	} else {
		reader = bufio.NewReader(readHandle)
	}
	return reader
}

func getPipeWriter(currentPipe string) (*bufio.Writer, *os.File) {
	fileHandle, err := os.OpenFile(currentPipe, os.O_WRONLY, os.ModeNamedPipe)
	utils.CheckError(err)
	pipeWriter := bufio.NewWriter(fileHandle)
	return pipeWriter, fileHandle
}

func flushAndCloseWriter() {
	if writer != nil {
		err := writer.Flush()
		utils.CheckError(err)
		writer = nil
	}
	if writeHandle != nil {
		err := writeHandle.Close()
		utils.CheckError(err)
		writeHandle = nil
	}
}

func fileExists(filename string) bool {
	_, err := utils.System.Stat(filename)
	return err == nil
}

func removeFileIfExists(filename string) {
	if fileExists(filename) {
		err := os.Remove(filename)
		utils.CheckError(err)
	}
}

/*
 * Shared helper functions
 */

func DoTeardown() {
	if err := recover(); err != nil {
		log("%v", err)
	}
	if wasTerminated {
		CleanupGroup.Wait()
		return
	}
	DoCleanup()
	os.Exit(gplog.GetErrorCode())
}

func DoCleanup() {
	defer func() {
		if err := recover(); err != nil {
			log("Encountered error during cleanup: %v", err)
		}
		log("Cleanup complete")
		CleanupGroup.Done()
	}()
	if wasTerminated {
		/*
		 * If the agent dies during the last table copy, it can still report
		 * success, so we create an error file and check for its presence in
		 * gprestore after the COPYs are finished.
		 */
		handle := utils.MustOpenFileForWriting(fmt.Sprintf("%s_error", *pipeFile))
		handle.Close()
	}
	if *restoreAgent {
		flushAndCloseWriter()
		removeFileIfExists(lastPipe)
		removeFileIfExists(currentPipe)
		removeFileIfExists(nextPipe)
	}
}

func log(s string, v ...interface{}) {
	s = fmt.Sprintf("Segment %d: %s", *content, s)
	logger.Verbose(s, v...)
}
