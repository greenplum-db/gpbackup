package helper

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpbackup/utils"
)

/*
 * Restore specific functions
 */

func doRestoreAgent() {
	tocEntries := utils.NewSegmentTOC(*tocFile).DataEntries
	var lastByte uint64
	oidList := getOidListFromFile()

	currentPipe = fmt.Sprintf("%s_%d", *pipeFile, oidList[0])
	log(fmt.Sprintf("Opening pipe for oid %d", oidList[0]))
	/*
	 * It is important that we create the writer before creating the reader
	 * so that we establish a connection to the first pipe (created by gprestore)
	 * and properly clean it up if an error occurs while creating the reader.
	 */
	writer, writeHandle = getRestorePipeWriter(currentPipe)
	reader := getRestorePipeReader()

	for i, oid := range oidList {
		if wasTerminated {
			return
		}
		log(fmt.Sprintf("Restoring table with oid %d", oid))
		if i < len(oidList)-1 {
			nextPipe = fmt.Sprintf("%s_%d", *pipeFile, oidList[i+1])
			createPipe(nextPipe)
		} else {
			nextPipe = ""
		}
		start := tocEntries[uint(oid)].StartByte
		end := tocEntries[uint(oid)].EndByte
		log(fmt.Sprintf("Start Byte: %d; End Byte: %d; Last Byte: %d", start, end, lastByte))
		_, err := reader.Discard(int(start - lastByte))
		gplog.FatalOnError(err)
		log(fmt.Sprintf("Discarded %d bytes", start-lastByte))
		bytesRead, err := io.CopyN(writer, reader, int64(end-start))
		log(fmt.Sprintf("Read %d bytes", bytesRead))
		gplog.FatalOnError(err, errBuf.String())
		log(fmt.Sprintf("Closing pipe for oid %d", oid))
		flushAndCloseRestoreWriter()
		lastByte = end

		lastPipe = currentPipe
		currentPipe = nextPipe
		removeFileIfExists(lastPipe)
		if currentPipe != "" {
			log(fmt.Sprintf("Opening pipe for oid %d", oid))
			writer, writeHandle = getRestorePipeWriter(currentPipe)
		}
	}
}

func getRestorePipeReader() *bufio.Reader {
	var readHandle io.Reader
	var err error
	if *pluginConfigFile != "" {
		readHandle = startRestorePluginCommand()
		defer func() {
			if len(errBuf.String()) != 0 {
				gplog.Error(errBuf.String())
			}
		}()
	} else {
		readHandle, err = os.Open(*dataFile)
		gplog.FatalOnError(err)
	}

	var bufIoReader *bufio.Reader
	if strings.HasSuffix(*dataFile, ".gz") {
		gzipReader, err := gzip.NewReader(readHandle)
		gplog.FatalOnError(err)
		bufIoReader = bufio.NewReader(gzipReader)
	} else {
		bufIoReader = bufio.NewReader(readHandle)
	}
	return bufIoReader
}

func getRestorePipeWriter(currentPipe string) (*bufio.Writer, *os.File) {
	fileHandle, err := os.OpenFile(currentPipe, os.O_WRONLY, os.ModeNamedPipe)
	gplog.FatalOnError(err)
	pipeWriter := bufio.NewWriter(fileHandle)
	return pipeWriter, fileHandle
}

func startRestorePluginCommand() io.Reader {
	pluginConfig := utils.ReadPluginConfig(*pluginConfigFile)
	cmdStr := fmt.Sprintf("%s restore_data %s %s", pluginConfig.ExecutablePath, pluginConfig.ConfigPath, *dataFile)
	cmd := exec.Command("bash", "-c", cmdStr)

	readHandle, err := cmd.StdoutPipe()
	gplog.FatalOnError(err)
	cmd.Stderr = &errBuf

	err = cmd.Start()
	gplog.FatalOnError(err)
	return readHandle

}
