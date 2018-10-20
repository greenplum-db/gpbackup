package helper

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	"github.com/greenplum-db/gpbackup/utils"
	"github.com/pkg/errors"
)

/*
 * Restore specific functions
 */

func doRestoreAgent() error {
	tocEntries := utils.NewSegmentTOC(*tocFile).DataEntries
	var lastByte uint64
	oidList, err := getOidListFromFile()
	if err != nil {
		return err
	}

	currentPipe = fmt.Sprintf("%s_%d", *pipeFile, oidList[0])
	log(fmt.Sprintf("Opening pipe for oid %d", oidList[0]))
	/*
	 * It is important that we create the writer before creating the reader
	 * so that we establish a connection to the first pipe (created by gprestore)
	 * and properly clean it up if an error occurs while creating the reader.
	 */
	writer, writeHandle, err = getRestorePipeWriter(currentPipe)
	if err != nil {
		return err
	}
	reader, err := getRestorePipeReader()
	if err != nil {
		return err
	}

	for i, oid := range oidList {
		if wasTerminated {
			return errors.New("Terminated due to user request")
		}
		log(fmt.Sprintf("Restoring table with oid %d", oid))
		if i < len(oidList)-1 {
			nextPipe = fmt.Sprintf("%s_%d", *pipeFile, oidList[i+1])
			err := createPipe(nextPipe)
			if err != nil {
				return err
			}
		} else {
			nextPipe = ""
		}
		start := tocEntries[uint(oid)].StartByte
		end := tocEntries[uint(oid)].EndByte
		log(fmt.Sprintf("Start Byte: %d; End Byte: %d; Last Byte: %d", start, end, lastByte))
		_, err := reader.Discard(int(start - lastByte))
		if err != nil {
			return err
		}
		log(fmt.Sprintf("Discarded %d bytes", start-lastByte))
		bytesRead, err := io.CopyN(writer, reader, int64(end-start))
		log(fmt.Sprintf("Read %d bytes", bytesRead))
		if err != nil {
			return errors.Wrap(err, strings.Trim(errBuf.String(), "\x00"))
		}
		log(fmt.Sprintf("Closing pipe for oid %d", oid))
		err = flushAndCloseRestoreWriter()
		if err != nil {
			return err
		}
		lastByte = end

		lastPipe = currentPipe
		currentPipe = nextPipe
		err = removeFileIfExists(lastPipe)
		if err != nil {
			return err
		}
		if currentPipe != "" {
			log(fmt.Sprintf("Opening pipe for oid %d", oid))
			writer, writeHandle, err = getRestorePipeWriter(currentPipe)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func getRestorePipeReader() (*bufio.Reader, error) {
	var readHandle io.Reader
	var err error
	if *pluginConfigFile != "" {
		readHandle, err = startRestorePluginCommand()
	} else {
		readHandle, err = os.Open(*dataFile)
	}
	if err != nil {
		return nil, err
	}

	var bufIoReader *bufio.Reader
	if strings.HasSuffix(*dataFile, ".gz") {
		gzipReader, err := gzip.NewReader(readHandle)
		if err != nil {
			return nil, err
		}
		bufIoReader = bufio.NewReader(gzipReader)
	} else {
		bufIoReader = bufio.NewReader(readHandle)
	}
	// Check that no error has occurred in plugin command
	errString := strings.Trim(errBuf.String(), "\x00")
	if len(errString) != 0 {
		return nil, errors.New(errString)
	}
	return bufIoReader, nil
}

func getRestorePipeWriter(currentPipe string) (*bufio.Writer, *os.File, error) {
	fileHandle, err := os.OpenFile(currentPipe, os.O_WRONLY, os.ModeNamedPipe)
	if err != nil {
		return nil, nil, err
	}
	pipeWriter := bufio.NewWriter(fileHandle)
	return pipeWriter, fileHandle, nil
}

func startRestorePluginCommand() (io.Reader, error) {
	pluginConfig, err := utils.ReadPluginConfig(*pluginConfigFile)
	if err != nil {
		return nil, err
	}
	cmdStr := fmt.Sprintf("%s restore_data %s %s", pluginConfig.ExecutablePath, pluginConfig.ConfigPath, *dataFile)
	cmd := exec.Command("bash", "-c", cmdStr)

	readHandle, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	cmd.Stderr = &errBuf

	err = cmd.Start()
	return readHandle, err

}
