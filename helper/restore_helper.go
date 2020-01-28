package helper

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	"github.com/greenplum-db/gpbackup/toc"
	"github.com/greenplum-db/gpbackup/utils"
	"github.com/pkg/errors"
)

/*
 * Restore specific functions
 */

func doRestoreAgent() error {
	tocEntries := toc.NewSegmentTOC(*tocFile).DataEntries
	var lastByte uint64
	var bytesRead int64
	var start uint64
	var end uint64
	var numDiscarded int
	var errRemove error
	var lastError error

	oidList, err := getOidListFromFile()
	if err != nil {
		return err
	}

	reader, err := getRestoreDataReader()
	if err != nil {
		return err
	}

	for i, oid := range oidList {
		if wasTerminated {
			return errors.New("Terminated due to user request")
		}

		currentPipe = fmt.Sprintf("%s_%d", *pipeFile, oidList[i])
		if i < len(oidList)-1 {
			nextPipe = fmt.Sprintf("%s_%d", *pipeFile, oidList[i+1])
			log(fmt.Sprintf("Creating pipe for oid %d: %s", oidList[i+1], nextPipe))
			err := createPipe(nextPipe)
			if err != nil {
				// In the case this error is hit it means we have lost the
				// ability to create pipes normally, so hard quit even if
				// --on-error-continue is given
				return err
			}
		}

		start = tocEntries[uint(oid)].StartByte
		end = tocEntries[uint(oid)].EndByte

		log(fmt.Sprintf("Opening pipe for oid %d: %s", oid, currentPipe))
		writer, writeHandle, err = getRestorePipeWriter(currentPipe)
		if err != nil {
			// In the case this error is hit it means we have lost the
			// ability to open pipes normally, so hard quit even if
			// --on-error-continue is given
			_ = removeFileIfExists(currentPipe)
			return err
		}

		log(fmt.Sprintf("Data Reader - Start Byte: %d; End Byte: %d; Last Byte: %d", start, end, lastByte))
		numDiscarded, err = reader.Discard(int(start - lastByte))
		if err != nil {
			// Always hard quit if data reader has issues
			_ = removeFileIfExists(currentPipe)
			return err
		}
		log(fmt.Sprintf("Data Reader discarded %d bytes", numDiscarded))

		log(fmt.Sprintf("Restoring table with oid %d", oid))
		bytesRead, err = io.CopyN(writer, reader, int64(end-start))
		if err != nil {
			// In case COPY FROM or copyN fails in the middle of a load. We
			// need to update the lastByte with the amount of bytes that was
			// copied before it errored out
			lastByte += uint64(bytesRead)
			err = errors.Wrap(err, strings.Trim(errBuf.String(), "\x00"))
			goto LoopEnd
		}
		lastByte = end
		log(fmt.Sprintf("Copied %d bytes into the pipe", bytesRead))

		log(fmt.Sprintf("Closing pipe for oid %d: %s", oid, currentPipe))
		err = flushAndCloseRestoreWriter()
		if err != nil {
			goto LoopEnd
		}

	LoopEnd:
		log(fmt.Sprintf("Removing pipe for oid %d: %s", oid, currentPipe))
		errRemove = removeFileIfExists(currentPipe)
		if errRemove != nil {
			_ = removeFileIfExists(nextPipe)
			return errRemove
		}

		if err != nil {
			if *onErrorContinue {
				logError(fmt.Sprintf("Error encountered: %v", err))
				lastError = err
				err = nil
				continue
			} else {
				return err
			}
		}
	}

	return lastError
}

func getRestoreDataReader() (*bufio.Reader, error) {
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
	errMsg := strings.Trim(errBuf.String(), "\x00")
	if len(errMsg) != 0 {
		return nil, errors.New(errMsg)
	}
	return bufIoReader, nil
}

func getRestorePipeWriter(currentPipe string) (*bufio.Writer, *os.File, error) {
	// Opening this pipe will block until a reader connects to the pipe
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
