package helper

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"

	"github.com/greenplum-db/gpbackup/toc"
	"github.com/greenplum-db/gpbackup/utils"
	"github.com/klauspost/compress/zstd"
	"github.com/pkg/errors"
)

/*
 * Restore specific functions
 */
type ReaderType string

const (
	SEEKABLE    ReaderType = "seekable" // reader which supports seek
	NONSEEKABLE            = "discard"  // reader which is not seekable
	SUBSET                 = "subset"   // reader which operates on pre filtered data
)

/* RestoreReader structure to wrap the underlying reader.
 * readerType identifies how the reader can be used
 * SEEKABLE uses seekReader. Used when restoring from uncompressed data with filters from local filesystem
 * NONSEEKABLE and SUBSET types uses bufReader.
 * SUBSET type applies when restoring using plugin(if compatible) from uncompressed data with filters
 * NONSEEKABLE type applies for every other restore scenario
 */
type RestoreReader struct {
	bufReader  *bufio.Reader
	seekReader io.ReadSeeker
	readerType ReaderType
}

func (r *RestoreReader) positionReader(pos uint64) error {
	switch r.readerType {
	case SEEKABLE:
		seekPosition, err := r.seekReader.Seek(int64(pos), io.SeekCurrent)
		if err != nil {
			// Always hard quit if data reader has issues
			_ = utils.RemoveFileIfExists(currentPipe)
			return err
		}
		log(fmt.Sprintf("Data Reader seeked forward to %d byte offset", seekPosition))
	case NONSEEKABLE:
		numDiscarded, err := r.bufReader.Discard(int(pos))
		if err != nil {
			// Always hard quit if data reader has issues
			_ = utils.RemoveFileIfExists(currentPipe)
			return err
		}
		log(fmt.Sprintf("Data Reader discarded %d bytes", numDiscarded))
	case SUBSET:
		// Do nothing as the stream is pre filtered
	}
	return nil
}

func (r *RestoreReader) copyData(num int64) (int64, error) {
	var bytesRead int64
	var err error
	switch r.readerType {
	case SEEKABLE:
		bytesRead, err = io.CopyN(writer, r.seekReader, num)
	case NONSEEKABLE, SUBSET:
		bytesRead, err = io.CopyN(writer, r.bufReader, num)
	}
	return bytesRead, err
}

func doRestoreAgent() error {
	segmentTOC := toc.NewSegmentTOC(*tocFile)
	tocEntries := segmentTOC.DataEntries

	var lastByte uint64
	var bytesRead int64
	var start uint64
	var end uint64
	var errRemove error
	var lastError error

	oidList, err := getOidListFromFile()
	if err != nil {
		return err
	}

	reader, err := getRestoreDataReader(segmentTOC, oidList)
	if err != nil {
		return err
	}
	log(fmt.Sprintf("Using reader type: %s", reader.readerType))

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
			_ = utils.RemoveFileIfExists(currentPipe)
			return err
		}

		log(fmt.Sprintf("Data Reader - Start Byte: %d; End Byte: %d; Last Byte: %d", start, end, lastByte))
		err = reader.positionReader(start - lastByte)
		if err != nil {
			return err
		}

		log(fmt.Sprintf("Restoring table with oid %d", oid))
		bytesRead, err = reader.copyData(int64(end - start))
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
		errRemove = utils.RemoveFileIfExists(currentPipe)
		if errRemove != nil {
			_ = utils.RemoveFileIfExists(nextPipe)
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

func getRestoreDataReader(toc *toc.SegmentTOC, oidList []int) (*RestoreReader, error) {
	var readHandle io.Reader
	var seekHandle io.ReadSeeker
	var isSubset bool
	var err error = nil
	restoreReader := new(RestoreReader)

	if *pluginConfigFile != "" {
		readHandle, isSubset, err = startRestorePluginCommand(toc, oidList)
		if isSubset {
			// Reader that operates on subset data
			restoreReader.readerType = SUBSET
		} else {
			// Regular reader which doesn't support seek
			restoreReader.readerType = NONSEEKABLE
		}
	} else {
		if *isFiltered && !strings.HasSuffix(*dataFile, ".gz") && !strings.HasSuffix(*dataFile, ".zst") {
			// Seekable reader if backup is not compressed and filters are set
			seekHandle, err = os.Open(*dataFile)
			restoreReader.readerType = SEEKABLE
		} else {
			// Regular reader which doesn't support seek
			readHandle, err = os.Open(*dataFile)
			restoreReader.readerType = NONSEEKABLE
		}
	}
	if err != nil {
		return nil, err
	}

	// Set the underlying stream reader in restoreReader
	if restoreReader.readerType == SEEKABLE {
		restoreReader.seekReader = seekHandle
	} else if strings.HasSuffix(*dataFile, ".gz") {
		gzipReader, err := gzip.NewReader(readHandle)
		if err != nil {
			return nil, err
		}
		restoreReader.bufReader = bufio.NewReader(gzipReader)
	} else if strings.HasSuffix(*dataFile, ".zst") {
		zstdReader, err := zstd.NewReader(readHandle)
		if err != nil {
			return nil, err
		}
		restoreReader.bufReader = bufio.NewReader(zstdReader)
	} else {
		restoreReader.bufReader = bufio.NewReader(readHandle)
	}

	// Check that no error has occurred in plugin command
	errMsg := strings.Trim(errBuf.String(), "\x00")
	if len(errMsg) != 0 {
		return nil, errors.New(errMsg)
	}

	return restoreReader, err
}

func getRestorePipeWriter(currentPipe string) (*bufio.Writer, *os.File, error) {
	// Opening this pipe will block until a reader connects to the pipe
	fileHandle, err := os.OpenFile(currentPipe, os.O_WRONLY, os.ModeNamedPipe)
	if err != nil {
		return nil, nil, err
	}

	// At the moment (Golang 1.15), the copy_file_range system call from the os.File
	// ReadFrom method is only supported for Linux platforms. Furthermore, cross-filesystem
	// support only works on kernel versions 5.3 and above. Until modern OS platforms start
	// adopting the new kernel, we must only use the bare essential methods Write() and
	// Close() for the pipe to avoid an extra buffer read that can happen in error
	// scenarios with --on-error-continue.
	pipeWriter := bufio.NewWriter(struct{ io.WriteCloser }{fileHandle})

	return pipeWriter, fileHandle, nil
}

func startRestorePluginCommand(toc *toc.SegmentTOC, oidList []int) (io.Reader, bool, error) {
	isSubset := false
	pluginConfig, err := utils.ReadPluginConfig(*pluginConfigFile)
	if err != nil {
		return nil, false, err
	}
	cmdStr := ""
	if pluginConfig.CanRestoreSubset() && *isFiltered && !strings.HasSuffix(*dataFile, ".gz") && !strings.HasSuffix(*dataFile, ".zst") {
		offsetsFile, _ := ioutil.TempFile("/tmp", "gprestore_offsets_")
		defer func() {
			offsetsFile.Close()
		}()
		w := bufio.NewWriter(offsetsFile)
		w.WriteString(fmt.Sprintf("%v", len(oidList)))

		for _, oid := range oidList {
			w.WriteString(fmt.Sprintf(" %v %v", toc.DataEntries[uint(oid)].StartByte, toc.DataEntries[uint(oid)].EndByte))
		}
		w.Flush()
		cmdStr = fmt.Sprintf("%s restore_data_subset %s %s %s", pluginConfig.ExecutablePath, pluginConfig.ConfigPath, *dataFile, offsetsFile.Name())
		isSubset = true
	} else {
		cmdStr = fmt.Sprintf("%s restore_data %s %s", pluginConfig.ExecutablePath, pluginConfig.ConfigPath, *dataFile)
	}
	log(fmt.Sprintf("%s", cmdStr))
	cmd := exec.Command("bash", "-c", cmdStr)

	readHandle, err := cmd.StdoutPipe()
	if err != nil {
		return nil, false, err
	}
	cmd.Stderr = &errBuf

	err = cmd.Start()
	return readHandle, isSubset, err
}
