package helper

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"strings"
	"time"

	"github.com/greenplum-db/gpbackup/toc"
	"github.com/greenplum-db/gpbackup/utils"
	"github.com/klauspost/compress/zstd"
	"github.com/pkg/errors"
	"golang.org/x/sys/unix"
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

func (r *RestoreReader) positionReader(pos uint64, oid int) error {
	switch r.readerType {
	case SEEKABLE:
		seekPosition, err := r.seekReader.Seek(int64(pos), io.SeekCurrent)
		if err != nil {
			// Always hard quit if data reader has issues
			return err
		}
		log(fmt.Sprintf("Oid %d: Data Reader seeked forward to %d byte offset", oid, seekPosition))
	case NONSEEKABLE:
		numDiscarded, err := r.bufReader.Discard(int(pos))
		if err != nil {
			// Always hard quit if data reader has issues
			return err
		}
		log(fmt.Sprintf("Oid %d: Data Reader discarded %d bytes", oid, numDiscarded))
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
	var lastError error

	oidList, err := getOidListFromFile()
	if err != nil {
		return err
	}

	reader, err := getRestoreDataReader(segmentTOC, oidList)
	if err != nil {
		logError(fmt.Sprintf("Error encountered getting restore data reader: %v", err))
		return err
	}
	log(fmt.Sprintf("Using reader type: %s", reader.readerType))

	preloadCreatedPipes(oidList, *copyQueue)

	var currentPipe string
	for i, oid := range oidList {
		if wasTerminated {
			logError("Terminated due to user request")
			return errors.New("Terminated due to user request")
		}

		currentPipe = fmt.Sprintf("%s_%d", *pipeFile, oidList[i])
		if i < len(oidList)-*copyQueue {
			nextPipeToCreate := fmt.Sprintf("%s_%d", *pipeFile, oidList[i+*copyQueue])
			log(fmt.Sprintf("Oid %d: Creating pipe %s\n", oidList[i+*copyQueue], nextPipeToCreate))
			err := createPipe(nextPipeToCreate)
			if err != nil {
				logError(fmt.Sprintf("Oid %d: Failed to create pipe %s\n", oidList[i+*copyQueue], nextPipeToCreate))
				// In the case this error is hit it means we have lost the
				// ability to create pipes normally, so hard quit even if
				// --on-error-continue is given
				return err
			}
		}

		start = tocEntries[uint(oid)].StartByte
		end = tocEntries[uint(oid)].EndByte

		log(fmt.Sprintf("Oid %d: Opening pipe %s", oid, currentPipe))
		for {
			writer, writeHandle, err = getRestorePipeWriter(currentPipe)
			if err != nil {
				if errors.Is(err, unix.ENXIO) {
					// COPY (the pipe reader) has not tried to access the pipe yet so our restore_helper
					// process will get ENXIO error on its nonblocking open call on the pipe. We loop in
					// here while looking to see if gprestore has created a skip file for this restore entry.
					//
					// TODO: Skip files will only be created when gprestore is run against GPDB 6+ so it
					// might be good to have a GPDB version check here. However, the restore helper should
					// not contain a database connection so the version should be passed through the helper
					// invocation from gprestore (e.g. create a --db-version flag option).
					if *onErrorContinue && utils.FileExists(fmt.Sprintf("%s_skip_%d", *pipeFile, oid)) {
						log(fmt.Sprintf("Skip file has been discovered for entry %d, skipping it", oid))
						err = nil
						goto LoopEnd
					} else {
						// keep trying to open the pipe
						time.Sleep(100 * time.Millisecond)
					}
				} else {
					// In the case this error is hit it means we have lost the
					// ability to open pipes normally, so hard quit even if
					// --on-error-continue is given
					logError(fmt.Sprintf("Oid %d: Pipes can no longer be created. Exiting with error: %v", err))
					return err
				}
			} else {
				// A reader has connected to the pipe and we have successfully opened
				// the writer for the pipe. To avoid having to write complex buffer
				// logic for when os.write() returns EAGAIN due to full buffer, set
				// the file descriptor to block on IO.
				unix.SetNonblock(int(writeHandle.Fd()), false)
				log(fmt.Sprintf("Oid %d: Reader connected to pipe %s", oid, path.Base(currentPipe)))
				break
			}
		}

		log(fmt.Sprintf("Oid %d: Data Reader - Start Byte: %d; End Byte: %d; Last Byte: %d", oid, start, end, lastByte))
		err = reader.positionReader(start - lastByte, oid)
		if err != nil {
			logError(fmt.Sprint("Oid %d: Error reading from pipe: %v", oid, err))
			return err
		}

		log(fmt.Sprintf("Oid %d: Start table restore", oid))
		bytesRead, err = reader.copyData(int64(end - start))
		if err != nil {
			// In case COPY FROM or copyN fails in the middle of a load. We
			// need to update the lastByte with the amount of bytes that was
			// copied before it errored out
			lastByte += uint64(bytesRead)
			if errBuf.Len() > 0 {
				err = errors.Wrap(err, strings.Trim(errBuf.String(), "\x00"))
			} else {
				err = errors.Wrap(err, "Error copying data")
			}
			goto LoopEnd
		}
		lastByte = end
		log(fmt.Sprintf("Oid %d: Copied %d bytes into the pipe", oid, bytesRead))

		err = flushAndCloseRestoreWriter(path.Base(currentPipe), oid)
		if err != nil {
			log(fmt.Sprintf("Oid %d: Failed to flush and close pipe", oid))
			goto LoopEnd
		}
		log(fmt.Sprintf("Oid %d: Successfully flushed and closed pipe", oid))

	LoopEnd:
		log(fmt.Sprintf("Oid %d: Attempt to delete pipe", oid))
		errPipe := deletePipe(currentPipe)
		if errPipe != nil {
			logError("Oid %d: Pipe remove failed with error: %v", oid, errPipe)
			return errPipe
		}

		if err != nil {
			if *onErrorContinue {
				logError(fmt.Sprintf("Oid %d: Error encountered: %v", oid, err))
				lastError = err
				err = nil
				continue
			} else {
				logError(fmt.Sprintf("Oid %d: Error encountered: %v", oid, err))
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
		// error logging handled by calling functions
		return nil, err
	}

	// Set the underlying stream reader in restoreReader
	if restoreReader.readerType == SEEKABLE {
		restoreReader.seekReader = seekHandle
	} else if strings.HasSuffix(*dataFile, ".gz") {
		gzipReader, err := gzip.NewReader(readHandle)
		if err != nil {
			// error logging handled by calling functions
			return nil, err
		}
		restoreReader.bufReader = bufio.NewReader(gzipReader)
	} else if strings.HasSuffix(*dataFile, ".zst") {
		zstdReader, err := zstd.NewReader(readHandle)
		if err != nil {
			// error logging handled by calling functions
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
	fileHandle, err := os.OpenFile(currentPipe, os.O_WRONLY|unix.O_NONBLOCK, os.ModeNamedPipe)
	if err != nil {
		// error logging handled by calling functions
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
		logError(fmt.Sprintf("Error encountered when reading plugin config: %v", err))
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
