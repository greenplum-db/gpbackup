package helper

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"os/exec"
	"path"
	"regexp"
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

var (
	contentRE *regexp.Regexp
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

func (r *RestoreReader) copyAllData() (int64, error) {
	var bytesRead int64
	var err error
	switch r.readerType {
	case SEEKABLE:
		bytesRead, err = io.Copy(writer, r.seekReader)
	case NONSEEKABLE, SUBSET:
		bytesRead, err = io.Copy(writer, r.bufReader)
	}
	return bytesRead, err
}

func doRestoreAgent() error {
	// We need to track various values separately per content for resize restore
	var segmentTOC map[int]*toc.SegmentTOC
	var tocEntries map[int]map[uint]toc.SegmentDataEntry
	var start map[int]uint64
	var end map[int]uint64
	var lastByte map[int]uint64

	var bytesRead int64
	var lastError error
	var replicatedTables map[int]bool

	readers := make(map[int]*RestoreReader)

	oidList, err := getOidListFromFile(*oidFile)
	if err != nil {
		return err
	}

	// During a larger-to-smaller restore, we need to do multiple passes for each oid, so the table
	// restore goes into another nested for loop below.  In the normal or smaller-to-larger cases,
	// this is equivalent to doing a single loop per table.
	batches := 1
	if *isResizeRestore && *origSize > *destSize {
		batches = *origSize / *destSize
		// If dest doesn't divide evenly into orig, there's one more incomplete batch
		if *origSize%*destSize != 0 {
			batches += 1
		}
	}

	if *singleDataFile {
		contentToRestore := *content
		segmentTOC = make(map[int]*toc.SegmentTOC)
		tocEntries = make(map[int]map[uint]toc.SegmentDataEntry)
		start = make(map[int]uint64)
		end = make(map[int]uint64)
		lastByte = make(map[int]uint64)
		for b := 0; b < batches; b++ {
			// When performing a resize restore, if the content of the file we're being asked to read from
			// is higher than any backup content, then no such file exists and we shouldn't try to open it.
			if *isResizeRestore && contentToRestore >= *origSize {
				break
			}
			tocFileForContent := replaceContentInFilename(*tocFile, contentToRestore)
			segmentTOC[contentToRestore] = toc.NewSegmentTOC(tocFileForContent)
			tocEntries[contentToRestore] = segmentTOC[contentToRestore].DataEntries

			filename := replaceContentInFilename(*dataFile, contentToRestore)
			readers[contentToRestore], err = getRestoreDataReader(filename, segmentTOC[contentToRestore], oidList)

			if err != nil {
				logError(fmt.Sprintf("Error encountered getting restore data reader for single data file: %v", err))
				return err
			}
			log(fmt.Sprintf("Using reader type: %s", readers[contentToRestore].readerType))

			contentToRestore += *destSize
		}
	}

	if *isResizeRestore && batches > 1 && utils.FileExists(*replicationFile) {
		// in the case of a larger to smaller restore, note all replicated tables
		// so that they do not get restored to the same segment multiple times

		// if no replicated tables were present in toc, the file is not written so
		// this step is skipped
		replicatedOidList, err := getOidListFromFile(*replicationFile)
		if err != nil {
			return err
		}

		replicatedTables = make(map[int]bool, len(replicatedOidList))
		for _, oid := range replicatedOidList {
			replicatedTables[oid] = true
		}
	}

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

		// The pipe creation queue goes before the below loop because that still happens just once per table;
		// the LoopEnd block goes after it because we're re-using the same pipes for each table.

		contentToRestore := *content

		for b := 0; b < batches; b++ {
			if replicatedTables != nil {
				tableIsNotYetRestored, tableIsPresent := replicatedTables[oid]
				if tableIsPresent && tableIsNotYetRestored {
					// this is the first batch encountering this replicated table.
					// restore it and mark it for no further restoration
					replicatedTables[oid] = false
				} else if !tableIsNotYetRestored {
					// table was already restored to this segment in a previous batch
					continue
				}
			}
			if *singleDataFile {
				start[contentToRestore] = tocEntries[contentToRestore][uint(oid)].StartByte
				end[contentToRestore] = tocEntries[contentToRestore][uint(oid)].EndByte
			} else if *isResizeRestore {
				if contentToRestore < *origSize {
					// We can only pass one filename to the helper, so we still pass in the single-data-file-style
					// filename in a non-SDF resize case, then add the oid manually and set up the reader for that.
					filename := constructSingleTableFilename(*dataFile, contentToRestore, oid)

					// We pre-create readers above for the sake of not re-opening SDF readers.  For MDF we can't
					// re-use them but still having them in a map simplifies overall code flow.  We repeatedly assign
					// to a map entry here intentionally.
					readers[contentToRestore], err = getRestoreDataReader(filename, nil, nil)
					if err != nil {
						logError(fmt.Sprintf("Error encountered getting restore data reader: %v", err))
						return err
					}
				}
			}

			log(fmt.Sprintf("Oid %d: Opening pipe %s", oid, currentPipe))
			retries := 0
			var elapsedWait time.Duration = 0
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
							// keep trying to open the pipe.  If we've been trying for more than 60 seconds, log warnings
							retries += 1

							var backoff time.Duration
							if elapsedWait > time.Duration(60)*time.Second {
								log(fmt.Sprintf("Waiting to open writer to pipe %s because reader has not yet tried to access it. Have been waiting for: %s", currentPipe, elapsedWait))
								backoff = time.Duration(60) * time.Second
							} else {
								backoff = time.Duration(math.Pow(2, float64(retries-1))) * 10 * time.Millisecond
							}

							elapsedWait += backoff
							time.Sleep(backoff)
						}
					} else {
						// In the case this error is hit it means we have lost the
						// ability to open pipes normally, so hard quit even if
						// --on-error-continue is given
						logError(fmt.Sprintf("Oid %d: Pipes can no longer be created. Exiting with error: %v", oid, err))
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

			// Only position reader in case of SDF.  MDF case reads entire file, and does not need positioning.
			// Further, in SDF case, map entries for contents that were not part of original backup will be nil,
			// and calling methods on them errors silently.
			if *singleDataFile && !(*isResizeRestore && contentToRestore >= *origSize) {
				log(fmt.Sprintf("Oid %d: Data Reader - Start Byte: %d; End Byte: %d; Last Byte: %d", oid, start[contentToRestore], end[contentToRestore], lastByte[contentToRestore]))
				err = readers[contentToRestore].positionReader(start[contentToRestore]-lastByte[contentToRestore], oid)
				if err != nil {
					logError(fmt.Sprintf("Oid %d: Error reading from pipe: %v", oid, err))
					return err
				}
			}

			log(fmt.Sprintf("Oid %d: Start table restore", oid))
			if *isResizeRestore {
				if contentToRestore < *origSize {
					if *singleDataFile {
						bytesRead, err = readers[contentToRestore].copyData(int64(end[contentToRestore] - start[contentToRestore]))
					} else {
						bytesRead, err = readers[contentToRestore].copyAllData()
					}
				} else {
					// Write "empty" data to the pipe for COPY ON SEGMENT to read.
					bytesRead = 0
					writer.Write([]byte{})
				}
			} else {
				bytesRead, err = readers[contentToRestore].copyData(int64(end[contentToRestore] - start[contentToRestore]))
			}
			if err != nil {
				// In case COPY FROM or copyN fails in the middle of a load. We
				// need to update the lastByte with the amount of bytes that was
				// copied before it errored out
				if *singleDataFile {
					lastByte[contentToRestore] += uint64(bytesRead)
				}
				if errBuf.Len() > 0 {
					err = errors.Wrap(err, strings.Trim(errBuf.String(), "\x00"))
				} else {
					err = errors.Wrap(err, "Error copying data")
				}
				goto LoopEnd
			}

			if *singleDataFile {
				lastByte[contentToRestore] = end[contentToRestore]
			}
			log(fmt.Sprintf("Oid %d: Copied %d bytes into the pipe", oid, bytesRead))

			log(fmt.Sprintf("Closing pipe for oid %d: %s", oid, currentPipe))
			err = flushAndCloseRestoreWriter(currentPipe, oid)
			if err != nil {
				log(fmt.Sprintf("Oid %d: Failed to flush and close pipe", oid))
				goto LoopEnd
			}

			// Recreate pipe to prevent data holdover bug
			err = deletePipe(currentPipe)
			if err != nil {
				log(fmt.Sprintf("Error deleting pipe %s at end of batch loop: %v", currentPipe, err))
				goto LoopEnd
			}
			err = createPipe(currentPipe)
			if err != nil {
				log(fmt.Sprintf("Error creating pipe %s at end of batch loop: %v", currentPipe, err))
				goto LoopEnd
			}

			contentToRestore += *destSize
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

func constructSingleTableFilename(name string, contentToRestore int, oid int) string {
	name = strings.ReplaceAll(name, fmt.Sprintf("gpbackup_%d", *content), fmt.Sprintf("gpbackup_%d", contentToRestore))
	nameParts := strings.Split(name, ".")
	filename := fmt.Sprintf("%s_%d", nameParts[0], oid)
	if len(nameParts) > 1 { // We only expect filenames ending in ".gz" or ".zst", but they can contain dots so handle arbitrary numbers of dots
		prefix := strings.Join(nameParts[0:len(nameParts)-1], ".")
		suffix := nameParts[len(nameParts)-1]
		filename = fmt.Sprintf("%s_%d.%s", prefix, oid, suffix)
	}
	return filename
}

func replaceContentInFilename(filename string, content int) string {
	if contentRE == nil {
		contentRE = regexp.MustCompile("gpbackup_([0-9]+)_")
	}
	return contentRE.ReplaceAllString(filename, fmt.Sprintf("gpbackup_%d_", content))
}

func getRestoreDataReader(fileToRead string, toc *toc.SegmentTOC, oidList []int) (*RestoreReader, error) {
	var readHandle io.Reader
	var seekHandle io.ReadSeeker
	var isSubset bool
	var err error = nil
	restoreReader := new(RestoreReader)

	if *pluginConfigFile != "" {
		readHandle, isSubset, err = startRestorePluginCommand(fileToRead, toc, oidList)
		if isSubset {
			// Reader that operates on subset data
			restoreReader.readerType = SUBSET
		} else {
			// Regular reader which doesn't support seek
			restoreReader.readerType = NONSEEKABLE
		}
	} else {
		if *isFiltered && !strings.HasSuffix(fileToRead, ".gz") && !strings.HasSuffix(fileToRead, ".zst") {
			// Seekable reader if backup is not compressed and filters are set
			seekHandle, err = os.Open(fileToRead)
			restoreReader.readerType = SEEKABLE
		} else {
			// Regular reader which doesn't support seek
			readHandle, err = os.Open(fileToRead)
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
	} else if strings.HasSuffix(fileToRead, ".gz") {
		gzipReader, err := gzip.NewReader(readHandle)
		if err != nil {
			// error logging handled by calling functions
			return nil, err
		}
		restoreReader.bufReader = bufio.NewReader(gzipReader)
	} else if strings.HasSuffix(fileToRead, ".zst") {
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

func startRestorePluginCommand(fileToRead string, toc *toc.SegmentTOC, oidList []int) (io.Reader, bool, error) {
	isSubset := false
	pluginConfig, err := utils.ReadPluginConfig(*pluginConfigFile)
	if err != nil {
		logError(fmt.Sprintf("Error encountered when reading plugin config: %v", err))
		return nil, false, err
	}
	cmdStr := ""
	if toc != nil && pluginConfig.CanRestoreSubset() && *isFiltered && !strings.HasSuffix(fileToRead, ".gz") && !strings.HasSuffix(fileToRead, ".zst") {
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
		cmdStr = fmt.Sprintf("%s restore_data_subset %s %s %s", pluginConfig.ExecutablePath, pluginConfig.ConfigPath, fileToRead, offsetsFile.Name())
		isSubset = true
	} else {
		cmdStr = fmt.Sprintf("%s restore_data %s %s", pluginConfig.ExecutablePath, pluginConfig.ConfigPath, fileToRead)
	}
	log(cmdStr)
	cmd := exec.Command("bash", "-c", cmdStr)

	readHandle, err := cmd.StdoutPipe()
	if err != nil {
		return nil, false, err
	}
	cmd.Stderr = &errBuf

	err = cmd.Start()
	return readHandle, isSubset, err
}
