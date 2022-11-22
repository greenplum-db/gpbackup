package helper

import (
	"bufio"
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
 * Backup specific functions
 */

func doBackupAgent() error {
	var lastRead uint64
	var (
		pipeWriter BackupPipeWriterCloser
		writeCmd   *exec.Cmd
	)
	tocfile := &toc.SegmentTOC{}
	tocfile.DataEntries = make(map[uint]toc.SegmentDataEntry)

	oidList, err := getOidListFromFile(*oidFile)
	if err != nil {
		// error logging handled in getOidListFromFile
		return err
	}

	preloadCreatedPipes(oidList, *copyQueue)
	var currentPipe string
	/*
	 * It is important that we create the reader before creating the writer
	 * so that we establish a connection to the first pipe (created by gpbackup)
	 * and properly clean it up if an error occurs while creating the writer.
	 */
	for i, oid := range oidList {
		currentPipe = fmt.Sprintf("%s_%d", *pipeFile, oidList[i])
		if wasTerminated {
			logError("Terminated due to user request")
			return errors.New("Terminated due to user request")
		}
		if i < len(oidList)-*copyQueue {
			nextPipeToCreate := fmt.Sprintf("%s_%d", *pipeFile, oidList[i+*copyQueue])
			log(fmt.Sprintf("Oid %d: Creating pipe %s\n", oidList[i+*copyQueue], nextPipeToCreate))
			err := createPipe(nextPipeToCreate)
			if err != nil {
				logError(fmt.Sprintf("Oid %d: Failed to create pipe %s\n", oidList[i+*copyQueue], nextPipeToCreate))
				return err
			}
		}

		log(fmt.Sprintf("Oid %d: Opening pipe %s", oid, currentPipe))
		reader, readHandle, err := getBackupPipeReader(currentPipe)
		if err != nil {
			logError(fmt.Sprintf("Oid %d: Error encountered getting backup pipe reader: %v", oid, err))
			return err
		}
		if i == 0 {
			pipeWriter, writeCmd, err = getBackupPipeWriter()
			if err != nil {
				logError(fmt.Sprintf("Oid %d: Error encountered getting backup pipe writer: %v", oid, err))
				return err
			}
		}

		log(fmt.Sprintf("Oid %d: Backing up table with pipe %s", oid, currentPipe))
		numBytes, err := io.Copy(pipeWriter, reader)
		if err != nil {
			logError(fmt.Sprintf("Oid %d: Error encountered copying bytes from pipeWriter to reader: %v", oid, err))
			return errors.Wrap(err, strings.Trim(errBuf.String(), "\x00"))
		}
		log(fmt.Sprintf("Oid %d: Read %d bytes\n", oid, numBytes))

		lastProcessed := lastRead + uint64(numBytes)
		tocfile.AddSegmentDataEntry(uint(oid), lastRead, lastProcessed)
		lastRead = lastProcessed

		_ = readHandle.Close()
		log(fmt.Sprintf("Oid %d: Deleting pipe: %s\n", oid, currentPipe))
		deletePipe(currentPipe)
	}

	_ = pipeWriter.Close()
	if *pluginConfigFile != "" {
		/*
		 * When using a plugin, the agent may take longer to finish than the
		 * main gpbackup process. We either write the TOC file if the agent finishes
		 * successfully or write an error file if it has an error after the COPYs have
		 * finished. We then wait on the gpbackup side until one of those files is
		 * written to verify the agent completed.
		 */
		log("Uploading remaining data to plugin destination")
		err := writeCmd.Wait()
		if err != nil {
			logError(fmt.Sprintf("Error encountered writing either TOC file or error file: %v", err))
			return errors.Wrap(err, strings.Trim(errBuf.String(), "\x00"))
		}
	}
	err = tocfile.WriteToFileAndMakeReadOnly(*tocFile)
	if err != nil {
		// error logging handled in util.go
		return err
	}
	log("Finished writing segment TOC")
	return nil
}

func getBackupPipeReader(currentPipe string) (io.Reader, io.ReadCloser, error) {
	readHandle, err := os.OpenFile(currentPipe, os.O_RDONLY, os.ModeNamedPipe)
	if err != nil {
		// error logging handled by calling functions
		return nil, nil, err
	}
	// This is a workaround for https://github.com/golang/go/issues/24164.
	// Once this bug is fixed, the call to Fd() can be removed
	readHandle.Fd()
	reader := bufio.NewReader(readHandle)
	return reader, readHandle, nil
}

func getBackupPipeWriter() (pipe BackupPipeWriterCloser, writeCmd *exec.Cmd, err error) {
	var writeHandle io.WriteCloser
	if *pluginConfigFile != "" {
		writeCmd, writeHandle, err = startBackupPluginCommand()
	} else {
		writeHandle, err = os.Create(*dataFile)
	}
	if err != nil {
		// error logging handled by calling functions
		return nil, nil, err
	}

	if *compressionLevel == 0 {
		pipe = NewCommonBackupPipeWriterCloser(writeHandle)
		return
	}

	if *compressionType == "gzip" {
		pipe, err = NewGZipBackupPipeWriterCloser(writeHandle, *compressionLevel)
		return
	}
	if *compressionType == "zstd" {
		pipe, err = NewZSTDBackupPipeWriterCloser(writeHandle, *compressionLevel)
		return
	}

	writeHandle.Close()
	// error logging handled by calling functions
	return nil, nil, fmt.Errorf("unknown compression type '%s' (compression level %d)", *compressionType, *compressionLevel)
}

func startBackupPluginCommand() (*exec.Cmd, io.WriteCloser, error) {
	pluginConfig, err := utils.ReadPluginConfig(*pluginConfigFile)
	if err != nil {
		// error logging handled by calling functions
		return nil, nil, err
	}
	cmdStr := fmt.Sprintf("%s backup_data %s %s", pluginConfig.ExecutablePath, pluginConfig.ConfigPath, *dataFile)
	writeCmd := exec.Command("bash", "-c", cmdStr)

	writeHandle, err := writeCmd.StdinPipe()
	if err != nil {
		// error logging handled by calling functions
		return nil, nil, err
	}
	writeCmd.Stderr = &errBuf
	err = writeCmd.Start()
	if err != nil {
		// error logging handled by calling functions
		return nil, nil, err
	}
	return writeCmd, writeHandle, nil
}
