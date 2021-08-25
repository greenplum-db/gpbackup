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

	oidList, err := getOidListFromFile()
	if err != nil {
		return err
	}

	currentPipe = fmt.Sprintf("%s_%d", *pipeFile, oidList[0])
	/*
	 * It is important that we create the reader before creating the writer
	 * so that we establish a connection to the first pipe (created by gpbackup)
	 * and properly clean it up if an error occurs while creating the writer.
	 */
	for i, oid := range oidList {
		if wasTerminated {
			return errors.New("Terminated due to user request")
		}
		if i < len(oidList)-1 {
			log(fmt.Sprintf("Creating pipe for oid %d\n", oidList[i+1]))
			nextPipe = fmt.Sprintf("%s_%d", *pipeFile, oidList[i+1])
			err := createPipe(nextPipe)
			if err != nil {
				return err
			}
		}

		log(fmt.Sprintf("Opening pipe for oid %d\n", oid))
		reader, readHandle, err := getBackupPipeReader(currentPipe)
		if err != nil {
			return err
		}
		if i == 0 {
			pipeWriter, writeCmd, err = getBackupPipeWriter()
			if err != nil {
				return err
			}
		}

		log(fmt.Sprintf("Backing up table with oid %d\n", oid))
		numBytes, err := io.Copy(pipeWriter, reader)
		if err != nil {
			return errors.Wrap(err, strings.Trim(errBuf.String(), "\x00"))
		}
		log(fmt.Sprintf("Read %d bytes\n", numBytes))

		lastProcessed := lastRead + uint64(numBytes)
		tocfile.AddSegmentDataEntry(uint(oid), lastRead, lastProcessed)
		lastRead = lastProcessed

		lastPipe = currentPipe
		currentPipe = nextPipe
		_ = readHandle.Close()
		err = utils.RemoveFileIfExists(lastPipe)
		if err != nil {
			return err
		}
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
			return errors.Wrap(err, strings.Trim(errBuf.String(), "\x00"))
		}
	}
	err = tocfile.WriteToFileAndMakeReadOnly(*tocFile)
	if err != nil {
		return err
	}
	log("Finished writing segment TOC")
	return nil
}

func getBackupPipeReader(currentPipe string) (io.Reader, io.ReadCloser, error) {
	readHandle, err := os.OpenFile(currentPipe, os.O_RDONLY, os.ModeNamedPipe)
	if err != nil {
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
	return nil, nil, fmt.Errorf("unknown compression type '%s' (compression level %d)", *compressionType, *compressionLevel)
}

func startBackupPluginCommand() (*exec.Cmd, io.WriteCloser, error) {
	pluginConfig, err := utils.ReadPluginConfig(*pluginConfigFile)
	if err != nil {
		return nil, nil, err
	}
	cmdStr := fmt.Sprintf("%s backup_data %s %s", pluginConfig.ExecutablePath, pluginConfig.ConfigPath, *dataFile)
	writeCmd := exec.Command("bash", "-c", cmdStr)

	writeHandle, err := writeCmd.StdinPipe()
	if err != nil {
		return nil, nil, err
	}
	writeCmd.Stderr = &errBuf
	err = writeCmd.Start()
	if err != nil {
		return nil, nil, err
	}
	return writeCmd, writeHandle, nil
}
