package utils

/*
 * This file contains structs and functions related to interacting with files
 * and directories, both locally and remotely over SSH.
 */

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
)

/*
 * Generic file/directory manipulation functions
 */

func MustPrintf(file io.Writer, s string, v ...interface{}) uint64 {
	bytesWritten, err := fmt.Fprintf(file, s, v...)
	if err != nil {
		gplog.Fatal(err, "Unable to write to file")
	}
	return uint64(bytesWritten)
}

func MustPrintln(file io.Writer, v ...interface{}) uint64 {
	bytesWritten, err := fmt.Fprintln(file, v...)
	if err != nil {
		gplog.Fatal(err, "Unable to write to file")
	}
	return uint64(bytesWritten)
}

/*
 * Structs and functions for file readers/writers that track bytes read/written
 */

type FileWithByteCount struct {
	Filename  string
	Writer    io.Writer
	File      *os.File
	ByteCount uint64
}

func NewFileWithByteCount(writer io.Writer) *FileWithByteCount {
	return &FileWithByteCount{"", writer, nil, 0}
}

func NewFileWithByteCountFromFile(filename string) *FileWithByteCount {
	file, err := OpenFileForWrite(filename)
	gplog.FatalOnError(err)
	return &FileWithByteCount{filename, file, file, 0}
}

func (file *FileWithByteCount) Close() {
	if file.File != nil {
		err := file.File.Sync()
		gplog.FatalOnError(err)
		err = file.File.Close()
		gplog.FatalOnError(err)
		if file.Filename != "" {
			err := os.Chmod(file.Filename, 0444)
			gplog.FatalOnError(err)
		}
	}
}

func (file *FileWithByteCount) MustPrintln(v ...interface{}) {
	bytesWritten, err := fmt.Fprintln(file.Writer, v...)
	gplog.FatalOnError(err, "Unable to write to file")
	file.ByteCount += uint64(bytesWritten)
}

func (file *FileWithByteCount) MustPrintf(s string, v ...interface{}) {
	bytesWritten, err := fmt.Fprintf(file.Writer, s, v...)
	gplog.FatalOnError(err, "Unable to write to file")
	file.ByteCount += uint64(bytesWritten)
}

func (file *FileWithByteCount) MustPrint(s string) {
	bytesWritten, err := fmt.Fprint(file.Writer, s)
	gplog.FatalOnError(err, "Unable to write to file")
	file.ByteCount += uint64(bytesWritten)
}

func CopyFile(src, dest string) error {
	info, err := os.Stat(src)
	if err == nil {
		var content []byte
		content, err = ioutil.ReadFile(src)
		if err != nil {
			gplog.Error(fmt.Sprintf("Error: %v, encountered when reading file: %s", err, src))
			return err
		}
		return ioutil.WriteFile(dest, content, info.Mode())
	}
	gplog.Error(fmt.Sprintf("Error: %v, encountered when trying to stat file: %s", err, src))
	return err
}
