package utils

/*
 * This file contains structs and functions related to interacting with files
 * and directories, both locally and remotely over SSH.
 */

import (
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gp-common-go-libs/iohelper"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/pkg/errors"
)

var (
	/* To be used in a Postgres query without being quoted, an identifier (schema or
	 * table) must begin with a lowercase letter or underscore, and may contain only
	 * lowercast letters, digits, and underscores.
	 */
	UnquotedIdentifier = regexp.MustCompile(`^([a-z_][a-z0-9_]*)$`)
	QuotedIdentifier   = regexp.MustCompile(`^"(.+)"$`)

	// Swap between double quotes and paired double quotes, and between literal whitespace characters and escape sequences
	ReplacerEscape   = strings.NewReplacer(`"`, `""`, `\`, `\\`)
	ReplacerUnescape = strings.NewReplacer(`""`, `"`, `\\`, `\`)
)

func SliceToQuotedString(slice []string) string {
	quotedStrings := make([]string, len(slice))
	for i, str := range slice {
		quotedStrings[i] = fmt.Sprintf("'%s'", EscapeSingleQuotes(str))
	}
	return strings.Join(quotedStrings, ",")
}

func EscapeSingleQuotes(str string) string {
	return strings.Replace(str, "'", "''", -1)
}

/*
 * Generic file/directory manipulation functions
 */

func CreateBackupLockFile(timestamp string) {
	timestampLockFile := fmt.Sprintf("/tmp/%s.lck", timestamp)
	_, err := operating.System.OpenFileWrite(timestampLockFile, os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		gplog.Fatal(errors.Errorf("A backup with timestamp %s is already in progress. Wait 1 second and try the backup again.", timestamp), "")
	}
}

func GetUserAndHostInfo() (string, string, string) {
	currentUser, _ := operating.System.CurrentUser()
	userName := currentUser.Username
	userDir := currentUser.HomeDir
	hostname, _ := operating.System.Hostname()
	return userName, userDir, hostname
}

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

func MustPrintBytes(file io.Writer, bytes []byte) uint64 {
	bytesWritten, err := file.Write(bytes)
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
	writer    io.Writer
	closer    io.WriteCloser
	ByteCount uint64
}

func NewFileWithByteCount(writer io.Writer) *FileWithByteCount {
	return &FileWithByteCount{"", writer, nil, 0}
}

func NewFileWithByteCountFromFile(filename string) *FileWithByteCount {
	file := iohelper.MustOpenFileForWriting(filename)
	return &FileWithByteCount{filename, file, file, 0}
}

func (file *FileWithByteCount) Close() {
	if file.closer != nil {
		_ = file.closer.Close()
		if file.Filename != "" {
			err := operating.System.Chmod(file.Filename, 0444)
			gplog.FatalOnError(err)
		}
	}
}

func (file *FileWithByteCount) MustPrintln(v ...interface{}) {
	bytesWritten, err := fmt.Fprintln(file.writer, v...)
	if err != nil {
		gplog.Fatal(err, "Unable to write to file")
	}
	file.ByteCount += uint64(bytesWritten)
}

func (file *FileWithByteCount) MustPrintf(s string, v ...interface{}) {
	bytesWritten, err := fmt.Fprintf(file.writer, s, v...)
	if err != nil {
		gplog.Fatal(err, "Unable to write to file")
	}
	file.ByteCount += uint64(bytesWritten)
}
