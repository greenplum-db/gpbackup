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
	"os/user"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gp-common-go-libs/operating"
)

func UnquoteIdent(ident string) string {
	if len(ident) <= 1 {
		return ident
	}

	if ident[0] == '"' && ident[len(ident)-1] == '"' {
		ident = ident[1 : len(ident)-1]
		unescape := strings.NewReplacer(`""`, `"`)
		ident = unescape.Replace(ident)
	}

	return ident
}

func QuoteIdent(connectionPool *dbconn.DBConn, ident string) string {
	return dbconn.MustSelectString(connectionPool, fmt.Sprintf(`SELECT quote_ident('%s')`, EscapeSingleQuotes(ident)))
}

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

func GetUserAndHostInfo() (string, string, string) {
	currentUser, _ := user.Current()
	userName := currentUser.Username
	userDir := currentUser.HomeDir
	hostname, _ := os.Hostname()
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
			err := operating.System.Chmod(file.Filename, 0444)
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
			return err
		}

		return ioutil.WriteFile(dest, content, info.Mode())
	}

	return err
}
