package utils

/*
 * This file contains miscellaneous functions that are generally useful and
 * don't fit into any other file.
 */

import (
	"crypto/sha256"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	path "path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/greenplum-db/gpbackup/filepath"
	"github.com/pkg/errors"
	"golang.org/x/sys/unix"
)

const MINIMUM_GPDB5_VERSION = "5.1.0"

/*
 * General helper functions
 */

func CommandExists(cmd string) bool {
	_, err := exec.LookPath(cmd)
	return err == nil
}

func FileExists(filename string) bool {
	_, err := os.Stat(filename)
	return err == nil
}

func RemoveFileIfExists(filename string) error {
	baseFilename := path.Base(filename)
	if FileExists(filename) {
		gplog.Debug("File %s: Exists, Attempting Removal", baseFilename)
		err := os.Remove(filename)
		if err != nil {
			gplog.Error("File %s: Failed to remove. Error %s", baseFilename, err.Error())
			return err
		}
		gplog.Debug("File %s: Successfully removed", baseFilename)
	} else {
		gplog.Debug("File %s: Does not exist. No removal needed", baseFilename)
	}
	return nil
}

func OpenFileForWrite(filename string) (*os.File, error) {
	baseFilename := path.Base(filename)
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		gplog.Error("File %s: Failed to open. Error %s", baseFilename, err.Error())
	}
	return file, err
}

func WriteToFileAndMakeReadOnly(filename string, contents []byte) error {
	baseFilename := path.Base(filename)
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		gplog.Error("File %s: Failed to open. Error %s", baseFilename, err.Error())
		return err
	}

	_, err = file.Write(contents)
	if err != nil {
		gplog.Error("File %s: Could not write to file. Error %s", baseFilename, err.Error())
		return err
	}

	err = file.Chmod(0444)
	if err != nil {
		gplog.Error("File %s: Could not chmod. Error %s", baseFilename, err.Error())
		return err
	}

	err = file.Sync()
	if err != nil {
		gplog.Error("File %s: Could not sync. Error %s", baseFilename, err.Error())
		return err
	}

	return file.Close()
}

// Dollar-quoting logic is based on appendStringLiteralDQ() in pg_dump.
func DollarQuoteString(literal string) string {
	delimStr := "_XXXXXXX"
	quoteStr := ""
	for i := range delimStr {
		testStr := "$" + delimStr[0:i]
		if !strings.Contains(literal, testStr) {
			quoteStr = testStr + "$"
			break
		}
	}
	return quoteStr + literal + quoteStr
}

// This function assumes that all identifiers are already appropriately quoted
func MakeFQN(schema string, object string) string {
	return fmt.Sprintf("%s.%s", schema, object)
}

// We require users to include schema and table, separated by a dot.  So, check that there's at
// least one dot with stuff on either side of it.  The remainder of validation is done in the
// ValidateTablesExist routine
func ValidateFQNs(tableList []string) error {
	validFormat := regexp.MustCompile(`^.+\..+$`)
	for _, fqn := range tableList {
		if !validFormat.Match([]byte(fqn)) {
			return errors.Errorf(`Table "%s" is not correctly fully-qualified.  Please ensure table is in the format "schema.table".`, fqn)
		}
	}
	return nil
}

func ValidateFullPath(path string) error {
	if len(path) > 0 && !(strings.HasPrefix(path, "/") || strings.HasPrefix(path, "~")) {
		return errors.Errorf("%s is not an absolute path.", path)
	}
	return nil
}

// A description of compression levels for some compression type
type CompressionLevelsDescription struct {
	Min int
	Max int
}

func ValidateCompressionTypeAndLevel(compressionType string, compressionLevel int) error {
	compressionLevelsForType := map[string]CompressionLevelsDescription{
		"gzip": {Min: 1, Max: 9},
		"zstd": {Min: 1, Max: 19},
	}

	if levelsDescription, ok := compressionLevelsForType[compressionType]; ok {
		if compressionLevel < levelsDescription.Min || compressionLevel > levelsDescription.Max {
			return fmt.Errorf("compression type '%s' only allows compression levels between %d and %d, but the provided level is %d", compressionType, levelsDescription.Min, levelsDescription.Max, compressionLevel)
		}
	} else {
		return fmt.Errorf("unknown compression type '%s'", compressionType)
	}

	return nil
}

func InitializeSignalHandler(cleanupFunc func(bool), procDesc string, termFlag *bool) {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, unix.SIGINT, unix.SIGTERM)
	go func() {
		sig := <-signalChan
		fmt.Println() // Add newline after "^C" is printed
		switch sig {
		case unix.SIGINT:
			gplog.Warn("Received an interrupt signal, aborting %s", procDesc)
		case unix.SIGTERM:
			gplog.Warn("Received a termination signal, aborting %s", procDesc)
		}
		*termFlag = true
		cleanupFunc(true)
		os.Exit(2)
	}()
}

// TODO: Uniquely identify COPY commands in the multiple data file case to allow terminating sessions
func TerminateHangingCopySessions(connectionPool *dbconn.DBConn, fpInfo filepath.FilePathInfo, appName string) {
	var query string
	copyFileName := fpInfo.GetSegmentPipePathForCopyCommand()
	if connectionPool.Version.Before("6") {
		query = fmt.Sprintf(`SELECT
		pg_terminate_backend(procpid)
	FROM pg_stat_activity
	WHERE application_name = '%s'
	AND current_query LIKE '%%%s%%'
	AND procpid <> pg_backend_pid()`, appName, copyFileName)
	} else {
		query = fmt.Sprintf(`SELECT
		pg_terminate_backend(pid)
	FROM pg_stat_activity
	WHERE application_name = '%s'
	AND query LIKE '%%%s%%'
	AND pid <> pg_backend_pid()`, appName, copyFileName)
	}
	// We don't check the error as the connection may have finished or been previously terminated
	_, _ = connectionPool.Exec(query)
}

func ValidateGPDBVersionCompatibility(connectionPool *dbconn.DBConn) {
	if connectionPool.Version.Before(MINIMUM_GPDB5_VERSION) {
		gplog.Fatal(errors.Errorf(`GPDB version %s is not supported. Please upgrade to GPDB %s or later.`, connectionPool.Version.VersionString, MINIMUM_GPDB5_VERSION), "")
	}
}

func LogExecutionTime(start time.Time, name string) {
	elapsed := time.Since(start)
	gplog.Debug(fmt.Sprintf("%s took %s", name, elapsed))
}

func Exists(slice []string, val string) bool {
	for _, item := range slice {
		if item == val {
			return true
		}
	}
	return false
}

func SchemaIsExcludedByUser(inSchemasUserInput []string, exSchemasUserInput []string, schemaName string) bool {
	included := Exists(inSchemasUserInput, schemaName) || len(inSchemasUserInput) == 0
	excluded := Exists(exSchemasUserInput, schemaName)
	return excluded || !included
}

func RelationIsExcludedByUser(inRelationsUserInput []string, exRelationsUserInput []string, tableFQN string) bool {
	included := Exists(inRelationsUserInput, tableFQN) || len(inRelationsUserInput) == 0
	excluded := Exists(exRelationsUserInput, tableFQN)
	return excluded || !included
}

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

func UnEscapeDoubleQuotes(str string) string {
	return strings.Replace(str, "\"\"", "\"", -1)
}

func GetFileHash(filename string) ([32]byte, error) {
	contents, err := operating.System.ReadFile(filename)
	if err != nil {
		gplog.Error("Failed to read contents of file %s: %v", filename, err)
		return [32]byte{}, err
	}
	filehash := sha256.Sum256(contents)
	if err != nil {
		gplog.Error("Failed to hash contents of file %s: %v", filename, err)
		return [32]byte{}, err
	}
	return filehash, nil
}
