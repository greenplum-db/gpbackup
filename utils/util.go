package utils

/*
 * This file contains miscellaneous functions that are generally useful and
 * don't fit into any other file.
 */

import (
	"fmt"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpbackup/backup_filepath"
	"github.com/pkg/errors"
)

const MINIMUM_GPDB4_VERSION = "4.3.17"
const MINIMUM_GPDB5_VERSION = "5.1.0"

/*
 * General helper functions
 */

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

func ValidateFQNs(fqns []string) {
	unquotedIdentString := "[a-z_][a-z0-9_]*"
	validIdentString := fmt.Sprintf("(?:\"(.*)\"|(%s))", unquotedIdentString)
	validFormat := regexp.MustCompile(fmt.Sprintf(`^%s\.%s$`, validIdentString, validIdentString))
	var matches []string
	for _, fqn := range fqns {
		if matches = validFormat.FindStringSubmatch(fqn); len(matches) == 0 {
			gplog.Fatal(errors.Errorf(`Table %s is not correctly fully-qualified.  Please ensure that it is in the format schema.table, it is quoted appropriately, and it has no preceding or trailing whitespace.`, fqn), "")
		}
	}
}

func ValidateFullPath(path string) error {
	if len(path) > 0 && !(strings.HasPrefix(path, "/") || strings.HasPrefix(path, "~")) {
		return errors.Errorf("%s is not an absolute path.", path)
	}
	return nil
}

func InitializeSignalHandler(cleanupFunc func(bool), procDesc string, termFlag *bool) {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		for range signalChan {
			fmt.Println() // Add newline after "^C" is printed
			gplog.Warn("Received a termination signal, aborting %s", procDesc)
			*termFlag = true
			cleanupFunc(true)
			os.Exit(2)
		}
	}()
}

// TODO: Uniquely identify COPY commands in the multiple data file case to allow terminating sessions
func TerminateHangingCopySessions(connectionPool *dbconn.DBConn, fpInfo backup_filepath.FilePathInfo, appName string) {
	copyFileName := fpInfo.GetSegmentPipePathForCopyCommand()
	query := fmt.Sprintf(`SELECT
	pg_terminate_backend(procpid)
FROM pg_stat_activity
WHERE application_name = '%s'
AND current_query LIKE '%%%s%%'
AND procpid <> pg_backend_pid()`, appName, copyFileName)
	// We don't check the error as the connection may have finished or been previously terminated
	_, _ = connectionPool.Exec(query)
}

func ValidateGPDBVersionCompatibility(connectionPool *dbconn.DBConn) {
	if connectionPool.Version.Before(MINIMUM_GPDB4_VERSION) {
		gplog.Fatal(errors.Errorf(`GPDB version %s is not supported. Please upgrade to GPDB %s.0 or later.`, connectionPool.Version.VersionString, MINIMUM_GPDB4_VERSION), "")
	} else if connectionPool.Version.Is("5") && connectionPool.Version.Before(MINIMUM_GPDB5_VERSION) {
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
