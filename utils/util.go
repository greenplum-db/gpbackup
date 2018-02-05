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

	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/pkg/errors"
)

func CheckError(err error) {
	if err != nil {
		logger.Fatal(err, "")
	}
}

/*
 * General helper functions
 */

func CurrentTimestamp() string {
	return operating.System.Now().Format("20060102150405")
}

func TryEnv(varname string, defval string) string {
	val := operating.System.Getenv(varname)
	if val == "" {
		return defval
	}
	return val
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

func ValidateFQNs(fqns []string) {
	unquotedIdentString := "[a-z_][a-z0-9_]*"
	validIdentString := fmt.Sprintf("(?:\"(.*)\"|(%s))", unquotedIdentString)
	validFormat := regexp.MustCompile(fmt.Sprintf(`^%s\.%s$`, validIdentString, validIdentString))
	var matches []string
	for _, fqn := range fqns {
		if matches = validFormat.FindStringSubmatch(fqn); len(matches) == 0 {
			logger.Fatal(errors.Errorf(`Table %s is not correctly fully-qualified.  Please ensure that it is in the format schema.table, it is quoted appropriately, and it has no preceding or trailing whitespace.`, fqn), "")
		}
	}
}

func InitializeSignalHandler(cleanupFunc func(), procDesc string, termFlag *bool) {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		for range signalChan {
			fmt.Println() // Add newline after "^C" is printed
			logger.Warn("Received a termination signal, aborting %s", procDesc)
			*termFlag = true
			cleanupFunc()
			os.Exit(2)
		}
	}()
}
