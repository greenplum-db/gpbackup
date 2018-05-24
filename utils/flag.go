package utils

/*
 * This file contains functions and structs relating to flag parsing.
 */

import (
	"regexp"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
)

/*
 * Functions for validating whether flags are set and in what combination
 */

// At most one of the flags passed to this function may be set
func CheckExclusiveFlags(flags *pflag.FlagSet, flagNames ...string) {
	numSet := 0
	for _, name := range flagNames {
		if flags.Changed(name) {
			numSet++
		}
	}
	if numSet > 1 {
		gplog.Fatal(errors.Errorf("The following flags may not be specified together: %s", strings.Join(flagNames, ", ")), "")
	}
}

/*
 * Functions for validating flag values
 */

/*
 * Restoring a future-dated backup is allowed (e.g. the backup was taken in a
 * different time zone that is ahead of the restore time zone), so only check
 * format, not whether the timestamp is earlier than the current time.
 */
func IsValidTimestamp(timestamp string) bool {
	timestampFormat := regexp.MustCompile(`^([0-9]{14})$`)
	return timestampFormat.MatchString(timestamp)
}

/*
 * Convert arguments that contain a single dash to double dashes for backward
 * compatibility.
 */
func HandleSingleDashes(args []string) []string {
	r, _ := regexp.Compile(`^-(\w{2,})`)
	var newArgs []string
	for _, arg := range args {
		newArg := r.ReplaceAllString(arg, "--$1")
		newArgs = append(newArgs, newArg)
	}
	return newArgs
}
