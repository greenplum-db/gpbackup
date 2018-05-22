package utils

/*
 * This file contains functions and structs relating to flag parsing.
 */

import (
	"regexp"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

/*
 * Functions for validating whether flags are set and in what combination
 */

// At most one of the flags passed to this function may be set
func CheckExclusiveFlags(cmd *cobra.Command, flagNames ...string) {
	numSet := 0
	for _, name := range flagNames {
		if cmd.Flags().Changed(name) {
			numSet++
		}
	}
	if numSet > 1 {
		gplog.Fatal(errors.Errorf("The following flags may not be specified together: %s", strings.Join(flagNames, ", ")), "")
	}
}

type ArrayFlags []string

func (i *ArrayFlags) String() string {
	return strings.Join(*i, ", ")
}

func (i *ArrayFlags) Set(value string) error {
	*i = append(*i, value)
	return nil
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
