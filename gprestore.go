// +build gprestore

package main

import (
	"os"
	"regexp"

	. "github.com/greenplum-db/gpbackup/restore"
	"github.com/spf13/cobra"
)

func main() {
	var rootCmd = &cobra.Command{
		Use:   "gprestore",
		Short: "gprestore is the parallel restore utility for Greenplum",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			defer DoTeardown()
			DoValidation(cmd)
			DoSetup()
			DoRestore()
		}}
	rootCmd.SetArgs(handleSingleDashes(os.Args[1:]))
	DoInit(rootCmd)
	if err := rootCmd.Execute(); err != nil {
		os.Exit(2)
	}
}

func handleSingleDashes(args []string) []string {
	r, _ := regexp.Compile(`^-(\w{2,})`)
	var newArgs []string
	for _, arg := range args {
		newArg := r.ReplaceAllString(arg, "--$1")
		newArgs = append(newArgs, newArg)
	}
	return newArgs
}
