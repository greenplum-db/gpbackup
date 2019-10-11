// +build gprestore

package main

import (
	"os"

	"github.com/greenplum-db/gpbackup/utils"
	"github.com/spf13/cobra"

	. "github.com/greenplum-db/gpbackup/restore"
)

func main() {
	var rootCmd = &cobra.Command{
		Use:     "gprestore",
		Short:   "gprestore is the parallel restore utility for Greenplum",
		Args:    cobra.NoArgs,
		Version: GetVersion(),
		Run: func(cmd *cobra.Command, args []string) {
			defer DoTeardown()
			DoValidation(cmd)
			DoSetup()
			DoRestore()
		}}
	rootCmd.SetArgs(utils.HandleSingleDashes(os.Args[1:]))
	DoInit(rootCmd)
	if err := rootCmd.Execute(); err != nil {
		os.Exit(2)
	}
}
