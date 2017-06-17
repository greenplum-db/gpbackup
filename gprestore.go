// +build gprestore

package main

import (
	. "github.com/greenplum-db/gpbackup/restore"
)

func main() {
	defer DoTeardown()
	DoInit()
	DoValidation()
	DoSetup()
	DoRestore()
}
