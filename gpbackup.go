// +build gpbackup

package main

import (
	. "github.com/greenplum-db/gpbackup/backup"
)

func main() {
	defer DoTeardown()
	DoInit()
	DoValidation()
	connection := DoSetup()
	defer func() {
		if connection != nil {
			connection.Close()
		}
	}()
	DoBackup(connection)
}
