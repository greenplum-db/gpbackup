package utils

/*
 * This file contains structs and functions related to connecting to a database
 * and executing queries.
 */

import (
	"fmt"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	_ "github.com/lib/pq" // Need driver for postgres
	"github.com/pkg/errors"
)

const MINIMUM_GPDB4_VERSION = "4.3.17"
const MINIMUM_GPDB5_VERSION = "5.1.0"

func GetDBSize(connection *dbconn.DBConn) string {
	size := struct{ DBSize string }{}
	sizeQuery := fmt.Sprintf("SELECT pg_size_pretty(sodddatsize) as dbsize FROM gp_toolkit.gp_size_of_database WHERE sodddatname=E'%s'", dbconn.EscapeConnectionParam(connection.DBName))
	err := connection.Get(&size, sizeQuery)
	CheckError(err)
	return size.DBSize
}

func SetDatabaseVersion(connection *dbconn.DBConn) {
	connection.Version.Initialize(connection)
	validateGPDBVersionCompatibility(connection)
}

func validateGPDBVersionCompatibility(connection *dbconn.DBConn) {
	if connection.Version.Before(MINIMUM_GPDB4_VERSION) {
		logger.Fatal(errors.Errorf(`GPDB version %s is not supported. Please upgrade to GPDB %s.0 or later.`, connection.Version.VersionString, MINIMUM_GPDB4_VERSION), "")
	} else if connection.Version.Is("5") && connection.Version.Before(MINIMUM_GPDB5_VERSION) {
		logger.Fatal(errors.Errorf(`GPDB version %s is not supported. Please upgrade to GPDB %s or later.`, connection.Version.VersionString, MINIMUM_GPDB5_VERSION), "")
	}
}

// TODO: Uniquely identify COPY commands in the multiple data file case to allow terminating sessions
func TerminateHangingCopySessions(connection *dbconn.DBConn, globalFPInfo FilePathInfo, appName string) {
	copyFileName := fmt.Sprintf("%s_%d", globalFPInfo.GetSegmentPipePathForCopyCommand(), globalFPInfo.PID)
	query := fmt.Sprintf(`SELECT
	pg_terminate_backend(procpid)
FROM pg_stat_activity
WHERE application_name = '%s'
AND current_query LIKE '%%%s%%'
AND procpid <> pg_backend_pid()`, appName, copyFileName)
	connection.MustExec(query)
}
