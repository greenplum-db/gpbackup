package backup

/*
 * This file contains functions related to use of synchronized database snapshots.
 */

import (
	"fmt"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
)

const (
	SNAPSHOT_GPDB_MIN_VERSION = "6.21.0"
)

 // Export synchronized snapshot using connection 0 and return snapshotId value as string
 func GetSynchronizedSnapshot(connectionPool *dbconn.DBConn) (string, error) {
	 var snapshotId string
	 err := connectionPool.Get(&snapshotId, "SELECT pg_catalog.pg_export_snapshot()", 0)
	 if err != nil {
		 return "", err
	 }
	 gplog.Debug("Exported synchronized snapshot %s", snapshotId)
	 return snapshotId, nil
 }

// Set synchronized snapshot for connNum to snapshotId
func SetSynchronizedSnapshot(connectionPool *dbconn.DBConn, connNum int,  snapshotId string) error {
	if connectionPool.Tx[connNum] == nil {
		err := connectionPool.Begin(connNum) // Begins transaction in repeatable read
		if err != nil {
			return err
		}
	}
	_, err := connectionPool.Exec(fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", snapshotId), connNum)
	if err != nil {
		return err
	}
	gplog.Debug("Worker %d: Setting synchronized snapshot to %s", connNum, snapshotId)
	return nil
}
