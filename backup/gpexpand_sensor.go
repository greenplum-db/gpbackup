package backup

import (
	"os"
	"path/filepath"

	"github.com/pkg/errors"

	"github.com/blang/vfs"
	"github.com/greenplum-db/gp-common-go-libs/dbconn"
)

const MasterDataDirQuery = `select datadir from gp_segment_configuration where content=-1 and role='p'`
const GpexpandTemporaryTableStatusQuery = `SELECT status FROM gpexpand.status LIMIT 1`

const GpexpandStatusFilename = "gpexpand.status"

type GpexpandSensor struct {
	fs           vfs.Filesystem
	postgresConn *dbconn.DBConn
}

func NewGpexpandSensor(myfs vfs.Filesystem, conn *dbconn.DBConn) GpexpandSensor {
	return GpexpandSensor{
		fs:           myfs,
		postgresConn: conn,
	}
}

func (sensor GpexpandSensor) IsGpexpandRunning() (bool, error) {
	err := validateConnection(sensor.postgresConn)
	if err != nil {
		return false, err
	}
	masterDataDir, err := dbconn.SelectString(sensor.postgresConn, MasterDataDirQuery)
	if err != nil {
		return false, err
	}

	_, err = sensor.fs.Stat(filepath.Join(masterDataDir, GpexpandStatusFilename))
	// error has 3 possible states:
	if err == nil {
		// file exists, so gpexpand is running
		return true, nil
	}
	if os.IsNotExist(err) {
		// file not present means gpexpand is not in "phase 1".
		// now check whether the postgres database has evidence of a "phase 2" status for gpexpand,
		// by querying a temporary status table
		_, err = dbconn.SelectString(sensor.postgresConn, GpexpandTemporaryTableStatusQuery)
		if err != nil {
			// Important: not a real error, just means that gpexpand is not in phase 2
			return false, nil
		}
		// if there is ANY successful return from this temporary table, it means gpexpand is running
		return true, nil
	} else {
		// Stat command returned a "real" error
		return false, err
	}
}

func validateConnection(conn *dbconn.DBConn) error {
	if conn.DBName != "postgres" {
		return errors.New("gpexpand sensor requires a connection to the postgres database")
	}
	if conn.Version.Before("6") {
		return errors.New("gpexpand sensor requires a connection to Greenplum version >= 6")
	}
	return nil
}
