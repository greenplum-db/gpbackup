package options

import (
	"github.com/greenplum-db/gp-common-go-libs/dbconn"
)

type DbValidator interface {
	ValidateInDatabase(tableList []string, conn *dbconn.DBConn)
}
