package backup_test

/*
 * This file contains integration tests for gpbackup as a whole, rather than
 * tests relating to functions in any particular file.
 */

import (
	"testing"

	sqlmock "gopkg.in/DATA-DOG/go-sqlmock.v1"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/utils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
)

var (
	connection   *dbconn.DBConn
	mock         sqlmock.Sqlmock
	stdout       *gbytes.Buffer
	stderr       *gbytes.Buffer
	logfile      *gbytes.Buffer
	buffer       = gbytes.NewBuffer()
	gpbackupPath = ""
	toc          *utils.TOC
	backupfile   *utils.FileWithByteCount
)

func TestBackup(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "backup tests")
}

var _ = BeforeSuite(func() {
	connection, mock, stdout, stderr, logfile = testutils.SetupTestEnvironment()
	backup.SetIncludeSchemas([]string{})
	backup.SetExcludeSchemas([]string{})
	backup.SetIncludeTables([]string{})
	backup.SetExcludeTables([]string{})
})

var _ = BeforeEach(func() {
	buffer = gbytes.NewBuffer()
	connection, mock = testutils.CreateAndConnectMockDB(1)
})
