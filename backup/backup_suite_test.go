package backup_test

/*
 * This file contains integration tests for gpbackup as a whole, rather than
 * tests relating to functions in any particular file.
 */

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/utils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
	"github.com/spf13/pflag"
)

var (
	connectionPool *dbconn.DBConn
	defaultConnNum = 0
	mock           sqlmock.Sqlmock
	stdout         *gbytes.Buffer
	stderr         *gbytes.Buffer
	logfile        *gbytes.Buffer
	buffer         = gbytes.NewBuffer()
	toc            *utils.TOC
	backupfile     *utils.FileWithByteCount
)

func TestBackup(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "backup tests")
}

var cmdFlags *pflag.FlagSet

var _ = BeforeEach(func() {
	cmdFlags = pflag.NewFlagSet("gpbackup", pflag.ExitOnError)

	backup.SetFlagDefaults(cmdFlags)

	backup.SetCmdFlags(cmdFlags)

	utils.SetPipeThroughProgram(utils.PipeThroughProgram{})

	connectionPool, mock, stdout, stderr, logfile = testutils.SetupTestEnvironment()
	backup.SetConnection(connectionPool)
	backup.InitializeMetadataParams(connectionPool)
	buffer = gbytes.NewBuffer()
})
