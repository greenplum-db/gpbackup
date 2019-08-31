package backup_filepath_test

import (
	"testing"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/testutils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

var (
	connectionPool *dbconn.DBConn
	mock           sqlmock.Sqlmock
	stdout         *gbytes.Buffer
	stderr         *gbytes.Buffer
	logfile        *gbytes.Buffer
	buffer         *gbytes.Buffer
)

func TestBackupFilepath(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "BackupFilepath Suite")
}

var _ = BeforeSuite(func() {
	connectionPool, mock, stdout, stderr, logfile = testutils.SetupTestEnvironment()
})

var _ = BeforeEach(func() {
	connectionPool, mock, stdout, stderr, logfile = testhelper.SetupTestEnvironment()
	operating.System = operating.InitializeSystemFunctions()
	buffer = gbytes.NewBuffer()
})
