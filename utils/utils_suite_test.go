package utils_test

import (
	"testing"

	sqlmock "gopkg.in/DATA-DOG/go-sqlmock.v1"

	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/utils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
)

var (
	connection *utils.DBConn
	mock       sqlmock.Sqlmock
	logger     *utils.Logger
	stdout     *gbytes.Buffer
	stderr     *gbytes.Buffer
	logfile    *gbytes.Buffer
	buffer     *gbytes.Buffer
	toc        *utils.TOC
	backupfile *utils.FileWithByteCount
)

func TestUtils(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "utils tests")
}

var _ = BeforeSuite(func() {
	connection, mock, logger, stdout, stderr, logfile = testutils.SetupTestEnvironment()
})

var _ = BeforeEach(func() {
	logger, stdout, stderr, logfile = testutils.SetupTestLogger()
	connection, mock = testutils.CreateAndConnectMockDB(1)
	utils.System = utils.InitializeSystemFunctions()
	buffer = gbytes.NewBuffer()
})
