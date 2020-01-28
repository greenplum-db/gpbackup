package utils_test

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gbytes"
)

var (
	connectionPool *dbconn.DBConn
	mock           sqlmock.Sqlmock
	stdout         *Buffer
	logfile        *Buffer
	buffer         *Buffer
)

func TestUtils(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "utils tests")
}

var _ = BeforeEach(func() {
	connectionPool, mock, stdout, _, logfile = testhelper.SetupTestEnvironment()
	buffer = NewBuffer()
})
