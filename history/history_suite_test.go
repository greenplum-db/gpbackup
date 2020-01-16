package history_test

import (
	"testing"

	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/onsi/gomega/gbytes"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var (
	testStdout  *gbytes.Buffer
	testStderr  *gbytes.Buffer
	testLogfile *gbytes.Buffer
)

func TestBackupHistory(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "History Suite")
}

var _ = BeforeSuite(func() {
	testStdout, testStderr, testLogfile = testhelper.SetupTestLogger()
})
