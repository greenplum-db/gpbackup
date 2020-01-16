package report_test

import (
	"testing"

	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/onsi/gomega/gbytes"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var (
	stdout       *gbytes.Buffer
	logfile      *gbytes.Buffer
	buffer       *gbytes.Buffer
)

func TestReport(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Report Suite")
}

var _ = BeforeEach(func() {
	stdout, buffer, logfile = testhelper.SetupTestLogger()
})
