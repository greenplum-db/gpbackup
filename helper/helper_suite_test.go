package helper_test

import (
	"testing"

	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/utils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
)

var (
	stdout  *gbytes.Buffer
	stderr  *gbytes.Buffer
	logfile *gbytes.Buffer
	toc     *utils.TOC
)

func TestUtils(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "utils tests")
}

var _ = BeforeEach(func() {
	stdout, stderr, logfile = testhelper.SetupTestLogger()
	operating.System = operating.InitializeSystemFunctions()
})
