package helper_test

import (
	"testing"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/utils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
)

var (
	logger  *gplog.Logger
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
	logger, stdout, stderr, logfile = testutils.SetupTestLogger()
	operating.System = operating.InitializeSystemFunctions()
})
