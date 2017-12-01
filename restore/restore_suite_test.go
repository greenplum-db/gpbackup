package restore_test

/*
 * This file contains integration tests for gprestore as a whole, rather than
 * tests relating to functions in any particular file.
 */

import (
	"os/exec"
	"testing"

	sqlmock "gopkg.in/DATA-DOG/go-sqlmock.v1"

	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/utils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
	"github.com/onsi/gomega/gexec"
)

var (
	connection    *utils.DBConn
	mock          sqlmock.Sqlmock
	logger        *utils.Logger
	stdout        *gbytes.Buffer
	stderr        *gbytes.Buffer
	logfile       *gbytes.Buffer
	buffer        *gbytes.Buffer
	gprestorePath = ""
)

/* This function is a helper function to execute gprestore and return a session
 * to allow checking its output.
 */
func gprestore() *gexec.Session {
	command := exec.Command(gprestorePath, "-timestamp", "20170101010101")
	session, err := gexec.Start(command, GinkgoWriter, GinkgoWriter)
	Expect(err).ShouldNot(HaveOccurred())
	<-session.Exited
	return session
}

func TestRestore(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "restore tests")
}

var _ = BeforeSuite(func() {
	connection, mock, logger, stdout, stderr, logfile = testutils.SetupTestEnvironment()
	buffer = gbytes.NewBuffer()
})

var _ = BeforeEach(func() {
	connection, mock = testutils.CreateAndConnectMockDB(1)
})
