package testutils

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/utils"

	"strconv"

	"github.com/jmoiron/sqlx"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
)

/*
 * Functions for setting up the test environment and mocking out global variables
 */

func CreateAndConnectMockDB() (*utils.DBConn, sqlmock.Sqlmock) {
	mockdb, mock := CreateMockDB()
	driver := TestDriver{DBExists: true, DB: mockdb, DBName: "testdb"}
	connection := utils.NewDBConn("testdb")
	connection.Driver = driver
	connection.Connect()
	return connection, mock
}

/*
 * This function creates a test logger and assigns it to both backup.logger and utils.logger,
 * so no assignment to those variables in the tests is necessary.  The logger and gbytes.buffers
 * are returned to allow checking for output written to those buffers during tests if desired.
 */
func SetupTestLogger() (*utils.Logger, *gbytes.Buffer, *gbytes.Buffer, *gbytes.Buffer) {
	testStdout := gbytes.NewBuffer()
	testStderr := gbytes.NewBuffer()
	testLogfile := gbytes.NewBuffer()
	testLogger := utils.NewLogger(testStdout, testStderr, testLogfile, utils.LOGINFO, "testProgram:testUser:testHost:000000-[%s]:-")
	backup.SetLogger(testLogger)
	utils.SetLogger(testLogger)
	return testLogger, testStdout, testStderr, testLogfile
}

func CreateMockDB() (*sqlx.DB, sqlmock.Sqlmock) {
	db, mock, err := sqlmock.New()
	mockdb := sqlx.NewDb(db, "sqlmock")
	if err != nil {
		Fail("Could not create mock database connection")
	}
	return mockdb, mock
}

func SetDefaultSegmentConfiguration() {
	utils.BaseDumpDir = utils.DefaultSegmentDir
	utils.DumpTimestamp = "20170101010101"
	configMaster := utils.QuerySegConfig{-1, "localhost", "/data/gpseg-1"}
	configSegOne := utils.QuerySegConfig{0, "localhost", "/data/gpseg0"}
	configSegTwo := utils.QuerySegConfig{1, "localhost", "/data/gpseg1"}
	utils.SetupSegmentConfiguration([]utils.QuerySegConfig{configMaster, configSegOne, configSegTwo})
}

/*
 * Wrapper functions aroung gomega operators for ease of use in tests
 */

func ExpectBegin(mock sqlmock.Sqlmock) {
	fakeResult := TestResult{Rows: 0}
	mock.ExpectBegin()
	mock.ExpectExec("SET TRANSACTION(.*)").WillReturnResult(fakeResult)
}

func ExpectRegexp(buffer *gbytes.Buffer, testStr string) {
	Expect(buffer).Should(gbytes.Say(regexp.QuoteMeta(testStr)))
}

func NotExpectRegexp(buffer *gbytes.Buffer, testStr string) {
	Expect(buffer).ShouldNot(gbytes.Say(regexp.QuoteMeta(testStr)))
}

func ExpectRegex(result string, testStr string) {
	Expect(result).Should(MatchRegexp(regexp.QuoteMeta(testStr)))
}

func ShouldPanicWithMessage(message string) {
	if r := recover(); r != nil {
		errorMessage := strings.TrimSpace(fmt.Sprintf("%v", r))
		if !strings.Contains(errorMessage, message) {
			Fail(fmt.Sprintf("Expected panic message '%s', got '%s'", message, errorMessage))
		}
	} else {
		Fail("Function did not panic as expected")
	}
}

func AssertQueryRuns(dbconn *utils.DBConn, query string) {
	_, err := dbconn.Exec(query)
	Expect(err).To(BeNil(), "%s", query)
}

func OidFromRelationName(dbconn *utils.DBConn, relname string) uint32 {
	oidQuery := fmt.Sprintf("SELECT '%s'::regclass::oid as string", relname)
	oidString := backup.SelectString(dbconn, oidQuery)
	oid, err := strconv.ParseUint(oidString, 10, 32)
	Expect(err).To(BeNil())
	return uint32(oid)
}

func OidFromFunctionName(dbconn *utils.DBConn, relname string) uint32 {
	oidQuery := fmt.Sprintf("SELECT '%s'::regproc::oid as string", relname)
	oidString := backup.SelectString(dbconn, oidQuery)
	oid, err := strconv.ParseUint(oidString, 10, 32)
	Expect(err).To(BeNil())
	return uint32(oid)
}
