package restore_test

import (
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/restore"
	"github.com/greenplum-db/gpbackup/utils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
)

var _ = Describe("wrapper tests", func() {
	Describe("SetMaxCsvLineLengthQuery", func() {
		It("returns nothing with a connection version of at least 6.0.0", func() {
			testhelper.SetDBVersion(connectionPool, "6.0.0")
			result := restore.SetMaxCsvLineLengthQuery(connectionPool)
			Expect(result).To(Equal(""))
		})
		It("sets gp_max_csv_line_length to 1GB when connection version is 4.X and at least 4.3.30.0", func() {
			testhelper.SetDBVersion(connectionPool, "4.3.30")
			result := restore.SetMaxCsvLineLengthQuery(connectionPool)
			Expect(result).To(Equal("SET gp_max_csv_line_length = 1073741824;\n"))
		})
		It("sets gp_max_csv_line_length to 1GB when connection version is 5.X and at least 5.11.0", func() {
			testhelper.SetDBVersion(connectionPool, "5.11.0")
			result := restore.SetMaxCsvLineLengthQuery(connectionPool)
			Expect(result).To(Equal("SET gp_max_csv_line_length = 1073741824;\n"))
		})
		It("sets gp_max_csv_line_length to 4MB when connection version is 4.X and before 4.3.30.0", func() {
			testhelper.SetDBVersion(connectionPool, "4.3.29")
			result := restore.SetMaxCsvLineLengthQuery(connectionPool)
			Expect(result).To(Equal("SET gp_max_csv_line_length = 4194304;\n"))
		})
		It("sets gp_max_csv_line_length to 4MB when connection version is 5.X and before 5.11.0", func() {
			testhelper.SetDBVersion(connectionPool, "5.10.999")
			result := restore.SetMaxCsvLineLengthQuery(connectionPool)
			Expect(result).To(Equal("SET gp_max_csv_line_length = 4194304;\n"))
		})
	})
	Describe("RestoreSchemas", func() {
		var (
			ignoredProgressBar utils.ProgressBar
			schemaArray        = []utils.StatementWithType{{Name: "foo", Statement: "create schema foo"}}
		)
		BeforeEach(func() {
			ignoredProgressBar = utils.NewProgressBar(1, "", utils.PB_NONE)
			ignoredProgressBar.Start()
		})
		AfterEach(func() {
			ignoredProgressBar.Finish()
		})
		It("logs nothing if there are no errors", func() {
			expectedResult := sqlmock.NewResult(0, 1)
			mock.ExpectExec("create schema foo").WillReturnResult(expectedResult)

			restore.RestoreSchemas(schemaArray, ignoredProgressBar)

			testhelper.NotExpectRegexp(logfile, "Schema foo already exists")
			testhelper.NotExpectRegexp(logfile, "Error encountered while creating schema foo")
		})
		It("logs warning if schema already exists", func() {
			expectedErr := errors.New(`schema "foo" already exists`)
			mock.ExpectExec("create schema foo").WillReturnError(expectedErr)

			restore.RestoreSchemas(schemaArray, ignoredProgressBar)

			testhelper.ExpectRegexp(logfile, "[WARNING]:-Schema foo already exists")
		})
		It("logs error if --on-error-continue is set", func() {
			cmdFlags.Set(utils.ON_ERROR_CONTINUE, "true")
			defer cmdFlags.Set(utils.ON_ERROR_CONTINUE, "false")
			expectedErr := errors.New("some other schema error")
			mock.ExpectExec("create schema foo").WillReturnError(expectedErr)

			restore.RestoreSchemas(schemaArray, ignoredProgressBar)

			expectedDebugMsg := "[DEBUG]:-Error encountered while creating schema foo: some other schema error"
			testhelper.ExpectRegexp(logfile, expectedDebugMsg)
			expectedErrMsg := "[ERROR]:-Encountered 1 errors during schema restore; see log file gbytes.Buffer for a list of errors."
			testhelper.ExpectRegexp(logfile, expectedErrMsg)
		})
		It("panics if create schema statement fails", func() {
			expectedErr := errors.New("some other schema error")
			mock.ExpectExec("create schema foo").WillReturnError(expectedErr)
			expectedPanicMsg := "[CRITICAL]:-some other schema error: Error encountered while creating schema foo"
			defer testhelper.ShouldPanicWithMessage(expectedPanicMsg)

			restore.RestoreSchemas(schemaArray, ignoredProgressBar)
		})
	})
})
