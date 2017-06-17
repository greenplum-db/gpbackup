package backup_test

import (
	"database/sql/driver"

	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
)

var _ = Describe("backup/queries tests", func() {
	var connection *utils.DBConn
	var mock sqlmock.Sqlmock
	BeforeEach(func() {
		connection, mock = testutils.CreateAndConnectMockDB()
		testutils.SetupTestLogger()
	})

	Describe("SelectString", func() {
		header := []string{"string"}
		rowOne := []driver.Value{"one"}
		rowTwo := []driver.Value{"two"}

		It("returns a single string if the query selects a single string", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(rowOne...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			result := backup.SelectString(connection, "SELECT foo FROM bar")
			Expect(result).To(Equal("one"))
		})
		It("returns an empty string if the query selects no strings", func() {
			fakeResult := sqlmock.NewRows(header)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			result := backup.SelectString(connection, "SELECT foo FROM bar")
			Expect(result).To(Equal(""))
		})
		It("panics if the query selects multiple strings", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(rowOne...).AddRow(rowTwo...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			defer testutils.ShouldPanicWithMessage("Too many rows returned from query: got 2 rows, expected 1 row")
			backup.SelectString(connection, "SELECT foo FROM bar")
		})
	})
	Describe("SelectStringSlice", func() {
		header := []string{"string"}
		rowOne := []driver.Value{"one"}
		rowTwo := []driver.Value{"two"}

		It("returns a slice containing a single string if the query selects a single string", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(rowOne...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results := backup.SelectStringSlice(connection, "SELECT foo FROM bar")
			Expect(len(results)).To(Equal(1))
			Expect(results[0]).To(Equal("one"))
		})
		It("returns an empty slice if the query selects no strings", func() {
			fakeResult := sqlmock.NewRows(header)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results := backup.SelectStringSlice(connection, "SELECT foo FROM bar")
			Expect(len(results)).To(Equal(0))
		})
		It("returns a slice containing multiple strings if the query selects multiple strings", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(rowOne...).AddRow(rowTwo...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results := backup.SelectStringSlice(connection, "SELECT foo FROM bar")
			Expect(len(results)).To(Equal(2))
			Expect(results[0]).To(Equal("one"))
			Expect(results[1]).To(Equal("two"))
		})
	})
})
