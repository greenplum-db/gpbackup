package utils_test

import (
	"database/sql/driver"
	"fmt"
	"os"
	"time"

	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/utils"

	"github.com/jmoiron/sqlx"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
)

var _ = Describe("utils/db tests", func() {
	BeforeEach(func() {
		utils.System.Now = func() time.Time { return time.Date(2017, time.January, 1, 1, 1, 1, 1, time.Local) }
	})
	Describe("NewDBConn", func() {
		It("gets the DBName from the dbname flag if it is set", func() {
			connection = utils.NewDBConn("testdb")
			Expect(connection.DBName).To(Equal("testdb"))
		})
		It("fails if no database is given with the dbname flag", func() {
			defer testutils.ShouldPanicWithMessage("No database provided")
			connection = utils.NewDBConn("")
		})
	})
	Describe("DBConn.Connect", func() {
		var mockdb *sqlx.DB
		BeforeEach(func() {
			mockdb, mock = testutils.CreateMockDB()
			if connection != nil {
				connection.Close()
			}
			connection = utils.NewDBConn("testdb")
			connection.Driver = testutils.TestDriver{DB: mockdb, User: "testrole"}
		})
		AfterEach(func() {
			if connection != nil {
				connection.Close()
			}
		})
		It("makes a single connection successfully if the database exists", func() {
			connection.Connect(1)
			Expect(connection.DBName).To(Equal("testdb"))
			Expect(connection.NumConns).To(Equal(1))
			Expect(len(connection.ConnPool)).To(Equal(1))
		})
		It("makes multiple connections successfully if the database exists", func() {
			connection.Connect(3)
			Expect(connection.DBName).To(Equal("testdb"))
			Expect(connection.NumConns).To(Equal(3))
			Expect(len(connection.ConnPool)).To(Equal(3))
		})
		It("does not connect if the database exists but the connection is refused", func() {
			connection.Driver = testutils.TestDriver{ErrToReturn: fmt.Errorf("pq: connection refused"), DB: mockdb, User: "testrole"}
			defer testutils.ShouldPanicWithMessage(`could not connect to server: Connection refused`)
			connection.Connect(1)
		})
		It("fails if an invalid number of connections is given", func() {
			defer testutils.ShouldPanicWithMessage("Must specify a connection pool size that is a positive integer")
			connection.Connect(0)
		})
		It("fails if the database does not exist", func() {
			connection.Driver = testutils.TestDriver{ErrToReturn: fmt.Errorf("pq: database \"testdb\" does not exist"), DB: mockdb, DBName: "testdb", User: "testrole"}
			Expect(connection.DBName).To(Equal("testdb"))
			defer testutils.ShouldPanicWithMessage("Database \"testdb\" does not exist, exiting")
			connection.Connect(1)
		})
		It("fails if the role does not exist", func() {
			oldPgUser := os.Getenv("PGUSER")
			os.Setenv("PGUSER", "nonexistent")
			defer os.Setenv("PGUSER", oldPgUser)

			connection = utils.NewDBConn("testdb")
			connection.Driver = testutils.TestDriver{ErrToReturn: fmt.Errorf("pq: role \"nonexistent\" does not exist"), DB: mockdb, DBName: "testdb", User: "nonexistent"}
			Expect(connection.User).To(Equal("nonexistent"))
			defer testutils.ShouldPanicWithMessage("Role \"nonexistent\" does not exist, exiting")
			connection.Connect(1)
		})
	})
	Describe("DBConn.Close", func() {
		var mockdb *sqlx.DB
		BeforeEach(func() {
			mockdb, mock = testutils.CreateMockDB()
			connection = utils.NewDBConn("testdb")
			connection.Driver = testutils.TestDriver{DB: mockdb, User: "testrole"}
		})
		It("successfully closes a dbconn with a single open connection", func() {
			connection.Connect(1)
			Expect(connection.NumConns).To(Equal(1))
			Expect(len(connection.ConnPool)).To(Equal(1))
			connection.Close()
			Expect(connection.NumConns).To(Equal(0))
			Expect(connection.ConnPool).To(BeNil())
		})
		It("successfully closes a dbconn with multiple open connections", func() {
			connection.Connect(3)
			Expect(connection.NumConns).To(Equal(3))
			Expect(len(connection.ConnPool)).To(Equal(3))
			connection.Close()
			Expect(connection.NumConns).To(Equal(0))
			Expect(connection.ConnPool).To(BeNil())
		})
		It("does nothing if there are no open connections", func() {
			connection.Connect(3)
			connection.Close()
			Expect(connection.NumConns).To(Equal(0))
			Expect(connection.ConnPool).To(BeNil())
			connection.Close()
			Expect(connection.NumConns).To(Equal(0))
			Expect(connection.ConnPool).To(BeNil())
		})
	})
	Describe("DBConn.Exec", func() {
		It("executes an INSERT outside of a transaction", func() {
			fakeResult := testutils.TestResult{Rows: 1}
			mock.ExpectExec("INSERT (.*)").WillReturnResult(fakeResult)

			res, err := connection.Exec("INSERT INTO pg_tables VALUES ('schema', 'table')")
			Expect(err).ToNot(HaveOccurred())
			rowsReturned, err := res.RowsAffected()
			Expect(rowsReturned).To(Equal(int64(1)))
		})
		It("executes an INSERT in a transaction", func() {
			fakeResult := testutils.TestResult{Rows: 1}
			testutils.ExpectBegin(mock)
			mock.ExpectExec("INSERT (.*)").WillReturnResult(fakeResult)
			mock.ExpectCommit()

			connection.Begin()
			res, err := connection.Exec("INSERT INTO pg_tables VALUES ('schema', 'table')")
			connection.Commit()
			Expect(err).ToNot(HaveOccurred())
			rowsReturned, err := res.RowsAffected()
			Expect(rowsReturned).To(Equal(int64(1)))
		})
	})
	Describe("DBConn.Get", func() {
		It("executes a GET outside of a transaction", func() {
			two_col_single_row := sqlmock.NewRows([]string{"schemaname", "tablename"}).
				AddRow("schema1", "table1")
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(two_col_single_row)

			testRecord := struct {
				Schemaname string
				Tablename  string
			}{}

			err := connection.Get(&testRecord, "SELECT schemaname, tablename FROM two_columns ORDER BY schemaname")

			Expect(err).ToNot(HaveOccurred())
			Expect(testRecord.Schemaname).To(Equal("schema1"))
			Expect(testRecord.Tablename).To(Equal("table1"))
		})
		It("executes a GET in a transaction", func() {
			two_col_single_row := sqlmock.NewRows([]string{"schemaname", "tablename"}).
				AddRow("schema1", "table1")
			testutils.ExpectBegin(mock)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(two_col_single_row)
			mock.ExpectCommit()

			testRecord := struct {
				Schemaname string
				Tablename  string
			}{}

			connection.Begin()
			err := connection.Get(&testRecord, "SELECT schemaname, tablename FROM two_columns ORDER BY schemaname")
			connection.Commit()
			Expect(err).ToNot(HaveOccurred())
			Expect(testRecord.Schemaname).To(Equal("schema1"))
			Expect(testRecord.Tablename).To(Equal("table1"))
		})
	})
	Describe("DBConn.Select", func() {
		It("executes a SELECT outside of a transaction", func() {
			two_col_rows := sqlmock.NewRows([]string{"schemaname", "tablename"}).
				AddRow("schema1", "table1").
				AddRow("schema2", "table2")
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(two_col_rows)

			testSlice := make([]struct {
				Schemaname string
				Tablename  string
			}, 0)

			err := connection.Select(&testSlice, "SELECT schemaname, tablename FROM two_columns ORDER BY schemaname LIMIT 2")

			Expect(err).ToNot(HaveOccurred())
			Expect(len(testSlice)).To(Equal(2))
			Expect(testSlice[0].Schemaname).To(Equal("schema1"))
			Expect(testSlice[0].Tablename).To(Equal("table1"))
			Expect(testSlice[1].Schemaname).To(Equal("schema2"))
			Expect(testSlice[1].Tablename).To(Equal("table2"))
		})
		It("executes a SELECT in a transaction", func() {
			two_col_rows := sqlmock.NewRows([]string{"schemaname", "tablename"}).
				AddRow("schema1", "table1").
				AddRow("schema2", "table2")
			testutils.ExpectBegin(mock)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(two_col_rows)
			mock.ExpectCommit()

			testSlice := make([]struct {
				Schemaname string
				Tablename  string
			}, 0)

			connection.Begin()
			err := connection.Select(&testSlice, "SELECT schemaname, tablename FROM two_columns ORDER BY schemaname LIMIT 2")
			connection.Commit()

			Expect(err).ToNot(HaveOccurred())
			Expect(len(testSlice)).To(Equal(2))
			Expect(testSlice[0].Schemaname).To(Equal("schema1"))
			Expect(testSlice[0].Tablename).To(Equal("table1"))
			Expect(testSlice[1].Schemaname).To(Equal("schema2"))
			Expect(testSlice[1].Tablename).To(Equal("table2"))
		})
	})
	Describe("DBConn.Begin", func() {
		It("successfully executes a BEGIN outside a transaction", func() {
			testutils.ExpectBegin(mock)
			connection.Begin()
			Expect(connection.Tx).To(Not(BeNil()))
		})
		It("panics if it executes a BEGIN in a transaction", func() {
			testutils.ExpectBegin(mock)
			connection.Begin()
			defer testutils.ShouldPanicWithMessage("Cannot begin transaction; there is already a transaction in progress")
			connection.Begin()
		})
	})
	Describe("DBConn.Commit", func() {
		It("successfully executes a COMMIT in a transaction", func() {
			testutils.ExpectBegin(mock)
			mock.ExpectCommit()
			connection.Begin()
			connection.Commit()
			Expect(connection.Tx).To(BeNil())
		})
		It("panics if it executes a COMMIT outside a transaction", func() {
			defer testutils.ShouldPanicWithMessage("Cannot commit transaction; there is no transaction in progress")
			connection.Commit()
		})
	})
	Describe("Dbconn.SetDatabaseVersion", func() {
		It("parses GPDB version string", func() {
			versionString := sqlmock.NewRows([]string{"versionstring"}).AddRow(" PostgreSQL 8.3.23 (Greenplum Database 5.1.0-beta.5+dev.65.g2a47ec9bfa build dev) on x86_64-apple-darwin16.5.0, compiled by GCC Apple LLVM version 8.1.0 (clang-802.0.42) compiled on Aug  4 2017 11:36:54")
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(versionString)

			connection.SetDatabaseVersion()

			Expect(connection.Version.VersionString).To(Equal("5.1.0-beta.5+dev.65.g2a47ec9bfa build dev"))

		})
		It("panics if GPDB version is less than 4.3.17", func() {
			defer testutils.ShouldPanicWithMessage("GPDB version 4.3.14.1+dev.83.ga57d1b7 build 1 is not supported. Please upgrade to GPDB 4.3.17.0 or later.")
			versionString := sqlmock.NewRows([]string{"versionstring"}).AddRow(" PostgreSQL 8.2.15 (Greenplum Database 4.3.14.1+dev.83.ga57d1b7 build 1) on x86_64-unknown-linux-gnu, compiled by GCC gcc (GCC) 4.4.7 20120313 (Red Hat 4.4.7-18) compiled on Sep 15 2017 17:31:20")
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(versionString)
			connection.SetDatabaseVersion()
		})
		It("panics if GPDB 5 version is less than 5.1.0", func() {
			defer testutils.ShouldPanicWithMessage("GPDB version 5.0.0+dev.92.g010f702 build dev 1 is not supported. Please upgrade to GPDB 5.1.0 or later.")
			versionString := sqlmock.NewRows([]string{"versionstring"}).AddRow(" PostgreSQL 8.3.23 (Greenplum Database 5.0.0+dev.92.g010f702 build dev 1) on x86_64-apple-darwin14.5.0, compiled by GCC Apple LLVM version 6.0 (clang-600.0.57) (based on LLVM 3.5svn) compiled on Sep 27 2017 14:40:25")
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(versionString)
			connection.SetDatabaseVersion()
		})
		It("does not panic if GPDB version is at least than 4.3.17", func() {
			versionString := sqlmock.NewRows([]string{"versionstring"}).AddRow(" PostgreSQL 8.2.15 (Greenplum Database 4.3.17.1+dev.83.ga57d1b7 build 1) on x86_64-unknown-linux-gnu, compiled by GCC gcc (GCC) 4.4.7 20120313 (Red Hat 4.4.7-18) compiled on Sep 15 2017 17:31:20")
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(versionString)
			connection.SetDatabaseVersion()
		})
		It("does not panic if GPDB version is at least 5.1.0", func() {
			versionString := sqlmock.NewRows([]string{"versionstring"}).AddRow(" PostgreSQL 8.3.23 (Greenplum Database 5.1.0-beta.9+dev.129.g4bd4e41 build dev) on x86_64-apple-darwin14.5.0, compiled by GCC Apple LLVM version 6.0 (clang-600.0.57) (based on LLVM 3.5svn) compiled on Sep  1 2017 16:57:41")
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(versionString)
			connection.SetDatabaseVersion()
		})
		It("does not panic if GPDB version is at least 6.0.0", func() {
			versionString := sqlmock.NewRows([]string{"versionstring"}).AddRow(" PostgreSQL 8.4.23 (Greenplum Database 6.0.0-beta.9+dev.129.g4bd4e41 build dev) on x86_64-apple-darwin14.5.0, compiled by GCC Apple LLVM version 6.0 (clang-600.0.57) (based on LLVM 3.5svn) compiled on Sep  1 2017 16:57:41")
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(versionString)
			connection.SetDatabaseVersion()
		})
	})
	Describe("Dbconn.ValidateConnNum", func() {
		BeforeEach(func() {
			connection.Close()
			connection.Connect(3)
		})
		AfterEach(func() {
			connection.Close()
		})
		It("returns the connection number if it is valid", func() {
			num := connection.ValidateConnNum(1)
			Expect(num).To(Equal(1))
		})
		It("defaults to 0 with no argument", func() {
			num := connection.ValidateConnNum()
			Expect(num).To(Equal(0))
		})
		It("panics if given multiple arguments", func() {
			defer testutils.ShouldPanicWithMessage("At most one connection number may be specified for a given connection")
			connection.ValidateConnNum(1, 2)
		})
		It("panics if given a negative number", func() {
			defer testutils.ShouldPanicWithMessage("Invalid connection number: -1")
			connection.ValidateConnNum(-1)
		})
		It("panics if given a number greater than NumConns", func() {
			defer testutils.ShouldPanicWithMessage("Invalid connection number: 4")
			connection.ValidateConnNum(4)
		})
	})
	Describe("SelectString", func() {
		header := []string{"string"}
		rowOne := []driver.Value{"one"}
		rowTwo := []driver.Value{"two"}

		It("returns a single string if the query selects a single string", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(rowOne...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			result := utils.SelectString(connection, "SELECT foo FROM bar")
			Expect(result).To(Equal("one"))
		})
		It("returns an empty string if the query selects no strings", func() {
			fakeResult := sqlmock.NewRows(header)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			result := utils.SelectString(connection, "SELECT foo FROM bar")
			Expect(result).To(Equal(""))
		})
		It("panics if the query selects multiple strings", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(rowOne...).AddRow(rowTwo...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			defer testutils.ShouldPanicWithMessage("Too many rows returned from query: got 2 rows, expected 1 row")
			utils.SelectString(connection, "SELECT foo FROM bar")
		})
	})
	Describe("SelectStringSlice", func() {
		header := []string{"string"}
		rowOne := []driver.Value{"one"}
		rowTwo := []driver.Value{"two"}

		It("returns a slice containing a single string if the query selects a single string", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(rowOne...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results := utils.SelectStringSlice(connection, "SELECT foo FROM bar")
			Expect(len(results)).To(Equal(1))
			Expect(results[0]).To(Equal("one"))
		})
		It("returns an empty slice if the query selects no strings", func() {
			fakeResult := sqlmock.NewRows(header)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results := utils.SelectStringSlice(connection, "SELECT foo FROM bar")
			Expect(len(results)).To(Equal(0))
		})
		It("returns a slice containing multiple strings if the query selects multiple strings", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(rowOne...).AddRow(rowTwo...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results := utils.SelectStringSlice(connection, "SELECT foo FROM bar")
			Expect(len(results)).To(Equal(2))
			Expect(results[0]).To(Equal("one"))
			Expect(results[1]).To(Equal("two"))
		})
	})
})
