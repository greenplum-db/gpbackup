package utils_test

import (
	"time"

	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
)

var _ = Describe("utils/db tests", func() {
	BeforeEach(func() {
		operating.System.Now = func() time.Time { return time.Date(2017, time.January, 1, 1, 1, 1, 1, time.Local) }
	})
	Describe("Dbconn.SetDatabaseVersion", func() {
		It("parses GPDB version string", func() {
			versionString := sqlmock.NewRows([]string{"versionstring"}).AddRow(" PostgreSQL 8.3.23 (Greenplum Database 5.1.0-beta.5+dev.65.g2a47ec9bfa build dev) on x86_64-apple-darwin16.5.0, compiled by GCC Apple LLVM version 8.1.0 (clang-802.0.42) compiled on Aug  4 2017 11:36:54")
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(versionString)

			utils.SetDatabaseVersion(connection)

			Expect(connection.Version.VersionString).To(Equal("5.1.0-beta.5+dev.65.g2a47ec9bfa build dev"))

		})
		It("panics if GPDB version is less than 4.3.17", func() {
			defer testhelper.ShouldPanicWithMessage("GPDB version 4.3.14.1+dev.83.ga57d1b7 build 1 is not supported. Please upgrade to GPDB 4.3.17.0 or later.")
			versionString := sqlmock.NewRows([]string{"versionstring"}).AddRow(" PostgreSQL 8.2.15 (Greenplum Database 4.3.14.1+dev.83.ga57d1b7 build 1) on x86_64-unknown-linux-gnu, compiled by GCC gcc (GCC) 4.4.7 20120313 (Red Hat 4.4.7-18) compiled on Sep 15 2017 17:31:20")
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(versionString)
			utils.SetDatabaseVersion(connection)
		})
		It("panics if GPDB 5 version is less than 5.1.0", func() {
			defer testhelper.ShouldPanicWithMessage("GPDB version 5.0.0+dev.92.g010f702 build dev 1 is not supported. Please upgrade to GPDB 5.1.0 or later.")
			versionString := sqlmock.NewRows([]string{"versionstring"}).AddRow(" PostgreSQL 8.3.23 (Greenplum Database 5.0.0+dev.92.g010f702 build dev 1) on x86_64-apple-darwin14.5.0, compiled by GCC Apple LLVM version 6.0 (clang-600.0.57) (based on LLVM 3.5svn) compiled on Sep 27 2017 14:40:25")
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(versionString)
			utils.SetDatabaseVersion(connection)
		})
		It("does not panic if GPDB version is at least than 4.3.17", func() {
			versionString := sqlmock.NewRows([]string{"versionstring"}).AddRow(" PostgreSQL 8.2.15 (Greenplum Database 4.3.17.1+dev.83.ga57d1b7 build 1) on x86_64-unknown-linux-gnu, compiled by GCC gcc (GCC) 4.4.7 20120313 (Red Hat 4.4.7-18) compiled on Sep 15 2017 17:31:20")
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(versionString)
			utils.SetDatabaseVersion(connection)
		})
		It("does not panic if GPDB version is at least 5.1.0", func() {
			versionString := sqlmock.NewRows([]string{"versionstring"}).AddRow(" PostgreSQL 8.3.23 (Greenplum Database 5.1.0-beta.9+dev.129.g4bd4e41 build dev) on x86_64-apple-darwin14.5.0, compiled by GCC Apple LLVM version 6.0 (clang-600.0.57) (based on LLVM 3.5svn) compiled on Sep  1 2017 16:57:41")
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(versionString)
			utils.SetDatabaseVersion(connection)
		})
		It("does not panic if GPDB version is at least 6.0.0", func() {
			versionString := sqlmock.NewRows([]string{"versionstring"}).AddRow(" PostgreSQL 8.4.23 (Greenplum Database 6.0.0-beta.9+dev.129.g4bd4e41 build dev) on x86_64-apple-darwin14.5.0, compiled by GCC Apple LLVM version 6.0 (clang-600.0.57) (based on LLVM 3.5svn) compiled on Sep  1 2017 16:57:41")
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(versionString)
			utils.SetDatabaseVersion(connection)
		})
	})
})
