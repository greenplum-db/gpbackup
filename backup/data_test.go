package backup_test

import (
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
)

var _ = Describe("backup/data tests", func() {
	var connection *utils.DBConn
	var mock sqlmock.Sqlmock
	BeforeEach(func() {
		connection, mock = testutils.CreateAndConnectMockDB()
		testutils.SetupTestLogger()
	})
	Describe("CopyTableOut", func() {
		It("will dump a table to its own file", func() {
			testTable := utils.Relation{2345, 3456, "public", "foo", "", ""}
			execStr := "COPY public.foo TO '<SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_3456' WITH CSV DELIMITER ',' ON SEGMENT;"
			mock.ExpectExec(execStr).WillReturnResult(sqlmock.NewResult(10, 0))
			filename := "<SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_3456"
			backup.CopyTableOut(connection, testTable, filename)
		})
	})
	Describe("GetTableDumpFilePath", func() {
		It("will create the dump path for data", func() {
			testTable := utils.Relation{2345, 3456, "public", "foo", "", ""}
			utils.DumpTimestamp = "20170101010101"
			expectedFilename := "<SEG_DATA_DIR>/backups/20170101/20170101010101/gpbackup_<SEGID>_20170101010101_3456"
			actualFilename := backup.GetTableDumpFilePath(testTable)
			Expect(actualFilename).To(Equal(expectedFilename))
		})
	})
})
