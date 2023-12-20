package backup_test

import (
	"database/sql"
	"database/sql/driver"
	"fmt"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/greenplum-db/gp-common-go-libs/structmatcher"
	"github.com/greenplum-db/gpbackup/backup"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup internal tests", func() {
	Describe("generateLockQueries", func() {
		It("batches tables together and generates lock queries", func() {
			tables := make([]backup.Relation, 0)
			for i := 0; i < 200; i++ {
				tables = append(tables, backup.Relation{0, 0, "public", fmt.Sprintf("foo%d", i)})
			}

			batchSize := 100
			lockQueries := backup.GenerateTableBatches(tables, batchSize)
			Expect(len(lockQueries)).To(Equal(2))
		})
		It("batches up remaining leftover tables together in a single lock query", func() {
			tables := make([]backup.Relation, 0)
			for i := 0; i < 101; i++ {
				tables = append(tables, backup.Relation{0, 0, "public", fmt.Sprintf("foo%d", i)})
			}

			batchSize := 50
			lockQueries := backup.GenerateTableBatches(tables, batchSize)
			Expect(len(lockQueries)).To(Equal(3))
		})
	})
	Describe("GetAllViews", func() {
		It("GetAllViews properly handles NULL view definitions", func() {
			columnDefHeader := []string{"attrelid", "attnum", "name", "attnotnull", "atthasdef", "type", "encoding", "attstattarget", "storagetype", "defaultval", "comment", "privileges", "kind", "options", "fdwoptions", "collation", "securitylabelprovider", "securitylabel", "attgenerated", "isinherited"}
			fakeColumnDef := sqlmock.NewRows(columnDefHeader)
			mock.ExpectQuery(`SELECT (.*)`).WillReturnRows(fakeColumnDef)

			header := []string{"oid", "schema", "name", "options", "definition", "tablespace", "ismaterialized"}
			rowOne := []driver.Value{"1", "mock_schema", "mock_table", "mock_options", "mock_def", "mock_tablespace", false}
			rowTwo := []driver.Value{"2", "mock_schema2", "mock_table2", "mock_options2", nil, "mock_tablespace2", false}
			fakeRows := sqlmock.NewRows(header).AddRow(rowOne...).AddRow(rowTwo...)
			mock.ExpectQuery(`SELECT (.*)`).WillReturnRows(fakeRows)

			headerDistPol := []string{"oid", "value"}
			fakeRowsDistPol := sqlmock.NewRows(headerDistPol)
			mock.ExpectQuery(`SELECT (.*)`).WillReturnRows(fakeRowsDistPol)

			result := backup.GetAllViews(connectionPool)

			// Expect the GetAllViews function to return only the 1st row since the 2nd row has a NULL view definition
			expectedResult := []backup.View{{Oid: 1, Schema: "mock_schema", Name: "mock_table", Options: "mock_options",
				Definition: sql.NullString{String: "mock_def", Valid: true}, Tablespace: "mock_tablespace", IsMaterialized: false}}
			Expect(result).To(HaveLen(1))
			structmatcher.ExpectStructsToMatch(&expectedResult[0], &result[0])
		})
	})
})
