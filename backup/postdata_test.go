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

var _ = Describe("backup/postdata tests", func() {
	var connection *utils.DBConn
	var mock sqlmock.Sqlmock
	BeforeEach(func() {
		connection, mock = testutils.CreateAndConnectMockDB()
		testutils.SetupTestLogger()
	})

	Describe("GetIndexesForAllTables", func() {
		tableOne := utils.BasicRelation("public", "table_one")
		tableTwo := utils.BasicRelation("public", "table_two")
		tableWithout := utils.BasicRelation("public", "table_no_index")

		header := []string{"name", "def", "comment"}
		resultEmpty := sqlmock.NewRows(header)
		btreeOne := []driver.Value{"btree_idx1", "CREATE INDEX btree_idx1 ON table_one USING btree (i)", ""}
		btreeTwo := []driver.Value{"btree_idx2", "CREATE INDEX btree_idx2 ON table_two USING btree (j)", ""}
		bitmapOne := []driver.Value{"bitmap_idx1", "CREATE INDEX bitmap_idx1 ON table_one USING bitmap (i)", ""}
		commentOne := []driver.Value{"btree_idx1", "CREATE INDEX btree_idx1 ON table_one USING btree (i)", "This is an index comment."}

		Context("Indexes on a single column", func() {
			It("returns a slice containing one CREATE INDEX statement for one table", func() {
				testTables := []utils.Relation{tableOne}
				resultOne := sqlmock.NewRows(header).AddRow(btreeOne...)
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(resultOne)
				indexes := backup.GetIndexesForAllTables(connection, testTables)
				Expect(len(indexes)).To(Equal(1))
				Expect(indexes[0]).To(Equal("\n\nCREATE INDEX btree_idx1 ON table_one USING btree (i);\n"))
			})
			It("returns a slice containing one CREATE INDEX statement for two tables", func() {
				testTables := []utils.Relation{tableOne, tableTwo}
				resultOne := sqlmock.NewRows(header).AddRow(btreeOne...)
				resultTwo := sqlmock.NewRows(header).AddRow(btreeTwo...)
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(resultOne)
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(resultTwo)
				indexes := backup.GetIndexesForAllTables(connection, testTables)
				Expect(len(indexes)).To(Equal(2))
				Expect(indexes[0]).To(Equal("\n\nCREATE INDEX btree_idx1 ON table_one USING btree (i);\n"))
				Expect(indexes[1]).To(Equal("\n\nCREATE INDEX btree_idx2 ON table_two USING btree (j);\n"))
			})
			It("returns a slice containing two CREATE INDEX statement for one table", func() {
				testTables := []utils.Relation{tableOne, tableTwo}
				resultOne := sqlmock.NewRows(header).AddRow(btreeOne...).AddRow(bitmapOne...)
				resultTwo := sqlmock.NewRows(header).AddRow(btreeTwo...)
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(resultOne)
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(resultTwo)
				indexes := backup.GetIndexesForAllTables(connection, testTables)
				Expect(len(indexes)).To(Equal(3))
				Expect(indexes[0]).To(Equal("\n\nCREATE INDEX btree_idx1 ON table_one USING btree (i);\n"))
				Expect(indexes[1]).To(Equal("\n\nCREATE INDEX bitmap_idx1 ON table_one USING bitmap (i);\n"))
				Expect(indexes[2]).To(Equal("\n\nCREATE INDEX btree_idx2 ON table_two USING btree (j);\n"))
			})
			It("returns a slice containing one CREATE INDEX statement when one table has an index and one does not", func() {
				testTables := []utils.Relation{tableOne, tableWithout}
				resultOne := sqlmock.NewRows(header).AddRow(btreeOne...)
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(resultOne)
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(resultEmpty)
				indexes := backup.GetIndexesForAllTables(connection, testTables)
				Expect(len(indexes)).To(Equal(1))
				Expect(indexes[0]).To(Equal("\n\nCREATE INDEX btree_idx1 ON table_one USING btree (i);\n"))
			})
			It("returns a slice containing one CREATE INDEX statement and accompanying comment", func() {
				testTables := []utils.Relation{tableOne}
				resultOne := sqlmock.NewRows(header).AddRow(commentOne...)
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(resultOne)
				indexes := backup.GetIndexesForAllTables(connection, testTables)
				Expect(len(indexes)).To(Equal(1))
				Expect(indexes[0]).To(Equal(`

CREATE INDEX btree_idx1 ON table_one USING btree (i);

COMMENT ON INDEX btree_idx1 IS 'This is an index comment.';`))
			})
		})
	})
})
