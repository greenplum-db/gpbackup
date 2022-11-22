package backup_test

import (
	"database/sql"
	"database/sql/driver"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/greenplum-db/gp-common-go-libs/structmatcher"
	"github.com/greenplum-db/gpbackup/backup"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup/queries_shared tests", func() {
	Describe("GetConstraints", func() {
		It("GetConstraints properly handles NULL constraint definitions", func() {
			if connectionPool.Version.AtLeast("6") {
				Skip("Test does not apply for GPDB versions after 5")
			}

			header := []string{"oid", "schema", "name", "contype", "condef", "conislocal", "owningobject", "isdomainconstraint", "ispartitionparent"}
			rowOne := []driver.Value{"1", "mock_schema", "mock_table", "mock_contype", "mock_condef", false, "mock_owningobject", false, false}
			rowTwo := []driver.Value{"2", "mock_schema2", "mock_table2", "mock_contype2", nil, false, "mock_owningobject2", false, false}
			fakeRows := sqlmock.NewRows(header).AddRow(rowOne...).AddRow(rowTwo...)
			mock.ExpectQuery(`SELECT (.*)`).WillReturnRows(fakeRows)
			result := backup.GetConstraints(connectionPool)

			// Expect the GetConstraints function to return only the 1st row since the 2nd row has a NULL constraint definition
			expectedResult := []backup.Constraint{{Oid: 1, Schema: "mock_schema", Name: "mock_table", ConType: "mock_contype",
				ConDef: sql.NullString{String: "mock_condef", Valid: true}, ConIsLocal: false, OwningObject: "mock_owningobject",
				IsDomainConstraint: false, IsPartitionParent: false}}
			Expect(result).To(HaveLen(1))
			structmatcher.ExpectStructsToMatch(&expectedResult[0], &result[0])
		})
	})
})
