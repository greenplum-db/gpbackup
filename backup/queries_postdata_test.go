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

var _ = Describe("backup/queries_postdata tests", func() {
	Describe("GetIndexes", func() {
		It("GetIndexes properly handles NULL index definitions", func() {
			if connectionPool.Version.Before("6") {
				implicitIndexNamesHeader := []string{"string"}
				implicitIndexNamesFakeRows := sqlmock.NewRows(implicitIndexNamesHeader)
				mock.ExpectQuery(`SELECT (.*)`).WillReturnRows(implicitIndexNamesFakeRows)
			}

			header := []string{"oid", "name", "owningschema", "owningtable", "tablespace", "def", "isclustered", "supportsconstraint", "isreplicaidentity"}
			rowOne := []driver.Value{"1", "mock_index", "mock_schema", "mock_table", "mock_tablespace", "mock_def", false, false, false}
			rowTwo := []driver.Value{"1", "mock_index2", "mock_schema2", "mock_table2", "mock_tablespace2", nil, false, false, false}
			fakeRows := sqlmock.NewRows(header).AddRow(rowOne...).AddRow(rowTwo...)
			mock.ExpectQuery(`SELECT (.*)`).WillReturnRows(fakeRows)
			result := backup.GetIndexes(connectionPool)

			// Expect the GetIndexes function to return only the 1st row since the 2nd row has a NULL index definition
			expectedResult := []backup.IndexDefinition{{Oid: 1, Name: "mock_index", OwningSchema: "mock_schema", OwningTable: "mock_table",
				Tablespace: "mock_tablespace", Def: sql.NullString{String: "mock_def", Valid: true}, IsClustered: false,
				SupportsConstraint: false, IsReplicaIdentity: false}}
			Expect(result).To(HaveLen(1))
			structmatcher.ExpectStructsToMatch(&expectedResult[0], &result[0])
		})
	})
	Describe("RenameExchangedPartitionIndexes", func() {
		It("RenameExchangedPartitionIndexes properly renames exchanged indexes to match their owning tables", func() {
			if !connectionPool.Version.Is("6") {
				Skip("Test only applies to GPDB version 6")
			}
			indexes := []backup.IndexDefinition{
				{Oid: 1, Name: "mock_index", OwningSchema: "mock_schema", OwningTable: "mock_table",
					Tablespace: "mock_tablespace", Def: sql.NullString{String: "CREATE INDEX mock_index ON mock_schema.mock_table", Valid: true}, IsClustered: false,
					SupportsConstraint: false, IsReplicaIdentity: false},
				{Oid: 2, Name: "realTableName_c_idx", OwningSchema: "mock_schema", OwningTable: "pt_heap_tab_1_prt_pqr",
					Tablespace: "mock_tablespace", Def: sql.NullString{String: "CREATE INDEX realTableName_c_idx ON mock_schema.pt_heap_tab_1_prt_pqr", Valid: true}, IsClustered: false,
					SupportsConstraint: false, IsReplicaIdentity: false},
				{Oid: 3, Name: "pt_heap_tab_1_prt_pqr_a_idx", OwningSchema: "mock_schema", OwningTable: "realTableName",
					Tablespace: "mock_tablespace", Def: sql.NullString{String: "CREATE INDEX pt_heap_tab_1_prt_pqr_a_idx ON mock_schema.realTableName", Valid: true}, IsClustered: false,
					SupportsConstraint: false, IsReplicaIdentity: false}}
			header := []string{"indexname", "tablename", "relispartition"}
			rowOne := []driver.Value{"mock_index", "mock_table", "false"}
			rowTwo := []driver.Value{"pt_heap_tab_1_prt_pqr_a_idx", "realTableName", "false"}
			rowThree := []driver.Value{"realTableName_c_idx", "pt_heap_tab", "true"}
			fakeRows := sqlmock.NewRows(header).AddRow(rowOne...).AddRow(rowTwo...).AddRow(rowThree...)
			mock.ExpectQuery(`SELECT (.*)`).WillReturnRows(fakeRows)
			backup.RenameExchangedPartitionIndexes(connectionPool, &indexes)

			Expect(indexes).To(HaveLen(3))
			for _, idx := range indexes {
				switch idx.Oid {
				case 1:
					Expect(idx.Name).To(Equal("mock_index"))
					Expect(idx.Def.String).To(Equal("CREATE INDEX mock_index ON mock_schema.mock_table"))
				case 2:
					// We do not rename the partition leaf index here, as it will be
					// system-generated correctly when we apply the index to the partition root table.
					Expect(idx.Name).To(Equal("realTableName_c_idx"))
					Expect(idx.Def.String).To(Equal("CREATE INDEX realTableName_c_idx ON mock_schema.pt_heap_tab_1_prt_pqr"))
				case 3:
					Expect(idx.Name).To(Equal("realTableName_c_idx"))
					Expect(idx.Def.String).To(Equal("CREATE INDEX realTableName_c_idx ON mock_schema.realTableName"))
				default:
					Fail("Unexpected index OID found")
				}
			}
		})
	})
	Describe("GetRules", func() {
		It("GetRules properly handles NULL rule definitions", func() {
			header := []string{"oid", "name", "owningschema", "owningtable", "def"}
			rowOne := []driver.Value{"1", "mock_rule", "mock_schema", "mock_table", "mock_def"}
			rowTwo := []driver.Value{"2", "mock_rule2", "mock_schema2", "mock_table2", nil}
			fakeRows := sqlmock.NewRows(header).AddRow(rowOne...).AddRow(rowTwo...)
			mock.ExpectQuery(`SELECT (.*)`).WillReturnRows(fakeRows)
			result := backup.GetRules(connectionPool)

			// Expect the GetRules function to return only the 1st row since the 2nd row has a NULL rule definition
			expectedResult := []backup.RuleDefinition{{Oid: 1, Name: "mock_rule", OwningSchema: "mock_schema", OwningTable: "mock_table", Def: sql.NullString{String: "mock_def", Valid: true}}}
			Expect(result).To(HaveLen(1))
			structmatcher.ExpectStructsToMatch(&expectedResult[0], &result[0])
		})
	})
	Describe("GetTriggers", func() {
		It("GetTriggers properly handles NULL trigger definitions", func() {
			header := []string{"oid", "name", "owningschema", "owningtable", "def"}
			rowOne := []driver.Value{"1", "mock_trigger", "mock_schema", "mock_table", "mock_def"}
			rowTwo := []driver.Value{"2", "mock_trigger2", "mock_schema2", "mock_table2", nil}
			fakeRows := sqlmock.NewRows(header).AddRow(rowOne...).AddRow(rowTwo...)
			mock.ExpectQuery(`SELECT (.*)`).WillReturnRows(fakeRows)
			result := backup.GetTriggers(connectionPool)

			// Expect the GetTriggers function to return only the 1st row since the 2nd row has a NULL trigger definition
			expectedResult := []backup.TriggerDefinition{{Oid: 1, Name: "mock_trigger", OwningSchema: "mock_schema", OwningTable: "mock_table", Def: sql.NullString{String: "mock_def", Valid: true}}}
			Expect(result).To(HaveLen(1))
			structmatcher.ExpectStructsToMatch(&expectedResult[0], &result[0])
		})
	})
})
