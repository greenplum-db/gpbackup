package toc_test

import (
	"bytes"
	"testing"

	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/toc"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gbytes"
)

var (
	buffer  *Buffer
	tocfile *toc.TOC
)

func TestTOC(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "TOC Suite")
}

var _ = Describe("utils/toc tests", func() {
	table1 := toc.StatementWithType{Schema: "schema", Name: "table1", ObjectType: toc.OBJ_TABLE, Statement: "CREATE TABLE schema.table1", Tier: []uint32{0, 0}}
	table1Len := uint64(len(table1.Statement))

	capsTable := toc.StatementWithType{Schema: "schema", Name: "TABLE_CAPS", ObjectType: toc.OBJ_TABLE, Statement: "CREATE TABLE schema.TABLE_CAPS", Tier: []uint32{0, 0}}
	capsTableLen := uint64(len(capsTable.Statement))

	table2 := toc.StatementWithType{Schema: "schema2", Name: "table2", ObjectType: toc.OBJ_TABLE, Statement: "CREATE TABLE schema2.table2", Tier: []uint32{0, 0}}
	table2Len := uint64(len(table2.Statement))

	view := toc.StatementWithType{Schema: "schema", Name: "view", ObjectType: toc.OBJ_VIEW, Statement: "CREATE VIEW schema.view", Tier: []uint32{0, 0}}
	viewLen := uint64(len(view.Statement))

	matView := toc.StatementWithType{Schema: "schema", Name: "matView", ObjectType: toc.OBJ_MATERIALIZED_VIEW, Statement: "CREATE MATERIALIZED VIEW schema.mat_view", Tier: []uint32{0, 0}}
	matViewLen := uint64(len(matView.Statement))

	sequence := toc.StatementWithType{Schema: "schema", Name: "sequence", ObjectType: toc.OBJ_SEQUENCE, Statement: "CREATE SEQUENCE schema.sequence START 100", Tier: []uint32{0, 0}}
	sequenceLen := uint64(len(sequence.Statement))

	index := toc.StatementWithType{Schema: "schema2", Name: "someindex", ObjectType: toc.OBJ_INDEX, Statement: "CREATE INDEX someindex ON schema2.table2(i)", ReferenceObject: "schema2.table2", Tier: []uint32{0, 0}}
	indexLen := uint64(len(index.Statement))

	BeforeEach(func() {
		tocfile, _ = testutils.InitializeTestTOC(buffer, "predata")
	})
	Describe("GetSQLStatementForObjectTypes", func() {
		// Dummy variables to help clarify which arguments are non-empty in a given test
		var noInObj, noExObj, noInSchema, noExSchema, noInRelation, noExRelation []string
		var metadataFile *bytes.Reader
		BeforeEach(func() {
			startCount := uint64(0)
			endCount := table1Len
			tocfile.AddMetadataEntry("predata", toc.MetadataEntry{Schema: "schema", Name: "table1", ObjectType: toc.OBJ_TABLE}, startCount, endCount, []uint32{0, 0})
			startCount = endCount
			endCount += capsTableLen
			tocfile.AddMetadataEntry("predata", toc.MetadataEntry{Schema: "schema", Name: "TABLE_CAPS", ObjectType: toc.OBJ_TABLE}, startCount, endCount, []uint32{0, 0})
			startCount = endCount
			endCount += table2Len
			tocfile.AddMetadataEntry("predata", toc.MetadataEntry{Schema: "schema2", Name: "table2", ObjectType: toc.OBJ_TABLE}, startCount, endCount, []uint32{0, 0})
			startCount = endCount
			endCount += viewLen
			tocfile.AddMetadataEntry("predata", toc.MetadataEntry{Schema: "schema", Name: "view", ObjectType: toc.OBJ_VIEW}, startCount, endCount, []uint32{0, 0})
			startCount = endCount
			endCount += matViewLen
			tocfile.AddMetadataEntry("predata", toc.MetadataEntry{Schema: "schema", Name: "matView", ObjectType: toc.OBJ_MATERIALIZED_VIEW}, startCount, endCount, []uint32{0, 0})
			startCount = endCount
			endCount += sequenceLen
			tocfile.AddMetadataEntry("predata", toc.MetadataEntry{Schema: "schema", Name: "sequence", ObjectType: toc.OBJ_SEQUENCE}, startCount, endCount, []uint32{0, 0})
			startCount = endCount
			endCount += indexLen
			tocfile.AddMetadataEntry("predata", toc.MetadataEntry{Schema: "schema2", Name: "someindex", ObjectType: toc.OBJ_INDEX, ReferenceObject: "schema2.table2"}, startCount, endCount, []uint32{0, 0})

			metadataFile = bytes.NewReader([]byte(table1.Statement + capsTable.Statement + table2.Statement + view.Statement + matView.Statement + sequence.Statement + index.Statement))
		})
		It("returns statement for a single object type", func() {
			statements := tocfile.GetSQLStatementForObjectTypes("predata", metadataFile, []string{toc.OBJ_VIEW}, noExObj, noInSchema, noExSchema, noInRelation, noExRelation)

			Expect(statements).To(Equal([]toc.StatementWithType{view}))
		})
		It("returns statement for multiple object types", func() {
			statements := tocfile.GetSQLStatementForObjectTypes("predata", metadataFile, []string{toc.OBJ_TABLE, toc.OBJ_VIEW}, noExObj, noInSchema, noExSchema, noInRelation, noExRelation)

			Expect(statements).To(Equal([]toc.StatementWithType{table1, capsTable, table2, view}))
		})
		It("does not return a statement type listed in the exclude list", func() {
			statements := tocfile.GetSQLStatementForObjectTypes("predata", metadataFile, noInObj, []string{toc.OBJ_TABLE}, noInSchema, noExSchema, noInRelation, noExRelation)

			Expect(statements).To(Equal([]toc.StatementWithType{view, matView, sequence, index}))
		})
		It("returns empty statement when no object types are found", func() {
			statements := tocfile.GetSQLStatementForObjectTypes("predata", metadataFile, []string{toc.OBJ_FUNCTION}, noExObj, noInSchema, noExSchema, noInRelation, noExRelation)

			Expect(statements).To(Equal([]toc.StatementWithType{}))
		})
		It("returns statement for a single object type with matching schema", func() {
			statements := tocfile.GetSQLStatementForObjectTypes("predata", metadataFile, []string{toc.OBJ_TABLE}, noExObj, []string{"schema2"}, noExSchema, noInRelation, noExRelation)

			Expect(statements).To(Equal([]toc.StatementWithType{table2}))
		})
		It("returns statement for any object type in the include schema", func() {
			statements := tocfile.GetSQLStatementForObjectTypes("predata", metadataFile, noInObj, noExObj, []string{"schema"}, noExSchema, noInRelation, noExRelation)

			Expect(statements).To(Equal([]toc.StatementWithType{table1, capsTable, view, matView, sequence}))
		})
		It("returns statement for any object type not in the exclude schema", func() {
			statements := tocfile.GetSQLStatementForObjectTypes("predata", metadataFile, noInObj, noExObj, noInSchema, []string{"schema2"}, noInRelation, noExRelation)

			Expect(statements).To(Equal([]toc.StatementWithType{table1, capsTable, view, matView, sequence}))
		})
		It("returns statement for a table matching an included table", func() {
			statements := tocfile.GetSQLStatementForObjectTypes("predata", metadataFile, noInObj, noExObj, noInSchema, noExSchema, []string{"schema.table1"}, noExRelation)

			Expect(statements).To(Equal([]toc.StatementWithType{table1}))
		})
		It("returns statement for a table matching an included table in caps", func() {
			statements := tocfile.GetSQLStatementForObjectTypes("predata", metadataFile, noInObj, noExObj, noInSchema, noExSchema, []string{"schema.TABLE_CAPS"}, noExRelation)

			tableCaps := toc.StatementWithType{Schema: "schema", Name: "TABLE_CAPS", ObjectType: toc.OBJ_TABLE, Statement: "CREATE TABLE schema.TABLE_CAPS", Tier: []uint32{0, 0}}

			Expect(statements).To(Equal([]toc.StatementWithType{tableCaps}))
		})
		It("returns statement for a view matching an included view", func() {
			statements := tocfile.GetSQLStatementForObjectTypes("predata", metadataFile, noInObj, noExObj, noInSchema, noExSchema, []string{"schema.view"}, noExRelation)

			Expect(statements).To(Equal([]toc.StatementWithType{view}))
		})
		It("returns statement for a materialized view matching an included materialized view", func() {
			statements := tocfile.GetSQLStatementForObjectTypes("predata", metadataFile, noInObj, noExObj, noInSchema, noExSchema, []string{"schema.matView"}, noExRelation)

			Expect(statements).To(Equal([]toc.StatementWithType{matView}))
		})
		It("returns no statements for an excluded view, materialized view or sequence", func() {
			statements := tocfile.GetSQLStatementForObjectTypes("predata", metadataFile, noInObj, noExObj, noInSchema, noExSchema, noInRelation, []string{"schema.view", "schema.matView", "schema.sequence"})

			Expect(statements).To(Equal([]toc.StatementWithType{table1, capsTable, table2, index}))
		})
		It("returns no statements for a non-relation object with matching name from relation list", func() {
			statements := tocfile.GetSQLStatementForObjectTypes("predata", metadataFile, noInObj, noExObj, noInSchema, noExSchema, []string{"schema.someindex"}, noExRelation)

			Expect(statements).To(Equal([]toc.StatementWithType{}))
		})

		Context("With reference object", func() {
			sequenceTable := toc.StatementWithType{Schema: "schema", Name: "sequence_table", ObjectType: toc.OBJ_TABLE,
				Statement: "CREATE TABLE schema.sequence_table", Tier: []uint32{0, 0}}
			sequenceTableLen := uint64(len(sequenceTable.Statement))

			sequenceOwner := toc.StatementWithType{Schema: "schema", Name: "sequence", ObjectType: toc.OBJ_SEQUENCE_OWNER,
				Statement: "ALTER SEQUENCE schema.sequence OWNED BY schema.sequence_table", ReferenceObject: "schema.sequence_table", Tier: []uint32{0, 0}}
			sequenceOwnerLen := uint64(len(sequenceOwner.Statement))

			BeforeEach(func() {
				startCount := table1Len + capsTableLen + table2Len + viewLen + matViewLen + sequenceLen + indexLen
				endCount := startCount + sequenceTableLen
				tocfile.AddMetadataEntry("predata", toc.MetadataEntry{Schema: "schema", Name: "sequence_table", ObjectType: toc.OBJ_TABLE}, startCount, endCount, []uint32{0, 0})
				startCount = endCount
				endCount += sequenceOwnerLen
				tocfile.AddMetadataEntry("predata", toc.MetadataEntry{Schema: "schema", Name: "sequence", ObjectType: toc.OBJ_SEQUENCE_OWNER, ReferenceObject: "schema.sequence_table"}, startCount, endCount, []uint32{0, 0})

				metadataFile = bytes.NewReader([]byte(table1.Statement + capsTable.Statement + table2.Statement + view.Statement + matView.Statement + sequence.Statement + index.Statement + sequenceTable.Statement + sequenceOwner.Statement))
			})
			It("does not return sequence owner statement when owning table is not included", func() {
				statements := tocfile.GetSQLStatementForObjectTypes("predata", metadataFile, noInObj, noExObj, noInSchema, noExSchema, []string{"schema.sequence"}, noExRelation)

				Expect(statements).To(Equal([]toc.StatementWithType{sequence}))
			})
			It("does not return sequence owner statement when sequence is not included", func() {
				statements := tocfile.GetSQLStatementForObjectTypes("predata", metadataFile, noInObj, noExObj, noInSchema, noExSchema, []string{"schema.sequence_table"}, noExRelation)

				Expect(statements).To(Equal([]toc.StatementWithType{sequenceTable}))
			})
			It("returns sequence owner statement when owning table and sequence are both included", func() {
				statements := tocfile.GetSQLStatementForObjectTypes("predata", metadataFile, noInObj, noExObj, noInSchema, noExSchema, []string{"schema.sequence_table", "schema.sequence"}, noExRelation)

				Expect(statements).To(Equal([]toc.StatementWithType{sequence, sequenceTable, sequenceOwner}))
			})
			It("returns statement for non sequence-owners with matching reference object", func() {
				statements := tocfile.GetSQLStatementForObjectTypes("predata", metadataFile, noInObj, noExObj, noInSchema, noExSchema, []string{"schema2.table2"}, noExRelation)

				Expect(statements).To(Equal([]toc.StatementWithType{table2, index}))
			})
			It("returns statement for any object type or reference object not matching an excluded table", func() {
				statements := tocfile.GetSQLStatementForObjectTypes("predata", metadataFile, noInObj, noExObj, noInSchema, noExSchema, noInRelation, []string{"schema.table1"})

				Expect(statements).To(Equal([]toc.StatementWithType{capsTable, table2, view, matView, sequence, index, sequenceTable, sequenceOwner}))
			})
			It("returns no statements for any object type with reference object matching an excluded table", func() {
				statements := tocfile.GetSQLStatementForObjectTypes("predata", metadataFile, noInObj, noExObj, noInSchema, noExSchema, noInRelation, []string{"schema2.table2"})

				Expect(statements).To(Equal([]toc.StatementWithType{table1, capsTable, view, matView, sequence, sequenceTable, sequenceOwner}))
			})
		})
	})
	Describe("GetDataEntriesMatching", func() {
		BeforeEach(func() {
			tocfile.AddCoordinatorDataEntry("schema1", "table1", 1, "(i)", 0, "", "", false)
			tocfile.AddCoordinatorDataEntry("schema2", "table2", 1, "(i)", 0, "", "", false)
			tocfile.AddCoordinatorDataEntry("schema3", "table3", 1, "(i)", 0, "", "", false)
			tocfile.AddCoordinatorDataEntry("schema3", "table3_partition1", 1, "(i)", 0, "table3", "", false)
			tocfile.AddCoordinatorDataEntry("schema3", "table3_partition2", 1, "(i)", 0, "table3", "", false)
		})
		Context("Non-empty restore plan", func() {
			restorePlanTableFQNs := []string{"schema1.table1", "schema2.table2", "schema3.table3", "schema3.table3_partition1", "schema3.table3_partition2"}

			It("returns matching entry on include schema", func() {
				includeSchemas := []string{"schema1"}

				matchingEntries := tocfile.GetDataEntriesMatching(includeSchemas, []string{},
					[]string{}, []string{}, restorePlanTableFQNs)

				Expect(matchingEntries).To(Equal(
					[]toc.CoordinatorDataEntry{
						{Schema: "schema1", Name: "table1", Oid: 1, AttributeString: "(i)"},
					},
				))
			})
			It("returns matching entry on exclude schema", func() {
				excludeSchemas := []string{"schema2"}

				matchingEntries := tocfile.GetDataEntriesMatching([]string{}, excludeSchemas,
					[]string{}, []string{}, restorePlanTableFQNs)

				Expect(matchingEntries).To(ConsistOf(
					[]toc.CoordinatorDataEntry{
						{Schema: "schema1", Name: "table1", Oid: 1, AttributeString: "(i)", PartitionRoot: ""},
						{Schema: "schema3", Name: "table3", Oid: 1, AttributeString: "(i)", PartitionRoot: ""},
						{Schema: "schema3", Name: "table3_partition1", Oid: 1, AttributeString: "(i)", PartitionRoot: "table3"},
						{Schema: "schema3", Name: "table3_partition2", Oid: 1, AttributeString: "(i)", PartitionRoot: "table3"},
					},
				))
			})
			It("returns matching entry on include table", func() {
				includeTables := []string{"schema1.table1"}

				matchingEntries := tocfile.GetDataEntriesMatching([]string{}, []string{},
					includeTables, []string{}, restorePlanTableFQNs)

				Expect(matchingEntries).To(Equal(
					[]toc.CoordinatorDataEntry{
						{Schema: "schema1", Name: "table1", Oid: 1, AttributeString: "(i)"},
					},
				))
			})
			It("returns matching entry on exclude table", func() {
				excludeTables := []string{"schema2.table2"}

				matchingEntries := tocfile.GetDataEntriesMatching([]string{}, []string{},
					[]string{}, excludeTables, restorePlanTableFQNs)

				Expect(matchingEntries).To(ConsistOf(
					[]toc.CoordinatorDataEntry{
						{Schema: "schema1", Name: "table1", Oid: 1, AttributeString: "(i)", PartitionRoot: ""},
						{Schema: "schema3", Name: "table3", Oid: 1, AttributeString: "(i)", PartitionRoot: ""},
						{Schema: "schema3", Name: "table3_partition1", Oid: 1, AttributeString: "(i)", PartitionRoot: "table3"},
						{Schema: "schema3", Name: "table3_partition2", Oid: 1, AttributeString: "(i)", PartitionRoot: "table3"},
					},
				))
			})
			It("returns all entries when not schema-filtered or table-filtered", func() {
				matchingEntries := tocfile.GetDataEntriesMatching([]string{}, []string{},
					[]string{}, []string{}, restorePlanTableFQNs)

				Expect(matchingEntries).To(ConsistOf(
					[]toc.CoordinatorDataEntry{
						{Schema: "schema1", Name: "table1", Oid: 1, AttributeString: "(i)", PartitionRoot: ""},
						{Schema: "schema2", Name: "table2", Oid: 1, AttributeString: "(i)", PartitionRoot: ""},
						{Schema: "schema3", Name: "table3", Oid: 1, AttributeString: "(i)", PartitionRoot: ""},
						{Schema: "schema3", Name: "table3_partition1", Oid: 1, AttributeString: "(i)", PartitionRoot: "table3"},
						{Schema: "schema3", Name: "table3_partition2", Oid: 1, AttributeString: "(i)", PartitionRoot: "table3"},
					},
				))
			})
			It("returns matching entry and its leaf partitions on include table", func() {
				includeTables := []string{"schema3.table3"}

				matchingEntries := tocfile.GetDataEntriesMatching([]string{}, []string{},
					includeTables, []string{}, restorePlanTableFQNs)

				Expect(matchingEntries).To(ConsistOf(
					[]toc.CoordinatorDataEntry{
						{Schema: "schema3", Name: "table3", Oid: 1, AttributeString: "(i)", PartitionRoot: ""},
						{Schema: "schema3", Name: "table3_partition1", Oid: 1, AttributeString: "(i)", PartitionRoot: "table3"},
						{Schema: "schema3", Name: "table3_partition2", Oid: 1, AttributeString: "(i)", PartitionRoot: "table3"},
					},
				))
			})
			It("does not return leaf partitions in include tables but not in the restore plan", func() {
				includeTables := []string{"schema3.table3"}
				customRestorePlanTableFQNs := []string{"schema1.table1", "schema2.table2", "schema3.table3", "schema3.table3_partition1"}

				matchingEntries := tocfile.GetDataEntriesMatching([]string{}, []string{},
					includeTables, []string{}, customRestorePlanTableFQNs)

				Expect(matchingEntries).To(ConsistOf(
					[]toc.CoordinatorDataEntry{
						{Schema: "schema3", Name: "table3", Oid: 1, AttributeString: "(i)", PartitionRoot: ""},
						{Schema: "schema3", Name: "table3_partition1", Oid: 1, AttributeString: "(i)", PartitionRoot: "table3"},
					},
				))
			})
			It("returns matching entry on exclude root partition table", func() {
				excludeTables := []string{"schema3.table3"}

				matchingEntries := tocfile.GetDataEntriesMatching([]string{}, []string{},
					[]string{}, excludeTables, restorePlanTableFQNs)

				Expect(matchingEntries).To(Equal(
					[]toc.CoordinatorDataEntry{
						{Schema: "schema1", Name: "table1", Oid: 1, AttributeString: "(i)", PartitionRoot: ""},
						{Schema: "schema2", Name: "table2", Oid: 1, AttributeString: "(i)", PartitionRoot: ""},
					},
				))
			})
			It("returns matching entry on exclude leaf partition table", func() {
				excludeTables := []string{"schema3.table3_partition2"}

				matchingEntries := tocfile.GetDataEntriesMatching([]string{}, []string{},
					[]string{}, excludeTables, restorePlanTableFQNs)

				Expect(matchingEntries).To(Equal(
					[]toc.CoordinatorDataEntry{
						{Schema: "schema1", Name: "table1", Oid: 1, AttributeString: "(i)", PartitionRoot: ""},
						{Schema: "schema2", Name: "table2", Oid: 1, AttributeString: "(i)", PartitionRoot: ""},
						{Schema: "schema3", Name: "table3", Oid: 1, AttributeString: "(i)", PartitionRoot: ""},
						{Schema: "schema3", Name: "table3_partition1", Oid: 1, AttributeString: "(i)", PartitionRoot: "table3"},
					},
				))
			})
		})

		Context("Empty restore plan", func() {
			restorePlanTableFQNs := make([]string, 0)

			Specify("That there are no matching entries", func() {
				matchingEntries := tocfile.GetDataEntriesMatching([]string{}, []string{},
					[]string{}, []string{}, restorePlanTableFQNs)

				Expect(matchingEntries).To(BeEmpty())
			})
		})
	})
	Describe("SubstituteRedirectDatabaseInStatements", func() {
		create := toc.StatementWithType{Schema: "", Name: "somedatabase", ObjectType: toc.OBJ_DATABASE, Statement: "CREATE DATABASE somedatabase TEMPLATE template0;\n"}
		wrongCreate := toc.StatementWithType{ObjectType: toc.OBJ_TABLE, Statement: "CREATE DATABASE somedatabase;\n"}
		encoding := toc.StatementWithType{ObjectType: toc.OBJ_DATABASE, Statement: "CREATE DATABASE somedatabase TEMPLATE template0 ENCODING 'UTF8';\n"}
		gucs := toc.StatementWithType{ObjectType: toc.OBJ_DATABASE_GUC, Statement: "ALTER DATABASE somedatabase SET fsync TO off;\n"}
		metadata := toc.StatementWithType{ObjectType: toc.OBJ_DATABASE_METADATA, Statement: "ALTER DATABASE somedatabase OWNER TO testrole;\n\nREVOKE ALL ON DATABASE somedatabase FROM public;\nGRANT ALL ON DATABASE somedatabase TO gpadmin;"}
		oldSpecial := toc.StatementWithType{ObjectType: toc.OBJ_DATABASE, Statement: `CREATE DATABASE "db-special-chär$" TEMPLATE template0 TABLESPACE test_tablespace;

COMMENT ON DATABASE "db-special-chär$" IS 'this is a database comment';`}
		It("can substitute a database name in a CREATE DATABASE statement", func() {
			statements := toc.SubstituteRedirectDatabaseInStatements([]toc.StatementWithType{create}, "somedatabase", "newdatabase")
			Expect(statements[0].Statement).To(Equal("CREATE DATABASE newdatabase TEMPLATE template0;\n"))
		})
		It("can substitute a database name in a CREATE DATABASE statement with encoding", func() {
			statements := toc.SubstituteRedirectDatabaseInStatements([]toc.StatementWithType{encoding}, "somedatabase", "newdatabase")
			Expect(statements[0].Statement).To(Equal("CREATE DATABASE newdatabase TEMPLATE template0 ENCODING 'UTF8';\n"))
		})
		It("can substitute a database name in an ALTER DATABASE OWNER statement", func() {
			statements := toc.SubstituteRedirectDatabaseInStatements([]toc.StatementWithType{metadata}, "somedatabase", "newdatabase")
			Expect(statements[0].Statement).To(Equal("ALTER DATABASE newdatabase OWNER TO testrole;\n\nREVOKE ALL ON DATABASE newdatabase FROM public;\nGRANT ALL ON DATABASE newdatabase TO gpadmin;"))
		})
		It("can substitute a database name in a database GUC statement", func() {
			statements := toc.SubstituteRedirectDatabaseInStatements([]toc.StatementWithType{gucs}, "somedatabase", "newdatabase")
			Expect(statements[0].Statement).To(Equal("ALTER DATABASE newdatabase SET fsync TO off;\n"))
		})
		It("can substitute a database name in a database GUC statement", func() {
			statements := toc.SubstituteRedirectDatabaseInStatements([]toc.StatementWithType{gucs}, "somedatabase", "newdatabase")
			Expect(statements[0].Statement).To(Equal("ALTER DATABASE newdatabase SET fsync TO off;\n"))
		})
		It("doesn't modify a statement of the wrong type", func() {
			statements := toc.SubstituteRedirectDatabaseInStatements([]toc.StatementWithType{wrongCreate}, "somedatabase", "newdatabase")
			Expect(statements[0].Statement).To(Equal("CREATE DATABASE somedatabase;\n"))
		})
		It("can substitute a database name if the old name contained special characters with a tablespace and comment", func() {
			statements := toc.SubstituteRedirectDatabaseInStatements([]toc.StatementWithType{oldSpecial}, `"db-special-chär$"`, "newdatabase")
			Expect(statements[0].Statement).To(Equal("CREATE DATABASE newdatabase TEMPLATE template0 TABLESPACE test_tablespace;\n\nCOMMENT ON DATABASE newdatabase IS 'this is a database comment';"))
		})
		It("can substitute a database name if the new name contained special characters", func() {
			statements := toc.SubstituteRedirectDatabaseInStatements([]toc.StatementWithType{create}, "somedatabase", `"db-special-chär$"`)
			Expect(statements[0].Statement).To(Equal(`CREATE DATABASE "db-special-chär$" TEMPLATE template0;
`))
		})
	})
	Describe("RemoveActiveRoles", func() {
		user1 := toc.StatementWithType{Name: "user1", ObjectType: toc.OBJ_ROLE, Statement: "CREATE ROLE user1 SUPERUSER;\n"}
		user2 := toc.StatementWithType{Name: "user2", ObjectType: toc.OBJ_ROLE, Statement: "CREATE ROLE user2;\n"}
		It("Removes current user from the list of roles to restore", func() {
			resultStatements := toc.RemoveActiveRole("user1", []toc.StatementWithType{user1, user2})

			Expect(resultStatements).To(Equal([]toc.StatementWithType{user2}))
		})
		It("Returns the same list if current user is not in it", func() {
			resultStatements := toc.RemoveActiveRole("user3", []toc.StatementWithType{user1, user2})

			Expect(resultStatements).To(Equal([]toc.StatementWithType{user1, user2}))
		})
	})
	Describe("GetIncludedPartitionRoots", func() {
		It("does not return anything if relations are not leaf partitions", func() {
			tocfile.AddCoordinatorDataEntry("schema0", "name0", 0, "attribute0", 1, "", "", false)
			tocfile.AddCoordinatorDataEntry("schema1", "name1", 1, "attribute0", 1, "", "", false)
			roots := toc.GetIncludedPartitionRoots(tocfile.DataEntries, []string{"schema0.name0", "schema1.name1"})
			Expect(roots).To(BeEmpty())
		})
		It("returns root parition of leaf partitions", func() {
			tocfile.AddCoordinatorDataEntry("schema0", "name0", 2, "attribute0", 1, "root0", "", false)
			tocfile.AddCoordinatorDataEntry("schema1", "name1", 3, "attribute0", 1, "root1", "", false)
			roots := toc.GetIncludedPartitionRoots(tocfile.DataEntries, []string{"schema0.name0", "schema1.name1"})
			Expect(roots).To(ConsistOf("schema0.root0", "schema1.root1"))
		})
		It("only returns root partitions of leaf partitions", func() {
			tocfile.AddCoordinatorDataEntry("schema0", "name0", 0, "attribute0", 1, "", "", false)
			tocfile.AddCoordinatorDataEntry("schema1", "name1", 1, "attribute0", 1, "", "", false)
			tocfile.AddCoordinatorDataEntry("schema2", "name2", 2, "attribute0", 1, "root2", "", false)
			tocfile.AddCoordinatorDataEntry("schema3", "name3", 3, "attribute0", 1, "root3", "", false)
			roots := toc.GetIncludedPartitionRoots(tocfile.DataEntries, []string{"schema2.name2", "schema3.name3"})
			Expect(roots).To(ConsistOf("schema2.root2", "schema3.root3"))
		})
		It("returns nothing if toc data entries are empty", func() {
			roots := toc.GetIncludedPartitionRoots(tocfile.DataEntries, []string{"schema2.name2", "schema3.name3"})
			Expect(roots).To(BeEmpty())
		})
		It("returns nothing if relation is not part of TOC data entries", func() {
			tocfile.AddCoordinatorDataEntry("schema0", "name0", 0, "attribute0", 1, "", "", false)
			tocfile.AddCoordinatorDataEntry("schema1", "name1", 1, "attribute0", 1, "", "", false)
			tocfile.AddCoordinatorDataEntry("schema2", "name2", 2, "attribute0", 1, "root2", "", false)
			tocfile.AddCoordinatorDataEntry("schema3", "name3", 3, "attribute0", 1, "root3", "", false)
			roots := toc.GetIncludedPartitionRoots(tocfile.DataEntries, []string{"schema4.name4", "schema5.name5"})
			Expect(roots).To(BeEmpty())
		})
		It("returns empty if no relations are passed in", func() {
			tocfile.AddCoordinatorDataEntry("schema0", "name0", 0, "attribute0", 1, "", "", false)
			tocfile.AddCoordinatorDataEntry("schema1", "name1", 1, "attribute0", 1, "", "", false)
			tocfile.AddCoordinatorDataEntry("schema2", "name2", 2, "attribute0", 1, "root2", "", false)
			tocfile.AddCoordinatorDataEntry("schema3", "name3", 3, "attribute0", 1, "root3", "", false)
			roots := toc.GetIncludedPartitionRoots(tocfile.DataEntries, []string{})
			Expect(roots).To(BeEmpty())
		})
	})
})
