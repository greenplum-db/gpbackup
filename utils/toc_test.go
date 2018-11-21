package utils_test

import (
	"bytes"

	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/utils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("utils/toc tests", func() {
	comment := utils.StatementWithType{ObjectType: "COMMENT", Statement: "-- This is a comment\n"}
	commentLen := uint64(len(comment.Statement))
	create := utils.StatementWithType{Schema: "", Name: "somedatabase", ObjectType: "DATABASE", Statement: "CREATE DATABASE somedatabase TEMPLATE template0;\n"}
	createLen := uint64(len(create.Statement))
	role1 := utils.StatementWithType{Schema: "", Name: "somerole1", ObjectType: "ROLE", Statement: "CREATE ROLE somerole1;\n"}
	role1Len := uint64(len(role1.Statement))
	role2 := utils.StatementWithType{Schema: "", Name: "somerole2", ObjectType: "ROLE", Statement: "CREATE ROLE somerole2;\n"}
	role2Len := uint64(len(role2.Statement))
	sequence := utils.StatementWithType{Schema: "schema", Name: "somesequence", ObjectType: "SEQUENCE", Statement: "CREATE SEQUENCE schema.somesequence"}
	referenceSequence := utils.StatementWithType{Schema: "schema", Name: "somesequence", ObjectType: "SEQUENCE", Statement: "CREATE SEQUENCE schema.somesequence", ReferenceObject: "schema.table2"}
	sequenceLen := uint64(len(sequence.Statement))
	table1 := utils.StatementWithType{Schema: "schema", Name: "table1", ObjectType: "TABLE", Statement: "CREATE TABLE schema.table1"}
	table1Len := uint64(len(table1.Statement))
	table2 := utils.StatementWithType{Schema: "schema2", Name: "table2", ObjectType: "TABLE", Statement: "CREATE TABLE schema2.table2"}
	table2Len := uint64(len(table2.Statement))
	view1 := utils.StatementWithType{Schema: "schema", Name: "view1", ObjectType: "VIEW", Statement: "CREATE VIEW schema.view1"}
	view1Len := uint64(len(view1.Statement))
	sequence1 := utils.StatementWithType{Schema: "schema", Name: "sequence1", ObjectType: "SEQUENCE", Statement: "CREATE SEQUENCE schema.sequence1 START 100"}
	sequence1Len := uint64(len(sequence1.Statement))
	index := utils.StatementWithType{Schema: "schema", Name: "someindex", ObjectType: "INDEX", Statement: "CREATE INDEX someindex ON schema.table(i)", ReferenceObject: "schema.table"}
	indexLen := uint64(len(index.Statement))
	BeforeEach(func() {
		toc, backupfile = testutils.InitializeTestTOC(buffer, "global")
	})
	Describe("GetSQLStatementForObjectTypes", func() {
		// Dummy variables to help clarify which arguments are non-empty in a given test
		var noInObj, noExObj, noInSchema, noExSchema, noInRelation, noExRelation []string
		It("returns statement for a single object type", func() {
			backupfile.ByteCount = commentLen + createLen
			toc.AddMetadataEntry("global", utils.MetadataEntry{"", "somedatabase", "DATABASE", "", 0, 0}, commentLen, backupfile.ByteCount)

			metadataFile := bytes.NewReader([]byte(comment.Statement + create.Statement))
			statements := toc.GetSQLStatementForObjectTypes("global", metadataFile, []string{"DATABASE"}, noExObj, noInSchema, noExSchema, noInRelation, noExRelation)

			Expect(statements).To(Equal([]utils.StatementWithType{create}))
		})
		It("returns statement for multiple object types", func() {
			backupfile.ByteCount = commentLen + createLen
			toc.AddMetadataEntry("global", utils.MetadataEntry{"", "somedatabase", "DATABASE", "", 0, 0}, commentLen, backupfile.ByteCount)
			backupfile.ByteCount += role1Len
			toc.AddMetadataEntry("global", utils.MetadataEntry{"", "somerole1", "ROLE", "", 0, 0}, commentLen+createLen, backupfile.ByteCount)
			backupfile.ByteCount += role2Len
			toc.AddMetadataEntry("global", utils.MetadataEntry{"", "somerole2", "ROLE", "", 0, 0}, commentLen+createLen+role1Len, backupfile.ByteCount)

			metadataFile := bytes.NewReader([]byte(comment.Statement + create.Statement + role1.Statement + role2.Statement))
			statements := toc.GetSQLStatementForObjectTypes("global", metadataFile, []string{"DATABASE", "ROLE"}, noExObj, noInSchema, noExSchema, noInRelation, noExRelation)

			Expect(statements).To(Equal([]utils.StatementWithType{create, role1, role2}))
		})
		It("does not return a statement type listed in the exclude list", func() {
			backupfile.ByteCount = commentLen + createLen
			toc.AddMetadataEntry("global", utils.MetadataEntry{"", "somedatabase", "DATABASE", "", 0, 0}, commentLen, backupfile.ByteCount)
			backupfile.ByteCount += role1Len
			toc.AddMetadataEntry("global", utils.MetadataEntry{"", "somerole1", "ROLE", "", 0, 0}, commentLen+createLen, backupfile.ByteCount)
			backupfile.ByteCount += role2Len
			toc.AddMetadataEntry("global", utils.MetadataEntry{"", "somerole2", "ROLE", "", 0, 0}, commentLen+createLen+role1Len, backupfile.ByteCount)

			metadataFile := bytes.NewReader([]byte(comment.Statement + create.Statement + role1.Statement + role2.Statement))
			statements := toc.GetSQLStatementForObjectTypes("global", metadataFile, noInObj, []string{"DATABASE"}, noInSchema, noExSchema, noInRelation, noExRelation)

			Expect(statements).To(Equal([]utils.StatementWithType{role1, role2}))
		})
		It("returns empty statement when no object types are found", func() {
			backupfile.ByteCount = commentLen + createLen
			toc.AddMetadataEntry("global", utils.MetadataEntry{"", "somedatabase", "DATABASE", "", 0, 0}, commentLen, backupfile.ByteCount)

			metadataFile := bytes.NewReader([]byte(comment.Statement + create.Statement))
			statements := toc.GetSQLStatementForObjectTypes("global", metadataFile, []string{"TABLE"}, noExObj, noInSchema, noExSchema, noInRelation, noExRelation)

			Expect(statements).To(Equal([]utils.StatementWithType{}))
		})
		It("returns statement for a single object type with matching schema", func() {
			backupfile.ByteCount = table1Len
			toc.AddMetadataEntry("global", utils.MetadataEntry{"schema", "table1", "TABLE", "", 0, 0}, 0, backupfile.ByteCount)
			backupfile.ByteCount += table2Len
			toc.AddMetadataEntry("global", utils.MetadataEntry{"schema2", "table2", "TABLE", "", 0, 0}, table1Len, backupfile.ByteCount)
			backupfile.ByteCount += sequenceLen
			toc.AddMetadataEntry("global", utils.MetadataEntry{"schema", "somesequence", "SEQUENCE", "", 0, 0}, table1Len+table2Len, backupfile.ByteCount)

			metadataFile := bytes.NewReader([]byte(table1.Statement + table2.Statement + sequence.Statement))
			statements := toc.GetSQLStatementForObjectTypes("global", metadataFile, []string{"TABLE"}, noExObj, []string{"schema"}, noExSchema, noInRelation, noExRelation)

			Expect(statements).To(Equal([]utils.StatementWithType{table1}))
		})
		It("returns statement for any object type in the include schema", func() {
			backupfile.ByteCount = table1Len
			toc.AddMetadataEntry("global", utils.MetadataEntry{"schema", "table1", "TABLE", "", 0, 0}, 0, backupfile.ByteCount)
			backupfile.ByteCount += table2Len
			toc.AddMetadataEntry("global", utils.MetadataEntry{"schema2", "table2", "TABLE", "", 0, 0}, table1Len, backupfile.ByteCount)
			backupfile.ByteCount += sequenceLen
			toc.AddMetadataEntry("global", utils.MetadataEntry{"schema", "somesequence", "SEQUENCE", "", 0, 0}, table1Len+table2Len, backupfile.ByteCount)

			metadataFile := bytes.NewReader([]byte(table1.Statement + table2.Statement + sequence.Statement))
			statements := toc.GetSQLStatementForObjectTypes("global", metadataFile, noInObj, noExObj, []string{"schema"}, noExSchema, noInRelation, noExRelation)

			Expect(statements).To(Equal([]utils.StatementWithType{table1, sequence}))
		})
		It("returns statement for any object type not in the exclude schema", func() {
			backupfile.ByteCount = table1Len
			toc.AddMetadataEntry("global", utils.MetadataEntry{"schema", "table1", "TABLE", "", 0, 0}, 0, backupfile.ByteCount)
			backupfile.ByteCount += table2Len
			toc.AddMetadataEntry("global", utils.MetadataEntry{"schema2", "table2", "TABLE", "", 0, 0}, table1Len, backupfile.ByteCount)
			backupfile.ByteCount += sequenceLen
			toc.AddMetadataEntry("global", utils.MetadataEntry{"schema", "somesequence", "SEQUENCE", "", 0, 0}, table1Len+table2Len, backupfile.ByteCount)

			metadataFile := bytes.NewReader([]byte(table1.Statement + table2.Statement + sequence.Statement))
			statements := toc.GetSQLStatementForObjectTypes("global", metadataFile, noInObj, noExObj, noInSchema, []string{"schema2"}, noInRelation, noExRelation)

			Expect(statements).To(Equal([]utils.StatementWithType{table1, sequence}))
		})
		It("returns statement for a table matching an included table", func() {
			backupfile.ByteCount = table1Len
			toc.AddMetadataEntry("global", utils.MetadataEntry{"schema", "table1", "TABLE", "", 0, 0}, 0, backupfile.ByteCount)
			backupfile.ByteCount += table2Len
			toc.AddMetadataEntry("global", utils.MetadataEntry{"schema2", "table2", "TABLE", "", 0, 0}, table1Len, backupfile.ByteCount)
			backupfile.ByteCount += sequenceLen
			toc.AddMetadataEntry("global", utils.MetadataEntry{"schema", "somesequence", "SEQUENCE", "", 0, 0}, table1Len+table2Len, backupfile.ByteCount)

			metadataFile := bytes.NewReader([]byte(table1.Statement + table2.Statement + sequence.Statement))
			statements := toc.GetSQLStatementForObjectTypes("global", metadataFile, noInObj, noExObj, noInSchema, noExSchema, []string{"schema.table1"}, noExRelation)

			Expect(statements).To(Equal([]utils.StatementWithType{table1}))
		})
		It("returns statement for a view matching an included view", func() {
			backupfile.ByteCount = view1Len
			toc.AddMetadataEntry("global", utils.MetadataEntry{"schema", "view1", "VIEW", "", 0, 0}, 0, backupfile.ByteCount)

			metadataFile := bytes.NewReader([]byte(view1.Statement))
			statements := toc.GetSQLStatementForObjectTypes("global", metadataFile, noInObj, noExObj, noInSchema, noExSchema, []string{"schema.view1"}, noExRelation)

			Expect(statements).To(Equal([]utils.StatementWithType{view1}))
		})
		It("returns statement for a sequence matching an included sequence", func() {
			backupfile.ByteCount = sequence1Len
			toc.AddMetadataEntry("global", utils.MetadataEntry{"schema", "sequence1", "SEQUENCE", "", 0, 0}, 0, backupfile.ByteCount)

			metadataFile := bytes.NewReader([]byte(sequence1.Statement))
			statements := toc.GetSQLStatementForObjectTypes("global", metadataFile, noInObj, noExObj, noInSchema, noExSchema, []string{"schema.sequence1"}, noExRelation)

			Expect(statements).To(Equal([]utils.StatementWithType{sequence1}))
		})
		It("returns statement for any object type or reference object not matching an excluded table", func() {
			backupfile.ByteCount = table1Len
			toc.AddMetadataEntry("global", utils.MetadataEntry{"schema", "table1", "TABLE", "", 0, 0}, 0, backupfile.ByteCount)
			backupfile.ByteCount += table2Len
			toc.AddMetadataEntry("global", utils.MetadataEntry{"schema2", "table2", "TABLE", "", 0, 0}, table1Len, backupfile.ByteCount)
			backupfile.ByteCount += sequenceLen
			toc.AddMetadataEntry("global", utils.MetadataEntry{"schema", "somesequence", "SEQUENCE", "schema.table2", 0, 0}, table1Len+table2Len, backupfile.ByteCount)

			metadataFile := bytes.NewReader([]byte(table1.Statement + table2.Statement + sequence.Statement))
			statements := toc.GetSQLStatementForObjectTypes("global", metadataFile, noInObj, noExObj, noInSchema, noExSchema, noInRelation, []string{"schema.table1"})

			Expect(statements).To(Equal([]utils.StatementWithType{table2, referenceSequence}))
		})
		It("returns no statements for any object type with reference object matching an excluded table", func() {
			backupfile.ByteCount = table1Len
			toc.AddMetadataEntry("global", utils.MetadataEntry{"schema", "table1", "TABLE", "", 0, 0}, 0, backupfile.ByteCount)
			backupfile.ByteCount += table2Len
			toc.AddMetadataEntry("global", utils.MetadataEntry{"schema2", "table2", "TABLE", "", 0, 0}, table1Len, backupfile.ByteCount)
			backupfile.ByteCount += sequenceLen
			toc.AddMetadataEntry("global", utils.MetadataEntry{"schema", "somesequence", "SEQUENCE", "schema.table1", 0, 0}, table1Len+table2Len, backupfile.ByteCount)
			backupfile.ByteCount += indexLen
			toc.AddMetadataEntry("global", utils.MetadataEntry{"schema", "someindex", "INDEX", "schema.table1", 0, 0}, table1Len+table2Len+sequenceLen, backupfile.ByteCount)

			metadataFile := bytes.NewReader([]byte(table1.Statement + table2.Statement + sequence.Statement + index.Statement))
			statements := toc.GetSQLStatementForObjectTypes("global", metadataFile, noInObj, noExObj, noInSchema, noExSchema, noInRelation, []string{"schema.table1"})

			Expect(statements).To(Equal([]utils.StatementWithType{table2}))
		})
		It("returns no statements for an excluded view or sequence", func() {
			backupfile.ByteCount = view1Len
			toc.AddMetadataEntry("global", utils.MetadataEntry{"schema", "view1", "VIEW", "", 0, 0}, 0, backupfile.ByteCount)
			backupfile.ByteCount += sequence1Len
			toc.AddMetadataEntry("global", utils.MetadataEntry{"schema", "sequence1", "SEQUENCE", "", 0, 0}, view1Len, backupfile.ByteCount)

			metadataFile := bytes.NewReader([]byte(view1.Statement + sequence1.Statement))
			statements := toc.GetSQLStatementForObjectTypes("global", metadataFile, noInObj, noExObj, noInSchema, noExSchema, noInRelation, []string{"schema.view1", "schema.sequence1"})

			Expect(statements).To(Equal([]utils.StatementWithType{}))
		})
		It("returns statement for any object type with matching reference object", func() {
			backupfile.ByteCount = table1Len
			toc.AddMetadataEntry("global", utils.MetadataEntry{"schema", "table1", "INDEX", "", 0, 0}, 0, backupfile.ByteCount)
			backupfile.ByteCount += table2Len
			toc.AddMetadataEntry("global", utils.MetadataEntry{"schema2", "table2", "TABLE", "", 0, 0}, table1Len, backupfile.ByteCount)
			backupfile.ByteCount += indexLen
			toc.AddMetadataEntry("global", utils.MetadataEntry{"schema", "someindex", "INDEX", "schema.table", 0, 0}, table1Len+table2Len, backupfile.ByteCount)

			metadataFile := bytes.NewReader([]byte(table1.Statement + table2.Statement + index.Statement))
			statements := toc.GetSQLStatementForObjectTypes("global", metadataFile, noInObj, noExObj, noInSchema, noExSchema, []string{"schema.table"}, noExRelation)

			Expect(statements).To(Equal([]utils.StatementWithType{index}))

		})
		It("returns no statements for a non-relation object with matching name from relation list", func() {
			backupfile.ByteCount = table1Len
			toc.AddMetadataEntry("global", utils.MetadataEntry{"schema", "table1", "TABLE", "", 0, 0}, 0, backupfile.ByteCount)
			backupfile.ByteCount += table2Len
			toc.AddMetadataEntry("global", utils.MetadataEntry{"schema2", "table2", "TABLE", "", 0, 0}, table1Len, backupfile.ByteCount)
			backupfile.ByteCount += sequenceLen
			toc.AddMetadataEntry("global", utils.MetadataEntry{"schema", "someindex", "INDEX", "", 0, 0}, table1Len+table2Len, backupfile.ByteCount)

			metadataFile := bytes.NewReader([]byte(table1.Statement + table2.Statement + index.Statement))
			statements := toc.GetSQLStatementForObjectTypes("global", metadataFile, noInObj, noExObj, noInSchema, noExSchema, []string{"schema.someindex"}, noExRelation)

			Expect(statements).To(Equal([]utils.StatementWithType{}))
		})
	})
	Describe("GetDataEntriesMatching", func() {
		BeforeEach(func() {
			toc.AddMasterDataEntry("schema1", "table1", 1, "(i)", 0, "")
			toc.AddMasterDataEntry("schema2", "table2", 1, "(i)", 0, "")
			toc.AddMasterDataEntry("schema3", "table3", 1, "(i)", 0, "")
			toc.AddMasterDataEntry("schema3", "table3_partition1", 1, "(i)", 0, "table3")
			toc.AddMasterDataEntry("schema3", "table3_partition2", 1, "(i)", 0, "table3")
		})
		Context("Non-empty restore plan", func() {
			restorePlanTableFQNs := []string{"schema1.table1", "schema2.table2", "schema3.table3", "schema3.table3_partition1", "schema3.table3_partition2"}

			It("returns matching entry on include schema", func() {
				includeSchemas := []string{"schema1"}

				matchingEntries := toc.GetDataEntriesMatching(includeSchemas, []string{},
					[]string{}, []string{}, restorePlanTableFQNs)

				Expect(matchingEntries).To(Equal(
					[]utils.MasterDataEntry{
						{Schema: "schema1", Name: "table1", Oid: 1, AttributeString: "(i)"},
					},
				))
			})
			It("returns matching entry on exclude schema", func() {
				excludeSchemas := []string{"schema2"}

				matchingEntries := toc.GetDataEntriesMatching([]string{}, excludeSchemas,
					[]string{}, []string{}, restorePlanTableFQNs)

				Expect(matchingEntries).To(ConsistOf(
					[]utils.MasterDataEntry{
						{Schema: "schema1", Name: "table1", Oid: 1, AttributeString: "(i)", PartitionRoot: ""},
						{Schema: "schema3", Name: "table3", Oid: 1, AttributeString: "(i)", PartitionRoot: ""},
						{Schema: "schema3", Name: "table3_partition1", Oid: 1, AttributeString: "(i)", PartitionRoot: "table3"},
						{Schema: "schema3", Name: "table3_partition2", Oid: 1, AttributeString: "(i)", PartitionRoot: "table3"},
					},
				))
			})
			It("returns matching entry on include table", func() {
				includeTables := []string{"schema1.table1"}

				matchingEntries := toc.GetDataEntriesMatching([]string{}, []string{},
					includeTables, []string{}, restorePlanTableFQNs)

				Expect(matchingEntries).To(Equal(
					[]utils.MasterDataEntry{
						{Schema: "schema1", Name: "table1", Oid: 1, AttributeString: "(i)"},
					},
				))
			})
			It("returns matching entry on exclude table", func() {
				excludeTables := []string{"schema2.table2"}

				matchingEntries := toc.GetDataEntriesMatching([]string{}, []string{},
					[]string{}, excludeTables, restorePlanTableFQNs)

				Expect(matchingEntries).To(ConsistOf(
					[]utils.MasterDataEntry{
						{Schema: "schema1", Name: "table1", Oid: 1, AttributeString: "(i)", PartitionRoot: ""},
						{Schema: "schema3", Name: "table3", Oid: 1, AttributeString: "(i)", PartitionRoot: ""},
						{Schema: "schema3", Name: "table3_partition1", Oid: 1, AttributeString: "(i)", PartitionRoot: "table3"},
						{Schema: "schema3", Name: "table3_partition2", Oid: 1, AttributeString: "(i)", PartitionRoot: "table3"},
					},
				))
			})
			It("returns all entries when not schema-filtered or table-filtered", func() {
				matchingEntries := toc.GetDataEntriesMatching([]string{}, []string{},
					[]string{}, []string{}, restorePlanTableFQNs)

				Expect(matchingEntries).To(ConsistOf(
					[]utils.MasterDataEntry{
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

				matchingEntries := toc.GetDataEntriesMatching([]string{}, []string{},
					includeTables, []string{}, restorePlanTableFQNs)

				Expect(matchingEntries).To(ConsistOf(
					[]utils.MasterDataEntry{
						{Schema: "schema3", Name: "table3", Oid: 1, AttributeString: "(i)", PartitionRoot: ""},
						{Schema: "schema3", Name: "table3_partition1", Oid: 1, AttributeString: "(i)", PartitionRoot: "table3"},
						{Schema: "schema3", Name: "table3_partition2", Oid: 1, AttributeString: "(i)", PartitionRoot: "table3"},
					},
				))
			})
			It("does not return leaf partitions in include tables but not in the restore plan", func() {
				includeTables := []string{"schema3.table3"}
				customRestorePlanTableFQNs := []string{"schema1.table1", "schema2.table2", "schema3.table3", "schema3.table3_partition1"}

				matchingEntries := toc.GetDataEntriesMatching([]string{}, []string{},
					includeTables, []string{}, customRestorePlanTableFQNs)

				Expect(matchingEntries).To(ConsistOf(
					[]utils.MasterDataEntry{
						{Schema: "schema3", Name: "table3", Oid: 1, AttributeString: "(i)", PartitionRoot: ""},
						{Schema: "schema3", Name: "table3_partition1", Oid: 1, AttributeString: "(i)", PartitionRoot: "table3"},
					},
				))
			})
			It("returns matching entry on exclude root partition table", func() {
				excludeTables := []string{"schema3.table3"}

				matchingEntries := toc.GetDataEntriesMatching([]string{}, []string{},
					[]string{}, excludeTables, restorePlanTableFQNs)

				Expect(matchingEntries).To(Equal(
					[]utils.MasterDataEntry{
						{Schema: "schema1", Name: "table1", Oid: 1, AttributeString: "(i)", PartitionRoot: ""},
						{Schema: "schema2", Name: "table2", Oid: 1, AttributeString: "(i)", PartitionRoot: ""},
					},
				))
			})
			It("returns matching entry on exclude leaf partition table", func() {
				excludeTables := []string{"schema3.table3_partition2"}

				matchingEntries := toc.GetDataEntriesMatching([]string{}, []string{},
					[]string{}, excludeTables, restorePlanTableFQNs)

				Expect(matchingEntries).To(Equal(
					[]utils.MasterDataEntry{
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
				matchingEntries := toc.GetDataEntriesMatching([]string{}, []string{},
					[]string{}, []string{}, restorePlanTableFQNs)

				Expect(matchingEntries).To(BeEmpty())
			})
		})
	})
	Describe("SubstituteRedirectDatabaseInStatements", func() {
		wrongCreate := utils.StatementWithType{ObjectType: "TABLE", Statement: "CREATE DATABASE somedatabase;\n"}
		encoding := utils.StatementWithType{ObjectType: "DATABASE", Statement: "CREATE DATABASE somedatabase TEMPLATE template0 ENCODING 'UTF8';\n"}
		gucs := utils.StatementWithType{ObjectType: "DATABASE GUC", Statement: "ALTER DATABASE somedatabase SET fsync TO off;\n"}
		metadata := utils.StatementWithType{ObjectType: "DATABASE METADATA", Statement: "ALTER DATABASE somedatabase OWNER TO testrole;\n\nREVOKE ALL ON DATABASE somedatabase FROM public;\nGRANT ALL ON DATABASE somedatabase TO gpadmin;"}
		oldSpecial := utils.StatementWithType{ObjectType: "DATABASE", Statement: `CREATE DATABASE "db-special-chär$" TEMPLATE template0 TABLESPACE test_tablespace;

COMMENT ON DATABASE "db-special-chär$" IS 'this is a database comment';`}
		It("can substitute a database name in a CREATE DATABASE statement", func() {
			statements := utils.SubstituteRedirectDatabaseInStatements([]utils.StatementWithType{create}, "somedatabase", "newdatabase")
			Expect(statements[0].Statement).To(Equal("CREATE DATABASE newdatabase TEMPLATE template0;\n"))
		})
		It("can substitute a database name in a CREATE DATABASE statement with encoding", func() {
			statements := utils.SubstituteRedirectDatabaseInStatements([]utils.StatementWithType{encoding}, "somedatabase", "newdatabase")
			Expect(statements[0].Statement).To(Equal("CREATE DATABASE newdatabase TEMPLATE template0 ENCODING 'UTF8';\n"))
		})
		It("can substitute a database name in an ALTER DATABASE OWNER statement", func() {
			statements := utils.SubstituteRedirectDatabaseInStatements([]utils.StatementWithType{metadata}, "somedatabase", "newdatabase")
			Expect(statements[0].Statement).To(Equal("ALTER DATABASE newdatabase OWNER TO testrole;\n\nREVOKE ALL ON DATABASE newdatabase FROM public;\nGRANT ALL ON DATABASE newdatabase TO gpadmin;"))
		})
		It("can substitute a database name in a database GUC statement", func() {
			statements := utils.SubstituteRedirectDatabaseInStatements([]utils.StatementWithType{gucs}, "somedatabase", "newdatabase")
			Expect(statements[0].Statement).To(Equal("ALTER DATABASE newdatabase SET fsync TO off;\n"))
		})
		It("can substitute a database name in a database GUC statement", func() {
			statements := utils.SubstituteRedirectDatabaseInStatements([]utils.StatementWithType{gucs}, "somedatabase", "newdatabase")
			Expect(statements[0].Statement).To(Equal("ALTER DATABASE newdatabase SET fsync TO off;\n"))
		})
		It("doesn't modify a statement of the wrong type", func() {
			statements := utils.SubstituteRedirectDatabaseInStatements([]utils.StatementWithType{wrongCreate}, "somedatabase", "newdatabase")
			Expect(statements[0].Statement).To(Equal("CREATE DATABASE somedatabase;\n"))
		})
		It("can substitute a database name if the old name contained special characters with a tablespace and comment", func() {
			statements := utils.SubstituteRedirectDatabaseInStatements([]utils.StatementWithType{oldSpecial}, `"db-special-chär$"`, "newdatabase")
			Expect(statements[0].Statement).To(Equal("CREATE DATABASE newdatabase TEMPLATE template0 TABLESPACE test_tablespace;\n\nCOMMENT ON DATABASE newdatabase IS 'this is a database comment';"))
		})
		It("can substitute a database name if the new name contained special characters", func() {
			statements := utils.SubstituteRedirectDatabaseInStatements([]utils.StatementWithType{create}, "somedatabase", `"db-special-chär$"`)
			Expect(statements[0].Statement).To(Equal(`CREATE DATABASE "db-special-chär$" TEMPLATE template0;
`))
		})
	})
	Describe("RemoveActiveRoles", func() {
		user1 := utils.StatementWithType{Name: "user1", ObjectType: "ROLE", Statement: "CREATE ROLE user1 SUPERUSER;\n"}
		user2 := utils.StatementWithType{Name: "user2", ObjectType: "ROLE", Statement: "CREATE ROLE user2;\n"}
		It("Removes current user from the list of roles to restore", func() {
			resultStatements := utils.RemoveActiveRole("user1", []utils.StatementWithType{user1, user2})

			Expect(resultStatements).To(Equal([]utils.StatementWithType{user2}))
		})
		It("Returns the same list if current user is not in it", func() {
			resultStatements := utils.RemoveActiveRole("user3", []utils.StatementWithType{user1, user2})

			Expect(resultStatements).To(Equal([]utils.StatementWithType{user1, user2}))
		})
	})
	Describe("GetIncludedPartitionRoots", func() {
		It("does not return anything if relations are not leaf partitions", func() {
			toc.AddMasterDataEntry("schema0", "name0", 0, "attribute0", 1, "")
			toc.AddMasterDataEntry("schema1", "name1", 1, "attribute0", 1, "")
			roots := utils.GetIncludedPartitionRoots(toc.DataEntries, []string{"schema0.name0", "schema1.name1"})
			Expect(roots).To(BeEmpty())
		})
		It("returns root parition of leaf partitions", func() {
			toc.AddMasterDataEntry("schema0", "name0", 2, "attribute0", 1, "root0")
			toc.AddMasterDataEntry("schema1", "name1", 3, "attribute0", 1, "root1")
			roots := utils.GetIncludedPartitionRoots(toc.DataEntries, []string{"schema0.name0", "schema1.name1"})
			Expect(roots).To(ConsistOf("schema0.root0", "schema1.root1"))
		})
		It("only returns root partitions of leaf partitions", func() {
			toc.AddMasterDataEntry("schema0", "name0", 0, "attribute0", 1, "")
			toc.AddMasterDataEntry("schema1", "name1", 1, "attribute0", 1, "")
			toc.AddMasterDataEntry("schema2", "name2", 2, "attribute0", 1, "root2")
			toc.AddMasterDataEntry("schema3", "name3", 3, "attribute0", 1, "root3")
			roots := utils.GetIncludedPartitionRoots(toc.DataEntries, []string{"schema2.name2", "schema3.name3"})
			Expect(roots).To(ConsistOf("schema2.root2", "schema3.root3"))
		})
		It("returns nothing if toc data entries are empty", func() {
			roots := utils.GetIncludedPartitionRoots(toc.DataEntries, []string{"schema2.name2", "schema3.name3"})
			Expect(roots).To(BeEmpty())
		})
		It("returns nothing if relation is not part of TOC data entries", func() {
			toc.AddMasterDataEntry("schema0", "name0", 0, "attribute0", 1, "")
			toc.AddMasterDataEntry("schema1", "name1", 1, "attribute0", 1, "")
			toc.AddMasterDataEntry("schema2", "name2", 2, "attribute0", 1, "root2")
			toc.AddMasterDataEntry("schema3", "name3", 3, "attribute0", 1, "root3")
			roots := utils.GetIncludedPartitionRoots(toc.DataEntries, []string{"schema4.name4", "schema5.name5"})
			Expect(roots).To(BeEmpty())
		})
		It("returns empty if no relations are passed in", func() {
			toc.AddMasterDataEntry("schema0", "name0", 0, "attribute0", 1, "")
			toc.AddMasterDataEntry("schema1", "name1", 1, "attribute0", 1, "")
			toc.AddMasterDataEntry("schema2", "name2", 2, "attribute0", 1, "root2")
			toc.AddMasterDataEntry("schema3", "name3", 3, "attribute0", 1, "root3")
			roots := utils.GetIncludedPartitionRoots(toc.DataEntries, []string{})
			Expect(roots).To(BeEmpty())
		})
	})
})
