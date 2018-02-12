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
	sequenceLen := uint64(len(sequence.Statement))
	table1 := utils.StatementWithType{Schema: "schema", Name: "table1", ObjectType: "TABLE", Statement: "CREATE TABLE schema.table1"}
	table1Len := uint64(len(table1.Statement))
	table2 := utils.StatementWithType{Schema: "schema", Name: "table2", ObjectType: "TABLE", Statement: "CREATE TABLE schema.table2"}
	table2Len := uint64(len(table2.Statement))
	index := utils.StatementWithType{Schema: "schema", Name: "someindex", ObjectType: "INDEX", Statement: "CREATE INDEX someindex ON schema.table(i)", ReferenceObject: "schema.table"}
	indexLen := uint64(len(index.Statement))
	BeforeEach(func() {
		toc, backupfile = testutils.InitializeTestTOC(buffer, "global")
	})
	Context("GetSqlStatementForObjectTypes", func() {
		It("returns statement for a single object type", func() {
			backupfile.ByteCount = commentLen + createLen
			toc.AddMetadataEntry("", "somedatabase", "DATABASE", "", commentLen, backupfile, "global")

			metadataFile := bytes.NewReader([]byte(comment.Statement + create.Statement))
			statements := toc.GetSQLStatementForObjectTypes("global", metadataFile, []string{"DATABASE"}, []string{}, []string{})

			Expect(statements).To(Equal([]utils.StatementWithType{create}))
		})
		It("returns statement for multiple object types", func() {
			backupfile.ByteCount = commentLen + createLen
			toc.AddMetadataEntry("", "somedatabase", "DATABASE", "", commentLen, backupfile, "global")
			backupfile.ByteCount += role1Len
			toc.AddMetadataEntry("", "somerole1", "ROLE", "", commentLen+createLen, backupfile, "global")
			backupfile.ByteCount += role2Len
			toc.AddMetadataEntry("", "somerole2", "ROLE", "", commentLen+createLen+role1Len, backupfile, "global")

			metadataFile := bytes.NewReader([]byte(comment.Statement + create.Statement + role1.Statement + role2.Statement))
			statements := toc.GetSQLStatementForObjectTypes("global", metadataFile, []string{"DATABASE", "ROLE"}, []string{}, []string{})

			Expect(statements).To(Equal([]utils.StatementWithType{create, role1, role2}))
		})
		It("returns empty statement when no object types are found", func() {
			backupfile.ByteCount = commentLen + createLen
			toc.AddMetadataEntry("", "somedatabase", "DATABASE", "", commentLen, backupfile, "global")

			metadataFile := bytes.NewReader([]byte(comment.Statement + create.Statement))
			statements := toc.GetSQLStatementForObjectTypes("global", metadataFile, []string{"TABLE"}, []string{}, []string{})

			Expect(statements).To(Equal([]utils.StatementWithType{}))
		})
		It("returns statement for a single object type with matching schema", func() {
			backupfile.ByteCount = table1Len
			toc.AddMetadataEntry("schema", "table1", "TABLE", "", 0, backupfile, "global")
			backupfile.ByteCount += table2Len
			toc.AddMetadataEntry("schema2", "table2", "TABLE", "", table1Len, backupfile, "global")
			backupfile.ByteCount += sequenceLen
			toc.AddMetadataEntry("schema", "somesequence", "SEQUENCE", "", table1Len+table2Len, backupfile, "global")

			metadataFile := bytes.NewReader([]byte(table1.Statement + table2.Statement + sequence.Statement))
			statements := toc.GetSQLStatementForObjectTypes("global", metadataFile, []string{"TABLE"}, []string{"schema"}, []string{})

			Expect(statements).To(Equal([]utils.StatementWithType{table1}))
		})
		It("returns statement for any object type with matching schema", func() {
			backupfile.ByteCount = table1Len
			toc.AddMetadataEntry("schema", "table1", "TABLE", "", 0, backupfile, "global")
			backupfile.ByteCount += table2Len
			toc.AddMetadataEntry("schema2", "table2", "TABLE", "", table1Len, backupfile, "global")
			backupfile.ByteCount += sequenceLen
			toc.AddMetadataEntry("schema", "somesequence", "SEQUENCE", "", table1Len+table2Len, backupfile, "global")

			metadataFile := bytes.NewReader([]byte(table1.Statement + table2.Statement + sequence.Statement))
			statements := toc.GetSQLStatementForObjectTypes("global", metadataFile, []string{}, []string{"schema"}, []string{})

			Expect(statements).To(Equal([]utils.StatementWithType{table1, sequence}))
		})
		It("returns statement for any object type with matching table", func() {
			backupfile.ByteCount = table1Len
			toc.AddMetadataEntry("schema", "table1", "TABLE", "", 0, backupfile, "global")
			backupfile.ByteCount += table2Len
			toc.AddMetadataEntry("schema2", "table2", "TABLE", "", table1Len, backupfile, "global")
			backupfile.ByteCount += sequenceLen
			toc.AddMetadataEntry("schema", "somesequence", "SEQUENCE", "", table1Len+table2Len, backupfile, "global")

			metadataFile := bytes.NewReader([]byte(table1.Statement + table2.Statement + sequence.Statement))
			statements := toc.GetSQLStatementForObjectTypes("global", metadataFile, []string{}, []string{}, []string{"schema.table1"})

			Expect(statements).To(Equal([]utils.StatementWithType{table1}))
		})

		It("returns statement for any object type with matching reference object", func() {
			backupfile.ByteCount = table1Len
			toc.AddMetadataEntry("schema", "table1", "INDEX", "", 0, backupfile, "global")
			backupfile.ByteCount += table2Len
			toc.AddMetadataEntry("schema2", "table2", "TABLE", "", table1Len, backupfile, "global")
			backupfile.ByteCount += indexLen
			toc.AddMetadataEntry("schema", "someindex", "INDEX", "schema.table", table1Len+table2Len, backupfile, "global")

			metadataFile := bytes.NewReader([]byte(table1.Statement + table2.Statement + index.Statement))
			statements := toc.GetSQLStatementForObjectTypes("global", metadataFile, []string{}, []string{}, []string{"schema.table"})

			Expect(statements).To(Equal([]utils.StatementWithType{index}))

		})
		It("returns no statements for a non-table object with matching name from table list", func() {
			backupfile.ByteCount = table1Len
			toc.AddMetadataEntry("schema", "table1", "TABLE", "", 0, backupfile, "global")
			backupfile.ByteCount += table2Len
			toc.AddMetadataEntry("schema2", "table2", "TABLE", "", table1Len, backupfile, "global")
			backupfile.ByteCount += sequenceLen
			toc.AddMetadataEntry("schema", "somesequence", "SEQUENCE", "", table1Len+table2Len, backupfile, "global")

			metadataFile := bytes.NewReader([]byte(table1.Statement + table2.Statement + sequence.Statement))
			statements := toc.GetSQLStatementForObjectTypes("global", metadataFile, []string{}, []string{}, []string{"schema.somesequence"})

			Expect(statements).To(Equal([]utils.StatementWithType{}))
		})
	})
	Context("GetDataEntriesMatching", func() {
		It("returns matching entry on schema", func() {
			includeSchemas := []string{"schema1"}
			toc.AddMasterDataEntry("schema1", "table1", 1, "(i)", 0)
			toc.AddMasterDataEntry("schema2", "table2", 1, "(i)", 0)
			matchingEntries := toc.GetDataEntriesMatching(includeSchemas, []string{})
			Expect(matchingEntries).To(Equal([]utils.MasterDataEntry{{Schema: "schema1", Name: "table1", Oid: 1, AttributeString: "(i)"}}))
		})
		It("returns all entries when not schema-filtered or table-filtered", func() {
			toc.AddMasterDataEntry("schema1", "table1", 1, "(i)", 0)
			toc.AddMasterDataEntry("schema2", "table2", 1, "(i)", 0)
			matchingEntries := toc.GetDataEntriesMatching([]string{}, []string{})
			Expect(matchingEntries).To(Equal([]utils.MasterDataEntry{{Schema: "schema1", Name: "table1", Oid: 1, AttributeString: "(i)"}, {Schema: "schema2", Name: "table2", Oid: 1, AttributeString: "(i)"}}))
		})
		It("returns matching entry on table", func() {
			includeTables := []string{"schema1.table1"}
			toc.AddMasterDataEntry("schema1", "table1", 1, "(i)", 0)
			toc.AddMasterDataEntry("schema2", "table2", 1, "(i)", 0)
			matchingEntries := toc.GetDataEntriesMatching([]string{}, includeTables)
			Expect(matchingEntries).To(Equal([]utils.MasterDataEntry{{Schema: "schema1", Name: "table1", Oid: 1, AttributeString: "(i)"}}))
		})
	})

	Context("GetAllSqlStatements", func() {
		It("returns statement for a single object type", func() {
			backupfile.ByteCount = createLen
			toc.AddMetadataEntry("", "somedatabase", "DATABASE", "", 0, backupfile, "global")

			metadataFile := bytes.NewReader([]byte(create.Statement))
			statements := toc.GetAllSQLStatements("global", metadataFile)

			Expect(statements).To(Equal([]utils.StatementWithType{create}))
		})
		It("returns statement for a multiple object types", func() {
			backupfile.ByteCount = createLen
			toc.AddMetadataEntry("", "somedatabase", "DATABASE", "", 0, backupfile, "global")
			backupfile.ByteCount += role1Len
			toc.AddMetadataEntry("", "somerole1", "ROLE", "", createLen, backupfile, "global")
			backupfile.ByteCount += role2Len
			toc.AddMetadataEntry("", "somerole2", "ROLE", "", createLen+role1Len, backupfile, "global")

			metadataFile := bytes.NewReader([]byte(create.Statement + role1.Statement + role2.Statement))
			statements := toc.GetAllSQLStatements("global", metadataFile)

			Expect(statements).To(Equal([]utils.StatementWithType{create, role1, role2}))
		})
		It("returns empty statement when no object types are found", func() {
			metadataFile := bytes.NewReader([]byte(create.Statement))
			statements := toc.GetAllSQLStatements("global", metadataFile)

			Expect(statements).To(Equal([]utils.StatementWithType{}))
		})
	})
	Context("SubstituteRedirectDatabaseInStatements", func() {
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
})
