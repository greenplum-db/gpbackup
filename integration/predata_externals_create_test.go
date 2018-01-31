package integration

import (
	"os"

	"github.com/greenplum-db/gp-common-go-libs/structmatcher"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup integration create statement tests", func() {
	BeforeEach(func() {
		toc, backupfile = testutils.InitializeTestTOC(buffer, "predata")
	})
	Describe("PrintExternalTableCreateStatement", func() {
		var (
			extTable  backup.ExternalTableDefinition
			testTable backup.Relation
			tableDef  backup.TableDefinition
		)
		BeforeEach(func() {
			extTable = backup.ExternalTableDefinition{Oid: 0, Type: 0, Protocol: backup.FILE, Location: "file://tmp/ext_table_file",
				ExecLocation: "ALL_SEGMENTS", FormatType: "t", FormatOpts: "delimiter '	' null '\\N' escape '\\'",
				Options: "", Command: "", RejectLimit: 0, RejectLimitType: "", ErrTable: "", Encoding: "UTF8",
				Writable: false, URIs: []string{"file://tmp/ext_table_file"}}
			testTable = backup.BasicRelation("public", "testtable")
			tableDef = backup.TableDefinition{IsExternal: true}
			os.Create("/tmp/ext_table_file")
		})
		AfterEach(func() {
			os.Remove("/tmp/ext_table_file")
			testutils.AssertQueryRuns(connection, "DROP EXTERNAL TABLE testtable")
			testutils.AssertQueryRuns(connection, "DROP TABLE IF EXISTS err_table")
		})
		It("creates a READABLE EXTERNAL table", func() {
			extTable.Type = backup.READABLE
			extTable.Writable = false
			extTable.ErrTable = "testtable"
			extTable.RejectLimit = 2
			extTable.RejectLimitType = "r"
			tableDef.ExtTableDef = extTable

			backup.PrintExternalTableCreateStatement(backupfile, toc, testTable, tableDef)

			testutils.AssertQueryRuns(connection, buffer.String())

			oid := testutils.OidFromObjectName(connection, "public", "testtable", backup.TYPE_RELATION)
			resultTableDefs := backup.GetExternalTableDefinitions(connection)
			resultTableDef := resultTableDefs[oid]
			resultTableDef.Type, resultTableDef.Protocol = backup.DetermineExternalTableCharacteristics(resultTableDef)

			structmatcher.ExpectStructsToMatchExcluding(&extTable, &resultTableDef, "Oid")
		})
		It("creates a READABLE EXTERNAL table with LOG ERRORS INTO", func() {
			testutils.SkipIfNot4(connection)
			extTable.Type = backup.READABLE
			extTable.Writable = false
			extTable.ErrTable = "err_table"
			extTable.RejectLimit = 2
			extTable.RejectLimitType = "r"
			tableDef.ExtTableDef = extTable

			backup.PrintExternalTableCreateStatement(backupfile, toc, testTable, tableDef)

			testutils.AssertQueryRuns(connection, buffer.String())

			oid := testutils.OidFromObjectName(connection, "public", "testtable", backup.TYPE_RELATION)
			resultTableDefs := backup.GetExternalTableDefinitions(connection)
			resultTableDef := resultTableDefs[oid]
			resultTableDef.Type, resultTableDef.Protocol = backup.DetermineExternalTableCharacteristics(resultTableDef)

			structmatcher.ExpectStructsToMatchExcluding(&extTable, &resultTableDef, "Oid")
		})
		It("creates a WRITABLE EXTERNAL table", func() {
			extTable.Type = backup.WRITABLE
			extTable.Writable = true
			extTable.Location = "gpfdist://outputhost:8081/data1.out"
			extTable.URIs = []string{"gpfdist://outputhost:8081/data1.out"}
			extTable.Protocol = backup.GPFDIST
			tableDef.ExtTableDef = extTable

			backup.PrintExternalTableCreateStatement(backupfile, toc, testTable, tableDef)

			testutils.AssertQueryRuns(connection, buffer.String())

			oid := testutils.OidFromObjectName(connection, "public", "testtable", backup.TYPE_RELATION)
			resultTableDefs := backup.GetExternalTableDefinitions(connection)
			resultTableDef := resultTableDefs[oid]
			resultTableDef.Type, resultTableDef.Protocol = backup.DetermineExternalTableCharacteristics(resultTableDef)

			structmatcher.ExpectStructsToMatchExcluding(&extTable, &resultTableDef, "Oid")
		})
	})
	Describe("PrintCreateExternalProtocolStatements", func() {
		funcInfoMap := map[uint32]backup.FunctionInfo{
			1: {QualifiedName: "public.write_to_s3", Arguments: "", IsInternal: false},
			2: {QualifiedName: "public.read_from_s3", Arguments: "", IsInternal: false},
		}
		protocolReadOnly := backup.ExternalProtocol{Oid: 1, Name: "s3_read", Owner: "testrole", Trusted: true, ReadFunction: 2, WriteFunction: 0, Validator: 0}
		protocolWriteOnly := backup.ExternalProtocol{Oid: 1, Name: "s3_write", Owner: "testrole", Trusted: false, ReadFunction: 0, WriteFunction: 1, Validator: 0}
		protocolReadWrite := backup.ExternalProtocol{Oid: 1, Name: "s3_read_write", Owner: "testrole", Trusted: false, ReadFunction: 2, WriteFunction: 1, Validator: 0}
		emptyMetadataMap := backup.MetadataMap{}

		It("creates a trusted protocol with a read function, privileges, and an owner", func() {
			externalProtocols := []backup.ExternalProtocol{protocolReadOnly}
			protoMetadataMap := testutils.DefaultMetadataMap("PROTOCOL", true, true, false)

			backup.PrintCreateExternalProtocolStatements(backupfile, toc, externalProtocols, funcInfoMap, protoMetadataMap)

			testutils.AssertQueryRuns(connection, "CREATE OR REPLACE FUNCTION read_from_s3() RETURNS integer AS '$libdir/gps3ext.so', 's3_import' LANGUAGE C STABLE;")
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION read_from_s3()")

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP PROTOCOL s3_read")

			resultExternalProtocols := backup.GetExternalProtocols(connection)

			Expect(len(resultExternalProtocols)).To(Equal(1))
			structmatcher.ExpectStructsToMatchExcluding(&protocolReadOnly, &resultExternalProtocols[0], "Oid", "ReadFunction")
		})
		It("creates a protocol with a write function", func() {
			externalProtocols := []backup.ExternalProtocol{protocolWriteOnly}

			backup.PrintCreateExternalProtocolStatements(backupfile, toc, externalProtocols, funcInfoMap, emptyMetadataMap)

			testutils.AssertQueryRuns(connection, "CREATE OR REPLACE FUNCTION write_to_s3() RETURNS integer AS '$libdir/gps3ext.so', 's3_export' LANGUAGE C STABLE;")
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION write_to_s3()")

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP PROTOCOL s3_write")

			resultExternalProtocols := backup.GetExternalProtocols(connection)

			Expect(len(resultExternalProtocols)).To(Equal(1))
			structmatcher.ExpectStructsToMatchExcluding(&protocolWriteOnly, &resultExternalProtocols[0], "Oid", "WriteFunction")
		})
		It("creates a protocol with a read and write function", func() {
			externalProtocols := []backup.ExternalProtocol{protocolReadWrite}

			backup.PrintCreateExternalProtocolStatements(backupfile, toc, externalProtocols, funcInfoMap, emptyMetadataMap)

			testutils.AssertQueryRuns(connection, "CREATE OR REPLACE FUNCTION read_from_s3() RETURNS integer AS '$libdir/gps3ext.so', 's3_import' LANGUAGE C STABLE;")
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION read_from_s3()")

			testutils.AssertQueryRuns(connection, "CREATE OR REPLACE FUNCTION write_to_s3() RETURNS integer AS '$libdir/gps3ext.so', 's3_export' LANGUAGE C STABLE;")
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION write_to_s3()")

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP PROTOCOL s3_read_write")

			resultExternalProtocols := backup.GetExternalProtocols(connection)

			Expect(len(resultExternalProtocols)).To(Equal(1))
			structmatcher.ExpectStructsToMatchExcluding(&protocolReadWrite, &resultExternalProtocols[0], "Oid", "ReadFunction", "WriteFunction")
		})
	})
	Describe("PrintExchangeExternalPartitionStatements", func() {
		tables := []backup.Relation{
			{Oid: 1, Schema: "public", Name: "part_tbl_ext_part_"},
			{Oid: 2, Schema: "public", Name: "part_tbl"},
		}
		emptyPartInfoMap := make(map[uint32]backup.PartitionInfo, 0)
		AfterEach(func() {
			testutils.AssertQueryRuns(connection, "DROP TABLE part_tbl")
		})
		It("writes an alter statement for a named list partition", func() {
			externalPartition := backup.PartitionInfo{
				PartitionRuleOid:       1,
				PartitionParentRuleOid: 0,
				ParentRelationOid:      2,
				ParentSchema:           "public",
				ParentRelationName:     "part_tbl",
				RelationOid:            1,
				PartitionName:          "girls",
				PartitionRank:          0,
				IsExternal:             true,
			}
			testutils.AssertQueryRuns(connection, `
CREATE TABLE part_tbl (id int, gender char(1))
DISTRIBUTED BY (id)
PARTITION BY LIST (gender)
( PARTITION girls VALUES ('F'),
  PARTITION boys VALUES ('M'),
  DEFAULT PARTITION other );`)
			testutils.AssertQueryRuns(connection, `
CREATE EXTERNAL WEB TABLE part_tbl_ext_part_ (like part_tbl_1_prt_girls)
EXECUTE 'echo -e "2\n1"' on host
FORMAT 'csv';`)
			externalPartitions := []backup.PartitionInfo{externalPartition}

			backup.PrintExchangeExternalPartitionStatements(backupfile, toc, externalPartitions, emptyPartInfoMap, tables)
			testutils.AssertQueryRuns(connection, buffer.String())

			resultExtPartitions, resultPartInfoMap := backup.GetExternalPartitionInfo(connection)
			Expect(len(resultExtPartitions)).To(Equal(1))
			Expect(len(resultPartInfoMap)).To(Equal(3))
			structmatcher.ExpectStructsToMatchExcluding(&externalPartition, &resultExtPartitions[0], "PartitionRuleOid", "RelationOid", "ParentRelationOid")
		})
		It("writes an alter statement for an unnamed range partition", func() {
			externalPartition := backup.PartitionInfo{
				PartitionRuleOid:       1,
				PartitionParentRuleOid: 0,
				ParentRelationOid:      2,
				ParentSchema:           "public",
				ParentRelationName:     "part_tbl",
				RelationOid:            1,
				PartitionName:          "",
				PartitionRank:          1,
				IsExternal:             true,
			}
			testutils.AssertQueryRuns(connection, `
CREATE TABLE part_tbl (a int)
DISTRIBUTED BY (a)
PARTITION BY RANGE (a)
(start(1) end(3) every(1));`)
			testutils.AssertQueryRuns(connection, `
CREATE EXTERNAL WEB TABLE part_tbl_ext_part_ (like part_tbl_1_prt_1)
EXECUTE 'echo -e "2\n1"' on host
FORMAT 'csv';`)
			externalPartitions := []backup.PartitionInfo{externalPartition}

			backup.PrintExchangeExternalPartitionStatements(backupfile, toc, externalPartitions, emptyPartInfoMap, tables)
			testutils.AssertQueryRuns(connection, buffer.String())

			resultExtPartitions, resultPartInfoMap := backup.GetExternalPartitionInfo(connection)
			Expect(len(resultExtPartitions)).To(Equal(1))
			Expect(len(resultPartInfoMap)).To(Equal(2))
			structmatcher.ExpectStructsToMatchExcluding(&externalPartition, &resultExtPartitions[0], "PartitionRuleOid", "RelationOid", "ParentRelationOid")
		})
		It("writes an alter statement for a two level partition", func() {
			testutils.SkipIf4(connection)
			externalPartition := backup.PartitionInfo{
				PartitionRuleOid:       10,
				PartitionParentRuleOid: 11,
				ParentRelationOid:      2,
				ParentSchema:           "public",
				ParentRelationName:     "part_tbl",
				RelationOid:            1,
				PartitionName:          "apj",
				PartitionRank:          0,
				IsExternal:             true,
			}
			externalPartitionParent := backup.PartitionInfo{
				PartitionRuleOid:       11,
				PartitionParentRuleOid: 0,
				ParentRelationOid:      2,
				ParentSchema:           "public",
				ParentRelationName:     "part_tbl",
				RelationOid:            0,
				PartitionName:          "Dec16",
				PartitionRank:          0,
				IsExternal:             false,
			}
			testutils.AssertQueryRuns(connection, `
CREATE TABLE part_tbl (a int,b date,c text,d int)
DISTRIBUTED BY (a)
PARTITION BY RANGE (b)
SUBPARTITION BY LIST (c)
SUBPARTITION TEMPLATE
(SUBPARTITION usa values ('usa'),
SUBPARTITION apj values ('apj'),
SUBPARTITION eur values ('eur'))
( PARTITION Sep16 START (date '2016-09-01') INCLUSIVE ,
  PARTITION Oct16 START (date '2016-10-01') INCLUSIVE ,
  PARTITION Nov16 START (date '2016-11-01') INCLUSIVE ,
  PARTITION Dec16 START (date '2016-12-01') INCLUSIVE
                  END (date '2017-01-01') EXCLUSIVE);
`)

			testutils.AssertQueryRuns(connection, `CREATE EXTERNAL TABLE part_tbl_ext_part_ (a int,b date,c text,d int) LOCATION ('gpfdist://127.0.0.1/apj') FORMAT 'text';`)
			partInfoMap := map[uint32]backup.PartitionInfo{externalPartitionParent.PartitionRuleOid: externalPartitionParent}
			externalPartitions := []backup.PartitionInfo{externalPartition}

			backup.PrintExchangeExternalPartitionStatements(backupfile, toc, externalPartitions, partInfoMap, tables)
			testutils.AssertQueryRuns(connection, buffer.String())

			resultExtPartitions, _ := backup.GetExternalPartitionInfo(connection)
			externalPartition.RelationOid = testutils.OidFromObjectName(connection, "public", "part_tbl_1_prt_dec16_2_prt_apj", backup.TYPE_RELATION)
			Expect(len(resultExtPartitions)).To(Equal(1))
			structmatcher.ExpectStructsToMatchExcluding(&externalPartition, &resultExtPartitions[0], "PartitionRuleOid", "PartitionParentRuleOid", "ParentRelationOid")
		})
		It("writes an alter statement for a three level partition", func() {
			testutils.SkipIf4(connection)
			externalPartition := backup.PartitionInfo{
				PartitionRuleOid:       10,
				PartitionParentRuleOid: 11,
				ParentRelationOid:      2,
				ParentSchema:           "public",
				ParentRelationName:     "part_tbl",
				RelationOid:            1,
				PartitionName:          "europe",
				PartitionRank:          0,
				IsExternal:             true,
			}
			externalPartitionParent1 := backup.PartitionInfo{
				PartitionRuleOid:       11,
				PartitionParentRuleOid: 12,
				ParentRelationOid:      2,
				ParentSchema:           "public",
				ParentRelationName:     "part_tbl",
				RelationOid:            0,
				PartitionName:          "",
				PartitionRank:          1,
				IsExternal:             false,
			}
			externalPartitionParent2 := backup.PartitionInfo{
				PartitionRuleOid:       12,
				PartitionParentRuleOid: 0,
				ParentRelationOid:      2,
				ParentSchema:           "public",
				ParentRelationName:     "part_tbl",
				RelationOid:            0,
				PartitionName:          "",
				PartitionRank:          3,
				IsExternal:             false,
			}
			testutils.AssertQueryRuns(connection, `
CREATE TABLE part_tbl (id int, year int, month int, day int, region text)
DISTRIBUTED BY (id)
PARTITION BY RANGE (year)
    SUBPARTITION BY RANGE (month)
       SUBPARTITION TEMPLATE (
        START (1) END (4) EVERY (1) )
           SUBPARTITION BY LIST (region)
             SUBPARTITION TEMPLATE (
               SUBPARTITION usa VALUES ('usa'),
               SUBPARTITION europe VALUES ('europe'),
               SUBPARTITION asia VALUES ('asia')
		)
( START (2002) END (2005) EVERY (1));
`)

			testutils.AssertQueryRuns(connection, `CREATE EXTERNAL TABLE part_tbl_ext_part_ (like part_tbl_1_prt_3_2_prt_1_3_prt_europe) LOCATION ('gpfdist://127.0.0.1/apj') FORMAT 'text';`)
			partInfoMap := map[uint32]backup.PartitionInfo{externalPartitionParent1.PartitionRuleOid: externalPartitionParent1, externalPartitionParent2.PartitionRuleOid: externalPartitionParent2}
			externalPartitions := []backup.PartitionInfo{externalPartition}

			backup.PrintExchangeExternalPartitionStatements(backupfile, toc, externalPartitions, partInfoMap, tables)
			testutils.AssertQueryRuns(connection, buffer.String())

			resultExtPartitions, _ := backup.GetExternalPartitionInfo(connection)
			externalPartition.RelationOid = testutils.OidFromObjectName(connection, "public", "part_tbl_1_prt_3_2_prt_1_3_prt_europe", backup.TYPE_RELATION)
			Expect(len(resultExtPartitions)).To(Equal(1))
			structmatcher.ExpectStructsToMatchExcluding(&externalPartition, &resultExtPartitions[0], "PartitionRuleOid", "PartitionParentRuleOid", "ParentRelationOid")
		})
	})
})
