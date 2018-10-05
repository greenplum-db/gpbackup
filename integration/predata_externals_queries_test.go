package integration

import (
	"github.com/greenplum-db/gp-common-go-libs/structmatcher"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup integration tests", func() {
	Describe("GetExternalTableDefinitions", func() {
		It("returns a slice for a basic external table definition", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.simple_table(i int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.simple_table")
			testhelper.AssertQueryRuns(connectionPool, `CREATE READABLE EXTERNAL TABLE public.ext_table(i int)
LOCATION ('file://tmp/myfile.txt')
FORMAT 'TEXT'`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP EXTERNAL TABLE public.ext_table")
			oid := testutils.OidFromObjectName(connectionPool, "public", "ext_table", backup.TYPE_RELATION)

			results := backup.GetExternalTableDefinitions(connectionPool)
			result := results[oid]

			extTable := backup.ExternalTableDefinition{Oid: 0, Type: 0, Protocol: 0, Location: "file://tmp/myfile.txt",
				ExecLocation: "ALL_SEGMENTS", FormatType: "t", FormatOpts: "delimiter '	' null '\\N' escape '\\'",
				Options: "", Command: "", RejectLimit: 0, RejectLimitType: "", ErrTableName: "", ErrTableSchema: "", Encoding: "UTF8",
				Writable: false, URIs: []string{"file://tmp/myfile.txt"}}
			structmatcher.ExpectStructsToMatchExcluding(&extTable, &result, "Oid")
		})
		It("returns a slice for a basic external web table definition", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.simple_table(i int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.simple_table")
			testhelper.AssertQueryRuns(connectionPool, `CREATE READABLE EXTERNAL WEB TABLE public.ext_table(i int)
EXECUTE 'hostname'
FORMAT 'TEXT'`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP EXTERNAL WEB TABLE public.ext_table")
			oid := testutils.OidFromObjectName(connectionPool, "public", "ext_table", backup.TYPE_RELATION)

			results := backup.GetExternalTableDefinitions(connectionPool)
			result := results[oid]

			extTable := backup.ExternalTableDefinition{Oid: 0, Type: 0, Protocol: 0, Location: "",
				ExecLocation: "ALL_SEGMENTS", FormatType: "t", FormatOpts: "delimiter '	' null '\\N' escape '\\'",
				Options: "", Command: "hostname", RejectLimit: 0, RejectLimitType: "", ErrTableName: "", ErrTableSchema: "", Encoding: "UTF8",
				Writable: false, URIs: nil}

			structmatcher.ExpectStructsToMatchExcluding(&extTable, &result, "Oid")
		})
		It("returns a slice for a complex external table definition", func() {
			testhelper.AssertQueryRuns(connectionPool, `CREATE READABLE EXTERNAL TABLE public.ext_table(i int)
LOCATION ('file://tmp/myfile.txt')
FORMAT 'TEXT'
LOG ERRORS
SEGMENT REJECT LIMIT 10 PERCENT
`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP EXTERNAL TABLE public.ext_table")
			oid := testutils.OidFromObjectName(connectionPool, "public", "ext_table", backup.TYPE_RELATION)

			results := backup.GetExternalTableDefinitions(connectionPool)
			result := results[oid]

			extTable := backup.ExternalTableDefinition{Oid: 0, Type: 0, Protocol: 0, Location: "file://tmp/myfile.txt",
				ExecLocation: "ALL_SEGMENTS", FormatType: "t", FormatOpts: "delimiter '	' null '\\N' escape '\\'",
				Options: "", Command: "", RejectLimit: 10, RejectLimitType: "p", ErrTableName: "ext_table", ErrTableSchema: "public", Encoding: "UTF8",
				Writable: false, URIs: []string{"file://tmp/myfile.txt"}}

			structmatcher.ExpectStructsToMatchExcluding(&extTable, &result, "Oid")
		})
		It("returns a slice for an external table using CSV format", func() {
			testhelper.AssertQueryRuns(connectionPool, `CREATE READABLE EXTERNAL TABLE public.ext_table(i int)
LOCATION ('file://tmp/myfile.txt')
FORMAT 'CSV' (delimiter E'|' null E'' escape E'\'' quote E'\'' force not null i)
LOG ERRORS
SEGMENT REJECT LIMIT 10 PERCENT
`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP EXTERNAL TABLE public.ext_table")
			oid := testutils.OidFromObjectName(connectionPool, "public", "ext_table", backup.TYPE_RELATION)

			results := backup.GetExternalTableDefinitions(connectionPool)
			result := results[oid]

			extTable := backup.ExternalTableDefinition{Oid: 0, Type: 0, Protocol: 0, Location: "file://tmp/myfile.txt",
				ExecLocation: "ALL_SEGMENTS", FormatType: "c", FormatOpts: `delimiter '|' null '' escape ''' quote ''' force not null i`,
				Options: "", Command: "", RejectLimit: 10, RejectLimitType: "p", ErrTableName: "ext_table", ErrTableSchema: "public", Encoding: "UTF8",
				Writable: false, URIs: []string{"file://tmp/myfile.txt"}}

			structmatcher.ExpectStructsToMatchExcluding(&extTable, &result, "Oid")
		})
		It("returns a slice for an external table using CUSTOM format", func() {
			testhelper.AssertQueryRuns(connectionPool, `CREATE READABLE EXTERNAL TABLE public.ext_table(i int)
LOCATION ('file://tmp/myfile.txt')
FORMAT 'CUSTOM' (formatter = E'fixedwidth_out', i = E'20')
LOG ERRORS
SEGMENT REJECT LIMIT 10 PERCENT
`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP EXTERNAL TABLE public.ext_table")
			oid := testutils.OidFromObjectName(connectionPool, "public", "ext_table", backup.TYPE_RELATION)

			results := backup.GetExternalTableDefinitions(connectionPool)
			result := results[oid]

			extTable := backup.ExternalTableDefinition{Oid: 0, Type: 0, Protocol: 0, Location: "file://tmp/myfile.txt",
				ExecLocation: "ALL_SEGMENTS", FormatType: "b", FormatOpts: `formatter 'fixedwidth_out' i '20' `,
				Options: "", Command: "", RejectLimit: 10, RejectLimitType: "p", ErrTableName: "ext_table", ErrTableSchema: "public", Encoding: "UTF8",
				Writable: false, URIs: []string{"file://tmp/myfile.txt"}}

			structmatcher.ExpectStructsToMatchExcluding(&extTable, &result, "Oid")
		})
		It("returns a slice for a complex external table definition with options", func() {
			testutils.SkipIfBefore5(connectionPool)
			testhelper.AssertQueryRuns(connectionPool, `CREATE READABLE EXTERNAL TABLE public.ext_table(i int)
LOCATION ('file://tmp/myfile.txt')
FORMAT 'TEXT'
OPTIONS (foo 'bar')
LOG ERRORS
SEGMENT REJECT LIMIT 10 PERCENT
`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP EXTERNAL TABLE public.ext_table")
			oid := testutils.OidFromObjectName(connectionPool, "public", "ext_table", backup.TYPE_RELATION)

			results := backup.GetExternalTableDefinitions(connectionPool)
			result := results[oid]

			extTable := backup.ExternalTableDefinition{Oid: 0, Type: 0, Protocol: 0, Location: "file://tmp/myfile.txt",
				ExecLocation: "ALL_SEGMENTS", FormatType: "t", FormatOpts: "delimiter '	' null '\\N' escape '\\'",
				Options: "foo 'bar'", Command: "", RejectLimit: 10, RejectLimitType: "p", ErrTableName: "ext_table", ErrTableSchema: "public", Encoding: "UTF8",
				Writable: false, URIs: []string{"file://tmp/myfile.txt"}}

			structmatcher.ExpectStructsToMatchExcluding(&extTable, &result, "Oid")
		})
	})
	Describe("GetExternalProtocols", func() {
		It("returns a slice for a protocol", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE OR REPLACE FUNCTION public.write_to_s3() RETURNS integer AS '$libdir/gps3ext.so', 's3_export' LANGUAGE C STABLE;")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP FUNCTION public.write_to_s3()")
			testhelper.AssertQueryRuns(connectionPool, "CREATE OR REPLACE FUNCTION public.read_from_s3() RETURNS integer AS '$libdir/gps3ext.so', 's3_import' LANGUAGE C STABLE;")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP FUNCTION public.read_from_s3()")
			testhelper.AssertQueryRuns(connectionPool, "CREATE PROTOCOL s3 (writefunc = public.write_to_s3, readfunc = public.read_from_s3);")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP PROTOCOL s3")

			readFunctionOid := testutils.OidFromObjectName(connectionPool, "public", "read_from_s3", backup.TYPE_FUNCTION)
			writeFunctionOid := testutils.OidFromObjectName(connectionPool, "public", "write_to_s3", backup.TYPE_FUNCTION)

			results := backup.GetExternalProtocols(connectionPool)

			protocolDef := backup.ExternalProtocol{Oid: 1, Name: "s3", Owner: "testrole", Trusted: false, ReadFunction: readFunctionOid, WriteFunction: writeFunctionOid, Validator: 0}

			Expect(results).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&protocolDef, &results[0], "Oid")
		})
	})
	Describe("GetExternalPartitionInfo", func() {
		AfterEach(func() {
			testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.part_tbl")
			testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.part_tbl_ext_part_")
		})
		It("returns a slice of external partition info for a named list partition", func() {
			testhelper.AssertQueryRuns(connectionPool, `
CREATE TABLE public.part_tbl (id int, gender char(1))
DISTRIBUTED BY (id)
PARTITION BY LIST (gender)
( PARTITION girls VALUES ('F'),
  PARTITION boys VALUES ('M'),
  DEFAULT PARTITION other );`)
			testhelper.AssertQueryRuns(connectionPool, `
CREATE EXTERNAL WEB TABLE public.part_tbl_ext_part_ (like public.part_tbl_1_prt_girls)
EXECUTE 'echo -e "2\n1"' on host
FORMAT 'csv';`)
			testhelper.AssertQueryRuns(connectionPool, `ALTER TABLE public.part_tbl EXCHANGE PARTITION girls WITH TABLE public.part_tbl_ext_part_ WITHOUT VALIDATION;`)

			resultExtPartitions, resultPartInfoMap := backup.GetExternalPartitionInfo(connectionPool)

			Expect(resultExtPartitions).To(HaveLen(1))
			Expect(resultPartInfoMap).To(HaveLen(3))
			expectedExternalPartition := backup.PartitionInfo{
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
			structmatcher.ExpectStructsToMatchExcluding(&expectedExternalPartition, &resultExtPartitions[0], "PartitionRuleOid", "RelationOid", "ParentRelationOid")
		})
		It("returns a slice of external partition info for an unnamed range partition", func() {
			testhelper.AssertQueryRuns(connectionPool, `
CREATE TABLE public.part_tbl (a int)
DISTRIBUTED BY (a)
PARTITION BY RANGE (a)
(start(1) end(3) every(1));`)
			testhelper.AssertQueryRuns(connectionPool, `
CREATE EXTERNAL WEB TABLE public.part_tbl_ext_part_ (like public.part_tbl_1_prt_1)
EXECUTE 'echo -e "2\n1"' on host
FORMAT 'csv';`)
			testhelper.AssertQueryRuns(connectionPool, `ALTER TABLE public.part_tbl EXCHANGE PARTITION FOR (RANK(1)) WITH TABLE public.part_tbl_ext_part_ WITHOUT VALIDATION;`)

			resultExtPartitions, resultPartInfoMap := backup.GetExternalPartitionInfo(connectionPool)

			Expect(resultExtPartitions).To(HaveLen(1))
			Expect(resultPartInfoMap).To(HaveLen(2))
			expectedExternalPartition := backup.PartitionInfo{
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
			structmatcher.ExpectStructsToMatchExcluding(&expectedExternalPartition, &resultExtPartitions[0], "PartitionRuleOid", "RelationOid", "ParentRelationOid")
		})
		It("returns a slice of info for a two level partition", func() {
			testutils.SkipIfBefore5(connectionPool)
			testhelper.AssertQueryRuns(connectionPool, `
CREATE TABLE public.part_tbl (a int,b date,c text,d int)
DISTRIBUTED BY (a)
PARTITION BY RANGE (b)
SUBPARTITION BY LIST (c)
SUBPARTITION TEMPLATE
(SUBPARTITION usa values ('usa'),
SUBPARTITION apj values ('apj'),
SUBPARTITION eur values ('eur'))
(PARTITION Sep16 START (date '2016-09-01') INCLUSIVE ,
  PARTITION Oct16 START (date '2016-10-01') INCLUSIVE ,
  PARTITION Nov16 START (date '2016-11-01') INCLUSIVE ,
  PARTITION Dec16 START (date '2016-12-01') INCLUSIVE
                  END (date '2017-01-01') EXCLUSIVE);
`)

			testhelper.AssertQueryRuns(connectionPool, `CREATE EXTERNAL TABLE public.part_tbl_ext_part_ (a int,b date,c text,d int) LOCATION ('gpfdist://127.0.0.1/apj') FORMAT 'text';`)
			testhelper.AssertQueryRuns(connectionPool, `ALTER TABLE public.part_tbl ALTER PARTITION Dec16 EXCHANGE PARTITION apj WITH TABLE public.part_tbl_ext_part_ WITHOUT VALIDATION;`)

			resultExtPartitions, _ := backup.GetExternalPartitionInfo(connectionPool)

			Expect(resultExtPartitions).To(HaveLen(1))
			expectedExternalPartition := backup.PartitionInfo{
				PartitionRuleOid:       1,
				PartitionParentRuleOid: 0,
				ParentRelationOid:      2,
				ParentSchema:           "public",
				ParentRelationName:     "part_tbl",
				RelationOid:            testutils.OidFromObjectName(connectionPool, "public", "part_tbl_1_prt_dec16_2_prt_apj", backup.TYPE_RELATION),
				PartitionName:          "apj",
				PartitionRank:          0,
				IsExternal:             true,
			}
			structmatcher.ExpectStructsToMatchExcluding(&expectedExternalPartition, &resultExtPartitions[0], "PartitionRuleOid", "PartitionParentRuleOid", "ParentRelationOid")
		})
		It("returns a slice of info for a three level partition", func() {
			testutils.SkipIfBefore5(connectionPool)
			testhelper.AssertQueryRuns(connectionPool, `
CREATE TABLE public.part_tbl (id int, year int, month int, day int, region text)
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

			testhelper.AssertQueryRuns(connectionPool, `CREATE EXTERNAL TABLE public.part_tbl_ext_part_ (like public.part_tbl_1_prt_3_2_prt_1_3_prt_europe) LOCATION ('gpfdist://127.0.0.1/apj') FORMAT 'text';`)
			testhelper.AssertQueryRuns(connectionPool, `ALTER TABLE public.part_tbl ALTER PARTITION FOR (RANK(3)) ALTER PARTITION FOR (RANK(1)) EXCHANGE PARTITION europe WITH TABLE public.part_tbl_ext_part_ WITHOUT VALIDATION;`)

			resultExtPartitions, _ := backup.GetExternalPartitionInfo(connectionPool)

			Expect(resultExtPartitions).To(HaveLen(1))
			expectedExternalPartition := backup.PartitionInfo{
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
			expectedExternalPartition.RelationOid = testutils.OidFromObjectName(connectionPool, "public", "part_tbl_1_prt_3_2_prt_1_3_prt_europe", backup.TYPE_RELATION)
			structmatcher.ExpectStructsToMatchExcluding(&expectedExternalPartition, &resultExtPartitions[0], "PartitionRuleOid", "PartitionParentRuleOid", "ParentRelationOid")
		})
	})
})
