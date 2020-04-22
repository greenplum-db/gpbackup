package backup_test

import (
	"fmt"

	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/lib/pq"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup/statistics tests", func() {
	getStatInsertReplace := func(smallint int, oid int) (string, string, string, string, string) {
		insertReplace1, insertReplace2, insertReplace3, insertReplace4, insertReplace5 := "", "", "", "", ""
		if connectionPool.Version.AtLeast("6") {
			insertReplace1 = `
	false::boolean,`
			insertReplace2 = fmt.Sprintf(`
	%d::smallint,`, smallint)
			insertReplace3 = fmt.Sprintf(`
	%d::oid,`, oid)
			insertReplace4 = `
	NULL::real[],`
			insertReplace5 = `
	NULL,`
		}
		return insertReplace1, insertReplace2, insertReplace3, insertReplace4, insertReplace5
	}

	Describe("PrintStatisticsStatementsForTable", func() {
		var (
			attStats       []backup.AttributeStatistic
			tupleStats     backup.TupleStatistic
			tableTestTable backup.Table
		)

		BeforeEach(func() {
			attStats = []backup.AttributeStatistic{}
			tupleStats = backup.TupleStatistic{}
			tocfile, backupfile = testutils.InitializeTestTOC(buffer, "statistics")
			tableTestTable = backup.Table{Relation: backup.Relation{Schema: "testschema", Name: "testtable"}}
		})
		It("prints tuple and attribute stats for single table with no stats", func() {
			tupleStats = backup.TupleStatistic{Schema: "testschema", Table: "testtable"}
			attStats = []backup.AttributeStatistic{}
			backup.PrintStatisticsStatementsForTable(backupfile, tocfile, tableTestTable, attStats, tupleStats)
			testutils.ExpectEntry(tocfile.StatisticsEntries, 0, "testschema", "", "testtable", "STATISTICS")
			testutils.AssertBufferContents(tocfile.StatisticsEntries, buffer, `UPDATE pg_class
SET
	relpages = 0::int,
	reltuples = 0.000000::real
WHERE oid = 'testschema.testtable'::regclass::oid;`)
		})
		It("prints tuple and attribute stats for single table with stats", func() {
			tupleStats = backup.TupleStatistic{Schema: "testschema", Table: "testtable"}
			attStats = []backup.AttributeStatistic{
				{Schema: "testschema", Table: "testtable", AttName: "testattWithArray", Type: "_array"},
				{Schema: "testschema", Table: "testtable", AttName: "testatt", Type: "_array", Relid: 2, AttNumber: 3, NullFraction: .4,
					Width: 10, Distinct: .5, Kind1: 20, Operator1: 10, Numbers1: []string{"1", "2", "3"}, Values1: []string{"4", "5", "6"}},
			}
			backup.PrintStatisticsStatementsForTable(backupfile, tocfile, tableTestTable, attStats, tupleStats)
			testutils.ExpectEntry(tocfile.StatisticsEntries, 0, "testschema", "", "testtable", "STATISTICS")

			insertReplace1, insertReplace2, insertReplace3, insertReplace4, insertReplace5 := getStatInsertReplace(0, 0)
			testutils.AssertBufferContents(tocfile.StatisticsEntries, buffer, fmt.Sprintf(`UPDATE pg_class
SET
	relpages = 0::int,
	reltuples = 0.000000::real
WHERE oid = 'testschema.testtable'::regclass::oid;


DELETE FROM pg_statistic WHERE starelid = 'testschema.testtable'::regclass::oid AND staattnum = 0;

INSERT INTO pg_statistic VALUES (
	'testschema.testtable'::regclass::oid,
	0::smallint,%[1]s
	0.000000::real,
	0::integer,
	0.000000::real,
	0::smallint,
	0::smallint,
	0::smallint,
	0::smallint,%[2]s
	0::oid,
	0::oid,
	0::oid,
	0::oid,%[3]s
	NULL::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],%[4]s
	NULL,
	NULL,
	NULL,%[5]s
	NULL
);


DELETE FROM pg_statistic WHERE starelid = 'testschema.testtable'::regclass::oid AND staattnum = 3;

INSERT INTO pg_statistic VALUES (
	'testschema.testtable'::regclass::oid,
	3::smallint,%[1]s
	0.400000::real,
	10::integer,
	0.500000::real,
	0::smallint,
	0::smallint,
	0::smallint,
	0::smallint,%[2]s
	0::oid,
	0::oid,
	0::oid,
	0::oid,%[3]s
	NULL::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],%[4]s
	NULL,
	NULL,
	NULL,%[5]s
	NULL
);`, insertReplace1, insertReplace2, insertReplace3, insertReplace4, insertReplace5))
		})
	})
	Describe("GenerateTupleStatisticsQuery", func() {
		It("generates tuple statistics query with double quotes and a single quote in the table name and schema name", func() {
			tableTestTable := backup.Table{Relation: backup.Relation{Schema: `"""test'schema"""`, Name: `"""test'table"""`}}
			tupleStats := backup.TupleStatistic{Schema: `"""test'schema"""`, Table: `"""test'table"""`}
			tupleQuery := backup.GenerateTupleStatisticsQuery(tableTestTable, tupleStats)
			Expect(tupleQuery).To(Equal(`UPDATE pg_class
SET
	relpages = 0::int,
	reltuples = 0.000000::real
WHERE oid = '"""test''schema"""."""test''table"""'::regclass::oid;`))
		})

	})
	Describe("GenerateAttributeStatisticsQuery", func() {
		tableTestTable := backup.Table{Relation: backup.Relation{Schema: "testschema", Name: `"test'table"`}}

		It("generates attribute statistics query for array type", func() {
			attStats := backup.AttributeStatistic{Schema: "testschema", Table: "testtable", AttName: "testatt", Type: "_array", Relid: 2,
				AttNumber: 3, NullFraction: .4, Width: 10, Distinct: .5, Kind1: 20, Operator1: 10,
				Numbers1: pq.StringArray([]string{"1", "2", "3"}), Values1: pq.StringArray([]string{"4", "5", "6"})}
			if connectionPool.Version.AtLeast("6") {
				attStats.Kind5 = 10
				attStats.Operator5 = 12
			}

			attStatsQuery := backup.GenerateAttributeStatisticsQuery(tableTestTable, attStats)

			insertReplace1, insertReplace2, insertReplace3, insertReplace4, insertReplace5 := getStatInsertReplace(0, 0)
			Expect(attStatsQuery).To(Equal(fmt.Sprintf(`DELETE FROM pg_statistic WHERE starelid = 'testschema."test''table"'::regclass::oid AND staattnum = 3;

INSERT INTO pg_statistic VALUES (
	'testschema."test''table"'::regclass::oid,
	3::smallint,%s
	0.400000::real,
	10::integer,
	0.500000::real,
	0::smallint,
	0::smallint,
	0::smallint,
	0::smallint,%s
	0::oid,
	0::oid,
	0::oid,
	0::oid,%s
	NULL::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],%s
	NULL,
	NULL,
	NULL,%s
	NULL
);`, insertReplace1, insertReplace2, insertReplace3, insertReplace4, insertReplace5)))
		})
		It("generates attribute statistics query for non-array type", func() {
			attStats := backup.AttributeStatistic{Schema: "testschema", Table: "testtable", AttName: "testatt", Type: "testtype", Relid: 2,
				AttNumber: 3, NullFraction: .4, Width: 10, Distinct: .5, Kind1: 20, Operator1: 10,
				Numbers1: pq.StringArray([]string{"1", "2", "3"}), Values1: pq.StringArray([]string{"4", "5", "6"})}
			if connectionPool.Version.AtLeast("6") {
				attStats.Kind5 = 10
				attStats.Operator5 = 12
			}

			attStatsQuery := backup.GenerateAttributeStatisticsQuery(tableTestTable, attStats)

			insertReplace1, insertReplace2, insertReplace3, insertReplace4, insertReplace5 := getStatInsertReplace(10, 12)
			Expect(attStatsQuery).To(Equal(fmt.Sprintf(`DELETE FROM pg_statistic WHERE starelid = 'testschema."test''table"'::regclass::oid AND staattnum = 3;

INSERT INTO pg_statistic VALUES (
	'testschema."test''table"'::regclass::oid,
	3::smallint,%s
	0.400000::real,
	10::integer,
	0.500000::real,
	20::smallint,
	0::smallint,
	0::smallint,
	0::smallint,%s
	10::oid,
	0::oid,
	0::oid,
	0::oid,%s
	'{"1","2","3"}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],%s
	array_in('{"4","5","6"}', 'testtype'::regtype::oid, -1),
	NULL,
	NULL,%s
	NULL
);`, insertReplace1, insertReplace2, insertReplace3, insertReplace4, insertReplace5)))
		})
	})
	Describe("AnyValues", func() {
		It("returns properly casted string when length of anyvalues is greater than 0", func() {
			castedString := backup.AnyValues([]string{"1", "2"}, "int")
			Expect(castedString).To(Equal(`array_in('{"1","2"}', 'int'::regtype::oid, -1)`))
		})
		It("returns NULL if anyvalues is of length 0", func() {
			castedString := backup.AnyValues([]string{}, "int")
			Expect(castedString).To(Equal(`NULL`))
		})
	})
	Describe("SliceToPostgresArray", func() {
		It("returns properly quoted string representing a Postgres array", func() {
			arrayString := backup.SliceToPostgresArray([]string{"ab'c", "ab\\c", "ab\"c", "ef\\'\"g"})
			Expect(arrayString).To(Equal("'{\"ab''c\",\"ab\\\\c\",\"ab\\\"c\",\"ef\\\\''\\\"g\"}'"))
		})
	})
})
