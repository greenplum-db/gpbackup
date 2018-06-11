package integration

import (
	"sort"

	"github.com/greenplum-db/gp-common-go-libs/structmatcher"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup integration tests", func() {
	tables := []backup.Relation{backup.BasicRelation("public", "foo")}
	var tableOid uint32
	BeforeEach(func() {
		testhelper.AssertQueryRuns(connection, "CREATE TABLE public.foo(i int, j text, k bool)")
		tableOid = testutils.OidFromObjectName(connection, "public", "foo", backup.TYPE_RELATION)
		testhelper.AssertQueryRuns(connection, "INSERT INTO public.foo VALUES (1, 'a', 't')")
		testhelper.AssertQueryRuns(connection, "INSERT INTO public.foo VALUES (2, 'b', 'f')")
		testhelper.AssertQueryRuns(connection, "ANALYZE public.foo")
	})
	AfterEach(func() {
		testhelper.AssertQueryRuns(connection, "DROP TABLE public.foo")
	})
	Describe("GetAttributeStatistics", func() {
		It("returns attribute statistics for a table", func() {
			attStats := backup.GetAttributeStatistics(connection, tables)
			Expect(len(attStats)).To(Equal(1))
			Expect(len(attStats[tableOid])).To(Equal(3))
			tableAttStatsI := attStats[tableOid][0]
			tableAttStatsJ := attStats[tableOid][1]
			tableAttStatsK := attStats[tableOid][2]

			/*
			 * Attribute statistics will vary by GPDB version, but statistics for a
			 * certain table should always be the same in a particular version given
			 * the same schema and data.
			 */
			expectedStats4I := backup.AttributeStatistic{Oid: tableOid, Schema: "public", Table: "foo", AttName: "i",
				Type: "int4", Relid: tableOid, AttNumber: 1, Width: 4, Distinct: -1, Kind1: 1, Kind2: 0, Operator1: 96,
				Operator2: 0, Numbers1: []string{"0.5", "0.5"}, Values1: []string{"1", "2"}}
			expectedStats4J := backup.AttributeStatistic{Oid: tableOid, Schema: "public", Table: "foo", AttName: "j",
				Type: "text", Relid: tableOid, AttNumber: 2, Width: 2, Distinct: -1, Kind1: 1, Kind2: 0, Operator1: 98,
				Operator2: 0, Numbers1: []string{"0.5", "0.5"}, Values1: []string{"a", "b"}}
			expectedStats4K := backup.AttributeStatistic{Oid: tableOid, Schema: "public", Table: "foo", AttName: "k",
				Type: "bool", Relid: tableOid, AttNumber: 3, Width: 1, Distinct: 2, Kind1: 1, Kind2: 0, Operator1: 91,
				Operator2: 0, Numbers1: []string{"0.5", "0.5"}, Values1: []string{"f", "t"}}
			expectedStats5I := backup.AttributeStatistic{Oid: tableOid, Schema: "public", Table: "foo", AttName: "i",
				Type: "int4", Relid: tableOid, AttNumber: 1, Inherit: false, Width: 4, Distinct: -1, Kind1: 2, Kind2: 3, Operator1: 97,
				Operator2: 97, Numbers2: []string{"1"}, Values1: []string{"1", "2"}}
			expectedStats5J := backup.AttributeStatistic{Oid: tableOid, Schema: "public", Table: "foo", AttName: "j",
				Type: "text", Relid: tableOid, AttNumber: 2, Inherit: false, Width: 2, Distinct: -1, Kind1: 2, Kind2: 3, Operator1: 664,
				Operator2: 664, Numbers2: []string{"1"}, Values1: []string{"a", "b"}}
			expectedStats5K := backup.AttributeStatistic{Oid: tableOid, Schema: "public", Table: "foo", AttName: "k",
				Type: "bool", Relid: tableOid, AttNumber: 3, Inherit: false, Width: 1, Distinct: -1, Kind1: 2, Kind2: 3, Operator1: 58,
				Operator2: 58, Numbers2: []string{"-1"}, Values1: []string{"f", "t"}}

			// The order in which the stavalues1 values is returned is not guaranteed to be deterministic
			sort.Strings(tableAttStatsI.Values1)
			sort.Strings(tableAttStatsJ.Values1)
			sort.Strings(tableAttStatsK.Values1)
			if connection.Version.Before("5") {
				structmatcher.ExpectStructsToMatchExcluding(&expectedStats4I, &tableAttStatsI, "Numbers2")
				structmatcher.ExpectStructsToMatchExcluding(&expectedStats4J, &tableAttStatsJ, "Numbers2")
				structmatcher.ExpectStructsToMatchExcluding(&expectedStats4K, &tableAttStatsK, "Numbers2")
			} else {
				structmatcher.ExpectStructsToMatchExcluding(&expectedStats5I, &tableAttStatsI, "Numbers2")
				structmatcher.ExpectStructsToMatchExcluding(&expectedStats5J, &tableAttStatsJ, "Numbers2")
				structmatcher.ExpectStructsToMatchExcluding(&expectedStats5K, &tableAttStatsK, "Numbers2")
			}
		})
	})
	Describe("GetTupleStatistics", func() {
		It("returns tuple statistics for a table", func() {
			tupleStats := backup.GetTupleStatistics(connection, tables)
			Expect(len(tupleStats)).To(Equal(1))
			tableTupleStats := tupleStats[tableOid]

			// Tuple statistics will not vary by GPDB version. Relpages may vary based on the hardware.
			expectedStats := backup.TupleStatistic{Oid: tableOid, Schema: "public", Table: "foo", RelTuples: 2}

			structmatcher.ExpectStructsToMatchExcluding(&expectedStats, &tableTupleStats, "RelPages")
		})
	})
})
