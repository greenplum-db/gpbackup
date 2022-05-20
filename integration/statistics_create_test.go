package integration

import (
	"github.com/greenplum-db/gp-common-go-libs/structmatcher"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup integration tests", func() {
	BeforeEach(func() {
		tocfile, backupfile = testutils.InitializeTestTOC(buffer, "predata")
	})
	Describe("PrintStatisticsStatementsForTable", func() {
		It("prints attribute and tuple statistics for a table", func() {
			tables := []backup.Table{
				{Relation: backup.Relation{SchemaOid: 2200, Schema: "public", Name: "foo"}},
			}

			// Create and ANALYZE a table to generate statistics
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.foo(i int, j text, k bool)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.foo")
			testhelper.AssertQueryRuns(connectionPool, "INSERT INTO public.foo VALUES (1, 'a', 't')")
			testhelper.AssertQueryRuns(connectionPool, "INSERT INTO public.foo VALUES (2, 'b', 'f')")
			testhelper.AssertQueryRuns(connectionPool, "ANALYZE public.foo")

			oldTableOid := testutils.OidFromObjectName(connectionPool, "public", "foo", backup.TYPE_RELATION)
			tables[0].Oid = oldTableOid

			beforeAttStats := backup.GetAttributeStatistics(connectionPool, tables)
			beforeTupleStats := backup.GetTupleStatistics(connectionPool, tables)
			beforeTupleStat := beforeTupleStats[oldTableOid]

			// Drop and recreate the table to clear the statistics
			testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.foo")
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.foo(i int, j text, k bool)")

			// Reload the retrieved statistics into the new table
			backup.PrintStatisticsStatements(backupfile, tocfile, tables, beforeAttStats, beforeTupleStats)
			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			newTableOid := testutils.OidFromObjectName(connectionPool, "public", "foo", backup.TYPE_RELATION)
			tables[0].Oid = newTableOid
			afterAttStats := backup.GetAttributeStatistics(connectionPool, tables)
			afterTupleStats := backup.GetTupleStatistics(connectionPool, tables)
			afterTupleStat := afterTupleStats[newTableOid]

			oldAtts := beforeAttStats[oldTableOid]
			newAtts := afterAttStats[newTableOid]

			// Ensure the statistics match
			Expect(afterTupleStats).To(HaveLen(len(beforeTupleStats)))
			structmatcher.ExpectStructsToMatchExcluding(&beforeTupleStat, &afterTupleStat, "Oid")
			Expect(oldAtts).To(HaveLen(3))
			Expect(newAtts).To(HaveLen(3))
			for i := range oldAtts {
				structmatcher.ExpectStructsToMatchExcluding(&oldAtts[i], &newAtts[i], "Oid", "Relid")
			}
		})
		It("prints attribute and tuple statistics for a quoted table", func() {
			tables := []backup.Table{
				{Relation: backup.Relation{SchemaOid: 2200, Schema: "public", Name: "\"foo'\"\"''bar\""}},
			}

			// Create and ANALYZE the tables to generate statistics
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.\"foo'\"\"''bar\"(i int, j text, k bool)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.\"foo'\"\"''bar\"")
			testhelper.AssertQueryRuns(connectionPool, "INSERT INTO public.\"foo'\"\"''bar\" VALUES (1, 'a', 't')")
			testhelper.AssertQueryRuns(connectionPool, "ANALYZE public.\"foo'\"\"''bar\"")

			oldTableOid := testutils.OidFromObjectName(connectionPool, "public", "foo''\"''''bar", backup.TYPE_RELATION)
			tables[0].Oid = oldTableOid

			beforeAttStats := backup.GetAttributeStatistics(connectionPool, tables)
			beforeTupleStats := backup.GetTupleStatistics(connectionPool, tables)
			beforeTupleStat := beforeTupleStats[oldTableOid]

			// Drop and recreate the table to clear the statistics
			testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.\"foo'\"\"''bar\"")
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.\"foo'\"\"''bar\"(i int, j text, k bool)")

			// Reload the retrieved statistics into the new table
			backup.PrintStatisticsStatements(backupfile, tocfile, tables, beforeAttStats, beforeTupleStats)
			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			newTableOid := testutils.OidFromObjectName(connectionPool, "public", "foo''\"''''bar", backup.TYPE_RELATION)
			tables[0].Oid = newTableOid
			afterAttStats := backup.GetAttributeStatistics(connectionPool, tables)
			afterTupleStats := backup.GetTupleStatistics(connectionPool, tables)
			afterTupleStat := afterTupleStats[newTableOid]

			oldAtts := beforeAttStats[oldTableOid]
			newAtts := afterAttStats[newTableOid]

			// Ensure the statistics match
			Expect(afterTupleStats).To(HaveLen(len(beforeTupleStats)))
			structmatcher.ExpectStructsToMatchExcluding(&beforeTupleStat, &afterTupleStat, "Oid")
			Expect(oldAtts).To(HaveLen(3))
			Expect(newAtts).To(HaveLen(3))
			for i := range oldAtts {
				structmatcher.ExpectStructsToMatchExcluding(&oldAtts[i], &newAtts[i], "Oid", "Relid")
			}
		})
	})
})
