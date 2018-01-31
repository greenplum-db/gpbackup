package integration

import (
	"github.com/greenplum-db/gp-common-go-libs/structmatcher"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup integration tests", func() {
	BeforeEach(func() {
		toc, backupfile = testutils.InitializeTestTOC(buffer, "predata")
	})
	Describe("PrintStatisticsStatementsForTable", func() {
		It("prints attribute and tuple statistics for a table", func() {
			tables := []backup.Relation{{SchemaOid: 2200, Schema: "public", Name: "foo"}}

			// Create and ANALYZE a table to generate statistics
			testutils.AssertQueryRuns(connection, "CREATE TABLE foo(i int, j text, k bool)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE foo")
			testutils.AssertQueryRuns(connection, "INSERT INTO foo VALUES (1, 'a', 't')")
			testutils.AssertQueryRuns(connection, "INSERT INTO foo VALUES (2, 'b', 'f')")
			testutils.AssertQueryRuns(connection, "ANALYZE foo")

			oldTableOid := testutils.OidFromObjectName(connection, "public", "foo", backup.TYPE_RELATION)
			tables[0].Oid = oldTableOid

			beforeAttStats := backup.GetAttributeStatistics(connection, tables)
			beforeTupleStats := backup.GetTupleStatistics(connection, tables)
			beforeTupleStat := beforeTupleStats[oldTableOid]

			// Drop and recreate the table to clear the statistics
			testutils.AssertQueryRuns(connection, "DROP TABLE foo")
			testutils.AssertQueryRuns(connection, "CREATE TABLE foo(i int, j text, k bool)")

			// Reload the retrieved statistics into the new table
			backup.PrintStatisticsStatements(backupfile, toc, tables, beforeAttStats, beforeTupleStats)
			testutils.AssertQueryRuns(connection, buffer.String())

			newTableOid := testutils.OidFromObjectName(connection, "public", "foo", backup.TYPE_RELATION)
			tables[0].Oid = newTableOid
			afterAttStats := backup.GetAttributeStatistics(connection, tables)
			afterTupleStats := backup.GetTupleStatistics(connection, tables)
			afterTupleStat := afterTupleStats[newTableOid]

			oldAtts := beforeAttStats[oldTableOid]
			newAtts := afterAttStats[newTableOid]

			// Ensure the statistics match
			Expect(len(afterTupleStats)).To(Equal(len(beforeTupleStats)))
			structmatcher.ExpectStructsToMatchExcluding(&beforeTupleStat, &afterTupleStat, "Oid")
			Expect(len(oldAtts)).To(Equal(3))
			Expect(len(newAtts)).To(Equal(3))
			for i := range oldAtts {
				structmatcher.ExpectStructsToMatchExcluding(&oldAtts[i], &newAtts[i], "Oid", "Relid")
			}
		})
	})
})
