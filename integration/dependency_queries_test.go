package integration

import (
	"fmt"

	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"
	. "github.com/onsi/ginkgo"
	// . "github.com/onsi/gomega"
)

func printDependencyMap(depMap map[backup.DepEntry]map[backup.DepEntry]bool) {
	for depEntry, deps := range depMap {
		fmt.Printf("ENTRY: %#v\n", depEntry)
		fmt.Printf("DEPS: %#v\n\n", deps)
	}
}

var _ = Describe("backup integration tests", func() {
	Describe("Get[type]Types functions", func() {
		var ()
		BeforeEach(func() {

		})
		It("runs select statement to query pg_depend", func() {
			testhelper.AssertQueryRuns(connection, "CREATE TABLE public.foo(i int, j text, k bool)")
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.foo")
			testhelper.AssertQueryRuns(connection, "CREATE TABLE public.bar(m int) inherits (public.foo)")
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.bar")
			oidFoo := testutils.OidFromObjectName(connection, "public", "foo", backup.TYPE_RELATION)
			oidBar := testutils.OidFromObjectName(connection, "public", "bar", backup.TYPE_RELATION)
			backupSet := make(map[backup.DepEntry]bool, 0)
			backupSet[backup.DepEntry{Classid: 1259, Objid: oidFoo}] = true
			backupSet[backup.DepEntry{Classid: 1259, Objid: oidBar}] = true

			deps := backup.GetDependencies(connection, backupSet)
			fmt.Printf("%v", deps)
		})
		// FIt("topilogical sort tables only", func() {
		// 	testhelper.AssertQueryRuns(connection, "CREATE TABLE public.foo(i int, j text, k bool)")
		// 	defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.foo")
		// 	testhelper.AssertQueryRuns(connection, "CREATE TABLE public.bar(m int) inherits (public.foo)")
		// 	defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.bar")
		// 	oidFoo := testutils.OidFromObjectName(connection, "public", "foo", backup.TYPE_RELATION)
		// 	oidBar := testutils.OidFromObjectName(connection, "public", "bar", backup.TYPE_RELATION)
		// 	backupSet := make(map[backup.DepEntry]bool, 0)
		// 	backupSet[backup.DepEntry{Classid: 1259, Objid: oidFoo}] = true
		// 	backupSet[backup.DepEntry{Classid: 1259, Objid: oidBar}] = true

		// 	relevantDeps := backup.GetDependencies(connection, backupSet)
		// 	tables := backup.GetAllUserTables(connection)

		// 	tmp := tables[1]
		// 	tables[1] = tables[0]
		// 	tables[0] = tmp

		// 	fmt.Printf("%+v\n\n", tables)
		// 	sortedSlice := backup.SortObjectsInDependencyOrder([]backup.Function{}, []backup.Type{}, tables, []backup.ExternalProtocol{}, relevantDeps)

		// 	fmt.Printf("%+v\n\n", sortedSlice)

		// })
		It("dependency query does not return duplicates", func() {
			testhelper.AssertQueryRuns(connection, "CREATE TABLE public.foo(i int, j text, k bool)")
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.foo")

			// result := backup.GetDependencies(connection)
			//fmt.Println(result)
			// printDependencyMap(result)
		})
		It("dependency query gets table inheritance", func() {

			testhelper.AssertQueryRuns(connection, `CREATE FUNCTION public.add(integer, integer) RETURNS integer
AS 'SELECT $1 + $2'
LANGUAGE SQL`)
			defer testhelper.AssertQueryRuns(connection, "DROP FUNCTION public.add(integer, integer)")

			functions := backup.GetFunctionsAllVersions(connection)
			var functionOids []string
			for _, function := range functions {
				functionOidStr := fmt.Sprintf("%d", function.Oid)
				functionOids = append(functionOids, functionOidStr)
			}
			// functionDeps := backup.GetDependencies(connection, functionOids)

			// fmt.Printf("%#v", functionDeps)

		})
	})
})
