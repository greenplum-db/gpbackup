package integration

import (
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/options"
	"github.com/spf13/cobra"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Wrappers Integration", func() {
	BeforeEach(func() {
		gplog.SetVerbosity(gplog.LOGERROR) // turn off progress bar in the lock-table routine
	})
	Describe("RetrieveAndProcessTables", func() {
		BeforeEach(func() {
			rootCmd := &cobra.Command{}
			includes := []string{"--include-table", "public.foo", "--include-table", "public.BAR"}
			rootCmd.SetArgs(options.HandleSingleDashes(includes))
			backup.DoInit(rootCmd) // initialize the ObjectCount
			rootCmd.Execute()
		})
		It("returns the data tables that have names with special characters", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.foo(i int); INSERT INTO public.foo VALUES (1);")
			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE public."BAR"(i int); INSERT INTO public."BAR" VALUES (1);`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.foo;")
			defer testhelper.AssertQueryRuns(connectionPool, `DROP TABLE public."BAR";`)

			// every backup occurs in a transaction; we are testing a small part of that backup
			connectionPool.MustBegin(0)
			defer connectionPool.MustCommit(0)

			_, dataTables := backup.RetrieveAndProcessTables()
			Expect(len(dataTables)).To(Equal(2))
			Expect(dataTables[0].Name).To(Equal("foo"))
			Expect(dataTables[1].Name).To(Equal(`"BAR"`))
		})
	})
	Describe("Tables order when no filtering is used or tables filtering is defined", func() {
		BeforeEach(func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.empty(i int);")
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.ten(i int); INSERT INTO public.ten SELECT generate_series(0, 10);")
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.thousands(i int); INSERT INTO public.thousands SELECT generate_series(0, 10000);")
			testhelper.AssertQueryRuns(connectionPool, "ANALYZE")

			connectionPool.MustBegin(0)
		})
		AfterEach(func() {
			testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.empty;")
			testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.ten;")
			testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.thousands;")

			connectionPool.MustCommit(0)
		})
		It("returns the data tables in descending order of their sizes (relpages)", func() {
			rootCmd := &cobra.Command{}
			backup.DoInit(rootCmd) // initialize the ObjectCount

			_, dataTables := backup.RetrieveAndProcessTables()
			Expect(len(dataTables)).To(Equal(3))
			Expect(dataTables[0].Name).To(Equal("thousands"))
			Expect(dataTables[1].Name).To(Equal("ten"))
			Expect(dataTables[2].Name).To(Equal("empty"))
		})
		It("returns the data tables in descending order of their sizes (relpages) when include-tables(-file) flag is used", func() {
			rootCmd := &cobra.Command{}
			includes := []string{"--include-table", "public.empty", "--include-table", "public.ten"}
			rootCmd.SetArgs(options.HandleSingleDashes(includes))
			backup.DoInit(rootCmd) // initialize the ObjectCount
			rootCmd.Execute()

			_, dataTables := backup.RetrieveAndProcessTables()
			Expect(len(dataTables)).To(Equal(2))
			Expect(dataTables[0].Name).To(Equal("ten"))
			Expect(dataTables[1].Name).To(Equal("empty"))
		})
		It("returns the data tables in descending order of their sizes (relpages) when exclude-tables(s) flag is used", func() {
			rootCmd := &cobra.Command{}
			includes := []string{"--exclude-table", "public.thousands"}
			rootCmd.SetArgs(options.HandleSingleDashes(includes))
			backup.DoInit(rootCmd) // initialize the ObjectCount
			rootCmd.Execute()

			_, dataTables := backup.RetrieveAndProcessTables()
			Expect(len(dataTables)).To(Equal(2))
			Expect(dataTables[0].Name).To(Equal("ten"))
			Expect(dataTables[1].Name).To(Equal("empty"))
		})
	})
	Describe("Tables order when schema filtering is defined", func() {
		BeforeEach(func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE SCHEMA filterschema;")
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE filterschema.empty(i int);")
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.ten(i int); INSERT INTO public.ten SELECT generate_series(0, 10);")
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE filterschema.thousands(i int); INSERT INTO filterschema.thousands SELECT generate_series(0, 1000);")
			testhelper.AssertQueryRuns(connectionPool, "ANALYZE")

			connectionPool.MustBegin(0)
		})
		AfterEach(func() {
			testhelper.AssertQueryRuns(connectionPool, "DROP TABLE filterschema.empty;")
			testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.ten;")
			testhelper.AssertQueryRuns(connectionPool, "DROP TABLE filterschema.thousands;")
			testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA filterschema;")

			connectionPool.MustCommit(0)
		})
		It("returns the data tables in descending order of their sizes (relpages) when include-schema(s) flag is used", func() {
			rootCmd := &cobra.Command{}
			includes := []string{"--include-schema", "filterschema"}
			rootCmd.SetArgs(options.HandleSingleDashes(includes))
			backup.DoInit(rootCmd) // initialize the ObjectCount
			rootCmd.Execute()

			_, dataTables := backup.RetrieveAndProcessTables()
			Expect(len(dataTables)).To(Equal(2))
			Expect(dataTables[0].Name).To(Equal("thousands"))
			Expect(dataTables[1].Name).To(Equal("empty"))
		})
		It("returns the data tables in descending order of their sizes (relpages) when exclude-schema(s) flag is used", func() {
			rootCmd := &cobra.Command{}
			includes := []string{"--exclude-schema", "public"}
			rootCmd.SetArgs(options.HandleSingleDashes(includes))
			backup.DoInit(rootCmd) // initialize the ObjectCount
			rootCmd.Execute()

			_, dataTables := backup.RetrieveAndProcessTables()
			Expect(len(dataTables)).To(Equal(2))
			Expect(dataTables[0].Name).To(Equal("thousands"))
			Expect(dataTables[1].Name).To(Equal("empty"))
		})
	})
	Describe("Tables order cases, when there is a partitioned table to backup", func() {
		BeforeEach(func() {
			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE public.partition_table (id int, gender char(1))
		DISTRIBUTED BY (id)
		PARTITION BY LIST (gender)
		( PARTITION girls VALUES ('F'),
			PARTITION boys VALUES ('M'),
			DEFAULT PARTITION other );`)
			testhelper.AssertQueryRuns(connectionPool, "INSERT INTO public.partition_table VALUES (generate_series(0,10000), 'F');")
			testhelper.AssertQueryRuns(connectionPool, "INSERT INTO public.partition_table VALUES (generate_series(0,10), NULL);")
			testhelper.AssertQueryRuns(connectionPool, "ANALYZE")

			connectionPool.MustBegin(0)
		})
		AfterEach(func() {
			testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.partition_table")

			connectionPool.MustCommit(0)
		})
		It("returns the data tables in descending order of their sizes (relpages), when there is a partitioned table to backup", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.ten(i int); INSERT INTO public.ten SELECT generate_series(0, 10);")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.ten;")
			testhelper.AssertQueryRuns(connectionPool, "ANALYZE")

			rootCmd := &cobra.Command{}
			backup.DoInit(rootCmd) // initialize the ObjectCount

			opts, _ := options.NewOptions(rootCmd.Flags())
			err := opts.ExpandIncludesForPartitions(connectionPool, rootCmd.Flags())
			Expect(err).ShouldNot(HaveOccurred())

			_, dataTables := backup.RetrieveAndProcessTables()
			Expect(len(dataTables)).To(Equal(2))
			Expect(dataTables[0].Name).To(Equal("partition_table"))
			Expect(dataTables[1].Name).To(Equal("ten"))
		})
		It("returns the data tables in descending order of their sizes (relpages), when there is a partitioned table to backup and leaf-partition-data flag is set", func() {
			rootCmd := &cobra.Command{}
			includes := []string{"--leaf-partition-data"}
			rootCmd.SetArgs(options.HandleSingleDashes(includes))
			backup.DoInit(rootCmd) // initialize the ObjectCount
			rootCmd.Execute()

			opts, _ := options.NewOptions(rootCmd.Flags())
			err := opts.ExpandIncludesForPartitions(connectionPool, rootCmd.Flags())
			Expect(err).ShouldNot(HaveOccurred())

			_, dataTables := backup.RetrieveAndProcessTables()

			Expect(len(dataTables)).To(Equal(3))
			Expect(dataTables[0].Name).To(Equal("partition_table_1_prt_girls"))
			Expect(dataTables[1].Name).To(Equal("partition_table_1_prt_other"))
			Expect(dataTables[2].Name).To(Equal("partition_table_1_prt_boys"))
		})
	})
	Describe("Tables order cases, when there is an AO/CO table to backup", func() {
		BeforeEach(func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.empty(i int);")
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.thousands(i int); INSERT INTO public.thousands SELECT generate_series(0, 10000);")
			testhelper.AssertQueryRuns(connectionPool, "ANALYZE")
		})
		AfterEach(func() {
			testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.empty;")
			testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.thousands;")

			connectionPool.MustCommit(0)
		})
		It("returns the data tables in descending order of their sizes (relpages), when there is an AO table to backup", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.hundred(i int) WITH (appendonly=true) DISTRIBUTED RANDOMLY;")
			testhelper.AssertQueryRuns(connectionPool, "INSERT INTO public.hundred SELECT generate_series(0, 100);")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.hundred;")
			testhelper.AssertQueryRuns(connectionPool, "VACUUM public.hundred") // relpages of AOCO is not updated by ANALYZE

			connectionPool.MustBegin(0) //VACUUM cannot be run inside a transaction block

			rootCmd := &cobra.Command{}
			backup.DoInit(rootCmd) // initialize the ObjectCount

			_, dataTables := backup.RetrieveAndProcessTables()
			Expect(len(dataTables)).To(Equal(3))

			Expect(dataTables[0].Name).To(Equal("thousands"))
			Expect(dataTables[1].Name).To(Equal("hundred"))
			Expect(dataTables[2].Name).To(Equal("empty"))

		})
		It("returns the data tables in descending order of their sizes (relpages), when there is an AOCO table to backup", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.hundred(i int) WITH (appendonly=true, orientation=column) DISTRIBUTED RANDOMLY;")
			testhelper.AssertQueryRuns(connectionPool, "INSERT INTO public.hundred SELECT generate_series(0, 100);")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.hundred;")
			testhelper.AssertQueryRuns(connectionPool, "VACUUM") // relpages of AOCO is not updated by ANALYZE

			connectionPool.MustBegin(0) //VACUUM cannot be run inside a transaction block

			rootCmd := &cobra.Command{}
			backup.DoInit(rootCmd) // initialize the ObjectCount

			_, dataTables := backup.RetrieveAndProcessTables()
			Expect(len(dataTables)).To(Equal(3))

			Expect(dataTables[0].Name).To(Equal("thousands"))
			Expect(dataTables[1].Name).To(Equal("hundred"))
			Expect(dataTables[2].Name).To(Equal("empty"))
		})
	})
})
