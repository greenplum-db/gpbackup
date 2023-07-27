package integration

import (
	"fmt"
	"sort"

	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/options"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func createGenericPartitionTable(tableName string) {
	testhelper.AssertQueryRuns(connectionPool, fmt.Sprintf(`CREATE TABLE %s (id int, gender char(1))
		DISTRIBUTED BY (id)
		PARTITION BY LIST (gender)
		( PARTITION girls VALUES ('F'),
		  PARTITION boys VALUES ('M'),
		  DEFAULT PARTITION other )`, tableName))
}

func createPartitionTableWithExternalPartition(tableName string, extPartName string) {
	createGenericPartitionTable(tableName)
	testhelper.AssertQueryRuns(connectionPool, fmt.Sprintf(`CREATE EXTERNAL WEB TABLE %[1]s_ext_part_ (like %[1]s_1_prt_%[2]s)
EXECUTE 'echo -e "2\n1"' on host
FORMAT 'csv';`, tableName, extPartName))
	testhelper.AssertQueryRuns(connectionPool, fmt.Sprintf(`ALTER TABLE %[1]s EXCHANGE PARTITION %[2]s WITH TABLE %[1]s_ext_part_ WITHOUT VALIDATION;`, tableName, extPartName))
}

func dropTableWithExternalPartition(tableName string) {
	testhelper.AssertQueryRuns(connectionPool, fmt.Sprintf("DROP TABLE %s", tableName))
	testhelper.AssertQueryRuns(connectionPool, fmt.Sprintf("DROP TABLE %s_ext_part_", tableName))
}

var _ = Describe("Options Integration", func() {
	Describe("QuoteTablesNames", func() {
		It("quotes identifiers as expected", func() {
			tableList := []string{
				`foo.bar`,  // no special characters
				`foo.BAR`,  // capital characters
				`foo.'bar`, // make sure that single quotes are escaped before string is fed to quote_ident
				`foo.2`,    // numbers
				`foo._bar`, // underscore
				`foo ~#$%^&*()_-+[]{}><\|;:/?!,.bar`,
				"foo.\tbar", // important to use double quotes to allow \t to become tab
				"foo.\nbar", // important to use double quotes to allow \n to become a new line
				`foo.\n`,
				`foo."bar`, // quote ident should escape double-quote with another double-quote
			}
			expected := []string{
				`foo.bar`,
				`foo."BAR"`,
				`foo."'bar"`,
				`foo."2"`,
				`foo._bar`, // underscore is not a special character
				`"foo ~#$%^&*()_-+[]{}><\|;:/?!,".bar`,
				"foo.\"\tbar\"",
				"foo.\"\nbar\"",
				`foo."\n"`,
				`foo."""bar"`,
			}

			resultFQNs, err := options.QuoteTableNames(connectionPool, tableList)
			Expect(err).ToNot(HaveOccurred())
			Expect(expected).To(Equal(resultFQNs))
		})
	})
	Describe("ValidateFilterTables", func() {
		It("validates special chars", func() {
			createSpecialCharacterTables := `
-- special chars
CREATE TABLE public."FOObar" (i int);
CREATE TABLE public."BAR" (i int);
CREATE SCHEMA "CAPschema";
CREATE TABLE "CAPschema"."BAR" (i int);
CREATE TABLE "CAPschema".baz (i int);
CREATE TABLE public.foo_bar (i int);
CREATE TABLE public."foo ~#$%^&*()_-+[]{}><\|;:/?!bar" (i int);
-- special chars: embedded tab char
CREATE TABLE public."tab	bar" (i int);
-- special chars: embedded newline char
CREATE TABLE public."newline
bar" (i int);
`
			dropSpecialCharacterTables := `
-- special chars
DROP TABLE public."FOObar";
DROP TABLE public."BAR";
DROP SCHEMA "CAPschema" cascade;
DROP TABLE public.foo_bar;
DROP TABLE public."foo ~#$%^&*()_-+[]{}><\|;:/?!bar";
-- special chars: embedded tab char
DROP TABLE public."tab	bar";
-- special chars: embedded newline char
DROP TABLE public."newline
bar";
`
			testhelper.AssertQueryRuns(connectionPool, createSpecialCharacterTables)
			defer testhelper.AssertQueryRuns(connectionPool, dropSpecialCharacterTables)

			tableList := []string{
				`public.BAR`,
				`CAPschema.BAR`,
				`CAPschema.baz`,
				`public.foo_bar`,
				`public.foo ~#$%^&*()_-+[]{}><\|;:/?!bar`,
				"public.tab\tbar",     // important to use double quotes to allow \t to become tab
				"public.newline\nbar", // important to use double quotes to allow \n to become newline
			}

			backup.ValidateTablesExist(connectionPool, tableList, false)
		})
	})
	Describe("ExpandIncludesForPartitions", func() {
		BeforeEach(func() {
			createGenericPartitionTable(`public."CAPpart"`)
		})

		AfterEach(func() {
			testhelper.AssertQueryRuns(connectionPool, `DROP TABLE public."CAPpart"`)
		})

		It("adds parent table when child partition with special chars is included", func() {
			err := backupCmdFlags.Set(options.INCLUDE_RELATION, `public.CAPpart_1_prt_girls`)
			Expect(err).ToNot(HaveOccurred())
			subject, err := options.NewOptions(backupCmdFlags)
			Expect(err).To(Not(HaveOccurred()))
			Expect(subject.GetIncludedTables()).To(ContainElement("public.CAPpart_1_prt_girls"))
			Expect(subject.GetIncludedTables()).To(HaveLen(1))

			err = subject.ExpandIncludesForPartitions(connectionPool, backupCmdFlags)
			Expect(err).To(Not(HaveOccurred()))
			Expect(subject.GetIncludedTables()).To(HaveLen(2))
			Expect(backupCmdFlags.GetStringArray(options.INCLUDE_RELATION)).To(HaveLen(2))
			Expect(subject.GetIncludedTables()).To(ContainElement("public.CAPpart_1_prt_girls"))
			Expect(subject.GetIncludedTables()).To(ContainElement("public.CAPpart"))

			// ensure that ExpandIncludesForPartitions does not disturb the original value
			// that the user typed in, which is used by initializeBackupReport() and
			// is important for incremental backups which must exactly match all flag input
			Expect(subject.GetOriginalIncludedTables()).To(Equal([]string{`public.CAPpart_1_prt_girls`}))
		})
		It("adds parent table when child partition with embedded quote character is included", func() {
			createGenericPartitionTable(`public."""hasquote"""`)
			defer testhelper.AssertQueryRuns(connectionPool, `DROP TABLE public."""hasquote"""`)

			err := backupCmdFlags.Set(options.INCLUDE_RELATION, `public."hasquote"_1_prt_girls`)
			Expect(err).ToNot(HaveOccurred())
			subject, err := options.NewOptions(backupCmdFlags)
			Expect(err).To(Not(HaveOccurred()))
			Expect(subject.GetIncludedTables()).To(ContainElement(`public."hasquote"_1_prt_girls`))
			Expect(subject.GetIncludedTables()).To(HaveLen(1))

			err = subject.ExpandIncludesForPartitions(connectionPool, backupCmdFlags)
			Expect(err).To(Not(HaveOccurred()))
			Expect(subject.GetIncludedTables()).To(HaveLen(2))
			Expect(backupCmdFlags.GetStringArray(options.INCLUDE_RELATION)).To(HaveLen(2))
			Expect(subject.GetIncludedTables()[0]).To(Equal(`public."hasquote"_1_prt_girls`))
			Expect(subject.GetIncludedTables()[1]).To(Equal(`public."hasquote"`))
		})
		It("returns child partition tables for an included parent table if the leaf-partition-data flag is set and the filter includes a parent partition table", func() {
			_ = backupCmdFlags.Set(options.LEAF_PARTITION_DATA, "true")
			_ = backupCmdFlags.Set(options.INCLUDE_RELATION, "public.rank")

			createGenericPartitionTable(`public.rank`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.rank")
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.test_table(i int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.test_table")

			subject, err := options.NewOptions(backupCmdFlags)
			Expect(err).To(Not(HaveOccurred()))

			err = subject.ExpandIncludesForPartitions(connectionPool, backupCmdFlags)
			Expect(err).To(Not(HaveOccurred()))

			expectedTableNames := []string{
				"public.rank",
				"public.rank_1_prt_boys",
				"public.rank_1_prt_girls",
				"public.rank_1_prt_other",
			}

			tables := subject.GetIncludedTables()
			sort.Strings(tables)
			Expect(tables).To(Equal(expectedTableNames))
		})
		It("returns parent and external leaf partition table if the filter includes a leaf table and leaf-partition-data is set", func() {
			_ = backupCmdFlags.Set(options.LEAF_PARTITION_DATA, "true")
			_ = backupCmdFlags.Set(options.INCLUDE_RELATION, "public.partition_table_1_prt_boys")
			createPartitionTableWithExternalPartition("public.partition_table", "girls")
			defer dropTableWithExternalPartition("public.partition_table")

			subject, err := options.NewOptions(backupCmdFlags)
			Expect(err).To(Not(HaveOccurred()))

			err = subject.ExpandIncludesForPartitions(connectionPool, backupCmdFlags)
			Expect(err).To(Not(HaveOccurred()))

			expectedTableNames := []string{
				"public.partition_table",
				"public.partition_table_1_prt_boys",
			}
			if connectionPool.Version.Before("7") {
				// For GPDB 4, 5, and 6, the leaf partition metadata is a part of the large root partition DDL.
				// For GPDB 7+, the leaf partition has its own separate DDL and attaches onto the root partition
				// with ALTER TABLE ATTACH PARTITION. Therefore, only the included leaf partition and its root
				// partition will have their metadata dumped.
				expectedTableNames = append(expectedTableNames, "public.partition_table_1_prt_girls")
			}

			tables := subject.GetIncludedTables()
			sort.Strings(tables)
			Expect(tables).To(Equal(expectedTableNames))
		})
		It("returns the parent table and no unrelated external partitions if the filter includes a leaf table and leaf-partition-data and no-inherits are set", func() {
			_ = backupCmdFlags.Set(options.LEAF_PARTITION_DATA, "true")
			_ = backupCmdFlags.Set(options.NO_INHERITS, "true")
			_ = backupCmdFlags.Set(options.INCLUDE_RELATION, "public.partition_table_1_prt_boys")
			createPartitionTableWithExternalPartition("public.partition_table", "girls")
			defer dropTableWithExternalPartition("public.partition_table")

			subject, err := options.NewOptions(backupCmdFlags)
			Expect(err).To(Not(HaveOccurred()))

			err = subject.ExpandIncludesForPartitions(connectionPool, backupCmdFlags)
			Expect(err).To(Not(HaveOccurred()))

			expectedTableNames := []string{
				"public.partition_table",
				"public.partition_table_1_prt_boys",
			}

			tables := subject.GetIncludedTables()
			sort.Strings(tables)
			Expect(tables).To(Equal(expectedTableNames))
		})
		It("returns external partition tables for an included parent table if the filter includes a parent partition table", func() {
			_ = backupCmdFlags.Set(options.INCLUDE_RELATION, "public.partition_table1")
			_ = backupCmdFlags.Set(options.INCLUDE_RELATION, "public.partition_table2_1_prt_other")

			createPartitionTableWithExternalPartition("public.partition_table1", "boys")
			defer dropTableWithExternalPartition("public.partition_table1")
			createPartitionTableWithExternalPartition("public.partition_table2", "girls")
			defer dropTableWithExternalPartition("public.partition_table2")
			createPartitionTableWithExternalPartition("public.partition_table3", "girls")
			defer dropTableWithExternalPartition("public.partition_table3")

			subject, err := options.NewOptions(backupCmdFlags)
			Expect(err).To(Not(HaveOccurred()))

			err = subject.ExpandIncludesForPartitions(connectionPool, backupCmdFlags)
			Expect(err).To(Not(HaveOccurred()))

			var expectedTableNames []string
			if connectionPool.Version.Before("7") {
				expectedTableNames = []string{
					"public.partition_table1",
					"public.partition_table1_1_prt_boys",
					"public.partition_table2",
					"public.partition_table2_1_prt_girls",
					"public.partition_table2_1_prt_other",
				}
			} else {
				// For GPDB 7+, each leaf partition of the included partition_table1 will have their own
				// separate DDL along with an ATTACH PARTITION. Since only partition_table2_1_prt_other
				// is included, only it and its root partition will be a part of the list.
				expectedTableNames = []string{
					"public.partition_table1",
					"public.partition_table1_1_prt_boys",
					"public.partition_table1_1_prt_girls",
					"public.partition_table1_1_prt_other",
					"public.partition_table2",
					"public.partition_table2_1_prt_other",
				}
			}

			tables := subject.GetIncludedTables()
			sort.Strings(tables)
			Expect(tables).To(Equal(expectedTableNames))
		})
		It("returns included parent tables but omits un-included external partitions if the filter includes a parent partition table and no-inherits is set", func() {
			_ = backupCmdFlags.Set(options.NO_INHERITS, "true")
			_ = backupCmdFlags.Set(options.INCLUDE_RELATION, "public.partition_table1")
			_ = backupCmdFlags.Set(options.INCLUDE_RELATION, "public.partition_table2_1_prt_other")

			createPartitionTableWithExternalPartition("public.partition_table1", "boys")
			defer dropTableWithExternalPartition("public.partition_table1")
			createPartitionTableWithExternalPartition("public.partition_table2", "girls")
			defer dropTableWithExternalPartition("public.partition_table2")
			createPartitionTableWithExternalPartition("public.partition_table3", "girls")
			defer dropTableWithExternalPartition("public.partition_table3")

			subject, err := options.NewOptions(backupCmdFlags)
			Expect(err).To(Not(HaveOccurred()))

			err = subject.ExpandIncludesForPartitions(connectionPool, backupCmdFlags)
			Expect(err).To(Not(HaveOccurred()))

			var expectedTableNames []string
			expectedTableNames = []string{
				"public.partition_table1",
				"public.partition_table2",
				"public.partition_table2_1_prt_other",
			}

			tables := subject.GetIncludedTables()
			sort.Strings(tables)
			Expect(tables).To(Equal(expectedTableNames))
		})
		It("returns parent and child tables if the filter includes a non-partition table with multiple inheritance relationships", func() {
			_ = backupCmdFlags.Set(options.INCLUDE_RELATION, "public.base")
			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE public.parent_one(one int);`)
			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE public.parent_two(two int);`)
			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE public.base() INHERITS (public.parent_one, public.parent_two);`)
			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE public.child_one() INHERITS (public.base);`)
			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE public.child_two() INHERITS (public.base);`)
			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE public.unrelated(three int);`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.parent_one CASCADE")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.parent_two CASCADE")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.unrelated")

			subject, err := options.NewOptions(backupCmdFlags)
			Expect(err).To(Not(HaveOccurred()))

			err = subject.ExpandIncludesForPartitions(connectionPool, backupCmdFlags)
			Expect(err).To(Not(HaveOccurred()))

			expectedTableNames := []string{
				"public.base",
				"public.child_one",
				"public.child_two",
				"public.parent_one",
				"public.parent_two",
			}

			tables := subject.GetIncludedTables()
			sort.Strings(tables)
			Expect(tables).To(Equal(expectedTableNames))
		})
		It("returns parent tables but not child tables if the filter includes a non-partition table with multiple inheritance relationships and no-inherits is set", func() {
			_ = backupCmdFlags.Set(options.NO_INHERITS, "true")
			_ = backupCmdFlags.Set(options.INCLUDE_RELATION, "public.base")
			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE public.parent_one(one int);`)
			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE public.parent_two(two int);`)
			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE public.base() INHERITS (public.parent_one, public.parent_two);`)
			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE public.child_one() INHERITS (public.base);`)
			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE public.child_two() INHERITS (public.base);`)
			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE public.unrelated(three int);`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.parent_one CASCADE")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.parent_two CASCADE")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.unrelated")

			subject, err := options.NewOptions(backupCmdFlags)
			Expect(err).To(Not(HaveOccurred()))

			err = subject.ExpandIncludesForPartitions(connectionPool, backupCmdFlags)
			Expect(err).To(Not(HaveOccurred()))

			expectedTableNames := []string{
				"public.base",
				"public.parent_one",
				"public.parent_two",
			}

			tables := subject.GetIncludedTables()
			sort.Strings(tables)
			Expect(tables).To(Equal(expectedTableNames))
		})
		It("returns parents of child tables if the children of an included table inherit from other tables", func() {
			_ = backupCmdFlags.Set(options.INCLUDE_RELATION, "public.base")
			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE public.base(one int);`)
			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE public.other(two int);`)
			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE public.child() INHERITS (public.base);`)
			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE public.grandchild() INHERITS (public.child, public.other);`)
			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE public.unrelated(three int);`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.base CASCADE")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.other CASCADE")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.unrelated")

			subject, err := options.NewOptions(backupCmdFlags)
			Expect(err).To(Not(HaveOccurred()))

			err = subject.ExpandIncludesForPartitions(connectionPool, backupCmdFlags)
			Expect(err).To(Not(HaveOccurred()))

			expectedTableNames := []string{
				"public.base",
				"public.child",
				"public.grandchild",
				"public.other",
			}

			tables := subject.GetIncludedTables()
			sort.Strings(tables)
			Expect(tables).To(Equal(expectedTableNames))
		})
		It("returns only the included table, without any children or other parents of children, if no-inherits is passed, ", func() {
			_ = backupCmdFlags.Set(options.NO_INHERITS, "true")
			_ = backupCmdFlags.Set(options.INCLUDE_RELATION, "public.base")
			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE public.base(one int);`)
			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE public.other(two int);`)
			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE public.child() INHERITS (public.base);`)
			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE public.grandchild() INHERITS (public.child, public.other);`)
			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE public.unrelated(three int);`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.base CASCADE")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.other CASCADE")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.unrelated")

			subject, err := options.NewOptions(backupCmdFlags)
			Expect(err).To(Not(HaveOccurred()))

			err = subject.ExpandIncludesForPartitions(connectionPool, backupCmdFlags)
			Expect(err).To(Not(HaveOccurred()))

			expectedTableNames := []string{
				"public.base",
			}

			tables := subject.GetIncludedTables()
			sort.Strings(tables)
			Expect(tables).To(Equal(expectedTableNames))
		})
	})
})
