package integration

import (
	"sort"

	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/options"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

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

			backup.DBValidate(connectionPool, tableList, false)
		})
	})
	Describe("ExpandIncludesForPartitions", func() {
		BeforeEach(func() {
			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE public."CAPpart"
				(id int, rank int, year int, gender char(1), count int )
					DISTRIBUTED BY (id)
					PARTITION BY LIST (gender)
					( PARTITION girls VALUES ('F'),
					  PARTITION boys VALUES ('M'),
					  DEFAULT PARTITION other )
			`)
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
			// that the user typed in, which is used by InitializeBackupReport() and
			// is important for incremental backups which must exactly match all flag input
			Expect(subject.GetOriginalIncludedTables()).To(Equal([]string{`public.CAPpart_1_prt_girls`}))
		})
		It("adds parent table when child partition with embedded quote character is included", func() {
			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE public."""hasquote"""
				(id int, rank int, year int, gender char(1), count int )
					DISTRIBUTED BY (id)
					PARTITION BY LIST (gender)
					( PARTITION girls VALUES ('F'),
					  PARTITION boys VALUES ('M'),
					  DEFAULT PARTITION other )
			`)
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

			createStmt := `CREATE TABLE public.rank (id int, rank int, year int, gender
char(1), count int )
DISTRIBUTED BY (id)
PARTITION BY LIST (gender)
( PARTITION girls VALUES ('F'),
  PARTITION boys VALUES ('M'),
  DEFAULT PARTITION other );`
			testhelper.AssertQueryRuns(connectionPool, createStmt)
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
			Expect(tables).To(HaveLen(4))
			Expect(tables).To(Equal(expectedTableNames))
		})
		It("returns parent and external leaf partition table if the filter includes a leaf table and leaf-partition-data is set", func() {
			_ = backupCmdFlags.Set(options.LEAF_PARTITION_DATA, "true")
			_ = backupCmdFlags.Set(options.INCLUDE_RELATION, "public.partition_table_1_prt_boys")
			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE public.partition_table (id int, gender char(1))
DISTRIBUTED BY (id)
PARTITION BY LIST (gender)
( PARTITION girls VALUES ('F'),
  PARTITION boys VALUES ('M'),
  DEFAULT PARTITION other );`)
			testhelper.AssertQueryRuns(connectionPool, `CREATE EXTERNAL WEB TABLE public.partition_table_ext_part_ (like public.partition_table_1_prt_girls)
EXECUTE 'echo -e "2\n1"' on host
FORMAT 'csv';`)
			testhelper.AssertQueryRuns(connectionPool, `ALTER TABLE public.partition_table EXCHANGE PARTITION girls WITH TABLE public.partition_table_ext_part_ WITHOUT VALIDATION;`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.partition_table")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.partition_table_ext_part_")

			subject, err := options.NewOptions(backupCmdFlags)
			Expect(err).To(Not(HaveOccurred()))

			err = subject.ExpandIncludesForPartitions(connectionPool, backupCmdFlags)
			Expect(err).To(Not(HaveOccurred()))

			expectedTableNames := []string{
				"public.partition_table",
				"public.partition_table_1_prt_boys",
				"public.partition_table_1_prt_girls",
			}

			tables := subject.GetIncludedTables()
			sort.Strings(tables)
			Expect(tables).To(HaveLen(3))
			Expect(tables).To(Equal(expectedTableNames))
		})
		It("returns external partition tables for an included parent table if the filter includes a parent partition table", func() {
			_ = backupCmdFlags.Set(options.INCLUDE_RELATION, "public.partition_table1")
			_ = backupCmdFlags.Set(options.INCLUDE_RELATION, "public.partition_table2_1_prt_other")

			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE public.partition_table1 (id int, gender char(1))
DISTRIBUTED BY (id)
PARTITION BY LIST (gender)
( PARTITION girls VALUES ('F'),
  PARTITION boys VALUES ('M'),
  DEFAULT PARTITION other );`)
			testhelper.AssertQueryRuns(connectionPool, `CREATE EXTERNAL WEB TABLE public.partition_table1_ext_part_ (like public.partition_table1_1_prt_boys)
EXECUTE 'echo -e "2\n1"' on host
FORMAT 'csv';`)
			testhelper.AssertQueryRuns(connectionPool, `ALTER TABLE public.partition_table1 EXCHANGE PARTITION boys WITH TABLE public.partition_table1_ext_part_ WITHOUT VALIDATION;`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.partition_table1")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.partition_table1_ext_part_")
			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE public.partition_table2 (id int, gender char(1))
DISTRIBUTED BY (id)
PARTITION BY LIST (gender)
( PARTITION girls VALUES ('F'),
  PARTITION boys VALUES ('M'),
  DEFAULT PARTITION other );`)
			testhelper.AssertQueryRuns(connectionPool, `CREATE EXTERNAL WEB TABLE public.partition_table2_ext_part_ (like public.partition_table2_1_prt_girls)
EXECUTE 'echo -e "2\n1"' on host
FORMAT 'csv';`)
			testhelper.AssertQueryRuns(connectionPool, `ALTER TABLE public.partition_table2 EXCHANGE PARTITION girls WITH TABLE public.partition_table2_ext_part_ WITHOUT VALIDATION;`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.partition_table2")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.partition_table2_ext_part_")
			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE public.partition_table3 (id int, gender char(1))
DISTRIBUTED BY (id)
PARTITION BY LIST (gender)
( PARTITION girls VALUES ('F'),
  PARTITION boys VALUES ('M'),
  DEFAULT PARTITION other );`)
			testhelper.AssertQueryRuns(connectionPool, `CREATE EXTERNAL WEB TABLE public.partition_table3_ext_part_ (like public.partition_table3_1_prt_girls)
EXECUTE 'echo -e "2\n1"' on host
FORMAT 'csv';`)
			testhelper.AssertQueryRuns(connectionPool, `ALTER TABLE public.partition_table3 EXCHANGE PARTITION girls WITH TABLE public.partition_table3_ext_part_ WITHOUT VALIDATION;`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.partition_table3")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.partition_table3_ext_part_")

			subject, err := options.NewOptions(backupCmdFlags)
			Expect(err).To(Not(HaveOccurred()))

			err = subject.ExpandIncludesForPartitions(connectionPool, backupCmdFlags)
			Expect(err).To(Not(HaveOccurred()))

			expectedTableNames := []string{
				"public.partition_table1",
				"public.partition_table1_1_prt_boys",
				"public.partition_table2",
				"public.partition_table2_1_prt_girls",
				"public.partition_table2_1_prt_other",
			}

			tables := subject.GetIncludedTables()
			sort.Strings(tables)
			Expect(tables).To(HaveLen(5))
			Expect(tables).To(Equal(expectedTableNames))
		})
	})
})
