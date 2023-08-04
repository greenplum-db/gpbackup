package options_test

import (
	"io/ioutil"
	"os"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/options"
	"github.com/spf13/pflag"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("options", func() {
	var (
		myflags *pflag.FlagSet
	)
	BeforeEach(func() {
		myflags = &pflag.FlagSet{}
		options.SetBackupFlagDefaults(myflags)
	})
	Describe("Options initialization", func() {
		It("returns no included tables when none specified", func() {
			subject, err := options.NewOptions(myflags)
			Expect(err).To(Not(HaveOccurred()))

			includedTables := subject.GetIncludedTables()
			Expect(includedTables).To(BeEmpty())
			originalIncludedTables := subject.GetOriginalIncludedTables()
			Expect(originalIncludedTables).To(BeEmpty())
		})
		It("returns the include tables when one table in flag", func() {
			err := myflags.Set(options.INCLUDE_RELATION, "foo.bar")
			Expect(err).ToNot(HaveOccurred())

			subject, err := options.NewOptions(myflags)
			Expect(err).To(Not(HaveOccurred()))

			includedTables := subject.GetIncludedTables()
			Expect(includedTables).To(HaveLen(1))
			Expect(includedTables[0]).To(Equal("foo.bar"))
			originalIncludedTables := subject.GetOriginalIncludedTables()
			Expect(originalIncludedTables[0]).To(Equal("foo.bar"))
		})
		It("returns an include with special characters besides quote and dot", func() {
			err := myflags.Set(options.INCLUDE_RELATION, `foo '~#$%^&*()_-+[]{}><\|;:/?!\t\n,.bar`)
			Expect(err).ToNot(HaveOccurred())
			subject, err := options.NewOptions(myflags)
			Expect(err).To(Not(HaveOccurred()))

			includedTables := subject.GetIncludedTables()
			Expect(includedTables).To(HaveLen(1))
			Expect(includedTables[0]).To(Equal(`foo '~#$%^&*()_-+[]{}><\|;:/?!\t\n,.bar`))
		})
		It("returns all included tables when multiple individual flags provided", func() {
			err := myflags.Set(options.INCLUDE_RELATION, "foo.bar")
			Expect(err).ToNot(HaveOccurred())
			err = myflags.Set(options.INCLUDE_RELATION, "bar.baz")
			Expect(err).ToNot(HaveOccurred())

			subject, err := options.NewOptions(myflags)
			Expect(err).To(Not(HaveOccurred()))

			includedTables := subject.GetIncludedTables()
			Expect(includedTables).To(HaveLen(2))
			Expect(includedTables[0]).To(Equal("foo.bar"))
			Expect(includedTables[1]).To(Equal("bar.baz"))
		})
		It("returns the text-file tables when specified", func() {
			file, err := ioutil.TempFile("/tmp", "gpbackup_test_options*.txt")
			Expect(err).To(Not(HaveOccurred()))
			defer func() {
				_ = os.Remove(file.Name())
			}()
			_, err = file.WriteString("myschema.mytable\n")
			Expect(err).To(Not(HaveOccurred()))
			_, err = file.WriteString("myschema.mytable2\n")
			Expect(err).To(Not(HaveOccurred()))
			err = file.Close()
			Expect(err).To(Not(HaveOccurred()))

			err = myflags.Set(options.INCLUDE_RELATION_FILE, file.Name())
			Expect(err).ToNot(HaveOccurred())
			subject, err := options.NewOptions(myflags)
			Expect(err).To(Not(HaveOccurred()))

			includedTables := subject.GetIncludedTables()
			Expect(includedTables).To(HaveLen(2))
			Expect(includedTables[0]).To(Equal("myschema.mytable"))
			Expect(includedTables[1]).To(Equal("myschema.mytable2"))
		})
		It("sets the INCLUDE_RELATIONS flag from file", func() {
			file, err := ioutil.TempFile("/tmp", "gpbackup_test_options*.txt")
			Expect(err).To(Not(HaveOccurred()))
			defer func() {
				_ = os.Remove(file.Name())
			}()
			_, err = file.WriteString("myschema.mytable\n")
			Expect(err).To(Not(HaveOccurred()))
			_, err = file.WriteString("myschema.mytable2\n")
			Expect(err).To(Not(HaveOccurred()))
			err = file.Close()
			Expect(err).To(Not(HaveOccurred()))

			err = myflags.Set(options.INCLUDE_RELATION_FILE, file.Name())
			Expect(err).ToNot(HaveOccurred())
			_, err = options.NewOptions(myflags)
			Expect(err).To(Not(HaveOccurred()))

			includedTables, err := myflags.GetStringArray(options.INCLUDE_RELATION)
			Expect(err).ToNot(HaveOccurred())
			Expect(includedTables).To(HaveLen(2))
			Expect(includedTables[0]).To(Equal("myschema.mytable"))
			Expect(includedTables[1]).To(Equal("myschema.mytable2"))
		})
		It("skips empty lines in files provided for filtering tables", func() {
			file, err := ioutil.TempFile("/tmp", "gpbackup_test_options*.txt")
			Expect(err).To(Not(HaveOccurred()))
			defer func() {
				_ = os.Remove(file.Name())
			}()
			_, err = file.WriteString("myschema.mytable\n")
			Expect(err).To(Not(HaveOccurred()))
			_, err = file.WriteString("\n")
			Expect(err).To(Not(HaveOccurred()))
			Expect(err).To(Not(HaveOccurred()))
			_, err = file.WriteString("\n")
			Expect(err).To(Not(HaveOccurred()))
			_, err = file.WriteString("myschema.mytable2\n")
			Expect(err).To(Not(HaveOccurred()))
			Expect(err).To(Not(HaveOccurred()))
			_, err = file.WriteString("\n")
			Expect(err).To(Not(HaveOccurred()))
			_, err = file.WriteString("\n")
			err = file.Close()
			Expect(err).To(Not(HaveOccurred()))

			err = myflags.Set(options.EXCLUDE_RELATION_FILE, file.Name())
			Expect(err).ToNot(HaveOccurred())
			_, err = options.NewOptions(myflags)
			Expect(err).To(Not(HaveOccurred()))

			excludedTables, err := myflags.GetStringArray(options.EXCLUDE_RELATION)
			Expect(err).ToNot(HaveOccurred())
			Expect(excludedTables).To(HaveLen(2))
			Expect(excludedTables[0]).To(Equal("myschema.mytable"))
			Expect(excludedTables[1]).To(Equal("myschema.mytable2"))
		})
		It("skips empty lines in files provided for filtering schemas", func() {
			file, err := ioutil.TempFile("/tmp", "gpbackup_test_options*.txt")
			Expect(err).To(Not(HaveOccurred()))
			defer func() {
				_ = os.Remove(file.Name())
			}()
			_, err = file.WriteString("myschema1\n")
			Expect(err).To(Not(HaveOccurred()))
			_, err = file.WriteString("\n")
			Expect(err).To(Not(HaveOccurred()))
			Expect(err).To(Not(HaveOccurred()))
			_, err = file.WriteString("\n")
			Expect(err).To(Not(HaveOccurred()))
			_, err = file.WriteString("myschema2\n")
			Expect(err).To(Not(HaveOccurred()))
			Expect(err).To(Not(HaveOccurred()))
			_, err = file.WriteString("\n")
			Expect(err).To(Not(HaveOccurred()))
			_, err = file.WriteString("\n")
			err = file.Close()
			Expect(err).To(Not(HaveOccurred()))

			err = myflags.Set(options.INCLUDE_SCHEMA_FILE, file.Name())
			Expect(err).ToNot(HaveOccurred())
			_, err = options.NewOptions(myflags)
			Expect(err).To(Not(HaveOccurred()))

			includedSchemas, err := myflags.GetStringArray(options.INCLUDE_SCHEMA)
			Expect(err).ToNot(HaveOccurred())
			Expect(includedSchemas).To(HaveLen(2))
			Expect(includedSchemas[0]).To(Equal("myschema1"))
			Expect(includedSchemas[1]).To(Equal("myschema2"))
		})
		It("it remembers flag values for INCLUDE_SCHEMA, EXCLUDE*, LEAF_PARTITION_DATA", func() {
			err := myflags.Set(options.INCLUDE_SCHEMA, "my include schema")
			Expect(err).ToNot(HaveOccurred())
			err = myflags.Set(options.EXCLUDE_SCHEMA, "my exclude schema")
			Expect(err).ToNot(HaveOccurred())
			err = myflags.Set(options.LEAF_PARTITION_DATA, "true")
			Expect(err).ToNot(HaveOccurred())

			subject, err := options.NewOptions(myflags)
			Expect(err).To(Not(HaveOccurred()))

			Expect(subject.GetIncludedSchemas()[0]).To(Equal("my include schema"))
			Expect(subject.GetExcludedSchemas()[0]).To(Equal("my exclude schema"))
		})
		It("returns an error upon invalid inclusions", func() {
			err := myflags.Set(options.INCLUDE_RELATION, "foo")
			Expect(err).ToNot(HaveOccurred())
			_, err = options.NewOptions(myflags)
			Expect(err).To(HaveOccurred())
		})
		Describe("AddIncludeRelation", func() {
			It("it adds a relation", func() {
				subject, err := options.NewOptions(myflags)
				Expect(err).To(Not(HaveOccurred()))
				subject.AddIncludedRelation("public.foobar")
				Expect(subject.GetIncludedTables()).To(Equal([]string{"public.foobar"}))
				Expect(subject.GetOriginalIncludedTables()).To(BeEmpty())
			})
		})
	})
	Describe("SeparateSchemaAndTable", func() {
		It("properly splits the strings", func() {
			tableList := []string{
				"foo.Bar",
				"FOO.Bar",
				"FO!@#.BAR",
				`ezschema."myt.able"`,          // dots in one part
				`"mys.chema"."othert.able"`,    // dots in both parts
				`easy."tryme""`,                // quotes with escaping
				`"mys.chema"."anoth"ert.able"`, // quotes without escaping
				`easy.no"quotes`,               // quotes without wrapping or escaping
			}
			expectedFqn := []options.Relation{
				{Schema: `foo`, Name: `Bar`},
				{Schema: `FOO`, Name: `Bar`},
				{Schema: `FO!@#`, Name: `BAR`},
				{Schema: "ezschema", Name: "myt.able"},
				{Schema: "mys.chema", Name: "othert.able"},
				{Schema: "easy", Name: "tryme\""},
				{Schema: "mys.chema", Name: "anoth\"ert.able"},
				{Schema: "easy", Name: "no\"quotes"},
			}
			resultFqn, err := options.SeparateSchemaAndTable(tableList)
			Expect(err).ToNot(HaveOccurred())
			Expect(resultFqn).To(Equal(expectedFqn))
		})
		It("fails to split TableName", func() {
			tableList := []string{"foo."}
			_, err := options.SeparateSchemaAndTable(tableList)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("foo."))
		})
		It("fails to split SchemaName", func() {
			tableList := []string{".bar"}
			_, err := options.SeparateSchemaAndTable(tableList)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(".bar"))
		})
		It("fails to split SchemaName or tableName (no '.')", func() {
			tableList := []string{"foobar"}
			_, err := options.SeparateSchemaAndTable(tableList)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("foobar"))
		})
		It("fails when there is more than one dot and no correct quoting", func() {
			// todo in a future story, establish a way for users to escape dots to show us which one is *in* the name versus the dot that divides schemaname from tablename
			tableList := []string{"foobar.baz.bam"}
			_, err := options.SeparateSchemaAndTable(tableList)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("foobar.baz.bam"))
		})
	})
	Describe("QuoteTableNames", func() {
		var (
			conn   *dbconn.DBConn
			mockdb sqlmock.Sqlmock
		)
		BeforeEach(func() {
			conn, mockdb, _, _, _ = testhelper.SetupTestEnvironment()
		})

		It("returns empty result if given empty list", func() {
			tablenames := make([]string, 0)
			quotedTableNames, err := options.QuoteTableNames(conn, tablenames)
			Expect(err).To(Not(HaveOccurred()))
			Expect(tablenames).To(Equal(quotedTableNames))
		})
		It("returns a single result when given a single fqn", func() {
			tablenames := []string{"public.foo"}
			queryMock := mockdb.ExpectQuery("SELECT quote_")
			resultRows := sqlmock.NewRows([]string{"schemaname", "tablename"}).
				AddRow("public", "foo")

			queryMock.WillReturnRows(resultRows)

			quotedTableNames, err := options.QuoteTableNames(conn, tablenames)
			Expect(err).To(Not(HaveOccurred()))
			Expect(tablenames).To(Equal(quotedTableNames))
		})
		It("returns an array of correctly formatted fqn's", func() {
			tablenames := []string{"public.one", "public.two", "public.three"}

			queryMock := mockdb.ExpectQuery("SELECT quote_")
			resultRows := sqlmock.NewRows([]string{"schemaname", "tablename"}).
				AddRow("public", "one")
			queryMock.WillReturnRows(resultRows)

			queryMock = mockdb.ExpectQuery("SELECT quote_")
			resultRows = sqlmock.NewRows([]string{"schemaname", "tablename"}).
				AddRow("public", "two")
			queryMock.WillReturnRows(resultRows)

			queryMock = mockdb.ExpectQuery("SELECT quote_")
			resultRows = sqlmock.NewRows([]string{"schemaname", "tablename"}).
				AddRow("public", "three")
			queryMock.WillReturnRows(resultRows)

			quotedTableNames, err := options.QuoteTableNames(conn, tablenames)
			Expect(err).To(Not(HaveOccurred()))
			Expect(tablenames).To(Equal(quotedTableNames))
		})
	})
})
