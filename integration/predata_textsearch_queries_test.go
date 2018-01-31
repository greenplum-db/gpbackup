package integration

import (
	"github.com/greenplum-db/gp-common-go-libs/structmatcher"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup integration tests", func() {
	Describe("GetTextSearchParsers", func() {
		BeforeEach(func() {
			testutils.SkipIf4(connection)
		})
		It("returns a text search parser without a headline", func() {
			testutils.AssertQueryRuns(connection, "CREATE TEXT SEARCH PARSER testparser(START = prsd_start, GETTOKEN = prsd_nexttoken, END = prsd_end, LEXTYPES = prsd_lextype);")
			defer testutils.AssertQueryRuns(connection, "DROP TEXT SEARCH PARSER testparser")
			parsers := backup.GetTextSearchParsers(connection)

			expectedParser := backup.TextSearchParser{Oid: 1, Schema: "public", Name: "testparser", StartFunc: "prsd_start", TokenFunc: "prsd_nexttoken", EndFunc: "prsd_end", LexTypesFunc: "prsd_lextype", HeadlineFunc: ""}

			Expect(len(parsers)).To(Equal(1))
			structmatcher.ExpectStructsToMatchExcluding(&expectedParser, &parsers[0], "Oid")
		})
		It("returns a text search parser with a headline", func() {
			testutils.AssertQueryRuns(connection, "CREATE TEXT SEARCH PARSER testparser(START = prsd_start, GETTOKEN = prsd_nexttoken, END = prsd_end, LEXTYPES = prsd_lextype, HEADLINE = prsd_headline);")
			defer testutils.AssertQueryRuns(connection, "DROP TEXT SEARCH PARSER testparser")
			parsers := backup.GetTextSearchParsers(connection)

			expectedParser := backup.TextSearchParser{Oid: 1, Schema: "public", Name: "testparser", StartFunc: "prsd_start", TokenFunc: "prsd_nexttoken", EndFunc: "prsd_end", LexTypesFunc: "prsd_lextype", HeadlineFunc: "prsd_headline"}

			Expect(len(parsers)).To(Equal(1))
			structmatcher.ExpectStructsToMatchExcluding(&expectedParser, &parsers[0], "Oid")
		})
		It("returns a text search parser from a specific schema ", func() {
			testutils.AssertQueryRuns(connection, "CREATE TEXT SEARCH PARSER testparser(START = prsd_start, GETTOKEN = prsd_nexttoken, END = prsd_end, LEXTYPES = prsd_lextype);")
			defer testutils.AssertQueryRuns(connection, "DROP TEXT SEARCH PARSER testparser")
			testutils.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
			defer testutils.AssertQueryRuns(connection, "DROP SCHEMA testschema")
			testutils.AssertQueryRuns(connection, "CREATE TEXT SEARCH PARSER testschema.testparser(START = prsd_start, GETTOKEN = prsd_nexttoken, END = prsd_end, LEXTYPES = prsd_lextype);")
			defer testutils.AssertQueryRuns(connection, "DROP TEXT SEARCH PARSER testschema.testparser")
			backup.SetIncludeSchemas([]string{"testschema"})

			parsers := backup.GetTextSearchParsers(connection)

			expectedParser := backup.TextSearchParser{Oid: 1, Schema: "testschema", Name: "testparser", StartFunc: "prsd_start", TokenFunc: "prsd_nexttoken", EndFunc: "prsd_end", LexTypesFunc: "prsd_lextype", HeadlineFunc: ""}

			Expect(len(parsers)).To(Equal(1))
			structmatcher.ExpectStructsToMatchExcluding(&expectedParser, &parsers[0], "Oid")
		})
	})
	Describe("GetTextSearchTemplates", func() {
		BeforeEach(func() {
			testutils.SkipIf4(connection)
		})
		It("returns a text search template without an init function", func() {
			testutils.AssertQueryRuns(connection, "CREATE TEXT SEARCH TEMPLATE testtemplate(LEXIZE = dsimple_lexize);")
			defer testutils.AssertQueryRuns(connection, "DROP TEXT SEARCH TEMPLATE testtemplate")
			templates := backup.GetTextSearchTemplates(connection)

			expectedTemplate := backup.TextSearchTemplate{Oid: 1, Schema: "public", Name: "testtemplate", InitFunc: "", LexizeFunc: "dsimple_lexize"}

			Expect(len(templates)).To(Equal(1))
			structmatcher.ExpectStructsToMatchExcluding(&expectedTemplate, &templates[0], "Oid")
		})
		It("returns a text search template with an init function", func() {
			testutils.AssertQueryRuns(connection, "CREATE TEXT SEARCH TEMPLATE testtemplate(INIT = dsimple_init, LEXIZE = dsimple_lexize);")
			defer testutils.AssertQueryRuns(connection, "DROP TEXT SEARCH TEMPLATE testtemplate")
			templates := backup.GetTextSearchTemplates(connection)

			expectedTemplate := backup.TextSearchTemplate{Oid: 1, Schema: "public", Name: "testtemplate", InitFunc: "dsimple_init", LexizeFunc: "dsimple_lexize"}

			Expect(len(templates)).To(Equal(1))
			structmatcher.ExpectStructsToMatchExcluding(&expectedTemplate, &templates[0], "Oid")
		})
		It("returns a text search template from a specific schema", func() {
			testutils.AssertQueryRuns(connection, "CREATE TEXT SEARCH TEMPLATE testtemplate(LEXIZE = dsimple_lexize);")
			defer testutils.AssertQueryRuns(connection, "DROP TEXT SEARCH TEMPLATE testtemplate")
			testutils.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
			defer testutils.AssertQueryRuns(connection, "DROP SCHEMA testschema")
			testutils.AssertQueryRuns(connection, "CREATE TEXT SEARCH TEMPLATE testschema.testtemplate(LEXIZE = dsimple_lexize);")
			defer testutils.AssertQueryRuns(connection, "DROP TEXT SEARCH TEMPLATE testschema.testtemplate")

			backup.SetIncludeSchemas([]string{"testschema"})
			templates := backup.GetTextSearchTemplates(connection)

			expectedTemplate := backup.TextSearchTemplate{Oid: 1, Schema: "testschema", Name: "testtemplate", InitFunc: "", LexizeFunc: "dsimple_lexize"}

			Expect(len(templates)).To(Equal(1))
			structmatcher.ExpectStructsToMatchExcluding(&expectedTemplate, &templates[0], "Oid")
		})
	})
	Describe("GetTextSearchDictionaries", func() {
		BeforeEach(func() {
			testutils.SkipIf4(connection)
		})
		It("returns a text search dictionary with init options", func() {
			testutils.AssertQueryRuns(connection, "CREATE TEXT SEARCH DICTIONARY testdictionary(TEMPLATE = snowball, LANGUAGE = 'russian', STOPWORDS = 'russian');")
			defer testutils.AssertQueryRuns(connection, "DROP TEXT SEARCH DICTIONARY testdictionary")
			dictionaries := backup.GetTextSearchDictionaries(connection)

			expectedDictionary := backup.TextSearchDictionary{Oid: 1, Schema: "public", Name: "testdictionary", Template: "pg_catalog.snowball", InitOption: "language = 'russian', stopwords = 'russian'"}
			Expect(len(dictionaries)).To(Equal(1))
			structmatcher.ExpectStructsToMatchExcluding(&expectedDictionary, &dictionaries[0], "Oid")
		})
		It("returns a text search dictionary without init options", func() {
			testutils.AssertQueryRuns(connection, "CREATE TEXT SEARCH DICTIONARY testdictionary (TEMPLATE = 'simple');")
			defer testutils.AssertQueryRuns(connection, "DROP TEXT SEARCH DICTIONARY testdictionary")
			dictionaries := backup.GetTextSearchDictionaries(connection)

			expectedDictionary := backup.TextSearchDictionary{Oid: 1, Schema: "public", Name: "testdictionary", Template: "pg_catalog.simple", InitOption: ""}
			Expect(len(dictionaries)).To(Equal(1))
			structmatcher.ExpectStructsToMatchExcluding(&expectedDictionary, &dictionaries[0], "Oid")
		})
		It("returns a text search dictionary from a specific schema", func() {
			testutils.AssertQueryRuns(connection, "CREATE TEXT SEARCH DICTIONARY testdictionary (TEMPLATE = 'simple');")
			defer testutils.AssertQueryRuns(connection, "DROP TEXT SEARCH DICTIONARY testdictionary")
			testutils.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
			defer testutils.AssertQueryRuns(connection, "DROP SCHEMA testschema")
			testutils.AssertQueryRuns(connection, "CREATE TEXT SEARCH DICTIONARY testschema.testdictionary (TEMPLATE = 'simple');")
			defer testutils.AssertQueryRuns(connection, "DROP TEXT SEARCH DICTIONARY testschema.testdictionary")

			backup.SetIncludeSchemas([]string{"testschema"})
			dictionaries := backup.GetTextSearchDictionaries(connection)

			expectedDictionary := backup.TextSearchDictionary{Oid: 1, Schema: "testschema", Name: "testdictionary", Template: "pg_catalog.simple", InitOption: ""}
			Expect(len(dictionaries)).To(Equal(1))
			structmatcher.ExpectStructsToMatchExcluding(&expectedDictionary, &dictionaries[0], "Oid")
		})
	})
	Describe("GetTextSearchConfigurations", func() {
		BeforeEach(func() {
			testutils.SkipIf4(connection)
		})
		It("returns a text search configuration without an init function", func() {
			testutils.AssertQueryRuns(connection, `CREATE TEXT SEARCH CONFIGURATION testconfiguration (PARSER = pg_catalog."default");`)
			defer testutils.AssertQueryRuns(connection, "DROP TEXT SEARCH CONFIGURATION testconfiguration")
			configurations := backup.GetTextSearchConfigurations(connection)

			expectedConfiguration := backup.TextSearchConfiguration{Oid: 1, Schema: "public", Name: "testconfiguration", Parser: `pg_catalog."default"`, TokenToDicts: map[string][]string{}}

			Expect(len(configurations)).To(Equal(1))
			structmatcher.ExpectStructsToMatchExcluding(&expectedConfiguration, &configurations[0], "Oid")
		})
		It("returns a text search configuration with an init function", func() {
			testutils.AssertQueryRuns(connection, `CREATE TEXT SEARCH CONFIGURATION testconfiguration ( PARSER = pg_catalog."default");`)
			defer testutils.AssertQueryRuns(connection, "DROP TEXT SEARCH CONFIGURATION testconfiguration")
			configurations := backup.GetTextSearchConfigurations(connection)

			expectedConfiguration := backup.TextSearchConfiguration{Oid: 1, Schema: "public", Name: "testconfiguration", Parser: `pg_catalog."default"`, TokenToDicts: map[string][]string{}}

			Expect(len(configurations)).To(Equal(1))
			structmatcher.ExpectStructsToMatchExcluding(&expectedConfiguration, &configurations[0], "Oid")
		})
		It("returns a text search configuration with mappings", func() {
			testutils.AssertQueryRuns(connection, `CREATE TEXT SEARCH CONFIGURATION testconfiguration ( PARSER = pg_catalog."default");`)
			defer testutils.AssertQueryRuns(connection, "DROP TEXT SEARCH CONFIGURATION testconfiguration")

			testutils.AssertQueryRuns(connection, "ALTER TEXT SEARCH CONFIGURATION testconfiguration ADD MAPPING FOR uint WITH simple;")
			testutils.AssertQueryRuns(connection, "ALTER TEXT SEARCH CONFIGURATION testconfiguration ADD MAPPING FOR asciiword WITH danish_stem;")

			configurations := backup.GetTextSearchConfigurations(connection)

			expectedConfiguration := backup.TextSearchConfiguration{Oid: 1, Schema: "public", Name: "testconfiguration", Parser: `pg_catalog."default"`, TokenToDicts: map[string][]string{}}
			expectedConfiguration.TokenToDicts = map[string][]string{"uint": {"simple"}, "asciiword": {"danish_stem"}}

			Expect(len(configurations)).To(Equal(1))
			structmatcher.ExpectStructsToMatchExcluding(&expectedConfiguration, &configurations[0], "Oid")
		})
		It("returns a text search configuration from a specific schema", func() {
			testutils.AssertQueryRuns(connection, `CREATE TEXT SEARCH CONFIGURATION testconfiguration (PARSER = pg_catalog."default");`)
			defer testutils.AssertQueryRuns(connection, "DROP TEXT SEARCH CONFIGURATION testconfiguration")
			testutils.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
			defer testutils.AssertQueryRuns(connection, "DROP SCHEMA testschema")
			testutils.AssertQueryRuns(connection, `CREATE TEXT SEARCH CONFIGURATION testschema.testconfiguration (PARSER = pg_catalog."default");`)
			defer testutils.AssertQueryRuns(connection, "DROP TEXT SEARCH CONFIGURATION testschema.testconfiguration")

			backup.SetIncludeSchemas([]string{"testschema"})
			configurations := backup.GetTextSearchConfigurations(connection)

			expectedConfiguration := backup.TextSearchConfiguration{Oid: 1, Schema: "testschema", Name: "testconfiguration", Parser: `pg_catalog."default"`, TokenToDicts: map[string][]string{}}

			Expect(len(configurations)).To(Equal(1))
			structmatcher.ExpectStructsToMatchExcluding(&expectedConfiguration, &configurations[0], "Oid")
		})
	})
})
