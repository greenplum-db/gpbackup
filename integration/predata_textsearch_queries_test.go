package integration

import (
	"github.com/greenplum-db/gp-common-go-libs/structmatcher"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/options"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup integration tests", func() {
	Describe("GetTextSearchParsers", func() {
		It("returns a text search parser without a headline", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TEXT SEARCH PARSER public.testparser(START = prsd_start, GETTOKEN = prsd_nexttoken, END = prsd_end, LEXTYPES = prsd_lextype);")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TEXT SEARCH PARSER public.testparser")
			parsers := backup.GetTextSearchParsers(connectionPool)

			expectedParser := backup.TextSearchParser{Oid: 1, Schema: "public", Name: "testparser", StartFunc: "prsd_start", TokenFunc: "prsd_nexttoken", EndFunc: "prsd_end", LexTypesFunc: "prsd_lextype", HeadlineFunc: ""}

			Expect(parsers).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&expectedParser, &parsers[0], "Oid")
		})
		It("returns a text search parser with a headline", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TEXT SEARCH PARSER public.testparser(START = prsd_start, GETTOKEN = prsd_nexttoken, END = prsd_end, LEXTYPES = prsd_lextype, HEADLINE = prsd_headline);")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TEXT SEARCH PARSER public.testparser")
			parsers := backup.GetTextSearchParsers(connectionPool)

			expectedParser := backup.TextSearchParser{Oid: 1, Schema: "public", Name: "testparser", StartFunc: "prsd_start", TokenFunc: "prsd_nexttoken", EndFunc: "prsd_end", LexTypesFunc: "prsd_lextype", HeadlineFunc: "prsd_headline"}

			Expect(parsers).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&expectedParser, &parsers[0], "Oid")
		})
		It("returns a text search parser from a specific schema ", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TEXT SEARCH PARSER public.testparser(START = prsd_start, GETTOKEN = prsd_nexttoken, END = prsd_end, LEXTYPES = prsd_lextype);")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TEXT SEARCH PARSER public.testparser")
			testhelper.AssertQueryRuns(connectionPool, "CREATE SCHEMA testschema")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA testschema")
			testhelper.AssertQueryRuns(connectionPool, "CREATE TEXT SEARCH PARSER testschema.testparser(START = prsd_start, GETTOKEN = prsd_nexttoken, END = prsd_end, LEXTYPES = prsd_lextype);")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TEXT SEARCH PARSER testschema.testparser")
			_ = backupCmdFlags.Set(options.INCLUDE_SCHEMA, "testschema")

			parsers := backup.GetTextSearchParsers(connectionPool)

			expectedParser := backup.TextSearchParser{Oid: 1, Schema: "testschema", Name: "testparser", StartFunc: "prsd_start", TokenFunc: "prsd_nexttoken", EndFunc: "prsd_end", LexTypesFunc: "prsd_lextype", HeadlineFunc: ""}

			Expect(parsers).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&expectedParser, &parsers[0], "Oid")
		})
	})
	Describe("GetTextSearchTemplates", func() {
		It("returns a text search template without an init function", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TEXT SEARCH TEMPLATE public.testtemplate(LEXIZE = dsimple_lexize);")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TEXT SEARCH TEMPLATE public.testtemplate")
			templates := backup.GetTextSearchTemplates(connectionPool)

			expectedTemplate := backup.TextSearchTemplate{Oid: 1, Schema: "public", Name: "testtemplate", InitFunc: "", LexizeFunc: "dsimple_lexize"}

			Expect(templates).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&expectedTemplate, &templates[0], "Oid")
		})
		It("returns a text search template with an init function", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TEXT SEARCH TEMPLATE public.testtemplate(INIT = dsimple_init, LEXIZE = dsimple_lexize);")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TEXT SEARCH TEMPLATE public.testtemplate")
			templates := backup.GetTextSearchTemplates(connectionPool)

			expectedTemplate := backup.TextSearchTemplate{Oid: 1, Schema: "public", Name: "testtemplate", InitFunc: "dsimple_init", LexizeFunc: "dsimple_lexize"}

			Expect(templates).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&expectedTemplate, &templates[0], "Oid")
		})
		It("returns a text search template from a specific schema", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TEXT SEARCH TEMPLATE public.testtemplate(LEXIZE = dsimple_lexize);")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TEXT SEARCH TEMPLATE public.testtemplate")
			testhelper.AssertQueryRuns(connectionPool, "CREATE SCHEMA testschema")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA testschema")
			testhelper.AssertQueryRuns(connectionPool, "CREATE TEXT SEARCH TEMPLATE testschema.testtemplate(LEXIZE = dsimple_lexize);")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TEXT SEARCH TEMPLATE testschema.testtemplate")

			_ = backupCmdFlags.Set(options.INCLUDE_SCHEMA, "testschema")
			templates := backup.GetTextSearchTemplates(connectionPool)

			expectedTemplate := backup.TextSearchTemplate{Oid: 1, Schema: "testschema", Name: "testtemplate", InitFunc: "", LexizeFunc: "dsimple_lexize"}

			Expect(templates).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&expectedTemplate, &templates[0], "Oid")
		})
	})
	Describe("GetTextSearchDictionaries", func() {
		It("returns a text search dictionary with init options", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TEXT SEARCH DICTIONARY public.testdictionary(TEMPLATE = snowball, LANGUAGE = 'russian', STOPWORDS = 'russian');")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TEXT SEARCH DICTIONARY public.testdictionary")
			dictionaries := backup.GetTextSearchDictionaries(connectionPool)

			expectedDictionary := backup.TextSearchDictionary{Oid: 1, Schema: "public", Name: "testdictionary", Template: "pg_catalog.snowball", InitOption: "language = 'russian', stopwords = 'russian'"}
			Expect(dictionaries).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&expectedDictionary, &dictionaries[0], "Oid")
		})
		It("returns a text search dictionary without init options", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TEXT SEARCH DICTIONARY public.testdictionary (TEMPLATE = 'simple');")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TEXT SEARCH DICTIONARY public.testdictionary")
			dictionaries := backup.GetTextSearchDictionaries(connectionPool)

			expectedDictionary := backup.TextSearchDictionary{Oid: 1, Schema: "public", Name: "testdictionary", Template: "pg_catalog.simple", InitOption: ""}
			Expect(dictionaries).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&expectedDictionary, &dictionaries[0], "Oid")
		})
		It("returns a text search dictionary from a specific schema", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TEXT SEARCH DICTIONARY public.testdictionary (TEMPLATE = 'simple');")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TEXT SEARCH DICTIONARY public.testdictionary")
			testhelper.AssertQueryRuns(connectionPool, "CREATE SCHEMA testschema")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA testschema")
			testhelper.AssertQueryRuns(connectionPool, "CREATE TEXT SEARCH DICTIONARY testschema.testdictionary (TEMPLATE = 'simple');")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TEXT SEARCH DICTIONARY testschema.testdictionary")

			_ = backupCmdFlags.Set(options.INCLUDE_SCHEMA, "testschema")
			dictionaries := backup.GetTextSearchDictionaries(connectionPool)

			expectedDictionary := backup.TextSearchDictionary{Oid: 1, Schema: "testschema", Name: "testdictionary", Template: "pg_catalog.simple", InitOption: ""}
			Expect(dictionaries).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&expectedDictionary, &dictionaries[0], "Oid")
		})
	})
	Describe("GetTextSearchConfigurations", func() {
		It("returns a text search configuration without an init function", func() {
			testhelper.AssertQueryRuns(connectionPool, `CREATE TEXT SEARCH CONFIGURATION public.testconfiguration (PARSER = pg_catalog."default");`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TEXT SEARCH CONFIGURATION public.testconfiguration")
			configurations := backup.GetTextSearchConfigurations(connectionPool)

			expectedConfiguration := backup.TextSearchConfiguration{Oid: 1, Schema: "public", Name: "testconfiguration", Parser: `pg_catalog."default"`, TokenToDicts: map[string][]string{}}

			Expect(configurations).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&expectedConfiguration, &configurations[0], "Oid")
		})
		It("returns a text search configuration with an init function", func() {
			testhelper.AssertQueryRuns(connectionPool, `CREATE TEXT SEARCH CONFIGURATION public.testconfiguration ( PARSER = pg_catalog."default");`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TEXT SEARCH CONFIGURATION public.testconfiguration")
			configurations := backup.GetTextSearchConfigurations(connectionPool)

			expectedConfiguration := backup.TextSearchConfiguration{Oid: 1, Schema: "public", Name: "testconfiguration", Parser: `pg_catalog."default"`, TokenToDicts: map[string][]string{}}

			Expect(configurations).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&expectedConfiguration, &configurations[0], "Oid")
		})
		It("returns a text search configuration with mappings", func() {
			testhelper.AssertQueryRuns(connectionPool, `CREATE TEXT SEARCH CONFIGURATION public.testconfiguration ( PARSER = pg_catalog."default");`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TEXT SEARCH CONFIGURATION public.testconfiguration")

			testhelper.AssertQueryRuns(connectionPool, "ALTER TEXT SEARCH CONFIGURATION public.testconfiguration ADD MAPPING FOR uint WITH simple;")
			testhelper.AssertQueryRuns(connectionPool, "ALTER TEXT SEARCH CONFIGURATION public.testconfiguration ADD MAPPING FOR asciiword WITH danish_stem;")

			configurations := backup.GetTextSearchConfigurations(connectionPool)

			expectedConfiguration := backup.TextSearchConfiguration{Oid: 1, Schema: "public", Name: "testconfiguration", Parser: `pg_catalog."default"`, TokenToDicts: map[string][]string{}}
			expectedConfiguration.TokenToDicts = map[string][]string{"uint": {"simple"}, "asciiword": {"danish_stem"}}

			Expect(configurations).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&expectedConfiguration, &configurations[0], "Oid")
		})
		It("returns a text search configuration from a specific schema", func() {
			testhelper.AssertQueryRuns(connectionPool, `CREATE TEXT SEARCH CONFIGURATION public.testconfiguration (PARSER = pg_catalog."default");`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TEXT SEARCH CONFIGURATION public.testconfiguration")
			testhelper.AssertQueryRuns(connectionPool, "CREATE SCHEMA testschema")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA testschema")
			testhelper.AssertQueryRuns(connectionPool, `CREATE TEXT SEARCH CONFIGURATION testschema.testconfiguration (PARSER = pg_catalog."default");`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TEXT SEARCH CONFIGURATION testschema.testconfiguration")

			_ = backupCmdFlags.Set(options.INCLUDE_SCHEMA, "testschema")
			configurations := backup.GetTextSearchConfigurations(connectionPool)

			expectedConfiguration := backup.TextSearchConfiguration{Oid: 1, Schema: "testschema", Name: "testconfiguration", Parser: `pg_catalog."default"`, TokenToDicts: map[string][]string{}}

			Expect(configurations).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&expectedConfiguration, &configurations[0], "Oid")
		})
	})
})
