package integration

import (
	"github.com/greenplum-db/gp-common-go-libs/structmatcher"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup integration create statement tests", func() {
	BeforeEach(func() {
		testutils.SkipIfBefore5(connectionPool)
		toc, backupfile = testutils.InitializeTestTOC(buffer, "predata")
	})
	Describe("PrintCreateTextSearchParserStatements", func() {
		It("creates a basic text search parser", func() {
			parsers := []backup.TextSearchParser{{Oid: 0, Schema: "public", Name: "testparser", StartFunc: "prsd_start", TokenFunc: "prsd_nexttoken", EndFunc: "prsd_end", LexTypesFunc: "prsd_lextype", HeadlineFunc: "prsd_headline"}}
			backup.PrintCreateTextSearchParserStatements(backupfile, toc, parsers, backup.MetadataMap{})

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TEXT SEARCH PARSER public.testparser")

			resultParsers := backup.GetTextSearchParsers(connectionPool)

			Expect(resultParsers).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&parsers[0], &resultParsers[0], "Oid")
		})
		It("creates a basic text search parser with a comment", func() {
			parsers := []backup.TextSearchParser{{Oid: 1, Schema: "public", Name: "testparser", StartFunc: "prsd_start", TokenFunc: "prsd_nexttoken", EndFunc: "prsd_end", LexTypesFunc: "prsd_lextype", HeadlineFunc: "prsd_headline"}}
			parserMetadataMap := testutils.DefaultMetadataMap("TEXT SEARCH PARSER", false, false, true)
			parserMetadata := parserMetadataMap[parsers[0].GetUniqueID()]

			backup.PrintCreateTextSearchParserStatements(backupfile, toc, parsers, parserMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TEXT SEARCH PARSER public.testparser")

			resultParsers := backup.GetTextSearchParsers(connectionPool)
			resultMetadataMap := backup.GetCommentsForObjectType(connectionPool, backup.TYPE_TSPARSER)

			Expect(resultParsers).To(HaveLen(1))
			resultMetadata := resultMetadataMap[resultParsers[0].GetUniqueID()]
			structmatcher.ExpectStructsToMatchExcluding(&parsers[0], &resultParsers[0], "Oid")
			structmatcher.ExpectStructsToMatch(&parserMetadata, &resultMetadata)
		})
	})
	Describe("PrintCreateTextSearchTemplateStatements", func() {
		It("creates a basic text search template", func() {
			templates := []backup.TextSearchTemplate{{Oid: 0, Schema: "public", Name: "testtemplate", InitFunc: "dsimple_init", LexizeFunc: "dsimple_lexize"}}
			backup.PrintCreateTextSearchTemplateStatements(backupfile, toc, templates, backup.MetadataMap{})

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TEXT SEARCH TEMPLATE public.testtemplate")

			resultTemplates := backup.GetTextSearchTemplates(connectionPool)

			Expect(resultTemplates).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&templates[0], &resultTemplates[0], "Oid")
		})
		It("creates a basic text search template with a comment", func() {
			templates := []backup.TextSearchTemplate{{Oid: 1, Schema: "public", Name: "testtemplate", InitFunc: "dsimple_init", LexizeFunc: "dsimple_lexize"}}
			templateMetadataMap := testutils.DefaultMetadataMap("TEXT SEARCH TEMPLATE", false, false, true)
			templateMetadata := templateMetadataMap[backup.UniqueID{Oid: 1}]

			backup.PrintCreateTextSearchTemplateStatements(backupfile, toc, templates, templateMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TEXT SEARCH TEMPLATE public.testtemplate")

			resultTemplates := backup.GetTextSearchTemplates(connectionPool)
			resultMetadataMap := backup.GetCommentsForObjectType(connectionPool, backup.TYPE_TSTEMPLATE)

			Expect(resultTemplates).To(HaveLen(1))
			resultMetadata := resultMetadataMap[templates[0].GetUniqueID()]
			structmatcher.ExpectStructsToMatchExcluding(&templates[0], &resultTemplates[0], "Oid")
			structmatcher.ExpectStructsToMatch(&templateMetadata, &resultMetadata)
		})
	})
	Describe("PrintCreateTextSearchDictionaryStatements", func() {
		It("creates a basic text search dictionary", func() {
			dictionaries := []backup.TextSearchDictionary{{Oid: 0, Schema: "public", Name: "testdictionary", Template: "pg_catalog.snowball", InitOption: "language = 'russian', stopwords = 'russian'"}}

			backup.PrintCreateTextSearchDictionaryStatements(backupfile, toc, dictionaries, backup.MetadataMap{})

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TEXT SEARCH DICTIONARY public.testdictionary")

			resultDictionaries := backup.GetTextSearchDictionaries(connectionPool)

			Expect(resultDictionaries).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&dictionaries[0], &resultDictionaries[0], "Oid")
		})
		It("creates a basic text search dictionary with a comment and owner", func() {
			dictionaries := []backup.TextSearchDictionary{{Oid: 1, Schema: "public", Name: "testdictionary", Template: "pg_catalog.snowball", InitOption: "language = 'russian', stopwords = 'russian'"}}
			dictionaryMetadataMap := testutils.DefaultMetadataMap("TEXT SEARCH DICTIONARY", false, true, true)
			dictionaryMetadata := dictionaryMetadataMap[backup.UniqueID{Oid: 1}]

			backup.PrintCreateTextSearchDictionaryStatements(backupfile, toc, dictionaries, dictionaryMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TEXT SEARCH DICTIONARY public.testdictionary")

			resultDictionaries := backup.GetTextSearchDictionaries(connectionPool)
			resultMetadataMap := backup.GetMetadataForObjectType(connectionPool, backup.TYPE_TSDICTIONARY)

			Expect(resultDictionaries).To(HaveLen(1))
			resultMetadata := resultMetadataMap[dictionaries[0].GetUniqueID()]
			structmatcher.ExpectStructsToMatchExcluding(&dictionaries[0], &resultDictionaries[0], "Oid")
			structmatcher.ExpectStructsToMatch(&dictionaryMetadata, &resultMetadata)
		})
	})
	Describe("PrintCreateTextSearchConfigurationStatements", func() {
		It("creates a basic text search configuration", func() {
			configurations := []backup.TextSearchConfiguration{{Oid: 0, Schema: "public", Name: "testconfiguration", Parser: `pg_catalog."default"`, TokenToDicts: map[string][]string{}}}

			backup.PrintCreateTextSearchConfigurationStatements(backupfile, toc, configurations, backup.MetadataMap{})

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TEXT SEARCH CONFIGURATION public.testconfiguration")

			resultConfigurations := backup.GetTextSearchConfigurations(connectionPool)

			Expect(resultConfigurations).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&configurations[0], &resultConfigurations[0], "Oid")
		})
		It("creates a basic text search configuration with a comment and owner", func() {
			configurations := []backup.TextSearchConfiguration{{Oid: 1, Schema: "public", Name: "testconfiguration", Parser: `pg_catalog."default"`, TokenToDicts: map[string][]string{}}}
			configurationMetadataMap := testutils.DefaultMetadataMap("TEXT SEARCH CONFIGURATION", false, true, true)
			configurationMetadata := configurationMetadataMap[backup.UniqueID{ClassID: backup.PG_TS_CONFIG_OID, Oid: 1}]

			backup.PrintCreateTextSearchConfigurationStatements(backupfile, toc, configurations, configurationMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TEXT SEARCH CONFIGURATION public.testconfiguration")

			resultConfigurations := backup.GetTextSearchConfigurations(connectionPool)
			resultMetadataMap := backup.GetMetadataForObjectType(connectionPool, backup.TYPE_TSCONFIGURATION)

			Expect(resultConfigurations).To(HaveLen(1))
			resultMetadata := resultMetadataMap[resultConfigurations[0].GetUniqueID()]
			structmatcher.ExpectStructsToMatchExcluding(&configurations[0], &resultConfigurations[0], "Oid")
			structmatcher.ExpectStructsToMatch(&configurationMetadata, &resultMetadata)
		})
	})
})
