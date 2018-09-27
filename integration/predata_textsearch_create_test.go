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
		parser := backup.TextSearchParser{Oid: 0, Schema: "public", Name: "testparser", StartFunc: "prsd_start", TokenFunc: "prsd_nexttoken", EndFunc: "prsd_end", LexTypesFunc: "prsd_lextype", HeadlineFunc: "prsd_headline"}
		It("creates a basic text search parser", func() {
			backup.PrintCreateTextSearchParserStatements(backupfile, toc, []backup.TextSearchParser{parser}, backup.MetadataMap{})

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TEXT SEARCH PARSER public.testparser")

			resultParsers := backup.GetTextSearchParsers(connectionPool)

			Expect(resultParsers).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&parser, &resultParsers[0], "Oid")
		})
		It("creates a basic text search parser with a comment", func() {
			parserMetadataMap := testutils.DefaultMetadataMap("TEXT SEARCH PARSER", false, false, true)
			parserMetadata := parserMetadataMap[parser.GetUniqueID()]

			backup.PrintCreateTextSearchParserStatements(backupfile, toc, []backup.TextSearchParser{parser}, parserMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TEXT SEARCH PARSER public.testparser")

			resultParsers := backup.GetTextSearchParsers(connectionPool)
			resultMetadataMap := backup.GetCommentsForObjectType(connectionPool, backup.TYPE_TSPARSER)

			Expect(resultParsers).To(HaveLen(1))
			uniqueID := testutils.UniqueIDFromObjectName(connectionPool, "public", "testparser", backup.TYPE_TSPARSER)
			resultMetadata := resultMetadataMap[uniqueID]
			structmatcher.ExpectStructsToMatchExcluding(&parser, &resultParsers[0], "Oid")
			structmatcher.ExpectStructsToMatch(&parserMetadata, &resultMetadata)
		})
	})
	Describe("PrintCreateTextSearchTemplateStatements", func() {
		template := backup.TextSearchTemplate{Oid: 1, Schema: "public", Name: "testtemplate", InitFunc: "dsimple_init", LexizeFunc: "dsimple_lexize"}
		It("creates a basic text search template", func() {
			backup.PrintCreateTextSearchTemplateStatements(backupfile, toc, []backup.TextSearchTemplate{template}, backup.MetadataMap{})

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TEXT SEARCH TEMPLATE public.testtemplate")

			resultTemplates := backup.GetTextSearchTemplates(connectionPool)

			Expect(resultTemplates).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&template, &resultTemplates[0], "Oid")
		})
		It("creates a basic text search template with a comment", func() {
			templateMetadataMap := testutils.DefaultMetadataMap("TEXT SEARCH TEMPLATE", false, false, true)
			templateMetadata := templateMetadataMap[template.GetUniqueID()]

			backup.PrintCreateTextSearchTemplateStatements(backupfile, toc, []backup.TextSearchTemplate{template}, templateMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TEXT SEARCH TEMPLATE public.testtemplate")

			resultTemplates := backup.GetTextSearchTemplates(connectionPool)
			resultMetadataMap := backup.GetCommentsForObjectType(connectionPool, backup.TYPE_TSTEMPLATE)

			Expect(resultTemplates).To(HaveLen(1))
			uniqueID := testutils.UniqueIDFromObjectName(connectionPool, "public", "testtemplate", backup.TYPE_TSTEMPLATE)
			resultMetadata := resultMetadataMap[uniqueID]
			structmatcher.ExpectStructsToMatchExcluding(&template, &resultTemplates[0], "Oid")
			structmatcher.ExpectStructsToMatch(&templateMetadata, &resultMetadata)
		})
	})
	Describe("PrintCreateTextSearchDictionaryStatements", func() {
		dictionary := backup.TextSearchDictionary{Oid: 1, Schema: "public", Name: "testdictionary", Template: "pg_catalog.snowball", InitOption: "language = 'russian', stopwords = 'russian'"}
		It("creates a basic text search dictionary", func() {

			backup.PrintCreateTextSearchDictionaryStatements(backupfile, toc, []backup.TextSearchDictionary{dictionary}, backup.MetadataMap{})

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TEXT SEARCH DICTIONARY public.testdictionary")

			resultDictionaries := backup.GetTextSearchDictionaries(connectionPool)

			Expect(resultDictionaries).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&dictionary, &resultDictionaries[0], "Oid")
		})
		It("creates a basic text search dictionary with a comment and owner", func() {
			dictionaryMetadataMap := testutils.DefaultMetadataMap("TEXT SEARCH DICTIONARY", false, true, true)
			dictionaryMetadata := dictionaryMetadataMap[dictionary.GetUniqueID()]

			backup.PrintCreateTextSearchDictionaryStatements(backupfile, toc, []backup.TextSearchDictionary{dictionary}, dictionaryMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TEXT SEARCH DICTIONARY public.testdictionary")

			resultDictionaries := backup.GetTextSearchDictionaries(connectionPool)
			resultMetadataMap := backup.GetMetadataForObjectType(connectionPool, backup.TYPE_TSDICTIONARY)

			Expect(resultDictionaries).To(HaveLen(1))
			uniqueID := testutils.UniqueIDFromObjectName(connectionPool, "public", "testdictionary", backup.TYPE_TSDICTIONARY)
			resultMetadata := resultMetadataMap[uniqueID]
			structmatcher.ExpectStructsToMatchExcluding(&dictionary, &resultDictionaries[0], "Oid")
			structmatcher.ExpectStructsToMatch(&dictionaryMetadata, &resultMetadata)
		})
	})
	Describe("PrintCreateTextSearchConfigurationStatements", func() {
		configuration := backup.TextSearchConfiguration{Oid: 1, Schema: "public", Name: "testconfiguration", Parser: `pg_catalog."default"`, TokenToDicts: map[string][]string{}}
		It("creates a basic text search configuration", func() {
			backup.PrintCreateTextSearchConfigurationStatements(backupfile, toc, []backup.TextSearchConfiguration{configuration}, backup.MetadataMap{})

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TEXT SEARCH CONFIGURATION public.testconfiguration")

			resultConfigurations := backup.GetTextSearchConfigurations(connectionPool)

			Expect(resultConfigurations).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&configuration, &resultConfigurations[0], "Oid")
		})
		It("creates a basic text search configuration with a comment and owner", func() {
			configurationMetadataMap := testutils.DefaultMetadataMap("TEXT SEARCH CONFIGURATION", false, true, true)
			configurationMetadata := configurationMetadataMap[configuration.GetUniqueID()]

			backup.PrintCreateTextSearchConfigurationStatements(backupfile, toc, []backup.TextSearchConfiguration{configuration}, configurationMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TEXT SEARCH CONFIGURATION public.testconfiguration")

			resultConfigurations := backup.GetTextSearchConfigurations(connectionPool)
			resultMetadataMap := backup.GetMetadataForObjectType(connectionPool, backup.TYPE_TSCONFIGURATION)

			Expect(resultConfigurations).To(HaveLen(1))
			uniqueID := testutils.UniqueIDFromObjectName(connectionPool, "public", "testconfiguration", backup.TYPE_TSCONFIGURATION)
			resultMetadata := resultMetadataMap[uniqueID]
			structmatcher.ExpectStructsToMatchExcluding(&configuration, &resultConfigurations[0], "Oid")
			structmatcher.ExpectStructsToMatch(&configurationMetadata, &resultMetadata)
		})
	})
})
