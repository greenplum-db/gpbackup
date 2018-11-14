package backup_test

import (
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"

	. "github.com/onsi/ginkgo"
)

var _ = Describe("backup/predata_textsearch tests", func() {
	BeforeEach(func() {
		toc, backupfile = testutils.InitializeTestTOC(buffer, "predata")
	})
	Describe("PrintCreateTextSearchParserStatements", func() {
		It("prints a basic text search parser", func() {
			parser := backup.TextSearchParser{Oid: 0, Schema: "public", Name: "testparser", StartFunc: "start_func", TokenFunc: "token_func", EndFunc: "end_func", LexTypesFunc: "lextypes_func"}
			backup.PrintCreateTextSearchParserStatement(backupfile, toc, parser, backup.ObjectMetadata{})
			testutils.ExpectEntry(toc.PredataEntries, 0, "public", "", "testparser", "TEXT SEARCH PARSER")
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TEXT SEARCH PARSER public.testparser (
	START = start_func,
	GETTOKEN = token_func,
	END = end_func,
	LEXTYPES = lextypes_func
);`)
		})
		It("prints a text search parser with a headline and comment", func() {
			parser := backup.TextSearchParser{Oid: 1, Schema: "public", Name: "testparser", StartFunc: "start_func", TokenFunc: "token_func", EndFunc: "end_func", LexTypesFunc: "lextypes_func", HeadlineFunc: "headline_func"}
			metadata := testutils.DefaultMetadata("TEXT SEARCH PARSER", false, false, true, false)
			backup.PrintCreateTextSearchParserStatement(backupfile, toc, parser, metadata)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TEXT SEARCH PARSER public.testparser (
	START = start_func,
	GETTOKEN = token_func,
	END = end_func,
	LEXTYPES = lextypes_func,
	HEADLINE = headline_func
);`, `COMMENT ON TEXT SEARCH PARSER public.testparser IS 'This is a text search parser comment.';`)
		})
	})
	Describe("PrintCreateTextSearchTemplateStatement", func() {
		It("prints a basic text search template with comment", func() {
			template := backup.TextSearchTemplate{Oid: 1, Schema: "public", Name: "testtemplate", InitFunc: "dsimple_init", LexizeFunc: "dsimple_lexize"}
			metadata := testutils.DefaultMetadata("TEXT SEARCH TEMPLATE", false, false, true, false)
			backup.PrintCreateTextSearchTemplateStatement(backupfile, toc, template, metadata)
			testutils.ExpectEntry(toc.PredataEntries, 0, "public", "", "testtemplate", "TEXT SEARCH TEMPLATE")
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TEXT SEARCH TEMPLATE public.testtemplate (
	INIT = dsimple_init,
	LEXIZE = dsimple_lexize
);`, `COMMENT ON TEXT SEARCH TEMPLATE public.testtemplate IS 'This is a text search template comment.';`)
		})
	})
	Describe("PrintCreateTextSearchDictionaryStatement", func() {
		It("prints a basic text search dictionary with comment", func() {
			dictionary := backup.TextSearchDictionary{Oid: 1, Schema: "public", Name: "testdictionary", Template: "testschema.snowball", InitOption: "language = 'russian', stopwords = 'russian'"}
			metadata := testutils.DefaultMetadata("TEXT SEARCH DICTIONARY", false, true, true, false)
			backup.PrintCreateTextSearchDictionaryStatement(backupfile, toc, dictionary, metadata)
			testutils.ExpectEntry(toc.PredataEntries, 0, "public", "", "testdictionary", "TEXT SEARCH DICTIONARY")
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TEXT SEARCH DICTIONARY public.testdictionary (
	TEMPLATE = testschema.snowball,
	language = 'russian', stopwords = 'russian'
);`, `COMMENT ON TEXT SEARCH DICTIONARY public.testdictionary IS 'This is a text search dictionary comment.';`, `ALTER TEXT SEARCH DICTIONARY public.testdictionary OWNER TO testrole;`)
		})
	})
	Describe("PrintCreateTextSearchConfigurationStatement", func() {
		It("prints a basic text search configuration", func() {
			configurations := backup.TextSearchConfiguration{Oid: 0, Schema: "public", Name: "testconfiguration", Parser: `pg_catalog."default"`, TokenToDicts: map[string][]string{}}
			backup.PrintCreateTextSearchConfigurationStatement(backupfile, toc, configurations, backup.ObjectMetadata{})
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TEXT SEARCH CONFIGURATION public.testconfiguration (
	PARSER = pg_catalog."default"
);`)
		})
		It("prints a text search configuration with multiple mappings and comment", func() {
			tokenToDicts := map[string][]string{"int": {"simple", "english_stem"}, "asciiword": {"english_stem"}}
			configurations := backup.TextSearchConfiguration{Oid: 1, Schema: "public", Name: "testconfiguration", Parser: `pg_catalog."default"`, TokenToDicts: tokenToDicts}
			metadata := testutils.DefaultMetadata("TEXT SEARCH CONFIGURATION", false, true, true, false)
			backup.PrintCreateTextSearchConfigurationStatement(backupfile, toc, configurations, metadata)
			testutils.ExpectEntry(toc.PredataEntries, 0, "public", "", "testconfiguration", "TEXT SEARCH CONFIGURATION")
			expectedStatements := []string{`CREATE TEXT SEARCH CONFIGURATION public.testconfiguration (
	PARSER = pg_catalog."default"
);`, `ALTER TEXT SEARCH CONFIGURATION public.testconfiguration
	ADD MAPPING FOR "asciiword" WITH english_stem;`,
				`ALTER TEXT SEARCH CONFIGURATION public.testconfiguration
	ADD MAPPING FOR "int" WITH simple, english_stem;`,
				`COMMENT ON TEXT SEARCH CONFIGURATION public.testconfiguration IS 'This is a text search configuration comment.';`,
				`ALTER TEXT SEARCH CONFIGURATION public.testconfiguration OWNER TO testrole;`}
			testutils.AssertBufferContents(toc.PredataEntries, buffer, expectedStatements...)
		})
	})
})
