package backup

/*
 * This file contains structs and functions related to backing up metadata on the
 * coordinator for objects relating to built-in text search that needs to be restored
 * before data is restored.
 *
 * Text search is not supported in GPDB 4.3, so none of these structs or functions
 * are used in a 4.3 backup.
 */

import (
	"sort"
	"strings"

	"github.com/greenplum-db/gpbackup/toc"
	"github.com/greenplum-db/gpbackup/utils"
)

func PrintCreateTextSearchParserStatement(metadataFile *utils.FileWithByteCount, objToc *toc.TOC, parser TextSearchParser, parserMetadata ObjectMetadata) {
	start := metadataFile.ByteCount
	metadataFile.MustPrintf("\n\nCREATE TEXT SEARCH PARSER %s (", parser.FQN())
	metadataFile.MustPrintf("\n\tSTART = %s,", parser.StartFunc)
	metadataFile.MustPrintf("\n\tGETTOKEN = %s,", parser.TokenFunc)
	metadataFile.MustPrintf("\n\tEND = %s,", parser.EndFunc)
	metadataFile.MustPrintf("\n\tLEXTYPES = %s", parser.LexTypesFunc)
	if parser.HeadlineFunc != "" {
		metadataFile.MustPrintf(",\n\tHEADLINE = %s", parser.HeadlineFunc)
	}
	metadataFile.MustPrintf("\n);")

	section, entry := parser.GetMetadataEntry()
	tier := globalTierMap[parser.GetUniqueID()]
	objToc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount, tier)
	PrintObjectMetadata(metadataFile, objToc, parserMetadata, parser, "", tier)
}

func PrintCreateTextSearchTemplateStatement(metadataFile *utils.FileWithByteCount, objToc *toc.TOC, template TextSearchTemplate, templateMetadata ObjectMetadata) {
	start := metadataFile.ByteCount
	metadataFile.MustPrintf("\n\nCREATE TEXT SEARCH TEMPLATE %s (", template.FQN())
	if template.InitFunc != "" {
		metadataFile.MustPrintf("\n\tINIT = %s,", template.InitFunc)
	}
	metadataFile.MustPrintf("\n\tLEXIZE = %s", template.LexizeFunc)
	metadataFile.MustPrintf("\n);")

	section, entry := template.GetMetadataEntry()
	tier := globalTierMap[template.GetUniqueID()]
	objToc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount, tier)
	PrintObjectMetadata(metadataFile, objToc, templateMetadata, template, "", tier)
}

func PrintCreateTextSearchDictionaryStatement(metadataFile *utils.FileWithByteCount, objToc *toc.TOC, dictionary TextSearchDictionary, dictionaryMetadata ObjectMetadata) {
	start := metadataFile.ByteCount
	metadataFile.MustPrintf("\n\nCREATE TEXT SEARCH DICTIONARY %s (", dictionary.FQN())
	metadataFile.MustPrintf("\n\tTEMPLATE = %s", dictionary.Template)
	if dictionary.InitOption != "" {
		metadataFile.MustPrintf(",\n\t%s", dictionary.InitOption)
	}
	metadataFile.MustPrintf("\n);")

	section, entry := dictionary.GetMetadataEntry()
	tier := globalTierMap[dictionary.GetUniqueID()]
	objToc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount, tier)
	PrintObjectMetadata(metadataFile, objToc, dictionaryMetadata, dictionary, "", tier)
}

func PrintCreateTextSearchConfigurationStatement(metadataFile *utils.FileWithByteCount, objToc *toc.TOC, configuration TextSearchConfiguration, configurationMetadata ObjectMetadata) {
	start := metadataFile.ByteCount
	metadataFile.MustPrintf("\n\nCREATE TEXT SEARCH CONFIGURATION %s (", configuration.FQN())
	metadataFile.MustPrintf("\n\tPARSER = %s", configuration.Parser)
	metadataFile.MustPrintf("\n);")

	section, entry := configuration.GetMetadataEntry()
	tier := globalTierMap[configuration.GetUniqueID()]
	objToc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount, tier)

	tokens := make([]string, 0)
	for token := range configuration.TokenToDicts {
		tokens = append(tokens, token)
	}
	sort.Strings(tokens)
	for _, token := range tokens {
		start := metadataFile.ByteCount
		dicts := configuration.TokenToDicts[token]
		metadataFile.MustPrintf("\n\nALTER TEXT SEARCH CONFIGURATION %s", configuration.FQN())
		metadataFile.MustPrintf("\n\tADD MAPPING FOR \"%s\" WITH %s;", token, strings.Join(dicts, ", "))
		objToc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount, tier)
	}
	PrintObjectMetadata(metadataFile, objToc, configurationMetadata, configuration, "", tier)
}
