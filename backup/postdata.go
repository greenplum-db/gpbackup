package backup

/*
 * This file contains structs and functions related to backing up "post-data" metadata
 * on the master, which is any metadata that needs to be restored after data is
 * restored, such as indexes and rules.
 */

import (
	"github.com/greenplum-db/gpbackup/utils"
)

func PrintCreateIndexStatements(metadataFile *utils.FileWithByteCount, toc *utils.TOC, indexes []QuerySimpleDefinition, indexMetadata MetadataMap) {
	for _, index := range indexes {
		start := metadataFile.ByteCount
		metadataFile.MustPrintf("\n\n%s;", index.Def)
		if index.Tablespace != "" {
			metadataFile.MustPrintf("\nALTER INDEX %s SET TABLESPACE %s;", index.Name, index.Tablespace)
		}
		PrintObjectMetadata(metadataFile, indexMetadata[index.Oid], index.Name, "INDEX")
		toc.AddPostdataEntry(index.OwningSchema, index.Name, "INDEX", start, metadataFile)
	}
}

func PrintCreateRuleStatements(metadataFile *utils.FileWithByteCount, toc *utils.TOC, rules []QuerySimpleDefinition, ruleMetadata MetadataMap) {
	for _, rule := range rules {
		start := metadataFile.ByteCount
		metadataFile.MustPrintf("\n\n%s", rule.Def)
		tableFQN := utils.MakeFQN(rule.OwningSchema, rule.OwningTable)
		PrintObjectMetadata(metadataFile, ruleMetadata[rule.Oid], rule.Name, "RULE", tableFQN)
		toc.AddPostdataEntry(rule.OwningSchema, rule.Name, "RULE", start, metadataFile)
	}
}

func PrintCreateTriggerStatements(metadataFile *utils.FileWithByteCount, toc *utils.TOC, triggers []QuerySimpleDefinition, triggerMetadata MetadataMap) {
	for _, trigger := range triggers {
		start := metadataFile.ByteCount
		metadataFile.MustPrintf("\n\n%s;", trigger.Def)
		tableFQN := utils.MakeFQN(trigger.OwningSchema, trigger.OwningTable)
		PrintObjectMetadata(metadataFile, triggerMetadata[trigger.Oid], trigger.Name, "TRIGGER", tableFQN)
		toc.AddPostdataEntry(trigger.OwningSchema, trigger.Name, "TRIGGER", start, metadataFile)
	}
}
