package backup

/*
 * This file contains structs and functions related to backing up "post-data" metadata
 * on the master, which is any metadata that needs to be restored after data is
 * restored, such as indexes and rules.
 */

import (
	"github.com/greenplum-db/gpbackup/utils"
)

func PrintCreateIndexStatements(metadataFile *utils.FileWithByteCount, toc *utils.TOC, indexes []IndexDefinition, indexMetadata MetadataMap) {
	for _, index := range indexes {
		start := metadataFile.ByteCount
		metadataFile.MustPrintf("\n\n%s;", index.Def)
		toc.AddMetadataEntry(index, start, metadataFile.ByteCount)
		indexFQN := utils.MakeFQN(index.OwningSchema, index.Name)
		if index.Tablespace != "" {
			start := metadataFile.ByteCount
			metadataFile.MustPrintf("\nALTER INDEX %s SET TABLESPACE %s;", indexFQN, index.Tablespace)
			toc.AddMetadataEntry(index, start, metadataFile.ByteCount)
		}
		tableFQN := utils.MakeFQN(index.OwningSchema, index.OwningTable)
		if index.IsClustered {
			start := metadataFile.ByteCount
			metadataFile.MustPrintf("\nALTER TABLE %s CLUSTER ON %s;", tableFQN, index.Name)
			toc.AddMetadataEntry(index, start, metadataFile.ByteCount)
		}
		PrintObjectMetadata(metadataFile, toc, indexMetadata[index.GetUniqueID()], index, "")
	}
}

func PrintCreateRuleStatements(metadataFile *utils.FileWithByteCount, toc *utils.TOC, rules []RuleDefinition, ruleMetadata MetadataMap) {
	for _, rule := range rules {
		start := metadataFile.ByteCount
		metadataFile.MustPrintf("\n\n%s", rule.Def)
		toc.AddMetadataEntry(rule, start, metadataFile.ByteCount)

		tableFQN := utils.MakeFQN(rule.OwningSchema, rule.OwningTable)
		PrintObjectMetadata(metadataFile, toc, ruleMetadata[rule.GetUniqueID()], rule, tableFQN)
	}
}

func PrintCreateTriggerStatements(metadataFile *utils.FileWithByteCount, toc *utils.TOC, triggers []TriggerDefinition, triggerMetadata MetadataMap) {
	for _, trigger := range triggers {
		start := metadataFile.ByteCount
		metadataFile.MustPrintf("\n\n%s;", trigger.Def)
		toc.AddMetadataEntry(trigger, start, metadataFile.ByteCount)

		tableFQN := utils.MakeFQN(trigger.OwningSchema, trigger.OwningTable)
		PrintObjectMetadata(metadataFile, toc, triggerMetadata[trigger.GetUniqueID()], trigger, tableFQN)
	}
}

func PrintCreateEventTriggerStatements(metadataFile *utils.FileWithByteCount, toc *utils.TOC, eventTriggers []EventTrigger, eventTriggerMetadata MetadataMap) {
	for _, eventTrigger := range eventTriggers {
		start := metadataFile.ByteCount
		metadataFile.MustPrintf("\n\nCREATE EVENT TRIGGER %s\nON %s", eventTrigger.Name, eventTrigger.Event)
		if eventTrigger.EventTags != "" {
			metadataFile.MustPrintf("\nWHEN TAG IN (%s)", eventTrigger.EventTags)
		}
		metadataFile.MustPrintf("\nEXECUTE PROCEDURE %s();", eventTrigger.FunctionName)
		toc.AddMetadataEntry(eventTrigger, start, metadataFile.ByteCount)
		if eventTrigger.Enabled != "O" {
			var enableOption string
			switch eventTrigger.Enabled {
			case "D":
				enableOption = "DISABLE"
			case "A":
				enableOption = "ENABLE ALWAYS"
			case "R":
				enableOption = "ENABLE REPLICA"
			default:
				enableOption = "ENABLE"
			}
			start := metadataFile.ByteCount
			metadataFile.MustPrintf("\nALTER EVENT TRIGGER %s %s;", eventTrigger.Name, enableOption)
			toc.AddMetadataEntry(eventTrigger, start, metadataFile.ByteCount)
		}
		PrintObjectMetadata(metadataFile, toc, eventTriggerMetadata[eventTrigger.GetUniqueID()], eventTrigger, "")
	}
}
