package backup

/*
 * This file contains structs and functions related to backing up "post-data" metadata
 * on the master, which is any metadata that needs to be restored after data is
 * restored, such as indexes and rules.
 */

import (
	"github.com/greenplum-db/gpbackup/toc"
	"github.com/greenplum-db/gpbackup/utils"
)

func PrintCreateIndexStatements(metadataFile *utils.FileWithByteCount, toc *toc.TOC, indexes []IndexDefinition, indexMetadata MetadataMap) {
	for _, index := range indexes {
		start := metadataFile.ByteCount
		if !index.SupportsConstraint {
			section, entry := index.GetMetadataEntry()

			metadataFile.MustPrintf("\n\n%s;", index.Def.String)
			toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)

			// Start INDEX metadata
			indexFQN := utils.MakeFQN(index.OwningSchema, index.Name)
			entry.ReferenceObject = indexFQN
			entry.ObjectType = "INDEX METADATA"
			if index.Tablespace != "" {
				start := metadataFile.ByteCount
				metadataFile.MustPrintf("\nALTER INDEX %s SET TABLESPACE %s;", indexFQN, index.Tablespace)
				toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
			}
			tableFQN := utils.MakeFQN(index.OwningSchema, index.OwningTable)
			if index.IsClustered {
				start := metadataFile.ByteCount
				metadataFile.MustPrintf("\nALTER TABLE %s CLUSTER ON %s;", tableFQN, index.Name)
				toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
			}
			if index.IsReplicaIdentity {
				start := metadataFile.ByteCount
				metadataFile.MustPrintf("\nALTER TABLE %s REPLICA IDENTITY USING INDEX %s;", tableFQN, index.Name)
				toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
			}
		}
		PrintObjectMetadata(metadataFile, toc, indexMetadata[index.GetUniqueID()], index, "")
	}
}

func PrintCreateRuleStatements(metadataFile *utils.FileWithByteCount, toc *toc.TOC, rules []RuleDefinition, ruleMetadata MetadataMap) {
	for _, rule := range rules {
		start := metadataFile.ByteCount
		metadataFile.MustPrintf("\n\n%s", rule.Def.String)

		section, entry := rule.GetMetadataEntry()
		toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
		tableFQN := utils.MakeFQN(rule.OwningSchema, rule.OwningTable)
		PrintObjectMetadata(metadataFile, toc, ruleMetadata[rule.GetUniqueID()], rule, tableFQN)
	}
}

func PrintCreateTriggerStatements(metadataFile *utils.FileWithByteCount, toc *toc.TOC, triggers []TriggerDefinition, triggerMetadata MetadataMap) {
	for _, trigger := range triggers {
		start := metadataFile.ByteCount
		metadataFile.MustPrintf("\n\n%s;", trigger.Def.String)

		section, entry := trigger.GetMetadataEntry()
		toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
		tableFQN := utils.MakeFQN(trigger.OwningSchema, trigger.OwningTable)
		PrintObjectMetadata(metadataFile, toc, triggerMetadata[trigger.GetUniqueID()], trigger, tableFQN)
	}
}

func PrintCreateEventTriggerStatements(metadataFile *utils.FileWithByteCount, toc *toc.TOC, eventTriggers []EventTrigger, eventTriggerMetadata MetadataMap) {
	for _, eventTrigger := range eventTriggers {
		start := metadataFile.ByteCount
		section, entry := eventTrigger.GetMetadataEntry()

		metadataFile.MustPrintf("\n\nCREATE EVENT TRIGGER %s\nON %s", eventTrigger.Name, eventTrigger.Event)
		if eventTrigger.EventTags != "" {
			metadataFile.MustPrintf("\nWHEN TAG IN (%s)", eventTrigger.EventTags)
		}
		metadataFile.MustPrintf("\nEXECUTE PROCEDURE %s();", eventTrigger.FunctionName)
		toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)

		// Start EVENT TRIGGER metadata
		entry.ReferenceObject = eventTrigger.Name
		entry.ObjectType = "EVENT TRIGGER METADATA"
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
			toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
		}
		PrintObjectMetadata(metadataFile, toc, eventTriggerMetadata[eventTrigger.GetUniqueID()], eventTrigger, "")
	}
}
