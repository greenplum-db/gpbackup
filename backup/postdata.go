package backup

/*
 * This file contains structs and functions related to backing up "post-data" metadata
 * on the master, which is any metadata that needs to be restored after data is
 * restored, such as indexes and rules.
 */

import (
	"fmt"

	"github.com/greenplum-db/gpbackup/utils"
)

func PrintStatements(metadataFile *utils.FileWithByteCount, toc *utils.TOC, obj utils.TOCObject, statements []string) {
	for _, statement := range statements {
		start := metadataFile.ByteCount
		metadataFile.MustPrintf("\n\n%s", statement)
		toc.NewAddMetadataEntry(obj, start, metadataFile.ByteCount)
	}
}

func PrintCreateIndexStatements(metadataFile *utils.FileWithByteCount, toc *utils.TOC, indexes []IndexDefinition, indexMetadata MetadataMap) {
	for _, index := range indexes {
		statements := []string{}
		statements = append(statements, fmt.Sprintf("%s;", index.Def))
		indexFQN := utils.MakeFQN(index.OwningSchema, index.Name)
		if index.Tablespace != "" {
			statements = append(statements, fmt.Sprintf("ALTER INDEX %s SET TABLESPACE %s;", indexFQN, index.Tablespace))
		}
		tableFQN := utils.MakeFQN(index.OwningSchema, index.OwningTable)
		if index.IsClustered {
			statements = append(statements, fmt.Sprintf("ALTER TABLE %s CLUSTER ON %s;", tableFQN, index.Name))
		}

		PrintStatements(metadataFile, toc, index, statements)
		NewPrintObjectMetadata(metadataFile, toc, indexMetadata[index.GetUniqueID()], index, indexFQN)
	}
}

func PrintCreateRuleStatements(metadataFile *utils.FileWithByteCount, toc *utils.TOC, rules []RuleDefinition, ruleMetadata MetadataMap) {
	for _, rule := range rules {
		statements := []string{fmt.Sprintf("%s", rule.Def)}
		PrintStatements(metadataFile, toc, rule, statements)
		tableFQN := utils.MakeFQN(rule.OwningSchema, rule.OwningTable)
		NewPrintObjectMetadata(metadataFile, toc, ruleMetadata[rule.GetUniqueID()], rule, rule.Name, tableFQN)
	}
}

func PrintCreateTriggerStatements(metadataFile *utils.FileWithByteCount, toc *utils.TOC, triggers []TriggerDefinition, triggerMetadata MetadataMap) {
	for _, trigger := range triggers {
		statements := []string{fmt.Sprintf("%s;", trigger.Def)}
		PrintStatements(metadataFile, toc, trigger, statements)
		tableFQN := utils.MakeFQN(trigger.OwningSchema, trigger.OwningTable)
		NewPrintObjectMetadata(metadataFile, toc, triggerMetadata[trigger.GetUniqueID()], trigger, trigger.Name, tableFQN)
	}
}

func PrintCreateEventTriggerStatements(metadataFile *utils.FileWithByteCount, toc *utils.TOC, eventTriggers []EventTrigger, eventTriggerMetadata MetadataMap) {
	for _, eventTrigger := range eventTriggers {
		statements := []string{}
		currStatement := fmt.Sprintf("CREATE EVENT TRIGGER %s\nON %s", eventTrigger.Name, eventTrigger.Event)
		if eventTrigger.EventTags != "" {
			currStatement += fmt.Sprintf("\nWHEN TAG IN (%s)", eventTrigger.EventTags)
		}
		currStatement += fmt.Sprintf("\nEXECUTE PROCEDURE %s();", eventTrigger.FunctionName)
		statements = append(statements, currStatement)
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
			statements = append(statements, fmt.Sprintf("ALTER EVENT TRIGGER %s %s;", eventTrigger.Name, enableOption))
		}
		PrintStatements(metadataFile, toc, eventTrigger, statements)
		NewPrintObjectMetadata(metadataFile, toc, eventTriggerMetadata[eventTrigger.GetUniqueID()], eventTrigger, eventTrigger.Name)
	}
}
