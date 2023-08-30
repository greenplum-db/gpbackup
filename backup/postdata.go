package backup

/*
 * This file contains structs and functions related to backing up "post-data" metadata
 * on the coordinator, which is any metadata that needs to be restored after data is
 * restored, such as indexes and rules.
 */

import (
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpbackup/toc"
	"github.com/greenplum-db/gpbackup/utils"
	"github.com/pkg/errors"
)

func PrintCreateIndexStatements(metadataFile *utils.FileWithByteCount, objToc *toc.TOC, indexes []IndexDefinition, indexMetadata MetadataMap) {
	for _, index := range indexes {
		start := metadataFile.ByteCount
		if !index.SupportsConstraint {
			section, entry := index.GetMetadataEntry()

			metadataFile.MustPrintf("\n\n%s;", index.Def.String)
			objToc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount, []uint32{0, 0})

			// Start INDEX metadata
			entry.ReferenceObject = index.FQN()
			entry.ObjectType = "INDEX METADATA"
			if index.Tablespace != "" {
				start := metadataFile.ByteCount
				metadataFile.MustPrintf("\nALTER INDEX %s SET TABLESPACE %s;", index.FQN(), index.Tablespace)
				objToc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount, []uint32{0, 0})
			}
			if index.ParentIndexFQN != "" && connectionPool.Version.AtLeast("7") {
				start := metadataFile.ByteCount
				metadataFile.MustPrintf("\nALTER INDEX %s ATTACH PARTITION %s;", index.ParentIndexFQN, index.FQN())
				objToc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount, []uint32{0, 0})
			}
			tableFQN := utils.MakeFQN(index.OwningSchema, index.OwningTable)
			if index.IsClustered {
				start := metadataFile.ByteCount
				metadataFile.MustPrintf("\nALTER TABLE %s CLUSTER ON %s;", tableFQN, index.Name)
				objToc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount, []uint32{0, 0})
			}
			if index.IsReplicaIdentity {
				start := metadataFile.ByteCount
				metadataFile.MustPrintf("\nALTER TABLE %s REPLICA IDENTITY USING INDEX %s;", tableFQN, index.Name)
				objToc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount, []uint32{0, 0})
			}
			if index.StatisticsColumns != "" && index.StatisticsValues != "" {
				cols := strings.Split(index.StatisticsColumns, ",")
				vals := strings.Split(index.StatisticsValues, ",")
				if len(cols) != len(vals) {
					gplog.Fatal(errors.Errorf("Index StatisticsColumns(%d) and StatisticsValues(%d) count don't match\n", len(cols), len(vals)), "")
				}
				for i := 0; i < len(cols); i++ {
					start := metadataFile.ByteCount
					metadataFile.MustPrintf("\nALTER INDEX %s ALTER COLUMN %s SET STATISTICS %s;", index.FQN(), cols[i], vals[i])
					objToc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount, []uint32{0, 0})
				}
			}
		}
		PrintObjectMetadata(metadataFile, objToc, indexMetadata[index.GetUniqueID()], index, "", []uint32{0, 0})
	}
}

func PrintCreateRuleStatements(metadataFile *utils.FileWithByteCount, objToc *toc.TOC, rules []RuleDefinition, ruleMetadata MetadataMap) {
	for _, rule := range rules {
		start := metadataFile.ByteCount
		metadataFile.MustPrintf("\n\n%s", rule.Def.String)

		section, entry := rule.GetMetadataEntry()
		objToc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount, []uint32{0, 0})
		tableFQN := utils.MakeFQN(rule.OwningSchema, rule.OwningTable)
		PrintObjectMetadata(metadataFile, objToc, ruleMetadata[rule.GetUniqueID()], rule, tableFQN, []uint32{0, 0})
	}
}

func PrintCreateTriggerStatements(metadataFile *utils.FileWithByteCount, objToc *toc.TOC, triggers []TriggerDefinition, triggerMetadata MetadataMap) {
	for _, trigger := range triggers {
		start := metadataFile.ByteCount
		metadataFile.MustPrintf("\n\n%s;", trigger.Def.String)

		section, entry := trigger.GetMetadataEntry()
		objToc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount, []uint32{0, 0})
		tableFQN := utils.MakeFQN(trigger.OwningSchema, trigger.OwningTable)
		PrintObjectMetadata(metadataFile, objToc, triggerMetadata[trigger.GetUniqueID()], trigger, tableFQN, []uint32{0, 0})
	}
}

func PrintCreateEventTriggerStatements(metadataFile *utils.FileWithByteCount, objToc *toc.TOC, eventTriggers []EventTrigger, eventTriggerMetadata MetadataMap) {
	for _, eventTrigger := range eventTriggers {
		start := metadataFile.ByteCount
		section, entry := eventTrigger.GetMetadataEntry()

		metadataFile.MustPrintf("\n\nCREATE EVENT TRIGGER %s\nON %s", eventTrigger.Name, eventTrigger.Event)
		if eventTrigger.EventTags != "" {
			metadataFile.MustPrintf("\nWHEN TAG IN (%s)", eventTrigger.EventTags)
		}
		if connectionPool.Version.AtLeast("7") {
			metadataFile.MustPrintf("\nEXECUTE FUNCTION %s();", eventTrigger.FunctionName)
		} else {
			metadataFile.MustPrintf("\nEXECUTE PROCEDURE %s();", eventTrigger.FunctionName)
		}
		objToc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount, []uint32{0, 0})

		// Start EVENT TRIGGER metadata
		entry.ReferenceObject = eventTrigger.Name
		entry.ObjectType = toc.OBJ_EVENT_TRIGGER + " METADATA"
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
			objToc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount, []uint32{0, 0})
		}
		PrintObjectMetadata(metadataFile, objToc, eventTriggerMetadata[eventTrigger.GetUniqueID()], eventTrigger, "", []uint32{0, 0})
	}
}

func PrintCreatePolicyStatements(metadataFile *utils.FileWithByteCount, objToc *toc.TOC, policies []RLSPolicy, policyMetadata MetadataMap) {
	for _, policy := range policies {
		start := metadataFile.ByteCount
		section, entry := policy.GetMetadataEntry()

		tableFQN := utils.MakeFQN(policy.Schema, policy.Table)
		metadataFile.MustPrintf("\n\nALTER TABLE %s ENABLE ROW LEVEL SECURITY;", tableFQN)
		objToc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount, []uint32{0, 0})

		permissiveOption := ""
		if policy.Permissive == "false" {
			permissiveOption = " AS RESTRICTIVE"
		}
		cmdOption := ""
		if policy.Cmd != "" {
			switch policy.Cmd {
			case "*":
				cmdOption = ""
			case "r":
				cmdOption = " FOR SELECT"
			case "a":
				cmdOption = " FOR INSERT"
			case "w":
				cmdOption = " FOR UPDATE"
			case "d":
				cmdOption = " FOR DELETE"
			default:
				gplog.Fatal(errors.Errorf("Unexpected policy command: expected '*|r|a|w|d' got '%s'\n", policy.Cmd), "")
			}
		}
		start = metadataFile.ByteCount
		metadataFile.MustPrintf("\nCREATE POLICY %s\nON %s%s%s", policy.Name, tableFQN, permissiveOption, cmdOption)

		if policy.Roles != "" {
			metadataFile.MustPrintf("\n TO %s", policy.Roles)
		}
		if policy.Qual != "" {
			metadataFile.MustPrintf("\n USING (%s)", policy.Qual)
		}
		if policy.WithCheck != "" {
			metadataFile.MustPrintf("\n WITH CHECK (%s)", policy.WithCheck)
		}
		metadataFile.MustPrintf(";\n")
		objToc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount, []uint32{0, 0})
		PrintObjectMetadata(metadataFile, objToc, policyMetadata[policy.GetUniqueID()], policy, "", []uint32{0, 0})
	}
}

func PrintCreateExtendedStatistics(metadataFile *utils.FileWithByteCount, objToc *toc.TOC, statExtObjects []StatisticExt, statMetadata MetadataMap) {
	for _, stat := range statExtObjects {
		start := metadataFile.ByteCount
		metadataFile.MustPrintln()

		metadataFile.MustPrintf("\n%s;", stat.Definition)
		section, entry := stat.GetMetadataEntry()
		objToc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount, []uint32{0, 0})
		PrintObjectMetadata(metadataFile, objToc, statMetadata[stat.GetUniqueID()], stat, "", []uint32{0, 0})
	}
}
