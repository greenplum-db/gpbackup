package backup

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpbackup/utils"
)

/*
 * This file contains structs and functions related to backing up global cluster
 * metadata on the master that needs to be restored before data is restored,
 * such as roles and database configuration.
 */

func PrintSessionGUCs(metadataFile *utils.FileWithByteCount, toc *utils.TOC, gucs SessionGUCs) {
	start := metadataFile.ByteCount
	metadataFile.MustPrintf(`
SET client_encoding = '%s';
`, gucs.ClientEncoding)
	toc.AddGlobalEntry("", "", "SESSION GUCS", start, metadataFile)
}

func PrintCreateDatabaseStatement(metadataFile *utils.FileWithByteCount, toc *utils.TOC, db Database, dbMetadata MetadataMap) {
	dbname := db.Name
	start := metadataFile.ByteCount
	metadataFile.MustPrintf("\n\nCREATE DATABASE %s TEMPLATE template0", dbname)
	if db.Tablespace != "pg_default" {
		metadataFile.MustPrintf(" TABLESPACE %s", db.Tablespace)
	}
	if db.Encoding != "" {
		metadataFile.MustPrintf(" ENCODING '%s'", db.Encoding)
	}
	if db.Collate != "" {
		metadataFile.MustPrintf(" LC_COLLATE '%s'", db.Collate)
	}
	if db.CType != "" {
		metadataFile.MustPrintf(" LC_CTYPE '%s'", db.CType)
	}
	metadataFile.MustPrintf(";")
	toc.AddGlobalEntry("", dbname, "DATABASE", start, metadataFile)
	start = metadataFile.ByteCount
	PrintObjectMetadata(metadataFile, dbMetadata[db.GetUniqueID()], dbname, "DATABASE")
	if metadataFile.ByteCount > start {
		toc.AddGlobalEntry("", dbname, "DATABASE METADATA", start, metadataFile)
	}
}

func PrintDatabaseGUCs(metadataFile *utils.FileWithByteCount, toc *utils.TOC, gucs []string, dbname string) {
	for _, guc := range gucs {
		start := metadataFile.ByteCount
		metadataFile.MustPrintf("\nALTER DATABASE %s %s;", dbname, guc)
		toc.AddGlobalEntry("", dbname, "DATABASE GUC", start, metadataFile)
	}
}

func PrintCreateResourceQueueStatements(metadataFile *utils.FileWithByteCount, toc *utils.TOC, resQueues []ResourceQueue, resQueueMetadata MetadataMap) {
	for _, resQueue := range resQueues {
		start := metadataFile.ByteCount
		attributes := []string{}
		if resQueue.ActiveStatements != -1 {
			attributes = append(attributes, fmt.Sprintf("ACTIVE_STATEMENTS=%d", resQueue.ActiveStatements))
		}
		maxCostFloat, maxCostErr := strconv.ParseFloat(resQueue.MaxCost, 64)
		gplog.FatalOnError(maxCostErr)
		if maxCostFloat > -1 {
			attributes = append(attributes, fmt.Sprintf("MAX_COST=%s", resQueue.MaxCost))
		}
		if resQueue.CostOvercommit {
			attributes = append(attributes, "COST_OVERCOMMIT=TRUE")
		}
		minCostFloat, minCostErr := strconv.ParseFloat(resQueue.MinCost, 64)
		gplog.FatalOnError(minCostErr)
		if minCostFloat > 0 {
			attributes = append(attributes, fmt.Sprintf("MIN_COST=%s", resQueue.MinCost))
		}
		if resQueue.Priority != "medium" {
			attributes = append(attributes, fmt.Sprintf("PRIORITY=%s", strings.ToUpper(resQueue.Priority)))
		}
		if resQueue.MemoryLimit != "-1" {
			attributes = append(attributes, fmt.Sprintf("MEMORY_LIMIT='%s'", resQueue.MemoryLimit))
		}
		action := "CREATE"
		if resQueue.Name == "pg_default" {
			action = "ALTER"
		}
		metadataFile.MustPrintf("\n\n%s RESOURCE QUEUE %s WITH (%s);", action, resQueue.Name, strings.Join(attributes, ", "))
		PrintObjectMetadata(metadataFile, resQueueMetadata[resQueue.GetUniqueID()], resQueue.Name, "RESOURCE QUEUE")
		toc.AddGlobalEntry("", resQueue.Name, "RESOURCE QUEUE", start, metadataFile)
	}
}

func PrintResetResourceGroupStatements(metadataFile *utils.FileWithByteCount, toc *utils.TOC) {
	/*
	 * total cpu_rate_limit and memory_limit should less than 100, so clean
	 * them before we seting new memory_limit and cpu_rate_limit.
	 */
	defSettings := []struct {
		name    string
		setting string
	}{
		{"admin_group", "SET CPU_RATE_LIMIT 1"},
		{"admin_group", "SET MEMORY_LIMIT 1"},
		{"default_group", "SET CPU_RATE_LIMIT 1"},
		{"default_group", "SET MEMORY_LIMIT 1"},
	}

	for _, prepare := range defSettings {
		start := metadataFile.ByteCount
		metadataFile.MustPrintf("\n\nALTER RESOURCE GROUP %s %s;", prepare.name, prepare.setting)
		toc.AddGlobalEntry("", prepare.name, "RESOURCE GROUP", start, metadataFile)
	}
}

func PrintCreateResourceGroupStatements(metadataFile *utils.FileWithByteCount, toc *utils.TOC, resGroups []ResourceGroup, resGroupMetadata MetadataMap) {
	for _, resGroup := range resGroups {
		start := metadataFile.ByteCount

		if resGroup.Name == "default_group" || resGroup.Name == "admin_group" {
			resGroupList := []struct {
				setting string
				value   int
			}{
				{"MEMORY_LIMIT", resGroup.MemoryLimit},
				{"MEMORY_SHARED_QUOTA", resGroup.MemorySharedQuota},
				{"MEMORY_SPILL_RATIO", resGroup.MemorySpillRatio},
				{"CONCURRENCY", resGroup.Concurrency},
			}
			for _, property := range resGroupList {
				metadataFile.MustPrintf("\n\nALTER RESOURCE GROUP %s SET %s %d;", resGroup.Name, property.setting, property.value)
			}

			/* special handling for cpu properties */
			if resGroup.CPURateLimit >= 0 {
				/* cpu rate mode */
				metadataFile.MustPrintf("\n\nALTER RESOURCE GROUP %s SET CPU_RATE_LIMIT %d;", resGroup.Name, resGroup.CPURateLimit)
			} else {
				/* cpuset mode */
				metadataFile.MustPrintf("\n\nALTER RESOURCE GROUP %s SET CPUSET '%s';", resGroup.Name, resGroup.Cpuset)
			}
			PrintObjectMetadata(metadataFile, resGroupMetadata[resGroup.GetUniqueID()], resGroup.Name, "RESOURCE GROUP")
			toc.AddGlobalEntry("", resGroup.Name, "RESOURCE GROUP", start, metadataFile)
		} else {
			start = metadataFile.ByteCount
			attributes := []string{}

			/* special handling for cpu properties */
			if resGroup.CPURateLimit >= 0 {
				/* cpu rate mode */
				attributes = append(attributes, fmt.Sprintf("CPU_RATE_LIMIT=%d", resGroup.CPURateLimit))
			} else {
				/* cpuset mode */
				attributes = append(attributes, fmt.Sprintf("CPUSET='%s'", resGroup.Cpuset))
			}

			/*
			 * Possible values of memory_auditor:
			 * - "1": cgroup;
			 * - "0": vmtracker;
			 * - "": not set, e.g. created on an older version which does not
			 *   support memory_auditor yet, consider it as vmtracker;
			 */
			if resGroup.MemoryAuditor == 1 {
				attributes = append(attributes, fmt.Sprintf("MEMORY_AUDITOR=cgroup"))
			} else {
				attributes = append(attributes, fmt.Sprintf("MEMORY_AUDITOR=vmtracker"))
			}

			attributes = append(attributes, fmt.Sprintf("MEMORY_LIMIT=%d", resGroup.MemoryLimit))
			attributes = append(attributes, fmt.Sprintf("MEMORY_SHARED_QUOTA=%d", resGroup.MemorySharedQuota))
			attributes = append(attributes, fmt.Sprintf("MEMORY_SPILL_RATIO=%d", resGroup.MemorySpillRatio))
			attributes = append(attributes, fmt.Sprintf("CONCURRENCY=%d", resGroup.Concurrency))
			metadataFile.MustPrintf("\n\nCREATE RESOURCE GROUP %s WITH (%s);", resGroup.Name, strings.Join(attributes, ", "))
			PrintObjectMetadata(metadataFile, resGroupMetadata[resGroup.GetUniqueID()], resGroup.Name, "RESOURCE GROUP")
			toc.AddGlobalEntry("", resGroup.Name, "RESOURCE GROUP", start, metadataFile)
		}
	}
}

func PrintCreateRoleStatements(metadataFile *utils.FileWithByteCount, toc *utils.TOC, roles []Role, roleGUCs map[string][]string, roleMetadata MetadataMap) {
	for _, role := range roles {
		start := metadataFile.ByteCount
		attrs := []string{}

		if role.Super {
			attrs = append(attrs, "SUPERUSER")
		} else {
			attrs = append(attrs, "NOSUPERUSER")
		}

		if role.Inherit {
			attrs = append(attrs, "INHERIT")
		} else {
			attrs = append(attrs, "NOINHERIT")
		}

		if role.CreateRole {
			attrs = append(attrs, "CREATEROLE")
		} else {
			attrs = append(attrs, "NOCREATEROLE")
		}

		if role.CreateDB {
			attrs = append(attrs, "CREATEDB")
		} else {
			attrs = append(attrs, "NOCREATEDB")
		}

		if role.CanLogin {
			attrs = append(attrs, "LOGIN")
		} else {
			attrs = append(attrs, "NOLOGIN")
		}
		if role.ConnectionLimit != -1 {
			attrs = append(attrs, fmt.Sprintf("CONNECTION LIMIT %d", role.ConnectionLimit))
		}

		if role.Password != "" {
			attrs = append(attrs, fmt.Sprintf("PASSWORD '%s'", role.Password))
		}

		if role.ValidUntil != "" {
			attrs = append(attrs, fmt.Sprintf("VALID UNTIL '%s'", role.ValidUntil))
		}

		attrs = append(attrs, fmt.Sprintf("RESOURCE QUEUE %s", role.ResQueue))

		if connectionPool.Version.AtLeast("5") {
			attrs = append(attrs, fmt.Sprintf("RESOURCE GROUP %s", role.ResGroup))
		}

		if role.Createrexthttp {
			attrs = append(attrs, "CREATEEXTTABLE (protocol='http')")
		}

		if role.Createrextgpfd {
			attrs = append(attrs, "CREATEEXTTABLE (protocol='gpfdist', type='readable')")
		}

		if role.Createwextgpfd {
			attrs = append(attrs, "CREATEEXTTABLE (protocol='gpfdist', type='writable')")
		}

		if role.Createrexthdfs {
			attrs = append(attrs, "CREATEEXTTABLE (protocol='gphdfs', type='readable')")
		}

		if role.Createwexthdfs {
			attrs = append(attrs, "CREATEEXTTABLE (protocol='gphdfs', type='writable')")
		}

		metadataFile.MustPrintf(`

CREATE ROLE %s;
ALTER ROLE %s WITH %s;`, role.Name, role.Name, strings.Join(attrs, " "))

		for _, roleGUC := range roleGUCs[role.Name] {
			metadataFile.MustPrintf(`

ALTER ROLE %s %s;`, role.Name, roleGUC)
		}

		if len(role.TimeConstraints) != 0 {
			for _, timeConstraint := range role.TimeConstraints {
				metadataFile.MustPrintf("\nALTER ROLE %s DENY BETWEEN DAY %d TIME '%s' AND DAY %d TIME '%s';", role.Name, timeConstraint.StartDay, timeConstraint.StartTime, timeConstraint.EndDay, timeConstraint.EndTime)
			}
		}
		PrintObjectMetadata(metadataFile, roleMetadata[role.GetUniqueID()], role.Name, "ROLE")
		toc.AddGlobalEntry("", role.Name, "ROLE", start, metadataFile)
	}
}

func PrintRoleMembershipStatements(metadataFile *utils.FileWithByteCount, toc *utils.TOC, roleMembers []RoleMember) {
	metadataFile.MustPrintf("\n\n")
	for _, roleMember := range roleMembers {
		start := metadataFile.ByteCount
		metadataFile.MustPrintf("\nGRANT %s TO %s", roleMember.Role, roleMember.Member)
		if roleMember.IsAdmin {
			metadataFile.MustPrintf(" WITH ADMIN OPTION")
		}
		metadataFile.MustPrintf(" GRANTED BY %s;", roleMember.Grantor)
		toc.AddGlobalEntry("", roleMember.Member, "ROLE GRANT", start, metadataFile)
	}
}

func PrintCreateTablespaceStatements(metadataFile *utils.FileWithByteCount, toc *utils.TOC, tablespaces []Tablespace, tablespaceMetadata MetadataMap) {
	for _, tablespace := range tablespaces {
		start := metadataFile.ByteCount
		if tablespace.SegmentLocations != nil {
			metadataFile.MustPrintf("\n\nCREATE TABLESPACE %s LOCATION %s\n\tOPTIONS (%s);",
				tablespace.Tablespace, tablespace.FileLocation, strings.Join(tablespace.SegmentLocations, ", "))
		} else {
			metadataFile.MustPrintf("\n\nCREATE TABLESPACE %s FILESPACE %s;", tablespace.Tablespace, tablespace.FileLocation)
		}
		toc.AddGlobalEntry("", tablespace.Tablespace, "TABLESPACE", start, metadataFile)
		start = metadataFile.ByteCount
		PrintObjectMetadata(metadataFile, tablespaceMetadata[tablespace.GetUniqueID()], tablespace.Tablespace, "TABLESPACE")
		if metadataFile.ByteCount > start {
			toc.AddGlobalEntry("", tablespace.Tablespace, "TABLESPACE METADATA", start, metadataFile)
		}
	}
}
