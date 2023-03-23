package backup

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpbackup/toc"
	"github.com/greenplum-db/gpbackup/utils"
)

/*
 * This file contains structs and functions related to backing up global cluster
 * metadata on the coordinator that needs to be restored before data is restored,
 * such as roles and database configuration.
 */

func PrintSessionGUCs(metadataFile *utils.FileWithByteCount, toc *toc.TOC, gucs SessionGUCs) {
	start := metadataFile.ByteCount
	metadataFile.MustPrintf(`
SET client_encoding = '%s';
`, gucs.ClientEncoding)

	section, entry := gucs.GetMetadataEntry()
	toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
}

func PrintCreateDatabaseStatement(metadataFile *utils.FileWithByteCount, tocfile *toc.TOC, defaultDB Database, db Database, dbMetadata MetadataMap) {
	start := metadataFile.ByteCount
	metadataFile.MustPrintf("\n\nCREATE DATABASE %s TEMPLATE template0", db.Name)
	if db.Tablespace != "pg_default" {
		metadataFile.MustPrintf(" TABLESPACE %s", db.Tablespace)
	}
	if db.Encoding != "" && (db.Encoding != defaultDB.Encoding) {
		metadataFile.MustPrintf(" ENCODING '%s'", db.Encoding)
	}
	if db.Collate != "" && (db.Collate != defaultDB.Collate) {
		metadataFile.MustPrintf(" LC_COLLATE '%s'", db.Collate)
	}
	if db.CType != "" && (db.CType != defaultDB.CType) {
		metadataFile.MustPrintf(" LC_CTYPE '%s'", db.CType)
	}
	metadataFile.MustPrintf(";")

	entry := toc.MetadataEntry{Name: db.Name, ObjectType: "DATABASE"}
	tocfile.AddMetadataEntry("global", entry, start, metadataFile.ByteCount)
	PrintObjectMetadata(metadataFile, tocfile, dbMetadata[db.GetUniqueID()], db, "")
}

func PrintDatabaseGUCs(metadataFile *utils.FileWithByteCount, tocfile *toc.TOC, gucs []string, dbname string) {
	for _, guc := range gucs {
		start := metadataFile.ByteCount
		metadataFile.MustPrintf("\nALTER DATABASE %s %s;", dbname, guc)

		entry := toc.MetadataEntry{Name: dbname, ObjectType: "DATABASE GUC"}
		tocfile.AddMetadataEntry("global", entry, start, metadataFile.ByteCount)
	}
}

func PrintCreateResourceQueueStatements(metadataFile *utils.FileWithByteCount, tocfile *toc.TOC, resQueues []ResourceQueue, resQueueMetadata MetadataMap) {
	for _, resQueue := range resQueues {
		start := metadataFile.ByteCount
		attributes := make([]string, 0)
		if resQueue.ActiveStatements != -1 {
			attributes = append(attributes, fmt.Sprintf("ACTIVE_STATEMENTS=%d", resQueue.ActiveStatements))
		}
		maxCostFloat, parseErr := strconv.ParseFloat(resQueue.MaxCost, 64)
		gplog.FatalOnError(parseErr)
		if maxCostFloat > -1 {
			attributes = append(attributes, fmt.Sprintf("MAX_COST=%s", resQueue.MaxCost))
		}
		if resQueue.CostOvercommit {
			attributes = append(attributes, "COST_OVERCOMMIT=TRUE")
		}
		minCostFloat, parseErr := strconv.ParseFloat(resQueue.MinCost, 64)
		gplog.FatalOnError(parseErr)
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

		section, entry := resQueue.GetMetadataEntry()
		tocfile.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
		PrintObjectMetadata(metadataFile, tocfile, resQueueMetadata[resQueue.GetUniqueID()], resQueue, "")
	}
}

func PrintResetResourceGroupStatements(metadataFile *utils.FileWithByteCount, tocfile *toc.TOC) {
	/*
	 * total cpu_rate_limit and memory_limit should less than 100, so clean
	 * them before we seting new memory_limit and cpu_rate_limit.
	 *
	 * Minimal memory_limit is adjusted from 1 to 0 since 5.20, however for
	 * backward compatibility we still use 1 as the minimal value.  The only
	 * failing case is that default_group has memory_limit=100 and admin_group
	 * has memory_limit=0, but this should not happen in real world.
	 */

	type DefSetting struct {
		name    string
		setting string
	}
	defSettings := make([]DefSetting, 0)

	if connectionPool.Version.Before("7") {
		defSettings = append(defSettings, DefSetting{"admin_group", "SET CPU_RATE_LIMIT 1"})
		defSettings = append(defSettings, DefSetting{"admin_group", "SET MEMORY_LIMIT 1"})
		defSettings = append(defSettings, DefSetting{"default_group", "SET CPU_RATE_LIMIT 1"})
		defSettings = append(defSettings, DefSetting{"default_group", "SET MEMORY_LIMIT 1"})
	} else { // GPDB7+
		defSettings = append(defSettings, DefSetting{"admin_group", "SET CPU_HARD_QUOTA_LIMIT 1"})
		defSettings = append(defSettings, DefSetting{"admin_group", "SET CPU_SOFT_PRIORITY 100"})
		defSettings = append(defSettings, DefSetting{"default_group", "SET CPU_HARD_QUOTA_LIMIT 1"})
		defSettings = append(defSettings, DefSetting{"default_group", "SET CPU_SOFT_PRIORITY 100"})
		defSettings = append(defSettings, DefSetting{"system_group", "SET CPU_HARD_QUOTA_LIMIT 1"})
		defSettings = append(defSettings, DefSetting{"system_group", "SET CPU_SOFT_PRIORITY 100"})
	}

	for _, prepare := range defSettings {
		start := metadataFile.ByteCount
		metadataFile.MustPrintf("\n\nALTER RESOURCE GROUP %s %s;", prepare.name, prepare.setting)

		entry := toc.MetadataEntry{Name: prepare.name, ObjectType: "RESOURCE GROUP"}
		tocfile.AddMetadataEntry("global", entry, start, metadataFile.ByteCount)
	}
}

func PrintCreateResourceGroupStatementsAtLeast7(metadataFile *utils.FileWithByteCount, toc *toc.TOC, resGroups []ResourceGroupAtLeast7, resGroupMetadata MetadataMap) {
	for _, resGroup := range resGroups {
		var start uint64
		section, entry := resGroup.GetMetadataEntry()
		if resGroup.Name == "default_group" || resGroup.Name == "admin_group" || resGroup.Name == "system_group" {
			resGroupList := []struct {
				setting string
				value   string
			}{
				{"CPU_SOFT_PRIORITY", resGroup.CpuSoftPriority},
				{"CONCURRENCY", resGroup.Concurrency},
			}
			for _, property := range resGroupList {
				start = metadataFile.ByteCount
				metadataFile.MustPrintf("\n\nALTER RESOURCE GROUP %s SET %s %s;", resGroup.Name, property.setting, property.value)

				toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
			}

			/* special handling for cpu properties */
			// TODO -- why do we handle these separately?
			// TODO -- is this still necessary for 7?
			start = metadataFile.ByteCount
			if !strings.HasPrefix(resGroup.CpuHardQuotaLimit, "-") {
				/* cpu rate mode */
				metadataFile.MustPrintf("\n\nALTER RESOURCE GROUP %s SET CPU_HARD_QUOTA_LIMIT %s;", resGroup.Name, resGroup.CpuHardQuotaLimit)
			} else {
				/* cpuset mode */
				metadataFile.MustPrintf("\n\nALTER RESOURCE GROUP %s SET CPUSET '%s';", resGroup.Name, resGroup.Cpuset)
			}

			toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
			PrintObjectMetadata(metadataFile, toc, resGroupMetadata[resGroup.GetUniqueID()], resGroup, "")
		} else {
			start = metadataFile.ByteCount
			attributes := make([]string, 0)

			/* special handling for cpu properties */
			// TODO -- why do we handle these separately?
			// TODO -- is this still necessary for 7?
			if !strings.HasPrefix(resGroup.CpuHardQuotaLimit, "-") {
				/* cpu rate mode */
				attributes = append(attributes, fmt.Sprintf("CPU_HARD_QUOTA_LIMIT=%s", resGroup.CpuHardQuotaLimit))
			} else if connectionPool.Version.AtLeast("5.9.0") {
				/* cpuset mode */
				attributes = append(attributes, fmt.Sprintf("CPUSET='%s'", resGroup.Cpuset))
			}
			attributes = append(attributes, fmt.Sprintf("CPU_SOFT_PRIORITY=%s", resGroup.CpuSoftPriority))
			attributes = append(attributes, fmt.Sprintf("CONCURRENCY=%s", resGroup.Concurrency))
			metadataFile.MustPrintf("\n\nCREATE RESOURCE GROUP %s WITH (%s);", resGroup.Name, strings.Join(attributes, ", "))

			toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
			PrintObjectMetadata(metadataFile, toc, resGroupMetadata[resGroup.GetUniqueID()], resGroup, "")
		}
	}
}

func PrintCreateResourceGroupStatementsBefore7(metadataFile *utils.FileWithByteCount, toc *toc.TOC, resGroups []ResourceGroupBefore7, resGroupMetadata MetadataMap) {
	for _, resGroup := range resGroups {

		// temporarily special case for 5x resource groups #temp5xResGroup
		memorySpillRatio := resGroup.MemorySpillRatio

		if connectionPool.Version.Is("5") {
			/*
			 * memory_spill_ratio can be set in absolute value format since 5.20,
			 * such as '1 MB', it has to be set as a quoted string, otherwise set
			 * it without quotes.
			 */
			if _, err := strconv.Atoi(memorySpillRatio); err != nil {
				/* memory_spill_ratio is in absolute value format, set it with quotes */

				memorySpillRatio = "'" + memorySpillRatio + "'"
			}
		}

		var start uint64
		section, entry := resGroup.GetMetadataEntry()
		if resGroup.Name == "default_group" || resGroup.Name == "admin_group" {
			resGroupList := []struct {
				setting string
				value   string
			}{
				{"MEMORY_LIMIT", resGroup.MemoryLimit},
				{"MEMORY_SHARED_QUOTA", resGroup.MemorySharedQuota},
				{"MEMORY_SPILL_RATIO", memorySpillRatio},
				{"CONCURRENCY", resGroup.Concurrency},
			}
			for _, property := range resGroupList {
				start = metadataFile.ByteCount
				metadataFile.MustPrintf("\n\nALTER RESOURCE GROUP %s SET %s %s;", resGroup.Name, property.setting, property.value)

				toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
			}

			/* special handling for cpu properties */
			start = metadataFile.ByteCount
			if !strings.HasPrefix(resGroup.CPURateLimit, "-") {
				/* cpu rate mode */
				metadataFile.MustPrintf("\n\nALTER RESOURCE GROUP %s SET CPU_RATE_LIMIT %s;", resGroup.Name, resGroup.CPURateLimit)
			} else if connectionPool.Version.AtLeast("5.9.0") {
				/* cpuset mode */
				metadataFile.MustPrintf("\n\nALTER RESOURCE GROUP %s SET CPUSET '%s';", resGroup.Name, resGroup.Cpuset)
			}

			toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
			PrintObjectMetadata(metadataFile, toc, resGroupMetadata[resGroup.GetUniqueID()], resGroup, "")
		} else {
			start = metadataFile.ByteCount
			attributes := make([]string, 0)

			/* special handling for cpu properties */
			if !strings.HasPrefix(resGroup.CPURateLimit, "-") {
				/* cpu rate mode */
				attributes = append(attributes, fmt.Sprintf("CPU_RATE_LIMIT=%s", resGroup.CPURateLimit))
			} else if connectionPool.Version.AtLeast("5.9.0") {
				/* cpuset mode */
				attributes = append(attributes, fmt.Sprintf("CPUSET='%s'", resGroup.Cpuset))
			}

			/*
			 * Possible values of memory_auditor:
			 * - "1": cgroup
			 * - "0": vmtracker (default)
			 */
			if resGroup.MemoryAuditor == "1" {
				attributes = append(attributes, "MEMORY_AUDITOR=cgroup")
			} else if connectionPool.Version.AtLeast("5.8.0") {
				attributes = append(attributes, "MEMORY_AUDITOR=vmtracker")
			}

			attributes = append(attributes, fmt.Sprintf("MEMORY_LIMIT=%s", resGroup.MemoryLimit))
			attributes = append(attributes, fmt.Sprintf("MEMORY_SHARED_QUOTA=%s", resGroup.MemorySharedQuota))
			attributes = append(attributes, fmt.Sprintf("MEMORY_SPILL_RATIO=%s", memorySpillRatio))
			attributes = append(attributes, fmt.Sprintf("CONCURRENCY=%s", resGroup.Concurrency))
			metadataFile.MustPrintf("\n\nCREATE RESOURCE GROUP %s WITH (%s);", resGroup.Name, strings.Join(attributes, ", "))

			toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
			PrintObjectMetadata(metadataFile, toc, resGroupMetadata[resGroup.GetUniqueID()], resGroup, "")
		}
	}
}

func PrintCreateRoleStatements(metadataFile *utils.FileWithByteCount, toc *toc.TOC, roles []Role, roleMetadata MetadataMap) {
	for _, role := range roles {
		start := metadataFile.ByteCount
		attrs := make([]string, 0)

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

		if role.Replication {
			attrs = append(attrs, "REPLICATION")
			/*
			* Not adding NOREPLICATION when this is false because that option
			* was not supported prior to 6 and NOREPLICATION is the default
			 */
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

		section, entry := role.GetMetadataEntry()
		toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)

		if len(role.TimeConstraints) != 0 {
			for _, timeConstraint := range role.TimeConstraints {
				start := metadataFile.ByteCount
				metadataFile.MustPrintf("\nALTER ROLE %s DENY BETWEEN DAY %d TIME '%s' AND DAY %d TIME '%s';", role.Name, timeConstraint.StartDay, timeConstraint.StartTime, timeConstraint.EndDay, timeConstraint.EndTime)
				toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
			}
		}
		PrintObjectMetadata(metadataFile, toc, roleMetadata[role.GetUniqueID()], role, "")
	}
}

func PrintRoleGUCStatements(metadataFile *utils.FileWithByteCount, toc *toc.TOC, roleGUCs map[string][]RoleGUC) {
	for roleName := range roleGUCs {
		for _, roleGUC := range roleGUCs[roleName] {
			start := metadataFile.ByteCount
			dbString := ""
			if roleGUC.DbName != "" {
				dbString = fmt.Sprintf("IN DATABASE %s ", roleGUC.DbName)
			}
			metadataFile.MustPrintf("\n\nALTER ROLE %s %s%s;", roleName, dbString, roleGUC.Config)

			section, entry := roleGUC.GetMetadataEntry()
			toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
		}
	}
}

func PrintRoleMembershipStatements(metadataFile *utils.FileWithByteCount, toc *toc.TOC, roleMembers []RoleMember) {
	metadataFile.MustPrintf("\n\n")
	for _, roleMember := range roleMembers {
		start := metadataFile.ByteCount
		metadataFile.MustPrintf("\nGRANT %s TO %s", roleMember.Role, roleMember.Member)
		if roleMember.IsAdmin {
			metadataFile.MustPrintf(" WITH ADMIN OPTION")
		}
		if roleMember.Grantor != "" {
			metadataFile.MustPrintf(" GRANTED BY %s", roleMember.Grantor)
		}
		metadataFile.MustPrintf(";")

		section, entry := roleMember.GetMetadataEntry()
		toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
	}
}

func PrintCreateTablespaceStatements(metadataFile *utils.FileWithByteCount, toc *toc.TOC, tablespaces []Tablespace, tablespaceMetadata MetadataMap) {
	for _, tablespace := range tablespaces {
		start := metadataFile.ByteCount
		locationStr := ""
		if tablespace.SegmentLocations == nil {
			locationStr = fmt.Sprintf("FILESPACE %s", tablespace.FileLocation)
		} else if len(tablespace.SegmentLocations) == 0 {
			locationStr = fmt.Sprintf("LOCATION %s", tablespace.FileLocation)
		} else {
			locationStr = fmt.Sprintf("LOCATION %s\n\tWITH (%s)", tablespace.FileLocation, strings.Join(tablespace.SegmentLocations, ", "))
		}
		metadataFile.MustPrintf("\n\nCREATE TABLESPACE %s %s;", tablespace.Tablespace, locationStr)

		section, entry := tablespace.GetMetadataEntry()
		toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)

		if tablespace.Options != "" {
			start = metadataFile.ByteCount
			metadataFile.MustPrintf("\n\nALTER TABLESPACE %s SET (%s);\n", tablespace.Tablespace, tablespace.Options)
			toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
		}
		PrintObjectMetadata(metadataFile, toc, tablespaceMetadata[tablespace.GetUniqueID()], tablespace, "")
	}
}
