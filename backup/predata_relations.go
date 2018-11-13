package backup

/*
 * This file contains structs and functions related to backing up relation
 * (sequence, table, and view) metadata on the master.
 */

import (
	"fmt"
	"strings"

	"math"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpbackup/utils"
)

/*
 * When leafPartitionData is set, for partition tables we want to print metadata
 * for the parent tables and data for the leaf tables, so we split them into
 * separate lists.  Intermediate tables are skipped, and non-partition tables are
 * backed up normally (both metadata and data).
 *
 * When the flag is not set, we want to back up both metadata and data for all
 * tables, so both returned arrays contain all tables.
 */
func SplitTablesByPartitionType(tables []Table, includeList []string) ([]Table, []Table) {
	metadataTables := make([]Table, 0)
	dataTables := make([]Table, 0)
	if MustGetFlagBool(utils.LEAF_PARTITION_DATA) || len(includeList) > 0 {
		includeSet := utils.NewSet(includeList)
		for _, table := range tables {
			if table.IsExternal && table.PartitionLevelInfo.Level == "l" {
				table.Name = AppendExtPartSuffix(table.Name)
				metadataTables = append(metadataTables, table)
			}
			partType := table.PartitionLevelInfo.Level
			if partType != "l" && partType != "i" {
				metadataTables = append(metadataTables, table)
			}
			if MustGetFlagBool(utils.LEAF_PARTITION_DATA) {
				if partType != "p" && partType != "i" {
					dataTables = append(dataTables, table)
				}
			} else if includeSet.MatchesFilter(table.FQN()) {
				dataTables = append(dataTables, table)
			}
		}
	} else {
		for _, table := range tables {
			if table.IsExternal && table.PartitionLevelInfo.Level == "l" {
				table.Name = AppendExtPartSuffix(table.Name)
			}
			metadataTables = append(metadataTables, table)
		}
		dataTables = tables
	}
	return metadataTables, dataTables
}

func AppendExtPartSuffix(name string) string {
	const SUFFIX = "_ext_part_"
	const MAX_LEN = 63                 // MAX_DATA_LEN - 1 is the maximum length of a relation name
	const QUOTED_MAX_LEN = MAX_LEN + 2 // We add 2 to account for a double quote on each end
	if name[len(name)-1] == '"' {

		if len(name)+len(SUFFIX) > QUOTED_MAX_LEN {
			return name[0:QUOTED_MAX_LEN-len(SUFFIX)] + SUFFIX + `"`
		}
		return name[0:len(name)-1] + SUFFIX + `"`
	}
	if len(name)+len(SUFFIX) > MAX_LEN {
		return name[0:MAX_LEN+1-len(SUFFIX)] + SUFFIX
	}
	return name + SUFFIX
}

func ExpandIncludeRelations(tables []Relation) {
	includeRelations := MustGetFlagStringSlice(utils.INCLUDE_RELATION)

	if len(includeRelations) == 0 {
		return
	}

	includeMap := make(map[string]bool, 0)
	for _, relation := range includeRelations {
		includeMap[relation] = true
	}

	for _, table := range tables {
		if _, ok := includeMap[table.FQN()]; !ok {
			err := cmdFlags.Set(utils.INCLUDE_RELATION, table.FQN()) //This appends to the slice underlying the flag.
			gplog.FatalOnError(err)
		}
	}
}

func ConstructColumnPrivilegesMap(results []ColumnPrivilegesQueryStruct) map[uint32]map[string][]ACL {
	metadataMap := make(map[uint32]map[string][]ACL)
	var tableMetadata map[string][]ACL
	var columnMetadata []ACL
	if len(results) > 0 {
		quotedRoleNames := GetQuotedRoleNames(connectionPool)
		currentTable := uint32(0)
		currentColumn := ""
		/*
		 * We group ACLs for each column into its own metadata object.
		 * All column metadata objects are stored in the result map as
		 * a nested map indexed by table oid.
		 */
		tableMetadata = make(map[string][]ACL, 0)
		for _, result := range results {
			privilegesStr := ""
			if result.Kind == "Empty" {
				privilegesStr = "GRANTEE=/GRANTOR"
			} else if result.Privileges.Valid {
				privilegesStr = result.Privileges.String
			}
			if result.TableOid != currentTable || result.Name != currentColumn {
				if currentTable != 0 && currentColumn != "" {
					tableMetadata[currentColumn] = sortACLs(columnMetadata)
					if result.TableOid != currentTable {
						metadataMap[currentTable] = tableMetadata
						tableMetadata = make(map[string][]ACL, 0)
					}
				}
				currentTable = result.TableOid
				currentColumn = result.Name
				columnMetadata = make([]ACL, 0)
			}
			privileges := ParseACL(privilegesStr, quotedRoleNames)
			if privileges != nil {
				columnMetadata = append(columnMetadata, *privileges)
			}
		}
		tableMetadata[currentColumn] = sortACLs(columnMetadata)
		metadataMap[currentTable] = tableMetadata
	}
	return metadataMap
}

/*
 * This function prints CREATE TABLE statements in a format very similar to pg_dump.  Unlike pg_dump,
 * however, table names are printed fully qualified with their schemas instead of relying on setting
 * the search_path; this will aid in later filtering to include or exclude certain tables during the
 * backup process, and allows customers to copy just the CREATE TABLE block in order to use it directly.
 */
func PrintCreateTableStatement(metadataFile *utils.FileWithByteCount, toc *utils.TOC, table Table, tableMetadata ObjectMetadata) {
	start := metadataFile.ByteCount
	// We use an empty TOC below to keep count of the bytes for testing purposes.
	if table.IsExternal && table.PartitionLevelInfo.Level != "p" {
		PrintExternalTableCreateStatement(metadataFile, nil, table)
	} else {
		PrintRegularTableCreateStatement(metadataFile, nil, table)
	}
	PrintPostCreateTableStatements(metadataFile, table, tableMetadata)
	toc.AddPredataEntry(table.Schema, table.Name, "TABLE", "", start, metadataFile)
}

func PrintRegularTableCreateStatement(metadataFile *utils.FileWithByteCount, toc *utils.TOC, table Table) {
	start := metadataFile.ByteCount

	typeStr := ""
	if table.TableType != "" {
		typeStr = fmt.Sprintf("OF %s ", table.TableType)
	}

	tableModifier := ""
	if table.IsUnlogged {
		tableModifier = "UNLOGGED "
	} else if table.ForeignDef != (ForeignTableDefinition{}) {
		tableModifier = "FOREIGN "
	}

	metadataFile.MustPrintf("\n\nCREATE %sTABLE %s %s(\n", tableModifier, table.FQN(), typeStr)

	printColumnDefinitions(metadataFile, table.ColumnDefs, table.TableType)
	metadataFile.MustPrintf(") ")
	if len(table.Inherits) != 0 {
		dependencyList := strings.Join(table.Inherits, ", ")
		metadataFile.MustPrintf("INHERITS (%s) ", dependencyList)
	}
	if table.ForeignDef != (ForeignTableDefinition{}) {
		metadataFile.MustPrintf("SERVER %s ", table.ForeignDef.Server)
		if table.ForeignDef.Options != "" {
			metadataFile.MustPrintf("OPTIONS (%s) ", table.ForeignDef.Options)
		}
	}
	if table.StorageOpts != "" {
		metadataFile.MustPrintf("WITH (%s) ", table.StorageOpts)
	}
	if table.TablespaceName != "" {
		metadataFile.MustPrintf("TABLESPACE %s ", table.TablespaceName)
	}
	metadataFile.MustPrintf("%s", table.DistPolicy)
	if table.PartDef != "" {
		metadataFile.MustPrintf(" %s", strings.TrimSpace(table.PartDef))
	}
	metadataFile.MustPrintln(";")
	if table.PartTemplateDef != "" {
		metadataFile.MustPrintf("%s;\n", strings.TrimSpace(table.PartTemplateDef))
	}
	printAlterColumnStatements(metadataFile, table, table.ColumnDefs)
	if toc != nil {
		toc.AddPredataEntry(table.Schema, table.Name, "TABLE", "", start, metadataFile)
	}
}

func printColumnDefinitions(metadataFile *utils.FileWithByteCount, columnDefs []ColumnDefinition, tableType string) {
	lines := make([]string, 0)
	for _, column := range columnDefs {
		line := fmt.Sprintf("\t%s %s", column.Name, column.Type)
		if tableType != "" {
			line = fmt.Sprintf("\t%s WITH OPTIONS", column.Name)
		}
		if column.FdwOptions != "" {
			line += fmt.Sprintf(" OPTIONS (%s)", column.FdwOptions)
		}
		if column.Collation != "" {
			line += fmt.Sprintf(" COLLATE %s", column.Collation)
		}
		if column.HasDefault {
			line += fmt.Sprintf(" DEFAULT %s", column.DefaultVal)
		}
		if column.NotNull {
			line += " NOT NULL"
		}
		if column.Encoding != "" {
			line += fmt.Sprintf(" ENCODING (%s)", column.Encoding)
		}
		lines = append(lines, line)
	}
	if len(lines) > 0 {
		metadataFile.MustPrintln(strings.Join(lines, ",\n"))
	}
}

func printAlterColumnStatements(metadataFile *utils.FileWithByteCount, table Table, columnDefs []ColumnDefinition) {
	for _, column := range columnDefs {
		if column.StatTarget > -1 {
			metadataFile.MustPrintf("\nALTER TABLE ONLY %s ALTER COLUMN %s SET STATISTICS %d;", table.FQN(), column.Name, column.StatTarget)
		}
		if column.StorageType != "" {
			metadataFile.MustPrintf("\nALTER TABLE ONLY %s ALTER COLUMN %s SET STORAGE %s;", table.FQN(), column.Name, column.StorageType)
		}
		if column.Options != "" {
			metadataFile.MustPrintf("\nALTER TABLE ONLY %s ALTER COLUMN %s SET (%s);", table.FQN(), column.Name, column.Options)
		}
	}
}

/*
 * This function prints additional statements that come after the CREATE TABLE
 * statement for both regular and external tables.
 */
func PrintPostCreateTableStatements(metadataFile *utils.FileWithByteCount, table Table, tableMetadata ObjectMetadata) {
	if (table.ForeignDef != ForeignTableDefinition{}) {
		PrintObjectMetadata(metadataFile, tableMetadata, table.FQN(), "FOREIGN TABLE")
	} else {
		PrintObjectMetadata(metadataFile, tableMetadata, table.FQN(), "TABLE")
	}

	for _, att := range table.ColumnDefs {
		if att.Comment != "" {
			escapedComment := utils.EscapeSingleQuotes(att.Comment)
			metadataFile.MustPrintf("\n\nCOMMENT ON COLUMN %s.%s IS '%s';\n", table.FQN(), att.Name, escapedComment)
		}
		if len(att.ACL) > 0 {
			columnMetadata := ObjectMetadata{Privileges: att.ACL, Owner: tableMetadata.Owner}
			columnPrivileges := columnMetadata.GetPrivilegesStatements(table.FQN(), "COLUMN", att.Name)
			metadataFile.MustPrintln(columnPrivileges)
		}
		if att.SecurityLabel != "" {
			escapedLabel := utils.EscapeSingleQuotes(att.SecurityLabel)
			metadataFile.MustPrintf("\n\nSECURITY LABEL FOR %s ON COLUMN %s.%s IS '%s';", att.SecurityLabelProvider, table.FQN(), att.Name, escapedLabel)
		}
	}
}

/*
 * This function is largely derived from the dumpSequence() function in pg_dump.c.  The values of
 * minVal and maxVal come from SEQ_MINVALUE and SEQ_MAXVALUE, defined in include/commands/sequence.h.
 */
func PrintCreateSequenceStatements(metadataFile *utils.FileWithByteCount, toc *utils.TOC, sequences []Sequence, sequenceMetadata MetadataMap) {
	maxVal := int64(math.MaxInt64)
	minVal := int64(math.MinInt64)
	for _, sequence := range sequences {
		start := metadataFile.ByteCount
		seqFQN := sequence.FQN()
		metadataFile.MustPrintln("\n\nCREATE SEQUENCE", seqFQN)
		if connectionPool.Version.AtLeast("6") {
			metadataFile.MustPrintln("\tSTART WITH", sequence.StartVal)
		} else if !sequence.IsCalled {
			metadataFile.MustPrintln("\tSTART WITH", sequence.LastVal)
		}
		metadataFile.MustPrintln("\tINCREMENT BY", sequence.Increment)

		if !((sequence.MaxVal == maxVal && sequence.Increment > 0) || (sequence.MaxVal == -1 && sequence.Increment < 0)) {
			metadataFile.MustPrintln("\tMAXVALUE", sequence.MaxVal)
		} else {
			metadataFile.MustPrintln("\tNO MAXVALUE")
		}
		if !((sequence.MinVal == minVal && sequence.Increment < 0) || (sequence.MinVal == 1 && sequence.Increment > 0)) {
			metadataFile.MustPrintln("\tMINVALUE", sequence.MinVal)
		} else {
			metadataFile.MustPrintln("\tNO MINVALUE")
		}
		cycleStr := ""
		if sequence.IsCycled {
			cycleStr = "\n\tCYCLE"
		}
		metadataFile.MustPrintf("\tCACHE %d%s;", sequence.CacheVal, cycleStr)

		metadataFile.MustPrintf("\n\nSELECT pg_catalog.setval('%s', %d, %v);\n", utils.EscapeSingleQuotes(seqFQN), sequence.LastVal, sequence.IsCalled)

		PrintObjectMetadata(metadataFile, sequenceMetadata[sequence.Relation.GetUniqueID()], seqFQN, "SEQUENCE")
		toc.AddPredataEntry(sequence.Relation.Schema, sequence.Relation.Name, "SEQUENCE", sequence.OwningTable, start, metadataFile)
	}
}

func PrintAlterSequenceStatements(metadataFile *utils.FileWithByteCount, toc *utils.TOC, sequences []Sequence, sequenceColumnOwners map[string]string) {
	gplog.Verbose("Writing ALTER SEQUENCE statements to metadata file")
	for _, sequence := range sequences {
		seqFQN := sequence.FQN()
		// owningColumn is quoted when the map is constructed in GetSequenceColumnOwnerMap() and doesn't need to be quoted again
		if owningColumn, hasColumnOwner := sequenceColumnOwners[seqFQN]; hasColumnOwner {
			start := metadataFile.ByteCount
			metadataFile.MustPrintf("\n\nALTER SEQUENCE %s OWNED BY %s;\n", seqFQN, owningColumn)
			toc.AddPredataEntry(sequence.Relation.Schema, sequence.Relation.Name, "SEQUENCE OWNER", sequence.OwningTable, start, metadataFile)
		}
	}
}

func PrintCreateViewStatement(metadataFile *utils.FileWithByteCount, toc *utils.TOC, view View, viewMetadata ObjectMetadata) {
	start := metadataFile.ByteCount
	viewFQN := utils.MakeFQN(view.Schema, view.Name)
	metadataFile.MustPrintf("\n\nCREATE VIEW %s%s AS %s\n", viewFQN, view.Options, view.Definition)
	PrintObjectMetadata(metadataFile, viewMetadata, viewFQN, "VIEW")
	toc.AddPredataEntry(view.Schema, view.Name, "VIEW", "", start, metadataFile)
}
