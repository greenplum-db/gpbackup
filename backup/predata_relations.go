package backup

/*
 * This file contains structs and functions related to backing up relation
 * (sequence, table, and view) metadata on the coordinator.
 */

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/pkg/errors"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpbackup/options"
	"github.com/greenplum-db/gpbackup/toc"
	"github.com/greenplum-db/gpbackup/utils"
	"github.com/spf13/pflag"
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
func SplitTablesByPartitionType(tables []Table, includeList []options.Relation) ([]Table, []Table) {
	metadataTables := make([]Table, 0)
	dataTables := make([]Table, 0)
	if MustGetFlagBool(options.LEAF_PARTITION_DATA) || len(includeList) > 0 {
		// generate a set of oids for more efficient matching in large dumps
		includeSet := make(map[uint32]bool, len(includeList))
		for _, item := range includeList {
			includeSet[item.Oid] = true
		}

		for _, table := range tables {
			if table.IsExternal && table.PartitionLevelInfo.Level == "l" {
				if connectionPool.Version.Before("7") {
					// GPDB7+ has different conventions for external partitions
					// and does not need the suffix added
					table.Name = AppendExtPartSuffix(table.Name)
				}
				metadataTables = append(metadataTables, table)
			}
			partType := table.PartitionLevelInfo.Level
			if connectionPool.Version.AtLeast("7") {
				// In GPDB 7+, we need to dump out the leaf partition DDL along with their
				// ALTER TABLE ATTACH PARTITION commands to construct the partition table
				metadataTables = append(metadataTables, table)
			} else if partType != "l" && partType != "i" {
				metadataTables = append(metadataTables, table)
			}
			if MustGetFlagBool(options.LEAF_PARTITION_DATA) {
				if partType != "p" && partType != "i" {
					dataTables = append(dataTables, table)
				}
			} else if connectionPool.Version.AtLeast("7") &&
				table.AttachPartitionInfo != (AttachPartitionInfo{}) {
				// For GPDB 7+ and without --leaf-partition-data, we must exclude the
				// leaf partitions from dumping data. The COPY will be called on the
				// top-most root partition.
				continue
			} else if _, match := includeSet[table.Oid]; match {
				dataTables = append(dataTables, table)
			}
		}
	} else {
		var excludeList *utils.FilterSet
		if connectionPool.Version.AtLeast("7") {
			excludeList = utils.NewExcludeSet(MustGetFlagStringArray(options.EXCLUDE_RELATION))
		} else {
			excludeList = utils.NewExcludeSet([]string{})
		}

		for _, table := range tables {
			// In GPDB 7+, we need to filter out leaf and intermediate subroot partitions
			// from being added to the metadata table list if their root partition parent
			// is in the exclude list. This is to prevent ATTACH PARTITION statements
			// against nonexistant root partitions from being printed to the metadata file.
			if connectionPool.Version.AtLeast("7") &&
				table.AttachPartitionInfo != (AttachPartitionInfo{}) &&
				!excludeList.MatchesFilter(table.AttachPartitionInfo.Parent) {
				continue
			}

			if connectionPool.Version.Before("7") && table.IsExternal && table.PartitionLevelInfo.Level == "l" {
				table.Name = AppendExtPartSuffix(table.Name)
			}

			metadataTables = append(metadataTables, table)
			// In GPDB 7+, we need to filter out leaf and intermediate subroot partitions
			// since the COPY will be called on the top-most root partition. It just so
			// happens that those particular partition types will always have an
			// AttachPartitionInfo initialized.
			if table.AttachPartitionInfo == (AttachPartitionInfo{}) {
				dataTables = append(dataTables, table)
			}
		}
	}
	return metadataTables, dataTables
}

func AppendExtPartSuffix(name string) string {
	// Do not call this function for GPDB7+
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

/*
 * This function prints CREATE TABLE statements in a format very similar to pg_dump.  Unlike pg_dump,
 * however, table names are printed fully qualified with their schemas instead of relying on setting
 * the search_path; this will aid in later filtering to include or exclude certain tables during the
 * backup process, and allows customers to copy just the CREATE TABLE block in order to use it directly.
 */
func PrintCreateTableStatement(metadataFile *utils.FileWithByteCount, toc *toc.TOC, table Table, tableMetadata ObjectMetadata) {
	start := metadataFile.ByteCount
	// We use an empty TOC below to keep count of the bytes for testing purposes.
	if table.IsExternal && table.PartitionLevelInfo.Level != "p" {
		PrintExternalTableCreateStatement(metadataFile, nil, table)
	} else {
		PrintRegularTableCreateStatement(metadataFile, nil, table)
	}
	section, entry := table.GetMetadataEntry()
	toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
	PrintPostCreateTableStatements(metadataFile, toc, table, tableMetadata)
}

func PrintRegularTableCreateStatement(metadataFile *utils.FileWithByteCount, toc *toc.TOC, table Table) {
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
	if table.PartitionKeyDef != "" {
		metadataFile.MustPrintf("PARTITION BY %s ", table.PartitionKeyDef)
	}
	if len(table.Inherits) != 0 && table.AttachPartitionInfo == (AttachPartitionInfo{}) {
		dependencyList := strings.Join(table.Inherits, ", ")
		metadataFile.MustPrintf("INHERITS (%s) ", dependencyList)
	}
	if table.ForeignDef != (ForeignTableDefinition{}) {
		metadataFile.MustPrintf("SERVER %s ", table.ForeignDef.Server)
		if table.ForeignDef.Options != "" {
			metadataFile.MustPrintf("OPTIONS (%s) ", table.ForeignDef.Options)
		}
	}
	if table.AccessMethodName != "" {
		metadataFile.MustPrintf("USING %s ", table.AccessMethodName)
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
		section, entry := table.GetMetadataEntry()
		toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
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
			if column.AttGenerated != "" {
				// Unlike most keywords, GENERATED cannot be applied to a column that inherits from a parent table,
				// even if the specified generation expression is identical to that of the column it inherits,
				// so we skip printing it in that case.
				if !column.IsInherited {
					line += fmt.Sprintf(" GENERATED ALWAYS AS %s %s", column.DefaultVal, column.AttGenerated)
				}
			} else {
				line += fmt.Sprintf(" DEFAULT %s", column.DefaultVal)
			}
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
func PrintPostCreateTableStatements(metadataFile *utils.FileWithByteCount, toc *toc.TOC, table Table, tableMetadata ObjectMetadata) {
	PrintObjectMetadata(metadataFile, toc, tableMetadata, table, "")
	statements := make([]string, 0)
	for _, att := range table.ColumnDefs {
		if att.Comment != "" {
			escapedComment := utils.EscapeSingleQuotes(att.Comment)
			statements = append(statements, fmt.Sprintf("COMMENT ON COLUMN %s.%s IS '%s';", table.FQN(), att.Name, escapedComment))
		}
		if att.Privileges.Valid {
			columnMetadata := ObjectMetadata{Privileges: getColumnACL(att.Privileges, att.Kind), Owner: tableMetadata.Owner}
			columnPrivileges := columnMetadata.GetPrivilegesStatements(table.FQN(), "COLUMN", att.Name)
			statements = append(statements, strings.TrimSpace(columnPrivileges))
		}
		if att.SecurityLabel != "" {
			escapedLabel := utils.EscapeSingleQuotes(att.SecurityLabel)
			statements = append(statements, fmt.Sprintf("SECURITY LABEL FOR %s ON COLUMN %s.%s IS '%s';", att.SecurityLabelProvider, table.FQN(), att.Name, escapedLabel))
		}
	}

	// It seems that replica identity on foreign tables default to "n" and cannot be altered in postgres 9.4
	if (table.ReplicaIdentity != "") && (table.ForeignDef == ForeignTableDefinition{}) {
		switch table.ReplicaIdentity {
		case "d", "i":
			// default values do not need to be written ; index values are handled when the index is created
			break
		case "n":
			statements = append(statements, fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY NOTHING;", table.FQN()))
		case "f":
			statements = append(statements, fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL;", table.FQN()))
		}
	}

	for _, alteredPartitionRelation := range table.PartitionAlteredSchemas {
		statements = append(statements,
			fmt.Sprintf("ALTER TABLE %s SET SCHEMA %s;",
				utils.MakeFQN(alteredPartitionRelation.OldSchema, alteredPartitionRelation.Name), alteredPartitionRelation.NewSchema))
	}

	if connectionPool.Version.AtLeast("7") {
		attachInfo := table.AttachPartitionInfo
		if (attachInfo != AttachPartitionInfo{}) {
			statements = append(statements,
				fmt.Sprintf("ALTER TABLE ONLY %s ATTACH PARTITION %s %s;", table.Inherits[0], attachInfo.Relname, attachInfo.Expr))
		}

		if table.ForceRowSecurity {
			statements = append(statements, fmt.Sprintf("ALTER TABLE ONLY %s FORCE ROW LEVEL SECURITY;", table.FQN()))
		}
	}

	PrintStatements(metadataFile, toc, table, statements)
}

func generateSequenceDefinitionStatement(sequence Sequence) string {
	statement := ""
	definition := sequence.Definition
	maxVal := int64(math.MaxInt64)
	minVal := int64(math.MinInt64)

	// Identity columns cannot be defined with `AS smallint/integer`
	if connectionPool.Version.AtLeast("7") && sequence.OwningColumnAttIdentity == "" {
		if definition.Type != "bigint" {
			statement += fmt.Sprintf("\n\tAS %s", definition.Type)
		}
		if definition.Type == "smallint" {
			maxVal = int64(math.MaxInt16)
			minVal = int64(math.MinInt16)
		} else if definition.Type == "integer" {
			maxVal = int64(math.MaxInt32)
			minVal = int64(math.MinInt32)
		}
	}
	if connectionPool.Version.AtLeast("6") {
		statement += fmt.Sprintf("\n\tSTART WITH %d", definition.StartVal)
	} else if !definition.IsCalled {
		statement += fmt.Sprintf("\n\tSTART WITH %d", definition.LastVal)
	}
	statement += fmt.Sprintf("\n\tINCREMENT BY %d", definition.Increment)

	if !((definition.MaxVal == maxVal && definition.Increment > 0) ||
		(definition.MaxVal == -1 && definition.Increment < 0)) {
		statement += fmt.Sprintf("\n\tMAXVALUE %d", definition.MaxVal)
	} else {
		statement += "\n\tNO MAXVALUE"
	}
	if !((definition.MinVal == minVal && definition.Increment < 0) ||
		(definition.MinVal == 1 && definition.Increment > 0)) {
		statement += fmt.Sprintf("\n\tMINVALUE %d", definition.MinVal)
	} else {
		statement += "\n\tNO MINVALUE"
	}
	statement += fmt.Sprintf("\n\tCACHE %d", definition.CacheVal)
	if definition.IsCycled {
		statement += "\n\tCYCLE"
	}
	return statement
}

func PrintIdentityColumns(metadataFile *utils.FileWithByteCount, toc *toc.TOC, sequences []Sequence) {
	for _, seq := range sequences {
		if seq.IsIdentity {
			start := metadataFile.ByteCount

			attrIdentityStr := ""
			if seq.OwningColumnAttIdentity == "a" {
				attrIdentityStr = "ALWAYS"
			} else if seq.OwningColumnAttIdentity == "d" {
				attrIdentityStr = "BY DEFAULT"
			} else {
				gplog.Fatal(errors.Errorf("Invalid Owning Column Attribute came for Identity sequence: expected 'a' or 'd', got '%s'\n", seq.OwningColumnAttIdentity), "")
			}

			metadataFile.MustPrintf("ALTER TABLE %s\nALTER COLUMN %s ADD GENERATED %s AS IDENTITY (",
				seq.OwningTable, seq.UnqualifiedOwningColumn, attrIdentityStr)
			metadataFile.MustPrintf("\n\tSEQUENCE NAME %s", seq.FQN())
			seqDefStatement := generateSequenceDefinitionStatement(seq)
			metadataFile.MustPrintf("%s);\n", seqDefStatement)
			section, entry := seq.GetMetadataEntry()
			toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
		}
	}
}

/*
 * This function is largely derived from the dumpSequence() function in pg_dump.c.  The values of
 * minVal and maxVal come from SEQ_MINVALUE and SEQ_MAXVALUE, defined in include/commands/sequence.h.
 */
func PrintCreateSequenceStatements(metadataFile *utils.FileWithByteCount,
	toc *toc.TOC, sequences []Sequence, sequenceMetadata MetadataMap) {
	for _, sequence := range sequences {
		if sequence.IsIdentity {
			continue
		}
		start := metadataFile.ByteCount
		definition := sequence.Definition
		metadataFile.MustPrintf("\n\nCREATE SEQUENCE %s", sequence.FQN())
		seqDefStatement := generateSequenceDefinitionStatement(sequence)
		metadataFile.MustPrint(seqDefStatement + ";")

		metadataFile.MustPrintf("\n\nSELECT pg_catalog.setval('%s', %d, %v);\n",
			utils.EscapeSingleQuotes(sequence.FQN()), definition.LastVal, definition.IsCalled)

		section, entry := sequence.GetMetadataEntry()
		toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
		PrintObjectMetadata(metadataFile, toc, sequenceMetadata[sequence.Relation.GetUniqueID()], sequence, "")
	}
}

func PrintAlterSequenceStatements(metadataFile *utils.FileWithByteCount,
	tocfile *toc.TOC, sequences []Sequence) {
	gplog.Verbose("Writing ALTER SEQUENCE statements to metadata file")
	for _, sequence := range sequences {
		if sequence.IsIdentity {
			continue
		}
		seqFQN := sequence.FQN()
		// owningColumn is quoted and doesn't need to be quoted again
		if sequence.OwningColumn != "" {
			start := metadataFile.ByteCount
			metadataFile.MustPrintf("\n\nALTER SEQUENCE %s OWNED BY %s;\n", seqFQN, sequence.OwningColumn)
			entry := toc.MetadataEntry{
				Schema:          sequence.Relation.Schema,
				Name:            sequence.Relation.Name,
				ObjectType:      "SEQUENCE OWNER",
				ReferenceObject: sequence.OwningTable,
			}
			tocfile.AddMetadataEntry("predata", entry, start, metadataFile.ByteCount)
		}
	}
}

// A view's column names are automatically factored into it's definition.
func PrintCreateViewStatement(metadataFile *utils.FileWithByteCount, toc *toc.TOC, view View, viewMetadata ObjectMetadata) {
	start := metadataFile.ByteCount
	var tablespaceClause string
	if view.Tablespace != "" {
		tablespaceClause = fmt.Sprintf(" TABLESPACE %s", view.Tablespace)
	}
	// Option's keyword WITH is expected to be prepended to its options in the SQL statement
	// Remove trailing ';' at the end of materialized view's definition
	if !view.IsMaterialized {
		metadataFile.MustPrintf("\n\nCREATE VIEW %s%s AS %s\n", view.FQN(), view.Options, view.Definition.String)
	} else {
		metadataFile.MustPrintf("\n\nCREATE MATERIALIZED VIEW %s%s%s AS %s\nWITH NO DATA\n%s;\n",
			view.FQN(), view.Options, tablespaceClause, view.Definition.String[:len(view.Definition.String)-1], view.DistPolicy)
	}
	section, entry := view.GetMetadataEntry()
	toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
	PrintObjectMetadata(metadataFile, toc, viewMetadata, view, "")
}

func ExpandIncludesForPartitions(conn *dbconn.DBConn, opts *options.Options, includeOids []string, flags *pflag.FlagSet) error {
	if len(opts.GetIncludedTables()) == 0 {
		return nil
	}

	allRelStructs, err := opts.GetUserTableRelationsWithIncludeFiltering(conn, includeOids, options.MustGetFlagBool(flags, options.NO_INHERITS))
	if err != nil {
		return err
	}

	includeSet := map[string]bool{}
	for _, oid := range includeOids {
		includeSet[oid] = true
	}

	allRelSet := map[string]options.Relation{}
	for _, rel := range allRelStructs {
		oidStr := strconv.FormatUint(uint64(rel.Oid), 10)
		allRelSet[oidStr] = rel
	}

	// set arithmetic: find difference
	diff := make([]options.Relation, 0)
	for oid, rel := range allRelSet {
		_, oidExists := includeSet[oid]
		if !oidExists {
			diff = append(diff, rel)
		}
	}

	if len(diff) > 0 {
		gplog.Info("The filtered table set has been expanded to include additional dependent tables; see the log file for a full list of tables that have been added")
	}
	for _, rel := range diff {
		fqn := fmt.Sprintf("%s.%s", utils.UnEscapeDoubleQuotes(rel.Schema), utils.UnEscapeDoubleQuotes(rel.Name))
		err = flags.Set(options.INCLUDE_RELATION, fqn)
		if err != nil {
			return err
		}
		opts.AddIncludedRelation(fqn)
		AddIncludedRelationFqn(rel)
		gplog.Verbose("Added %s to the backup set", fqn)
	}

	return nil
}
