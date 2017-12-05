package backup

/*
 * This file contains structs and functions related to backing up relation
 * (sequence, table, and view) metadata on the master.
 */

import (
	"fmt"
	"strings"

	"github.com/greenplum-db/gpbackup/utils"
)

type Relation struct {
	SchemaOid   uint32
	Oid         uint32
	Schema      string
	Name        string
	DependsUpon []string // Used for dependency sorting
	Inherits    []string // Only used for printing INHERITS statement
}

/*
 * This function prints a table in fully-qualified schema.table format, with
 * everything quoted and escaped appropriately.
 */
func (t Relation) ToString() string {
	return utils.MakeFQN(t.Schema, t.Name)
}

func BasicRelation(schema string, relation string) Relation {
	return Relation{
		SchemaOid: 0,
		Schema:    schema,
		Oid:       0,
		Name:      relation,
	}
}

/*
 * Given a list of Relations, this function returns a sorted list of their Schemas.
 * It assumes that the Relation list is sorted by schema and then by table, so it
 * doesn't need to do any sorting itself.
 */
func GetUniqueSchemas(schemas []Schema, tables []Relation) []Schema {
	currentSchemaOid := uint32(0)
	uniqueSchemas := make([]Schema, 0)
	schemaMap := make(map[uint32]Schema, 0)
	for _, schema := range schemas {
		schemaMap[schema.Oid] = schema
	}
	for _, table := range tables {
		if table.SchemaOid != currentSchemaOid {
			currentSchemaOid = table.SchemaOid
			uniqueSchemas = append(uniqueSchemas, schemaMap[currentSchemaOid])
		}
	}
	return uniqueSchemas
}

/*
 * When leafPartitionData is set, for partition tables we want to print metadata
 * for the parent tables and data for the leaf tables, so we split them into
 * separate lists.  Intermediate tables are skipped, and non-partition tables are
 * backed up normally (both metadata and data).
 *
 * When the flag is not set, we want to back up both metadata and data for all
 * tables, so both returned arrays contain all tables.
 */
func SplitTablesByPartitionType(tables []Relation, tableDefs map[uint32]TableDefinition, includeList []string) ([]Relation, []Relation) {
	metadataTables := make([]Relation, 0)
	dataTables := make([]Relation, 0)
	if *leafPartitionData || len(includeList) > 0 {
		includeSet := utils.NewIncludeSet(includeList)
		for _, table := range tables {
			if tableDefs[table.Oid].IsExternal && tableDefs[table.Oid].PartitionType == "l" {
				table.Name = AppendExtPartSuffix(table.Name)
				metadataTables = append(metadataTables, table)
			}
			partType := tableDefs[table.Oid].PartitionType
			if partType != "l" && partType != "i" {
				metadataTables = append(metadataTables, table)
			}
			if *leafPartitionData {
				if partType != "p" && partType != "i" {
					dataTables = append(dataTables, table)
				}
			} else if len(includeList) > 0 {
				if includeSet.MatchesFilter(table.ToString()) {
					dataTables = append(dataTables, table)
				}
			}
		}
	} else {
		for _, table := range tables {
			if tableDefs[table.Oid].IsExternal && tableDefs[table.Oid].PartitionType == "l" {
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
	const MAX_LEN = 63 // MAX_DATA_LEN - 1 is the maximum length of a relation name
	if name[len(name)-1] == '"' {
		if len(name)+len(SUFFIX) > MAX_LEN+2 {
			return name[0:MAX_LEN+2-len(SUFFIX)] + SUFFIX + `"`
		}
		return name[0:len(name)-1] + SUFFIX + `"`
	}
	if len(name)+len(SUFFIX) > MAX_LEN {
		return name[0:MAX_LEN+1-len(SUFFIX)] + SUFFIX
	}
	return name + SUFFIX
}

type TableDefinition struct {
	DistPolicy      string
	PartDef         string
	PartTemplateDef string
	StorageOpts     string
	TablespaceName  string
	ColumnDefs      []ColumnDefinition
	IsExternal      bool
	ExtTableDef     ExternalTableDefinition
	PartitionType   string
}

/*
 * This function calls all the functions needed to gather the metadata for a
 * single table and assembles the metadata into ColumnDef and TableDef structs
 * for more convenient handling in the PrintCreateTableStatement() function.
 */
func ConstructDefinitionsForTables(connection *utils.DBConn, tables []Relation) map[uint32]TableDefinition {
	tableDefinitionMap := make(map[uint32]TableDefinition, 0)

	logger.Info("Gathering additional table metadata")
	logger.Verbose("Retrieving column information")
	columnDefs := GetColumnDefinitions(connection)
	distributionPolicies := GetDistributionPolicies(connection, tables)
	logger.Verbose("Retrieving partition information")
	partitionDefs := GetPartitionDefinitions(connection)
	partTemplateDefs := GetPartitionTemplates(connection)
	logger.Verbose("Retrieving storage information")
	storageOptions := GetStorageOptions(connection)
	tablespaceNames := GetTablespaceNames(connection)
	logger.Verbose("Retrieving external table information")
	extTableDefs := GetExternalTableDefinitions(connection)
	partTableMap := GetPartitionTableMap(connection)

	logger.Verbose("Constructing table definition map")
	for _, table := range tables {
		oid := table.Oid
		tableDef := TableDefinition{
			distributionPolicies[oid],
			partitionDefs[oid],
			partTemplateDefs[oid],
			storageOptions[oid],
			tablespaceNames[oid],
			columnDefs[oid],
			(extTableDefs[oid].Oid != 0),
			extTableDefs[oid],
			partTableMap[oid],
		}
		tableDefinitionMap[oid] = tableDef
	}
	return tableDefinitionMap
}

/*
 * This function prints CREATE TABLE statements in a format very similar to pg_dump.  Unlike pg_dump,
 * however, table names are printed fully qualified with their schemas instead of relying on setting
 * the search_path; this will aid in later filtering to include or exclude certain tables during the
 * backup process, and allows customers to copy just the CREATE TABLE block in order to use it directly.
 */
func PrintCreateTableStatement(predataFile *utils.FileWithByteCount, toc *utils.TOC, table Relation, tableDef TableDefinition, tableMetadata ObjectMetadata) {
	start := predataFile.ByteCount
	// We use an empty TOC below to keep count of the bytes for testing purposes.
	if tableDef.IsExternal && tableDef.PartitionType != "p" {
		PrintExternalTableCreateStatement(predataFile, nil, table, tableDef)
	} else {
		PrintRegularTableCreateStatement(predataFile, nil, table, tableDef)
	}
	PrintPostCreateTableStatements(predataFile, table, tableDef, tableMetadata)
	toc.AddMetadataEntry(table.Schema, table.Name, "TABLE", start, predataFile)
}

func PrintRegularTableCreateStatement(predataFile *utils.FileWithByteCount, toc *utils.TOC, table Relation, tableDef TableDefinition) {
	start := predataFile.ByteCount
	predataFile.MustPrintf("\n\nCREATE TABLE %s (\n", table.ToString())
	printColumnDefinitions(predataFile, tableDef.ColumnDefs)
	predataFile.MustPrintf(") ")
	if len(table.Inherits) != 0 {
		dependencyList := strings.Join(table.Inherits, ", ")
		predataFile.MustPrintf("INHERITS (%s) ", dependencyList)
	}
	if tableDef.StorageOpts != "" {
		predataFile.MustPrintf("WITH (%s) ", tableDef.StorageOpts)
	}
	if tableDef.TablespaceName != "" {
		predataFile.MustPrintf("TABLESPACE %s ", tableDef.TablespaceName)
	}
	predataFile.MustPrintf("%s", tableDef.DistPolicy)
	if tableDef.PartDef != "" {
		predataFile.MustPrintf(" %s", strings.TrimSpace(tableDef.PartDef))
	}
	predataFile.MustPrintln(";")
	if tableDef.PartTemplateDef != "" {
		predataFile.MustPrintf("%s;\n", strings.TrimSpace(tableDef.PartTemplateDef))
	}
	printAlterColumnStatements(predataFile, table, tableDef.ColumnDefs)
	if toc != nil {
		toc.AddMetadataEntry(table.Schema, table.Name, "TABLE", start, predataFile)
	}
}

func printColumnDefinitions(predataFile *utils.FileWithByteCount, columnDefs []ColumnDefinition) {
	lines := make([]string, 0)
	for _, column := range columnDefs {
		line := fmt.Sprintf("\t%s %s", column.Name, column.Type)
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
		predataFile.MustPrintln(strings.Join(lines, ",\n"))
	}
}

func printAlterColumnStatements(predataFile *utils.FileWithByteCount, table Relation, columnDefs []ColumnDefinition) {
	for _, column := range columnDefs {
		if column.StatTarget > -1 {
			predataFile.MustPrintf("\nALTER TABLE ONLY %s ALTER COLUMN %s SET STATISTICS %d;", table.ToString(), column.Name, column.StatTarget)
		}
		if column.StorageType != "" {
			predataFile.MustPrintf("\nALTER TABLE ONLY %s ALTER COLUMN %s SET STORAGE %s;", table.ToString(), column.Name, column.StorageType)
		}
	}
}

/*
 * This function prints additional statements that come after the CREATE TABLE
 * statement for both regular and external tables.
 */
func PrintPostCreateTableStatements(predataFile *utils.FileWithByteCount, table Relation, tableDef TableDefinition, tableMetadata ObjectMetadata) {
	PrintObjectMetadata(predataFile, tableMetadata, table.ToString(), "TABLE")

	for _, att := range tableDef.ColumnDefs {
		if att.Comment != "" {
			predataFile.MustPrintf("\n\nCOMMENT ON COLUMN %s.%s IS '%s';\n", table.ToString(), att.Name, att.Comment)
		}
	}
}

type Sequence struct {
	Relation
	SequenceDefinition
}

func GetAllSequences(connection *utils.DBConn) []Sequence {
	sequenceRelations := GetAllSequenceRelations(connection)
	sequences := make([]Sequence, 0)
	for _, seqRelation := range sequenceRelations {
		seqDef := GetSequenceDefinition(connection, seqRelation.ToString())
		sequence := Sequence{seqRelation, seqDef}
		sequences = append(sequences, sequence)
	}
	return sequences
}

/*
 * This function is largely derived from the dumpSequence() function in pg_dump.c.  The values of
 * minVal and maxVal come from SEQ_MINVALUE and SEQ_MAXVALUE, defined in include/commands/sequence.h.
 */
func PrintCreateSequenceStatements(predataFile *utils.FileWithByteCount, toc *utils.TOC, sequences []Sequence, sequenceMetadata MetadataMap) {
	maxVal := int64(9223372036854775807)
	minVal := int64(-9223372036854775807)
	for _, sequence := range sequences {
		start := predataFile.ByteCount
		seqFQN := sequence.ToString()
		predataFile.MustPrintln("\n\nCREATE SEQUENCE", seqFQN)
		if connection.Version.AtLeast("6") {
			predataFile.MustPrintln("\tSTART WITH", sequence.StartVal)
		} else if !sequence.IsCalled {
			predataFile.MustPrintln("\tSTART WITH", sequence.LastVal)
		}
		predataFile.MustPrintln("\tINCREMENT BY", sequence.Increment)

		if !((sequence.MaxVal == maxVal && sequence.Increment > 0) || (sequence.MaxVal == -1 && sequence.Increment < 0)) {
			predataFile.MustPrintln("\tMAXVALUE", sequence.MaxVal)
		} else {
			predataFile.MustPrintln("\tNO MAXVALUE")
		}
		if !((sequence.MinVal == minVal && sequence.Increment < 0) || (sequence.MinVal == 1 && sequence.Increment > 0)) {
			predataFile.MustPrintln("\tMINVALUE", sequence.MinVal)
		} else {
			predataFile.MustPrintln("\tNO MINVALUE")
		}
		cycleStr := ""
		if sequence.IsCycled {
			cycleStr = "\n\tCYCLE"
		}
		predataFile.MustPrintf("\tCACHE %d%s;", sequence.CacheVal, cycleStr)

		predataFile.MustPrintf("\n\nSELECT pg_catalog.setval('%s', %d, %v);\n", seqFQN, sequence.LastVal, sequence.IsCalled)

		PrintObjectMetadata(predataFile, sequenceMetadata[sequence.Oid], seqFQN, "SEQUENCE")
		toc.AddMetadataEntry(sequence.Relation.Schema, sequence.Relation.Name, "SEQUENCE", start, predataFile)
	}
}

func PrintAlterSequenceStatements(predataFile *utils.FileWithByteCount, toc *utils.TOC, sequences []Sequence, sequenceColumnOwners map[string]string) {
	for _, sequence := range sequences {
		seqFQN := sequence.ToString()
		// owningColumn is quoted when the map is constructed in GetSequenceColumnOwnerMap() and doesn't need to be quoted again
		if owningColumn, hasColumnOwner := sequenceColumnOwners[seqFQN]; hasColumnOwner {
			start := predataFile.ByteCount
			predataFile.MustPrintf("\n\nALTER SEQUENCE %s OWNED BY %s;\n", seqFQN, owningColumn)
			toc.AddMetadataEntry(sequence.Relation.Schema, sequence.Relation.Name, "SEQUENCE OWNER", start, predataFile)
		}
	}
}

func PrintCreateViewStatements(predataFile *utils.FileWithByteCount, toc *utils.TOC, views []View, viewMetadata MetadataMap) {
	for _, view := range views {
		start := predataFile.ByteCount
		viewFQN := utils.MakeFQN(view.Schema, view.Name)
		predataFile.MustPrintf("\n\nCREATE VIEW %s AS %s\n", viewFQN, view.Definition)
		PrintObjectMetadata(predataFile, viewMetadata[view.Oid], viewFQN, "VIEW")
		toc.AddMetadataEntry(view.Schema, view.Name, "VIEW", start, predataFile)
	}
}
