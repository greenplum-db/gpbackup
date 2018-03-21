package backup

/*
 * This file contains structs and functions related to backing up relation
 * (sequence, table, and view) metadata on the master.
 */

import (
	"fmt"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
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
	TableType       string
}

/*
 * This function calls all the functions needed to gather the metadata for a
 * single table and assembles the metadata into ColumnDef and TableDef structs
 * for more convenient handling in the PrintCreateTableStatement() function.
 */
func ConstructDefinitionsForTables(connection *dbconn.DBConn, tables []Relation) map[uint32]TableDefinition {
	tableDefinitionMap := make(map[uint32]TableDefinition, 0)

	gplog.Info("Gathering additional table metadata")
	gplog.Verbose("Retrieving column information")
	columnMetadata := GetPrivilegesForColumns(connection)
	columnDefs := GetColumnDefinitions(connection, columnMetadata)
	distributionPolicies := GetDistributionPolicies(connection, tables)
	gplog.Verbose("Retrieving partition information")
	partitionDefs := GetPartitionDefinitions(connection)
	partTemplateDefs := GetPartitionTemplates(connection)
	gplog.Verbose("Retrieving storage information")
	tableStorageOptions := GetTableStorageOptions(connection)
	tablespaceNames := GetTablespaceNames(connection)
	gplog.Verbose("Retrieving external table information")
	extTableDefs := GetExternalTableDefinitions(connection)
	partTableMap := GetPartitionTableMap(connection)
	tableTypeMap := GetTableType(connection)

	gplog.Verbose("Constructing table definition map")
	for _, table := range tables {
		oid := table.Oid
		tableDef := TableDefinition{
			distributionPolicies[oid],
			partitionDefs[oid],
			partTemplateDefs[oid],
			tableStorageOptions[oid],
			tablespaceNames[oid],
			columnDefs[oid],
			(extTableDefs[oid].Oid != 0),
			extTableDefs[oid],
			partTableMap[oid],
			tableTypeMap[oid],
		}
		tableDefinitionMap[oid] = tableDef
	}
	return tableDefinitionMap
}

func ConstructColumnPrivilegesMap(results []ColumnPrivilegesQueryStruct) map[uint32]map[string][]ACL {
	metadataMap := make(map[uint32]map[string][]ACL)
	var tableMetadata map[string][]ACL
	var columnMetadata []ACL
	if len(results) > 0 {
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
			privileges := ParseACL(privilegesStr)
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
func PrintCreateTableStatement(metadataFile *utils.FileWithByteCount, toc *utils.TOC, table Relation, tableDef TableDefinition, tableMetadata ObjectMetadata) {
	start := metadataFile.ByteCount
	// We use an empty TOC below to keep count of the bytes for testing purposes.
	if tableDef.IsExternal && tableDef.PartitionType != "p" {
		PrintExternalTableCreateStatement(metadataFile, nil, table, tableDef)
	} else {
		PrintRegularTableCreateStatement(metadataFile, nil, table, tableDef)
	}
	PrintPostCreateTableStatements(metadataFile, table, tableDef, tableMetadata)
	toc.AddPredataEntry(table.Schema, table.Name, "TABLE", "", start, metadataFile)
}

func PrintRegularTableCreateStatement(metadataFile *utils.FileWithByteCount, toc *utils.TOC, table Relation, tableDef TableDefinition) {
	start := metadataFile.ByteCount

	typeStr := ""
	if tableDef.TableType != "" {
		typeStr = fmt.Sprintf("OF %s ", tableDef.TableType)
	}

	metadataFile.MustPrintf("\n\nCREATE TABLE %s %s(\n", table.ToString(), typeStr)

	printColumnDefinitions(metadataFile, tableDef.ColumnDefs, tableDef.TableType)
	metadataFile.MustPrintf(") ")
	if len(table.Inherits) != 0 {
		dependencyList := strings.Join(table.Inherits, ", ")
		metadataFile.MustPrintf("INHERITS (%s) ", dependencyList)
	}
	if tableDef.StorageOpts != "" {
		metadataFile.MustPrintf("WITH (%s) ", tableDef.StorageOpts)
	}
	if tableDef.TablespaceName != "" {
		metadataFile.MustPrintf("TABLESPACE %s ", tableDef.TablespaceName)
	}
	metadataFile.MustPrintf("%s", tableDef.DistPolicy)
	if tableDef.PartDef != "" {
		metadataFile.MustPrintf(" %s", strings.TrimSpace(tableDef.PartDef))
	}
	metadataFile.MustPrintln(";")
	if tableDef.PartTemplateDef != "" {
		metadataFile.MustPrintf("%s;\n", strings.TrimSpace(tableDef.PartTemplateDef))
	}
	printAlterColumnStatements(metadataFile, table, tableDef.ColumnDefs)
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

func printAlterColumnStatements(metadataFile *utils.FileWithByteCount, table Relation, columnDefs []ColumnDefinition) {
	for _, column := range columnDefs {
		if column.StatTarget > -1 {
			metadataFile.MustPrintf("\nALTER TABLE ONLY %s ALTER COLUMN %s SET STATISTICS %d;", table.ToString(), column.Name, column.StatTarget)
		}
		if column.StorageType != "" {
			metadataFile.MustPrintf("\nALTER TABLE ONLY %s ALTER COLUMN %s SET STORAGE %s;", table.ToString(), column.Name, column.StorageType)
		}
	}
}

/*
 * This function prints additional statements that come after the CREATE TABLE
 * statement for both regular and external tables.
 */
func PrintPostCreateTableStatements(metadataFile *utils.FileWithByteCount, table Relation, tableDef TableDefinition, tableMetadata ObjectMetadata) {
	PrintObjectMetadata(metadataFile, tableMetadata, table.ToString(), "TABLE")

	for _, att := range tableDef.ColumnDefs {
		if att.Comment != "" {
			escapedComment := strings.Replace(att.Comment, "'", "''", -1)
			metadataFile.MustPrintf("\n\nCOMMENT ON COLUMN %s.%s IS '%s';\n", table.ToString(), att.Name, escapedComment)
		}
		if len(att.ACL) > 0 {
			columnMetadata := ObjectMetadata{Privileges: att.ACL, Owner: tableMetadata.Owner}
			columnPrivileges := columnMetadata.GetPrivilegesStatements(table.ToString(), "COLUMN", att.Name)
			metadataFile.MustPrintln(columnPrivileges)
		}
	}
}

type Sequence struct {
	Relation
	SequenceDefinition
}

func GetAllSequences(connection *dbconn.DBConn) []Sequence {
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
func PrintCreateSequenceStatements(metadataFile *utils.FileWithByteCount, toc *utils.TOC, sequences []Sequence, sequenceMetadata MetadataMap) {
	maxVal := int64(9223372036854775807)
	minVal := int64(-9223372036854775807)
	for _, sequence := range sequences {
		start := metadataFile.ByteCount
		seqFQN := sequence.ToString()
		metadataFile.MustPrintln("\n\nCREATE SEQUENCE", seqFQN)
		if connection.Version.AtLeast("6") {
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

		metadataFile.MustPrintf("\n\nSELECT pg_catalog.setval('%s', %d, %v);\n", seqFQN, sequence.LastVal, sequence.IsCalled)

		PrintObjectMetadata(metadataFile, sequenceMetadata[sequence.Oid], seqFQN, "SEQUENCE")
		toc.AddPredataEntry(sequence.Relation.Schema, sequence.Relation.Name, "SEQUENCE", "", start, metadataFile)
	}
}

func PrintAlterSequenceStatements(metadataFile *utils.FileWithByteCount, toc *utils.TOC, sequences []Sequence, sequenceColumnOwners map[string]string) {
	for _, sequence := range sequences {
		seqFQN := sequence.ToString()
		// owningColumn is quoted when the map is constructed in GetSequenceColumnOwnerMap() and doesn't need to be quoted again
		if owningColumn, hasColumnOwner := sequenceColumnOwners[seqFQN]; hasColumnOwner {
			start := metadataFile.ByteCount
			metadataFile.MustPrintf("\n\nALTER SEQUENCE %s OWNED BY %s;\n", seqFQN, owningColumn)
			toc.AddPredataEntry(sequence.Relation.Schema, sequence.Relation.Name, "SEQUENCE OWNER", "", start, metadataFile)
		}
	}
}

func PrintCreateViewStatements(metadataFile *utils.FileWithByteCount, toc *utils.TOC, views []View, viewMetadata MetadataMap) {
	for _, view := range views {
		start := metadataFile.ByteCount
		viewFQN := utils.MakeFQN(view.Schema, view.Name)
		metadataFile.MustPrintf("\n\nCREATE VIEW %s AS %s\n", viewFQN, view.Definition)
		PrintObjectMetadata(metadataFile, viewMetadata[view.Oid], viewFQN, "VIEW")
		toc.AddPredataEntry(view.Schema, view.Name, "VIEW", "", start, metadataFile)
	}
}
