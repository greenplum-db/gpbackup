package backup

/*
 * This file contains structs and functions related to dumping regular table
 * (non-external table) metadata on the master.
 */

import (
	"fmt"
	"io"
	"strings"

	"github.com/greenplum-db/gpbackup/utils"
)

type ColumnDefinition struct {
	Num        int
	Name       string
	NotNull    bool
	HasDefault bool
	IsDropped  bool
	TypName    string
	Encoding   string
	Comment    string
	DefaultVal string
}

type TableDefinition struct {
	DistPolicy      string
	PartDef         string
	PartTemplateDef string
	StorageOpts     string
	ColumnDefs      []ColumnDefinition
	IsExternal      bool
	ExtTableDef     ExternalTableDefinition
}

/*
 * This function calls all the functions needed to gather the metadata for a
 * single table and assembles the metadata into ColumnDef and TableDef structs
 * for more convenient handling in the PrintCreateTableStatement() function.
 */
func ConstructDefinitionsForTable(connection *utils.DBConn, table utils.Relation, isExternal bool) TableDefinition {
	tableAttributes := GetTableAttributes(connection, table.RelationOid)
	tableDefaults := GetTableDefaults(connection, table.RelationOid)

	distributionPolicy := GetDistributionPolicy(connection, table.RelationOid)
	partitionDef := GetPartitionDefinition(connection, table.RelationOid)
	partTemplateDef := GetPartitionTemplateDefinition(connection, table.RelationOid)
	storageOptions := GetStorageOptions(connection, table.RelationOid)

	columnDefs := ConsolidateColumnInfo(tableAttributes, tableDefaults)
	var extTableDef ExternalTableDefinition
	if isExternal {
		extTableDef = GetExternalTableDefinition(connection, table.RelationOid)
	}
	tableDef := TableDefinition{distributionPolicy, partitionDef, partTemplateDef, storageOptions, columnDefs, isExternal, extTableDef}
	return tableDef
}

/*
 * This function zips up table attributes and column default information into
 * one struct, instead of performing an expensive join to get everything in
 * a single query.
 */
func ConsolidateColumnInfo(atts []QueryTableAtts, defs []QueryTableDefault) []ColumnDefinition {
	colDefs := make([]ColumnDefinition, 0)
	/*
	 * The queries to get attributes and defaults ORDER BY oid and then attribute
	 * number, so we can assume the arrays are in the same order without sorting.
	 */
	j := 0
	for i := range atts {
		defaultVal := ""
		if atts[i].AttHasDefault {
			for j < len(defs) {
				if atts[i].AttNum == defs[j].AdNum {
					defaultVal = defs[j].DefaultVal
					break
				}
				j++
			}
		}
		colDef := ColumnDefinition{
			Num:        atts[i].AttNum,
			Name:       atts[i].AttName,
			NotNull:    atts[i].AttNotNull,
			HasDefault: atts[i].AttHasDefault,
			IsDropped:  atts[i].AttIsDropped,
			TypName:    atts[i].AttTypName,
			Encoding:   atts[i].AttEncoding,
			Comment:    atts[i].AttComment,
			DefaultVal: defaultVal,
		}
		colDefs = append(colDefs, colDef)
	}
	return colDefs
}

/*
 * This function prints CREATE TABLE statements in a format very similar to pg_dump.  Unlike pg_dump,
 * however, table names are printed fully qualified with their schemas instead of relying on setting
 * the search_path; this will aid in later filtering to include or exclude certain tables during the
 * backup process, and allows customers to copy just the CREATE TABLE block in order to use it directly.
 */
func PrintCreateTableStatement(predataFile io.Writer, table utils.Relation, tableDef TableDefinition) {
	if tableDef.IsExternal {
		PrintExternalTableCreateStatement(predataFile, table, tableDef)
	} else {
		PrintRegularTableCreateStatement(predataFile, table, tableDef)
	}
	PrintPostCreateTableStatements(predataFile, table, tableDef)
}

func PrintRegularTableCreateStatement(predataFile io.Writer, table utils.Relation, tableDef TableDefinition) {
	utils.MustPrintf(predataFile, "\n\nCREATE TABLE %s (\n", table.ToString())
	printColumnStatements(predataFile, table, tableDef.ColumnDefs)
	utils.MustPrintf(predataFile, ") ")
	if tableDef.StorageOpts != "" {
		utils.MustPrintf(predataFile, "WITH (%s) ", tableDef.StorageOpts)
	}
	utils.MustPrintf(predataFile, "%s", tableDef.DistPolicy)
	if tableDef.PartDef != "" {
		utils.MustPrintf(predataFile, " %s", strings.TrimSpace(tableDef.PartDef))
	}
	utils.MustPrintln(predataFile, ";")
	if tableDef.PartTemplateDef != "" {
		utils.MustPrintf(predataFile, "%s;\n", strings.TrimSpace(tableDef.PartTemplateDef))
	}
}

func printColumnStatements(predataFile io.Writer, table utils.Relation, columnDefs []ColumnDefinition) {
	lines := make([]string, 0)
	for _, column := range columnDefs {
		if !column.IsDropped {
			line := fmt.Sprintf("\t%s %s", utils.QuoteIdent(column.Name), column.TypName)
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
	}
	if len(lines) > 0 {
		utils.MustPrintln(predataFile, strings.Join(lines, ",\n"))
	}
}

/*
 * This function prints additional statements that come after the CREATE TABLE
 * statement for both regular and external tables.
 */
func PrintPostCreateTableStatements(predataFile io.Writer, table utils.Relation, tableDef TableDefinition) {
	if table.Comment != "" {
		utils.MustPrintf(predataFile, "\n\nCOMMENT ON TABLE %s IS '%s';\n", table.ToString(), table.Comment)
	}
	if table.Owner != "" {
		utils.MustPrintf(predataFile, "\n\nALTER TABLE %s OWNER TO %s;\n", table.ToString(), utils.QuoteIdent(table.Owner))
	}

	for _, att := range tableDef.ColumnDefs {
		if att.Comment != "" {
			utils.MustPrintf(predataFile, "\n\nCOMMENT ON COLUMN %s.%s IS '%s';\n", table.ToString(), att.Name, att.Comment)
		}
	}
}
