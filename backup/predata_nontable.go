package backup

/*
 * This file contains structs and functions related to dumping non-table-related
 * metadata on the master that needs to be restored before data is restored, such
 * as sequences and check constraints.
 */

import (
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/greenplum-db/gpbackup/utils"
)

type SequenceDefinition struct {
	utils.Relation
	QuerySequenceDefinition
}

/*
 * Functions to print to the predata file
 */

/*
 * This function calls per-table functions to get constraints related to each
 * table, then consolidates them in two slices holding all constraints for all
 * tables.  Two slices are needed because FOREIGN KEY constraints must be dumped
 * after PRIMARY KEY constraints, so they're separated out to be handled last.
 */
func ConstructConstraintsForAllTables(connection *utils.DBConn, tables []utils.Relation) ([]string, []string) {
	allConstraints := make([]string, 0)
	allFkConstraints := make([]string, 0)
	for _, table := range tables {
		constraintList := GetConstraints(connection, table.RelationOid)
		tableConstraints, tableFkConstraints := ProcessConstraints(table, constraintList)
		allConstraints = append(allConstraints, tableConstraints...)
		allFkConstraints = append(allFkConstraints, tableFkConstraints...)
	}
	return allConstraints, allFkConstraints
}

/*
 * There's no built-in function to generate constraint definitions like there is for other types of
 * metadata, so this function constructs them.
 */
func ProcessConstraints(table utils.Relation, constraints []QueryConstraint) ([]string, []string) {
	alterStr := fmt.Sprintf("\n\nALTER TABLE ONLY %s ADD CONSTRAINT %s %s;", table.ToString(), "%s", "%s")
	commentStr := fmt.Sprintf("\n\nCOMMENT ON CONSTRAINT %s ON %s IS '%s';", "%s", table.ToString(), "%s")
	cons := make([]string, 0)
	fkCons := make([]string, 0)
	for _, constraint := range constraints {
		conStr := fmt.Sprintf(alterStr, constraint.ConName, constraint.ConDef)
		if constraint.ConComment != "" {
			conStr += fmt.Sprintf(commentStr, constraint.ConName, constraint.ConComment)
		}
		if constraint.ConType == "f" {
			fkCons = append(fkCons, conStr)
		} else {
			cons = append(cons, conStr)
		}
	}
	return cons, fkCons
}

func PrintConstraintStatements(predataFile io.Writer, constraints []string, fkConstraints []string) {
	sort.Strings(constraints)
	sort.Strings(fkConstraints)
	for _, constraint := range constraints {
		utils.MustPrintln(predataFile, constraint)
	}
	for _, constraint := range fkConstraints {
		utils.MustPrintln(predataFile, constraint)
	}
}

func PrintCreateSchemaStatements(predataFile io.Writer, schemas []utils.Schema) {
	for _, schema := range schemas {
		utils.MustPrintln(predataFile)
		if schema.SchemaName != "public" {
			utils.MustPrintf(predataFile, "\nCREATE SCHEMA %s;", schema.ToString())
		}
		if schema.Owner != "" {
			utils.MustPrintf(predataFile, "\nALTER SCHEMA %s OWNER TO %s;", schema.ToString(), utils.QuoteIdent(schema.Owner))
		}
		if schema.Comment != "" {
			utils.MustPrintf(predataFile, "\nCOMMENT ON SCHEMA %s IS '%s';", schema.ToString(), schema.Comment)
		}
	}
}

func GetAllSequenceDefinitions(connection *utils.DBConn) []SequenceDefinition {
	allSequences := GetAllSequences(connection)
	sequenceDefs := make([]SequenceDefinition, 0)
	for _, seq := range allSequences {
		sequence := GetSequenceDefinition(connection, seq.ToString())
		sequenceDef := SequenceDefinition{seq, sequence}
		sequenceDefs = append(sequenceDefs, sequenceDef)
	}
	return sequenceDefs
}

/*
 * This function is largely derived from the dumpSequence() function in pg_dump.c.  The values of
 * minVal and maxVal come from SEQ_MINVALUE and SEQ_MAXVALUE, defined in include/commands/sequence.h.
 */
func PrintCreateSequenceStatements(predataFile io.Writer, sequences []SequenceDefinition, sequenceOwners map[string]string) {
	maxVal := int64(9223372036854775807)
	minVal := int64(-9223372036854775807)
	for _, sequence := range sequences {
		seqFQN := sequence.ToString()
		utils.MustPrintln(predataFile, "\n\nCREATE SEQUENCE", seqFQN)
		if !sequence.IsCalled {
			utils.MustPrintln(predataFile, "\tSTART WITH", sequence.LastVal)
		}
		utils.MustPrintln(predataFile, "\tINCREMENT BY", sequence.Increment)

		if !((sequence.MaxVal == maxVal && sequence.Increment > 0) || (sequence.MaxVal == -1 && sequence.Increment < 0)) {
			utils.MustPrintln(predataFile, "\tMAXVALUE", sequence.MaxVal)
		} else {
			utils.MustPrintln(predataFile, "\tNO MAXVALUE")
		}
		if !((sequence.MinVal == minVal && sequence.Increment < 0) || (sequence.MinVal == 1 && sequence.Increment > 0)) {
			utils.MustPrintln(predataFile, "\tMINVALUE", sequence.MinVal)
		} else {
			utils.MustPrintln(predataFile, "\tNO MINVALUE")
		}
		cycleStr := ""
		if sequence.IsCycled {
			cycleStr = "\n\tCYCLE"
		}
		utils.MustPrintf(predataFile, "\tCACHE %d%s;", sequence.CacheVal, cycleStr)

		utils.MustPrintf(predataFile, "\n\nSELECT pg_catalog.setval('%s', %d, %v);\n", seqFQN, sequence.LastVal, sequence.IsCalled)

		if sequence.Owner != "" {
			utils.MustPrintf(predataFile, "\n\nALTER TABLE %s OWNER TO %s;\n", seqFQN, utils.QuoteIdent(sequence.Owner))
		}
		if owningColumn, hasOwner := sequenceOwners[seqFQN]; hasOwner {
			utils.MustPrintf(predataFile, "\n\nALTER SEQUENCE %s OWNED BY %s;\n", seqFQN, owningColumn)
		}

		if sequence.Comment != "" {
			utils.MustPrintf(predataFile, "\n\nCOMMENT ON SEQUENCE %s IS '%s';\n", seqFQN, sequence.Comment)
		}
	}
}

func PrintCreateLanguageStatements(predataFile io.Writer, procLangs []QueryProceduralLanguage, funcInfoMap map[uint32]FunctionInfo) {
	for _, procLang := range procLangs {
		quotedOwner := utils.QuoteIdent(procLang.Owner)
		quotedLanguage := utils.QuoteIdent(procLang.Name)
		utils.MustPrintf(predataFile, "\n\nCREATE ")
		if procLang.PlTrusted {
			utils.MustPrintf(predataFile, "TRUSTED ")
		}
		utils.MustPrintf(predataFile, "PROCEDURAL LANGUAGE %s;", quotedLanguage)
		/*
		 * If the handler, validator, and inline functions are in pg_pltemplate, we can
		 * dump a CREATE LANGUAGE command without specifying them individually.
		 *
		 * The schema of the handler function should match the schema of the language itself, but
		 * the inline and validator functions can be in a different schema and must be schema-qualified.
		 */

		if procLang.Handler != 0 {
			handlerInfo := funcInfoMap[procLang.Handler]
			utils.MustPrintf(predataFile, "\nALTER FUNCTION %s(%s) OWNER TO %s;", handlerInfo.QualifiedName, handlerInfo.Arguments, quotedOwner)
		}
		if procLang.Inline != 0 {
			inlineInfo := funcInfoMap[procLang.Inline]
			utils.MustPrintf(predataFile, "\nALTER FUNCTION %s(%s) OWNER TO %s;", inlineInfo.QualifiedName, inlineInfo.Arguments, quotedOwner)
		}
		if procLang.Validator != 0 {
			validatorInfo := funcInfoMap[procLang.Validator]
			utils.MustPrintf(predataFile, "\nALTER FUNCTION %s(%s) OWNER TO %s;", validatorInfo.QualifiedName, validatorInfo.Arguments, quotedOwner)
		}
		if procLang.Owner != "" {
			utils.MustPrintf(predataFile, "\nALTER LANGUAGE %s OWNER TO %s;", quotedLanguage, quotedOwner)
		}
		if procLang.Comment != "" {
			utils.MustPrintf(predataFile, "\n\nCOMMENT ON LANGUAGE %s IS '%s';", quotedLanguage, procLang.Comment)
		}
		utils.MustPrintln(predataFile)
	}
}

func PrintCreateFunctionStatements(predataFile io.Writer, funcDefs []QueryFunctionDefinition) {
	for _, funcDef := range funcDefs {
		funcFQN := utils.MakeFQN(funcDef.SchemaName, funcDef.FunctionName)
		utils.MustPrintf(predataFile, "\n\nCREATE FUNCTION %s(%s) RETURNS ", funcFQN, funcDef.Arguments)
		utils.MustPrintf(predataFile, "%s AS", funcDef.ResultType)
		PrintFunctionBodyOrPath(predataFile, funcDef)
		utils.MustPrintf(predataFile, "LANGUAGE %s", funcDef.Language)
		PrintFunctionModifiers(predataFile, funcDef)
		utils.MustPrintln(predataFile, ";")

		if funcDef.Owner != "" {
			utils.MustPrintf(predataFile, "\nALTER FUNCTION %s(%s) OWNER TO %s;\n", funcFQN, funcDef.IdentArgs, utils.QuoteIdent(funcDef.Owner))
		}
		if funcDef.Comment != "" {
			utils.MustPrintf(predataFile, "\nCOMMENT ON FUNCTION %s(%s) IS '%s';\n", funcFQN, funcDef.IdentArgs, funcDef.Comment)
		}
	}
}

/*
 * This function either prints a path to an executable function (for C and
 * internal functions) or a function definition (for functions in other languages).
 */
func PrintFunctionBodyOrPath(predataFile io.Writer, funcDef QueryFunctionDefinition) {
	/*
	 * pg_proc.probin uses either NULL (in this case an empty string) or "-"
	 * to signify an unused path, for historical reasons.  See dumpFunc in
	 * pg_dump.c for details.
	 */
	if funcDef.BinaryPath != "" && funcDef.BinaryPath != "-" {
		utils.MustPrintf(predataFile, "\n'%s', '%s'\n", funcDef.BinaryPath, funcDef.FunctionBody)
	} else {
		utils.MustPrintf(predataFile, "\n%s\n", utils.DollarQuoteString(funcDef.FunctionBody))
	}
}

func PrintFunctionModifiers(predataFile io.Writer, funcDef QueryFunctionDefinition) {
	switch funcDef.SqlUsage {
	case "c":
		utils.MustPrintf(predataFile, " CONTAINS SQL")
	case "m":
		utils.MustPrintf(predataFile, " MODIFIES SQL DATA")
	case "n":
		utils.MustPrintf(predataFile, " NO SQL")
	case "r":
		utils.MustPrintf(predataFile, " READS SQL DATA")
	}
	switch funcDef.Volatility {
	case "i":
		utils.MustPrintf(predataFile, " IMMUTABLE")
	case "s":
		utils.MustPrintf(predataFile, " STABLE")
	case "v": // Default case, don't print anything else
	}
	if funcDef.IsStrict {
		utils.MustPrintf(predataFile, " STRICT")
	}
	if funcDef.IsSecurityDefiner {
		utils.MustPrintf(predataFile, " SECURITY DEFINER")
	}
	// Default cost is 1 for C and internal functions or 100 for functions in other languages
	isInternalOrC := funcDef.Language == "c" || funcDef.Language == "internal"
	if !((!isInternalOrC && funcDef.Cost == 100) || (isInternalOrC && funcDef.Cost == 1)) {
		utils.MustPrintf(predataFile, "\nCOST %v", funcDef.Cost)
	}
	if funcDef.ReturnsSet && funcDef.NumRows != 0 && funcDef.NumRows != 1000 {
		utils.MustPrintf(predataFile, "\nROWS %v", funcDef.NumRows)
	}
	if funcDef.Config != "" {
		utils.MustPrintf(predataFile, "\n%s", funcDef.Config)
	}
}

func PrintCreateAggregateStatements(predataFile io.Writer, aggDefs []QueryAggregateDefinition, funcInfoMap map[uint32]FunctionInfo) {
	for _, aggDef := range aggDefs {
		aggFQN := utils.MakeFQN(aggDef.SchemaName, aggDef.AggregateName)
		orderedStr := ""
		if aggDef.IsOrdered {
			orderedStr = "ORDERED "
		}
		utils.MustPrintf(predataFile, "\n\nCREATE %sAGGREGATE %s(%s) (\n", orderedStr, aggFQN, aggDef.Arguments)
		utils.MustPrintf(predataFile, "\tSFUNC = %s,\n", funcInfoMap[aggDef.TransitionFunction].QualifiedName)
		utils.MustPrintf(predataFile, "\tSTYPE = %s", aggDef.TransitionDataType)

		if aggDef.PreliminaryFunction != 0 {
			utils.MustPrintf(predataFile, ",\n\tPREFUNC = %s", funcInfoMap[aggDef.PreliminaryFunction].QualifiedName)
		}
		if aggDef.FinalFunction != 0 {
			utils.MustPrintf(predataFile, ",\n\tFINALFUNC = %s", funcInfoMap[aggDef.FinalFunction].QualifiedName)
		}
		if aggDef.InitialValue != "" {
			utils.MustPrintf(predataFile, ",\n\tINITCOND = '%s'", aggDef.InitialValue)
		}
		if aggDef.SortOperator != 0 {
			utils.MustPrintf(predataFile, ",\n\tSORTOP = %s", funcInfoMap[aggDef.SortOperator].QualifiedName)
		}

		utils.MustPrintln(predataFile, "\n);")

		if aggDef.Owner != "" {
			utils.MustPrintf(predataFile, "\nALTER AGGREGATE %s(%s) OWNER TO %s;\n", aggFQN, aggDef.IdentArgs, utils.QuoteIdent(aggDef.Owner))
		}
		if aggDef.Comment != "" {
			utils.MustPrintf(predataFile, "\nCOMMENT ON AGGREGATE %s(%s) IS '%s';\n", aggFQN, aggDef.IdentArgs, aggDef.Comment)
		}
	}
}

func PrintCreateCastStatements(predataFile io.Writer, castDefs []QueryCastDefinition) {
	for _, castDef := range castDefs {
		castStr := fmt.Sprintf("CAST (%s AS %s)", castDef.SourceType, castDef.TargetType)
		utils.MustPrintf(predataFile, "\n\nCREATE %s\n", castStr)
		if castDef.FunctionSchema != "" {
			funcFQN := fmt.Sprintf("%s.%s", utils.QuoteIdent(castDef.FunctionSchema), utils.QuoteIdent(castDef.FunctionName))
			utils.MustPrintf(predataFile, "\tWITH FUNCTION %s(%s)", funcFQN, castDef.FunctionArgs)
		} else {
			utils.MustPrintf(predataFile, "\tWITHOUT FUNCTION")
		}
		switch castDef.CastContext {
		case "a":
			utils.MustPrintf(predataFile, "\nAS ASSIGNMENT")
		case "i":
			utils.MustPrintf(predataFile, "\nAS IMPLICIT")
		case "e": // Default case, don't print anything else
		}
		utils.MustPrintln(predataFile, ";")
		if castDef.Comment != "" {
			utils.MustPrintf(predataFile, "\nCOMMENT ON %s IS '%s';\n", castStr, castDef.Comment)
		}
	}
}

/*
 * Because only base types are dependent on functions, we only need to print
 * shell type statements for base types.
 */
func PrintShellTypeStatements(predataFile io.Writer, types []TypeDefinition) {
	utils.MustPrintln(predataFile, "\n")
	for _, typ := range types {
		if typ.Type == "b" || typ.Type == "p" {
			typeFQN := utils.MakeFQN(typ.TypeSchema, typ.TypeName)
			utils.MustPrintf(predataFile, "CREATE TYPE %s;\n", typeFQN)
		}
	}
}

func PrintCreateBaseTypeStatements(predataFile io.Writer, types []TypeDefinition) {
	i := 0
	for i < len(types) {
		typ := types[i]
		if typ.Type == "b" {
			typeFQN := utils.MakeFQN(typ.TypeSchema, typ.TypeName)
			utils.MustPrintf(predataFile, "\n\nCREATE TYPE %s (\n", typeFQN)

			utils.MustPrintf(predataFile, "\tINPUT = %s,\n\tOUTPUT = %s", typ.Input, typ.Output)
			if typ.Receive != "-" {
				utils.MustPrintf(predataFile, ",\n\tRECEIVE = %s", typ.Receive)
			}
			if typ.Send != "-" {
				utils.MustPrintf(predataFile, ",\n\tSEND = %s", typ.Send)
			}
			if typ.ModIn != "-" {
				utils.MustPrintf(predataFile, ",\n\tTYPMOD_IN = %s", typ.ModIn)
			}
			if typ.ModOut != "-" {
				utils.MustPrintf(predataFile, ",\n\tTYPMOD_OUT = %s", typ.ModOut)
			}
			if typ.InternalLength > 0 {
				utils.MustPrintf(predataFile, ",\n\tINTERNALLENGTH = %d", typ.InternalLength)
			}
			if typ.IsPassedByValue {
				utils.MustPrintf(predataFile, ",\n\tPASSEDBYVALUE")
			}
			if typ.Alignment != "-" {
				switch typ.Alignment {
				case "d":
					utils.MustPrintf(predataFile, ",\n\tALIGNMENT = double")
				case "i":
					utils.MustPrintf(predataFile, ",\n\tALIGNMENT = int4")
				case "s":
					utils.MustPrintf(predataFile, ",\n\tALIGNMENT = int2")
				case "c": // Default case, don't print anything else
				}
			}
			if typ.Storage != "" {
				switch typ.Storage {
				case "e":
					utils.MustPrintf(predataFile, ",\n\tSTORAGE = extended")
				case "m":
					utils.MustPrintf(predataFile, ",\n\tSTORAGE = main")
				case "x":
					utils.MustPrintf(predataFile, ",\n\tSTORAGE = external")
				case "p": // Default case, don't print anything else
				}
			}
			if typ.DefaultVal != "" {
				utils.MustPrintf(predataFile, ",\n\tDEFAULT = %s", typ.DefaultVal)
			}
			if typ.Element != "-" {
				utils.MustPrintf(predataFile, ",\n\tELEMENT = %s", typ.Element)
			}
			if typ.Delimiter != "" {
				utils.MustPrintf(predataFile, ",\n\tDELIMITER = '%s'", typ.Delimiter)
			}
			utils.MustPrintln(predataFile, "\n);")
			if typ.Comment != "" {
				utils.MustPrintf(predataFile, "\nCOMMENT ON TYPE %s IS '%s';\n", typeFQN, typ.Comment)
			}
			if typ.Owner != "" {
				utils.MustPrintf(predataFile, "\nALTER TYPE %s OWNER TO %s;\n", typeFQN, typ.Owner)
			}
		}
		i++
	}
}

func PrintCreateCompositeAndEnumTypeStatements(predataFile io.Writer, types []TypeDefinition) {
	i := 0
	for i < len(types) {
		typ := types[i]
		if typ.Type == "c" {
			compositeTypes := make([]TypeDefinition, 0)
			/*
			 * Since types is sorted by schema then by type, all TypeDefinitions
			 * for the same composite type are grouped together.  Collect them in
			 * one list to use for printing
			 */
			for {
				if i < len(types) && typ.TypeSchema == types[i].TypeSchema && typ.TypeName == types[i].TypeName {
					compositeTypes = append(compositeTypes, types[i])
					i++
				} else {
					break
				}
			}
			/*
			 * All values except AttName and AttType will be the same for each TypeDefinition,
			 * so we can grab all other values from the first TypeDefinition in the list.
			 */
			composite := compositeTypes[0]
			typeFQN := utils.MakeFQN(composite.TypeSchema, composite.TypeName)
			utils.MustPrintf(predataFile, "\n\nCREATE TYPE %s AS (\n", typeFQN)
			atts := make([]string, 0)
			for _, composite := range compositeTypes {
				atts = append(atts, fmt.Sprintf("\t%s %s", composite.AttName, composite.AttType))
			}
			utils.MustPrintf(predataFile, strings.Join(atts, ",\n"))
			utils.MustPrintln(predataFile, "\n);")
			if composite.Comment != "" {
				utils.MustPrintf(predataFile, "\nCOMMENT ON TYPE %s IS '%s';\n", typeFQN, composite.Comment)
			}
			if composite.Owner != "" {
				utils.MustPrintf(predataFile, "\nALTER TYPE %s OWNER TO %s;\n", typeFQN, composite.Owner)
			}
		} else if typ.Type == "e" {
			typeFQN := utils.MakeFQN(typ.TypeSchema, typ.TypeName)
			utils.MustPrintf(predataFile, "\n\nCREATE TYPE %s AS ENUM (\n\t%s\n);\n", typeFQN, typ.EnumLabels)
			if typ.Comment != "" {
				utils.MustPrintf(predataFile, "\nCOMMENT ON TYPE %s IS '%s';\n", typeFQN, typ.Comment)
			}
			if typ.Owner != "" {
				utils.MustPrintf(predataFile, "\nALTER TYPE %s OWNER TO %s;\n", typeFQN, typ.Owner)
			}
			i++

		} else {
			i++
		}
	}
}

/*
 * Functions to print to the global or postdata file instead of, or in addition
 * to, the predata file.
 */

func PrintConnectionString(metadataFile io.Writer, dbname string) {
	utils.MustPrintf(metadataFile, "\\c %s\n", dbname)
}

func PrintSessionGUCs(metadataFile io.Writer, gucs QuerySessionGUCs) {
	utils.MustPrintf(metadataFile, `SET statement_timeout = 0;
SET check_function_bodies = false;
SET client_min_messages = error;
SET client_encoding = '%s';
SET standard_conforming_strings = %s;
SET default_with_oids = %s;
`, gucs.ClientEncoding, gucs.StdConformingStrings, gucs.DefaultWithOids)
}

func PrintCreateDatabaseStatement(globalFile io.Writer) {
	dbname := utils.QuoteIdent(connection.DBName)
	owner := utils.QuoteIdent(GetDatabaseOwner(connection))
	utils.MustPrintf(globalFile, "\n\nCREATE DATABASE %s;", dbname)
	utils.MustPrintf(globalFile, "\nALTER DATABASE %s OWNER TO %s;", dbname, owner)
}

func PrintDatabaseGUCs(globalFile io.Writer, gucs []string, dbname string) {
	for _, guc := range gucs {
		utils.MustPrintf(globalFile, "\nALTER DATABASE %s %s;", dbname, guc)
	}
}
