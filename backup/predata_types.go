package backup

/*
 * This file contains structs and functions related to backing up type
 * metadata on the master that needs to be restored before data is restored.
 */

import (
	"strings"

	"github.com/greenplum-db/gpbackup/utils"
)

/*
 * Functions to print to the predata file
 */

/*
 * Because only base types are dependent on functions, we only need to print
 * shell type statements for base types.
 */
func PrintCreateShellTypeStatements(metadataFile *utils.FileWithByteCount, toc *utils.TOC, types []Type) {
	start := metadataFile.ByteCount
	metadataFile.MustPrintln("\n")
	for _, typ := range types {
		if typ.Type == "b" || typ.Type == "p" {
			typeFQN := utils.MakeFQN(typ.Schema, typ.Name)
			metadataFile.MustPrintf("CREATE TYPE %s;\n", typeFQN)
			toc.AddPredataEntry(typ.Schema, typ.Name, "TYPE", "", start, metadataFile)
			start = metadataFile.ByteCount
		}
	}
}

func PrintCreateDomainStatement(metadataFile *utils.FileWithByteCount, toc *utils.TOC, domain Type, typeMetadata ObjectMetadata, constraints []Constraint) {
	start := metadataFile.ByteCount
	typeFQN := utils.MakeFQN(domain.Schema, domain.Name)
	metadataFile.MustPrintf("\nCREATE DOMAIN %s AS %s", typeFQN, domain.BaseType)
	if domain.DefaultVal != "" {
		metadataFile.MustPrintf(" DEFAULT %s", domain.DefaultVal)
	}
	if domain.NotNull {
		metadataFile.MustPrintf(" NOT NULL")
	}
	for _, constraint := range constraints {
		metadataFile.MustPrintf("\n\tCONSTRAINT %s %s", constraint.Name, constraint.ConDef)
	}
	metadataFile.MustPrintln(";")
	PrintObjectMetadata(metadataFile, typeMetadata, typeFQN, "DOMAIN")
	toc.AddPredataEntry(domain.Schema, domain.Name, "DOMAIN", "", start, metadataFile)
}

func PrintCreateBaseTypeStatement(metadataFile *utils.FileWithByteCount, toc *utils.TOC, base Type, typeMetadata ObjectMetadata) {
	start := metadataFile.ByteCount
	typeFQN := utils.MakeFQN(base.Schema, base.Name)
	metadataFile.MustPrintf("\n\nCREATE TYPE %s (\n", typeFQN)

	// All of the following functions are stored in quoted form and don't need to be quoted again
	metadataFile.MustPrintf("\tINPUT = %s,\n\tOUTPUT = %s", base.Input, base.Output)
	if base.Receive != "" {
		metadataFile.MustPrintf(",\n\tRECEIVE = %s", base.Receive)
	}
	if base.Send != "" {
		metadataFile.MustPrintf(",\n\tSEND = %s", base.Send)
	}
	if connection.Version.AtLeast("5") {
		if base.ModIn != "" {
			metadataFile.MustPrintf(",\n\tTYPMOD_IN = %s", base.ModIn)
		}
		if base.ModOut != "" {
			metadataFile.MustPrintf(",\n\tTYPMOD_OUT = %s", base.ModOut)
		}
	}
	if base.InternalLength > 0 {
		metadataFile.MustPrintf(",\n\tINTERNALLENGTH = %d", base.InternalLength)
	}
	if base.IsPassedByValue {
		metadataFile.MustPrintf(",\n\tPASSEDBYVALUE")
	}
	if base.Alignment != "" {
		switch base.Alignment {
		case "d":
			metadataFile.MustPrintf(",\n\tALIGNMENT = double")
		case "i":
			metadataFile.MustPrintf(",\n\tALIGNMENT = int4")
		case "s":
			metadataFile.MustPrintf(",\n\tALIGNMENT = int2")
		case "c": // Default case, don't print anything else
		}
	}
	if base.Storage != "" {
		switch base.Storage {
		case "e":
			metadataFile.MustPrintf(",\n\tSTORAGE = extended")
		case "m":
			metadataFile.MustPrintf(",\n\tSTORAGE = main")
		case "x":
			metadataFile.MustPrintf(",\n\tSTORAGE = external")
		case "p": // Default case, don't print anything else
		}
	}
	if base.DefaultVal != "" {
		metadataFile.MustPrintf(",\n\tDEFAULT = '%s'", base.DefaultVal)
	}
	if base.Element != "" {
		metadataFile.MustPrintf(",\n\tELEMENT = %s", base.Element)
	}
	if base.Delimiter != "" {
		metadataFile.MustPrintf(",\n\tDELIMITER = '%s'", base.Delimiter)
	}
	if base.Category != "U" {
		metadataFile.MustPrintf(",\n\tCATEGORY = '%s'", base.Category)
	}
	if base.Preferred {
		metadataFile.MustPrintf(",\n\tPREFERRED = true")
	}
	metadataFile.MustPrintln("\n);")
	if base.StorageOptions != "" {
		metadataFile.MustPrintf("\nALTER TYPE %s\n\tSET DEFAULT ENCODING (%s);", typeFQN, base.StorageOptions)
	}
	PrintObjectMetadata(metadataFile, typeMetadata, typeFQN, "TYPE")
	toc.AddPredataEntry(base.Schema, base.Name, "TYPE", "", start, metadataFile)
}

func PrintCreateCompositeTypeStatement(metadataFile *utils.FileWithByteCount, toc *utils.TOC, composite Type, typeMetadata ObjectMetadata) {
	start := metadataFile.ByteCount
	typeFQN := utils.MakeFQN(composite.Schema, composite.Name)
	metadataFile.MustPrintf("\n\nCREATE TYPE %s AS (\n", typeFQN)
	metadataFile.MustPrintln(strings.Join(composite.Attributes, ",\n"))
	metadataFile.MustPrintf(");")
	PrintObjectMetadata(metadataFile, typeMetadata, typeFQN, "TYPE")
	toc.AddPredataEntry(composite.Schema, composite.Name, "TYPE", "", start, metadataFile)
}

func PrintCreateEnumTypeStatements(metadataFile *utils.FileWithByteCount, toc *utils.TOC, enums []Type, typeMetadata MetadataMap) {
	start := metadataFile.ByteCount
	for _, enum := range enums {
		typeFQN := utils.MakeFQN(enum.Schema, enum.Name)
		metadataFile.MustPrintf("\n\nCREATE TYPE %s AS ENUM (\n\t%s\n);\n", typeFQN, enum.EnumLabels)
		PrintObjectMetadata(metadataFile, typeMetadata[enum.Oid], typeFQN, "TYPE")
		toc.AddPredataEntry(enum.Schema, enum.Name, "TYPE", "", start, metadataFile)
	}
}

func PrintCreateCollationStatements(metadataFile *utils.FileWithByteCount, toc *utils.TOC, collations []Collation, collationMetadata MetadataMap) {
	for _, collation := range collations {
		collationFQN := utils.MakeFQN(collation.Schema, collation.Name)
		start := metadataFile.ByteCount
		metadataFile.MustPrintf("\nCREATE COLLATION %s (LC_COLLATE = '%s', LC_CTYPE = '%s');", collationFQN, collation.Collate, collation.Ctype)
		PrintObjectMetadata(metadataFile, collationMetadata[collation.Oid], collation.Name, "COLLATION")
		toc.AddPredataEntry(collation.Schema, collation.Name, "COLLATION", "", start, metadataFile)
	}
}
