package backup

/*
 * This file contains structs and functions related to backing up type
 * metadata on the coordinator that needs to be restored before data is restored.
 */

import (
	"fmt"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpbackup/toc"
	"github.com/greenplum-db/gpbackup/utils"
	"github.com/pkg/errors"
)

/*
 * Functions to print to the predata file
 */

func PrintCreateShellTypeStatements(metadataFile *utils.FileWithByteCount, tocfile *toc.TOC, shellTypes []ShellType, baseTypes []BaseType, rangeTypes []RangeType) {
	metadataFile.MustPrintf("\n\n")

	types := make([]toc.TOCObjectWithMetadata, 0)
	for _, shellType := range shellTypes {
		types = append(types, toc.TOCObjectWithMetadata(shellType))
	}
	for _, baseType := range baseTypes {
		types = append(types, toc.TOCObjectWithMetadata(baseType))
	}
	for _, rangeType := range rangeTypes {
		types = append(types, toc.TOCObjectWithMetadata(rangeType))
	}

	for _, typ := range types {
		start := metadataFile.ByteCount
		metadataFile.MustPrintf("CREATE TYPE %s;\n", typ.FQN())

		section, entry := typ.GetMetadataEntry()
		tocfile.AddMetadataEntry(section, entry, start, metadataFile.ByteCount, []uint32{0, 0})
	}
}

func PrintCreateDomainStatement(metadataFile *utils.FileWithByteCount, objToc *toc.TOC, domain Domain, typeMetadata ObjectMetadata, constraints []Constraint) {
	start := metadataFile.ByteCount
	metadataFile.MustPrintf("\nCREATE DOMAIN %s AS %s", domain.FQN(), domain.BaseType)
	if domain.DefaultVal != "" {
		metadataFile.MustPrintf(" DEFAULT %s", domain.DefaultVal)
	}
	if domain.Collation != "" {
		metadataFile.MustPrintf(" COLLATE %s", domain.Collation)
	}
	if domain.NotNull {
		metadataFile.MustPrintf(" NOT NULL")
	}
	for _, constraint := range constraints {
		metadataFile.MustPrintf("\n\tCONSTRAINT %s %s", constraint.Name, constraint.Def.String)
	}
	metadataFile.MustPrintln(";")

	section, entry := domain.GetMetadataEntry()
	tier := globalTierMap[domain.GetUniqueID()]
	objToc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount, tier)
	PrintObjectMetadata(metadataFile, objToc, typeMetadata, domain, "", tier)
}

func PrintCreateBaseTypeStatement(metadataFile *utils.FileWithByteCount, objToc *toc.TOC, base BaseType, typeMetadata ObjectMetadata) {
	start := metadataFile.ByteCount
	metadataFile.MustPrintf("\n\nCREATE TYPE %s (\n", base.FQN())

	// All of the following functions are stored in quoted form and don't need to be quoted again
	metadataFile.MustPrintf("\tINPUT = %s,\n\tOUTPUT = %s", base.Input, base.Output)
	if base.Receive != "" {
		metadataFile.MustPrintf(",\n\tRECEIVE = %s", base.Receive)
	}
	if base.Send != "" {
		metadataFile.MustPrintf(",\n\tSEND = %s", base.Send)
	}
	if base.ModIn != "" {
		metadataFile.MustPrintf(",\n\tTYPMOD_IN = %s", base.ModIn)
	}
	if base.ModOut != "" {
		metadataFile.MustPrintf(",\n\tTYPMOD_OUT = %s", base.ModOut)
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
			metadataFile.MustPrintf(",\n\tSTORAGE = external")
		case "m":
			metadataFile.MustPrintf(",\n\tSTORAGE = main")
		case "x":
			metadataFile.MustPrintf(",\n\tSTORAGE = extended")
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
	if base.Collatable {
		metadataFile.MustPrintf(",\n\tCOLLATABLE = true")
	}
	metadataFile.MustPrintln("\n);")
	if base.StorageOptions != "" {
		metadataFile.MustPrintf("\nALTER TYPE %s\n\tSET DEFAULT ENCODING (%s);", base.FQN(), base.StorageOptions)
	}
	section, entry := base.GetMetadataEntry()
	tier := globalTierMap[base.GetUniqueID()]
	objToc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount, tier)
	PrintObjectMetadata(metadataFile, objToc, typeMetadata, base, "", tier)
}

func PrintCreateCompositeTypeStatement(metadataFile *utils.FileWithByteCount, objToc *toc.TOC, composite CompositeType, typeMetadata ObjectMetadata) {
	var attributeList []string
	for _, att := range composite.Attributes {
		collationStr := ""
		if att.Collation != "" {
			collationStr = fmt.Sprintf(" COLLATE %s", att.Collation)
		}
		attributeList = append(attributeList, fmt.Sprintf("\t%s %s%s", att.Name, att.Type, collationStr))
	}

	start := metadataFile.ByteCount
	metadataFile.MustPrintf("\n\nCREATE TYPE %s AS (\n", composite.FQN())
	metadataFile.MustPrintln(strings.Join(attributeList, ",\n"))
	metadataFile.MustPrintf(");")

	section, entry := composite.GetMetadataEntry()
	tier := globalTierMap[composite.GetUniqueID()]
	objToc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount, tier)
	printPostCreateCompositeTypeStatement(metadataFile, objToc, composite, typeMetadata, tier)
}

func printPostCreateCompositeTypeStatement(metadataFile *utils.FileWithByteCount, objToc *toc.TOC, composite CompositeType, typeMetadata ObjectMetadata, tier []uint32) {
	PrintObjectMetadata(metadataFile, objToc, typeMetadata, composite, "", tier)
	statements := make([]string, 0)
	for _, att := range composite.Attributes {
		if att.Comment != "" {
			statements = append(statements, fmt.Sprintf("COMMENT ON COLUMN %s.%s IS %s;", composite.FQN(), att.Name, att.Comment))
		}
	}
	PrintStatements(metadataFile, objToc, composite, statements, tier)
}

func PrintCreateEnumTypeStatements(metadataFile *utils.FileWithByteCount, objToc *toc.TOC, enums []EnumType, typeMetadata MetadataMap) {
	for _, enum := range enums {
		start := metadataFile.ByteCount
		metadataFile.MustPrintf("\n\nCREATE TYPE %s AS ENUM (\n\t%s\n);\n", enum.FQN(), enum.EnumLabels)

		section, entry := enum.GetMetadataEntry()
		tier := globalTierMap[enum.GetUniqueID()]
		objToc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount, tier)
		PrintObjectMetadata(metadataFile, objToc, typeMetadata[enum.GetUniqueID()], enum, "", tier)
	}
}

func PrintCreateRangeTypeStatement(metadataFile *utils.FileWithByteCount, objToc *toc.TOC, rangeType RangeType, typeMetadata ObjectMetadata) {
	start := metadataFile.ByteCount
	metadataFile.MustPrintf("\n\nCREATE TYPE %s AS RANGE (\n\tSUBTYPE = %s", rangeType.FQN(), rangeType.SubType)

	if rangeType.SubTypeOpClass != "" {
		metadataFile.MustPrintf(",\n\tSUBTYPE_OPCLASS = %s", rangeType.SubTypeOpClass)
	}
	if rangeType.Collation != "" {
		metadataFile.MustPrintf(",\n\tCOLLATION = %s", rangeType.Collation)
	}
	if rangeType.Canonical != "" {
		metadataFile.MustPrintf(",\n\tCANONICAL = %s", rangeType.Canonical)
	}
	if rangeType.SubTypeDiff != "" {
		metadataFile.MustPrintf(",\n\tSUBTYPE_DIFF = %s", rangeType.SubTypeDiff)
	}
	metadataFile.MustPrintf("\n);\n")

	section, entry := rangeType.GetMetadataEntry()
	tier := globalTierMap[rangeType.GetUniqueID()]
	objToc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount, tier)
	PrintObjectMetadata(metadataFile, objToc, typeMetadata, rangeType, "", tier)
}

func PrintCreateCollationStatements(metadataFile *utils.FileWithByteCount, objToc *toc.TOC, collations []Collation, collationMetadata MetadataMap) {
	for _, collation := range collations {
		start := metadataFile.ByteCount
		metadataFile.MustPrintf("\nCREATE COLLATION %s (LC_COLLATE = '%s', LC_CTYPE = '%s'", collation.FQN(), collation.Collate, collation.Ctype)
		if collation.Provider != "" {
			providerOption := ""
			switch collation.Provider {
			case "c":
				providerOption = "libc"
			case "i":
				providerOption = "icu"
			case "d":
				providerOption = "default"
			default:
				gplog.Fatal(errors.Errorf("Unexpected collation provider: expected 'c|i|d' got '%s'\n", collation.Provider), "")
			}
			metadataFile.MustPrintf(", PROVIDER = '%s'", providerOption)
		}
		if collation.IsDeterministic == "f" {
			metadataFile.MustPrintf(", DETERMINISTIC = 'false'")
		}
		metadataFile.MustPrintf(");")

		section, entry := collation.GetMetadataEntry()
		tier := globalTierMap[collation.GetUniqueID()]
		objToc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount, tier)
		PrintObjectMetadata(metadataFile, objToc, collationMetadata[collation.GetUniqueID()], collation, "", tier)
	}
}
