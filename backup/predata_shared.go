package backup

/*
 * This file contains structs and functions related to backing up metadata shared
 * among many or all object types (privileges, owners, and comments) on the
 * coordinator that needs to be restored before data is restored.
 */

import (
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpbackup/toc"
	"github.com/greenplum-db/gpbackup/utils"
	"github.com/pkg/errors"
)

/*
 * There's no built-in function to generate constraint definitions like there is for other types of
 * metadata, so this function constructs them.
 */
func PrintConstraintStatement(metadataFile *utils.FileWithByteCount, objToc *toc.TOC, constraint Constraint, conMetadata ObjectMetadata) {
	alterStr := "\n\nALTER %s %s ADD CONSTRAINT %s %s;\n"
	start := metadataFile.ByteCount
	// ConIsLocal should always return true from GetConstraints because we filter out constraints that are inherited using the INHERITS clause, or inherited from a parent partition table. This field only accurately reflects constraints in GPDB6+ because check constraints on parent tables must propogate to children. For GPDB versions 5 or lower, this field will default to false.
	objStr := "TABLE ONLY"
	if constraint.IsPartitionParent || (constraint.ConType == "c" && constraint.ConIsLocal) {
		// this is not strictly an object type but it shares use with them so we use the const here
		objStr = toc.OBJ_TABLE
	}
	metadataFile.MustPrintf(alterStr, objStr, constraint.OwningObject, constraint.Name, constraint.Def.String)

	section, entry := constraint.GetMetadataEntry()
	tier := globalTierMap[constraint.GetUniqueID()]
	objToc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount, tier)
	PrintObjectMetadata(metadataFile, objToc, conMetadata, constraint, constraint.OwningObject, tier)
}

func PrintCreateSchemaStatements(metadataFile *utils.FileWithByteCount, objToc *toc.TOC, schemas []Schema, schemaMetadata MetadataMap) {
	for _, schema := range schemas {
		start := metadataFile.ByteCount
		metadataFile.MustPrintln()
		if schema.Name != "public" {
			metadataFile.MustPrintf("\nCREATE SCHEMA %s;", schema.Name)
		}
		section, entry := schema.GetMetadataEntry()
		tier := globalTierMap[schema.GetUniqueID()]
		objToc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount, tier)
		PrintObjectMetadata(metadataFile, objToc, schemaMetadata[schema.GetUniqueID()], schema, "", tier)
	}
}

func PrintAccessMethodStatements(metadataFile *utils.FileWithByteCount, objToc *toc.TOC, accessMethods []AccessMethod, accessMethodMetadata MetadataMap) {
	for _, method := range accessMethods {
		start := metadataFile.ByteCount
		methodTypeStr := ""
		switch method.Type {
		case "t":
			// these are not strictly object types but they share use with them so we use the consts here
			methodTypeStr = toc.OBJ_TABLE
		case "i":
			methodTypeStr = toc.OBJ_INDEX
		default:
			gplog.Fatal(errors.Errorf("Invalid access method type: expected 't' or 'i', got '%s'\n", method.Type), "")
		}
		metadataFile.MustPrintf("\n\nCREATE ACCESS METHOD %s TYPE %s HANDLER %s;", method.Name, methodTypeStr, method.Handler)
		section, entry := method.GetMetadataEntry()
		tier := globalTierMap[method.GetUniqueID()]
		objToc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount, tier)
		PrintObjectMetadata(metadataFile, objToc, accessMethodMetadata[method.GetUniqueID()], method, "", tier)
	}
}
