package backup

/*
 * This file contains structs and functions related to backing up metadata shared
 * among many or all object types (privileges, owners, and comments) on the
 * master that needs to be restored before data is restored.
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
func PrintConstraintStatement(metadataFile *utils.FileWithByteCount, toc *toc.TOC, constraint Constraint, conMetadata ObjectMetadata) {
	alterStr := "\n\nALTER %s %s ADD CONSTRAINT %s %s;\n"
	start := metadataFile.ByteCount
	// ConIsLocal should always return true from GetConstraints because we filter out constraints that are inherited using the INHERITS clause, or inherited from a parent partition table. This field only accurately reflects constraints in GPDB6+ because check constraints on parent tables must propogate to children. For GPDB versions 5 or lower, this field will default to false.
	objStr := "TABLE ONLY"
	if constraint.IsPartitionParent || (constraint.ConType == "c" && constraint.ConIsLocal) {
		objStr = "TABLE"
	}
	metadataFile.MustPrintf(alterStr, objStr, constraint.OwningObject, constraint.Name, constraint.Def.String)

	section, entry := constraint.GetMetadataEntry()
	toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
	PrintObjectMetadata(metadataFile, toc, conMetadata, constraint, constraint.OwningObject)
}

func PrintCreateSchemaStatements(metadataFile *utils.FileWithByteCount, toc *toc.TOC, schemas []Schema, schemaMetadata MetadataMap) {
	for _, schema := range schemas {
		start := metadataFile.ByteCount
		metadataFile.MustPrintln()
		if schema.Name != "public" {
			metadataFile.MustPrintf("\nCREATE SCHEMA %s;", schema.Name)
		}
		section, entry := schema.GetMetadataEntry()
		toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
		PrintObjectMetadata(metadataFile, toc, schemaMetadata[schema.GetUniqueID()], schema, "")
	}
}

func PrintAccessMethodStatements(metadataFile *utils.FileWithByteCount, toc *toc.TOC, accessMethods []AccessMethod, accessMethodMetadata MetadataMap) {
	for _, method := range accessMethods {
		start := metadataFile.ByteCount
		methodTypeStr := ""
		switch method.Type {
		case "t":
			methodTypeStr = "TABLE"
		case "i":
			methodTypeStr = "INDEX"
		default:
			gplog.Fatal(errors.Errorf("Invalid access method type: expected 't' or 'i', got '%s'\n", method.Type), "")
		}
		metadataFile.MustPrintf("\n\nCREATE ACCESS METHOD %s TYPE %s HANDLER %s;", method.Name, methodTypeStr, method.Handler)
		section, entry := method.GetMetadataEntry()
		toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
		PrintObjectMetadata(metadataFile, toc, accessMethodMetadata[method.GetUniqueID()], method, "")
	}
}
