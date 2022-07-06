package backup

/*
 * This file contains structs and functions related to backing up metadata shared
 * among many or all object types (privileges, owners, and comments) on the
 * master that needs to be restored before data is restored.
 */

import (
	"github.com/greenplum-db/gpbackup/toc"
	"github.com/greenplum-db/gpbackup/utils"
)

/*
 * There's no built-in function to generate constraint definitions like there is for other types of
 * metadata, so this function constructs them.
 */
func PrintConstraintStatements(metadataFile *utils.FileWithByteCount, toc *toc.TOC, constraints []Constraint, conMetadata MetadataMap) {
	allConstraints := make([]Constraint, 0)
	allFkConstraints := make([]Constraint, 0)
	/*
	 * Because FOREIGN KEY constraints must be backed up after PRIMARY KEY
	 * constraints, we separate the two types then concatenate the lists,
	 * so FOREIGN KEY are guaranteed to be printed last.
	 */
	for _, constraint := range constraints {
		if constraint.ConType == "f" {
			allFkConstraints = append(allFkConstraints, constraint)
		} else if constraint.ConType == "t" {
			/*
			* Trigger constraints are added as triggers in backupPostdata.
			* We do not need to add them here also.
			* Further, the ALTER TABLE ADD CONSTRAINT syntax does not support adding triggers
			 */
			continue
		} else {
			allConstraints = append(allConstraints, constraint)
		}
	}
	constraints = append(allConstraints, allFkConstraints...)

	alterStr := "\n\nALTER %s %s ADD CONSTRAINT %s %s;\n"
	for _, constraint := range constraints {
		start := metadataFile.ByteCount
		if constraint.IsDomainConstraint {
			continue
		}
		// ConIsLocal should always return true from GetConstraints because we filter out constraints that are inherited using the INHERITS clause, or inherited from a parent partition table. This field only accurately reflects constraints in GPDB6+ because check constraints on parent tables must propogate to children. For GPDB versions 5 or lower, this field will default to false.
		objStr := "TABLE ONLY"
		if constraint.IsPartitionParent || (constraint.ConType == "c" && constraint.ConIsLocal) {
			objStr = "TABLE"
		}
		metadataFile.MustPrintf(alterStr, objStr, constraint.OwningObject, constraint.Name, constraint.ConDef.String)

		section, entry := constraint.GetMetadataEntry()
		toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
		PrintObjectMetadata(metadataFile, toc, conMetadata[constraint.GetUniqueID()], constraint, constraint.OwningObject)
	}
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
