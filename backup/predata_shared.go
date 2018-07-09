package backup

/*
 * This file contains structs and functions related to backing up metadata shared
 * among many or all object types (privileges, owners, and comments) on the
 * master that needs to be restored before data is restored.
 */

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpbackup/utils"
	"github.com/pkg/errors"
)

/*
 * Generic functions and structs relating to schemas
 */
type Schema struct {
	Oid  uint32
	Name string
}

func SchemaFromString(name string) Schema {
	var schema string
	var matches []string
	if matches = utils.QuotedIdentifier.FindStringSubmatch(name); len(matches) != 0 {
		schema = utils.ReplacerUnescape.Replace(matches[1])
	} else if matches = utils.UnquotedIdentifier.FindStringSubmatch(name); len(matches) != 0 {
		schema = utils.ReplacerUnescape.Replace(matches[1])
	} else {
		gplog.Fatal(errors.Errorf("%s is not a valid identifier", name), "")
	}
	return Schema{0, schema}
}

/*
 * There's no built-in function to generate constraint definitions like there is for other types of
 * metadata, so this function constructs them.
 */
func PrintConstraintStatements(metadataFile *utils.FileWithByteCount, toc *utils.TOC, constraints []Constraint, conMetadata MetadataMap) {
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
		objStr := "TABLE ONLY"
		if constraint.IsPartitionParent {
			objStr = "TABLE"
		}
		metadataFile.MustPrintf(alterStr, objStr, constraint.OwningObject, constraint.Name, constraint.ConDef)
		PrintObjectMetadata(metadataFile, conMetadata[constraint.Oid], constraint.Name, "CONSTRAINT", constraint.OwningObject)
		toc.AddPredataEntry(constraint.Schema, constraint.Name, "CONSTRAINT", constraint.OwningObject, start, metadataFile)
	}
}

func PrintCreateSchemaStatements(backupfile *utils.FileWithByteCount, toc *utils.TOC, schemas []Schema, schemaMetadata MetadataMap) {
	for _, schema := range schemas {
		start := backupfile.ByteCount
		backupfile.MustPrintln()
		if schema.Name != "public" {
			backupfile.MustPrintf("\nCREATE SCHEMA %s;", schema.Name)
		}
		PrintObjectMetadata(backupfile, schemaMetadata[schema.Oid], schema.Name, "SCHEMA")
		toc.AddPredataEntry(schema.Name, schema.Name, "SCHEMA", "", start, backupfile)
	}
}

/*
 * Structs and functions relating to generic metadata handling.
 */

type ObjectMetadata struct {
	Privileges []ACL
	Owner      string
	Comment    string
}

type ACL struct {
	Grantee             string
	Select              bool
	SelectWithGrant     bool
	Insert              bool
	InsertWithGrant     bool
	Update              bool
	UpdateWithGrant     bool
	Delete              bool
	DeleteWithGrant     bool
	Truncate            bool
	TruncateWithGrant   bool
	References          bool
	ReferencesWithGrant bool
	Trigger             bool
	TriggerWithGrant    bool
	Usage               bool
	UsageWithGrant      bool
	Execute             bool
	ExecuteWithGrant    bool
	Create              bool
	CreateWithGrant     bool
	Temporary           bool
	TemporaryWithGrant  bool
	Connect             bool
	ConnectWithGrant    bool
}

type MetadataMap map[uint32]ObjectMetadata

func PrintObjectMetadata(file *utils.FileWithByteCount, obj ObjectMetadata, objectName string, objectType string, owningTable ...string) {
	if comment := obj.GetCommentStatement(objectName, objectType, owningTable...); comment != "" {
		file.MustPrintln(comment)
	}
	if owner := obj.GetOwnerStatement(objectName, objectType); owner != "" {
		if !(connectionPool.Version.Before("5") && objectType == "LANGUAGE") {
			// Languages have implicit owners in 4.3, but do not support ALTER OWNER
			file.MustPrintln(owner)
		}
	}
	if privileges := obj.GetPrivilegesStatements(objectName, objectType); privileges != "" {
		file.MustPrintln(privileges)
	}
}

func ConstructMetadataMap(results []MetadataQueryStruct) MetadataMap {
	metadataMap := make(MetadataMap)
	var metadata ObjectMetadata
	if len(results) > 0 {
		currentOid := uint32(0)
		// Collect all entries for the same object into one ObjectMetadata
		for _, result := range results {
			privilegesStr := ""
			if result.Kind == "Empty" {
				privilegesStr = "GRANTEE=/GRANTOR"
			} else if result.Privileges.Valid {
				privilegesStr = result.Privileges.String
			}
			if result.Oid != currentOid {
				if currentOid != 0 {
					metadata.Privileges = sortACLs(metadata.Privileges)
					metadataMap[currentOid] = metadata
				}
				currentOid = result.Oid
				metadata = ObjectMetadata{}
				metadata.Privileges = make([]ACL, 0)
				metadata.Owner = result.Owner
				metadata.Comment = result.Comment
			}
			privileges := ParseACL(privilegesStr)
			if privileges != nil {
				metadata.Privileges = append(metadata.Privileges, *privileges)
			}
		}
		metadata.Privileges = sortACLs(metadata.Privileges)
		metadataMap[currentOid] = metadata
	}
	return metadataMap
}

func ParseACL(aclStr string) *ACL {
	aclRegex := regexp.MustCompile(`^(?:\"(.*)\"|(.*))=([a-zA-Z\*]*)/(?:\"(.*)\"|(.*))$`)
	grantee := ""
	acl := ACL{}
	if matches := aclRegex.FindStringSubmatch(aclStr); len(matches) != 0 {
		if matches[1] != "" {
			grantee = matches[1]
		} else if matches[2] != "" {
			grantee = matches[2]
		} else {
			grantee = "" // Empty string indicates privileges granted to PUBLIC
		}
		permStr := matches[3]
		var lastChar rune
		for _, char := range permStr {
			switch char {
			case 'a':
				acl.Insert = true
			case 'r':
				acl.Select = true
			case 'w':
				acl.Update = true
			case 'd':
				acl.Delete = true
			case 'D':
				acl.Truncate = true
			case 'x':
				acl.References = true
			case 't':
				acl.Trigger = true
			case 'X':
				acl.Execute = true
			case 'U':
				acl.Usage = true
			case 'C':
				acl.Create = true
			case 'T':
				acl.Temporary = true
			case 'c':
				acl.Connect = true
			case '*':
				switch lastChar {
				case 'a':
					acl.Insert = false
					acl.InsertWithGrant = true
				case 'r':
					acl.Select = false
					acl.SelectWithGrant = true
				case 'w':
					acl.Update = false
					acl.UpdateWithGrant = true
				case 'd':
					acl.Delete = false
					acl.DeleteWithGrant = true
				case 'D':
					acl.Truncate = false
					acl.TruncateWithGrant = true
				case 'x':
					acl.References = false
					acl.ReferencesWithGrant = true
				case 't':
					acl.Trigger = false
					acl.TriggerWithGrant = true
				case 'X':
					acl.Execute = false
					acl.ExecuteWithGrant = true
				case 'U':
					acl.Usage = false
					acl.UsageWithGrant = true
				case 'C':
					acl.Create = false
					acl.CreateWithGrant = true
				case 'T':
					acl.Temporary = false
					acl.TemporaryWithGrant = true
				case 'c':
					acl.Connect = false
					acl.ConnectWithGrant = true
				}
			}
			lastChar = char
		}
		acl.Grantee = grantee
		return &acl
	}
	return nil
}

func (obj ObjectMetadata) GetPrivilegesStatements(objectName string, objectType string, columnName ...string) string {
	statements := []string{}
	typeStr := fmt.Sprintf("%s ", objectType)
	if objectType == "VIEW" {
		typeStr = ""
	} else if objectType == "COLUMN" {
		typeStr = "TABLE "
	}
	columnStr := ""
	if len(columnName) == 1 {
		columnStr = fmt.Sprintf("(%s) ", columnName[0])
	}
	if len(obj.Privileges) != 0 {
		statements = append(statements, fmt.Sprintf("REVOKE ALL %sON %s%s FROM PUBLIC;", columnStr, typeStr, objectName))
		if obj.Owner != "" {
			statements = append(statements, fmt.Sprintf("REVOKE ALL %sON %s%s FROM %s;", columnStr, typeStr, objectName, obj.Owner))
		}
		for _, acl := range obj.Privileges {
			/*
			 * Determine whether to print "GRANT ALL" instead of granting individual
			 * privileges.  Information on which privileges exist for a given object
			 * comes from src/include/utils/acl.h in GPDB.
			 */
			hasAllPrivileges := false
			hasAllPrivilegesWithGrant := false
			privStr := ""
			privWithGrantStr := ""
			grantee := ""
			if acl.Grantee == "" {
				grantee = "PUBLIC"
			} else {
				grantee = acl.Grantee
			}
			switch objectType {
			case "COLUMN":
				hasAllPrivileges = acl.Select && acl.Insert && acl.Update && acl.References
				hasAllPrivilegesWithGrant = acl.SelectWithGrant && acl.InsertWithGrant && acl.UpdateWithGrant && acl.ReferencesWithGrant
			case "DATABASE":
				hasAllPrivileges = acl.Create && acl.Temporary && acl.Connect
				hasAllPrivilegesWithGrant = acl.CreateWithGrant && acl.TemporaryWithGrant && acl.ConnectWithGrant
			case "FOREIGN DATA WRAPPER":
				hasAllPrivileges = acl.Usage
				hasAllPrivilegesWithGrant = acl.UsageWithGrant
			case "FOREIGN SERVER":
				hasAllPrivileges = acl.Usage
				hasAllPrivilegesWithGrant = acl.UsageWithGrant
			case "FUNCTION":
				hasAllPrivileges = acl.Execute
				hasAllPrivilegesWithGrant = acl.ExecuteWithGrant
			case "LANGUAGE":
				hasAllPrivileges = acl.Usage
				hasAllPrivilegesWithGrant = acl.UsageWithGrant
			case "PROTOCOL":
				hasAllPrivileges = acl.Select && acl.Insert
				hasAllPrivilegesWithGrant = acl.SelectWithGrant && acl.InsertWithGrant
			case "SCHEMA":
				hasAllPrivileges = acl.Usage && acl.Create
				hasAllPrivilegesWithGrant = acl.UsageWithGrant && acl.CreateWithGrant
			case "SEQUENCE":
				hasAllPrivileges = acl.Select && acl.Update && acl.Usage
				hasAllPrivilegesWithGrant = acl.SelectWithGrant && acl.UpdateWithGrant && acl.UsageWithGrant
			case "TABLE":
				hasAllPrivileges = acl.Select && acl.Insert && acl.Update && acl.Delete && acl.Truncate && acl.References && acl.Trigger
				hasAllPrivilegesWithGrant = acl.SelectWithGrant && acl.InsertWithGrant && acl.UpdateWithGrant && acl.DeleteWithGrant &&
					acl.TruncateWithGrant && acl.ReferencesWithGrant && acl.TriggerWithGrant
			case "TABLESPACE":
				hasAllPrivileges = acl.Create
				hasAllPrivilegesWithGrant = acl.CreateWithGrant
			case "VIEW":
				hasAllPrivileges = acl.Select && acl.Insert && acl.Update && acl.Delete && acl.Truncate && acl.References && acl.Trigger
				hasAllPrivilegesWithGrant = acl.SelectWithGrant && acl.InsertWithGrant && acl.UpdateWithGrant && acl.DeleteWithGrant &&
					acl.TruncateWithGrant && acl.ReferencesWithGrant && acl.TriggerWithGrant
			}
			if hasAllPrivileges {
				privStr = "ALL"
			} else {
				privList := make([]string, 0)
				if acl.Select {
					privList = append(privList, "SELECT")
				}
				if acl.Insert {
					privList = append(privList, "INSERT")
				}
				if acl.Update {
					privList = append(privList, "UPDATE")
				}
				if acl.Delete {
					privList = append(privList, "DELETE")
				}
				if acl.Truncate {
					privList = append(privList, "TRUNCATE")
				}
				if acl.References {
					privList = append(privList, "REFERENCES")
				}
				if acl.Trigger {
					privList = append(privList, "TRIGGER")
				}
				/*
				 * We skip checking whether acl.Execute is set here because only Functions have Execute,
				 * and functions only have Execute, so Execute == hasAllPrivileges for Functions.
				 */
				if acl.Usage {
					privList = append(privList, "USAGE")
				}
				if acl.Create {
					privList = append(privList, "CREATE")
				}
				if acl.Temporary {
					privList = append(privList, "TEMPORARY")
				}
				if acl.Connect {
					privList = append(privList, "CONNECT")
				}
				privStr = strings.Join(privList, ",")
			}
			if hasAllPrivilegesWithGrant {
				privWithGrantStr = "ALL"
			} else {
				privWithGrantList := make([]string, 0)
				if acl.SelectWithGrant {
					privWithGrantList = append(privWithGrantList, "SELECT")
				}
				if acl.InsertWithGrant {
					privWithGrantList = append(privWithGrantList, "INSERT")
				}
				if acl.UpdateWithGrant {
					privWithGrantList = append(privWithGrantList, "UPDATE")
				}
				if acl.DeleteWithGrant {
					privWithGrantList = append(privWithGrantList, "DELETE")
				}
				if acl.TruncateWithGrant {
					privWithGrantList = append(privWithGrantList, "TRUNCATE")
				}
				if acl.ReferencesWithGrant {
					privWithGrantList = append(privWithGrantList, "REFERENCES")
				}
				if acl.TriggerWithGrant {
					privWithGrantList = append(privWithGrantList, "TRIGGER")
				}
				// The comment above regarding Execute applies to ExecuteWithGrant as well.
				if acl.UsageWithGrant {
					privWithGrantList = append(privWithGrantList, "USAGE")
				}
				if acl.CreateWithGrant {
					privWithGrantList = append(privWithGrantList, "CREATE")
				}
				if acl.TemporaryWithGrant {
					privWithGrantList = append(privWithGrantList, "TEMPORARY")
				}
				if acl.ConnectWithGrant {
					privWithGrantList = append(privWithGrantList, "CONNECT")
				}
				privWithGrantStr = strings.Join(privWithGrantList, ",")
			}
			if privStr != "" {
				statements = append(statements, fmt.Sprintf("GRANT %s %sON %s%s TO %s;", privStr, columnStr, typeStr, objectName, grantee))
			}
			if privWithGrantStr != "" {
				statements = append(statements, fmt.Sprintf("GRANT %s %sON %s%s TO %s WITH GRANT OPTION;", privWithGrantStr, columnStr, typeStr, objectName, grantee))
			}
		}
	}
	if len(statements) > 0 {
		return "\n\n" + strings.Join(statements, "\n")
	}
	return ""
}

func (obj ObjectMetadata) GetOwnerStatement(objectName string, objectType string) string {
	if objectType == "VIEW" {
		return ""
	}
	typeStr := objectType
	if objectType == "SEQUENCE" {
		typeStr = "TABLE"
	} else if objectType == "FOREIGN SERVER" {
		typeStr = "SERVER"
	}
	ownerStr := ""
	if obj.Owner != "" {
		ownerStr = fmt.Sprintf("\n\nALTER %s %s OWNER TO %s;", typeStr, objectName, obj.Owner)
	}
	return ownerStr
}

func (obj ObjectMetadata) GetCommentStatement(objectName string, objectType string, owningTable ...string) string {
	commentStr := ""
	tableStr := ""
	if len(owningTable) == 1 {
		tableStr = fmt.Sprintf(" ON %s", owningTable[0])
	}
	if obj.Comment != "" {
		escapedComment := strings.Replace(obj.Comment, "'", "''", -1)
		commentStr = fmt.Sprintf("\n\nCOMMENT ON %s %s%s IS '%s';", objectType, objectName, tableStr, escapedComment)
	}
	return commentStr
}

func PrintDependentObjectStatements(metadataFile *utils.FileWithByteCount, toc *utils.TOC, objects []Sortable, metadataMap MetadataMap, tableDefsMap map[uint32]TableDefinition, constraints []Constraint) {
	conMap := make(map[string][]Constraint)
	for _, constraint := range constraints {
		conMap[constraint.OwningObject] = append(conMap[constraint.OwningObject], constraint)
	}
	for _, object := range objects {
		switch obj := object.(type) {
		case Type:
			switch obj.Type {
			case "b":
				PrintCreateBaseTypeStatement(metadataFile, toc, obj, metadataMap[obj.Oid])
			case "c":
				PrintCreateCompositeTypeStatement(metadataFile, toc, obj, metadataMap[obj.Oid])
			case "d":
				domainName := utils.MakeFQN(obj.Schema, obj.Name)
				PrintCreateDomainStatement(metadataFile, toc, obj, metadataMap[obj.Oid], conMap[domainName])
			}
		case Function:
			PrintCreateFunctionStatement(metadataFile, toc, obj, metadataMap[obj.Oid])
		case Relation:
			PrintCreateTableStatement(metadataFile, toc, obj, tableDefsMap[obj.Oid], metadataMap[obj.Oid])
		case ExternalProtocol:
			PrintCreateExternalProtocolStatement(metadataFile, toc, obj, metadataMap[obj.Oid])
		}
	}
}
