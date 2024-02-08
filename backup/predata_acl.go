package backup

import (
	"database/sql"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/greenplum-db/gpbackup/toc"
	"github.com/greenplum-db/gpbackup/utils"
)

var ACLRegex = regexp.MustCompile(`^(.*)=([a-zA-Z\*]*)/(.*)$`)

/*
 * Structs and functions relating to generic metadata handling.
 */

type ObjectMetadata struct {
	Privileges            []ACL
	ObjectType            string
	Owner                 string
	Comment               string
	SecurityLabelProvider string
	SecurityLabel         string
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

type MetadataMap map[UniqueID]ObjectMetadata

func PrintStatements(metadataFile *utils.FileWithByteCount, objToc *toc.TOC,
	obj toc.TOCObject, statements []string, tier []uint32) {
	for _, statement := range statements {
		start := metadataFile.ByteCount
		metadataFile.MustPrintf("\n\n%s\n", statement)
		section, entry := obj.GetMetadataEntry()

		/*
		 * Postdata metadata needs to be specifically marked as such to
		 * help with gprestore postdata parallel restore (--jobs).
		 */
		if section == "postdata" {
			entry.ObjectType = fmt.Sprintf("%s METADATA", entry.ObjectType)
			if entry.Schema != "" {
				entry.ReferenceObject = utils.MakeFQN(entry.Schema, entry.Name)
			} else {
				entry.ReferenceObject = entry.Name
			}
		}
		objToc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount, tier)
	}
}

func PrintObjectMetadata(metadataFile *utils.FileWithByteCount, objToc *toc.TOC,
	metadata ObjectMetadata, obj toc.TOCObjectWithMetadata, owningTable string, tier []uint32) {
	_, entry := obj.GetMetadataEntry()

	if entry.ObjectType == toc.OBJ_DATABASE_METADATA {
		entry.ObjectType = toc.OBJ_DATABASE
	}
	statements := make([]string, 0)
	if comment := metadata.GetCommentStatement(obj.FQN(), entry.ObjectType, owningTable); comment != "" {
		statements = append(statements, strings.TrimSpace(comment))
	}

	objectType := entry.ObjectType
	if connectionPool.Version.AtLeast("7") {
		switch object := obj.(type) {
		case Function:
			if object.Kind == "p" {
				// Procedures are handled as a kind of function but with different syntax and capabilities.
				// Distinction made when printing here
				objectType = toc.OBJ_PROCEDURE
			}
		}
	}

	if owner := metadata.GetOwnerStatement(obj.FQN(), objectType); owner != "" {
		statements = append(statements, strings.TrimSpace(owner))
	}
	if privileges := metadata.GetPrivilegesStatements(obj.FQN(), entry.ObjectType); privileges != "" {
		statements = append(statements, strings.TrimSpace(privileges))
	}
	if securityLabel := metadata.GetSecurityLabelStatement(obj.FQN(), entry.ObjectType); securityLabel != "" {
		statements = append(statements, strings.TrimSpace(securityLabel))
	}
	PrintStatements(metadataFile, objToc, obj, statements, tier)
}

// Only print grant statements for any functions that belong to extensions
func printExtensionFunctionACLs(metadataFile *utils.FileWithByteCount, objToc *toc.TOC,
	metadataMap MetadataMap, funcInfoMap map[uint32]FunctionInfo) {
	type objectInfo struct {
		FunctionInfo
		ObjectMetadata
	}
	var objects []objectInfo
	for uniqueId, obj := range metadataMap {
		// e.g. Grants for any functions that belong to extensions
		info, ok := funcInfoMap[uniqueId.Oid]
		if ok {
			objects = append(objects, objectInfo{info, obj})
		}
	}
	// Sort by function signature
	sort.SliceStable(objects, func(i, j int) bool {
		return objects[i].FQN() < objects[j].FQN()
	})
	statements := make([]string, 0)
	for _, obj := range objects {
		if privileges := obj.GetPrivilegesStatements(obj.FQN(), toc.OBJ_FUNCTION); privileges != "" {
			statements = append(statements, strings.TrimSpace(privileges))
			PrintStatements(metadataFile, objToc, obj, statements, []uint32{0, 0})
		}
	}
}

func ConstructMetadataMap(results []MetadataQueryStruct) MetadataMap {
	metadataMap := make(MetadataMap)
	if len(results) == 0 {
		return MetadataMap{}
	}
	var metadata ObjectMetadata
	currentUniqueID := UniqueID{}
	// Collect all entries for the same object into one ObjectMetadata
	for _, result := range results {
		privilegesStr := ""
		if result.Kind == "Empty" {
			privilegesStr = "GRANTEE=/GRANTOR"
		} else if result.Privileges.Valid {
			privilegesStr = result.Privileges.String
		}
		if result.UniqueID != currentUniqueID {
			if (currentUniqueID != UniqueID{}) {
				metadata.Privileges = sortACLs(metadata.Privileges)
				metadataMap[currentUniqueID] = metadata
			}
			currentUniqueID = result.UniqueID
			metadata = ObjectMetadata{}
			metadata.Privileges = make([]ACL, 0)
			metadata.Owner = result.Owner
			metadata.ObjectType = result.ObjectType
			metadata.Comment = result.Comment
			metadata.SecurityLabelProvider = result.SecurityLabelProvider
			metadata.SecurityLabel = result.SecurityLabel
		}

		privileges := ParseACL(privilegesStr)
		if privileges != nil {
			metadata.Privileges = append(metadata.Privileges, *privileges)
		}
	}
	metadata.Privileges = sortACLs(metadata.Privileges)
	metadataMap[currentUniqueID] = metadata
	return metadataMap
}

func getColumnACL(privileges sql.NullString, kind string) []ACL {
	privilegesStr := ""
	if kind == "Empty" {
		privilegesStr = "GRANTEE=/GRANTOR"
	} else if privileges.Valid {
		privilegesStr = privileges.String
	}
	columnMetadata := make([]ACL, 0)
	acl := ParseACL(privilegesStr)
	if acl != nil {
		columnMetadata = append(columnMetadata, *acl)
	}
	return columnMetadata
}

func ParseACL(aclStr string) *ACL {
	grantee := ""
	acl := ACL{}
	if matches := ACLRegex.FindStringSubmatch(aclStr); len(matches) != 0 {
		if matches[1] != "" {
			grantee = matches[1]
		} else {
			grantee = "" // Empty string indicates privileges granted to PUBLIC
		}
		permStr := matches[2]
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
		if quotedRoleName, ok := quotedRoleNames[grantee]; ok {
			acl.Grantee = quotedRoleName
		} else {
			acl.Grantee = grantee
		}

		return &acl
	}
	return nil
}

func (obj ObjectMetadata) GetPrivilegesStatements(objectName string, objectType string, columnName ...string) string {
	statements := make([]string, 0)
	typeStr := fmt.Sprintf("%s ", objectType)
	if objectType == toc.OBJ_VIEW || objectType == toc.OBJ_FOREIGN_TABLE || objectType == toc.OBJ_MATERIALIZED_VIEW {
		typeStr = "TABLE "
	} else if objectType == toc.OBJ_COLUMN {
		typeStr = "TABLE "
	} else if objectType == toc.OBJ_AGGREGATE {
		typeStr = "FUNCTION "
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
			grantee := ""
			if acl.Grantee == "" {
				grantee = "PUBLIC"
			} else {
				grantee = acl.Grantee
			}
			privStr, privWithGrantStr := createPrivilegeStrings(acl, objectType)
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

func createPrivilegeStrings(acl ACL, objectType string) (string, string) {
	/*
	 * Determine whether to print "GRANT ALL" instead of granting individual
	 * privileges.  Information on which privileges exist for a given object
	 * comes from src/include/utils/acl.h in GPDB.
	 */
	hasAllPrivileges := false
	hasAllPrivilegesWithGrant := false
	privStr := ""
	privWithGrantStr := ""
	switch objectType {
	case toc.OBJ_COLUMN:
		hasAllPrivileges = acl.Select && acl.Insert && acl.Update && acl.References
		hasAllPrivilegesWithGrant = acl.SelectWithGrant && acl.InsertWithGrant && acl.UpdateWithGrant && acl.ReferencesWithGrant
	case toc.OBJ_DATABASE:
		hasAllPrivileges = acl.Create && acl.Temporary && acl.Connect
		hasAllPrivilegesWithGrant = acl.CreateWithGrant && acl.TemporaryWithGrant && acl.ConnectWithGrant
	case toc.OBJ_FOREIGN_DATA_WRAPPER:
		hasAllPrivileges = acl.Usage
		hasAllPrivilegesWithGrant = acl.UsageWithGrant
	case toc.OBJ_FOREIGN_SERVER:
		hasAllPrivileges = acl.Usage
		hasAllPrivilegesWithGrant = acl.UsageWithGrant
	case toc.OBJ_FOREIGN_TABLE:
		hasAllPrivileges = acl.Select && acl.Insert && acl.Update && acl.Delete && acl.References && acl.Trigger
		hasAllPrivilegesWithGrant = acl.SelectWithGrant && acl.InsertWithGrant && acl.UpdateWithGrant && acl.DeleteWithGrant &&
			acl.ReferencesWithGrant && acl.TriggerWithGrant
	case toc.OBJ_FUNCTION:
		hasAllPrivileges = acl.Execute
		hasAllPrivilegesWithGrant = acl.ExecuteWithGrant
	case toc.OBJ_LANGUAGE:
		hasAllPrivileges = acl.Usage
		hasAllPrivilegesWithGrant = acl.UsageWithGrant
	case toc.OBJ_PROTOCOL:
		hasAllPrivileges = acl.Select && acl.Insert
		hasAllPrivilegesWithGrant = acl.SelectWithGrant && acl.InsertWithGrant
	case toc.OBJ_SCHEMA:
		hasAllPrivileges = acl.Usage && acl.Create
		hasAllPrivilegesWithGrant = acl.UsageWithGrant && acl.CreateWithGrant
	case toc.OBJ_SEQUENCE:
		hasAllPrivileges = acl.Select && acl.Update && acl.Usage
		hasAllPrivilegesWithGrant = acl.SelectWithGrant && acl.UpdateWithGrant && acl.UsageWithGrant
	case toc.OBJ_TABLE, toc.OBJ_VIEW, toc.OBJ_MATERIALIZED_VIEW:
		hasAllPrivileges = acl.Select && acl.Insert && acl.Update && acl.Delete && acl.Truncate && acl.References && acl.Trigger
		hasAllPrivilegesWithGrant = acl.SelectWithGrant && acl.InsertWithGrant && acl.UpdateWithGrant && acl.DeleteWithGrant &&
			acl.TruncateWithGrant && acl.ReferencesWithGrant && acl.TriggerWithGrant
	case toc.OBJ_TABLESPACE:
		hasAllPrivileges = acl.Create
		hasAllPrivilegesWithGrant = acl.CreateWithGrant
	case toc.OBJ_TYPE:
		hasAllPrivileges = acl.Usage
		hasAllPrivilegesWithGrant = acl.UsageWithGrant
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

	return privStr, privWithGrantStr

}
func (obj ObjectMetadata) GetOwnerStatement(objectName string, objectType string) string {
	typeStr := objectType
	if connectionPool.Version.Before("6") && (objectType == toc.OBJ_SEQUENCE || objectType == toc.OBJ_VIEW) {
		// these aren't strictly object types but share use with them so we use the const strings here
		typeStr = toc.OBJ_TABLE
	} else if objectType == toc.OBJ_FOREIGN_SERVER {
		typeStr = toc.OBJ_SERVER
	}
	ownerStr := ""
	if obj.Owner != "" {
		ownerStr = fmt.Sprintf("ALTER %s %s OWNER TO %s;", typeStr, objectName, obj.Owner)
	}
	return ownerStr
}

func (obj ObjectMetadata) GetCommentStatement(objectName string, objectType string, owningTable string) string {
	commentStr := ""
	tableStr := ""
	if owningTable != "" {
		tableStr = fmt.Sprintf(" ON %s", owningTable)
	}
	if obj.Comment != "" {
		escapedComment := utils.EscapeSingleQuotes(obj.Comment)
		commentStr = fmt.Sprintf("COMMENT ON %s %s%s IS '%s';", objectType, objectName, tableStr, escapedComment)
	}
	return commentStr
}

func (obj ObjectMetadata) GetSecurityLabelStatement(objectName string, objectType string) string {
	securityLabelStr := ""
	if obj.SecurityLabel != "" {
		escapedLabel := utils.EscapeSingleQuotes(obj.SecurityLabel)
		securityLabelStr = fmt.Sprintf("SECURITY LABEL FOR %s ON %s %s IS '%s';", obj.SecurityLabelProvider, objectType, objectName, escapedLabel)
	}
	return securityLabelStr
}

func PrintDefaultPrivilegesStatements(metadataFile *utils.FileWithByteCount, objToc *toc.TOC, privileges []DefaultPrivileges) {
	for _, priv := range privileges {
		statements := make([]string, 0)
		roleStr := ""
		if priv.Owner != "" {
			roleStr = fmt.Sprintf(" FOR ROLE %s", priv.Owner)
		}
		schemaStr := ""
		if priv.Schema != "" {
			schemaStr = fmt.Sprintf(" IN SCHEMA %s", priv.Schema)
		}

		objectType := ""
		switch priv.ObjectType {
		case "r":
			objectType = toc.OBJ_TABLE
		case "S":
			objectType = toc.OBJ_SEQUENCE
		case "f":
			objectType = toc.OBJ_FUNCTION
		case "T":
			objectType = toc.OBJ_TYPE
		}
		alterPrefix := fmt.Sprintf("ALTER DEFAULT PRIVILEGES%s%s", roleStr, schemaStr)
		statements = append(statements, fmt.Sprintf("%s REVOKE ALL ON %sS FROM PUBLIC;", alterPrefix, objectType))
		if priv.Owner != "" {
			statements = append(statements, fmt.Sprintf("%s REVOKE ALL ON %sS FROM %s;", alterPrefix, objectType, priv.Owner))
		}
		for _, acl := range priv.Privileges {
			grantee := ""
			if acl.Grantee == "" {
				grantee = "PUBLIC"
			} else {
				grantee = acl.Grantee
			}
			privStr, privWithGrantStr := createPrivilegeStrings(acl, objectType)
			if privStr != "" {
				statements = append(statements, fmt.Sprintf("%s GRANT %s ON %sS TO %s;", alterPrefix, privStr, objectType, grantee))
			}
			if privWithGrantStr != "" {
				statements = append(statements, fmt.Sprintf("%s GRANT %s ON %sS TO %s WITH GRANT OPTION;", alterPrefix, privWithGrantStr, objectType, grantee))
			}
		}
		start := metadataFile.ByteCount
		metadataFile.MustPrintln("\n\n" + strings.Join(statements, "\n"))
		section, entry := priv.GetMetadataEntry()
		objToc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount, []uint32{0, 0})
	}
}

func ConstructDefaultPrivileges(results []DefaultPrivilegesQueryStruct) []DefaultPrivileges {
	if len(results) == 0 {
		return []DefaultPrivileges{}
	}
	defaultPrivileges := make([]DefaultPrivileges, 0)
	var priv DefaultPrivileges
	var currentOid uint32
	for _, result := range results {
		privilegesStr := ""
		if result.Kind == "Empty" {
			privilegesStr = "GRANTEE=/GRANTOR"
		} else if result.Privileges.Valid {
			privilegesStr = result.Privileges.String
		}
		if result.Oid != currentOid {
			if currentOid != 0 {
				priv.Privileges = sortACLs(priv.Privileges)
				defaultPrivileges = append(defaultPrivileges, priv)
			}
			currentOid = result.Oid
			priv = DefaultPrivileges{}
			priv.Privileges = make([]ACL, 0)
			priv.Owner = result.Owner
			priv.Schema = result.Schema
			priv.ObjectType = result.ObjectType
		}

		privileges := ParseACL(privilegesStr)
		if privileges != nil {
			priv.Privileges = append(priv.Privileges, *privileges)
		}
	}
	priv.Privileges = sortACLs(priv.Privileges)
	defaultPrivileges = append(defaultPrivileges, priv)

	return defaultPrivileges
}
