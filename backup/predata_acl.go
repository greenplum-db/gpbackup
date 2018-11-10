package backup

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/greenplum-db/gpbackup/utils"
)

var ACLRegex = regexp.MustCompile(`^(.*)=([a-zA-Z\*]*)/(.*)$`)

/*
 * Structs and functions relating to generic metadata handling.
 */

type ObjectMetadata struct {
	Privileges            []ACL
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

func NewPrintObjectMetadata(file *utils.FileWithByteCount, toc *utils.TOC, metadata ObjectMetadata, obj utils.TOCObject, objectName string, owningTable ...string) {
	_, entry := obj.GetMetadataEntry(0, 0)
	statements := []string{}
	if comment := metadata.GetCommentStatement(objectName, entry.ObjectType, owningTable...); comment != "" {
		statements = append(statements, comment)
	}
	if owner := metadata.GetOwnerStatement(objectName, entry.ObjectType); owner != "" {
		if !(connectionPool.Version.Before("5") && entry.ObjectType == "LANGUAGE") {
			// Languages have implicit owners in 4.3, but do not support ALTER OWNER
			statements = append(statements, owner)
		}
	}
	if privileges := metadata.GetPrivilegesStatements(objectName, entry.ObjectType); privileges != "" {
		statements = append(statements, privileges)
	}
	if securityLabel := metadata.GetSecurityLabelStatement(objectName, entry.ObjectType); securityLabel != "" {
		statements = append(statements, securityLabel)
	}
	PrintStatements(file, toc, obj, statements)
}

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
	if securityLabel := obj.GetSecurityLabelStatement(objectName, objectType); securityLabel != "" {
		file.MustPrintln(securityLabel)
	}
}

func ConstructMetadataMap(results []MetadataQueryStruct) MetadataMap {
	metadataMap := make(MetadataMap)
	if len(results) == 0 {
		return MetadataMap{}
	}
	var metadata ObjectMetadata
	quotedRoleNames := GetQuotedRoleNames(connectionPool)
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
			metadata.Comment = result.Comment
			metadata.SecurityLabelProvider = result.SecurityLabelProvider
			metadata.SecurityLabel = result.SecurityLabel
		}

		privileges := ParseACL(privilegesStr, quotedRoleNames)
		if privileges != nil {
			metadata.Privileges = append(metadata.Privileges, *privileges)
		}
	}
	metadata.Privileges = sortACLs(metadata.Privileges)
	metadataMap[currentUniqueID] = metadata
	return metadataMap
}

func ParseACL(aclStr string, quotedRoleNames map[string]string) *ACL {
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
	statements := []string{}
	typeStr := fmt.Sprintf("%s ", objectType)
	if objectType == "VIEW" || objectType == "FOREIGN TABLE" {
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
	case "FOREIGN TABLE":
		hasAllPrivileges = acl.Select && acl.Insert && acl.Update && acl.Delete && acl.References && acl.Trigger
		hasAllPrivilegesWithGrant = acl.SelectWithGrant && acl.InsertWithGrant && acl.UpdateWithGrant && acl.DeleteWithGrant &&
			acl.ReferencesWithGrant && acl.TriggerWithGrant
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
	case "TYPE":
		hasAllPrivileges = acl.Usage
		hasAllPrivilegesWithGrant = acl.UsageWithGrant
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

	return privStr, privWithGrantStr

}
func (obj ObjectMetadata) GetOwnerStatement(objectName string, objectType string) string {
	typeStr := objectType
	if connectionPool.Version.Before("6") && (objectType == "SEQUENCE" || objectType == "VIEW") {
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
		escapedComment := utils.EscapeSingleQuotes(obj.Comment)
		commentStr = fmt.Sprintf("\n\nCOMMENT ON %s %s%s IS '%s';", objectType, objectName, tableStr, escapedComment)
	}
	return commentStr
}

func (obj ObjectMetadata) GetSecurityLabelStatement(objectName string, objectType string) string {
	securityLabelStr := ""
	if obj.SecurityLabel != "" {
		escapedLabel := utils.EscapeSingleQuotes(obj.SecurityLabel)
		securityLabelStr = fmt.Sprintf("\n\nSECURITY LABEL FOR %s ON %s %s IS '%s';", obj.SecurityLabelProvider, objectType, objectName, escapedLabel)
	}
	return securityLabelStr
}

func PrintDefaultPrivilegesStatements(metadataFile *utils.FileWithByteCount, toc *utils.TOC, privileges []DefaultPrivileges) {
	for _, priv := range privileges {
		statements := []string{}
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
			objectType = "TABLE"
		case "S":
			objectType = "SEQUENCE"
		case "f":
			objectType = "FUNCTION"
		case "T":
			objectType = "TYPE"
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
		toc.AddPostdataEntry(priv.Schema, "", "DEFAULT PRIVILEGES", "", start, metadataFile)
	}
}

func ConstructDefaultPrivileges(results []DefaultPrivilegesQueryStruct) []DefaultPrivileges {
	if len(results) == 0 {
		return []DefaultPrivileges{}
	}
	quotedRoles := GetQuotedRoleNames(connectionPool)

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

		privileges := ParseACL(privilegesStr, quotedRoles)
		if privileges != nil {
			priv.Privileges = append(priv.Privileges, *privileges)
		}
	}
	priv.Privileges = sortACLs(priv.Privileges)
	defaultPrivileges = append(defaultPrivileges, priv)

	return defaultPrivileges
}
