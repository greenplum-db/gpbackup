package testutils

import (
	"fmt"
	"io"
	"os/exec"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/structmatcher"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/restore"
	"github.com/greenplum-db/gpbackup/utils"
	"github.com/sergi/go-diff/diffmatchpatch"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
)

/*
 * Functions for setting up the test environment and mocking out variables
 */

func SetupTestEnvironment() (*dbconn.DBConn, sqlmock.Sqlmock, *gbytes.Buffer, *gbytes.Buffer, *gbytes.Buffer) {
	connection, mock, testStdout, testStderr, testLogfile := testhelper.SetupTestEnvironment()
	SetupTestCluster()
	backup.SetVersion("0.1.0")
	return connection, mock, testStdout, testStderr, testLogfile
}

func CreateAndConnectMockDB(numConns int) (*dbconn.DBConn, sqlmock.Sqlmock) {
	connection, mock := testhelper.CreateAndConnectMockDB(numConns)
	backup.SetConnection(connection)
	restore.SetConnection(connection)
	backup.InitializeMetadataParams(connection)
	return connection, mock
}

func SetupTestCluster() {
	testCluster := SetDefaultSegmentConfiguration()
	backup.SetCluster(testCluster)
	restore.SetCluster(testCluster)
	testFPInfo := utils.NewFilePathInfo(testCluster, "", "20170101010101", "gpseg")
	backup.SetFPInfo(testFPInfo)
	restore.SetFPInfo(testFPInfo)
}

func SetupTestDbConn(dbname string) *dbconn.DBConn {
	conn := dbconn.NewDBConnFromEnvironment(dbname)
	conn.MustConnect(1)
	return conn
}

func SetDefaultSegmentConfiguration() *cluster.Cluster {
	configMaster := cluster.SegConfig{ContentID: -1, Hostname: "localhost", DataDir: "gpseg-1"}
	configSegOne := cluster.SegConfig{ContentID: 0, Hostname: "localhost", DataDir: "gpseg0"}
	configSegTwo := cluster.SegConfig{ContentID: 1, Hostname: "localhost", DataDir: "gpseg1"}
	cluster := cluster.NewCluster([]cluster.SegConfig{configMaster, configSegOne, configSegTwo})
	return cluster
}

// objType should be an all-caps string like TABLE, INDEX, etc.
func DefaultMetadataMap(objType string, hasPrivileges bool, hasOwner bool, hasComment bool) backup.MetadataMap {
	privileges := []backup.ACL{}
	if hasPrivileges {
		privileges = []backup.ACL{DefaultACLForType("testrole", objType)}
	}
	owner := ""
	if hasOwner {
		owner = "testrole"
	}
	comment := ""
	if hasComment {
		n := ""
		switch objType[0] {
		case 'A', 'E', 'I', 'O', 'U':
			n = "n"
		}
		comment = fmt.Sprintf("This is a%s %s comment.", n, strings.ToLower(objType))
	}
	return backup.MetadataMap{
		1: {Privileges: privileges, Owner: owner, Comment: comment},
	}
}

func DefaultACLForType(grantee string, objType string) backup.ACL {
	return backup.ACL{
		Grantee:    grantee,
		Select:     objType == "PROTOCOL" || objType == "SEQUENCE" || objType == "TABLE" || objType == "VIEW" || objType == "FOREIGN TABLE",
		Insert:     objType == "PROTOCOL" || objType == "TABLE" || objType == "VIEW" || objType == "FOREIGN TABLE",
		Update:     objType == "SEQUENCE" || objType == "TABLE" || objType == "VIEW" || objType == "FOREIGN TABLE",
		Delete:     objType == "TABLE" || objType == "VIEW" || objType == "FOREIGN TABLE",
		Truncate:   objType == "TABLE" || objType == "VIEW",
		References: objType == "TABLE" || objType == "VIEW" || objType == "FOREIGN TABLE",
		Trigger:    objType == "TABLE" || objType == "VIEW" || objType == "FOREIGN TABLE",
		Usage:      objType == "LANGUAGE" || objType == "SCHEMA" || objType == "SEQUENCE" || objType == "FOREIGN DATA WRAPPER" || objType == "FOREIGN SERVER",
		Execute:    objType == "FUNCTION",
		Create:     objType == "DATABASE" || objType == "SCHEMA" || objType == "TABLESPACE",
		Temporary:  objType == "DATABASE",
		Connect:    objType == "DATABASE",
	}
}

func DefaultACLForTypeWithGrant(grantee string, objType string) backup.ACL {
	return backup.ACL{
		Grantee:             grantee,
		SelectWithGrant:     objType == "PROTOCOL" || objType == "SEQUENCE" || objType == "TABLE" || objType == "VIEW",
		InsertWithGrant:     objType == "PROTOCOL" || objType == "TABLE" || objType == "VIEW",
		UpdateWithGrant:     objType == "SEQUENCE" || objType == "TABLE" || objType == "VIEW",
		DeleteWithGrant:     objType == "TABLE" || objType == "VIEW",
		TruncateWithGrant:   objType == "TABLE" || objType == "VIEW",
		ReferencesWithGrant: objType == "TABLE" || objType == "VIEW",
		TriggerWithGrant:    objType == "TABLE" || objType == "VIEW",
		UsageWithGrant:      objType == "LANGUAGE" || objType == "SCHEMA" || objType == "SEQUENCE" || objType == "FOREIGN DATA WRAPPER" || objType == "FOREIGN SERVER",
		ExecuteWithGrant:    objType == "FUNCTION",
		CreateWithGrant:     objType == "DATABASE" || objType == "SCHEMA" || objType == "TABLESPACE",
		TemporaryWithGrant:  objType == "DATABASE",
		ConnectWithGrant:    objType == "DATABASE",
	}
}

func DefaultACLWithout(grantee string, objType string, revoke ...string) backup.ACL {
	defaultACL := DefaultACLForType(grantee, objType)
	for _, priv := range revoke {
		switch priv {
		case "SELECT":
			defaultACL.Select = false
		case "INSERT":
			defaultACL.Insert = false
		case "UPDATE":
			defaultACL.Update = false
		case "DELETE":
			defaultACL.Delete = false
		case "TRUNCATE":
			defaultACL.Truncate = false
		case "REFERENCES":
			defaultACL.References = false
		case "TRIGGER":
			defaultACL.Trigger = false
		case "EXECUTE":
			defaultACL.Execute = false
		case "USAGE":
			defaultACL.Usage = false
		case "CREATE":
			defaultACL.Create = false
		case "TEMPORARY":
			defaultACL.Temporary = false
		case "CONNECT":
			defaultACL.Connect = false
		}
	}
	return defaultACL
}

func DefaultACLWithGrantWithout(grantee string, objType string, revoke ...string) backup.ACL {
	defaultACL := DefaultACLForTypeWithGrant(grantee, objType)
	for _, priv := range revoke {
		switch priv {
		case "SELECT":
			defaultACL.SelectWithGrant = false
		case "INSERT":
			defaultACL.InsertWithGrant = false
		case "UPDATE":
			defaultACL.UpdateWithGrant = false
		case "DELETE":
			defaultACL.DeleteWithGrant = false
		case "TRUNCATE":
			defaultACL.TruncateWithGrant = false
		case "REFERENCES":
			defaultACL.ReferencesWithGrant = false
		case "TRIGGER":
			defaultACL.TriggerWithGrant = false
		case "EXECUTE":
			defaultACL.ExecuteWithGrant = false
		case "USAGE":
			defaultACL.UsageWithGrant = false
		case "CREATE":
			defaultACL.CreateWithGrant = false
		case "TEMPORARY":
			defaultACL.TemporaryWithGrant = false
		case "CONNECT":
			defaultACL.ConnectWithGrant = false
		}
	}
	return defaultACL
}

func DefaultTypeDefinition(typeType string, typeName string) backup.Type {
	return backup.Type{Oid: 1, Schema: "public", Name: typeName, Type: typeType, Input: "", Output: "", Receive: "", Send: "", ModIn: "", ModOut: "", InternalLength: -1, IsPassedByValue: false, Alignment: "c", Storage: "p", DefaultVal: "", Element: "", Category: "U", Preferred: false, Delimiter: "", EnumLabels: "", BaseType: "", NotNull: false, Attributes: nil}
}

/*
 * Wrapper functions around gomega operators for ease of use in tests
 */

func SliceBufferByEntries(entries []utils.MetadataEntry, buffer *gbytes.Buffer) ([]string, string) {
	contents := buffer.Contents()
	hunks := []string{}
	length := uint64(len(contents))
	var end uint64
	for _, entry := range entries {
		start := entry.StartByte
		end = entry.EndByte
		if start > length {
			start = length
		}
		if end > length {
			end = length
		}
		hunks = append(hunks, string(contents[start:end]))
	}
	return hunks, string(contents[end:])
}

func CompareSlicesIgnoringWhitespace(actual []string, expected []string) bool {
	if len(actual) != len(expected) {
		return false
	}
	for i := range actual {
		if strings.TrimSpace(actual[i]) != expected[i] {
			return false
		}
	}
	return true
}

func formatEntries(entries []utils.MetadataEntry, slice []string) string {
	formatted := ""
	for i, item := range slice {
		formatted += fmt.Sprintf("%v -> %q\n", entries[i], item)
	}
	return formatted
}

func formatContents(slice []string) string {
	formatted := ""
	for _, item := range slice {
		formatted += fmt.Sprintf("%q\n", item)
	}
	return formatted
}

func formatDiffs(actual []string, expected []string) string {
	dmp := diffmatchpatch.New()
	diffs := ""
	for idx := range actual {
		diffs += dmp.DiffPrettyText(dmp.DiffMain(expected[idx], actual[idx], false))
	}
	return diffs
}

func AssertBufferContents(entries []utils.MetadataEntry, buffer *gbytes.Buffer, expected ...string) {
	if len(entries) == 0 {
		Fail("TOC is empty")
	}
	hunks, remaining := SliceBufferByEntries(entries, buffer)
	if remaining != "" {
		Fail(fmt.Sprintf("Buffer contains extra contents that are not being counted by TOC:\n%s\n\nActual TOC entries were:\n\n%s", remaining, formatEntries(entries, hunks)))
	}
	ok := CompareSlicesIgnoringWhitespace(hunks, expected)
	if !ok {
		Fail(fmt.Sprintf("Actual TOC entries:\n\n%s\n\ndid not match expected contents (ignoring whitespace):\n\n%s \n\nDiff:\n>>%s\x1b[31m<<", formatEntries(entries, hunks), formatContents(expected), formatDiffs(hunks, expected)))
	}
}

func ExpectEntry(entries []utils.MetadataEntry, index int, schema, referenceObject, name, objectType string) {
	Expect(len(entries)).To(BeNumerically(">", index))
	structmatcher.ExpectStructsToMatchExcluding(entries[index], utils.MetadataEntry{Schema: schema, Name: name, ObjectType: objectType, ReferenceObject: referenceObject, StartByte: 0, EndByte: 0}, "StartByte", "EndByte")
}

func ExecuteSQLFile(connection *dbconn.DBConn, filename string) {
	connStr := []string{
		"-U", connection.User,
		"-d", connection.DBName,
		"-h", connection.Host,
		"-p", fmt.Sprintf("%d", connection.Port),
		"-f", filename,
		"-v", "ON_ERROR_STOP=1",
		"-q",
	}
	out, err := exec.Command("psql", connStr...).CombinedOutput()
	if err != nil {
		Fail(fmt.Sprintf("Execution of SQL file encountered an error: %s", out))
	}
}

func BufferLength(buffer *gbytes.Buffer) uint64 {
	return uint64(len(buffer.Contents()))
}

func OidFromCast(connection *dbconn.DBConn, castSource uint32, castTarget uint32) uint32 {
	query := fmt.Sprintf("SELECT c.oid FROM pg_cast c WHERE castsource = '%d' AND casttarget = '%d'", castSource, castTarget)
	result := struct {
		Oid uint32
	}{}
	err := connection.Get(&result, query)
	if err != nil {
		Fail(fmt.Sprintf("Execution of query failed: %v", err))
	}
	return result.Oid
}

func OidFromObjectName(connection *dbconn.DBConn, schemaName string, objectName string, params backup.MetadataQueryParams) uint32 {
	catalogTable := params.CatalogTable
	if params.OidTable != "" {
		catalogTable = params.OidTable
	}
	schemaStr := ""
	if schemaName != "" {
		schemaStr = fmt.Sprintf(" AND %s = (SELECT oid FROM pg_namespace WHERE nspname = '%s')", params.SchemaField, schemaName)
	}
	query := fmt.Sprintf("SELECT oid FROM %s WHERE %s ='%s'%s", catalogTable, params.NameField, objectName, schemaStr)
	result := struct {
		Oid uint32
	}{}
	err := connection.Get(&result, query)
	if err != nil {
		Fail(fmt.Sprintf("Execution of query failed: %v", err))
	}
	return result.Oid
}

func GetUserByID(connection *dbconn.DBConn, oid uint32) string {
	return dbconn.MustSelectString(connection, fmt.Sprintf("SELECT rolname AS string FROM pg_roles WHERE oid = %d", oid))
}

func SkipIfNot4(connection *dbconn.DBConn) {
	if connection.Version.AtLeast("5") {
		Skip("Test only applicable to GPDB4")
	}
}

func SkipIfBefore5(connection *dbconn.DBConn) {
	if connection.Version.Before("5") {
		Skip("Test only applicable to GPDB5 and above")
	}
}

func SkipIfBefore6(connection *dbconn.DBConn) {
	if connection.Version.Before("6") {
		Skip("Test only applicable to GPDB6 and above")
	}
}

func InitializeTestTOC(buffer io.Writer, which string) (*utils.TOC, *utils.FileWithByteCount) {
	toc := &utils.TOC{}
	toc.InitializeMetadataEntryMap()
	backupfile := utils.NewFileWithByteCount(buffer)
	backupfile.Filename = which
	return toc, backupfile
}
