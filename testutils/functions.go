package testutils

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"regexp"
	"strings"

	"github.com/blang/semver"
	"github.com/greenplum-db/gp-common-go-libs/structmatcher"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/helper"
	"github.com/greenplum-db/gpbackup/restore"
	"github.com/greenplum-db/gpbackup/utils"

	"github.com/jmoiron/sqlx"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
)

/*
 * Functions for setting up the test environment and mocking out variables
 */

func SetupTestEnvironment() (*utils.DBConn, sqlmock.Sqlmock, *utils.Logger, *gbytes.Buffer, *gbytes.Buffer, *gbytes.Buffer) {
	testLogger, testStdout, testStderr, testLogfile := SetupTestLogger()
	connection, mock := CreateAndConnectMockDB(1)
	SetupTestCluster()
	utils.System = utils.InitializeSystemFunctions()
	backup.SetVersion("0.1.0")
	return connection, mock, testLogger, testStdout, testStderr, testLogfile
}

func CreateAndConnectMockDB(numConns int) (*utils.DBConn, sqlmock.Sqlmock) {
	mockdb, mock := CreateMockDB()
	driver := TestDriver{DB: mockdb, DBName: "testdb", User: "testrole"}
	connection := utils.NewDBConn("testdb")
	connection.Driver = driver
	connection.Connect(numConns)
	SetDBVersion(connection, "5.1.0")
	backup.SetConnection(connection)
	restore.SetConnection(connection)
	backup.InitializeMetadataParams(connection)
	return connection, mock
}

/*
 * This function creates a test logger and assigns it to both backup.logger and utils.logger,
 * so no assignment to those variables in the tests is necessary.  The logger and gbytes.buffers
 * are returned to allow checking for output written to those buffers during tests if desired.
 */
func SetupTestLogger() (*utils.Logger, *gbytes.Buffer, *gbytes.Buffer, *gbytes.Buffer) {
	testStdout := gbytes.NewBuffer()
	testStderr := gbytes.NewBuffer()
	testLogfile := gbytes.NewBuffer()
	testLogger := utils.NewLogger(testStdout, testStderr, testLogfile, "gbytes.Buffer", utils.LOGINFO, "testProgram:testUser:testHost:000000-[%s]:-")
	backup.SetLogger(testLogger)
	restore.SetLogger(testLogger)
	utils.SetLogger(testLogger)
	helper.SetLogger(testLogger)
	return testLogger, testStdout, testStderr, testLogfile
}

func SetupTestCluster() {
	testCluster := SetDefaultSegmentConfiguration()
	backup.SetCluster(testCluster)
	restore.SetCluster(testCluster)
}

func CreateMockDB() (*sqlx.DB, sqlmock.Sqlmock) {
	db, mock, err := sqlmock.New()
	mockdb := sqlx.NewDb(db, "sqlmock")
	if err != nil {
		Fail("Could not create mock database connection")
	}
	return mockdb, mock
}

func SetDefaultSegmentConfiguration() utils.Cluster {
	configMaster := utils.SegConfig{ContentID: -1, Hostname: "localhost", DataDir: "gpseg-1"}
	configSegOne := utils.SegConfig{ContentID: 0, Hostname: "localhost", DataDir: "gpseg0"}
	configSegTwo := utils.SegConfig{ContentID: 1, Hostname: "localhost", DataDir: "gpseg1"}
	cluster := utils.NewCluster([]utils.SegConfig{configMaster, configSegOne, configSegTwo}, "", "20170101010101", "gpseg")
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
		Select:     objType == "PROTOCOL" || objType == "SEQUENCE" || objType == "TABLE" || objType == "VIEW",
		Insert:     objType == "PROTOCOL" || objType == "TABLE" || objType == "VIEW",
		Update:     objType == "SEQUENCE" || objType == "TABLE" || objType == "VIEW",
		Delete:     objType == "TABLE" || objType == "VIEW",
		Truncate:   objType == "TABLE" || objType == "VIEW",
		References: objType == "TABLE" || objType == "VIEW",
		Trigger:    objType == "TABLE" || objType == "VIEW",
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
	return backup.Type{Oid: 1, Schema: "public", Name: typeName, Type: typeType, Input: "", Output: "", Receive: "", Send: "", ModIn: "", ModOut: "", InternalLength: -1, IsPassedByValue: false, Alignment: "c", Storage: "p", DefaultVal: "", Element: "", Category: "U", Preferred: false, Delimiter: "", EnumLabels: "", BaseType: "", NotNull: false, Attributes: nil, DependsUpon: nil}
}

/*
 * Wrapper functions around gomega operators for ease of use in tests
 */

func ExpectBegin(mock sqlmock.Sqlmock) {
	fakeResult := TestResult{Rows: 0}
	mock.ExpectBegin()
	mock.ExpectExec("SET TRANSACTION(.*)").WillReturnResult(fakeResult)
}

func ExpectRegexp(buffer *gbytes.Buffer, testStr string) {
	Expect(buffer).Should(gbytes.Say(regexp.QuoteMeta(testStr)))
}

func NotExpectRegexp(buffer *gbytes.Buffer, testStr string) {
	Expect(buffer).ShouldNot(gbytes.Say(regexp.QuoteMeta(testStr)))
}

func ExpectRegex(result string, testStr string) {
	Expect(result).Should(MatchRegexp(regexp.QuoteMeta(testStr)))
}

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
		Fail(fmt.Sprintf("Actual TOC entries:\n\n%s\n\ndid not match expected contents (ignoring whitespace):\n\n%s", formatEntries(entries, hunks), formatContents(expected)))
	}
}

func ExpectEntry(entries []utils.MetadataEntry, index int, schema, referenceObject, name, objectType string) {
	Expect(len(entries)).To(BeNumerically(">", index))
	structmatcher.ExpectStructsToMatchExcluding(entries[index], utils.MetadataEntry{Schema: schema, Name: name, ObjectType: objectType, ReferenceObject: referenceObject, StartByte: 0, EndByte: 0}, "StartByte", "EndByte")
}

func ExpectPathToExist(path string) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		Fail(fmt.Sprintf("Expected %s to exist", path))
	}
}

func ShouldPanicWithMessage(message string) {
	if r := recover(); r != nil {
		errorMessage := strings.TrimSpace(fmt.Sprintf("%v", r))
		if !strings.Contains(errorMessage, message) {
			Fail(fmt.Sprintf("Expected panic message '%s', got '%s'", message, errorMessage))
		}
	} else {
		Fail("Function did not panic as expected")
	}
}

func AssertQueryRuns(dbconn *utils.DBConn, query string) {
	_, err := dbconn.Exec(query)
	Expect(err).To(BeNil(), "%s", query)
}

func ExecuteSQLFile(dbconn *utils.DBConn, filename string) {
	connStr := []string{
		"-U", dbconn.User,
		"-d", dbconn.DBName,
		"-h", dbconn.Host,
		"-p", fmt.Sprintf("%d", dbconn.Port),
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

func OidFromCast(connection *utils.DBConn, castSource uint32, castTarget uint32) uint32 {
	query := fmt.Sprintf("SELECT c.oid FROM pg_cast c WHERE castsource = '%d' AND casttarget = '%d'", castSource, castTarget)
	result := struct {
		Oid uint32
	}{}
	err := connection.Get(&result, query)
	utils.CheckError(err)
	return result.Oid
}

func OidFromObjectName(dbconn *utils.DBConn, schemaName string, objectName string, params backup.MetadataQueryParams) uint32 {
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
	err := dbconn.Get(&result, query)
	utils.CheckError(err)
	return result.Oid
}

func GetUserByID(dbconn *utils.DBConn, oid uint32) string {
	return utils.SelectString(dbconn, fmt.Sprintf("SELECT rolname AS string FROM pg_roles WHERE oid = %d", oid))
}

func SkipIf4(dbconn *utils.DBConn) {
	if dbconn.Version.Before("5") {
		Skip("Test not applicable to GPDB4")
	}
}

func SkipIfNot4(dbconn *utils.DBConn) {
	if dbconn.Version.AtLeast("5") {
		Skip("Test only applicable to GPDB4")
	}
}

func SkipIfBefore6(dbconn *utils.DBConn) {
	if dbconn.Version.Before("6") {
		Skip("Test only applicable to GPDB6 and above")
	}
}

func InitializeTestTOC(buffer io.Writer, which string) (*utils.TOC, *utils.FileWithByteCount) {
	toc := &utils.TOC{}
	toc.InitializeEntryMap()
	backupfile := utils.NewFileWithByteCount(buffer)
	backupfile.Filename = which
	return toc, backupfile
}

func SetDBVersion(dbconn *utils.DBConn, versionStr string) {
	dbconn.Version = utils.GPDBVersion{VersionString: versionStr, SemVer: semver.MustParse(versionStr)}
}
