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
	"github.com/greenplum-db/gpbackup/backup_filepath"
	"github.com/greenplum-db/gpbackup/restore"
	"github.com/greenplum-db/gpbackup/utils"
	"github.com/sergi/go-diff/diffmatchpatch"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
	"github.com/DATA-DOG/go-sqlmock"
)

/*
 * Functions for setting up the test environment and mocking out variables
 */

func SetupTestEnvironment() (*dbconn.DBConn, sqlmock.Sqlmock, *gbytes.Buffer, *gbytes.Buffer, *gbytes.Buffer) {
	connectionPool, mock, testStdout, testStderr, testLogfile := testhelper.SetupTestEnvironment()
	SetupTestCluster()
	backup.SetVersion("0.1.0")
	return connectionPool, mock, testStdout, testStderr, testLogfile
}

func SetupTestCluster() *cluster.Cluster {
	testCluster := SetDefaultSegmentConfiguration()
	backup.SetCluster(testCluster)
	restore.SetCluster(testCluster)
	testFPInfo := backup_filepath.NewFilePathInfo(testCluster, "", "20170101010101", "gpseg")
	backup.SetFPInfo(testFPInfo)
	restore.SetFPInfo(testFPInfo)
	return testCluster
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

func SetupTestFilespace(connectionPool *dbconn.DBConn, testCluster *cluster.Cluster) {
	remoteOutput := testCluster.GenerateAndExecuteCommand("Creating filespace test directory", func(contentID int) string {
		return fmt.Sprintf("mkdir -p /tmp/test_dir")
	}, cluster.ON_HOSTS_AND_MASTER)
	if remoteOutput.NumErrors != 0 {
		Fail("Could not create filespace test directory on 1 or more hosts")
	}
	// Construct a filespace config like the one that gpfilespace generates
	filespaceConfigQuery := `COPY (SELECT hostname || ':' || dbid || ':/tmp/test_dir/' || preferred_role || content FROM gp_segment_configuration AS subselect) TO '/tmp/temp_filespace_config';`
	testhelper.AssertQueryRuns(connectionPool, filespaceConfigQuery)
	out, err := exec.Command("bash", "-c", "echo \"filespace:test_dir\" > /tmp/filespace_config").CombinedOutput()
	if err != nil {
		Fail(fmt.Sprintf("Cannot create test filespace configuration: %s: %s", out, err.Error()))
	}
	out, err = exec.Command("bash", "-c", "cat /tmp/temp_filespace_config >> /tmp/filespace_config").CombinedOutput()
	if err != nil {
		Fail(fmt.Sprintf("Cannot finalize test filespace configuration: %s: %s", out, err.Error()))
	}
	// Create the filespace and verify it was created successfully
	out, err = exec.Command("bash", "-c", "gpfilespace --config /tmp/filespace_config").CombinedOutput()
	if err != nil {
		Fail(fmt.Sprintf("Cannot create test filespace: %s: %s", out, err.Error()))
	}
	filespaceName := dbconn.MustSelectString(connectionPool, "SELECT fsname AS string FROM pg_filespace WHERE fsname = 'test_dir';")
	if filespaceName != "test_dir" {
		Fail("Filespace test_dir was not successfully created")
	}
}

func DestroyTestFilespace(connectionPool *dbconn.DBConn) {
	filespaceName := dbconn.MustSelectString(connectionPool, "SELECT fsname AS string FROM pg_filespace WHERE fsname = 'test_dir';")
	if filespaceName != "test_dir" {
		return
	}
	testhelper.AssertQueryRuns(connectionPool, "DROP FILESPACE test_dir")
	out, err := exec.Command("bash", "-c", "rm -rf /tmp/test_dir /tmp/filespace_config /tmp/temp_filespace_config").CombinedOutput()
	if err != nil {
		Fail(fmt.Sprintf("Could not remove test filespace directory and configuration files: %s: %s", out, err.Error()))
	}
}

func DefaultMetadata(objType string, hasPrivileges bool, hasOwner bool, hasComment bool, hasSecurityLabel bool) backup.ObjectMetadata {
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
	securityLabelProvider := ""
	securityLabel := ""
	if hasSecurityLabel {
		securityLabelProvider = "dummy"
		securityLabel = "unclassified"
	}
	return backup.ObjectMetadata{Privileges: privileges, Owner: owner, Comment: comment, SecurityLabelProvider: securityLabelProvider, SecurityLabel: securityLabel}

}

// objType should be an all-caps string like TABLE, INDEX, etc.
func DefaultMetadataMap(objType string, hasPrivileges bool, hasOwner bool, hasComment bool, hasSecurityLabel bool) backup.MetadataMap {
	return backup.MetadataMap{
		backup.UniqueID{ClassID: ClassIDFromObjectName(objType), Oid: 1}: DefaultMetadata(objType, hasPrivileges, hasOwner, hasComment, hasSecurityLabel),
	}
}

var objNameToClassID = map[string]uint32{
	"AGGREGATE":                 1255,
	"CAST":                      2605,
	"COLLATION":                 3456,
	"CONSTRAINT":                2606,
	"CONVERSION":                2607,
	"DATABASE":                  1262,
	"DOMAIN":                    1247,
	"EVENT TRIGGER":             3466,
	"EXTENSION":                 3079,
	"FOREIGN DATA WRAPPER":      2328,
	"FOREIGN SERVER":            1417,
	"FUNCTION":                  1255,
	"INDEX":                     2610,
	"LANGUAGE":                  2612,
	"OPERATOR CLASS":            2616,
	"OPERATOR FAMILY":           2753,
	"OPERATOR":                  2617,
	"PROTOCOL":                  7175,
	"RESOURCE GROUP":            6436,
	"RESOURCE QUEUE":            6026,
	"ROLE":                      1260,
	"RULE":                      2618,
	"SCHEMA":                    2615,
	"SEQUENCE":                  1259,
	"TABLE":                     1259,
	"TABLESPACE":                1213,
	"TEXT SEARCH CONFIGURATION": 3602,
	"TEXT SEARCH DICTIONARY":    3600,
	"TEXT SEARCH PARSER":        3601,
	"TEXT SEARCH TEMPLATE":      3764,
	"TRIGGER":                   2620,
	"TYPE":                      1247,
	"USER MAPPING":              1418,
	"VIEW":                      1259,
}

func ClassIDFromObjectName(objName string) uint32 {
	return objNameToClassID[objName]

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
		Execute:    objType == "FUNCTION" || objType == "AGGREGATE",
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

func ExecuteSQLFile(connectionPool *dbconn.DBConn, filename string) {
	connStr := []string{
		"-U", connectionPool.User,
		"-d", connectionPool.DBName,
		"-h", connectionPool.Host,
		"-p", fmt.Sprintf("%d", connectionPool.Port),
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

func OidFromCast(connectionPool *dbconn.DBConn, castSource uint32, castTarget uint32) uint32 {
	query := fmt.Sprintf("SELECT c.oid FROM pg_cast c WHERE castsource = '%d' AND casttarget = '%d'", castSource, castTarget)
	result := struct {
		Oid uint32
	}{}
	err := connectionPool.Get(&result, query)
	if err != nil {
		Fail(fmt.Sprintf("Execution of query failed: %v", err))
	}
	return result.Oid
}

func OidFromObjectName(connectionPool *dbconn.DBConn, schemaName string, objectName string, params backup.MetadataQueryParams) uint32 {
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
	err := connectionPool.Get(&result, query)
	if err != nil {
		Fail(fmt.Sprintf("Execution of query failed: %v", err))
	}
	return result.Oid
}

func UniqueIDFromObjectName(connectionPool *dbconn.DBConn, schemaName string, objectName string, params backup.MetadataQueryParams) backup.UniqueID {
	query := fmt.Sprintf("SELECT '%s'::regclass::oid", params.CatalogTable)
	result := struct {
		Oid uint32
	}{}
	err := connectionPool.Get(&result, query)
	if err != nil {
		Fail(fmt.Sprintf("Execution of query failed: %v", err))
	}

	return backup.UniqueID{ClassID: result.Oid, Oid: OidFromObjectName(connectionPool, schemaName, objectName, params)}
}

func GetUserByID(connectionPool *dbconn.DBConn, oid uint32) string {
	return dbconn.MustSelectString(connectionPool, fmt.Sprintf("SELECT rolname AS string FROM pg_roles WHERE oid = %d", oid))
}

func CreateSecurityLabelIfGPDB6(connectionPool *dbconn.DBConn, objectType string, objectName string) {
	if connectionPool.Version.AtLeast("6") {
		testhelper.AssertQueryRuns(connectionPool, fmt.Sprintf("SECURITY LABEL FOR dummy ON %s %s IS 'unclassified';", objectType, objectName))
	}
}

func SkipIfNot4(connectionPool *dbconn.DBConn) {
	if connectionPool.Version.AtLeast("5") {
		Skip("Test only applicable to GPDB4")
	}
}

func SkipIfBefore5(connectionPool *dbconn.DBConn) {
	if connectionPool.Version.Before("5") {
		Skip("Test only applicable to GPDB5 and above")
	}
}

func SkipIfBefore6(connectionPool *dbconn.DBConn) {
	if connectionPool.Version.Before("6") {
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

type TestExecutorMultiple struct {
	ClusterOutputs      []*cluster.RemoteOutput
	ClusterCommands     []map[int][]string
	ErrorOnExecNum      int // Throw the specified error after this many executions of Execute[...]Command(); 0 means always return error
	NumLocalExecutions  int
	NumRemoteExecutions int
	LocalOutput         string
	LocalError          error
	LocalCommands       []string
}

func (executor *TestExecutorMultiple) ExecuteLocalCommand(commandStr string) (string, error) {
	executor.NumLocalExecutions++
	executor.LocalCommands = append(executor.LocalCommands, commandStr)
	if executor.ErrorOnExecNum == 0 || executor.NumLocalExecutions == executor.ErrorOnExecNum {
		return executor.LocalOutput, executor.LocalError
	}
	return executor.LocalOutput, nil
}

func (executor *TestExecutorMultiple) ExecuteClusterCommand(scope int, commandMap map[int][]string) (result *cluster.RemoteOutput) {
	originalExecutions := executor.NumRemoteExecutions
	executor.NumRemoteExecutions++
	executor.ClusterCommands = append(executor.ClusterCommands, commandMap)
	if executor.ErrorOnExecNum == 0 || executor.NumRemoteExecutions == executor.ErrorOnExecNum {
		// return the indexed item if exists, otherwise the last item
		numOutputs := len(executor.ClusterOutputs)
		result = executor.ClusterOutputs[numOutputs-1]
		if originalExecutions < numOutputs {
			result = executor.ClusterOutputs[originalExecutions]
		}
	}
	return result
}
