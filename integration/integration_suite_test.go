package integration

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"os/exec"

	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"
)

var (
	buffer     *bytes.Buffer
	connection *utils.DBConn
	toc        *utils.TOC
	backupfile *utils.FileWithByteCount
)

func TestQueries(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "database query tests")
}

var _ = BeforeSuite(func() {
	exec.Command("dropdb", "testdb").Run()
	err := exec.Command("createdb", "testdb").Run()
	if err != nil {
		Fail("Cannot create database testdb; is GPDB running?")
	}
	Expect(err).To(BeNil())
	testutils.SetupTestLogger()
	connection = utils.NewDBConn("testdb")
	connection.Connect(1)
	// We can't use AssertQueryRuns since if a role already exists it will error
	connection.Exec("CREATE ROLE testrole SUPERUSER")
	connection.Exec("CREATE ROLE anothertestrole SUPERUSER")
	connection.SetDatabaseVersion()
	backup.InitializeMetadataParams(connection)
	backup.SetConnection(connection)
	testutils.AssertQueryRuns(connection, "SET ROLE testrole")
	testutils.AssertQueryRuns(connection, "ALTER DATABASE testdb OWNER TO anothertestrole")
	testutils.AssertQueryRuns(connection, "ALTER SCHEMA public OWNER TO anothertestrole")
	testutils.AssertQueryRuns(connection, "DROP PROTOCOL IF EXISTS gphdfs")
	testutils.AssertQueryRuns(connection, `SET standard_conforming_strings TO "on"`)
	segConfig := utils.GetSegmentConfiguration(connection)
	cluster := utils.NewCluster(segConfig, "/tmp/test_dir", "20170101010101", "gpseg")
	if connection.Version.Before("6") {
		setupTestFilespace(cluster)
	} else {
		err := os.Mkdir("/tmp/test_dir", 0777)
		if err != nil {
			Fail(fmt.Sprintf("Could not create test directory: %s", err.Error()))
		}
	}
})

var _ = BeforeEach(func() {
	buffer = bytes.NewBuffer([]byte(""))
	backup.SetIncludeSchemas([]string{})
	backup.SetExcludeTables([]string{})
	backup.SetIncludeTables([]string{})
})

var _ = AfterSuite(func() {
	gexec.CleanupBuildArtifacts()
	if connection.Version.Before("6") {
		destroyTestFilespace()
	} else {
		err := os.RemoveAll("/tmp/test_dir")
		if err != nil {
			Fail(fmt.Sprintf("Could not remove test directory: %s", err.Error()))
		}
	}
	if connection != nil {
		connection.Close()
		err := exec.Command("dropdb", "testdb").Run()
		Expect(err).To(BeNil())
	}
	connection1 := utils.NewDBConn("template1")
	connection1.Connect(1)
	testutils.AssertQueryRuns(connection1, "DROP ROLE testrole")
	testutils.AssertQueryRuns(connection1, "DROP ROLE anothertestrole")
	connection1.Close()
})

func setupTestFilespace(cluster utils.Cluster) {
	backup.CreateBackupDirectoriesOnAllHosts(cluster)
	// Construct a filespace config like the one that gpfilespace generates
	filespaceConfigQuery := `COPY (SELECT hostname || ':' || dbid || ':/tmp/test_dir/' || preferred_role || content FROM gp_segment_configuration AS subselect) TO '/tmp/temp_filespace_config';`
	testutils.AssertQueryRuns(connection, filespaceConfigQuery)
	out, err := exec.Command("sh", "-c", "echo \"filespace:test_dir\" > /tmp/filespace_config").CombinedOutput()
	if err != nil {
		Fail(fmt.Sprintf("Cannot create test filespace configuration: %s: %s", out, err.Error()))
	}
	out, err = exec.Command("sh", "-c", "cat /tmp/temp_filespace_config >> /tmp/filespace_config").CombinedOutput()
	if err != nil {
		Fail(fmt.Sprintf("Cannot finalize test filespace configuration: %s: %s", out, err.Error()))
	}
	// Create the filespace and verify it was created successfully
	out, err = exec.Command("sh", "-c", "gpfilespace --config /tmp/filespace_config").CombinedOutput()
	if err != nil {
		Fail(fmt.Sprintf("Cannot create test filespace: %s: %s", out, err.Error()))
	}
	filespaceName := utils.SelectString(connection, "SELECT fsname AS string FROM pg_filespace WHERE fsname = 'test_dir';")
	if filespaceName != "test_dir" {
		Fail("Filespace test_dir was not successfully created")
	}
}

func destroyTestFilespace() {
	filespaceName := utils.SelectString(connection, "SELECT fsname AS string FROM pg_filespace WHERE fsname = 'test_dir';")
	if filespaceName != "test_dir" {
		return
	}
	testutils.AssertQueryRuns(connection, "DROP FILESPACE test_dir")
	out, err := exec.Command("sh", "-c", "rm -rf /tmp/test_dir /tmp/filespace_config /tmp/temp_filespace_config").CombinedOutput()
	if err != nil {
		Fail(fmt.Sprintf("Could not remove test filespace directory and configuration files: %s: %s", out, err.Error()))
	}
}
