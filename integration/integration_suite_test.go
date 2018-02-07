package integration

import (
	"bytes"
	"fmt"
	"testing"

	"os/exec"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"
)

var (
	buffer      *bytes.Buffer
	connection  *dbconn.DBConn
	toc         *utils.TOC
	backupfile  *utils.FileWithByteCount
	testCluster cluster.Cluster
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
	testhelper.SetupTestLogger()
	connection = dbconn.NewDBConn("testdb")
	connection.MustConnect(1)
	// We can't use AssertQueryRuns since if a role already exists it will error
	connection.Exec("CREATE ROLE testrole SUPERUSER")
	connection.Exec("CREATE ROLE anothertestrole SUPERUSER")
	utils.SetDatabaseVersion(connection)
	backup.InitializeMetadataParams(connection)
	backup.SetConnection(connection)
	testhelper.AssertQueryRuns(connection, "SET ROLE testrole")
	testhelper.AssertQueryRuns(connection, "ALTER DATABASE testdb OWNER TO anothertestrole")
	testhelper.AssertQueryRuns(connection, "ALTER SCHEMA public OWNER TO anothertestrole")
	testhelper.AssertQueryRuns(connection, "DROP PROTOCOL IF EXISTS gphdfs")
	testhelper.AssertQueryRuns(connection, `SET standard_conforming_strings TO "on"`)
	segConfig := cluster.GetSegmentConfiguration(connection)
	testCluster = cluster.NewCluster(segConfig)
	if connection.Version.Before("6") {
		setupTestFilespace(testCluster)
	} else {
		remoteOutput := testCluster.GenerateAndExecuteCommand("Creating /tmp/test_dir directory on all hosts", func(contentID int) string {
			return fmt.Sprintf("mkdir -p /tmp/test_dir")
		}, true)
		if remoteOutput.NumErrors != 0 {
			Fail("Could not create /tmp/test_dir directory on 1 or more hosts")
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
		remoteOutput := testCluster.GenerateAndExecuteCommand("Removing /tmp/test_dir directory on all hosts", func(contentID int) string {
			return fmt.Sprintf("rm -rf /tmp/test_dir")
		}, true)
		if remoteOutput.NumErrors != 0 {
			Fail("Could not remove /tmp/test_dir directory on 1 or more hosts")
		}
	}
	if connection != nil {
		connection.Close()
		err := exec.Command("dropdb", "testdb").Run()
		Expect(err).To(BeNil())
	}
	connection1 := dbconn.NewDBConn("template1")
	connection1.MustConnect(1)
	testhelper.AssertQueryRuns(connection1, "DROP ROLE testrole")
	testhelper.AssertQueryRuns(connection1, "DROP ROLE anothertestrole")
	connection1.Close()
})

func setupTestFilespace(testCluster cluster.Cluster) {
	backup.CreateBackupDirectoriesOnAllHosts()
	// Construct a filespace config like the one that gpfilespace generates
	filespaceConfigQuery := `COPY (SELECT hostname || ':' || dbid || ':/tmp/test_dir/' || preferred_role || content FROM gp_segment_configuration AS subselect) TO '/tmp/temp_filespace_config';`
	testhelper.AssertQueryRuns(connection, filespaceConfigQuery)
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
	filespaceName := dbconn.MustSelectString(connection, "SELECT fsname AS string FROM pg_filespace WHERE fsname = 'test_dir';")
	if filespaceName != "test_dir" {
		Fail("Filespace test_dir was not successfully created")
	}
}

func destroyTestFilespace() {
	filespaceName := dbconn.MustSelectString(connection, "SELECT fsname AS string FROM pg_filespace WHERE fsname = 'test_dir';")
	if filespaceName != "test_dir" {
		return
	}
	testhelper.AssertQueryRuns(connection, "DROP FILESPACE test_dir")
	out, err := exec.Command("bash", "-c", "rm -rf /tmp/test_dir /tmp/filespace_config /tmp/temp_filespace_config").CombinedOutput()
	if err != nil {
		Fail(fmt.Sprintf("Could not remove test filespace directory and configuration files: %s: %s", out, err.Error()))
	}
}
