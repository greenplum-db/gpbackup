package integration

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"os/exec"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/utils"

	"github.com/greenplum-db/gpbackup/restore"
	"github.com/greenplum-db/gpbackup/testutils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
	"github.com/onsi/gomega/gexec"
	"github.com/spf13/pflag"
)

var (
	buffer                  *bytes.Buffer
	connection              *dbconn.DBConn
	toc                     *utils.TOC
	backupfile              *utils.FileWithByteCount
	testCluster             *cluster.Cluster
	gpbackupHelperPath      string
	stdout, stderr, logFile *gbytes.Buffer
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
	stdout, stderr, logFile = testhelper.SetupTestLogger()
	connection = testutils.SetupTestDbConn("testdb")
	// We can't use AssertQueryRuns since if a role already exists it will error
	connection.Exec("CREATE ROLE testrole SUPERUSER")
	connection.Exec("CREATE ROLE anothertestrole SUPERUSER")
	backup.InitializeMetadataParams(connection)
	backup.SetConnection(connection)
	segConfig := cluster.MustGetSegmentConfiguration(connection)
	testCluster = cluster.NewCluster(segConfig)
	testhelper.AssertQueryRuns(connection, "SET ROLE testrole")
	testhelper.AssertQueryRuns(connection, "ALTER DATABASE testdb OWNER TO anothertestrole")
	testhelper.AssertQueryRuns(connection, "ALTER SCHEMA public OWNER TO anothertestrole")
	testhelper.AssertQueryRuns(connection, "DROP PROTOCOL IF EXISTS gphdfs")
	testhelper.AssertQueryRuns(connection, `SET standard_conforming_strings TO "on"`)
	testhelper.AssertQueryRuns(connection, `SET search_path=pg_catalog`)
	if connection.Version.AtLeast("6") {
		// Drop plpgsql extension to not interfere in extension tests
		testhelper.AssertQueryRuns(connection, "DROP EXTENSION plpgsql CASCADE")
		testhelper.AssertQueryRuns(connection, "CREATE LANGUAGE plpgsql")
		testhelper.AssertQueryRuns(connection, "SET allow_system_table_mods = true")
	}
	if connection.Version.Before("6") {
		testhelper.AssertQueryRuns(connection, "SET allow_system_table_mods = 'DML'")
		setupTestFilespace(testCluster)
	} else {
		remoteOutput := testCluster.GenerateAndExecuteCommand("Creating filespace test directories on all hosts", func(contentID int) string {
			return fmt.Sprintf("mkdir -p /tmp/test_dir && mkdir -p /tmp/test_dir1 && mkdir -p /tmp/test_dir2")
		}, cluster.ON_HOSTS_AND_MASTER)
		if remoteOutput.NumErrors != 0 {
			Fail("Could not create filespace test directory on 1 or more hosts")
		}
	}

	gpbackupHelperPath = buildAndInstallBinaries()
})

var backupCmdFlags *pflag.FlagSet
var restoreCmdFlags *pflag.FlagSet

var _ = BeforeEach(func() {
	buffer = bytes.NewBuffer([]byte(""))

	backupCmdFlags = pflag.NewFlagSet("gpbackup", pflag.ExitOnError)

	backup.SetFlagDefaults(backupCmdFlags)
	backup.SetCmdFlags(backupCmdFlags)

	restoreCmdFlags = pflag.NewFlagSet("gprestore", pflag.ExitOnError)

	restore.SetFlagDefaults(restoreCmdFlags)
	restore.SetCmdFlags(restoreCmdFlags)
})

var _ = AfterSuite(func() {
	gexec.CleanupBuildArtifacts()
	if connection.Version.Before("6") {
		destroyTestFilespace()
	} else {
		remoteOutput := testCluster.GenerateAndExecuteCommand("Removing /tmp/test_dir* directories on all hosts", func(contentID int) string {
			return fmt.Sprintf("rm -rf /tmp/test_dir*")
		}, cluster.ON_HOSTS_AND_MASTER)
		if remoteOutput.NumErrors != 0 {
			Fail("Could not remove /tmp/testdir* directories on 1 or more hosts")
		}
	}
	if connection != nil {
		connection.Close()
		err := exec.Command("dropdb", "testdb").Run()
		Expect(err).To(BeNil())
	}
	connection1 := testutils.SetupTestDbConn("template1")
	testhelper.AssertQueryRuns(connection1, "DROP ROLE testrole")
	testhelper.AssertQueryRuns(connection1, "DROP ROLE anothertestrole")
	connection1.Close()
	os.RemoveAll("/tmp/helper_test")
	os.RemoveAll("/tmp/plugin_dest")
})

func setupTestFilespace(testCluster *cluster.Cluster) {
	remoteOutput := testCluster.GenerateAndExecuteCommand("Creating filespace test directory", func(contentID int) string {
		return fmt.Sprintf("mkdir -p /tmp/test_dir")
	}, cluster.ON_HOSTS_AND_MASTER)
	if remoteOutput.NumErrors != 0 {
		Fail("Could not create filespace test directory on 1 or more hosts")
	}
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
