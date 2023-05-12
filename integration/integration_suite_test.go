package integration

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"testing"

	"github.com/blang/semver"
	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/restore"
	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/toc"
	"github.com/greenplum-db/gpbackup/utils"
	"github.com/spf13/pflag"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gbytes"
	. "github.com/onsi/gomega/gexec"
)

var (
	buffer             *bytes.Buffer
	connectionPool     *dbconn.DBConn
	tocfile            *toc.TOC
	backupfile         *utils.FileWithByteCount
	testCluster        *cluster.Cluster
	gpbackupHelperPath string
	stderr, logFile    *Buffer

	// GUC defaults. Initially set to GPDB4 values
	concurrencyDefault    = "20"
	memSharedDefault      = "20"
	memSpillDefault       = "20"
	memAuditDefault       = "0"
	cpuSetDefault         = "-1"
	includeSecurityLabels = false

	useOldBackupVersion = false
	oldBackupSemVer     semver.Version
)

func TestQueries(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "database query tests")
}

var _ = BeforeSuite(func() {
	_ = exec.Command("dropdb", "testdb").Run()
	err := exec.Command("createdb", "testdb").Run()
	if err != nil {
		Fail("Cannot create database testdb; is GPDB running?")
	}
	Expect(err).To(BeNil())
	_, stderr, logFile = testhelper.SetupTestLogger()
	connectionPool = testutils.SetupTestDbConn("testdb")
	// We can't use AssertQueryRuns since if a role already exists it will error
	_, _ = connectionPool.Exec("CREATE ROLE testrole SUPERUSER")
	_, _ = connectionPool.Exec("CREATE ROLE anothertestrole SUPERUSER")
	backup.InitializeMetadataParams(connectionPool)
	backup.SetConnection(connectionPool)
	segConfig := cluster.MustGetSegmentConfiguration(connectionPool)
	testCluster = cluster.NewCluster(segConfig)
	testhelper.AssertQueryRuns(connectionPool, "SET ROLE testrole")
	testhelper.AssertQueryRuns(connectionPool, "ALTER DATABASE testdb OWNER TO anothertestrole")
	testhelper.AssertQueryRuns(connectionPool, "ALTER SCHEMA public OWNER TO anothertestrole")
	testhelper.AssertQueryRuns(connectionPool, "DROP PROTOCOL IF EXISTS gphdfs")
	testhelper.AssertQueryRuns(connectionPool, `SET standard_conforming_strings TO "on"`)
	testhelper.AssertQueryRuns(connectionPool, `SET search_path=pg_catalog`)
	if connectionPool.Version.Before("6") {
		testhelper.AssertQueryRuns(connectionPool, "SET allow_system_table_mods = 'DML'")
		testutils.SetupTestFilespace(connectionPool, testCluster)
	} else {
		testhelper.AssertQueryRuns(connectionPool, "SET allow_system_table_mods = true")

		remoteOutput := testCluster.GenerateAndExecuteCommand(
			"Creating filespace test directories on all hosts",
			cluster.ON_HOSTS|cluster.INCLUDE_COORDINATOR,
			func(contentID int) string {
				return fmt.Sprintf("mkdir -p /tmp/test_dir && mkdir -p /tmp/test_dir1 && mkdir -p /tmp/test_dir2")
			})
		if remoteOutput.NumErrors != 0 {
			Fail("Could not create filespace test directory on 1 or more hosts")
		}
	}

	gpbackupHelperPath = buildAndInstallBinaries()

	// Set GUC Defaults and version logic
	if connectionPool.Version.AtLeast("6") {
		memSharedDefault = "80"
		memSpillDefault = "0"

		includeSecurityLabels = true
	}

	// handle backwards-compatibility tests
	useOldBackupVersion = os.Getenv("OLD_BACKUP_VERSION") != ""
	if useOldBackupVersion {
		oldBackupSemVer = semver.MustParse(os.Getenv("OLD_BACKUP_VERSION"))
	}
})

var backupCmdFlags *pflag.FlagSet
var restoreCmdFlags *pflag.FlagSet

var _ = BeforeEach(func() {
	buffer = bytes.NewBuffer([]byte(""))

	backupCmdFlags = pflag.NewFlagSet("gpbackup", pflag.ExitOnError)
	backup.SetCmdFlags(backupCmdFlags)
	backup.SetFilterRelationClause("")

	restoreCmdFlags = pflag.NewFlagSet("gprestore", pflag.ExitOnError)
	restore.SetCmdFlags(restoreCmdFlags)
})

var _ = AfterSuite(func() {
	CleanupBuildArtifacts()
	if connectionPool.Version.Before("6") {
		testutils.DestroyTestFilespace(connectionPool)
	} else {
		remoteOutput := testCluster.GenerateAndExecuteCommand(
			"Removing /tmp/test_dir* directories on all hosts",
			cluster.ON_HOSTS|cluster.INCLUDE_COORDINATOR,
			func(contentID int) string {
				return fmt.Sprintf("rm -rf /tmp/test_dir*")
			})
		if remoteOutput.NumErrors != 0 {
			Fail("Could not remove /tmp/testdir* directories on 1 or more hosts")
		}
	}
	if connectionPool != nil {
		connectionPool.Close()
		err := exec.Command("dropdb", "testdb").Run()
		Expect(err).To(BeNil())
	}
	connection1 := testutils.SetupTestDbConn("template1")
	testhelper.AssertQueryRuns(connection1, "DROP ROLE testrole")
	testhelper.AssertQueryRuns(connection1, "DROP ROLE anothertestrole")
	connection1.Close()
	_ = os.RemoveAll("/tmp/helper_test")
	_ = os.RemoveAll("/tmp/plugin_dest")
})
