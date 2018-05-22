package restore

import (
	"sync"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gpbackup/utils"
)

/*
 * This file contains global variables and setter functions for those variables
 * used in testing.
 */

/*
 * Non-flag variables
 */

var (
	backupConfig     *utils.BackupConfig
	connection       *dbconn.DBConn
	globalCluster    cluster.Cluster
	globalFPInfo     utils.FilePathInfo
	globalTOC        *utils.TOC
	pluginConfig     *utils.PluginConfig
	restoreStartTime string
	version          string
	wasTerminated    bool

	/*
	 * Used for synchronizing DoCleanup.  In DoInit() we increment the group
	 * and then wait for at least one DoCleanup to finish, either in DoTeardown
	 * or the signal handler.
	 */
	CleanupGroup *sync.WaitGroup
)

/*
 * Command-line flags
 */

var (
	backupDir        *string
	createDB         *bool
	dataOnly         *bool
	debug            *bool
	excludeSchemas   *[]string
	excludeTableFile *string
	excludeTables    *[]string
	includeSchemas   *[]string
	includeTableFile *string
	includeTables    *[]string
	metadataOnly     *bool
	numJobs          *int
	onErrorContinue  *bool
	pluginConfigFile *string
	printVersion     *bool
	quiet            *bool
	redirect         *string
	restoreGlobals   *bool
	timestamp        *string
	verbose          *bool
	withStats        *bool
)

/*
 * Setter functions
 */

func SetBackupConfig(config *utils.BackupConfig) {
	backupConfig = config
}

func SetConnection(conn *dbconn.DBConn) {
	connection = conn
}

func SetCluster(cluster cluster.Cluster) {
	globalCluster = cluster
}

func SetFPInfo(fpInfo utils.FilePathInfo) {
	globalFPInfo = fpInfo
}

func SetOnErrorContinue(errContinue bool) {
	onErrorContinue = &errContinue
}

func SetNumJobs(jobs int) {
	numJobs = &jobs
}

func SetTOC(toc *utils.TOC) {
	globalTOC = toc
}
