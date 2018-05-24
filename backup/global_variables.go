package backup

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
	backupReport  *utils.Report
	connection    *dbconn.DBConn
	globalCluster cluster.Cluster
	globalFPInfo  utils.FilePathInfo
	globalTOC     *utils.TOC
	objectCounts  map[string]int
	pluginConfig  *utils.PluginConfig
	version       string
	wasTerminated bool

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
	backupDir         *string
	compressionLevel  *int
	dataOnly          *bool
	dbname            *string
	debug             *bool
	excludeSchemas    *[]string
	excludeTableFile  *string
	excludeTables     *[]string
	includeSchemas    *[]string
	includeTableFile  *string
	includeTables     *[]string
	leafPartitionData *bool
	metadataOnly      *bool
	noCompression     *bool
	pluginConfigFile  *string
	quiet             *bool
	singleDataFile    *bool
	verbose           *bool
	withStats         *bool
)

/*
 * Setter functions
 */

func SetConnection(conn *dbconn.DBConn) {
	connection = conn
}

func SetCluster(cluster cluster.Cluster) {
	globalCluster = cluster
}

func SetExcludeSchemas(schemas []string) {
	excludeSchemas = &schemas
}

func SetExcludeTables(tables []string) {
	excludeTables = &tables
}

func SetFPInfo(fpInfo utils.FilePathInfo) {
	globalFPInfo = fpInfo
}

func SetIncludeSchemas(schemas []string) {
	includeSchemas = &schemas
}

func SetIncludeTables(tables []string) {
	includeTables = &tables
}

func SetLeafPartitionData(which bool) {
	leafPartitionData = &which
}

func SetReport(report *utils.Report) {
	backupReport = report
}

func GetReport() *utils.Report {
	return backupReport
}

func SetSingleDataFile(which bool) {
	singleDataFile = &which
}

func SetTOC(toc *utils.TOC) {
	globalTOC = toc
}

func SetVersion(v string) {
	version = v
}
