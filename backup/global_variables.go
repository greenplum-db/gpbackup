package backup

import (
	"sync"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gpbackup/utils"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/spf13/pflag"
)

/*
 * This file contains global variables and setter functions for those variables
 * used in testing.
 */

/*
 * Non-flag variables
 */
var (
	backupReport   *utils.Report
	connectionPool *dbconn.DBConn
	globalCluster  *cluster.Cluster
	globalFPInfo   utils.FilePathInfo
	globalTOC      *utils.TOC
	objectCounts   map[string]int
	pluginConfig   *utils.PluginConfig
	version        string
	wasTerminated  bool

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
var cmdFlags *pflag.FlagSet

const (
	BACKUP_DIR            = "backup-dir"
	COMPRESSION_LEVEL     = "compression-level"
	DATA_ONLY             = "data-only"
	DBNAME                = "dbname"
	DEBUG                 = "debug"
	EXCLUDE_RELATION      = "exclude-table"
	EXCLUDE_RELATION_FILE = "exclude-table-file"
	EXCLUDE_SCHEMA        = "exclude-schema"
	INCLUDE_RELATION      = "include-table-file"
	INCLUDE_RELATION_FILE = "include-table-file"
	INCLUDE_SCHEMA        = "include-schema"
	JOBS                  = "jobs"
	LEAF_PARTITION_DATA   = "leaf-partition-data"
	METADATA_ONLY         = "metadata-only"
	NO_COMPRESSION        = "no-compression"
	PLUGIN_CONFIG         = "plugin-config"
	QUIET                 = "quiet"
	SINGLE_DATA_FILE      = "single-data-file"
	VERBOSE               = "verbose"
	WITH_STATS            = "with-stats"
)

/*
 * Setter functions
 */

func SetCmdFlags(flagSet *pflag.FlagSet) {
	cmdFlags = flagSet
}

func SetConnection(conn *dbconn.DBConn) {
	connectionPool = conn
}

func SetCluster(cluster *cluster.Cluster) {
	globalCluster = cluster
}

func SetFPInfo(fpInfo utils.FilePathInfo) {
	globalFPInfo = fpInfo
}

func SetReport(report *utils.Report) {
	backupReport = report
}

func GetReport() *utils.Report {
	return backupReport
}

func SetTOC(toc *utils.TOC) {
	globalTOC = toc
}

func SetVersion(v string) {
	version = v
}

func MustGetFlagString(flagName string) string {
	value, err := cmdFlags.GetString(flagName)
	gplog.FatalOnError(err)
	return value
}

func MustGetFlagInt(flagName string) int {
	value, err := cmdFlags.GetInt(flagName)
	gplog.FatalOnError(err)
	return value
}

func MustGetFlagBool(flagName string) bool {
	value, err := cmdFlags.GetBool(flagName)
	gplog.FatalOnError(err)
	return value
}

func MustGetFlagStringSlice(flagName string) []string {
	value, err := cmdFlags.GetStringSlice(flagName)
	gplog.FatalOnError(err)
	return value
}
