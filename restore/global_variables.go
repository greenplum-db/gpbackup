package restore

import (
	"sync"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gpbackup/utils"
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
	backupConfig     *utils.BackupConfig
	connectionPool   *dbconn.DBConn
	globalCluster    *cluster.Cluster
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
var cmdFlags *pflag.FlagSet

const (
	BACKUP_DIR            = "backup-dir"
	CREATE_DB             = "create-db"
	DATA_ONLY             = "data-only"
	DEBUG                 = "debug"
	EXCLUDE_RELATION      = "exclude-table"
	EXCLUDE_RELATION_FILE = "exclude-table-file"
	EXCLUDE_SCHEMA        = "exclude-schema"
	INCLUDE_RELATION      = "include-table"
	INCLUDE_RELATION_FILE = "include-table-file"
	INCLUDE_SCHEMA        = "include-schema"
	JOBS                  = "jobs"
	LEAF_PARTITION_DATA   = "leaf-partition-data"
	METADATA_ONLY         = "metadata-only"
	ON_ERROR_CONTINUE     = "on-error-continue"
	PLUGIN_CONFIG         = "plugin-config"
	QUIET                 = "quiet"
	REDIRECT_DB           = "redirect-db"
	TIMESTAMP             = "timestamp"
	VERBOSE               = "verbose"
	WITH_GLOBALS          = "with-globals"
	WITH_STATS            = "with-stats"
)

/*
 * Setter functions
 */

func SetCmdFlags(flagSet *pflag.FlagSet) {
	cmdFlags = flagSet
}

func SetBackupConfig(config *utils.BackupConfig) {
	backupConfig = config
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

func SetTOC(toc *utils.TOC) {
	globalTOC = toc
}

// Util functions to enable ease of access to global flag values

func MustGetFlagString(flagName string) string {
	return utils.MustGetFlagString(cmdFlags, flagName)
}

func MustGetFlagInt(flagName string) int {
	return utils.MustGetFlagInt(cmdFlags, flagName)
}

func MustGetFlagBool(flagName string) bool {
	return utils.MustGetFlagBool(cmdFlags, flagName)
}

func MustGetFlagStringSlice(flagName string) []string {
	return utils.MustGetFlagStringSlice(cmdFlags, flagName)
}
