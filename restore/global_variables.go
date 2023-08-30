package restore

import (
	"sync"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gpbackup/filepath"
	"github.com/greenplum-db/gpbackup/history"
	"github.com/greenplum-db/gpbackup/options"
	"github.com/greenplum-db/gpbackup/toc"
	"github.com/greenplum-db/gpbackup/utils"
	"github.com/spf13/pflag"
)

/*
 * Empty struct type used for value with 0 bytes
 */
type Empty struct{}

/*
 * This file contains global variables and setter functions for those variables
 * used in testing.
 */

/*
 * Non-flag variables
 */

var (
	backupConfig        *history.BackupConfig
	connectionPool      *dbconn.DBConn
	globalCluster       *cluster.Cluster
	globalFPInfo        filepath.FilePathInfo
	globalTOC           *toc.TOC
	pluginConfig        *utils.PluginConfig
	restoreStartTime    string
	version             string
	wasTerminated       bool
	errorTablesMetadata map[string]Empty
	errorTablesData     map[string]Empty
	opts                *options.Options
	/*
	 * Used for synchronizing DoCleanup.  In DoInit() we increment the group
	 * and then wait for at least one DoCleanup to finish, either in DoTeardown
	 * or the signal handler.
	 */
	CleanupGroup *sync.WaitGroup
)

func init() {
	// Initialize global variables
	errorTablesMetadata = make(map[string]Empty)
	errorTablesData = make(map[string]Empty)
}

/*
 * Command-line flags
 */
var cmdFlags *pflag.FlagSet

/*
 * Setter functions
 */

func SetCmdFlags(flagSet *pflag.FlagSet) {
	cmdFlags = flagSet
	options.SetRestoreFlagDefaults(cmdFlags)
}

func SetBackupConfig(config *history.BackupConfig) {
	backupConfig = config
}

func SetConnection(conn *dbconn.DBConn) {
	connectionPool = conn
}

func SetCluster(cluster *cluster.Cluster) {
	globalCluster = cluster
}

func SetFPInfo(fpInfo filepath.FilePathInfo) {
	globalFPInfo = fpInfo
}

func SetPluginConfig(config *utils.PluginConfig) {
	pluginConfig = config
}

func SetTOC(objToc *toc.TOC) {
	globalTOC = objToc
}

// Util functions to enable ease of access to global flag values

func FlagChanged(flagName string) bool {
	return cmdFlags.Changed(flagName)
}

func MustGetFlagString(flagName string) string {
	return options.MustGetFlagString(cmdFlags, flagName)
}

func MustGetFlagInt(flagName string) int {
	return options.MustGetFlagInt(cmdFlags, flagName)
}

func MustGetFlagBool(flagName string) bool {
	return options.MustGetFlagBool(cmdFlags, flagName)
}

func MustGetFlagStringSlice(flagName string) []string {
	return options.MustGetFlagStringSlice(cmdFlags, flagName)
}

func MustGetFlagStringArray(flagName string) []string {
	return options.MustGetFlagStringArray(cmdFlags, flagName)
}

func GetVersion() string {
	return version
}

func SetVersion(v string) {
	version = v
}
