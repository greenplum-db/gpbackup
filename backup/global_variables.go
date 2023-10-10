package backup

import (
	"sync"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gpbackup/filepath"
	"github.com/greenplum-db/gpbackup/options"
	"github.com/greenplum-db/gpbackup/report"
	"github.com/greenplum-db/gpbackup/toc"
	"github.com/greenplum-db/gpbackup/utils"
	"github.com/nightlyone/lockfile"
	"github.com/spf13/pflag"
)

/*
 * This file contains global variables and setter functions for those variables
 * used in testing.
 */

/*
Table backup state constants
*/
const (
	Unknown int = iota
	Deferred
	Complete
	PG_LOCK_NOT_AVAILABLE = "55P03"
)

/*
 * Non-flag variables
 */
var (
	backupReport         *report.Report
	connectionPool       *dbconn.DBConn
	globalCluster        *cluster.Cluster
	globalFPInfo         filepath.FilePathInfo
	globalTierMap        map[UniqueID][]uint32
	globalTOC            *toc.TOC
	objectCounts         map[string]int
	pluginConfig         *utils.PluginConfig
	version              string
	wasTerminated        bool
	backupLockFile       lockfile.Lockfile
	filterRelationClause string
	quotedRoleNames      map[string]string
	backupSnapshot       string
	/*
	 * Used for synchronizing DoCleanup.  In DoInit() we increment the group
	 * and then wait for at least one DoCleanup to finish, either in DoTeardown
	 * or the signal handler.
	 */
	CleanupGroup *sync.WaitGroup

	// Allow global access to pre-processed include and exclude table lists
	IncludedRelationFqns []options.Relation
	ExcludedRelationFqns []options.Relation

	// sections to backup
	BackupSections options.Sections
)

/*
 * Command-line flags
 */
var cmdFlags *pflag.FlagSet

/*
 * Setter functions
 */

func SetCmdFlags(flagSet *pflag.FlagSet) {
	cmdFlags = flagSet
	options.SetBackupFlagDefaults(cmdFlags)
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

func GetFPInfo() filepath.FilePathInfo {
	return globalFPInfo
}

func SetBackupSnapshot(snapshot string) {
	backupSnapshot = snapshot
}

func GetBackupSnapshot() string {
	return backupSnapshot
}

func SetPluginConfig(config *utils.PluginConfig) {
	pluginConfig = config
}

func SetReport(report *report.Report) {
	backupReport = report
}

func GetReport() *report.Report {
	return backupReport
}

func SetTOC(toc *toc.TOC) {
	globalTOC = toc
}

func SetVersion(v string) {
	version = v
}

func SetFilterRelationClause(filterClause string) {
	filterRelationClause = filterClause
}

func SetQuotedRoleNames(quotedRoles map[string]string) {
	quotedRoleNames = quotedRoles
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

// Allow global access to pre-processed include and exclude table lists
func AddIncludedRelationFqn(relation options.Relation) {
	IncludedRelationFqns = append(IncludedRelationFqns, relation)
}

func AddExcludedRelationFqn(relation options.Relation) {
	ExcludedRelationFqns = append(ExcludedRelationFqns, relation)
}
