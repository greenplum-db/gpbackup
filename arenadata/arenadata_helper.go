package arenadata

import (
	"regexp"
	"strconv"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/pkg/errors"
)

var (
	adPattern = regexp.MustCompile(`_arenadata(\d+)`)
)

func EnsureAdVersionCompatibility(backupVersion string, restoreVersion string) {
	var (
		adBackup, adRestore int
		err                 error
	)
	if strVersion := adPattern.FindAllStringSubmatch(backupVersion, -1); len(strVersion) > 0 {
		adBackup, err = strconv.Atoi(strVersion[0][1])
		gplog.FatalOnError(err)
	} else {
		gplog.Fatal(errors.Errorf("Invalid arenadata version format for gpbackup: %s", backupVersion), "")
	}
	if strVersion := adPattern.FindAllStringSubmatch(restoreVersion, -1); len(strVersion) > 0 {
		adRestore, err = strconv.Atoi(strVersion[0][1])
		gplog.FatalOnError(err)
	} else {
		gplog.Fatal(errors.Errorf("Invalid arenadata version format for gprestore: %s", restoreVersion), "")
	}
	if adRestore < adBackup {
		gplog.Fatal(errors.Errorf("gprestore arenadata%d cannot restore a backup taken with gpbackup arenadata%d; please use gprestore arenadata%d or later.",
			adRestore, adBackup, adBackup), "")
	}
}

// fullVersion: gpbackup version + '_' + arenadata release + ('+' + gpbackup build)
// example: 1.20.4_arenadata2+dev.1.g768b7e0 -> 1.20.4+dev.1.g768b7e0
func GetOriginalVersion(fullVersion string) string {
	return adPattern.ReplaceAllString(fullVersion, "")
}
