package report

import (
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/blang/semver"
	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gp-common-go-libs/iohelper"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/greenplum-db/gpbackup/history"
	"github.com/greenplum-db/gpbackup/utils"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"
)

/*
 * This struct holds information that will be printed to the report file
 * after a backup, as well as information printed to the configuration
 * file that we will want to read in for a restore.
 */
type Report struct {
	BackupParamsString string
	DatabaseSize       string
	history.BackupConfig
}

type LineInfo struct {
	Key   string
	Value string
}

func ParseErrorMessage(errStr string) string {
	if errStr == "" {
		return ""
	}
	errLevelStr := "[CRITICAL]:-"
	headerIndex := strings.Index(errStr, errLevelStr)
	errMsg := errStr[headerIndex+len(errLevelStr):]
	return errMsg
}

func (report *Report) ConstructBackupParamsString() {
	filterStr := ""
	if report.IncludeSchemaFiltered {
		filterStr = "Include Schema Filter "
	}
	if report.IncludeTableFiltered {
		filterStr = "Include Table Filter"
	}
	if report.ExcludeSchemaFiltered {
		filterStr = "Exclude Schema Filter"
	}
	if report.ExcludeTableFiltered {
		filterStr += "Exclude Table Filter"
	}
	if filterStr == "" {
		filterStr = "None"
	}
	compressStr := "None"
	program := utils.GetPipeThroughProgram()
	if report.Compressed {
		compressStr = program.Name
	}
	pluginStr := "None"
	if report.Plugin != "" {
		pluginStr = report.Plugin
	}
	sectionStr := "All Sections"
	if report.DataOnly {
		sectionStr = "Data Only"
	}
	if report.MetadataOnly {
		sectionStr = "Metadata Only"
	}
	filesStr := "Multiple Data Files Per Segment"
	if report.MetadataOnly {
		filesStr = "No Data Files"
	} else if report.SingleDataFile {
		filesStr = "Single Data File Per Segment"
	}
	statsStr := "No"
	if report.WithStatistics {
		statsStr = "Yes"
	}
	backupParamsTemplate := `compression: %s
plugin executable: %s
backup section: %s
object filtering: %s
includes statistics: %s
data file format: %s
%s`
	report.BackupParamsString = fmt.Sprintf(backupParamsTemplate, compressStr, pluginStr, sectionStr, filterStr,
		statsStr, filesStr, report.constructIncrementalSection())
}

func (report *Report) constructIncrementalSection() string {
	if !report.Incremental {
		return "incremental: False"
	}
	backupTimestamps := make([]string, 0)
	for _, restorePlanEntry := range report.RestorePlan {
		backupTimestamps = append(backupTimestamps, restorePlanEntry.Timestamp)
	}
	return fmt.Sprintf(`incremental: True
incremental backup set:
%s`, strings.Join(backupTimestamps, "\n"))
}

func (report *Report) WriteBackupReportFile(reportFilename string, timestamp string, endtime time.Time, objectCounts map[string]int, errMsg string) {
	reportFile, err := iohelper.OpenFileForWriting(reportFilename)
	if err != nil {
		gplog.Error("Unable to open backup report file %s", reportFilename)
		return
	}

	gpbackupCommandLine := strings.Join(os.Args, " ")
	start, end, duration := GetDurationInfo(timestamp, endtime)

	reportInfo := make([]LineInfo, 0)
	reportInfo = append(reportInfo,
		LineInfo{Key: "timestamp key:", Value: timestamp},
		LineInfo{Key: "gpdb version:", Value: report.DatabaseVersion},
		LineInfo{Key: "gpbackup version:", Value: fmt.Sprintf("%s\n", report.BackupVersion)},
		LineInfo{Key: "database name:", Value: report.DatabaseName},
		LineInfo{Key: "command line:", Value: gpbackupCommandLine},
	)

	AppendBackupParams(&reportInfo, report.BackupParamsString)

	reportInfo = append(reportInfo,
		LineInfo{},
		LineInfo{Key: "start time:", Value: start},
		LineInfo{Key: "end time:", Value: end},
		LineInfo{Key: "duration:", Value: duration})

	if errMsg != "" {
		reportInfo = append(reportInfo,
			LineInfo{},
			LineInfo{Key: "backup status:", Value: history.BackupStatusFailed},
			LineInfo{Key: "backup error:", Value: errMsg})
	} else {
		reportInfo = append(reportInfo,
			LineInfo{},
			LineInfo{Key: "backup status:", Value: history.BackupStatusSucceed})
	}
	reportInfo = append(reportInfo, LineInfo{})
	if report.DatabaseSize != "" {
		reportInfo = append(reportInfo,
			LineInfo{Key: "database size:", Value: strings.ToUpper(report.DatabaseSize)})
	}
	reportInfo = append(reportInfo,
		LineInfo{Key: "segment count:", Value: fmt.Sprintf("%d", report.SegmentCount)})

	_, err = fmt.Fprint(reportFile, "Greenplum Database Backup Report\n\n")
	if err != nil {
		gplog.Error("Unable to write backup report file %s", reportFilename)
		return
	}

	logOutputReport(reportFile, reportInfo)

	PrintObjectCounts(reportFile, objectCounts)

	err = reportFile.Close()
	gplog.FatalOnError(err)
	_ = operating.System.Chmod(reportFilename, 0444)
}

func WriteRestoreReportFile(reportFilename string, backupTimestamp string, startTimestamp string, connectionPool *dbconn.DBConn, restoreVersion string, origSize int, destSize int, errMsg string) {
	reportFile, err := iohelper.OpenFileForWriting(reportFilename)
	if err != nil {
		gplog.Warn("Unable to open restore report file %s, skipping report creation", reportFilename)
		return
	}

	gprestoreCommandLine := strings.Join(os.Args, " ")
	start, end, duration := GetDurationInfo(startTimestamp, operating.System.Now())

	utils.MustPrintf(reportFile, "Greenplum Database Restore Report\n\n")

	reportInfo := make([]LineInfo, 0)
	reportInfo = append(reportInfo,
		LineInfo{Key: "timestamp key:", Value: backupTimestamp},
		LineInfo{Key: "gpdb version:", Value: connectionPool.Version.VersionString},
		LineInfo{Key: "gprestore version:", Value: fmt.Sprintf("%s\n", restoreVersion)},
		LineInfo{Key: "database name:", Value: connectionPool.DBName},
		LineInfo{Key: "command line:", Value: fmt.Sprintf("%s\n", gprestoreCommandLine)},
		LineInfo{Key: "backup segment count:", Value: fmt.Sprintf("%d", origSize)},
		LineInfo{Key: "restore segment count:", Value: fmt.Sprintf("%d", destSize)},
		LineInfo{Key: "start time:", Value: start},
		LineInfo{Key: "end time:", Value: end},
		LineInfo{Key: "duration:", Value: duration},
	)

	var restoreStatus string
	errorCode := gplog.GetErrorCode()
	if errorCode == 1 {
		restoreStatus = fmt.Sprintf("Success but non-fatal errors occurred. See log file %s for details.", gplog.GetLogFilePath())
		reportInfo = append(reportInfo,
			LineInfo{},
			LineInfo{Key: "restore status:", Value: restoreStatus})
	} else if errMsg != "" {
		reportInfo = append(reportInfo,
			LineInfo{},
			LineInfo{Key: "restore status:", Value: "Failure"},
			LineInfo{Key: "restore error:", Value: errMsg})
	} else {
		reportInfo = append(reportInfo,
			LineInfo{},
			LineInfo{Key: "restore status:", Value: "Success"})
	}

	logOutputReport(reportFile, reportInfo)

	err = reportFile.Close()
	gplog.FatalOnError(err)
	_ = operating.System.Chmod(reportFilename, 0444)
}

func logOutputReport(reportFile io.WriteCloser, reportInfo []LineInfo) {
	maxSize := 0
	for _, lineInfo := range reportInfo {
		k := lineInfo.Key
		if len(k) > maxSize {
			maxSize = len(k)
		}
	}

	for _, lineInfo := range reportInfo {
		if lineInfo.Key == "" {
			utils.MustPrintf(reportFile, fmt.Sprintf("\n"))
		} else {
			utils.MustPrintf(reportFile, fmt.Sprintf("%-*s%s\n", maxSize+3, lineInfo.Key, lineInfo.Value))
		}
	}
}

func GetDurationInfo(timestamp string, endTime time.Time) (string, string, string) {
	startTime, _ := time.ParseInLocation("20060102150405", timestamp, operating.System.Local)
	duration := reformatDuration(endTime.Sub(startTime))
	startTimestamp := startTime.Format("Mon Jan 02 2006 15:04:05")
	endTimestamp := endTime.Format("Mon Jan 02 2006 15:04:05")
	return startTimestamp, endTimestamp, duration
}

// Turns "1h2m3.456s" into "1:02:03"
func reformatDuration(duration time.Duration) string {
	hour := duration / time.Hour
	duration -= hour * time.Hour
	min := duration / time.Minute
	duration -= min * time.Minute
	sec := duration / time.Second
	return fmt.Sprintf("%d:%02d:%02d", hour, min, sec)
}

func PrintObjectCounts(reportFile io.WriteCloser, objectCounts map[string]int) {
	objectStr := "\ncount of database objects in backup:\n"
	objectSlice := make([]string, 0)
	maxSize := 0
	for k := range objectCounts {
		objectSlice = append(objectSlice, k)
		if len(k) > maxSize {
			maxSize = len(k)
		}
	}
	sort.Strings(objectSlice)
	for _, object := range objectSlice {
		if object == "Database GUC's" {
			objectStr += fmt.Sprintf("%-*s%d\n", maxSize+3, "database GUC's", objectCounts[object])
		} else {
			objectStr += fmt.Sprintf("%-*s%d\n", maxSize+3, strings.ToLower(object), objectCounts[object])
		}
	}
	utils.MustPrintf(reportFile, objectStr)
}

/*
 * This function will not error out if the user has gprestore X.Y.Z
 * and gpbackup X.Y.Z+dev, when technically the uncommitted code changes
 * in the +dev version of gpbackup may have incompatibilities with the
 * committed version of gprestore.
 *
 * We assume this condition will never arise in practice, as gpbackup and
 * gprestore will be built with identical versions during development, and
 * users will never use a +dev version in production.
 */
func EnsureBackupVersionCompatibility(backupVersion string, restoreVersion string) {
	backupSemVer, err := semver.Make(backupVersion)
	gplog.FatalOnError(err)
	restoreSemVer, err := semver.Make(restoreVersion)
	gplog.FatalOnError(err)
	if backupSemVer.GT(restoreSemVer) {
		gplog.Fatal(errors.Errorf("gprestore %s cannot restore a backup taken with gpbackup %s; please use gprestore %s or later.",
			restoreVersion, backupVersion, backupVersion), "")
	}
}

func EnsureDatabaseVersionCompatibility(backupGPDBVersion string, restoreGPDBVersion dbconn.GPDBVersion) {
	pattern := regexp.MustCompile(`\d+\.\d+\.\d+`)
	threeDigitVersion := pattern.FindStringSubmatch(backupGPDBVersion)[0]
	backupGPDBSemVer, err := semver.Make(threeDigitVersion)
	gplog.FatalOnError(err)
	if backupGPDBSemVer.Major > restoreGPDBVersion.SemVer.Major {
		gplog.Fatal(errors.Errorf("Cannot restore from GPDB version %s to %s due to catalog incompatibilities.", backupGPDBVersion, restoreGPDBVersion.VersionString), "")
	}
}

type ContactFile struct {
	Contacts map[string][]EmailContact
}

type EmailContact struct {
	Address string
	Status  map[string]bool
}

func GetContacts(filename string, utility string) string {
	contactFile := &ContactFile{}
	contents, err := operating.System.ReadFile(filename)
	gplog.FatalOnError(err)
	err = yaml.Unmarshal(contents, contactFile)
	if err != nil {
		gplog.Warn("Unable to send email report: Error reading email contacts file.")
		gplog.Warn("Please ensure that the email contacts file is in valid YAML format.")
		return ""
	}

	errorCode := gplog.GetErrorCode()
	exitStatus := "success"
	if errorCode == 1 {
		exitStatus = "success_with_errors"
	} else if errorCode == 2 {
		exitStatus = "failure"
	}

	contactList := make([]string, 0)
	for _, contact := range contactFile.Contacts[utility] {
		if contact.Status[exitStatus] {
			contactList = append(contactList, contact.Address)
		}
	}
	return strings.Join(contactList, " ")
}

func ConstructEmailMessage(timestamp string, contactList string, reportFilePath string, utility string, status bool, dbname string) string {
	hostname, _ := operating.System.Hostname()
	statusString := history.BackupStatusSucceed
	if !status {
		statusString = history.BackupStatusFailed
	}
	emailHeader := fmt.Sprintf(`To: %s
Subject: %s %s of database %s on %s completed: %s
Content-Type: text/html
Content-Disposition: inline
<html>
<body>
<pre style=\"font: monospace\">
`, contactList, utility, timestamp, dbname, hostname, statusString)

	emailFooter := `
</pre>
</body>
</html>`
	fileContents := strings.Join(iohelper.MustReadLinesFromFile(reportFilePath), "\n")
	return emailHeader + fileContents + emailFooter
}

func EmailReport(c *cluster.Cluster, timestamp string, reportFilePath string, utility string, status bool, dbname string) {
	contactsFilename := "gp_email_contacts.yaml"
	gphomeFile := fmt.Sprintf("%s/bin/%s", operating.System.Getenv("GPHOME"), contactsFilename)
	homeFile := fmt.Sprintf("%s/%s", operating.System.Getenv("HOME"), contactsFilename)
	_, homeErr := c.ExecuteLocalCommand(fmt.Sprintf("test -f %s", homeFile))
	if homeErr != nil {
		_, gphomeErr := c.ExecuteLocalCommand(fmt.Sprintf("test -f %s", gphomeFile))
		if gphomeErr != nil {
			gplog.Info("Found neither %s nor %s", gphomeFile, homeFile)
			gplog.Info("Email containing %s report %s will not be sent", utility, reportFilePath)
			return
		}
		contactsFilename = gphomeFile
	} else {
		contactsFilename = homeFile
	}
	gplog.Info("%s list found, %s will be sent", contactsFilename, reportFilePath)
	contactList := GetContacts(contactsFilename, utility)
	if contactList == "" {
		return
	}
	message := ConstructEmailMessage(timestamp, contactList, reportFilePath, utility, status, dbname)
	gplog.Verbose("Sending email report to the following addresses: %s", contactList)
	output, sendErr := c.ExecuteLocalCommand(fmt.Sprintf(`echo "%s" | sendmail -t`, message))
	if sendErr != nil {
		gplog.Warn("Unable to send email report: %s", output)
	}
}

func AppendBackupParams(infoArr *[]LineInfo, paramsStr string) {
	paramsStr = strings.Trim(paramsStr, "\n")
	params := strings.Split(paramsStr, "\n")
	for _, param := range params {
		if strings.Contains(param, ":") {
			tup := strings.Split(param, ":")
			k := strings.TrimSpace(tup[0])
			v := strings.TrimSpace(tup[1])
			*infoArr = append(*infoArr, LineInfo{Key: k + ":", Value: v})
		} else {
			// timestamps following 'incremental backup set' do not have colons
			*infoArr = append(*infoArr, LineInfo{Key: param, Value: ""})
		}
	}
}
