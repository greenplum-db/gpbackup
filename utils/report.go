package utils

import (
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	yaml "gopkg.in/yaml.v2"

	"github.com/blang/semver"
	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gp-common-go-libs/iohelper"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/pkg/errors"
)

type BackupConfig struct {
	BackupVersion         string
	DatabaseName          string
	DatabaseVersion       string
	Compressed            bool
	DataOnly              bool
	IncludeSchemaFiltered bool
	IncludeTableFiltered  bool
	ExcludeSchemaFiltered bool
	ExcludeTableFiltered  bool
	MetadataOnly          bool
	Plugin                string
	SingleDataFile        bool
	WithStatistics        bool
}

/*
 * This struct holds information that will be printed to the report file
 * after a backup, as well as information printed to the configuration
 * file that we will want to read in for a restore.
 */
type Report struct {
	BackupParamsString string
	DatabaseSize       string
	BackupConfig
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

func (report *Report) SetBackupParamsFromFlags(dataOnly bool, ddlOnly bool, plugin string, isIncludeSchemaFiltered bool, isIncludeTableFiltered bool, isExcludeSchemaFiltered bool, isExcludeTableFiltered bool, singleDataFile bool, withStats bool) {
	compressed, _ := GetCompressionParameters()
	report.Compressed = compressed
	report.IncludeSchemaFiltered = isIncludeSchemaFiltered
	report.IncludeTableFiltered = isIncludeTableFiltered
	report.ExcludeSchemaFiltered = isExcludeSchemaFiltered
	report.ExcludeTableFiltered = isExcludeTableFiltered
	report.DataOnly = dataOnly
	report.MetadataOnly = ddlOnly
	report.Plugin = plugin
	report.SingleDataFile = singleDataFile
	report.WithStatistics = withStats
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
	_, program := GetCompressionParameters()
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
	backupParamsTemplate := `Compression: %s
Plugin Executable: %s
Backup Section: %s
Object Filtering: %s
Includes Statistics: %s
Data File Format: %s`
	report.BackupParamsString = fmt.Sprintf(backupParamsTemplate, compressStr, pluginStr, sectionStr, filterStr, statsStr, filesStr)
}

func ReadConfigFile(filename string) *BackupConfig {
	config := &BackupConfig{}
	contents, err := operating.System.ReadFile(filename)
	gplog.FatalOnError(err)
	err = yaml.Unmarshal(contents, config)
	gplog.FatalOnError(err)
	return config
}

func (report *Report) WriteConfigFile(configFilename string) {
	configFile := iohelper.MustOpenFileForWriting(configFilename)
	config := report.BackupConfig
	configContents, _ := yaml.Marshal(config)
	MustPrintBytes(configFile, configContents)
	err := operating.System.Chmod(configFilename, 0444)
	gplog.FatalOnError(err)
}

func (report *Report) WriteBackupReportFile(reportFilename string, timestamp string, objectCounts map[string]int, errMsg string) {
	reportFile := iohelper.MustOpenFileForWriting(reportFilename)
	reportFileTemplate := `Greenplum Database Backup Report

Timestamp Key: %s
GPDB Version: %s
gpbackup Version: %s

Database Name: %s
Command Line: %s
%s

Start Time: %s
End Time: %s
Duration: %s

Backup Status: %s
%s`

	gpbackupCommandLine := strings.Join(os.Args, " ")
	start, end, duration := GetDurationInfo(timestamp, operating.System.Now())
	backupStatus := "Success"
	if errMsg != "" {
		backupStatus = fmt.Sprintf("Failure\nBackup Error: %s", errMsg)
	}
	dbSizeStr := ""
	if report.DatabaseSize != "" {
		dbSizeStr = fmt.Sprintf("\nDatabase Size: %s", report.DatabaseSize)
	}

	MustPrintf(reportFile, reportFileTemplate,
		timestamp, report.DatabaseVersion, report.BackupVersion,
		report.DatabaseName, gpbackupCommandLine, report.BackupParamsString,
		start, end, duration,
		backupStatus, dbSizeStr)

	PrintObjectCounts(reportFile, objectCounts)
	err := operating.System.Chmod(reportFilename, 0444)
	gplog.FatalOnError(err)
}

func WriteRestoreReportFile(reportFilename string, backupTimestamp string, startTimestamp string, connection *dbconn.DBConn, restoreVersion string, errMsg string) {
	reportFile := iohelper.MustOpenFileForWriting(reportFilename)
	reportFileTemplate := `Greenplum Database Restore Report

Timestamp Key: %s
GPDB Version: %s
gprestore Version: %s

Database Name: %s
Command Line: %s

Start Time: %s
End Time: %s
Duration: %s

Restore Status: %s`

	gprestoreCommandLine := strings.Join(os.Args, " ")
	start, end, duration := GetDurationInfo(startTimestamp, operating.System.Now())
	restoreStatus := "Success"
	errorCode := gplog.GetErrorCode()
	if errorCode == 1 {
		restoreStatus = fmt.Sprintf("Success but non-fatal errors occurred. See log file %s for details.", gplog.GetLogFilePath())
	} else if errMsg != "" {
		restoreStatus = fmt.Sprintf("Failure\nRestore Error: %s", errMsg)
	}

	MustPrintf(reportFile, reportFileTemplate,
		backupTimestamp, connection.Version.VersionString, restoreVersion,
		connection.DBName, gprestoreCommandLine,
		start, end, duration, restoreStatus)
	err := operating.System.Chmod(reportFilename, 0444)
	gplog.FatalOnError(err)
}

func GetDurationInfo(timestamp string, endTime time.Time) (string, string, string) {
	startTime, _ := time.ParseInLocation("20060102150405", timestamp, operating.System.Local)
	duration := reformatDuration(endTime.Sub(startTime))
	startTimestamp := startTime.Format("2006-01-02 15:04:05")
	endTimestamp := endTime.Format("2006-01-02 15:04:05")
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
	objectStr := "\nCount of Database Objects in Backup:\n"
	objectSlice := make([]string, 0)
	for k := range objectCounts {
		objectSlice = append(objectSlice, k)
	}
	sort.Strings(objectSlice)
	for _, object := range objectSlice {
		objectStr += fmt.Sprintf("%-29s%d\n", object, objectCounts[object])

	}
	MustPrintf(reportFile, objectStr)
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

func ConstructEmailMessage(timestamp string, contactList string, reportFilePath string, utility string) string {
	hostname, _ := operating.System.Hostname()
	emailHeader := fmt.Sprintf(`To: %s
Subject: %s %s on %s completed
Content-Type: text/html
Content-Disposition: inline
<html>
<body>
<pre style=\"font: monospace\">
`, contactList, utility, timestamp, hostname)
	emailFooter := `
</pre>
</body>
</html>`
	fileContents := strings.Join(iohelper.MustReadLinesFromFile(reportFilePath), "\n")
	return emailHeader + fileContents + emailFooter
}

func EmailReport(c *cluster.Cluster, timestamp string, reportFilePath string, utility string) {
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
	message := ConstructEmailMessage(timestamp, contactList, reportFilePath, utility)
	gplog.Verbose("Sending email report to the following addresses: %s", contactList)
	output, sendErr := c.ExecuteLocalCommand(fmt.Sprintf(`echo "%s" | sendmail -t`, message))
	if sendErr != nil {
		gplog.Warn("Unable to send email report: %s", output)
	}
}
