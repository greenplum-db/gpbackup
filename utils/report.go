package utils

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	yaml "gopkg.in/yaml.v2"

	"github.com/blang/semver"
	"github.com/pkg/errors"
)

type BackupConfig struct {
	BackupVersion   string
	DatabaseName    string
	DatabaseVersion string
	Compressed      bool
	DataOnly        bool
	SchemaFiltered  bool
	TableFiltered   bool
	MetadataOnly    bool
	WithStatistics  bool
	SingleDataFile  bool
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

func ParseErrorMessage(errStr string) (string, int) {
	if errStr == "" {
		return "", 0
	}
	errLevelStr := "[CRITICAL]:-"
	headerIndex := strings.Index(errStr, errLevelStr)
	errMsg := errStr[headerIndex+len(errLevelStr):]
	exitCode := 1 // TODO: Define different error codes for different kinds of errors
	return errMsg, exitCode
}

func (report *Report) ConstructBackupParamsStringFromFlags(dataOnly bool, ddlOnly bool, isSchemaFiltered bool, isTableFiltered bool, singleDataFile bool, withStats bool) {
	filterStr := "None"
	if isSchemaFiltered {
		report.SchemaFiltered = true
		filterStr = "Schema Filter"
	} else if isTableFiltered {
		report.TableFiltered = true
		filterStr = "Table Filter"
	}
	compressStr := "None"
	compressed, program := GetCompressionParameters()
	if compressed {
		report.Compressed = true
		compressStr = program.Name
	}
	sectionStr := "All Sections"
	if dataOnly {
		report.DataOnly = true
		sectionStr = "Data Only"
	}
	if ddlOnly {
		report.MetadataOnly = true
		sectionStr = "Metadata Only"
	}
	filesStr := "Multiple Data Files Per Segment"
	if ddlOnly {
		filesStr = "No Data Files"
	} else if singleDataFile {
		report.SingleDataFile = true
		filesStr = "Single Data File Per Segment"
	}
	statsStr := "No"
	if withStats {
		report.WithStatistics = true
		statsStr = "Yes"
	}
	backupParamsTemplate := `Compression: %s
Backup Section: %s
Object Filtering: %s
Includes Statistics: %s
Data File Format: %s`
	report.BackupParamsString = fmt.Sprintf(backupParamsTemplate, compressStr, sectionStr, filterStr, statsStr, filesStr)
}

func ReadConfigFile(filename string) *BackupConfig {
	config := &BackupConfig{}
	contents, err := ioutil.ReadFile(filename)
	CheckError(err)
	err = yaml.Unmarshal(contents, config)
	CheckError(err)
	return config
}

func (report *Report) WriteConfigFile(configFilename string) {
	configFile := MustOpenFileForWriting(configFilename)
	defer System.Chmod(configFilename, 0444)
	config := report.BackupConfig
	configContents, _ := yaml.Marshal(config)
	MustPrintBytes(configFile, configContents)
}

func (report *Report) WriteReportFile(reportFilename string, timestamp string, objectCounts map[string]int, endTime time.Time, errMsg string) {
	reportFile := MustOpenFileForWriting(reportFilename)
	defer System.Chmod(reportFilename, 0444)
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
	start, end, duration := GetBackupTimeInfo(timestamp, endTime)
	backupStatus, dbSizeStr := getStatusAndSizeStrings(errMsg, report.DatabaseSize)

	MustPrintf(reportFile, reportFileTemplate,
		timestamp, report.DatabaseVersion, report.BackupVersion,
		report.DatabaseName, gpbackupCommandLine, report.BackupParamsString,
		start, end, duration,
		backupStatus, dbSizeStr)

	PrintObjectCounts(reportFile, objectCounts)
}

func GetBackupTimeInfo(timestamp string, endTime time.Time) (string, string, string) {
	startTime, _ := time.ParseInLocation("20060102150405", timestamp, time.Local)
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

func getStatusAndSizeStrings(errMsg string, dbSize string) (string, string) {
	backupStatus := "Success"
	if errMsg != "" {
		backupStatus = fmt.Sprintf("Failure\nBackup Error: %s", errMsg)
	}
	dbSizeStr := ""
	if dbSize != "" {
		dbSizeStr = fmt.Sprintf("\nDatabase Size: %s", dbSize)
	}
	return backupStatus, dbSizeStr
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
	CheckError(err)
	restoreSemVer, err := semver.Make(restoreVersion)
	CheckError(err)
	if backupSemVer.GT(restoreSemVer) {
		logger.Fatal(errors.Errorf("gprestore %s cannot restore a backup taken with gpbackup %s; please use gprestore %s or later.",
			restoreVersion, backupVersion, backupVersion), "")
	}
}

func EnsureDatabaseVersionCompatibility(backupGPDBVersion string, restoreGPDBVersion GPDBVersion) {
	pattern := regexp.MustCompile(`\d+\.\d+\.\d+`)
	threeDigitVersion := pattern.FindStringSubmatch(backupGPDBVersion)[0]
	backupGPDBSemVer, err := semver.Make(threeDigitVersion)
	CheckError(err)
	if backupGPDBSemVer.Major > restoreGPDBVersion.SemVer.Major {
		logger.Fatal(errors.Errorf("Cannot restore from GPDB version %s to %s due to catalog incompatibilities.", backupGPDBVersion, restoreGPDBVersion.VersionString), "")
	}
}

func ConstructEmailMessage(cluster Cluster, contactList string) string {
	hostname, _ := System.Hostname()
	emailHeader := fmt.Sprintf(`To: %s
Subject: gpbackup %s on %s completed
Content-Type: text/html
Content-Disposition: inline
<html>
<body>
<pre style=\"font: monospace\">
`, contactList, cluster.Timestamp, hostname)
	emailFooter := `
</pre>
</body>
</html>`
	fileContents := strings.Join(ReadLinesFromFile(cluster.GetReportFilePath()), "\n")
	return emailHeader + fileContents + emailFooter
}

func EmailReport(cluster Cluster) {
	contactsFilename := "mail_contacts"
	gphomeFile := fmt.Sprintf("%s/bin/%s", System.Getenv("GPHOME"), contactsFilename)
	homeFile := fmt.Sprintf("%s/%s", System.Getenv("HOME"), contactsFilename)
	homeErr := cluster.ExecuteLocalCommand(fmt.Sprintf("test -f %s", homeFile))
	if homeErr != nil {
		gphomeErr := cluster.ExecuteLocalCommand(fmt.Sprintf("test -f %s", gphomeFile))
		if gphomeErr != nil {
			logger.Warn("Found neither %s nor %s", gphomeFile, homeFile)
			logger.Warn("Unable to send backup email notification")
			return
		}
		contactsFilename = gphomeFile
	} else {
		contactsFilename = homeFile
	}
	contactList := strings.Join(ReadLinesFromFile(contactsFilename), " ")
	message := ConstructEmailMessage(cluster, contactList)
	logger.Verbose("Sending email report to the following addresses: %s", contactList)
	sendErr := cluster.ExecuteLocalCommand(fmt.Sprintf(`echo "%s" | sendmail -t`, message))
	if sendErr != nil {
		logger.Warn("Unable to send email report: %s", sendErr.Error())
	}
}
