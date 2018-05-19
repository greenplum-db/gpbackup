package utils_test

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"time"

	"github.com/blang/semver"
	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/greenplum-db/gp-common-go-libs/structmatcher"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/utils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
	"github.com/pkg/errors"
	yaml "gopkg.in/yaml.v2"
)

var _ = Describe("utils/report tests", func() {
	Describe("ParseErrorMessage", func() {
		It("Parses a CRITICAL error message and returns error code 1", func() {
			errStr := "testProgram:testUser:testHost:000000-[CRITICAL]:-Error Message"
			errMsg := utils.ParseErrorMessage(errStr)
			Expect(errMsg).To(Equal("Error Message"))
		})
		It("Returns error code 0 for an empty error message", func() {
			errMsg := utils.ParseErrorMessage("")
			Expect(errMsg).To(Equal(""))
		})
	})
	Describe("WriteBackupReportFile", func() {
		timestamp := "20170101010101"
		config := utils.BackupConfig{
			BackupVersion:   "0.1.0",
			DatabaseName:    "testdb",
			DatabaseVersion: "5.0.0 build test",
		}
		backupReport := &utils.Report{}
		objectCounts := map[string]int{"tables": 42, "sequences": 1, "types": 1000}
		BeforeEach(func() {
			backupReport = &utils.Report{
				BackupParamsString: `Compression: gzip
Backup Section: All Sections
Object Filtering: None
Includes Statistics: No
Data File Format: Single Data File Per Segment`,
				DatabaseSize: "42 MB",
				BackupConfig: config,
			}
			operating.System.OpenFileWrite = func(name string, flag int, perm os.FileMode) (io.WriteCloser, error) {
				return buffer, nil
			}
			operating.System.Now = func() time.Time {
				return time.Date(2017, 1, 1, 5, 4, 3, 2, time.Local)
			}
			operating.System.Chmod = func(name string, mode os.FileMode) error {
				return nil
			}

		})

		It("writes a report for a successful backup", func() {
			backupReport.WriteBackupReportFile("filename", timestamp, objectCounts, "")
			Expect(buffer).To(gbytes.Say(`Greenplum Database Backup Report

Timestamp Key: 20170101010101
GPDB Version: 5\.0\.0 build test
gpbackup Version: 0\.1\.0

Database Name: testdb
Command Line: .*
Compression: gzip
Backup Section: All Sections
Object Filtering: None
Includes Statistics: No
Data File Format: Single Data File Per Segment

Start Time: 2017-01-01 01:01:01
End Time: 2017-01-01 05:04:03
Duration: 4:03:02

Backup Status: Success

Database Size: 42 MB
Count of Database Objects in Backup:
sequences                    1
tables                       42
types                        1000`))
		})
		It("writes a report for a failed backup", func() {
			backupReport.WriteBackupReportFile("filename", timestamp, objectCounts, "Cannot access /tmp/backups: Permission denied")
			Expect(buffer).To(gbytes.Say(`Greenplum Database Backup Report

Timestamp Key: 20170101010101
GPDB Version: 5\.0\.0 build test
gpbackup Version: 0\.1\.0

Database Name: testdb
Command Line: .*
Compression: gzip
Backup Section: All Sections
Object Filtering: None
Includes Statistics: No
Data File Format: Single Data File Per Segment

Start Time: 2017-01-01 01:01:01
End Time: 2017-01-01 05:04:03
Duration: 4:03:02

Backup Status: Failure
Backup Error: Cannot access /tmp/backups: Permission denied

Database Size: 42 MB
Count of Database Objects in Backup:
sequences                    1
tables                       42
types                        1000`))
		})
		It("writes a report without database size information", func() {
			backupReport.DatabaseSize = ""
			backupReport.WriteBackupReportFile("filename", timestamp, objectCounts, "")
			Expect(buffer).To(gbytes.Say(`Greenplum Database Backup Report

Timestamp Key: 20170101010101
GPDB Version: 5\.0\.0 build test
gpbackup Version: 0\.1\.0

Database Name: testdb
Command Line: .*
Compression: gzip
Backup Section: All Sections
Object Filtering: None
Includes Statistics: No
Data File Format: Single Data File Per Segment

Start Time: 2017-01-01 01:01:01
End Time: 2017-01-01 05:04:03
Duration: 4:03:02

Backup Status: Success

Count of Database Objects in Backup:
sequences                    1
tables                       42
types                        1000`))
		})
	})
	Describe("WriteRestoreReportFile", func() {
		timestamp := "20170101010101"
		restoreStartTime := "20170101010102"
		restoreVersion := "0.1.0"
		connection := &dbconn.DBConn{
			DBName: "testdb",
			Version: dbconn.GPDBVersion{
				VersionString: "5.0.0 build test",
			},
		}
		BeforeEach(func() {
			operating.System.OpenFileWrite = func(name string, flag int, perm os.FileMode) (io.WriteCloser, error) {
				return buffer, nil
			}
			operating.System.Now = func() time.Time {
				return time.Date(2017, 1, 1, 5, 4, 3, 2, time.Local)
			}
			operating.System.Chmod = func(name string, mode os.FileMode) error {
				return nil
			}

		})
		AfterEach(func() {
			gplog.SetErrorCode(0)
		})

		It("writes a report for a failed restore", func() {
			gplog.SetErrorCode(2)
			utils.WriteRestoreReportFile("filename", timestamp, restoreStartTime, connection, restoreVersion, "Cannot access /tmp/backups: Permission denied")
			Expect(buffer).To(gbytes.Say(`Greenplum Database Restore Report

Timestamp Key: 20170101010101
GPDB Version: 5\.0\.0 build test
gprestore Version: 0\.1\.0

Database Name: testdb
Command Line: .*

Start Time: 2017-01-01 01:01:02
End Time: 2017-01-01 05:04:03
Duration: 4:03:01

Restore Status: Failure
Restore Error: Cannot access /tmp/backups: Permission denied`))
		})
		It("writes a report for a successful restore", func() {
			gplog.SetErrorCode(0)
			utils.WriteRestoreReportFile("filename", timestamp, restoreStartTime, connection, restoreVersion, "")
			Expect(buffer).To(gbytes.Say(`Greenplum Database Restore Report

Timestamp Key: 20170101010101
GPDB Version: 5\.0\.0 build test
gprestore Version: 0\.1\.0

Database Name: testdb
Command Line: .*

Start Time: 2017-01-01 01:01:02
End Time: 2017-01-01 05:04:03
Duration: 4:03:01

Restore Status: Success`))
		})
		It("writes a report for a successful restore with errors", func() {
			gplog.SetErrorCode(1)
			utils.WriteRestoreReportFile("filename", timestamp, restoreStartTime, connection, restoreVersion, "")
			Expect(buffer).To(gbytes.Say(`Greenplum Database Restore Report

Timestamp Key: 20170101010101
GPDB Version: 5\.0\.0 build test
gprestore Version: 0\.1\.0

Database Name: testdb
Command Line: .*

Start Time: 2017-01-01 01:01:02
End Time: 2017-01-01 05:04:03
Duration: 4:03:01

Restore Status: Success but non-fatal errors occurred. See log file .+ for details.`))
		})
	})
	Describe("SetBackupParamFromFlags", func() {
		var backupReport *utils.Report
		AfterEach(func() {
			utils.InitializeCompressionParameters(false, 0)
		})
		It("configures the Report struct correctly", func() {
			backupReport = &utils.Report{}
			utils.InitializeCompressionParameters(true, 0)
			backupReport.SetBackupParamsFromFlags(true, true, "plugin", true, true, true, true, true, true)
			structmatcher.ExpectStructsToMatch(backupReport.BackupConfig, utils.BackupConfig{
				BackupVersion: "", DatabaseName: "", DatabaseVersion: "",
				DataOnly: true, Compressed: true, Plugin: "plugin", IncludeSchemaFiltered: true,
				IncludeTableFiltered: true, ExcludeSchemaFiltered: true, ExcludeTableFiltered: true,
				MetadataOnly: true, WithStatistics: true, SingleDataFile: true,
			})
		})
	})
	Describe("ConstructBackupParamString", func() {
		var backupReport *utils.Report
		BeforeEach(func() {
			backupReport = &utils.Report{}
		})
		AfterEach(func() {
			utils.InitializeCompressionParameters(false, 0)
		})
		DescribeTable("Backup type classification", func(dataOnly bool, ddlOnly bool, noCompression bool, plugin string, isIncludeSchemaFiltered bool, isIncludeTableFiltered bool, isExcludeSchemaFiltered bool, isExcludeTableFiltered bool, singleDataFile bool, withStats bool, expectedType string) {
			utils.InitializeCompressionParameters(!noCompression, 0)
			backupReport.SetBackupParamsFromFlags(dataOnly, ddlOnly, plugin, isIncludeSchemaFiltered, isIncludeTableFiltered, isExcludeSchemaFiltered, isExcludeTableFiltered, singleDataFile, withStats)
			backupReport.ConstructBackupParamsString()
			Expect(backupReport.BackupParamsString).To(Equal(expectedType))
		},
			Entry("classifies a default backup",
				false, false, false, "", false, false, false, false, false, false, `Compression: gzip
Plugin Executable: None
Backup Section: All Sections
Object Filtering: None
Includes Statistics: No
Data File Format: Multiple Data Files Per Segment`),
			Entry("classifies a default backup with stats",
				false, false, false, "", false, false, false, false, false, true, `Compression: gzip
Plugin Executable: None
Backup Section: All Sections
Object Filtering: None
Includes Statistics: Yes
Data File Format: Multiple Data Files Per Segment`),
			Entry("classifies a default backup with a single data file",
				false, false, false, "", false, false, false, false, true, false, `Compression: gzip
Plugin Executable: None
Backup Section: All Sections
Object Filtering: None
Includes Statistics: No
Data File Format: Single Data File Per Segment`),
			Entry("classifies a default backup with statistics and a single data file",
				false, false, false, "", false, false, false, false, true, true, `Compression: gzip
Plugin Executable: None
Backup Section: All Sections
Object Filtering: None
Includes Statistics: Yes
Data File Format: Single Data File Per Segment`),
			Entry("classifies a metadata-only backup",
				false, true, false, "", false, false, false, false, false, false, `Compression: gzip
Plugin Executable: None
Backup Section: Metadata Only
Object Filtering: None
Includes Statistics: No
Data File Format: No Data Files`),
			Entry("classifies a data-only backup",
				true, false, false, "", false, false, false, false, false, false, `Compression: gzip
Plugin Executable: None
Backup Section: Data Only
Object Filtering: None
Includes Statistics: No
Data File Format: Multiple Data Files Per Segment`),
			Entry("classifies an uncompressed backup",
				false, false, true, "", false, false, false, false, false, false, `Compression: None
Plugin Executable: None
Backup Section: All Sections
Object Filtering: None
Includes Statistics: No
Data File Format: Multiple Data Files Per Segment`),
			Entry("classifies a backup using a plugin",
				false, false, false, "/tmp/plugin", false, false, false, false, false, false, `Compression: gzip
Plugin Executable: /tmp/plugin
Backup Section: All Sections
Object Filtering: None
Includes Statistics: No
Data File Format: Multiple Data Files Per Segment`),
			Entry("classifies an include-schema-filtered backup",
				false, false, false, "", true, false, false, false, false, false, `Compression: gzip
Plugin Executable: None
Backup Section: All Sections
Object Filtering: Include Schema Filter 
Includes Statistics: No
Data File Format: Multiple Data Files Per Segment`),
			Entry("classifies an include-table-filtered backup",
				false, false, false, "", false, true, false, false, false, false, `Compression: gzip
Plugin Executable: None
Backup Section: All Sections
Object Filtering: Include Table Filter
Includes Statistics: No
Data File Format: Multiple Data Files Per Segment`),
			Entry("classifies an exclude-schema-filtered backup",
				false, false, false, "", false, false, true, false, false, false, `Compression: gzip
Plugin Executable: None
Backup Section: All Sections
Object Filtering: Exclude Schema Filter
Includes Statistics: No
Data File Format: Multiple Data Files Per Segment`),
			Entry("classifies an exclude-table-filtered backup",
				false, false, false, "", false, false, false, true, false, false, `Compression: gzip
Plugin Executable: None
Backup Section: All Sections
Object Filtering: Exclude Table Filter
Includes Statistics: No
Data File Format: Multiple Data Files Per Segment`),
			Entry("classifies an include-schema-filtered, exclude-table-filtered backup",
				false, false, false, "", true, false, false, true, false, false, `Compression: gzip
Plugin Executable: None
Backup Section: All Sections
Object Filtering: Include Schema Filter Exclude Table Filter
Includes Statistics: No
Data File Format: Multiple Data Files Per Segment`),
		)
	})
	Describe("GetDurationInfo", func() {
		timestamp := "20170101010101"
		AfterEach(func() {
			operating.System.Local = time.Local
		})
		It("prints times and duration for a sub-minute backup", func() {
			endTime := time.Date(2017, 1, 1, 1, 1, 3, 2, operating.System.Local)
			start, end, duration := utils.GetDurationInfo(timestamp, endTime)
			Expect(start).To(Equal("2017-01-01 01:01:01"))
			Expect(end).To(Equal("2017-01-01 01:01:03"))
			Expect(duration).To(Equal("0:00:02"))
		})
		It("prints times and duration for a sub-hour backup", func() {
			endTime := time.Date(2017, 1, 1, 1, 4, 3, 2, operating.System.Local)
			start, end, duration := utils.GetDurationInfo(timestamp, endTime)
			Expect(start).To(Equal("2017-01-01 01:01:01"))
			Expect(end).To(Equal("2017-01-01 01:04:03"))
			Expect(duration).To(Equal("0:03:02"))
		})
		It("prints times and duration for a multiple-hour backup", func() {
			endTime := time.Date(2017, 1, 1, 5, 4, 3, 2, operating.System.Local)
			start, end, duration := utils.GetDurationInfo(timestamp, endTime)
			Expect(start).To(Equal("2017-01-01 01:01:01"))
			Expect(end).To(Equal("2017-01-01 05:04:03"))
			Expect(duration).To(Equal("4:03:02"))
		})
		It("prints times and duration for a backup going past midnight", func() {
			endTime := time.Date(2017, 1, 2, 1, 4, 3, 2, operating.System.Local)
			start, end, duration := utils.GetDurationInfo(timestamp, endTime)
			Expect(start).To(Equal("2017-01-01 01:01:01"))
			Expect(end).To(Equal("2017-01-02 01:04:03"))
			Expect(duration).To(Equal("24:03:02"))
		})
		It("prints times and duration for a backup during the spring time change", func() {
			operating.System.Local, _ = time.LoadLocation("America/Los_Angeles") // Ensure test works regardless of time zone of test machine
			dst := "20170312010000"
			endTime := time.Date(2017, 3, 12, 3, 0, 0, 0, operating.System.Local)
			start, end, duration := utils.GetDurationInfo(dst, endTime)
			Expect(start).To(Equal("2017-03-12 01:00:00"))
			Expect(end).To(Equal("2017-03-12 03:00:00"))
			Expect(duration).To(Equal("1:00:00"))
		})
		It("prints times and duration for a backup during the fall time change", func() {
			operating.System.Local, _ = time.LoadLocation("America/Los_Angeles") // Ensure test works regardless of time zone of test machine
			dst := "20171105010000"
			endTime := time.Date(2017, 11, 5, 3, 0, 0, 0, operating.System.Local)
			start, end, duration := utils.GetDurationInfo(dst, endTime)
			Expect(start).To(Equal("2017-11-05 01:00:00"))
			Expect(end).To(Equal("2017-11-05 03:00:00"))
			Expect(duration).To(Equal("3:00:00"))
		})
	})
	Describe("EnsureBackupVersionCompatibility", func() {
		It("Panics if gpbackup version is greater than gprestore version", func() {
			defer testhelper.ShouldPanicWithMessage("gprestore 0.1.0 cannot restore a backup taken with gpbackup 0.2.0; please use gprestore 0.2.0 or later.")
			utils.EnsureBackupVersionCompatibility("0.2.0", "0.1.0")
		})
		It("Does not panic if gpbackup version is less than gprestore version", func() {
			utils.EnsureBackupVersionCompatibility("0.1.0", "0.1.3")
		})
		It("Does not panic if gpbackup version equals gprestore version", func() {
			utils.EnsureBackupVersionCompatibility("0.1.0", "0.1.0")
		})
	})
	Describe("EnsureDatabaseVersionCompatibility", func() {
		var restoreVersion dbconn.GPDBVersion
		BeforeEach(func() {
			semver, _ := semver.Make("5.0.0")
			restoreVersion = dbconn.GPDBVersion{
				VersionString: "5.0.0-beta.9+dev.129.g4bd4e41 build dev",
				SemVer:        semver,
			}
		})
		It("Panics if backup database major version is greater than restore major version", func() {
			defer testhelper.ShouldPanicWithMessage("Cannot restore from GPDB version 6.0.0-beta.9+dev.129.g4bd4e41 build dev to 5.0.0-beta.9+dev.129.g4bd4e41 build dev due to catalog incompatibilities.")
			utils.EnsureDatabaseVersionCompatibility("6.0.0-beta.9+dev.129.g4bd4e41 build dev", restoreVersion)
		})
		It("Does not panic if backup database major version is greater than restore major version", func() {
			utils.EnsureDatabaseVersionCompatibility("4.3.16-beta.9+dev.129.g4bd4e41 build dev", restoreVersion)
		})
		It("Does not panic if backup database major version is equal to restore major version", func() {
			utils.EnsureDatabaseVersionCompatibility("5.0.6-beta.9+dev.129.g4bd4e41 build dev", restoreVersion)
		})
	})

	Describe("Email-related functions", func() {
		reportFileContents := []byte(`Greenplum Database Backup Report

Timestamp Key: 20170101010101`)
		contactsFileContents, _ := yaml.Marshal(utils.ContactFile{
			Contacts: map[string][]utils.EmailContact{
				"gpbackup": {
					{Address: "contact1@example.com",
						Status: map[string]bool{
							"success":             true,
							"success_with_errors": true,
							"failure":             false,
						}},
					{Address: "contact2@example.org",
						Status: map[string]bool{
							"success":             false,
							"success_with_errors": true,
							"failure":             true,
						}},
				},
				"gprestore": {
					{Address: "contact3@example.com"},
					{Address: "contact4@example.org",
						Status: map[string]bool{
							"success":             true,
							"success_with_errors": true,
							"failure":             true,
						}},
				},
			}})
		contactsList := "contact1@example.com contact2@example.org"

		var (
			testExecutor *testhelper.TestExecutor
			testCluster  cluster.Cluster
			testFPInfo   utils.FilePathInfo
			w            *os.File
			r            *os.File
		)
		BeforeEach(func() {
			r, w, _ = os.Pipe()
			testCluster = testutils.SetDefaultSegmentConfiguration()
			testFPInfo = utils.NewFilePathInfo(testCluster.SegDirMap, "", "20170101010101", "gpseg")
			operating.System.OpenFileRead = func(name string, flag int, perm os.FileMode) (operating.ReadCloserAt, error) { return r, nil }
			operating.System.ReadFile = func(filename string) ([]byte, error) { return ioutil.ReadAll(r) }
			operating.System.Hostname = func() (string, error) { return "localhost", nil }
			operating.System.Getenv = func(key string) string {
				if key == "HOME" {
					return "home"
				} else {
					return "gphome"
				}
			}
			testExecutor = &testhelper.TestExecutor{}
			testCluster.Executor = testExecutor
			gplog.SetErrorCode(0)
		})
		AfterEach(func() {
			operating.InitializeSystemFunctions()
			gplog.SetErrorCode(0)
		})
		Context("GetContacts", func() {
			contactsFilename := fmt.Sprintf("%s/bin/gp_email_contacts.yaml", operating.System.Getenv("GPHOME"))
			It("Gets a list of gpbackup contacts on success", func() {
				gplog.SetErrorCode(0)
				w.Write(contactsFileContents)
				w.Close()

				contacts := utils.GetContacts(contactsFilename, "gpbackup")
				Expect(contacts).To(Equal("contact1@example.com"))
			})
			It("Gets a list of gpbackup contacts on success with errors", func() {
				gplog.SetErrorCode(1)
				w.Write(contactsFileContents)
				w.Close()

				contacts := utils.GetContacts(contactsFilename, "gpbackup")
				Expect(contacts).To(Equal("contact1@example.com contact2@example.org"))
			})
			It("Gets a list of gpbackup contacts on failure", func() {
				gplog.SetErrorCode(2)
				w.Write(contactsFileContents)
				w.Close()

				contacts := utils.GetContacts(contactsFilename, "gpbackup")
				Expect(contacts).To(Equal("contact2@example.org"))
			})
			It("Gets a list of gprestore contacts and doesn't fail when no status specified", func() {
				gplog.SetErrorCode(0)
				w.Write(contactsFileContents)
				w.Close()

				contacts := utils.GetContacts(contactsFilename, "gprestore")
				Expect(contacts).To(Equal("contact4@example.org"))
			})
		})
		Context("ConstructEmailMessage", func() {
			It("adds HTML formatting to the contents of the report file", func() {
				w.Write(reportFileContents)
				w.Close()

				message := utils.ConstructEmailMessage(testFPInfo.Timestamp, contactsList, "report_file", "gpbackup")
				expectedMessage := `To: contact1@example.com contact2@example.org
Subject: gpbackup 20170101010101 on localhost completed
Content-Type: text/html
Content-Disposition: inline
<html>
<body>
<pre style=\"font: monospace\">
Greenplum Database Backup Report

Timestamp Key: 20170101010101
</pre>
</body>
</html>`
				Expect(message).To(Equal(expectedMessage))
			})
		})
		Context("EmailReport", func() {
			var (
				expectedHomeCmd   = "test -f home/gp_email_contacts.yaml"
				expectedGpHomeCmd = "test -f gphome/bin/gp_email_contacts.yaml"
				expectedMessage   = `echo "To: contact1@example.com
Subject: gpbackup 20170101010101 on localhost completed
Content-Type: text/html
Content-Disposition: inline
<html>
<body>
<pre style=\"font: monospace\">

</pre>
</body>
</html>" | sendmail -t`
			)
			It("sends no email and raises a warning if no gp_email_contacts.yaml file is found", func() {
				w.Write(contactsFileContents)
				w.Close()

				testExecutor.LocalError = errors.Errorf("exit status 2")

				utils.EmailReport(testCluster, testFPInfo.Timestamp, "report_file", "gpbackup")
				Expect(testExecutor.NumExecutions).To(Equal(2))
				Expect(testExecutor.LocalCommands).To(Equal([]string{expectedHomeCmd, expectedGpHomeCmd}))
				Expect(stdout).To(gbytes.Say("Found neither gphome/bin/gp_email_contacts.yaml nor home/gp_email_contacts.yaml"))
			})
			It("sends an email to contacts in $HOME/gp_email_contacts.yaml if only that file is found", func() {
				w.Write(contactsFileContents)
				w.Close()

				testExecutor.ErrorOnExecNum = 2 // Shouldn't hit this case, as it shouldn't be executed a second time
				testExecutor.LocalError = errors.Errorf("exit status 2")

				utils.EmailReport(testCluster, testFPInfo.Timestamp, "report_file", "gpbackup")
				Expect(testExecutor.NumExecutions).To(Equal(2))
				Expect(testExecutor.LocalCommands).To(Equal([]string{expectedHomeCmd, expectedMessage}))
				Expect(logfile).To(gbytes.Say("Sending email report to the following addresses: contact1@example.com"))
			})
			It("sends an email to contacts in $GPHOME/bin/gp_email_contacts.yaml if only that file is found", func() {
				w.Write(contactsFileContents)
				w.Close()

				testExecutor.ErrorOnExecNum = 1
				testExecutor.LocalError = errors.Errorf("exit status 2")

				utils.EmailReport(testCluster, testFPInfo.Timestamp, "report_file", "gpbackup")
				Expect(testExecutor.NumExecutions).To(Equal(3))
				Expect(testExecutor.LocalCommands).To(Equal([]string{expectedHomeCmd, expectedGpHomeCmd, expectedMessage}))
				Expect(logfile).To(gbytes.Say("Sending email report to the following addresses: contact1@example.com"))
			})
			It("sends an email to contacts in $HOME/gp_email_contacts.yaml if a file exists in both $HOME and $GPHOME/bin", func() {
				w.Write(contactsFileContents)
				w.Close()

				utils.EmailReport(testCluster, testFPInfo.Timestamp, "report_file", "gpbackup")
				Expect(testExecutor.NumExecutions).To(Equal(2))
				Expect(testExecutor.LocalCommands).To(Equal([]string{expectedHomeCmd, expectedMessage}))
				Expect(logfile).To(gbytes.Say("Sending email report to the following addresses: contact1@example.com"))
			})
		})
	})
})
