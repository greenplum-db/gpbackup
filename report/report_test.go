package report_test

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/blang/semver"
	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/greenplum-db/gp-common-go-libs/structmatcher"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/filepath"
	"github.com/greenplum-db/gpbackup/history"
	"github.com/greenplum-db/gpbackup/options"
	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/utils"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
	"gopkg.in/yaml.v2"

	"github.com/greenplum-db/gpbackup/report"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gbytes"
)

var (
	stdout  *Buffer
	logfile *Buffer
	buffer  *Buffer
)

func TestReport(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Report Suite")
}

var _ = BeforeEach(func() {
	stdout, buffer, logfile = testhelper.SetupTestLogger()
})

var _ = Describe("utils/report tests", func() {
	Describe("ParseErrorMessage", func() {
		It("Parses a CRITICAL error message and returns error code 1", func() {
			errStr := "testProgram:testUser:testHost:000000-[CRITICAL]:-Error Message"
			errMsg := report.ParseErrorMessage(errStr)
			Expect(errMsg).To(Equal("Error Message"))
		})
		It("Returns error code 0 for an empty error message", func() {
			errMsg := report.ParseErrorMessage("")
			Expect(errMsg).To(Equal(""))
		})
	})
	Describe("WriteBackupReportFile", func() {
		timestamp := "20170101010101"
		endtime := time.Date(2017, 1, 1, 5, 4, 3, 2, time.Local)
		config := history.BackupConfig{
			BackupVersion:   "0.1.0",
			DatabaseName:    "testdb",
			DatabaseVersion: "5.0.0 build test",
			SegmentCount:    3,
		}
		backupReport := &report.Report{}
		objectCounts := map[string]int{"tables": 42, "sequences": 1, "types": 1000}
		BeforeEach(func() {
			backupReport = &report.Report{
				BackupParamsString: `compression: gzip
backup section: All Sections
object filtering: None
includes statistics: No
data file format: Single Data File Per Segment`,
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
			backupReport.WriteBackupReportFile("filename", timestamp, endtime, objectCounts, "")
			Expect(buffer).To(Say(`Greenplum Database Backup Report

timestamp key:         20170101010101
gpdb version:          5\.0\.0 build test
gpbackup version:      0\.1\.0

database name:         testdb
command line:          .*
compression:           gzip
backup section:        All Sections
object filtering:      None
includes statistics:   No
data file format:      Single Data File Per Segment

start time:            Sun Jan 01 2017 01:01:01
end time:              Sun Jan 01 2017 05:04:03
duration:              4:03:02

backup status:         Success

database size:         42 MB
segment count:         3

count of database objects in backup:
sequences   1
tables      42
types       1000`))
		})
		It("writes a report for a failed backup", func() {
			backupReport.WriteBackupReportFile("filename", timestamp, endtime, objectCounts, "Cannot access /tmp/backups: Permission denied")
			Expect(buffer).To(Say(`Greenplum Database Backup Report

timestamp key:         20170101010101
gpdb version:          5\.0\.0 build test
gpbackup version:      0\.1\.0

database name:         testdb
command line:          .*
compression:           gzip
backup section:        All Sections
object filtering:      None
includes statistics:   No
data file format:      Single Data File Per Segment

start time:            Sun Jan 01 2017 01:01:01
end time:              Sun Jan 01 2017 05:04:03
duration:              4:03:02

backup status:         Failure
backup error:          Cannot access /tmp/backups: Permission denied

database size:         42 MB
segment count:         3

count of database objects in backup:
sequences   1
tables      42
types       1000`))
		})
		It("writes a report without database size information", func() {
			backupReport.DatabaseSize = ""
			backupReport.WriteBackupReportFile("filename", timestamp, endtime, objectCounts, "")
			Expect(buffer).To(Say(`Greenplum Database Backup Report

timestamp key:         20170101010101
gpdb version:          5\.0\.0 build test
gpbackup version:      0\.1\.0

database name:         testdb
command line:          .*
compression:           gzip
backup section:        All Sections
object filtering:      None
includes statistics:   No
data file format:      Single Data File Per Segment

start time:            Sun Jan 01 2017 01:01:01
end time:              Sun Jan 01 2017 05:04:03
duration:              4:03:02

backup status:         Success

segment count:         3

count of database objects in backup:
sequences   1
tables      42
types       1000`))
		})
	})
	Describe("AppendBackupParams", func() {
		It("correctly parses the string and appends to the LineInfo array", func() {
			testParamsStr := `compression: exampleStr
data file format: exampleStr
`
			lineInfoTest := make([]report.LineInfo, 0)

			report.AppendBackupParams(&lineInfoTest, testParamsStr)
			Expect(lineInfoTest[0].Key).To(Equal("compression:"))
			Expect(lineInfoTest[0].Value).To(Equal("exampleStr"))
			Expect(lineInfoTest[1].Key).To(Equal("data file format:"))
			Expect(lineInfoTest[1].Value).To(Equal("exampleStr"))
		})
		It("correctly parses a string with incremental info", func() {
			backupTimestamps := []string{"1234", "5678"}
			testParamsStr := fmt.Sprintf(`incremental: True
incremental backup set:
%s`, strings.Join(backupTimestamps, "\n"))
			lineInfoTest := make([]report.LineInfo, 0)

			report.AppendBackupParams(&lineInfoTest, testParamsStr)
			Expect(lineInfoTest[0].Key).To(Equal("incremental:"))
			Expect(lineInfoTest[0].Value).To(Equal("True"))
			Expect(lineInfoTest[1].Key).To(Equal("incremental backup set:"))
			Expect(lineInfoTest[1].Value).To(Equal(""))
			Expect(lineInfoTest[2].Key).To(Equal("1234"))
			Expect(lineInfoTest[2].Value).To(Equal(""))
			Expect(lineInfoTest[3].Key).To(Equal("5678"))
			Expect(lineInfoTest[3].Value).To(Equal(""))

		})
	})
	Describe("WriteRestoreReportFile", func() {
		timestamp := "20170101010101"
		restoreStartTime := "20170101010102"
		restoreVersion := "0.1.0"
		connectionPool := &dbconn.DBConn{
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
			report.WriteRestoreReportFile("filename", timestamp, restoreStartTime, connectionPool, restoreVersion, 3, 4, "Cannot access /tmp/backups: Permission denied")
			Expect(buffer).To(Say(`Greenplum Database Restore Report

timestamp key:           20170101010101
gpdb version:            5\.0\.0 build test
gprestore version:       0\.1\.0

database name:           testdb
command line:            .*

backup segment count:    3
restore segment count:   4
start time:              Sun Jan 01 2017 01:01:02
end time:                Sun Jan 01 2017 05:04:03
duration:                4:03:01

restore status:          Failure
restore error:           Cannot access /tmp/backups: Permission denied`))
		})
		It("writes a report for a successful restore", func() {
			gplog.SetErrorCode(0)
			report.WriteRestoreReportFile("filename", timestamp, restoreStartTime, connectionPool, restoreVersion, 3, 3, "")
			Expect(buffer).To(Say(`Greenplum Database Restore Report

timestamp key:           20170101010101
gpdb version:            5\.0\.0 build test
gprestore version:       0\.1\.0

database name:           testdb
command line:            .*

backup segment count:    3
restore segment count:   3
start time:              Sun Jan 01 2017 01:01:02
end time:                Sun Jan 01 2017 05:04:03
duration:                4:03:01

restore status:          Success`))
		})
		It("writes a report for a successful restore with errors", func() {
			gplog.SetErrorCode(1)
			report.WriteRestoreReportFile("filename", timestamp, restoreStartTime, connectionPool, restoreVersion, 3, 3, "")
			Expect(buffer).To(Say(`Greenplum Database Restore Report

timestamp key:           20170101010101
gpdb version:            5\.0\.0 build test
gprestore version:       0\.1\.0

database name:           testdb
command line:            .*

backup segment count:    3
restore segment count:   3
start time:              Sun Jan 01 2017 01:01:02
end time:                Sun Jan 01 2017 05:04:03
duration:                4:03:01

restore status:          Success but non-fatal errors occurred. See log file .+ for details.`))
		})
	})
	Describe("SetBackupParamFromFlags", func() {
		AfterEach(func() {
			utils.InitializePipeThroughParameters(false, "", 0)
		})
		It("configures the Report struct correctly", func() {
			utils.InitializePipeThroughParameters(true, "gzip", 0)
			backupCmdFlags := pflag.NewFlagSet("gpbackup", pflag.ExitOnError)
			backup.SetCmdFlags(backupCmdFlags)
			err := backupCmdFlags.Set(options.INCLUDE_RELATION, "public.foobar")
			Expect(err).ToNot(HaveOccurred())
			opts, err := options.NewOptions(backupCmdFlags)
			Expect(err).ToNot(HaveOccurred())
			opts.AddIncludedRelation("public.baz")
			err = backupCmdFlags.Set(options.INCLUDE_RELATION, "public.baz")
			Expect(err).ToNot(HaveOccurred())

			backupConfig := backup.NewBackupConfig("testdb",
				"5.0.0 build test", "0.1.0",
				"/tmp/plugin.sh", "timestamp1", *opts)
			structmatcher.ExpectStructsToMatch(history.BackupConfig{
				BackupVersion:        "0.1.0",
				Compressed:           true,
				CompressionType:      "gzip",
				DatabaseName:         "testdb",
				DatabaseVersion:      "5.0.0 build test",
				IncludeSchemas:       []string{},
				IncludeRelations:     []string{"public.foobar"},
				ExcludeSchemas:       []string{},
				ExcludeRelations:     []string{},
				Plugin:               "/tmp/plugin.sh",
				Timestamp:            "timestamp1",
				IncludeTableFiltered: true,
				Status:               history.BackupStatusInProgress,
			}, backupConfig)
		})
	})
	Describe("GetDurationInfo", func() {
		timestamp := "20170101010101"
		AfterEach(func() {
			operating.System.Local = time.Local
		})
		It("prints times and duration for a sub-minute backup", func() {
			endTime := time.Date(2017, 1, 1, 1, 1, 3, 2, operating.System.Local)
			start, end, duration := report.GetDurationInfo(timestamp, endTime)
			Expect(start).To(Equal("Sun Jan 01 2017 01:01:01"))
			Expect(end).To(Equal("Sun Jan 01 2017 01:01:03"))
			Expect(duration).To(Equal("0:00:02"))
		})
		It("prints times and duration for a sub-hour backup", func() {
			endTime := time.Date(2017, 1, 1, 1, 4, 3, 2, operating.System.Local)
			start, end, duration := report.GetDurationInfo(timestamp, endTime)
			Expect(start).To(Equal("Sun Jan 01 2017 01:01:01"))
			Expect(end).To(Equal("Sun Jan 01 2017 01:04:03"))
			Expect(duration).To(Equal("0:03:02"))
		})
		It("prints times and duration for a multiple-hour backup", func() {
			endTime := time.Date(2017, 1, 1, 5, 4, 3, 2, operating.System.Local)
			start, end, duration := report.GetDurationInfo(timestamp, endTime)
			Expect(start).To(Equal("Sun Jan 01 2017 01:01:01"))
			Expect(end).To(Equal("Sun Jan 01 2017 05:04:03"))
			Expect(duration).To(Equal("4:03:02"))
		})
		It("prints times and duration for a backup going past midnight", func() {
			endTime := time.Date(2017, 1, 2, 1, 4, 3, 2, operating.System.Local)
			start, end, duration := report.GetDurationInfo(timestamp, endTime)
			Expect(start).To(Equal("Sun Jan 01 2017 01:01:01"))
			Expect(end).To(Equal("Mon Jan 02 2017 01:04:03"))
			Expect(duration).To(Equal("24:03:02"))
		})
		It("prints times and duration for a backup during the spring time change", func() {
			operating.System.Local, _ = time.LoadLocation("America/Los_Angeles") // Ensure test works regardless of time zone of test machine
			dst := "20170312010000"
			endTime := time.Date(2017, 3, 12, 3, 0, 0, 0, operating.System.Local)
			start, end, duration := report.GetDurationInfo(dst, endTime)
			Expect(start).To(Equal("Sun Mar 12 2017 01:00:00"))
			Expect(end).To(Equal("Sun Mar 12 2017 03:00:00"))
			Expect(duration).To(Equal("1:00:00"))
		})
		It("prints times and duration for a backup during the fall time change", func() {
			operating.System.Local, _ = time.LoadLocation("America/Los_Angeles") // Ensure test works regardless of time zone of test machine
			dst := "20171105010000"
			endTime := time.Date(2017, 11, 5, 3, 0, 0, 0, operating.System.Local)
			start, end, duration := report.GetDurationInfo(dst, endTime)
			Expect(start).To(Equal("Sun Nov 05 2017 01:00:00"))
			Expect(end).To(Equal("Sun Nov 05 2017 03:00:00"))
			Expect(duration).To(Equal("3:00:00"))
		})
	})
	Describe("EnsureBackupVersionCompatibility", func() {
		It("Panics if gpbackup version is greater than gprestore version", func() {
			defer testhelper.ShouldPanicWithMessage("gprestore 0.1.0 cannot restore a backup taken with gpbackup 0.2.0; please use gprestore 0.2.0 or later.")
			report.EnsureBackupVersionCompatibility("0.2.0", "0.1.0")
		})
		It("Does not panic if gpbackup version is less than gprestore version", func() {
			report.EnsureBackupVersionCompatibility("0.1.0", "0.1.3")
		})
		It("Does not panic if gpbackup version equals gprestore version", func() {
			report.EnsureBackupVersionCompatibility("0.1.0", "0.1.0")
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
			report.EnsureDatabaseVersionCompatibility("6.0.0-beta.9+dev.129.g4bd4e41 build dev", restoreVersion)
		})
		It("Does not panic if backup database major version is greater than restore major version", func() {
			report.EnsureDatabaseVersionCompatibility("4.3.16-beta.9+dev.129.g4bd4e41 build dev", restoreVersion)
		})
		It("Does not panic if backup database major version is equal to restore major version", func() {
			report.EnsureDatabaseVersionCompatibility("5.0.6-beta.9+dev.129.g4bd4e41 build dev", restoreVersion)
		})
	})

	Describe("Email-related functions", func() {
		reportFileContents := []byte(`Greenplum Database Backup Report

Timestamp Key: 20170101010101`)
		contactsFileContents, _ := yaml.Marshal(report.ContactFile{
			Contacts: map[string][]report.EmailContact{
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
			testCluster  *cluster.Cluster
			testFPInfo   filepath.FilePathInfo
			w            *os.File
			r            *os.File
		)
		BeforeEach(func() {
			r, w, _ = os.Pipe()
			testCluster = testutils.SetDefaultSegmentConfiguration()
			testFPInfo = filepath.NewFilePathInfo(testCluster, "", "20170101010101", "gpseg")
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
				_, _ = w.Write(contactsFileContents)
				_ = w.Close()

				contacts := report.GetContacts(contactsFilename, "gpbackup")
				Expect(contacts).To(Equal("contact1@example.com"))
			})
			It("Gets a list of gpbackup contacts on success with errors", func() {
				gplog.SetErrorCode(1)
				_, _ = w.Write(contactsFileContents)
				_ = w.Close()

				contacts := report.GetContacts(contactsFilename, "gpbackup")
				Expect(contacts).To(Equal("contact1@example.com contact2@example.org"))
			})
			It("Gets a list of gpbackup contacts on failure", func() {
				gplog.SetErrorCode(2)
				_, _ = w.Write(contactsFileContents)
				_ = w.Close()

				contacts := report.GetContacts(contactsFilename, "gpbackup")
				Expect(contacts).To(Equal("contact2@example.org"))
			})
			It("Gets a list of gprestore contacts and doesn't fail when no status specified", func() {
				gplog.SetErrorCode(0)
				_, _ = w.Write(contactsFileContents)
				_ = w.Close()

				contacts := report.GetContacts(contactsFilename, "gprestore")
				Expect(contacts).To(Equal("contact4@example.org"))
			})
		})
		Context("ConstructEmailMessage for successful backup", func() {
			It("adds HTML formatting to the contents of the report file", func() {
				_, _ = w.Write(reportFileContents)
				_ = w.Close()

				message := report.ConstructEmailMessage(testFPInfo.Timestamp, contactsList, "report_file", "gpbackup", true)
				expectedMessage := `To: contact1@example.com contact2@example.org
Subject: gpbackup 20170101010101 on localhost completed: Success
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
		Context("ConstructEmailMessage for Failed backup", func() {
			It("adds HTML formatting to the contents of the report file", func() {
				_, _ = w.Write(reportFileContents)
				_ = w.Close()

				message := report.ConstructEmailMessage(testFPInfo.Timestamp, contactsList, "report_file", "gpbackup", false)
				expectedMessage := `To: contact1@example.com contact2@example.org
Subject: gpbackup 20170101010101 on localhost completed: Failure
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
Subject: gpbackup 20170101010101 on localhost completed: Success
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
				_, _ = w.Write(contactsFileContents)
				_ = w.Close()

				testExecutor.LocalError = errors.Errorf("exit status 2")

				report.EmailReport(testCluster, testFPInfo.Timestamp, "report_file", "gpbackup", true)
				Expect(testExecutor.NumExecutions).To(Equal(2))
				Expect(testExecutor.LocalCommands).To(Equal([]string{expectedHomeCmd, expectedGpHomeCmd}))
				Expect(stdout).To(Say("Found neither gphome/bin/gp_email_contacts.yaml nor home/gp_email_contacts.yaml"))
			})
			It("sends an email to contacts in $HOME/gp_email_contacts.yaml if only that file is found", func() {
				_, _ = w.Write(contactsFileContents)
				_ = w.Close()

				testExecutor.ErrorOnExecNum = 2 // Shouldn't hit this case, as it shouldn't be executed a second time
				testExecutor.LocalError = errors.Errorf("exit status 2")

				report.EmailReport(testCluster, testFPInfo.Timestamp, "report_file", "gpbackup", true)
				Expect(testExecutor.NumExecutions).To(Equal(2))
				Expect(testExecutor.LocalCommands).To(Equal([]string{expectedHomeCmd, expectedMessage}))
				Expect(logfile).To(Say("Sending email report to the following addresses: contact1@example.com"))
			})
			It("sends an email to contacts in $GPHOME/bin/gp_email_contacts.yaml if only that file is found", func() {
				_, _ = w.Write(contactsFileContents)
				_ = w.Close()

				testExecutor.ErrorOnExecNum = 1
				testExecutor.LocalError = errors.Errorf("exit status 2")

				report.EmailReport(testCluster, testFPInfo.Timestamp, "report_file", "gpbackup", true)
				Expect(testExecutor.NumExecutions).To(Equal(3))
				Expect(testExecutor.LocalCommands).To(Equal([]string{expectedHomeCmd, expectedGpHomeCmd, expectedMessage}))
				Expect(logfile).To(Say("Sending email report to the following addresses: contact1@example.com"))
			})
			It("sends an email to contacts in $HOME/gp_email_contacts.yaml if a file exists in both $HOME and $GPHOME/bin", func() {
				_, _ = w.Write(contactsFileContents)
				_ = w.Close()

				report.EmailReport(testCluster, testFPInfo.Timestamp, "report_file", "gpbackup", true)
				Expect(testExecutor.NumExecutions).To(Equal(2))
				Expect(testExecutor.LocalCommands).To(Equal([]string{expectedHomeCmd, expectedMessage}))
				Expect(logfile).To(Say("Sending email report to the following addresses: contact1@example.com"))
			})
		})
	})
})
