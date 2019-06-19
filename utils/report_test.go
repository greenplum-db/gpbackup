package utils_test

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"time"

	"github.com/greenplum-db/gpbackup/options"

	"github.com/blang/semver"
	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/greenplum-db/gp-common-go-libs/structmatcher"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/backup_filepath"
	"github.com/greenplum-db/gpbackup/backup_history"
	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/utils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
	"gopkg.in/yaml.v2"
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
		config := backup_history.BackupConfig{
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

Start Time: Sun Jan 01 2017 01:01:01
End Time: Sun Jan 01 2017 05:04:03
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

Start Time: Sun Jan 01 2017 01:01:01
End Time: Sun Jan 01 2017 05:04:03
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

Start Time: Sun Jan 01 2017 01:01:01
End Time: Sun Jan 01 2017 05:04:03
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
			utils.WriteRestoreReportFile("filename", timestamp, restoreStartTime, connectionPool, restoreVersion, "Cannot access /tmp/backups: Permission denied")
			Expect(buffer).To(gbytes.Say(`Greenplum Database Restore Report

Timestamp Key: 20170101010101
GPDB Version: 5\.0\.0 build test
gprestore Version: 0\.1\.0

Database Name: testdb
Command Line: .*

Start Time: Sun Jan 01 2017 01:01:02
End Time: Sun Jan 01 2017 05:04:03
Duration: 4:03:01

Restore Status: Failure
Restore Error: Cannot access /tmp/backups: Permission denied`))
		})
		It("writes a report for a successful restore", func() {
			gplog.SetErrorCode(0)
			utils.WriteRestoreReportFile("filename", timestamp, restoreStartTime, connectionPool, restoreVersion, "")
			Expect(buffer).To(gbytes.Say(`Greenplum Database Restore Report

Timestamp Key: 20170101010101
GPDB Version: 5\.0\.0 build test
gprestore Version: 0\.1\.0

Database Name: testdb
Command Line: .*

Start Time: Sun Jan 01 2017 01:01:02
End Time: Sun Jan 01 2017 05:04:03
Duration: 4:03:01

Restore Status: Success`))
		})
		It("writes a report for a successful restore with errors", func() {
			gplog.SetErrorCode(1)
			utils.WriteRestoreReportFile("filename", timestamp, restoreStartTime, connectionPool, restoreVersion, "")
			Expect(buffer).To(gbytes.Say(`Greenplum Database Restore Report

Timestamp Key: 20170101010101
GPDB Version: 5\.0\.0 build test
gprestore Version: 0\.1\.0

Database Name: testdb
Command Line: .*

Start Time: Sun Jan 01 2017 01:01:02
End Time: Sun Jan 01 2017 05:04:03
Duration: 4:03:01

Restore Status: Success but non-fatal errors occurred. See log file .+ for details.`))
		})
	})
	Describe("SetBackupParamFromFlags", func() {
		AfterEach(func() {
			utils.InitializePipeThroughParameters(false, 0)
		})
		It("configures the Report struct correctly", func() {
			utils.InitializePipeThroughParameters(true, 0)
			backupCmdFlags := pflag.NewFlagSet("gpbackup", pflag.ExitOnError)
			backup.SetFlagDefaults(backupCmdFlags)
			backup.SetCmdFlags(backupCmdFlags)
			err := backupCmdFlags.Set(utils.INCLUDE_RELATION, "public.foobar")
			Expect(err).ToNot(HaveOccurred())
			opts, err := options.NewOptions(backupCmdFlags)
			Expect(err).ToNot(HaveOccurred())
			opts.AddIncludedRelation("public.baz")
			err = backupCmdFlags.Set(utils.INCLUDE_RELATION, "public.baz")
			Expect(err).ToNot(HaveOccurred())

			backupConfig := backup.NewBackupConfig("testdb",
				"5.0.0 build test", "0.1.0",
				"/tmp/plugin.sh", "timestamp1", *opts)
			structmatcher.ExpectStructsToMatch(backup_history.BackupConfig{
				BackupVersion:        "0.1.0",
				Compressed:           true,
				DatabaseName:         "testdb",
				DatabaseVersion:      "5.0.0 build test",
				IncludeSchemas:       []string{},
				IncludeRelations:     []string{"public.foobar"},
				ExcludeSchemas:       []string{},
				ExcludeRelations:     []string{},
				Plugin:               "/tmp/plugin.sh",
				Timestamp:            "timestamp1",
				IncludeTableFiltered: true,
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
			start, end, duration := utils.GetDurationInfo(timestamp, endTime)
			Expect(start).To(Equal("Sun Jan 01 2017 01:01:01"))
			Expect(end).To(Equal("Sun Jan 01 2017 01:01:03"))
			Expect(duration).To(Equal("0:00:02"))
		})
		It("prints times and duration for a sub-hour backup", func() {
			endTime := time.Date(2017, 1, 1, 1, 4, 3, 2, operating.System.Local)
			start, end, duration := utils.GetDurationInfo(timestamp, endTime)
			Expect(start).To(Equal("Sun Jan 01 2017 01:01:01"))
			Expect(end).To(Equal("Sun Jan 01 2017 01:04:03"))
			Expect(duration).To(Equal("0:03:02"))
		})
		It("prints times and duration for a multiple-hour backup", func() {
			endTime := time.Date(2017, 1, 1, 5, 4, 3, 2, operating.System.Local)
			start, end, duration := utils.GetDurationInfo(timestamp, endTime)
			Expect(start).To(Equal("Sun Jan 01 2017 01:01:01"))
			Expect(end).To(Equal("Sun Jan 01 2017 05:04:03"))
			Expect(duration).To(Equal("4:03:02"))
		})
		It("prints times and duration for a backup going past midnight", func() {
			endTime := time.Date(2017, 1, 2, 1, 4, 3, 2, operating.System.Local)
			start, end, duration := utils.GetDurationInfo(timestamp, endTime)
			Expect(start).To(Equal("Sun Jan 01 2017 01:01:01"))
			Expect(end).To(Equal("Mon Jan 02 2017 01:04:03"))
			Expect(duration).To(Equal("24:03:02"))
		})
		It("prints times and duration for a backup during the spring time change", func() {
			operating.System.Local, _ = time.LoadLocation("America/Los_Angeles") // Ensure test works regardless of time zone of test machine
			dst := "20170312010000"
			endTime := time.Date(2017, 3, 12, 3, 0, 0, 0, operating.System.Local)
			start, end, duration := utils.GetDurationInfo(dst, endTime)
			Expect(start).To(Equal("Sun Mar 12 2017 01:00:00"))
			Expect(end).To(Equal("Sun Mar 12 2017 03:00:00"))
			Expect(duration).To(Equal("1:00:00"))
		})
		It("prints times and duration for a backup during the fall time change", func() {
			operating.System.Local, _ = time.LoadLocation("America/Los_Angeles") // Ensure test works regardless of time zone of test machine
			dst := "20171105010000"
			endTime := time.Date(2017, 11, 5, 3, 0, 0, 0, operating.System.Local)
			start, end, duration := utils.GetDurationInfo(dst, endTime)
			Expect(start).To(Equal("Sun Nov 05 2017 01:00:00"))
			Expect(end).To(Equal("Sun Nov 05 2017 03:00:00"))
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
			testCluster  *cluster.Cluster
			testFPInfo   backup_filepath.FilePathInfo
			w            *os.File
			r            *os.File
		)
		BeforeEach(func() {
			r, w, _ = os.Pipe()
			testCluster = testutils.SetDefaultSegmentConfiguration()
			testFPInfo = backup_filepath.NewFilePathInfo(testCluster, "", "20170101010101", "gpseg")
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
