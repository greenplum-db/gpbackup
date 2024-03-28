package end_to_end_test

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"

	"github.com/blang/semver"
	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/history"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("End to End incremental tests", func() {
	BeforeEach(func() {
		end_to_end_setup()
	})
	AfterEach(func() {
		end_to_end_teardown()
	})

	Describe("Incremental backup", func() {
		BeforeEach(func() {
			skipIfOldBackupVersionBefore("1.7.0")
		})
		It("restores from an incremental backup specified with a timestamp", func() {
			output := gpbackup(gpbackupPath, backupHelperPath,
				"--leaf-partition-data")
			fullBackupTimestamp := getBackupTimestamp(string(output))

			testhelper.AssertQueryRuns(backupConn,
				"INSERT into schema2.ao1 values(1001)")
			defer testhelper.AssertQueryRuns(backupConn,
				"DELETE from schema2.ao1 where i=1001")
			output = gpbackup(gpbackupPath, backupHelperPath,
				"--incremental",
				"--leaf-partition-data",
				"--from-timestamp", fullBackupTimestamp)
			incremental1Timestamp := getBackupTimestamp(string(output))

			testhelper.AssertQueryRuns(backupConn,
				"INSERT into schema2.ao1 values(1002)")
			defer testhelper.AssertQueryRuns(backupConn,
				"DELETE from schema2.ao1 where i=1002")
			output = gpbackup(gpbackupPath, backupHelperPath,
				"--incremental",
				"--leaf-partition-data",
				"--from-timestamp", incremental1Timestamp)
			incremental2Timestamp := getBackupTimestamp(string(output))

			gprestore(gprestorePath, restoreHelperPath, incremental2Timestamp,
				"--redirect-db", "restoredb")

			assertRelationsCreated(restoreConn, TOTAL_RELATIONS)
			assertDataRestored(restoreConn, publicSchemaTupleCounts)
			schema2TupleCounts["schema2.ao1"] = 1002
			assertDataRestored(restoreConn, schema2TupleCounts)
		})
		It("restores from an incremental backup with AO Table consisting of multiple segment files", func() {
			// Versions before 1.13.0 incorrectly handle AO table inserts involving multiple seg files
			skipIfOldBackupVersionBefore("1.13.0")

			testhelper.AssertQueryRuns(backupConn,
				"CREATE TABLE foobar WITH (appendonly=true) AS SELECT i FROM generate_series(1,5) i")
			defer testhelper.AssertQueryRuns(backupConn,
				"DROP TABLE foobar")
			testhelper.AssertQueryRuns(backupConn, "VACUUM foobar")
			entriesInTable := dbconn.MustSelectString(backupConn,
				"SELECT count(*) FROM foobar")
			Expect(entriesInTable).To(Equal(strconv.Itoa(5)))

			output := gpbackup(gpbackupPath, backupHelperPath,
				"--leaf-partition-data")
			fullBackupTimestamp := getBackupTimestamp(string(output))

			testhelper.AssertQueryRuns(backupConn,
				"INSERT INTO foobar VALUES (1)")

			// Ensure two distinct aoseg entries contain 'foobar' data
			var numRows string
			if backupConn.Version.Before("6") {
				numRows = dbconn.MustSelectString(backupConn,
					"SELECT count(*) FROM gp_toolkit.__gp_aoseg_name('foobar')")
			} else if backupConn.Version.Before("7") {
				numRows = dbconn.MustSelectString(backupConn,
					"SELECT count(*) FROM gp_toolkit.__gp_aoseg('foobar'::regclass)")
			} else {
				// For GPDB 7+, the gp_toolkit function returns the aoseg entries from the segments
				numRows = dbconn.MustSelectString(backupConn,
					"SELECT count(distinct(segno)) FROM gp_toolkit.__gp_aoseg('foobar'::regclass)")
			}
			Expect(numRows).To(Equal(strconv.Itoa(2)))

			entriesInTable = dbconn.MustSelectString(backupConn,
				"SELECT count(*) FROM foobar")
			Expect(entriesInTable).To(Equal(strconv.Itoa(6)))

			output = gpbackup(gpbackupPath, backupHelperPath,
				"--incremental",
				"--leaf-partition-data",
				"--from-timestamp", fullBackupTimestamp)
			incremental1Timestamp := getBackupTimestamp(string(output))

			gprestore(gprestorePath, restoreHelperPath, incremental1Timestamp,
				"--redirect-db", "restoredb")

			// The insertion should have been recorded in the incremental backup
			entriesInTable = dbconn.MustSelectString(restoreConn,
				"SELECT count(*) FROM foobar")
			Expect(entriesInTable).To(Equal(strconv.Itoa(6)))
		})
		It("can restore from an old backup with an incremental taken from new binaries with --include-table", func() {
			if !useOldBackupVersion {
				Skip("This test is only needed for old backup versions")
			}
			_ = gpbackup(gpbackupPath, backupHelperPath,
				"--leaf-partition-data",
				"--include-table=public.sales")
			testhelper.AssertQueryRuns(backupConn,
				"INSERT into sales values(1, '2017-01-01', 99.99)")
			defer testhelper.AssertQueryRuns(backupConn,
				"DELETE from sales where amt=99.99")
			_ = gpbackup(gpbackupPath, backupHelperPath,
				"--incremental",
				"--leaf-partition-data",
				"--include-table=public.sales")

			gpbackupPathOld, backupHelperPathOld := gpbackupPath, backupHelperPath
			gpbackupPath, backupHelperPath, _ = buildAndInstallBinaries()
			migrateCommand := exec.Command("gpbackup_manager", "migrate-history")
			mustRunCommand(migrateCommand)

			testhelper.AssertQueryRuns(backupConn,
				"INSERT into sales values(2, '2017-02-01', 88.88)")
			defer testhelper.AssertQueryRuns(backupConn,
				"DELETE from sales where amt=88.88")
			output := gpbackup(gpbackupPath, backupHelperPath,
				"--incremental",
				"--leaf-partition-data",
				"--include-table=public.sales")
			gpbackupPath, backupHelperPath = gpbackupPathOld, backupHelperPathOld
			incremental2Timestamp := getBackupTimestamp(string(output))

			gprestore(gprestorePath, restoreHelperPath, incremental2Timestamp,
				"--redirect-db", "restoredb")

			localTupleCounts := map[string]int{
				"public.sales": 15,
			}
			assertRelationsCreated(restoreConn, 13)
			assertDataRestored(restoreConn, localTupleCounts)
		})
		Context("Without a timestamp", func() {
			It("restores from a incremental backup specified with a backup directory", func() {
				_ = gpbackup(gpbackupPath, backupHelperPath,
					"--leaf-partition-data",
					"--backup-dir", backupDir)

				testhelper.AssertQueryRuns(backupConn,
					"INSERT into schema2.ao1 values(1001)")
				defer testhelper.AssertQueryRuns(backupConn,
					"DELETE from schema2.ao1 where i=1001")
				_ = gpbackup(gpbackupPath, backupHelperPath,
					"--incremental",
					"--leaf-partition-data",
					"--backup-dir", backupDir)

				testhelper.AssertQueryRuns(backupConn,
					"INSERT into schema2.ao1 values(1002)")
				defer testhelper.AssertQueryRuns(backupConn,
					"DELETE from schema2.ao1 where i=1002")
				output := gpbackup(gpbackupPath, backupHelperPath,
					"--incremental",
					"--leaf-partition-data",
					"--backup-dir", backupDir)
				incremental2Timestamp := getBackupTimestamp(string(output))

				gprestore(gprestorePath, restoreHelperPath, incremental2Timestamp,
					"--redirect-db", "restoredb",
					"--backup-dir", backupDir)

				assertRelationsCreated(restoreConn, TOTAL_RELATIONS)
				assertDataRestored(restoreConn, publicSchemaTupleCounts)
				schema2TupleCounts["schema2.ao1"] = 1002
				assertDataRestored(restoreConn, schema2TupleCounts)

				_ = os.Remove(backupDir)
			})
			It("restores from --include filtered incremental backup with partition tables", func() {
				_ = gpbackup(gpbackupPath, backupHelperPath,
					"--leaf-partition-data",
					"--include-table", "public.sales")

				testhelper.AssertQueryRuns(backupConn,
					"INSERT into sales VALUES(19, '2017-02-15'::date, 100)")
				defer testhelper.AssertQueryRuns(backupConn,
					"DELETE from sales where id=19")
				_ = gpbackup(gpbackupPath, backupHelperPath,
					"--incremental",
					"--leaf-partition-data",
					"--include-table", "public.sales")

				testhelper.AssertQueryRuns(backupConn,
					"INSERT into sales VALUES(20, '2017-03-15'::date, 100)")
				defer testhelper.AssertQueryRuns(backupConn,
					"DELETE from sales where id=20")
				output := gpbackup(gpbackupPath, backupHelperPath,
					"--incremental",
					"--leaf-partition-data",
					"--include-table", "public.sales")
				incremental2Timestamp := getBackupTimestamp(string(output))

				gprestore(gprestorePath, restoreHelperPath, incremental2Timestamp,
					"--redirect-db", "restoredb")

				assertDataRestored(restoreConn, map[string]int{
					"public.sales":             15,
					"public.sales_1_prt_feb17": 2,
					"public.sales_1_prt_mar17": 2,
				})
			})
			It("restores from --exclude filtered incremental backup with partition tables", func() {
				skipIfOldBackupVersionBefore("1.18.0")
				publicSchemaTupleCountsWithExclude := map[string]int{
					"public.foo":   40000, // holds is excluded and doesn't exist
					"public.sales": 12,    // 13 original - 1 for excluded partition
				}
				schema2TupleCountsWithExclude := map[string]int{
					"schema2.returns": 6,
					"schema2.foo2":    0,
					"schema2.foo3":    100,
					"schema2.ao2":     1001, // +1 for new row, ao1 is excluded and doesn't exist
				}

				_ = gpbackup(gpbackupPath, backupHelperPath,
					"--leaf-partition-data",
					"--exclude-table", "public.holds",
					"--exclude-table", "public.sales_1_prt_mar17",
					"--exclude-table", "schema2.ao1")

				testhelper.AssertQueryRuns(backupConn,
					"INSERT into sales VALUES(20, '2017-03-15'::date, 100)")
				defer testhelper.AssertQueryRuns(backupConn,
					"DELETE from sales where id=20")
				testhelper.AssertQueryRuns(backupConn,
					"INSERT into schema2.ao1 values(1001)")
				defer testhelper.AssertQueryRuns(backupConn,
					"DELETE from schema2.ao1 where i=1001")
				testhelper.AssertQueryRuns(backupConn,
					"INSERT into schema2.ao2 values(1002)")
				defer testhelper.AssertQueryRuns(backupConn,
					"DELETE from schema2.ao2 where i=1002")

				output := gpbackup(gpbackupPath, backupHelperPath,
					"--incremental",
					"--leaf-partition-data",
					"--exclude-table", "public.holds",
					"--exclude-table", "public.sales_1_prt_mar17",
					"--exclude-table", "schema2.ao1")
				incremental1Timestamp := getBackupTimestamp(string(output))

				gprestore(gprestorePath, restoreHelperPath, incremental1Timestamp,
					"--redirect-db", "restoredb")

				if backupConn.Version.Before("7") {
					// -2 for public.holds and schema2.ao1, excluded partition will be included anyway but it's data - will not
					assertRelationsCreated(restoreConn, TOTAL_RELATIONS-2)
				} else {
					// In GPDB 7+, the sales_1_prt_mar17 partition will not be created.
					// TODO: Should the leaf partition actually be created when it has
					// been excluded with the new GPDB partitioning logic?
					assertRelationsCreated(restoreConn, TOTAL_RELATIONS-3)
				}
				assertDataRestored(restoreConn, publicSchemaTupleCountsWithExclude)
				assertDataRestored(restoreConn, schema2TupleCountsWithExclude)
			})
			It("restores from full incremental backup with partition tables with restore table filtering", func() {
				skipIfOldBackupVersionBefore("1.7.2")
				testhelper.AssertQueryRuns(backupConn,
					"INSERT into sales VALUES(19, '2017-02-15'::date, 100)")
				defer testhelper.AssertQueryRuns(backupConn,
					"DELETE from sales where id=19")
				_ = gpbackup(gpbackupPath, backupHelperPath,
					"--leaf-partition-data")

				output := gpbackup(gpbackupPath, backupHelperPath,
					"--incremental", "--leaf-partition-data")
				incremental1Timestamp := getBackupTimestamp(string(output))

				gprestore(gprestorePath, restoreHelperPath, incremental1Timestamp,
					"--redirect-db", "restoredb",
					"--include-table", "public.sales_1_prt_feb17")

				assertDataRestored(restoreConn, map[string]int{
					"public.sales":             2,
					"public.sales_1_prt_feb17": 2,
				})
			})
			Context("old binaries", func() {
				It("can restore from a backup with an incremental taken from new binaries", func() {
					if !useOldBackupVersion {
						Skip("This test is only needed for old backup versions")
					}
					_ = gpbackup(gpbackupPath, backupHelperPath,
						"--leaf-partition-data")

					testhelper.AssertQueryRuns(backupConn,
						"INSERT into schema2.ao1 values(1001)")
					defer testhelper.AssertQueryRuns(backupConn,
						"DELETE from schema2.ao1 where i=1001")
					_ = gpbackup(gpbackupPath, backupHelperPath,
						"--incremental",
						"--leaf-partition-data")

					gpbackupPathOld, backupHelperPathOld := gpbackupPath, backupHelperPath
					gpbackupPath, backupHelperPath, _ = buildAndInstallBinaries()
					migrateCommand := exec.Command("gpbackup_manager", "migrate-history")
					mustRunCommand(migrateCommand)

					testhelper.AssertQueryRuns(backupConn,
						"INSERT into schema2.ao1 values(1002)")
					defer testhelper.AssertQueryRuns(backupConn,
						"DELETE from schema2.ao1 where i=1002")
					output := gpbackup(gpbackupPath, backupHelperPath,
						"--incremental",
						"--leaf-partition-data")
					incremental2Timestamp := getBackupTimestamp(string(output))

					gpbackupPath, backupHelperPath = gpbackupPathOld, backupHelperPathOld
					gprestore(gprestorePath, restoreHelperPath, incremental2Timestamp,
						"--redirect-db", "restoredb")

					assertRelationsCreated(restoreConn, TOTAL_RELATIONS)
					assertDataRestored(restoreConn, publicSchemaTupleCounts)
					schema2TupleCounts["schema2.ao1"] = 1002
					assertDataRestored(restoreConn, schema2TupleCounts)
				})
			})
		})
		Context("With a plugin", func() {
			BeforeEach(func() {
				// FIXME: we are temporarily disabling these tests because we will be altering our backwards compatibility logic.
				if useOldBackupVersion {
					Skip("This test is only needed for the most recent backup versions")
				}
				copyPluginToAllHosts(backupConn, examplePluginExec)
			})
			It("Restores from an incremental backup based on a from-timestamp incremental", func() {
				output := gpbackup(gpbackupPath, backupHelperPath,
					"--leaf-partition-data",
					"--single-data-file",
					"--plugin-config", examplePluginTestConfig)
				fullBackupTimestamp := getBackupTimestamp(string(output))

				forceMetadataFileDownloadFromPlugin(backupConn, fullBackupTimestamp)
				testhelper.AssertQueryRuns(backupConn,
					"INSERT into schema2.ao1 values(1001)")
				defer testhelper.AssertQueryRuns(backupConn,
					"DELETE from schema2.ao1 where i=1001")
				output = gpbackup(gpbackupPath, backupHelperPath,
					"--incremental",
					"--leaf-partition-data",
					"--single-data-file",
					"--from-timestamp", fullBackupTimestamp,
					"--plugin-config", examplePluginTestConfig)
				incremental1Timestamp := getBackupTimestamp(string(output))

				forceMetadataFileDownloadFromPlugin(backupConn, incremental1Timestamp)

				testhelper.AssertQueryRuns(backupConn,
					"INSERT into schema2.ao1 values(1002)")
				defer testhelper.AssertQueryRuns(backupConn,
					"DELETE from schema2.ao1 where i=1002")
				output = gpbackup(gpbackupPath, backupHelperPath,
					"--incremental",
					"--leaf-partition-data",
					"--single-data-file",
					"--plugin-config", examplePluginTestConfig)
				incremental2Timestamp := getBackupTimestamp(string(output))

				forceMetadataFileDownloadFromPlugin(backupConn, incremental2Timestamp)

				gprestore(gprestorePath, restoreHelperPath, incremental2Timestamp,
					"--redirect-db", "restoredb",
					"--plugin-config", examplePluginTestConfig)

				assertRelationsCreated(restoreConn, TOTAL_RELATIONS)
				assertDataRestored(restoreConn, publicSchemaTupleCounts)
				schema2TupleCounts["schema2.ao1"] = 1002
				assertDataRestored(restoreConn, schema2TupleCounts)
				assertArtifactsCleaned(fullBackupTimestamp)
				assertArtifactsCleaned(incremental1Timestamp)
				assertArtifactsCleaned(incremental2Timestamp)
			})
			It("Restores from an incremental backup based on a from-timestamp incremental with --copy-queue-size", func() {
				output := gpbackup(gpbackupPath, backupHelperPath,
					"--leaf-partition-data",
					"--single-data-file",
					"--copy-queue-size", "4",
					"--plugin-config", examplePluginTestConfig)
				fullBackupTimestamp := getBackupTimestamp(string(output))

				forceMetadataFileDownloadFromPlugin(backupConn, fullBackupTimestamp)
				testhelper.AssertQueryRuns(backupConn,
					"INSERT into schema2.ao1 values(1001)")
				defer testhelper.AssertQueryRuns(backupConn,
					"DELETE from schema2.ao1 where i=1001")
				output = gpbackup(gpbackupPath, backupHelperPath,
					"--incremental",
					"--leaf-partition-data",
					"--single-data-file",
					"--copy-queue-size", "4",
					"--from-timestamp", fullBackupTimestamp,
					"--plugin-config", examplePluginTestConfig)
				incremental1Timestamp := getBackupTimestamp(string(output))

				forceMetadataFileDownloadFromPlugin(backupConn, incremental1Timestamp)

				testhelper.AssertQueryRuns(backupConn,
					"INSERT into schema2.ao1 values(1002)")
				defer testhelper.AssertQueryRuns(backupConn,
					"DELETE from schema2.ao1 where i=1002")
				output = gpbackup(gpbackupPath, backupHelperPath,
					"--incremental",
					"--leaf-partition-data",
					"--single-data-file",
					"--copy-queue-size", "4",
					"--plugin-config", examplePluginTestConfig)
				incremental2Timestamp := getBackupTimestamp(string(output))

				forceMetadataFileDownloadFromPlugin(backupConn, incremental2Timestamp)

				gprestore(gprestorePath, restoreHelperPath, incremental2Timestamp,
					"--redirect-db", "restoredb",
					"--copy-queue-size", "4",
					"--plugin-config", examplePluginTestConfig)

				assertRelationsCreated(restoreConn, TOTAL_RELATIONS)
				assertDataRestored(restoreConn, publicSchemaTupleCounts)
				schema2TupleCounts["schema2.ao1"] = 1002
				assertDataRestored(restoreConn, schema2TupleCounts)
				assertArtifactsCleaned(fullBackupTimestamp)
				assertArtifactsCleaned(incremental1Timestamp)
				assertArtifactsCleaned(incremental2Timestamp)
			})
			It("Runs backup and restore if plugin location changed", func() {
				output := gpbackup(gpbackupPath, backupHelperPath,
					"--leaf-partition-data",
					"--plugin-config", examplePluginTestConfig)
				fullBackupTimestamp := getBackupTimestamp(string(output))

				otherPluginExecutablePath := fmt.Sprintf("%s/other_plugin_location/example_plugin.bash", backupDir)
				command := exec.Command("bash", "-c", fmt.Sprintf("mkdir %s/other_plugin_location && cp %s %s/other_plugin_location", backupDir, examplePluginExec, backupDir))
				mustRunCommand(command)
				newCongig := fmt.Sprintf(`EOF1
executablepath: %s/other_plugin_location/example_plugin.bash
options:
 password: unknown
EOF1`, backupDir)
				otherPluginConfig := fmt.Sprintf("%s/other_plugin_location/example_plugin_config.yml", backupDir)
				command = exec.Command("bash", "-c", fmt.Sprintf("cat > %s << %s", otherPluginConfig, newCongig))
				mustRunCommand(command)

				copyPluginToAllHosts(backupConn, otherPluginExecutablePath)

				output = gpbackup(gpbackupPath, backupHelperPath,
					"--leaf-partition-data",
					"--incremental",
					"--plugin-config", otherPluginConfig)
				incrementalBackupTimestamp := getBackupTimestamp(string(output))

				Expect(incrementalBackupTimestamp).NotTo(BeNil())

				gprestore(gprestorePath, restoreHelperPath, incrementalBackupTimestamp,
					"--redirect-db", "restoredb",
					"--plugin-config", otherPluginConfig)

				assertRelationsCreated(restoreConn, TOTAL_RELATIONS)
				assertDataRestored(restoreConn, publicSchemaTupleCounts)
				assertArtifactsCleaned(fullBackupTimestamp)
				assertArtifactsCleaned(incrementalBackupTimestamp)
			})
		})
	})
	Describe("Incremental restore", func() {
		var oldSchemaTupleCounts, newSchemaTupleCounts map[string]int
		BeforeEach(func() {
			skipIfOldBackupVersionBefore("1.16.0")
		})
		Context("Simple incremental restore", func() {
			It("Existing tables should be excluded from metadata restore", func() {
				// Create a heap, ao, co, and external table and create a backup
				testhelper.AssertQueryRuns(backupConn,
					"DROP SCHEMA IF EXISTS testschema CASCADE; CREATE SCHEMA testschema;")
				testhelper.AssertQueryRuns(backupConn,
					"CREATE TABLE testschema.heap_table (a int);")
				testhelper.AssertQueryRuns(backupConn,
					"CREATE TABLE testschema.ao_table (a int) WITH (appendonly=true);")
				testhelper.AssertQueryRuns(backupConn,
					"CREATE TABLE testschema.co_table (a int) WITH (appendonly=true, orientation=column);")
				testhelper.AssertQueryRuns(backupConn,
					"CREATE EXTERNAL WEB TABLE testschema.external_table (a text) EXECUTE E'echo hi' FORMAT 'csv';")
				output := gpbackup(gpbackupPath, backupHelperPath, "--leaf-partition-data")
				backupTimestamp := getBackupTimestamp(string(output))

				// Restore the backup to a different database
				testhelper.AssertQueryRuns(restoreConn,
					"DROP SCHEMA IF EXISTS testschema CASCADE;")
				gprestore(gprestorePath, restoreHelperPath, backupTimestamp, "--redirect-db", "restoredb")

				// Trigger an incremental backup
				testhelper.AssertQueryRuns(backupConn,
					"INSERT INTO testschema.ao_table VALUES (1);")
				output = gpbackup(gpbackupPath, backupHelperPath, "--leaf-partition-data", "--incremental")
				incrementalBackupTimestamp := getBackupTimestamp(string(output))

				// Restore the incremental backup. We should see gprestore
				// not error out due to already existing tables.
				gprestore(gprestorePath, restoreHelperPath, incrementalBackupTimestamp, "--redirect-db", "restoredb", "--incremental", "--data-only")

				// Cleanup
				testhelper.AssertQueryRuns(backupConn,
					"DROP SCHEMA IF EXISTS testschema CASCADE;")
				testhelper.AssertQueryRuns(restoreConn,
					"DROP SCHEMA IF EXISTS testschema CASCADE;")
			})
			It("Does not try to restore postdata", func() {
				testhelper.AssertQueryRuns(backupConn,
					"CREATE TABLE zoo (a int) WITH (appendonly=true);")
				testhelper.AssertQueryRuns(backupConn,
					"CREATE  INDEX fooidx ON zoo USING btree(a);")
				output := gpbackup(gpbackupPath, backupHelperPath, "--leaf-partition-data")
				backupTimestamp := getBackupTimestamp(string(output))

				testhelper.AssertQueryRuns(backupConn,
					"INSERT INTO zoo VALUES (1);")
				output = gpbackup(gpbackupPath, backupHelperPath, "--leaf-partition-data", "--incremental")
				incrementalBackupTimestamp := getBackupTimestamp(string(output))

				gprestore(gprestorePath, restoreHelperPath, backupTimestamp, "--redirect-db", "restoredb")
				gprestore(gprestorePath, restoreHelperPath, incrementalBackupTimestamp, "--redirect-db", "restoredb", "--incremental", "--data-only")

				// Cleanup
				testhelper.AssertQueryRuns(backupConn,
					"DROP TABLE IF EXISTS zoo;")
				testhelper.AssertQueryRuns(restoreConn,
					"DROP TABLE IF EXISTS zoo;")
			})
			It("Does not incremental restore without --data-only", func() {
				args := []string{
					"--timestamp", "23432432",
					"--incremental",
					"--redirect-db", "restoredb"}
				cmd := exec.Command(gprestorePath, args...)
				output, err := cmd.CombinedOutput()
				Expect(err).To(HaveOccurred())
				Expect(string(output)).To(ContainSubstring("Cannot use --incremental without --data-only"))
			})
			It("Does not incremental restore with --metadata-only", func() {
				args := []string{
					"--timestamp", "23432432",
					"--incremental", "--metadata-only",
					"--redirect-db", "restoredb"}
				cmd := exec.Command(gprestorePath, args...)
				output, err := cmd.CombinedOutput()
				Expect(err).To(HaveOccurred())
				Expect(string(output)).To(ContainSubstring(
					"The following flags may not be specified together: truncate-table, metadata-only, incremental"))
			})
		})
		Context("No DDL no partitioning", func() {
			BeforeEach(func() {
				testhelper.AssertQueryRuns(backupConn,
					"DROP SCHEMA IF EXISTS new_schema CASCADE; DROP SCHEMA IF EXISTS old_schema CASCADE; CREATE SCHEMA old_schema;")
				testhelper.AssertQueryRuns(backupConn,
					"CREATE TABLE old_schema.old_table0 (mydata int) WITH (appendonly=true) DISTRIBUTED BY (mydata);")
				testhelper.AssertQueryRuns(backupConn,
					"CREATE TABLE old_schema.old_table1 (mydata int) WITH (appendonly=true) DISTRIBUTED BY (mydata);")
				testhelper.AssertQueryRuns(backupConn,
					"CREATE TABLE old_schema.old_table2 (mydata int) WITH (appendonly=true) DISTRIBUTED BY (mydata);")
				testhelper.AssertQueryRuns(backupConn,
					"INSERT INTO old_schema.old_table0 SELECT generate_series(1, 5);")
				testhelper.AssertQueryRuns(backupConn,
					"INSERT INTO old_schema.old_table1 SELECT generate_series(1, 10);")
				testhelper.AssertQueryRuns(backupConn,
					"INSERT INTO old_schema.old_table2 SELECT generate_series(1, 15);")

				oldSchemaTupleCounts = map[string]int{
					"old_schema.old_table0": 5,
					"old_schema.old_table1": 10,
					"old_schema.old_table2": 15,
				}
				newSchemaTupleCounts = map[string]int{}
				output := gpbackup(gpbackupPath, backupHelperPath,
					"--leaf-partition-data")
				baseTimestamp := getBackupTimestamp(string(output))

				testhelper.AssertQueryRuns(restoreConn,
					"DROP SCHEMA IF EXISTS new_schema CASCADE; DROP SCHEMA IF EXISTS old_schema CASCADE;")
				gprestore(gprestorePath, restoreHelperPath, baseTimestamp,
					"--redirect-db", "restoredb")
			})
			AfterEach(func() {
				testhelper.AssertQueryRuns(backupConn,
					"DROP SCHEMA IF EXISTS new_schema CASCADE; DROP SCHEMA IF EXISTS old_schema CASCADE;")
				testhelper.AssertQueryRuns(restoreConn,
					"DROP SCHEMA IF EXISTS new_schema CASCADE; DROP SCHEMA IF EXISTS old_schema CASCADE;")
			})
			Context("Include/Exclude schemas and tables", func() {
				var timestamp string
				BeforeEach(func() {
					testhelper.AssertQueryRuns(backupConn,
						"CREATE SCHEMA new_schema;")
					testhelper.AssertQueryRuns(backupConn,
						"CREATE TABLE new_schema.new_table1 (mydata int) WITH (appendonly=true) DISTRIBUTED BY (mydata);")
					testhelper.AssertQueryRuns(backupConn,
						"CREATE TABLE new_schema.new_table2 (mydata int) WITH (appendonly=true) DISTRIBUTED BY (mydata);")
					testhelper.AssertQueryRuns(backupConn,
						"INSERT INTO new_schema.new_table1 SELECT generate_series(1, 30);")
					testhelper.AssertQueryRuns(backupConn,
						"INSERT INTO new_schema.new_table2 SELECT generate_series(1, 35);")
					testhelper.AssertQueryRuns(backupConn,
						"INSERT INTO old_schema.old_table1 SELECT generate_series(11, 20);")
					output := gpbackup(gpbackupPath, backupHelperPath,
						"--leaf-partition-data", "--incremental")
					timestamp = getBackupTimestamp(string(output))
				})
				AfterEach(func() {
					testhelper.AssertQueryRuns(backupConn,
						"DROP SCHEMA IF EXISTS new_schema CASCADE;")
					testhelper.AssertQueryRuns(restoreConn,
						"DROP SCHEMA IF EXISTS new_schema CASCADE;")
					testhelper.AssertQueryRuns(backupConn,
						"DELETE FROM old_schema.old_table1 where mydata>10;")
					oldSchemaTupleCounts = map[string]int{}
					newSchemaTupleCounts = map[string]int{}
					assertArtifactsCleaned(timestamp)
				})
				It("Restores only tables included by use if user input is provided", func() {
					gprestore(gprestorePath, restoreHelperPath, timestamp,
						"--incremental", "--data-only",
						"--include-table", "old_schema.old_table1",
						"--redirect-db", "restoredb")
					oldSchemaTupleCounts["old_schema.old_table1"] = 20
					// new_schema should not be present
					assertSchemasExist(restoreConn, 4)
					assertRelationsExistForIncremental(restoreConn, 3)
					assertDataRestored(restoreConn, oldSchemaTupleCounts)
					assertDataRestored(restoreConn, newSchemaTupleCounts)
				})
				It("Does not restore tables excluded by user if user input is provided", func() {
					gprestore(gprestorePath, restoreHelperPath, timestamp,
						"--incremental", "--data-only",
						"--exclude-table", "new_schema.new_table1",
						"--exclude-table", "new_schema.new_table2",
						"--redirect-db", "restoredb")
					oldSchemaTupleCounts["old_schema.old_table1"] = 20
					// new_schema should not be present
					assertSchemasExist(restoreConn, 4)
					assertRelationsExistForIncremental(restoreConn, 3)
					assertDataRestored(restoreConn, oldSchemaTupleCounts)
					assertDataRestored(restoreConn, newSchemaTupleCounts)
				})
				It("Restores only schemas included by user if user input is provided", func() {
					gprestore(gprestorePath, restoreHelperPath, timestamp,
						"--incremental", "--data-only",
						"--include-schema", "old_schema",
						"--redirect-db", "restoredb")
					oldSchemaTupleCounts["old_schema.old_table1"] = 20
					assertSchemasExist(restoreConn, 4)
					assertRelationsExistForIncremental(restoreConn, 3)
					assertDataRestored(restoreConn, oldSchemaTupleCounts)
					assertDataRestored(restoreConn, newSchemaTupleCounts)
				})
				It("Does not restore schemas excluded by user if user input is provided", func() {
					gprestore(gprestorePath, restoreHelperPath, timestamp,
						"--incremental", "--data-only",
						"--exclude-schema", "new_schema",
						"--redirect-db", "restoredb")
					oldSchemaTupleCounts["old_schema.old_table1"] = 20
					assertSchemasExist(restoreConn, 4)
					assertRelationsExistForIncremental(restoreConn, 3)
					assertDataRestored(restoreConn, oldSchemaTupleCounts)
					assertDataRestored(restoreConn, newSchemaTupleCounts)
				})
			})
			Context("New tables and schemas", func() {
				var timestamp string
				BeforeEach(func() {
					testhelper.AssertQueryRuns(backupConn,
						"CREATE SCHEMA new_schema;")
					testhelper.AssertQueryRuns(backupConn,
						"CREATE TABLE new_schema.new_table1 (mydata int) WITH (appendonly=true) DISTRIBUTED BY (mydata);")
					testhelper.AssertQueryRuns(backupConn,
						"INSERT INTO new_schema.new_table1 SELECT generate_series(1, 30);")
					testhelper.AssertQueryRuns(backupConn,
						"CREATE TABLE old_schema.new_table1 (mydata int) WITH (appendonly=true) DISTRIBUTED BY (mydata);")
					testhelper.AssertQueryRuns(backupConn,
						"INSERT INTO old_schema.new_table1 SELECT generate_series(1, 20);")
					testhelper.AssertQueryRuns(backupConn,
						"INSERT INTO new_schema.new_table1 SELECT generate_series(1, 25);")
					testhelper.AssertQueryRuns(backupConn,
						"INSERT INTO old_schema.old_table1 SELECT generate_series(11, 20);")
					output := gpbackup(gpbackupPath, backupHelperPath,
						"--leaf-partition-data", "--incremental")
					timestamp = getBackupTimestamp(string(output))
				})
				AfterEach(func() {
					testhelper.AssertQueryRuns(backupConn,
						"DROP TABLE IF EXISTS old_schema.new_table1 CASCADE;")
					testhelper.AssertQueryRuns(backupConn,
						"DROP TABLE IF EXISTS old_schema.new_table2 CASCADE;")
					testhelper.AssertQueryRuns(restoreConn,
						"DROP TABLE IF EXISTS old_schema.new_table1 CASCADE;")
					testhelper.AssertQueryRuns(restoreConn,
						"DROP TABLE IF EXISTS old_schema.new_table2 CASCADE;")
					testhelper.AssertQueryRuns(backupConn,
						"DELETE FROM old_schema.old_table1 where mydata>10;")
					testhelper.AssertQueryRuns(backupConn,
						"DROP SCHEMA IF EXISTS new_schema CASCADE;")
					testhelper.AssertQueryRuns(restoreConn,
						"DROP SCHEMA IF EXISTS new_schema CASCADE;")
					oldSchemaTupleCounts = map[string]int{}
					newSchemaTupleCounts = map[string]int{}
					assertArtifactsCleaned(timestamp)
				})
				It("Does not restore old/new tables and exits gracefully", func() {
					args := []string{
						"--timestamp", timestamp,
						"--incremental", "--data-only",
						"--redirect-db", "restoredb"}
					cmd := exec.Command(gprestorePath, args...)
					output, err := cmd.CombinedOutput()
					Expect(err).To(HaveOccurred())
					Expect(string(output)).To(ContainSubstring(
						"objects are missing from the target database: " +
							"[new_schema new_schema.new_table1 old_schema.new_table1]"))
					assertRelationsExistForIncremental(restoreConn, 3)
					assertDataRestored(restoreConn, oldSchemaTupleCounts)
					assertDataRestored(restoreConn, newSchemaTupleCounts)
				})
				It("Only restores existing tables if --on-error-continue is specified", func() {
					args := []string{
						"--timestamp", timestamp,
						"--incremental", "--data-only",
						"--on-error-continue",
						"--redirect-db", "restoredb"}
					cmd := exec.Command(gprestorePath, args...)
					_, err := cmd.CombinedOutput()
					Expect(err).To(HaveOccurred())
					oldSchemaTupleCounts["old_schema.old_table1"] = 20
					assertRelationsExistForIncremental(restoreConn, 3)
					assertDataRestored(restoreConn, oldSchemaTupleCounts)
					assertDataRestored(restoreConn, newSchemaTupleCounts)
				})
			})
			Context("Existing tables in existing schemas were updated", func() {
				var timestamp string
				BeforeEach(func() {
					testhelper.AssertQueryRuns(backupConn,
						"INSERT INTO old_schema.old_table1 SELECT generate_series(11, 20);")
					testhelper.AssertQueryRuns(backupConn,
						"INSERT INTO old_schema.old_table2 SELECT generate_series(16, 30);")
					output := gpbackup(gpbackupPath, backupHelperPath,
						"--leaf-partition-data", "--incremental")
					timestamp = getBackupTimestamp(string(output))
				})
				AfterEach(func() {
					testhelper.AssertQueryRuns(backupConn,
						"DELETE FROM old_schema.old_table1 where mydata>10;")
					testhelper.AssertQueryRuns(backupConn,
						"DELETE FROM old_schema.old_table2 where mydata>15;")
					oldSchemaTupleCounts["old_schema.old_table1"] = 10
					oldSchemaTupleCounts["old_schema.old_table2"] = 15
					testhelper.AssertQueryRuns(restoreConn,
						"DELETE FROM old_schema.old_table1 where mydata>10;")
					testhelper.AssertQueryRuns(restoreConn,
						"DELETE FROM old_schema.old_table2 where mydata>15;")
					assertArtifactsCleaned(timestamp)
				})
				It("Updates data in existing tables", func() {
					gprestore(gprestorePath, restoreHelperPath, timestamp,
						"--incremental", "--data-only",
						"--redirect-db", "restoredb")
					oldSchemaTupleCounts["old_schema.old_table1"] = 20
					oldSchemaTupleCounts["old_schema.old_table2"] = 30
					assertSchemasExist(restoreConn, 4)
					assertRelationsExistForIncremental(restoreConn, 3)
					assertDataRestored(restoreConn, oldSchemaTupleCounts)
					assertDataRestored(restoreConn, newSchemaTupleCounts)
				})
				It("Updates only tables included by user if user input is provided", func() {
					gprestore(gprestorePath, restoreHelperPath, timestamp,
						"--incremental", "--data-only",
						"--include-table", "old_schema.old_table1",
						"--redirect-db", "restoredb")
					oldSchemaTupleCounts["old_schema.old_table1"] = 20
					assertSchemasExist(restoreConn, 4)
					assertRelationsExistForIncremental(restoreConn, 3)
					assertDataRestored(restoreConn, oldSchemaTupleCounts)
					assertDataRestored(restoreConn, newSchemaTupleCounts)
				})
				It("Does not update tables excluded by user if user input is provided", func() {
					gprestore(gprestorePath, restoreHelperPath, timestamp,
						"--incremental", "--data-only",
						"--exclude-table", "old_schema.old_table1",
						"--redirect-db", "restoredb")
					oldSchemaTupleCounts["old_schema.old_table2"] = 30
					assertSchemasExist(restoreConn, 4)
					assertRelationsExistForIncremental(restoreConn, 3)
					assertDataRestored(restoreConn, oldSchemaTupleCounts)
					assertDataRestored(restoreConn, newSchemaTupleCounts)
				})
				It("Does not update anything if user excluded all tables", func() {
					gprestore(gprestorePath, restoreHelperPath, timestamp,
						"--incremental", "--data-only",
						"--exclude-table", "old_schema.old_table1",
						"--exclude-table", "old_schema.old_table2",
						"--redirect-db", "restoredb")
					assertSchemasExist(restoreConn, 4)
					assertRelationsExistForIncremental(restoreConn, 3)
					assertDataRestored(restoreConn, oldSchemaTupleCounts)
					assertDataRestored(restoreConn, newSchemaTupleCounts)
				})
				It("Does not update tables if user input is provided and schema is not included", func() {
					gprestore(gprestorePath, restoreHelperPath, timestamp,
						"--incremental", "--data-only",
						"--include-schema", "public",
						"--redirect-db", "restoredb")
					assertSchemasExist(restoreConn, 4)
					assertRelationsExistForIncremental(restoreConn, 3)
					assertDataRestored(restoreConn, oldSchemaTupleCounts)
					assertDataRestored(restoreConn, newSchemaTupleCounts)
				})
				It("Does not restore tables if user input is provide and schema is excluded by user", func() {
					gprestore(gprestorePath, restoreHelperPath, timestamp,
						"--incremental", "--data-only",
						"--exclude-schema", "old_schema",
						"--redirect-db", "restoredb")
					assertSchemasExist(restoreConn, 4)
					assertRelationsExistForIncremental(restoreConn, 3)
					assertDataRestored(restoreConn, oldSchemaTupleCounts)
					assertDataRestored(restoreConn, newSchemaTupleCounts)
				})
			})
		})
	})
	Describe("Incremental restore plans in gpbackup_history", func() {
		BeforeEach(func() {
			skipIfOldBackupVersionBefore("1.7.0")
			testhelper.AssertQueryRuns(backupConn,
				"CREATE SCHEMA test_schema;")
		})
		AfterEach(func() {
			testhelper.AssertQueryRuns(backupConn,
				"DROP SCHEMA IF EXISTS test_schema CASCADE;")

		})
		It("Stores and retrieves expected restore plans for AO table backups in an incremental series", func() {
			// set up initial ddl+dml
			testhelper.AssertQueryRuns(backupConn,
				"CREATE TABLE test_schema.new_table1 (mydata int) WITH (appendonly=true) DISTRIBUTED BY (mydata);")
			testhelper.AssertQueryRuns(backupConn,
				"INSERT INTO test_schema.new_table1 SELECT generate_series(1, 30);")
			testhelper.AssertQueryRuns(backupConn,
				"CREATE TABLE test_schema.new_table2 (mydata int) WITH (appendonly=true) DISTRIBUTED BY (mydata);")
			testhelper.AssertQueryRuns(backupConn,
				"INSERT INTO test_schema.new_table2 SELECT generate_series(1, 30);")

			// take a full backup to start out
			output := gpbackup(gpbackupPath, backupHelperPath, "--leaf-partition-data", "--include-schema", "test_schema")
			timestamp1 := getBackupTimestamp(string(output))
			defer assertArtifactsCleaned(timestamp1)

			// add some data and take an incremental backup dependent on the full backup
			testhelper.AssertQueryRuns(backupConn,
				"INSERT INTO test_schema.new_table2 SELECT generate_series(1, 30);")
			output = gpbackup(gpbackupPath, backupHelperPath, "--leaf-partition-data", "--incremental", "--include-schema", "test_schema")
			timestamp2 := getBackupTimestamp(string(output))
			defer assertArtifactsCleaned(timestamp2)

			// finally, take a second incremental backup with no new data
			output = gpbackup(gpbackupPath, backupHelperPath, "--leaf-partition-data", "--incremental", "--include-schema", "test_schema")
			timestamp3 := getBackupTimestamp(string(output))
			defer assertArtifactsCleaned(timestamp3)

			// run a migration on history file to support mixed-version test suites
			if useOldBackupVersion && oldBackupSemVer.LT(semver.MustParse("1.7.2")) {
				migrateCommand := exec.Command("gpbackup_manager", "migrate-history")
				_, _ = migrateCommand.CombinedOutput()
			}

			// initialize historyDB to allow us to examine the stored restore plans
			historyDB, err := history.InitializeHistoryDatabase(historyFilePath)
			Expect(err).ToNot(HaveOccurred())

			// get restore plans for the full backup, assert expectations
			backupConfig1, err := history.GetBackupConfig(timestamp1, historyDB)
			Expect(err).ToNot(HaveOccurred())
			Expect(backupConfig1.RestorePlan).To(HaveLen(1))
			Expect(backupConfig1.RestorePlan[0].TableFQNs).To(HaveLen(2))
			Expect(backupConfig1.RestorePlan[0].Timestamp).To(Equal(timestamp1))

			// get restore plans for first incremental backup, assert expectations
			backupConfig2, err := history.GetBackupConfig(timestamp2, historyDB)
			Expect(err).ToNot(HaveOccurred())
			Expect(backupConfig2.RestorePlan).To(HaveLen(2))

			for restorePlanIdx := range backupConfig2.RestorePlan {
				switch backupConfig2.RestorePlan[restorePlanIdx].Timestamp {
				case timestamp1:
					Expect(backupConfig2.RestorePlan[restorePlanIdx].TableFQNs).To(HaveLen(1))
					Expect(backupConfig2.RestorePlan[restorePlanIdx].TableFQNs[0]).To(Equal("test_schema.new_table1"))
				case timestamp2:
					Expect(backupConfig2.RestorePlan[restorePlanIdx].TableFQNs).To(HaveLen(1))
					Expect(backupConfig2.RestorePlan[restorePlanIdx].TableFQNs[0]).To(Equal("test_schema.new_table2"))
				default:
					Fail(fmt.Sprintf("Unexpected timestamp %s in restore plan", backupConfig2.RestorePlan[restorePlanIdx].Timestamp))
				}
			}

			// get restore plans for second incremental backup, assert expectations
			backupConfig3, err := history.GetBackupConfig(timestamp3, historyDB)
			Expect(err).ToNot(HaveOccurred())
			Expect(backupConfig3.RestorePlan).To(HaveLen(3))

			for restorePlanIdx := range backupConfig3.RestorePlan {
				switch backupConfig3.RestorePlan[restorePlanIdx].Timestamp {
				case timestamp1:
					Expect(backupConfig3.RestorePlan[restorePlanIdx].TableFQNs).To(HaveLen(1))
					Expect(backupConfig3.RestorePlan[restorePlanIdx].TableFQNs[0]).To(Equal("test_schema.new_table1"))
				case timestamp2:
					Expect(backupConfig3.RestorePlan[restorePlanIdx].TableFQNs).To(HaveLen(1))
					Expect(backupConfig3.RestorePlan[restorePlanIdx].TableFQNs[0]).To(Equal("test_schema.new_table2"))
				case timestamp3:
					Expect(backupConfig3.RestorePlan[restorePlanIdx].TableFQNs).To(HaveLen(0))
				default:
					Fail(fmt.Sprintf("Unexpected timestamp %s in restore plan", backupConfig2.RestorePlan[restorePlanIdx].Timestamp))
				}
			}
		})
		It("Stores and retrieves expected restore plans for heap table backups in an incremental series", func() {
			// set up initial ddl+dml
			testhelper.AssertQueryRuns(backupConn,
				"CREATE TABLE test_schema.new_table1 (mydata int) DISTRIBUTED BY (mydata);")
			testhelper.AssertQueryRuns(backupConn,
				"INSERT INTO test_schema.new_table1 SELECT generate_series(1, 30);")
			testhelper.AssertQueryRuns(backupConn,
				"CREATE TABLE test_schema.new_table2 (mydata int) DISTRIBUTED BY (mydata);")
			testhelper.AssertQueryRuns(backupConn,
				"INSERT INTO test_schema.new_table2 SELECT generate_series(1, 30);")

			// take a full backup to start out
			output := gpbackup(gpbackupPath, backupHelperPath, "--leaf-partition-data", "--include-schema", "test_schema")
			timestamp1 := getBackupTimestamp(string(output))
			defer assertArtifactsCleaned(timestamp1)

			// add some data and take an incremental backup dependent on the full backup
			testhelper.AssertQueryRuns(backupConn,
				"INSERT INTO test_schema.new_table2 SELECT generate_series(1, 30);")
			output = gpbackup(gpbackupPath, backupHelperPath, "--leaf-partition-data", "--incremental", "--include-schema", "test_schema")
			timestamp2 := getBackupTimestamp(string(output))
			defer assertArtifactsCleaned(timestamp2)

			// run a migration on history file to support mixed-version test suites
			if useOldBackupVersion && oldBackupSemVer.LT(semver.MustParse("1.7.2")) {
				migrateCommand := exec.Command("gpbackup_manager", "migrate-history")
				_, _ = migrateCommand.CombinedOutput()
			}

			// initialize historyDB to allow us to examine the stored restore plans
			historyDB, err := history.InitializeHistoryDatabase(historyFilePath)
			Expect(err).ToNot(HaveOccurred())

			// get restore plans for the full backup, assert expectations
			backupConfig1, err := history.GetBackupConfig(timestamp1, historyDB)
			Expect(err).ToNot(HaveOccurred())
			Expect(backupConfig1.RestorePlan).To(HaveLen(1))
			Expect(backupConfig1.RestorePlan[0].TableFQNs).To(HaveLen(2))
			Expect(backupConfig1.RestorePlan[0].Timestamp).To(Equal(timestamp1))

			// there is no re-use of tables from prior restore plans, so all tables will be in the
			// newer restore plan despite no change to new_table1
			backupConfig2, err := history.GetBackupConfig(timestamp2, historyDB)
			Expect(err).ToNot(HaveOccurred())
			Expect(backupConfig2.RestorePlan).To(HaveLen(2))

			for restorePlanIdx := range backupConfig2.RestorePlan {
				switch backupConfig2.RestorePlan[restorePlanIdx].Timestamp {
				case timestamp1:
					Expect(backupConfig2.RestorePlan[restorePlanIdx].TableFQNs).To(HaveLen(0))
				case timestamp2:
					Expect(backupConfig2.RestorePlan[restorePlanIdx].TableFQNs).To(HaveLen(2))
				default:
					Fail(fmt.Sprintf("Unexpected timestamp %s in restore plan", backupConfig2.RestorePlan[restorePlanIdx].Timestamp))
				}
			}
		})
	})
})
