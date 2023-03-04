package end_to_end_test

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"

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
			fullBackupTimestamp := gpbackup(gpbackupPath, backupHelperPath,
				"--leaf-partition-data")

			testhelper.AssertQueryRuns(backupConn,
				"INSERT into schema2.ao1 values(1001)")
			defer testhelper.AssertQueryRuns(backupConn,
				"DELETE from schema2.ao1 where i=1001")
			incremental1Timestamp := gpbackup(gpbackupPath, backupHelperPath,
				"--incremental",
				"--leaf-partition-data",
				"--from-timestamp", fullBackupTimestamp)

			testhelper.AssertQueryRuns(backupConn,
				"INSERT into schema2.ao1 values(1002)")
			defer testhelper.AssertQueryRuns(backupConn,
				"DELETE from schema2.ao1 where i=1002")
			incremental2Timestamp := gpbackup(gpbackupPath, backupHelperPath,
				"--incremental",
				"--leaf-partition-data",
				"--from-timestamp", incremental1Timestamp)

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

			fullBackupTimestamp := gpbackup(gpbackupPath, backupHelperPath,
				"--leaf-partition-data")

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

			incremental1Timestamp := gpbackup(gpbackupPath, backupHelperPath,
				"--incremental",
				"--leaf-partition-data",
				"--from-timestamp", fullBackupTimestamp)

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

			testhelper.AssertQueryRuns(backupConn,
				"INSERT into sales values(2, '2017-02-01', 88.88)")
			defer testhelper.AssertQueryRuns(backupConn,
				"DELETE from sales where amt=88.88")
			incremental2Timestamp := gpbackup(gpbackupPath, backupHelperPath,
				"--incremental",
				"--leaf-partition-data",
				"--include-table=public.sales")
			gpbackupPath, backupHelperPath = gpbackupPathOld, backupHelperPathOld

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
				incremental2Timestamp := gpbackup(gpbackupPath, backupHelperPath,
					"--incremental",
					"--leaf-partition-data",
					"--backup-dir", backupDir)

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
				incremental2Timestamp := gpbackup(gpbackupPath, backupHelperPath,
					"--incremental",
					"--leaf-partition-data",
					"--include-table", "public.sales")
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

				incremental1Timestamp := gpbackup(gpbackupPath, backupHelperPath,
					"--incremental",
					"--leaf-partition-data",
					"--exclude-table", "public.holds",
					"--exclude-table", "public.sales_1_prt_mar17",
					"--exclude-table", "schema2.ao1")

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

				incremental1Timestamp := gpbackup(gpbackupPath, backupHelperPath,
					"--incremental", "--leaf-partition-data")
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
					// TODO -- ensure that this is installed and we have a path to it?  Only called
					// in CI currently
					migrateCommand := exec.Command("gpbackup_manager", "migrate-history")
					mustRunCommand(migrateCommand)

					testhelper.AssertQueryRuns(backupConn,
						"INSERT into schema2.ao1 values(1002)")
					defer testhelper.AssertQueryRuns(backupConn,
						"DELETE from schema2.ao1 where i=1002")
					incremental2Timestamp := gpbackup(gpbackupPath, backupHelperPath,
						"--incremental",
						"--leaf-partition-data")
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
				pluginExecutablePath := fmt.Sprintf("%s/src/github.com/greenplum-db/gpbackup/plugins/example_plugin.bash", os.Getenv("GOPATH"))
				copyPluginToAllHosts(backupConn, pluginExecutablePath)
			})
			It("Restores from an incremental backup based on a from-timestamp incremental", func() {
				fullBackupTimestamp := gpbackup(gpbackupPath, backupHelperPath,
					"--leaf-partition-data",
					"--single-data-file",
					"--plugin-config", pluginConfigPath)
				forceMetadataFileDownloadFromPlugin(backupConn, fullBackupTimestamp)
				testhelper.AssertQueryRuns(backupConn,
					"INSERT into schema2.ao1 values(1001)")
				defer testhelper.AssertQueryRuns(backupConn,
					"DELETE from schema2.ao1 where i=1001")
				incremental1Timestamp := gpbackup(gpbackupPath, backupHelperPath,
					"--incremental",
					"--leaf-partition-data",
					"--single-data-file",
					"--from-timestamp", fullBackupTimestamp,
					"--plugin-config", pluginConfigPath)
				forceMetadataFileDownloadFromPlugin(backupConn, incremental1Timestamp)

				testhelper.AssertQueryRuns(backupConn,
					"INSERT into schema2.ao1 values(1002)")
				defer testhelper.AssertQueryRuns(backupConn,
					"DELETE from schema2.ao1 where i=1002")
				incremental2Timestamp := gpbackup(gpbackupPath, backupHelperPath,
					"--incremental",
					"--leaf-partition-data",
					"--single-data-file",
					"--plugin-config", pluginConfigPath)
				forceMetadataFileDownloadFromPlugin(backupConn, incremental2Timestamp)

				gprestore(gprestorePath, restoreHelperPath, incremental2Timestamp,
					"--redirect-db", "restoredb",
					"--plugin-config", pluginConfigPath)

				assertRelationsCreated(restoreConn, TOTAL_RELATIONS)
				assertDataRestored(restoreConn, publicSchemaTupleCounts)
				schema2TupleCounts["schema2.ao1"] = 1002
				assertDataRestored(restoreConn, schema2TupleCounts)
				assertArtifactsCleaned(restoreConn, fullBackupTimestamp)
				assertArtifactsCleaned(restoreConn, incremental1Timestamp)
				assertArtifactsCleaned(restoreConn, incremental2Timestamp)
			})
			It("Restores from an incremental backup based on a from-timestamp incremental with --copy-queue-size", func() {
				fullBackupTimestamp := gpbackup(gpbackupPath, backupHelperPath,
					"--leaf-partition-data",
					"--single-data-file",
					"--copy-queue-size", "4",
					"--plugin-config", pluginConfigPath)
				forceMetadataFileDownloadFromPlugin(backupConn, fullBackupTimestamp)
				testhelper.AssertQueryRuns(backupConn,
					"INSERT into schema2.ao1 values(1001)")
				defer testhelper.AssertQueryRuns(backupConn,
					"DELETE from schema2.ao1 where i=1001")
				incremental1Timestamp := gpbackup(gpbackupPath, backupHelperPath,
					"--incremental",
					"--leaf-partition-data",
					"--single-data-file",
					"--copy-queue-size", "4",
					"--from-timestamp", fullBackupTimestamp,
					"--plugin-config", pluginConfigPath)
				forceMetadataFileDownloadFromPlugin(backupConn, incremental1Timestamp)

				testhelper.AssertQueryRuns(backupConn,
					"INSERT into schema2.ao1 values(1002)")
				defer testhelper.AssertQueryRuns(backupConn,
					"DELETE from schema2.ao1 where i=1002")
				incremental2Timestamp := gpbackup(gpbackupPath, backupHelperPath,
					"--incremental",
					"--leaf-partition-data",
					"--single-data-file",
					"--copy-queue-size", "4",
					"--plugin-config", pluginConfigPath)
				forceMetadataFileDownloadFromPlugin(backupConn, incremental2Timestamp)

				gprestore(gprestorePath, restoreHelperPath, incremental2Timestamp,
					"--redirect-db", "restoredb",
					"--copy-queue-size", "4",
					"--plugin-config", pluginConfigPath)

				assertRelationsCreated(restoreConn, TOTAL_RELATIONS)
				assertDataRestored(restoreConn, publicSchemaTupleCounts)
				schema2TupleCounts["schema2.ao1"] = 1002
				assertDataRestored(restoreConn, schema2TupleCounts)
				assertArtifactsCleaned(restoreConn, fullBackupTimestamp)
				assertArtifactsCleaned(restoreConn, incremental1Timestamp)
				assertArtifactsCleaned(restoreConn, incremental2Timestamp)
			})
			It("Runs backup and restore if plugin location changed", func() {
				pluginExecutablePath := fmt.Sprintf("%s/src/github.com/greenplum-db/gpbackup/plugins/example_plugin.bash", os.Getenv("GOPATH"))
				fullBackupTimestamp := gpbackup(gpbackupPath, backupHelperPath,
					"--leaf-partition-data",
					"--plugin-config", pluginConfigPath)

				otherPluginExecutablePath := fmt.Sprintf("%s/other_plugin_location/example_plugin.bash", backupDir)
				command := exec.Command("bash", "-c", fmt.Sprintf("mkdir %s/other_plugin_location && cp %s %s/other_plugin_location", backupDir, pluginExecutablePath, backupDir))
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

				incrementalBackupTimestamp := gpbackup(gpbackupPath, backupHelperPath,
					"--leaf-partition-data",
					"--incremental",
					"--plugin-config", otherPluginConfig)

				Expect(incrementalBackupTimestamp).NotTo(BeNil())

				gprestore(gprestorePath, restoreHelperPath, incrementalBackupTimestamp,
					"--redirect-db", "restoredb",
					"--plugin-config", otherPluginConfig)

				assertRelationsCreated(restoreConn, TOTAL_RELATIONS)
				assertDataRestored(restoreConn, publicSchemaTupleCounts)
				assertArtifactsCleaned(restoreConn, fullBackupTimestamp)
				assertArtifactsCleaned(restoreConn, incrementalBackupTimestamp)
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
				backupTimestamp := gpbackup(gpbackupPath, backupHelperPath, "--leaf-partition-data")

				// Restore the backup to a different database
				testhelper.AssertQueryRuns(restoreConn,
					"DROP SCHEMA IF EXISTS testschema CASCADE;")
				gprestore(gprestorePath, restoreHelperPath, backupTimestamp, "--redirect-db", "restoredb")

				// Trigger an incremental backup
				testhelper.AssertQueryRuns(backupConn,
					"INSERT INTO testschema.ao_table VALUES (1);")
				incrementalBackupTimestamp := gpbackup(gpbackupPath, backupHelperPath, "--leaf-partition-data", "--incremental")

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
				backupTimestamp := gpbackup(gpbackupPath, backupHelperPath, "--leaf-partition-data")
				testhelper.AssertQueryRuns(backupConn,
					"INSERT INTO zoo VALUES (1);")
				incrementalBackupTimestamp := gpbackup(gpbackupPath, backupHelperPath, "--leaf-partition-data", "--incremental")
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
				baseTimestamp := gpbackup(gpbackupPath, backupHelperPath,
					"--leaf-partition-data")

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
					timestamp = gpbackup(gpbackupPath, backupHelperPath,
						"--leaf-partition-data", "--incremental")
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
					assertArtifactsCleaned(restoreConn, timestamp)
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
					timestamp = gpbackup(gpbackupPath, backupHelperPath,
						"--leaf-partition-data", "--incremental")
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
					assertArtifactsCleaned(restoreConn, timestamp)
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
					timestamp = gpbackup(gpbackupPath, backupHelperPath,
						"--leaf-partition-data", "--incremental")
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
					assertArtifactsCleaned(restoreConn, timestamp)
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
})
