package integration

import (
	"fmt"

	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/options"
	"github.com/greenplum-db/gpbackup/toc"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup integration tests", func() {
	const (
		dropTableSQL  = "DROP TABLE %s"
		addColumnSQL  = "ALTER TABLE %s ADD COLUMN k int DEFAULT 0"
		dropColumnSQL = "ALTER TABLE %s DROP COLUMN k"
		insertSQL     = "INSERT INTO %s values(10)"
		deleteSQL     = "DELETE FROM %s"
	)

	var aoTableFQN = "public.ao_foo"
	var aoCOTableFQN = "public.ao_co_foo"
	var aoPartParentTableFQN = "public.ao_part"
	var aoPartChildTableFQN = "public.ao_part_1_prt_child"
	BeforeEach(func() {
		testhelper.AssertQueryRuns(connectionPool,
			fmt.Sprintf("CREATE TABLE %s (i int) WITH (appendonly=true)", aoTableFQN))
		testhelper.AssertQueryRuns(connectionPool,
			fmt.Sprintf("CREATE TABLE %s (i int) WITH (appendonly=true,orientation='column')", aoCOTableFQN))
		testhelper.AssertQueryRuns(connectionPool,
			fmt.Sprintf(`CREATE TABLE %s (i int) WITH (appendonly=true)
	DISTRIBUTED BY (i)
	PARTITION BY LIST (i)
	(
		PARTITION child VALUES (10)
	);`, aoPartParentTableFQN))
	})
	AfterEach(func() {
		testhelper.AssertQueryRuns(connectionPool, fmt.Sprintf(dropTableSQL, aoTableFQN))
		testhelper.AssertQueryRuns(connectionPool, fmt.Sprintf(dropTableSQL, aoCOTableFQN))
		testhelper.AssertQueryRuns(connectionPool, fmt.Sprintf(dropTableSQL, aoPartParentTableFQN))
	})
	Describe("GetAOIncrementalMetadata", func() {
		Context("AO, AO_CO and AO partition tables are only just created", func() {
			var aoIncrementalMetadata map[string]toc.AOEntry
			BeforeEach(func() {
				aoIncrementalMetadata = backup.GetAOIncrementalMetadata(connectionPool)
			})
			It("should have a modcount of 0", func() {
				Expect(aoIncrementalMetadata[aoTableFQN].Modcount).To(Equal(int64(0)))
				Expect(aoIncrementalMetadata[aoCOTableFQN].Modcount).To(Equal(int64(0)))
				Expect(aoIncrementalMetadata[aoPartParentTableFQN].Modcount).To(Equal(int64(0)))
				Expect(aoIncrementalMetadata[aoPartChildTableFQN].Modcount).To(Equal(int64(0)))
			})
			It("should have a last DDL timestamp", func() {
				Expect(aoIncrementalMetadata[aoTableFQN].LastDDLTimestamp).To(Not(BeEmpty()))
				Expect(aoIncrementalMetadata[aoCOTableFQN].LastDDLTimestamp).To(Not(BeEmpty()))
				Expect(aoIncrementalMetadata[aoPartChildTableFQN].LastDDLTimestamp).To(Not(BeEmpty()))

				// For GPDB 7+, the root partition is not included.
				// TODO: Should the root be included?
				if connectionPool.Version.Before("7") {
					Expect(aoIncrementalMetadata[aoPartParentTableFQN].LastDDLTimestamp).To(Not(BeEmpty()))
				}

			})
		})
		Context("AO, AO_CO and AO partition tables have DML changes", func() {
			Context("After an insert(s)", func() {
				var initialAOIncrementalMetadata map[string]toc.AOEntry
				var aoIncrementalMetadata map[string]toc.AOEntry
				BeforeEach(func() {
					initialAOIncrementalMetadata = backup.GetAOIncrementalMetadata(connectionPool)

					testhelper.AssertQueryRuns(connectionPool, fmt.Sprintf(insertSQL, aoTableFQN))
					testhelper.AssertQueryRuns(connectionPool, fmt.Sprintf(insertSQL, aoCOTableFQN))
					testhelper.AssertQueryRuns(connectionPool, fmt.Sprintf(insertSQL, aoPartParentTableFQN))

					aoIncrementalMetadata = backup.GetAOIncrementalMetadata(connectionPool)
				})
				AfterEach(func() {
					testhelper.AssertQueryRuns(connectionPool, fmt.Sprintf(deleteSQL, aoTableFQN))
					testhelper.AssertQueryRuns(connectionPool, fmt.Sprintf(deleteSQL, aoCOTableFQN))
					testhelper.AssertQueryRuns(connectionPool, fmt.Sprintf(deleteSQL, aoPartParentTableFQN))
				})
				It("should increase modcount for non partition tables", func() {
					Expect(aoIncrementalMetadata[aoTableFQN].Modcount).
						To(BeNumerically(">", initialAOIncrementalMetadata[aoTableFQN].Modcount))
					Expect(aoIncrementalMetadata[aoCOTableFQN].Modcount).
						To(BeNumerically(">", initialAOIncrementalMetadata[aoCOTableFQN].Modcount))
				})
				It("should NOT increase modcount for parent partition tables", func() {
					Expect(aoIncrementalMetadata[aoPartParentTableFQN].Modcount).
						To(Equal(initialAOIncrementalMetadata[aoPartParentTableFQN].Modcount))
				})
				It("should increase modcount for modified child partition tables", func() {
					Expect(aoIncrementalMetadata[aoPartChildTableFQN].Modcount).
						To(BeNumerically(">", initialAOIncrementalMetadata[aoPartChildTableFQN].Modcount))
				})
				It("should have a last DDL timestamp", func() {
					Expect(aoIncrementalMetadata[aoTableFQN].LastDDLTimestamp).To(Not(BeEmpty()))
					Expect(aoIncrementalMetadata[aoCOTableFQN].LastDDLTimestamp).To(Not(BeEmpty()))
					Expect(aoIncrementalMetadata[aoPartChildTableFQN].LastDDLTimestamp).To(Not(BeEmpty()))

					// For GPDB 7+, the root partition is not included.
					// TODO: Should the root be included?
					if connectionPool.Version.Before("7") {
						Expect(aoIncrementalMetadata[aoPartParentTableFQN].LastDDLTimestamp).To(Not(BeEmpty()))
					}
				})
			})
			Context("After a delete operation", func() {
				var initialAOIncrementalMetadata map[string]toc.AOEntry
				var aoIncrementalMetadata map[string]toc.AOEntry
				BeforeEach(func() {
					testhelper.AssertQueryRuns(connectionPool, fmt.Sprintf(insertSQL, aoTableFQN))
					testhelper.AssertQueryRuns(connectionPool, fmt.Sprintf(insertSQL, aoCOTableFQN))
					testhelper.AssertQueryRuns(connectionPool, fmt.Sprintf(insertSQL, aoPartParentTableFQN))

					initialAOIncrementalMetadata = backup.GetAOIncrementalMetadata(connectionPool)

					testhelper.AssertQueryRuns(connectionPool, fmt.Sprintf(deleteSQL, aoTableFQN))
					testhelper.AssertQueryRuns(connectionPool, fmt.Sprintf(deleteSQL, aoCOTableFQN))
					testhelper.AssertQueryRuns(connectionPool, fmt.Sprintf(deleteSQL, aoPartParentTableFQN))

					aoIncrementalMetadata = backup.GetAOIncrementalMetadata(connectionPool)
				})
				It("should increase modcount for non partition tables", func() {
					Expect(aoIncrementalMetadata[aoTableFQN].Modcount).
						To(BeNumerically(">", initialAOIncrementalMetadata[aoTableFQN].Modcount))
					Expect(aoIncrementalMetadata[aoCOTableFQN].Modcount).
						To(BeNumerically(">", initialAOIncrementalMetadata[aoCOTableFQN].Modcount))
				})
				It("should NOT increase modcount for parent partition tables", func() {
					Expect(aoIncrementalMetadata[aoPartParentTableFQN].Modcount).
						To(Equal(initialAOIncrementalMetadata[aoPartParentTableFQN].Modcount))
				})
				It("should increase modcount for modified child partition tables", func() {
					Expect(aoIncrementalMetadata[aoPartChildTableFQN].Modcount).
						To(BeNumerically(">", initialAOIncrementalMetadata[aoPartChildTableFQN].Modcount))
				})
				It("should have a last DDL timestamp", func() {
					Expect(aoIncrementalMetadata[aoTableFQN].LastDDLTimestamp).To(Not(BeEmpty()))
					Expect(aoIncrementalMetadata[aoCOTableFQN].LastDDLTimestamp).To(Not(BeEmpty()))
					Expect(aoIncrementalMetadata[aoPartChildTableFQN].LastDDLTimestamp).To(Not(BeEmpty()))

					// For GPDB 7+, the root partition is not included.
					// TODO: Should the root be included?
					if connectionPool.Version.Before("7") {
						Expect(aoIncrementalMetadata[aoPartParentTableFQN].LastDDLTimestamp).To(Not(BeEmpty()))
					}
				})
			})
		})
		Context("AO, AO_CO and AO partition tables have DDL changes", func() {
			var initialAOIncrementalMetadata map[string]toc.AOEntry
			var aoIncrementalMetadata map[string]toc.AOEntry
			Context("After a column add", func() {
				BeforeEach(func() {
					initialAOIncrementalMetadata = backup.GetAOIncrementalMetadata(connectionPool)

					testhelper.AssertQueryRuns(connectionPool, fmt.Sprintf(addColumnSQL, aoTableFQN))
					testhelper.AssertQueryRuns(connectionPool, fmt.Sprintf(addColumnSQL, aoCOTableFQN))
					testhelper.AssertQueryRuns(connectionPool, fmt.Sprintf(addColumnSQL, aoPartParentTableFQN))

					aoIncrementalMetadata = backup.GetAOIncrementalMetadata(connectionPool)
				})
				It("should NOT care about modcount", func() {})
				It("should have a changed last DDL timestamp", func() {
					Expect(aoIncrementalMetadata[aoTableFQN].LastDDLTimestamp).
						To(Not(Equal(initialAOIncrementalMetadata[aoTableFQN].LastDDLTimestamp)))
					Expect(aoIncrementalMetadata[aoCOTableFQN].LastDDLTimestamp).
						To(Not(Equal(initialAOIncrementalMetadata[aoCOTableFQN].LastDDLTimestamp)))
					Expect(aoIncrementalMetadata[aoPartParentTableFQN].LastDDLTimestamp).
						To(Not(Equal(initialAOIncrementalMetadata[aoCOTableFQN].LastDDLTimestamp)))
					Expect(aoIncrementalMetadata[aoPartChildTableFQN].LastDDLTimestamp).
						To(Not(Equal(initialAOIncrementalMetadata[aoCOTableFQN].LastDDLTimestamp)))
				})
			})
			Context("After a column drop", func() {
				BeforeEach(func() {
					testhelper.AssertQueryRuns(connectionPool, fmt.Sprintf(addColumnSQL, aoTableFQN))
					testhelper.AssertQueryRuns(connectionPool, fmt.Sprintf(addColumnSQL, aoCOTableFQN))
					testhelper.AssertQueryRuns(connectionPool, fmt.Sprintf(addColumnSQL, aoPartParentTableFQN))
					initialAOIncrementalMetadata = backup.GetAOIncrementalMetadata(connectionPool)

					testhelper.AssertQueryRuns(connectionPool, fmt.Sprintf(dropColumnSQL, aoTableFQN))
					testhelper.AssertQueryRuns(connectionPool, fmt.Sprintf(dropColumnSQL, aoCOTableFQN))
					testhelper.AssertQueryRuns(connectionPool, fmt.Sprintf(dropColumnSQL, aoPartParentTableFQN))

					aoIncrementalMetadata = backup.GetAOIncrementalMetadata(connectionPool)
				})
				It("should NOT care about modcount", func() {})
				It("should have a changed last DDL timestamp", func() {
					Expect(aoIncrementalMetadata[aoTableFQN].LastDDLTimestamp).
						To(Not(Equal(initialAOIncrementalMetadata[aoTableFQN].LastDDLTimestamp)))
					Expect(aoIncrementalMetadata[aoCOTableFQN].LastDDLTimestamp).
						To(Not(Equal(initialAOIncrementalMetadata[aoCOTableFQN].LastDDLTimestamp)))
					Expect(aoIncrementalMetadata[aoPartParentTableFQN].LastDDLTimestamp).
						To(Not(Equal(initialAOIncrementalMetadata[aoCOTableFQN].LastDDLTimestamp)))
					Expect(aoIncrementalMetadata[aoPartChildTableFQN].LastDDLTimestamp).
						To(Not(Equal(initialAOIncrementalMetadata[aoCOTableFQN].LastDDLTimestamp)))
				})
			})
			Context("After truncating a child partition", func() {
				BeforeEach(func() {
					initialAOIncrementalMetadata = backup.GetAOIncrementalMetadata(connectionPool)

					testhelper.AssertQueryRuns(connectionPool, fmt.Sprintf("TRUNCATE TABLE %s", aoPartChildTableFQN))

					aoIncrementalMetadata = backup.GetAOIncrementalMetadata(connectionPool)
				})
				It("should NOT care about modcount", func() {})
				It("should have a changed last DDL timestamp for the child", func() {
					Expect(aoIncrementalMetadata[aoPartChildTableFQN].LastDDLTimestamp).
						To(Not(Equal(initialAOIncrementalMetadata[aoPartChildTableFQN].LastDDLTimestamp)))
				})
				It("should NOT have a changed last DDL timestamp for the parent", func() {
					Expect(aoIncrementalMetadata[aoPartParentTableFQN].LastDDLTimestamp).
						To(Equal(initialAOIncrementalMetadata[aoPartParentTableFQN].LastDDLTimestamp))
				})
			})
		})
		Context("AO, AO_CO and AO partition tables have DML and DDL changes", func() {
			var initialAOIncrementalMetadata map[string]toc.AOEntry
			var aoIncrementalMetadata map[string]toc.AOEntry
			Context("After an insert followed by an ALTER table", func() {
				BeforeEach(func() {
					initialAOIncrementalMetadata = backup.GetAOIncrementalMetadata(connectionPool)

					testhelper.AssertQueryRuns(connectionPool, fmt.Sprintf(insertSQL, aoTableFQN))
					testhelper.AssertQueryRuns(connectionPool, fmt.Sprintf(addColumnSQL, aoTableFQN))
					testhelper.AssertQueryRuns(connectionPool, fmt.Sprintf(insertSQL, aoCOTableFQN))
					testhelper.AssertQueryRuns(connectionPool, fmt.Sprintf(addColumnSQL, aoCOTableFQN))
					testhelper.AssertQueryRuns(connectionPool, fmt.Sprintf(insertSQL, aoPartParentTableFQN))
					testhelper.AssertQueryRuns(connectionPool, fmt.Sprintf(addColumnSQL, aoPartParentTableFQN))

					aoIncrementalMetadata = backup.GetAOIncrementalMetadata(connectionPool)
				})
				It("should NOT care about modcount", func() {
					//We don't care about modcount because DDL operations can reset the modcount value
				})
				It("should have a changed last DDL timestamp", func() {
					Expect(aoIncrementalMetadata[aoTableFQN].LastDDLTimestamp).
						To(Not(Equal(initialAOIncrementalMetadata[aoTableFQN].LastDDLTimestamp)))
					Expect(aoIncrementalMetadata[aoCOTableFQN].LastDDLTimestamp).
						To(Not(Equal(initialAOIncrementalMetadata[aoCOTableFQN].LastDDLTimestamp)))
					Expect(aoIncrementalMetadata[aoPartParentTableFQN].LastDDLTimestamp).
						To(Not(Equal(initialAOIncrementalMetadata[aoCOTableFQN].LastDDLTimestamp)))
					Expect(aoIncrementalMetadata[aoPartChildTableFQN].LastDDLTimestamp).
						To(Not(Equal(initialAOIncrementalMetadata[aoCOTableFQN].LastDDLTimestamp)))
				})
			})
		})
		Context("Filtered backup", func() {
			var aoIncrementalMetadata map[string]toc.AOEntry
			Context("During a table-filtered backup", func() {
				It("only retrieves ao metadata for specific tables", func() {
					_ = backupCmdFlags.Set(options.INCLUDE_RELATION, aoTableFQN)
					opts, err := options.NewOptions(backupCmdFlags)
					Expect(err).NotTo(HaveOccurred())
					backup.ValidateAndProcessFilterLists(opts)

					aoIncrementalMetadata = backup.GetAOIncrementalMetadata(connectionPool)
					Expect(aoIncrementalMetadata).To(HaveLen(1))
				})
			})
			Context("During a schema-filtered backup", func() {
				It("only retrieves ao metadata for tables in a specific schema", func() {
					testhelper.AssertQueryRuns(connectionPool, "CREATE SCHEMA testschema")
					defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA testschema CASCADE")
					testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE testschema.ao_foo (i int) WITH (appendonly=true)")

					_ = backupCmdFlags.Set(options.INCLUDE_SCHEMA, "testschema")

					aoIncrementalMetadata = backup.GetAOIncrementalMetadata(connectionPool)
					Expect(aoIncrementalMetadata).To(HaveLen(1))
				})
			})
		})
	})
})
