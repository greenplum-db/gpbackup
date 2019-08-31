package backup_test

import (
	"database/sql"
	"math"
	"sort"

	"github.com/greenplum-db/gp-common-go-libs/structmatcher"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/utils"
	sqlmock "github.com/DATA-DOG/go-sqlmock"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup/predata_relations tests", func() {
	BeforeEach(func() {
		toc, backupfile = testutils.InitializeTestTOC(buffer, "predata")
	})
	Describe("PrintCreateSequenceStatements", func() {
		baseSequence := backup.Relation{SchemaOid: 0, Oid: 1, Schema: "public", Name: "seq_name"}
		seqDefault := backup.Sequence{Relation: baseSequence, SequenceDefinition: backup.SequenceDefinition{LastVal: 7, Increment: 1, MaxVal: math.MaxInt64, MinVal: 1, CacheVal: 5, LogCnt: 42, IsCycled: false, IsCalled: true}}
		seqNegIncr := backup.Sequence{Relation: baseSequence, SequenceDefinition: backup.SequenceDefinition{LastVal: 7, Increment: -1, MaxVal: -1, MinVal: math.MinInt64, CacheVal: 5, LogCnt: 42, IsCycled: false, IsCalled: true}}
		seqMaxPos := backup.Sequence{Relation: baseSequence, SequenceDefinition: backup.SequenceDefinition{LastVal: 7, Increment: 1, MaxVal: 100, MinVal: 1, CacheVal: 5, LogCnt: 42, IsCycled: false, IsCalled: true}}
		seqMinPos := backup.Sequence{Relation: baseSequence, SequenceDefinition: backup.SequenceDefinition{LastVal: 7, Increment: 1, MaxVal: math.MaxInt64, MinVal: 10, CacheVal: 5, LogCnt: 42, IsCycled: false, IsCalled: true}}
		seqMaxNeg := backup.Sequence{Relation: baseSequence, SequenceDefinition: backup.SequenceDefinition{LastVal: 7, Increment: -1, MaxVal: -10, MinVal: math.MinInt64, CacheVal: 5, LogCnt: 42, IsCycled: false, IsCalled: true}}
		seqMinNeg := backup.Sequence{Relation: baseSequence, SequenceDefinition: backup.SequenceDefinition{LastVal: 7, Increment: -1, MaxVal: -1, MinVal: -100, CacheVal: 5, LogCnt: 42, IsCycled: false, IsCalled: true}}
		seqCycle := backup.Sequence{Relation: baseSequence, SequenceDefinition: backup.SequenceDefinition{LastVal: 7, Increment: 1, MaxVal: math.MaxInt64, MinVal: 1, CacheVal: 5, LogCnt: 42, IsCycled: true, IsCalled: true}}
		seqStart := backup.Sequence{Relation: baseSequence, SequenceDefinition: backup.SequenceDefinition{LastVal: 7, Increment: 1, MaxVal: math.MaxInt64, MinVal: 1, CacheVal: 5, LogCnt: 42, IsCycled: false, IsCalled: false}}
		emptySequenceMetadataMap := backup.MetadataMap{}

		It("can print a sequence with all default options", func() {
			sequences := []backup.Sequence{seqDefault}
			backup.PrintCreateSequenceStatements(backupfile, toc, sequences, emptySequenceMetadataMap)
			testutils.ExpectEntry(toc.PredataEntries, 0, "public", "", "seq_name", "SEQUENCE")
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE SEQUENCE public.seq_name
	INCREMENT BY 1
	NO MAXVALUE
	NO MINVALUE
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, true);`)
		})
		It("can print a decreasing sequence", func() {
			sequences := []backup.Sequence{seqNegIncr}
			backup.PrintCreateSequenceStatements(backupfile, toc, sequences, emptySequenceMetadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE SEQUENCE public.seq_name
	INCREMENT BY -1
	NO MAXVALUE
	NO MINVALUE
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, true);`)
		})
		It("can print an increasing sequence with a maximum value", func() {
			sequences := []backup.Sequence{seqMaxPos}
			backup.PrintCreateSequenceStatements(backupfile, toc, sequences, emptySequenceMetadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE SEQUENCE public.seq_name
	INCREMENT BY 1
	MAXVALUE 100
	NO MINVALUE
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, true);`)
		})
		It("can print an increasing sequence with a minimum value", func() {
			sequences := []backup.Sequence{seqMinPos}
			backup.PrintCreateSequenceStatements(backupfile, toc, sequences, emptySequenceMetadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE SEQUENCE public.seq_name
	INCREMENT BY 1
	NO MAXVALUE
	MINVALUE 10
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, true);`)
		})
		It("can print a decreasing sequence with a maximum value", func() {
			sequences := []backup.Sequence{seqMaxNeg}
			backup.PrintCreateSequenceStatements(backupfile, toc, sequences, emptySequenceMetadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE SEQUENCE public.seq_name
	INCREMENT BY -1
	MAXVALUE -10
	NO MINVALUE
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, true);`)
		})
		It("can print a decreasing sequence with a minimum value", func() {
			sequences := []backup.Sequence{seqMinNeg}
			backup.PrintCreateSequenceStatements(backupfile, toc, sequences, emptySequenceMetadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE SEQUENCE public.seq_name
	INCREMENT BY -1
	NO MAXVALUE
	MINVALUE -100
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, true);`)
		})
		It("can print a sequence that cycles", func() {
			sequences := []backup.Sequence{seqCycle}
			backup.PrintCreateSequenceStatements(backupfile, toc, sequences, emptySequenceMetadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE SEQUENCE public.seq_name
	INCREMENT BY 1
	NO MAXVALUE
	NO MINVALUE
	CACHE 5
	CYCLE;

SELECT pg_catalog.setval('public.seq_name', 7, true);`)
		})
		It("can print a sequence with a start value", func() {
			sequences := []backup.Sequence{seqStart}
			backup.PrintCreateSequenceStatements(backupfile, toc, sequences, emptySequenceMetadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE SEQUENCE public.seq_name
	START WITH 7
	INCREMENT BY 1
	NO MAXVALUE
	NO MINVALUE
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, false);`)
		})
		It("escapes a sequence containing single quotes", func() {
			baseSequenceWithQuote := backup.Relation{SchemaOid: 0, Oid: 1, Schema: "public", Name: "seq_'name"}
			seqWithQuote := backup.Sequence{Relation: baseSequenceWithQuote, SequenceDefinition: backup.SequenceDefinition{LastVal: 7, Increment: 1, MaxVal: math.MaxInt64, MinVal: 1, CacheVal: 5, LogCnt: 42, IsCycled: false, IsCalled: true}}
			sequences := []backup.Sequence{seqWithQuote}
			backup.PrintCreateSequenceStatements(backupfile, toc, sequences, emptySequenceMetadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE SEQUENCE public.seq_'name
	INCREMENT BY 1
	NO MAXVALUE
	NO MINVALUE
	CACHE 5;

SELECT pg_catalog.setval('public.seq_''name', 7, true);`)
		})
		It("can print a sequence with privileges, an owner, and a comment for version < 6", func() {
			testhelper.SetDBVersion(connectionPool, "5.0.0")
			sequenceMetadataMap := testutils.DefaultMetadataMap("SEQUENCE", true, true, true, false)
			sequenceMetadata := sequenceMetadataMap[seqDefault.GetUniqueID()]
			sequenceMetadata.Privileges[0].Update = false
			sequenceMetadataMap[seqDefault.GetUniqueID()] = sequenceMetadata
			sequences := []backup.Sequence{seqDefault}
			backup.PrintCreateSequenceStatements(backupfile, toc, sequences, sequenceMetadataMap)
			expectedEntries := []string{`CREATE SEQUENCE public.seq_name
	INCREMENT BY 1
	NO MAXVALUE
	NO MINVALUE
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, true);`,
				"COMMENT ON SEQUENCE public.seq_name IS 'This is a sequence comment.';",
				"ALTER TABLE public.seq_name OWNER TO testrole;",
				`REVOKE ALL ON SEQUENCE public.seq_name FROM PUBLIC;
REVOKE ALL ON SEQUENCE public.seq_name FROM testrole;
GRANT SELECT,USAGE ON SEQUENCE public.seq_name TO testrole;`}
			testutils.AssertBufferContents(toc.PredataEntries, buffer, expectedEntries...)
			testhelper.SetDBVersion(connectionPool, "5.1.0")
		})
		It("can print a sequence with privileges, an owner, security label, and a comment for version >= 6", func() {
			testhelper.SetDBVersion(connectionPool, "6.0.0")
			sequenceMetadataMap := testutils.DefaultMetadataMap("SEQUENCE", true, true, true, true)
			sequenceMetadata := sequenceMetadataMap[seqDefault.GetUniqueID()]
			sequenceMetadata.Privileges[0].Update = false
			sequenceMetadataMap[seqDefault.GetUniqueID()] = sequenceMetadata
			sequences := []backup.Sequence{seqDefault}
			backup.PrintCreateSequenceStatements(backupfile, toc, sequences, sequenceMetadataMap)
			expectedEntries := []string{`CREATE SEQUENCE public.seq_name
	START WITH 0
	INCREMENT BY 1
	NO MAXVALUE
	NO MINVALUE
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, true);`,
				"COMMENT ON SEQUENCE public.seq_name IS 'This is a sequence comment.';",
				"ALTER SEQUENCE public.seq_name OWNER TO testrole;",
				`REVOKE ALL ON SEQUENCE public.seq_name FROM PUBLIC;
REVOKE ALL ON SEQUENCE public.seq_name FROM testrole;
GRANT SELECT,USAGE ON SEQUENCE public.seq_name TO testrole;`,
				"SECURITY LABEL FOR dummy ON SEQUENCE public.seq_name IS 'unclassified';"}
			testutils.AssertBufferContents(toc.PredataEntries, buffer, expectedEntries...)
			testhelper.SetDBVersion(connectionPool, "5.1.0")
		})
		It("can print a sequence with privileges WITH GRANT OPTION", func() {
			sequenceMetadata := backup.ObjectMetadata{Privileges: []backup.ACL{testutils.DefaultACLWithGrantWithout("testrole", "SEQUENCE", "UPDATE")}}
			sequenceMetadataMap := backup.MetadataMap{seqDefault.GetUniqueID(): sequenceMetadata}
			sequences := []backup.Sequence{seqDefault}
			backup.PrintCreateSequenceStatements(backupfile, toc, sequences, sequenceMetadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE SEQUENCE public.seq_name
	INCREMENT BY 1
	NO MAXVALUE
	NO MINVALUE
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, true);`,
				`REVOKE ALL ON SEQUENCE public.seq_name FROM PUBLIC;
GRANT SELECT,USAGE ON SEQUENCE public.seq_name TO testrole WITH GRANT OPTION;`)
		})
	})
	Describe("PrintCreateViewStatement", func() {
		var (
			view          backup.View
			emptyMetadata backup.ObjectMetadata
		)
		BeforeEach(func() {
			view = backup.View{Oid: 1, Schema: "shamwow", Name: "shazam", Definition: "SELECT count(*) FROM pg_tables;"}
			emptyMetadata = backup.ObjectMetadata{}
		})
		It("can print a basic view", func() {
			backup.PrintCreateViewStatement(backupfile, toc, view, emptyMetadata)
			testutils.ExpectEntry(toc.PredataEntries, 0, "shamwow", "", "shazam", "VIEW")
			testutils.AssertBufferContents(toc.PredataEntries, buffer,
				`CREATE VIEW shamwow.shazam AS SELECT count(*) FROM pg_tables;`)
		})
		It("can print a view with privileges, an owner, and a comment for version < 6", func() {
			testhelper.SetDBVersion(connectionPool, "5.0.0")
			defer testhelper.SetDBVersion(connectionPool, "5.1.0")

			viewMetadata := testutils.DefaultMetadata("VIEW", true, true, true, false)
			backup.PrintCreateViewStatement(backupfile, toc, view, viewMetadata)
			expectedEntries := []string{"CREATE VIEW shamwow.shazam AS SELECT count(*) FROM pg_tables;",
				"COMMENT ON VIEW shamwow.shazam IS 'This is a view comment.';",
				"ALTER TABLE shamwow.shazam OWNER TO testrole;",
				`REVOKE ALL ON shamwow.shazam FROM PUBLIC;
REVOKE ALL ON shamwow.shazam FROM testrole;
GRANT ALL ON shamwow.shazam TO testrole;`}
			testutils.AssertBufferContents(toc.PredataEntries, buffer, expectedEntries...)
		})
		It("can print a view with privileges, an owner, security label, and a comment for version >= 6", func() {
			testhelper.SetDBVersion(connectionPool, "6.0.0")
			defer testhelper.SetDBVersion(connectionPool, "5.1.0")

			viewMetadata := testutils.DefaultMetadata("VIEW", true, true, true, true)
			backup.PrintCreateViewStatement(backupfile, toc, view, viewMetadata)
			expectedEntries := []string{"CREATE VIEW shamwow.shazam AS SELECT count(*) FROM pg_tables;",
				"COMMENT ON VIEW shamwow.shazam IS 'This is a view comment.';",
				"ALTER VIEW shamwow.shazam OWNER TO testrole;",
				`REVOKE ALL ON shamwow.shazam FROM PUBLIC;
REVOKE ALL ON shamwow.shazam FROM testrole;
GRANT ALL ON shamwow.shazam TO testrole;`,
				"SECURITY LABEL FOR dummy ON VIEW shamwow.shazam IS 'unclassified';"}
			testutils.AssertBufferContents(toc.PredataEntries, buffer, expectedEntries...)
		})
		It("can print a view with options", func() {
			view.Options = " WITH (security_barrier=true)"
			backup.PrintCreateViewStatement(backupfile, toc, view, emptyMetadata)
			testutils.ExpectEntry(toc.PredataEntries, 0, "shamwow", "", "shazam", "VIEW")
			testutils.AssertBufferContents(toc.PredataEntries, buffer,
				`CREATE VIEW shamwow.shazam WITH (security_barrier=true) AS SELECT count(*) FROM pg_tables;`)
		})
	})
	Describe("PrintAlterSequenceStatements", func() {
		baseSequence := backup.Relation{Schema: "public", Name: "seq_name"}
		seqDefault := backup.Sequence{Relation: baseSequence, SequenceDefinition: backup.SequenceDefinition{LastVal: 7, Increment: 1, MaxVal: math.MaxInt64, MinVal: 1, CacheVal: 5, LogCnt: 42, IsCycled: false, IsCalled: true}}
		emptyColumnOwnerMap := make(map[string]string)
		It("prints nothing for a sequence without an owning column", func() {
			sequences := []backup.Sequence{seqDefault}
			backup.PrintAlterSequenceStatements(backupfile, toc, sequences, emptyColumnOwnerMap)
			Expect(toc.PredataEntries).To(BeEmpty())
			testhelper.NotExpectRegexp(buffer, `ALTER SEQUENCE`)
		})
		It("does not write an alter sequence statement for a sequence that is not in the backup", func() {
			columnOwnerMap := map[string]string{"public.seq_name2": "public.tablename.col_one"}
			sequences := []backup.Sequence{seqDefault}
			backup.PrintAlterSequenceStatements(backupfile, toc, sequences, columnOwnerMap)
			Expect(toc.PredataEntries).To(BeEmpty())
			testhelper.NotExpectRegexp(buffer, `ALTER SEQUENCE`)
		})
		It("can print an ALTER SEQUENCE statement for a sequence with an owning column", func() {
			columnOwnerMap := map[string]string{"public.seq_name": "public.tablename.col_one"}
			sequences := []backup.Sequence{seqDefault}
			backup.PrintAlterSequenceStatements(backupfile, toc, sequences, columnOwnerMap)
			testutils.ExpectEntry(toc.PredataEntries, 0, "public", "", "seq_name", "SEQUENCE OWNER")
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `ALTER SEQUENCE public.seq_name OWNED BY public.tablename.col_one;`)
		})
	})
	Describe("SplitTablesByPartitionType", func() {
		var tables []backup.Table
		var includeList []string
		var expectedMetadataTables = []backup.Table{
			{
				Relation:        backup.Relation{Oid: 1, Schema: "public", Name: "part_parent1"},
				TableDefinition: backup.TableDefinition{PartitionLevelInfo: backup.PartitionLevelInfo{Level: "p"}},
			},
			{
				Relation:        backup.Relation{Oid: 2, Schema: "public", Name: "part_parent2"},
				TableDefinition: backup.TableDefinition{PartitionLevelInfo: backup.PartitionLevelInfo{Level: "p"}},
			},
			{
				Relation:        backup.Relation{Oid: 8, Schema: "public", Name: "test_table"},
				TableDefinition: backup.TableDefinition{PartitionLevelInfo: backup.PartitionLevelInfo{Level: "n"}},
			},
		}
		BeforeEach(func() {
			tables = []backup.Table{
				{
					Relation:        backup.Relation{Oid: 1, Schema: "public", Name: "part_parent1"},
					TableDefinition: backup.TableDefinition{PartitionLevelInfo: backup.PartitionLevelInfo{Level: "p"}},
				},
				{
					Relation:        backup.Relation{Oid: 2, Schema: "public", Name: "part_parent2"},
					TableDefinition: backup.TableDefinition{PartitionLevelInfo: backup.PartitionLevelInfo{Level: "p"}},
				},
				{
					Relation:        backup.Relation{Oid: 3, Schema: "public", Name: "part_parent1_inter1"},
					TableDefinition: backup.TableDefinition{PartitionLevelInfo: backup.PartitionLevelInfo{Level: "i"}},
				},
				{
					Relation:        backup.Relation{Oid: 4, Schema: "public", Name: "part_parent1_child1"},
					TableDefinition: backup.TableDefinition{PartitionLevelInfo: backup.PartitionLevelInfo{Level: "l"}},
				},
				{
					Relation:        backup.Relation{Oid: 5, Schema: "public", Name: "part_parent1_child2"},
					TableDefinition: backup.TableDefinition{PartitionLevelInfo: backup.PartitionLevelInfo{Level: "l"}},
				},
				{
					Relation:        backup.Relation{Oid: 6, Schema: "public", Name: "part_parent2_child1"},
					TableDefinition: backup.TableDefinition{PartitionLevelInfo: backup.PartitionLevelInfo{Level: "l"}},
				},
				{
					Relation:        backup.Relation{Oid: 7, Schema: "public", Name: "part_parent2_child2"},
					TableDefinition: backup.TableDefinition{PartitionLevelInfo: backup.PartitionLevelInfo{Level: "l"}},
				},
				{
					Relation:        backup.Relation{Oid: 8, Schema: "public", Name: "test_table"},
					TableDefinition: backup.TableDefinition{PartitionLevelInfo: backup.PartitionLevelInfo{Level: "n"}},
				},
			}
		})
		Context("leafPartitionData and includeTables", func() {
			It("gets only parent partitions of included tables for metadata and only child partitions for data", func() {
				includeList = []string{"public.part_parent1", "public.part_parent2_child1", "public.part_parent2_child2", "public.test_table"}
				_ = cmdFlags.Set(utils.LEAF_PARTITION_DATA, "true")

				metadataTables, dataTables := backup.SplitTablesByPartitionType(tables, includeList)

				Expect(metadataTables).To(Equal(expectedMetadataTables))

				expectedDataTables := []string{"public.part_parent1_child1", "public.part_parent1_child2", "public.part_parent2_child1", "public.part_parent2_child2", "public.test_table"}
				dataTableNames := make([]string, 0)
				for _, table := range dataTables {
					dataTableNames = append(dataTableNames, table.FQN())
				}
				sort.Strings(dataTableNames)

				Expect(dataTables).To(HaveLen(5))
				Expect(dataTableNames).To(Equal(expectedDataTables))
			})
		})
		Context("leafPartitionData only", func() {
			It("gets only parent partitions for metadata and only child partitions in data", func() {
				_ = cmdFlags.Set(utils.LEAF_PARTITION_DATA, "true")
				includeList = []string{}
				metadataTables, dataTables := backup.SplitTablesByPartitionType(tables, includeList)

				Expect(metadataTables).To(Equal(expectedMetadataTables))

				expectedDataTables := []string{"public.part_parent1_child1", "public.part_parent1_child2", "public.part_parent2_child1", "public.part_parent2_child2", "public.test_table"}
				dataTableNames := make([]string, 0)
				for _, table := range dataTables {
					dataTableNames = append(dataTableNames, table.FQN())
				}
				sort.Strings(dataTableNames)

				Expect(dataTables).To(HaveLen(5))
				Expect(dataTableNames).To(Equal(expectedDataTables))
			})
		})
		Context("includeTables only", func() {
			It("gets only parent partitions of included tables for metadata and only included tables for data", func() {
				_ = cmdFlags.Set(utils.LEAF_PARTITION_DATA, "false")
				includeList = []string{"public.part_parent1", "public.part_parent2_child1", "public.part_parent2_child2", "public.test_table"}
				metadataTables, dataTables := backup.SplitTablesByPartitionType(tables, includeList)

				Expect(metadataTables).To(Equal(expectedMetadataTables))

				expectedDataTables := []string{"public.part_parent1", "public.part_parent2_child1", "public.part_parent2_child2", "public.test_table"}
				dataTableNames := make([]string, 0)
				for _, table := range dataTables {
					dataTableNames = append(dataTableNames, table.FQN())
				}
				sort.Strings(dataTableNames)

				Expect(dataTables).To(HaveLen(4))
				Expect(dataTableNames).To(Equal(expectedDataTables))
			})
		})
		Context("neither leafPartitionData nor includeTables", func() {
			It("gets the same table list for both metadata and data", func() {
				includeList = []string{}
				tables = []backup.Table{
					{
						Relation:        backup.Relation{Oid: 1, Schema: "public", Name: "part_parent1"},
						TableDefinition: backup.TableDefinition{PartitionLevelInfo: backup.PartitionLevelInfo{Level: "p"}},
					},
					{
						Relation:        backup.Relation{Oid: 2, Schema: "public", Name: "part_parent2"},
						TableDefinition: backup.TableDefinition{PartitionLevelInfo: backup.PartitionLevelInfo{Level: "p"}},
					},
					{
						Relation:        backup.Relation{Oid: 8, Schema: "public", Name: "test_table"},
						TableDefinition: backup.TableDefinition{PartitionLevelInfo: backup.PartitionLevelInfo{Level: "n"}},
					},
				}
				_ = cmdFlags.Set(utils.LEAF_PARTITION_DATA, "false")
				_ = cmdFlags.Set(utils.INCLUDE_RELATION, "")
				metadataTables, dataTables := backup.SplitTablesByPartitionType(tables, includeList)

				Expect(metadataTables).To(Equal(expectedMetadataTables))

				expectedDataTables := []string{"public.part_parent1", "public.part_parent2", "public.test_table"}
				dataTableNames := make([]string, 0)
				for _, table := range dataTables {
					dataTableNames = append(dataTableNames, table.FQN())
				}
				sort.Strings(dataTableNames)

				Expect(dataTables).To(HaveLen(3))
				Expect(dataTableNames).To(Equal(expectedDataTables))
			})
			It("adds a suffix to external partition tables", func() {
				includeList = []string{}
				tables = []backup.Table{
					{
						Relation:        backup.Relation{Oid: 1, Schema: "public", Name: "part_parent1_prt_1"},
						TableDefinition: backup.TableDefinition{PartitionLevelInfo: backup.PartitionLevelInfo{Level: "l"}, IsExternal: true},
					},
					{
						Relation:        backup.Relation{Oid: 2, Schema: "public", Name: "long_naaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaame"},
						TableDefinition: backup.TableDefinition{PartitionLevelInfo: backup.PartitionLevelInfo{Level: "l"}, IsExternal: true},
					},
				}
				_ = cmdFlags.Set(utils.LEAF_PARTITION_DATA, "false")
				_ = cmdFlags.Set(utils.INCLUDE_RELATION, "")
				metadataTables, _ := backup.SplitTablesByPartitionType(tables, includeList)

				expectedTables := []backup.Table{
					{
						Relation:        backup.Relation{Oid: 1, Schema: "public", Name: "part_parent1_prt_1_ext_part_"},
						TableDefinition: backup.TableDefinition{PartitionLevelInfo: backup.PartitionLevelInfo{Level: "l"}, IsExternal: true},
					},
					{
						Relation:        backup.Relation{Oid: 2, Schema: "public", Name: "long_naaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa_ext_part_"},
						TableDefinition: backup.TableDefinition{PartitionLevelInfo: backup.PartitionLevelInfo{Level: "l"}, IsExternal: true},
					},
				}
				Expect(metadataTables).To(HaveLen(2))
				structmatcher.ExpectStructsToMatch(&expectedTables[0], &metadataTables[0])
				structmatcher.ExpectStructsToMatch(&expectedTables[1], &metadataTables[1])
			})
		})
	})
	Describe("AppendExtPartSuffix", func() {
		It("adds a suffix to an unquoted external partition table", func() {
			tablename := "name"
			expectedName := "name_ext_part_"
			suffixName := backup.AppendExtPartSuffix(tablename)
			Expect(suffixName).To(Equal(expectedName))
		})
		It("adds a suffix to an unquoted external partition table that is too long", func() {
			tablename := "long_naaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaame"
			expectedName := "long_naaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa_ext_part_"
			suffixName := backup.AppendExtPartSuffix(tablename)
			Expect(suffixName).To(Equal(expectedName))
		})
		It("adds a suffix to a quoted external partition table", func() {
			tablename := `"!name"`
			expectedName := `"!name_ext_part_"`
			suffixName := backup.AppendExtPartSuffix(tablename)
			Expect(suffixName).To(Equal(expectedName))
		})
		It("adds a suffix to a quoted external partition table that is too long", func() {
			tablename := `"long!naaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaame"`
			expectedName := `"long!naaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa_ext_part_"`
			suffixName := backup.AppendExtPartSuffix(tablename)
			Expect(suffixName).To(Equal(expectedName))
		})
	})
	Describe("ConstructColumnPrivilegesMap", func() {
		expectedACL := []backup.ACL{{Grantee: "gpadmin", Select: true}}
		colI := backup.ColumnPrivilegesQueryStruct{TableOid: 1, Name: "i", Privileges: sql.NullString{String: "gpadmin=r/gpadmin", Valid: true}, Kind: ""}
		colJ := backup.ColumnPrivilegesQueryStruct{TableOid: 1, Name: "j", Privileges: sql.NullString{String: "gpadmin=r/gpadmin", Valid: true}, Kind: ""}
		colK1 := backup.ColumnPrivilegesQueryStruct{TableOid: 2, Name: "k", Privileges: sql.NullString{String: "gpadmin=r/gpadmin", Valid: true}, Kind: ""}
		colK2 := backup.ColumnPrivilegesQueryStruct{TableOid: 2, Name: "k", Privileges: sql.NullString{String: "testrole=r/testrole", Valid: true}, Kind: ""}
		colDefault := backup.ColumnPrivilegesQueryStruct{TableOid: 2, Name: "l", Privileges: sql.NullString{String: "", Valid: false}, Kind: "Default"}
		colEmpty := backup.ColumnPrivilegesQueryStruct{TableOid: 2, Name: "m", Privileges: sql.NullString{String: "", Valid: false}, Kind: "Empty"}
		privileges := []backup.ColumnPrivilegesQueryStruct{}
		BeforeEach(func() {
			rolnames := sqlmock.NewRows([]string{"rolename", "quotedrolename"}).
				AddRow("gpadmin", "gpadmin").
				AddRow("testrole", "testrole")
			mock.ExpectQuery("SELECT rolname (.*)").
				WillReturnRows(rolnames)
			privileges = []backup.ColumnPrivilegesQueryStruct{}
		})
		It("No columns", func() {
			metadataMap := backup.ConstructColumnPrivilegesMap(privileges)
			Expect(metadataMap).To(BeEmpty())
		})
		It("One column", func() {
			privileges = []backup.ColumnPrivilegesQueryStruct{colI}
			metadataMap := backup.ConstructColumnPrivilegesMap(privileges)
			Expect(metadataMap).To(HaveLen(1))
			Expect(metadataMap[1]).To(HaveLen(1))
			Expect(metadataMap[1]["i"]).To(Equal(expectedACL))
		})
		It("Multiple columns on same table", func() {
			privileges = []backup.ColumnPrivilegesQueryStruct{colI, colJ}
			metadataMap := backup.ConstructColumnPrivilegesMap(privileges)
			Expect(metadataMap).To(HaveLen(1))
			Expect(metadataMap[1]).To(HaveLen(2))
			Expect(metadataMap[1]["i"]).To(Equal(expectedACL))
			Expect(metadataMap[1]["j"]).To(Equal(expectedACL))
		})
		It("Multiple columns on multiple tables", func() {
			privileges = []backup.ColumnPrivilegesQueryStruct{colI, colJ, colK1, colK2}
			metadataMap := backup.ConstructColumnPrivilegesMap(privileges)

			expectedACLForK := []backup.ACL{{Grantee: "gpadmin", Select: true}, {Grantee: "testrole", Select: true}}

			Expect(metadataMap).To(HaveLen(2))
			Expect(metadataMap[1]).To(HaveLen(2))
			Expect(metadataMap[2]).To(HaveLen(1))
			Expect(metadataMap[1]["i"]).To(Equal(expectedACL))
			Expect(metadataMap[1]["j"]).To(Equal(expectedACL))
			Expect(metadataMap[2]["k"]).To(Equal(expectedACLForK))
		})
		It("Default kind", func() {
			privileges = []backup.ColumnPrivilegesQueryStruct{colDefault}
			metadataMap := backup.ConstructColumnPrivilegesMap(privileges)

			expectedACLForDefaultKind := []backup.ACL{}

			Expect(metadataMap).To(HaveLen(1))
			Expect(metadataMap[2]).To(HaveLen(1))
			Expect(metadataMap[2]["l"]).To(Equal(expectedACLForDefaultKind))
		})
		It("'Empty' kind", func() {
			privileges = []backup.ColumnPrivilegesQueryStruct{colEmpty}
			metadataMap := backup.ConstructColumnPrivilegesMap(privileges)

			expectedACLForEmptyKind := []backup.ACL{{Grantee: "GRANTEE"}}

			Expect(metadataMap).To(HaveLen(1))
			Expect(metadataMap[2]).To(HaveLen(1))
			Expect(metadataMap[2]["m"]).To(Equal(expectedACLForEmptyKind))
		})
	})
})
