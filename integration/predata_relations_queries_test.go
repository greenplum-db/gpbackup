package integration

import (
	"sort"

	"github.com/greenplum-db/gp-common-go-libs/structmatcher"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"math"
)

var _ = Describe("backup integration tests", func() {
	Describe("GetAllUserTables", func() {
		BeforeEach(func() {
			backup.SetLeafPartitionData(false)
		})
		It("returns user table information for basic heap tables", func() {
			testhelper.AssertQueryRuns(connection, "CREATE TABLE public.foo(i int)")
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.foo")
			testhelper.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
			defer testhelper.AssertQueryRuns(connection, "DROP SCHEMA testschema CASCADE")
			testhelper.AssertQueryRuns(connection, "CREATE TABLE testschema.testtable(t text)")

			tables := backup.GetAllUserTables(connection)

			tableFoo := backup.Relation{Schema: "public", Name: "foo"}

			tableTestTable := backup.Relation{Schema: "testschema", Name: "testtable"}

			Expect(len(tables)).To(Equal(2))
			structmatcher.ExpectStructsToMatchExcluding(&tableFoo, &tables[0], "SchemaOid", "Oid")
			structmatcher.ExpectStructsToMatchExcluding(&tableTestTable, &tables[1], "SchemaOid", "Oid")
		})
		Context("Retrieving external partitions", func() {
			It("returns parent and external leaf partition table if the filter includes a leaf table and leaf-partition-data is set", func() {
				backup.SetLeafPartitionData(true)
				backup.SetIncludeRelations([]string{"public.partition_table_1_prt_boys"})
				defer backup.SetIncludeRelations([]string{})
				testhelper.AssertQueryRuns(connection, `CREATE TABLE public.partition_table (id int, gender char(1))
DISTRIBUTED BY (id)
PARTITION BY LIST (gender)
( PARTITION girls VALUES ('F'),
  PARTITION boys VALUES ('M'),
  DEFAULT PARTITION other );`)
				testhelper.AssertQueryRuns(connection, `CREATE EXTERNAL WEB TABLE public.partition_table_ext_part_ (like public.partition_table_1_prt_girls)
EXECUTE 'echo -e "2\n1"' on host
FORMAT 'csv';`)
				testhelper.AssertQueryRuns(connection, `ALTER TABLE public.partition_table EXCHANGE PARTITION girls WITH TABLE public.partition_table_ext_part_ WITHOUT VALIDATION;`)
				defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.partition_table")
				defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.partition_table_ext_part_")

				tables := backup.GetAllUserTables(connection)

				expectedTableNames := []string{"public.partition_table", "public.partition_table_1_prt_boys", "public.partition_table_1_prt_girls"}
				tableNames := make([]string, 0)
				for _, table := range tables {
					tableNames = append(tableNames, table.FQN())
				}
				sort.Strings(tableNames)

				Expect(len(tables)).To(Equal(3))
				Expect(tableNames).To(Equal(expectedTableNames))
			})
			It("returns external partition tables for an included parent table if the filter includes a parent partition table", func() {
				backup.SetIncludeRelations([]string{"public.partition_table1", "public.partition_table2_1_prt_other"})
				defer backup.SetIncludeRelations([]string{})
				testhelper.AssertQueryRuns(connection, `CREATE TABLE public.partition_table1 (id int, gender char(1))
DISTRIBUTED BY (id)
PARTITION BY LIST (gender)
( PARTITION girls VALUES ('F'),
  PARTITION boys VALUES ('M'),
  DEFAULT PARTITION other );`)
				testhelper.AssertQueryRuns(connection, `CREATE EXTERNAL WEB TABLE public.partition_table1_ext_part_ (like public.partition_table1_1_prt_boys)
EXECUTE 'echo -e "2\n1"' on host
FORMAT 'csv';`)
				testhelper.AssertQueryRuns(connection, `ALTER TABLE public.partition_table1 EXCHANGE PARTITION boys WITH TABLE public.partition_table1_ext_part_ WITHOUT VALIDATION;`)
				defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.partition_table1")
				defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.partition_table1_ext_part_")
				testhelper.AssertQueryRuns(connection, `CREATE TABLE public.partition_table2 (id int, gender char(1))
DISTRIBUTED BY (id)
PARTITION BY LIST (gender)
( PARTITION girls VALUES ('F'),
  PARTITION boys VALUES ('M'),
  DEFAULT PARTITION other );`)
				testhelper.AssertQueryRuns(connection, `CREATE EXTERNAL WEB TABLE public.partition_table2_ext_part_ (like public.partition_table2_1_prt_girls)
EXECUTE 'echo -e "2\n1"' on host
FORMAT 'csv';`)
				testhelper.AssertQueryRuns(connection, `ALTER TABLE public.partition_table2 EXCHANGE PARTITION girls WITH TABLE public.partition_table2_ext_part_ WITHOUT VALIDATION;`)
				defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.partition_table2")
				defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.partition_table2_ext_part_")
				testhelper.AssertQueryRuns(connection, `CREATE TABLE public.partition_table3 (id int, gender char(1))
DISTRIBUTED BY (id)
PARTITION BY LIST (gender)
( PARTITION girls VALUES ('F'),
  PARTITION boys VALUES ('M'),
  DEFAULT PARTITION other );`)
				testhelper.AssertQueryRuns(connection, `CREATE EXTERNAL WEB TABLE public.partition_table3_ext_part_ (like public.partition_table3_1_prt_girls)
EXECUTE 'echo -e "2\n1"' on host
FORMAT 'csv';`)
				testhelper.AssertQueryRuns(connection, `ALTER TABLE public.partition_table3 EXCHANGE PARTITION girls WITH TABLE public.partition_table3_ext_part_ WITHOUT VALIDATION;`)
				defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.partition_table3")
				defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.partition_table3_ext_part_")

				tables := backup.GetAllUserTables(connection)

				expectedTableNames := []string{"public.partition_table1", "public.partition_table1_1_prt_boys", "public.partition_table2", "public.partition_table2_1_prt_girls", "public.partition_table2_1_prt_other"}
				tableNames := make([]string, 0)
				for _, table := range tables {
					tableNames = append(tableNames, table.FQN())
				}
				sort.Strings(tableNames)

				Expect(len(tables)).To(Equal(5))
				Expect(tableNames).To(Equal(expectedTableNames))
			})
		})
		Context("leaf-partition-data flag", func() {
			It("returns only parent partition tables if the leaf-partition-data flag is not set and there are no include tables", func() {
				createStmt := `CREATE TABLE public.rank (id int, rank int, year int, gender
char(1), count int )
DISTRIBUTED BY (id)
PARTITION BY LIST (gender)
( PARTITION girls VALUES ('F'),
  PARTITION boys VALUES ('M'),
  DEFAULT PARTITION other );`
				testhelper.AssertQueryRuns(connection, createStmt)
				defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.rank")

				tables := backup.GetAllUserTables(connection)

				tableRank := backup.Relation{Schema: "public", Name: "rank"}

				Expect(len(tables)).To(Equal(1))
				structmatcher.ExpectStructsToMatchExcluding(&tableRank, &tables[0], "SchemaOid", "Oid")
			})
			It("returns both parent and leaf partition tables if the leaf-partition-data flag is set and there are no include tables", func() {
				backup.SetLeafPartitionData(true)
				createStmt := `CREATE TABLE public.rank (id int, rank int, year int, gender
char(1), count int )
DISTRIBUTED BY (id)
PARTITION BY LIST (gender)
( PARTITION girls VALUES ('F'),
  PARTITION boys VALUES ('M'),
  DEFAULT PARTITION other );`
				testhelper.AssertQueryRuns(connection, createStmt)
				defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.rank")

				tables := backup.GetAllUserTables(connection)

				expectedTableNames := []string{"public.rank", "public.rank_1_prt_boys", "public.rank_1_prt_girls", "public.rank_1_prt_other"}
				tableNames := make([]string, 0)
				for _, table := range tables {
					tableNames = append(tableNames, table.FQN())
				}
				sort.Strings(tableNames)

				Expect(len(tables)).To(Equal(4))
				Expect(tableNames).To(Equal(expectedTableNames))
			})
			It("returns parent and included child partition table if the filter includes a leaf table; with and without leaf-partition-data", func() {
				backup.SetIncludeRelations([]string{"public.rank_1_prt_girls"})
				defer backup.SetIncludeRelations([]string{})
				createStmt := `CREATE TABLE public.rank (id int, rank int, year int, gender
char(1), count int )
DISTRIBUTED BY (id)
PARTITION BY LIST (gender)
( PARTITION girls VALUES ('F'),
  PARTITION boys VALUES ('M'),
  DEFAULT PARTITION other );`
				testhelper.AssertQueryRuns(connection, createStmt)
				defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.rank")

				expectedTableNames := []string{"public.rank", "public.rank_1_prt_girls"}

				tables := backup.GetAllUserTables(connection)
				tableNames := make([]string, 0)
				for _, table := range tables {
					tableNames = append(tableNames, table.FQN())
				}
				sort.Strings(tableNames)

				Expect(len(tables)).To(Equal(2))
				Expect(tableNames).To(Equal(expectedTableNames))

				backup.SetLeafPartitionData(true)
				tables = backup.GetAllUserTables(connection)
				tableNames = make([]string, 0)
				for _, table := range tables {
					tableNames = append(tableNames, table.FQN())
				}
				sort.Strings(tableNames)

				Expect(len(tables)).To(Equal(2))
				Expect(tableNames).To(Equal(expectedTableNames))
			})
			It("returns child partition tables for an included parent table if the leaf-partition-data flag is set and the filter includes a parent partition table", func() {
				backup.SetLeafPartitionData(true)
				backup.SetIncludeRelations([]string{"public.rank"})
				defer backup.SetIncludeRelations([]string{})
				createStmt := `CREATE TABLE public.rank (id int, rank int, year int, gender
char(1), count int )
DISTRIBUTED BY (id)
PARTITION BY LIST (gender)
( PARTITION girls VALUES ('F'),
  PARTITION boys VALUES ('M'),
  DEFAULT PARTITION other );`
				testhelper.AssertQueryRuns(connection, createStmt)
				defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.rank")
				testhelper.AssertQueryRuns(connection, "CREATE TABLE public.test_table(i int)")
				defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.test_table")

				tables := backup.GetAllUserTables(connection)

				expectedTableNames := []string{"public.rank", "public.rank_1_prt_boys", "public.rank_1_prt_girls", "public.rank_1_prt_other"}
				tableNames := make([]string, 0)
				for _, table := range tables {
					tableNames = append(tableNames, table.FQN())
				}
				sort.Strings(tableNames)

				Expect(len(tables)).To(Equal(4))
				Expect(tableNames).To(Equal(expectedTableNames))
			})
		})
		It("returns user table information for table in specific schema", func() {
			testhelper.AssertQueryRuns(connection, "CREATE TABLE public.foo(i int)")
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.foo")
			testhelper.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
			defer testhelper.AssertQueryRuns(connection, "DROP SCHEMA testschema")
			testhelper.AssertQueryRuns(connection, "CREATE TABLE testschema.foo(i int)")
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE testschema.foo")

			backup.SetIncludeSchemas([]string{"testschema"})
			tables := backup.GetAllUserTables(connection)

			tableFoo := backup.Relation{Schema: "testschema", Name: "foo"}

			Expect(len(tables)).To(Equal(1))
			structmatcher.ExpectStructsToMatchExcluding(&tableFoo, &tables[0], "SchemaOid", "Oid")
		})
		It("returns user table information for tables in includeTables", func() {
			testhelper.AssertQueryRuns(connection, "CREATE TABLE public.foo(i int)")
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.foo")
			testhelper.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
			defer testhelper.AssertQueryRuns(connection, "DROP SCHEMA testschema")
			testhelper.AssertQueryRuns(connection, "CREATE TABLE testschema.foo(i int)")
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE testschema.foo")

			backup.SetIncludeRelations([]string{"testschema.foo"})
			tables := backup.GetAllUserTables(connection)

			tableFoo := backup.Relation{Schema: "testschema", Name: "foo"}

			Expect(len(tables)).To(Equal(1))
			structmatcher.ExpectStructsToMatchExcluding(&tableFoo, &tables[0], "SchemaOid", "Oid")
		})
		It("returns user table information for tables not in excludeTables", func() {
			testhelper.AssertQueryRuns(connection, "CREATE TABLE public.foo(i int)")
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.foo")
			testhelper.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
			defer testhelper.AssertQueryRuns(connection, "DROP SCHEMA testschema")
			testhelper.AssertQueryRuns(connection, "CREATE TABLE testschema.foo(i int)")
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE testschema.foo")

			backup.SetExcludeRelations([]string{"testschema.foo"})
			tables := backup.GetAllUserTables(connection)

			tableFoo := backup.Relation{Schema: "public", Name: "foo"}

			Expect(len(tables)).To(Equal(1))
			structmatcher.ExpectStructsToMatchExcluding(&tableFoo, &tables[0], "SchemaOid", "Oid")
		})
		It("returns user table information for tables in includeSchema but not in excludeTables", func() {
			testhelper.AssertQueryRuns(connection, "CREATE TABLE public.foo(i int)")
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.foo")
			testhelper.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
			defer testhelper.AssertQueryRuns(connection, "DROP SCHEMA testschema")
			testhelper.AssertQueryRuns(connection, "CREATE TABLE testschema.foo(i int)")
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE testschema.foo")
			testhelper.AssertQueryRuns(connection, "CREATE TABLE testschema.bar(i int)")
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE testschema.bar")

			backup.SetIncludeSchemas([]string{"testschema"})
			backup.SetExcludeRelations([]string{"testschema.foo"})
			tables := backup.GetAllUserTables(connection)

			tableFoo := backup.Relation{Schema: "testschema", Name: "bar"}
			Expect(len(tables)).To(Equal(1))
			structmatcher.ExpectStructsToMatchExcluding(&tableFoo, &tables[0], "SchemaOid", "Oid")
		})
	})
	Describe("GetPartitionTableMap", func() {
		It("correctly maps oids to parent or leaf table types", func() {
			createStmt := `CREATE TABLE public.summer_sales (id int, year int, month int)
DISTRIBUTED BY (id)
PARTITION BY RANGE (year)
    SUBPARTITION BY RANGE (month)
       SUBPARTITION TEMPLATE (
        START (6) END (8) EVERY (1),
        DEFAULT SUBPARTITION other_months )
( START (2015) END (2017) EVERY (1),
  DEFAULT PARTITION outlying_years );
`
			testhelper.AssertQueryRuns(connection, createStmt)
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.summer_sales")

			parent := testutils.OidFromObjectName(connection, "public", "summer_sales", backup.TYPE_RELATION)
			intermediate1 := testutils.OidFromObjectName(connection, "public", "summer_sales_1_prt_outlying_years", backup.TYPE_RELATION)
			leaf11 := testutils.OidFromObjectName(connection, "public", "summer_sales_1_prt_outlying_years_2_prt_2", backup.TYPE_RELATION)
			leaf12 := testutils.OidFromObjectName(connection, "public", "summer_sales_1_prt_outlying_years_2_prt_3", backup.TYPE_RELATION)
			leaf13 := testutils.OidFromObjectName(connection, "public", "summer_sales_1_prt_outlying_years_2_prt_other_months", backup.TYPE_RELATION)
			intermediate2 := testutils.OidFromObjectName(connection, "public", "summer_sales_1_prt_2", backup.TYPE_RELATION)
			leaf21 := testutils.OidFromObjectName(connection, "public", "summer_sales_1_prt_2_2_prt_2", backup.TYPE_RELATION)
			leaf22 := testutils.OidFromObjectName(connection, "public", "summer_sales_1_prt_2_2_prt_3", backup.TYPE_RELATION)
			leaf23 := testutils.OidFromObjectName(connection, "public", "summer_sales_1_prt_2_2_prt_other_months", backup.TYPE_RELATION)
			intermediate3 := testutils.OidFromObjectName(connection, "public", "summer_sales_1_prt_3", backup.TYPE_RELATION)
			leaf31 := testutils.OidFromObjectName(connection, "public", "summer_sales_1_prt_3_2_prt_2", backup.TYPE_RELATION)
			leaf32 := testutils.OidFromObjectName(connection, "public", "summer_sales_1_prt_3_2_prt_3", backup.TYPE_RELATION)
			leaf33 := testutils.OidFromObjectName(connection, "public", "summer_sales_1_prt_3_2_prt_other_months", backup.TYPE_RELATION)
			partTableMap := backup.GetPartitionTableMap(connection)

			Expect(len(partTableMap)).To(Equal(13))
			Expect(partTableMap[parent]).To(Equal("p"))
			Expect(partTableMap[intermediate1]).To(Equal("i"))
			Expect(partTableMap[intermediate2]).To(Equal("i"))
			Expect(partTableMap[intermediate3]).To(Equal("i"))
			Expect(partTableMap[leaf11]).To(Equal("l"))
			Expect(partTableMap[leaf12]).To(Equal("l"))
			Expect(partTableMap[leaf13]).To(Equal("l"))
			Expect(partTableMap[leaf21]).To(Equal("l"))
			Expect(partTableMap[leaf22]).To(Equal("l"))
			Expect(partTableMap[leaf23]).To(Equal("l"))
			Expect(partTableMap[leaf31]).To(Equal("l"))
			Expect(partTableMap[leaf32]).To(Equal("l"))
			Expect(partTableMap[leaf33]).To(Equal("l"))
		})
	})
	Describe("GetColumnDefinitions", func() {
		emptyColumnACL := []backup.ACL{}
		It("returns table attribute information for a heap table", func() {
			testhelper.AssertQueryRuns(connection, "CREATE TABLE public.atttable(a float, b text, c text NOT NULL, d int DEFAULT(5), e text)")
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.atttable")
			testhelper.AssertQueryRuns(connection, "COMMENT ON COLUMN public.atttable.a IS 'att comment'")
			testhelper.AssertQueryRuns(connection, "ALTER TABLE public.atttable DROP COLUMN b")
			testhelper.AssertQueryRuns(connection, "ALTER TABLE public.atttable ALTER COLUMN e SET STORAGE PLAIN")
			oid := testutils.OidFromObjectName(connection, "public", "atttable", backup.TYPE_RELATION)
			privileges := backup.GetPrivilegesForColumns(connection)
			tableAtts := backup.GetColumnDefinitions(connection, privileges)[oid]

			columnA := backup.ColumnDefinition{Oid: 0, Num: 1, Name: "a", NotNull: false, HasDefault: false, Type: "double precision", Encoding: "", StatTarget: -1, StorageType: "", DefaultVal: "", Comment: "att comment", ACL: emptyColumnACL}
			columnC := backup.ColumnDefinition{Oid: 0, Num: 3, Name: "c", NotNull: true, HasDefault: false, Type: "text", Encoding: "", StatTarget: -1, StorageType: "", DefaultVal: "", Comment: "", ACL: emptyColumnACL}
			columnD := backup.ColumnDefinition{Oid: 0, Num: 4, Name: "d", NotNull: false, HasDefault: true, Type: "integer", Encoding: "", StatTarget: -1, StorageType: "", DefaultVal: "5", Comment: "", ACL: emptyColumnACL}
			columnE := backup.ColumnDefinition{Oid: 0, Num: 5, Name: "e", NotNull: false, HasDefault: false, Type: "text", Encoding: "", StatTarget: -1, StorageType: "PLAIN", DefaultVal: "", Comment: "", ACL: emptyColumnACL}

			Expect(len(tableAtts)).To(Equal(4))

			structmatcher.ExpectStructsToMatchExcluding(&columnA, &tableAtts[0], "Oid")
			structmatcher.ExpectStructsToMatchExcluding(&columnC, &tableAtts[1], "Oid")
			structmatcher.ExpectStructsToMatchExcluding(&columnD, &tableAtts[2], "Oid")
			structmatcher.ExpectStructsToMatchExcluding(&columnE, &tableAtts[3], "Oid")
		})
		It("returns table attributes including encoding for a column oriented table", func() {
			testhelper.AssertQueryRuns(connection, "CREATE TABLE public.co_atttable(a float, b text ENCODING(blocksize=65536)) WITH (appendonly=true, orientation=column)")
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.co_atttable")
			oid := testutils.OidFromObjectName(connection, "public", "co_atttable", backup.TYPE_RELATION)
			privileges := backup.GetPrivilegesForColumns(connection)
			tableAtts := backup.GetColumnDefinitions(connection, privileges)[oid]

			columnA := backup.ColumnDefinition{Oid: 0, Num: 1, Name: "a", NotNull: false, HasDefault: false, Type: "double precision", Encoding: "compresstype=none,blocksize=32768,compresslevel=0", StatTarget: -1, StorageType: "", DefaultVal: "", Comment: "", ACL: emptyColumnACL}
			columnB := backup.ColumnDefinition{Oid: 0, Num: 2, Name: "b", NotNull: false, HasDefault: false, Type: "text", Encoding: "blocksize=65536,compresstype=none,compresslevel=0", StatTarget: -1, StorageType: "", DefaultVal: "", Comment: "", ACL: emptyColumnACL}

			Expect(len(tableAtts)).To(Equal(2))

			structmatcher.ExpectStructsToMatchExcluding(&columnA, &tableAtts[0], "Oid")
			structmatcher.ExpectStructsToMatchExcluding(&columnB, &tableAtts[1], "Oid")
		})
		It("returns an empty attribute array for a table with no columns", func() {
			testhelper.AssertQueryRuns(connection, "CREATE TABLE public.nocol_atttable()")
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.nocol_atttable")
			oid := testutils.OidFromObjectName(connection, "public", "nocol_atttable", backup.TYPE_RELATION)

			privileges := backup.GetPrivilegesForColumns(connection)
			tableAtts := backup.GetColumnDefinitions(connection, privileges)[oid]

			Expect(len(tableAtts)).To(Equal(0))
		})
		It("returns table attributes with per-attribute options", func() {
			testutils.SkipIfBefore6(connection)
			testhelper.AssertQueryRuns(connection, "CREATE TABLE public.atttable(i int)")
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.atttable")
			testhelper.AssertQueryRuns(connection, "ALTER TABLE ONLY public.atttable ALTER COLUMN i SET (n_distinct=1);")
			oid := testutils.OidFromObjectName(connection, "public", "atttable", backup.TYPE_RELATION)
			privileges := backup.GetPrivilegesForColumns(connection)
			tableAtts := backup.GetColumnDefinitions(connection, privileges)[oid]

			columnA := backup.ColumnDefinition{Oid: 0, Num: 1, Name: "i", NotNull: false, HasDefault: false, Type: "integer", Encoding: "", StatTarget: -1, StorageType: "", DefaultVal: "", Comment: "", ACL: emptyColumnACL, Options: "n_distinct=1"}

			Expect(len(tableAtts)).To(Equal(1))

			structmatcher.ExpectStructsToMatchExcluding(&columnA, &tableAtts[0], "Oid")
		})
	})
	Describe("GetPrivilegesForColumns", func() {
		It("Default column", func() {
			testutils.SkipIfBefore6(connection)
			testhelper.AssertQueryRuns(connection, "CREATE TABLE public.default_privileges(i int)")
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.default_privileges")

			metadataMap := backup.GetPrivilegesForColumns(connection)

			oid := testutils.OidFromObjectName(connection, "public", "default_privileges", backup.TYPE_RELATION)
			expectedACL := []backup.ACL{}
			Expect(len(metadataMap)).To(Equal(1))
			Expect(len(metadataMap[oid])).To(Equal(1))
			Expect(metadataMap[oid]["i"]).To(Equal(expectedACL))
		})
		It("Column with granted privileges", func() {
			testutils.SkipIfBefore6(connection)
			testhelper.AssertQueryRuns(connection, "CREATE TABLE public.granted_privileges(i int)")
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.granted_privileges")
			testhelper.AssertQueryRuns(connection, "GRANT SELECT (i) ON TABLE public.granted_privileges TO testrole")

			metadataMap := backup.GetPrivilegesForColumns(connection)

			oid := testutils.OidFromObjectName(connection, "public", "granted_privileges", backup.TYPE_RELATION)
			expectedACL := []backup.ACL{{Grantee: "testrole", Select: true}}
			Expect(len(metadataMap)).To(Equal(1))
			Expect(len(metadataMap[oid])).To(Equal(1))
			Expect(metadataMap[oid]["i"]).To(Equal(expectedACL))
		})
	})
	Describe("GetDistributionPolicies", func() {
		It("returns distribution policy info for a table DISTRIBUTED RANDOMLY", func() {
			testhelper.AssertQueryRuns(connection, "CREATE TABLE public.dist_random(a int, b text) DISTRIBUTED RANDOMLY")
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.dist_random")
			oid := testutils.OidFromObjectName(connection, "public", "dist_random", backup.TYPE_RELATION)

			distPolicies := backup.GetDistributionPolicies(connection)[oid]

			Expect(distPolicies).To(Equal("DISTRIBUTED RANDOMLY"))
		})
		It("returns distribution policy info for a table DISTRIBUTED BY one column", func() {
			testhelper.AssertQueryRuns(connection, "CREATE TABLE public.dist_one(a int, b text) DISTRIBUTED BY (a)")
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.dist_one")
			oid := testutils.OidFromObjectName(connection, "public", "dist_one", backup.TYPE_RELATION)

			distPolicies := backup.GetDistributionPolicies(connection)[oid]

			Expect(distPolicies).To(Equal("DISTRIBUTED BY (a)"))
		})
		It("returns distribution policy info for a table DISTRIBUTED BY two columns", func() {
			testhelper.AssertQueryRuns(connection, "CREATE TABLE public.dist_two(a int, b text) DISTRIBUTED BY (a, b)")
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.dist_two")
			oid := testutils.OidFromObjectName(connection, "public", "dist_two", backup.TYPE_RELATION)

			distPolicies := backup.GetDistributionPolicies(connection)[oid]

			Expect(distPolicies).To(Equal("DISTRIBUTED BY (a, b)"))
		})
		It("returns distribution policy info for a table DISTRIBUTED BY column name as keyword", func() {
			testhelper.AssertQueryRuns(connection, `CREATE TABLE public.dist_one(a int, "group" text) DISTRIBUTED BY ("group")`)
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.dist_one")
			oid := testutils.OidFromObjectName(connection, "public", "dist_one", backup.TYPE_RELATION)

			distPolicies := backup.GetDistributionPolicies(connection)[oid]

			Expect(distPolicies).To(Equal(`DISTRIBUTED BY ("group")`))
		})
		It("returns distribution policy info for a table DISTRIBUTED REPLICATED", func() {
			testutils.SkipIfBefore6(connection)
			testhelper.AssertQueryRuns(connection, `CREATE TABLE public.dist_one(a int, "group" text) DISTRIBUTED REPLICATED`)
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.dist_one")
			oid := testutils.OidFromObjectName(connection, "public", "dist_one", backup.TYPE_RELATION)

			distPolicies := backup.GetDistributionPolicies(connection)[oid]

			Expect(distPolicies).To(Equal(`DISTRIBUTED REPLICATED`))
		})
	})
	Describe("GetPartitionDefinitions", func() {
		It("returns empty string when no partition exists", func() {
			testhelper.AssertQueryRuns(connection, "CREATE TABLE public.simple_table(i int)")
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.simple_table")
			oid := testutils.OidFromObjectName(connection, "public", "simple_table", backup.TYPE_RELATION)

			result := backup.GetPartitionDefinitions(connection)[oid]

			Expect(result).To(Equal(""))
		})
		It("returns a value for a partition definition", func() {
			testhelper.AssertQueryRuns(connection, `CREATE TABLE public.part_table (id int, rank int, year int, gender
char(1), count int )
DISTRIBUTED BY (id)
PARTITION BY LIST (gender)
( PARTITION girls VALUES ('F'),
  PARTITION boys VALUES ('M'),
  DEFAULT PARTITION other );
			`)
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.part_table")
			oid := testutils.OidFromObjectName(connection, "public", "part_table", backup.TYPE_RELATION)

			result := backup.GetPartitionDefinitions(connection)[oid]

			// The spacing is very specific here and is output from the postgres function
			expectedResult := `PARTITION BY LIST(gender) 
          (
          PARTITION girls VALUES('F') WITH (tablename='part_table_1_prt_girls', appendonly=false ), 
          PARTITION boys VALUES('M') WITH (tablename='part_table_1_prt_boys', appendonly=false ), 
          DEFAULT PARTITION other  WITH (tablename='part_table_1_prt_other', appendonly=false )
          )`
			Expect(result).To(Equal(expectedResult))
		})
		It("returns a value for a partition definition for a specific table", func() {
			testhelper.AssertQueryRuns(connection, `CREATE TABLE public.part_table (id int, rank int, year int, gender
char(1), count int )
DISTRIBUTED BY (id)
PARTITION BY LIST (gender)
( PARTITION girls VALUES ('F'),
  PARTITION boys VALUES ('M'),
  DEFAULT PARTITION other );
			`)
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.part_table")
			testhelper.AssertQueryRuns(connection, `CREATE TABLE public.part_table2 (id int, rank int, year int, gender
char(1), count int )
DISTRIBUTED BY (id)
PARTITION BY LIST (gender)
( PARTITION girls VALUES ('F'),
  PARTITION boys VALUES ('M'),
  DEFAULT PARTITION other );
			`)
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.part_table2")
			oid := testutils.OidFromObjectName(connection, "public", "part_table", backup.TYPE_RELATION)

			backup.SetIncludeRelations([]string{"public.part_table"})

			results := backup.GetPartitionDefinitions(connection)
			Expect(len(results)).To(Equal(1))
			result := results[oid]

			// The spacing is very specific here and is output from the postgres function
			expectedResult := `PARTITION BY LIST(gender) 
          (
          PARTITION girls VALUES('F') WITH (tablename='part_table_1_prt_girls', appendonly=false ), 
          PARTITION boys VALUES('M') WITH (tablename='part_table_1_prt_boys', appendonly=false ), 
          DEFAULT PARTITION other  WITH (tablename='part_table_1_prt_other', appendonly=false )
          )`
			Expect(result).To(Equal(expectedResult))
		})
		It("returns a value for a partition definition in a specific schema", func() {
			testhelper.AssertQueryRuns(connection, `CREATE TABLE public.part_table (id int, rank int, year int, gender
char(1), count int )
DISTRIBUTED BY (id)
PARTITION BY LIST (gender)
( PARTITION girls VALUES ('F'),
  PARTITION boys VALUES ('M'),
  DEFAULT PARTITION other );
			`)
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.part_table")
			testhelper.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
			defer testhelper.AssertQueryRuns(connection, "DROP SCHEMA testschema CASCADE")
			testhelper.AssertQueryRuns(connection, `CREATE TABLE testschema.part_table (id int, rank int, year int, gender
char(1), count int )
DISTRIBUTED BY (id)
PARTITION BY LIST (gender)
( PARTITION girls VALUES ('F'),
  PARTITION boys VALUES ('M'),
  DEFAULT PARTITION other );
			`)
			oid := testutils.OidFromObjectName(connection, "testschema", "part_table", backup.TYPE_RELATION)

			backup.SetIncludeSchemas([]string{"testschema"})

			results := backup.GetPartitionDefinitions(connection)
			Expect(len(results)).To(Equal(1))
			result := results[oid]

			// The spacing is very specific here and is output from the postgres function
			expectedResult := `PARTITION BY LIST(gender) 
          (
          PARTITION girls VALUES('F') WITH (tablename='part_table_1_prt_girls', appendonly=false ), 
          PARTITION boys VALUES('M') WITH (tablename='part_table_1_prt_boys', appendonly=false ), 
          DEFAULT PARTITION other  WITH (tablename='part_table_1_prt_other', appendonly=false )
          )`
			Expect(result).To(Equal(expectedResult))
		})
	})
	Describe("GetPartitionTemplates", func() {
		It("returns empty string when no partition definition template exists", func() {
			testhelper.AssertQueryRuns(connection, "CREATE TABLE public.simple_table(i int)")
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.simple_table")
			oid := testutils.OidFromObjectName(connection, "public", "simple_table", backup.TYPE_RELATION)

			result := backup.GetPartitionTemplates(connection)[oid]

			Expect(result).To(Equal(""))
		})
		It("returns a value for a subpartition template", func() {
			testhelper.AssertQueryRuns(connection, `CREATE TABLE public.part_table (trans_id int, date date, amount decimal(9,2), region text)
  DISTRIBUTED BY (trans_id)
  PARTITION BY RANGE (date)
  SUBPARTITION BY LIST (region)
  SUBPARTITION TEMPLATE
    ( SUBPARTITION usa VALUES ('usa'),
      SUBPARTITION asia VALUES ('asia'),
      SUBPARTITION europe VALUES ('europe'),
      DEFAULT SUBPARTITION other_regions )
  ( START (date '2014-01-01') INCLUSIVE
    END (date '2014-04-01') EXCLUSIVE
    EVERY (INTERVAL '1 month') ) `)
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.part_table")
			oid := testutils.OidFromObjectName(connection, "public", "part_table", backup.TYPE_RELATION)

			result := backup.GetPartitionTemplates(connection)[oid]

			// The spacing is very specific here and is output from the postgres function
			expectedResult := `ALTER TABLE public.part_table 
SET SUBPARTITION TEMPLATE  
          (
          SUBPARTITION usa VALUES('usa') WITH (tablename='part_table'), 
          SUBPARTITION asia VALUES('asia') WITH (tablename='part_table'), 
          SUBPARTITION europe VALUES('europe') WITH (tablename='part_table'), 
          DEFAULT SUBPARTITION other_regions  WITH (tablename='part_table')
          )
`

			Expect(result).To(Equal(expectedResult))
		})
		It("returns a value for a subpartition template for a specific table", func() {
			testhelper.AssertQueryRuns(connection, `CREATE TABLE public.part_table (trans_id int, date date, amount decimal(9,2), region text)
  DISTRIBUTED BY (trans_id)
  PARTITION BY RANGE (date)
  SUBPARTITION BY LIST (region)
  SUBPARTITION TEMPLATE
    ( SUBPARTITION usa VALUES ('usa'),
      SUBPARTITION asia VALUES ('asia'),
      SUBPARTITION europe VALUES ('europe'),
      DEFAULT SUBPARTITION other_regions )
  ( START (date '2014-01-01') INCLUSIVE
    END (date '2014-04-01') EXCLUSIVE
    EVERY (INTERVAL '1 month') ) `)
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.part_table")
			testhelper.AssertQueryRuns(connection, `CREATE TABLE public.part_table2 (trans_id int, date date, amount decimal(9,2), region text)
  DISTRIBUTED BY (trans_id)
  PARTITION BY RANGE (date)
  SUBPARTITION BY LIST (region)
  SUBPARTITION TEMPLATE
    ( SUBPARTITION usa VALUES ('usa'),
      SUBPARTITION asia VALUES ('asia'),
      SUBPARTITION europe VALUES ('europe'),
      DEFAULT SUBPARTITION other_regions )
  ( START (date '2014-01-01') INCLUSIVE
    END (date '2014-04-01') EXCLUSIVE
    EVERY (INTERVAL '1 month') ) `)
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.part_table2")
			oid := testutils.OidFromObjectName(connection, "public", "part_table", backup.TYPE_RELATION)

			backup.SetIncludeRelations([]string{"public.part_table"})

			results := backup.GetPartitionTemplates(connection)
			Expect(len(results)).To(Equal(1))
			result := results[oid]

			// The spacing is very specific here and is output from the postgres function
			expectedResult := `ALTER TABLE public.part_table 
SET SUBPARTITION TEMPLATE  
          (
          SUBPARTITION usa VALUES('usa') WITH (tablename='part_table'), 
          SUBPARTITION asia VALUES('asia') WITH (tablename='part_table'), 
          SUBPARTITION europe VALUES('europe') WITH (tablename='part_table'), 
          DEFAULT SUBPARTITION other_regions  WITH (tablename='part_table')
          )
`

			Expect(result).To(Equal(expectedResult))
		})
		It("returns a value for a subpartition template in a specific schema", func() {
			testhelper.AssertQueryRuns(connection, `CREATE TABLE public.part_table (trans_id int, date date, amount decimal(9,2), region text)
  DISTRIBUTED BY (trans_id)
  PARTITION BY RANGE (date)
  SUBPARTITION BY LIST (region)
  SUBPARTITION TEMPLATE
    ( SUBPARTITION usa VALUES ('usa'),
      SUBPARTITION asia VALUES ('asia'),
      SUBPARTITION europe VALUES ('europe'),
      DEFAULT SUBPARTITION other_regions )
  ( START (date '2014-01-01') INCLUSIVE
    END (date '2014-04-01') EXCLUSIVE
    EVERY (INTERVAL '1 month') ) `)
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.part_table")
			testhelper.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
			defer testhelper.AssertQueryRuns(connection, "DROP SCHEMA testschema CASCADE")
			testhelper.AssertQueryRuns(connection, `CREATE TABLE testschema.part_table (trans_id int, date date, amount decimal(9,2), region text)
  DISTRIBUTED BY (trans_id)
  PARTITION BY RANGE (date)
  SUBPARTITION BY LIST (region)
  SUBPARTITION TEMPLATE
    ( SUBPARTITION usa VALUES ('usa'),
      SUBPARTITION asia VALUES ('asia'),
      SUBPARTITION europe VALUES ('europe'),
      DEFAULT SUBPARTITION other_regions )
  ( START (date '2014-01-01') INCLUSIVE
    END (date '2014-04-01') EXCLUSIVE
    EVERY (INTERVAL '1 month') ) `)
			oid := testutils.OidFromObjectName(connection, "testschema", "part_table", backup.TYPE_RELATION)

			backup.SetIncludeSchemas([]string{"testschema"})

			results := backup.GetPartitionTemplates(connection)
			Expect(len(results)).To(Equal(1))
			result := results[oid]

			// The spacing is very specific here and is output from the postgres function
			expectedResult := `ALTER TABLE testschema.part_table 
SET SUBPARTITION TEMPLATE  
          (
          SUBPARTITION usa VALUES('usa') WITH (tablename='part_table'), 
          SUBPARTITION asia VALUES('asia') WITH (tablename='part_table'), 
          SUBPARTITION europe VALUES('europe') WITH (tablename='part_table'), 
          DEFAULT SUBPARTITION other_regions  WITH (tablename='part_table')
          )
`

			Expect(result).To(Equal(expectedResult))
		})
	})
	Describe("GetTableType", func() {
		It("Returns a map when a table OF type exists", func() {
			testutils.SkipIfBefore6(connection)
			testhelper.AssertQueryRuns(connection, "CREATE TYPE public.some_type AS (a text, b numeric)")
			defer testhelper.AssertQueryRuns(connection, "DROP TYPE public.some_type")
			testhelper.AssertQueryRuns(connection, "CREATE TABLE public.some_table OF public.some_type (PRIMARY KEY (a), b WITH OPTIONS DEFAULT 1000)")
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.some_table")
			oid := testutils.OidFromObjectName(connection, "public", "some_table", backup.TYPE_RELATION)

			result := backup.GetTableType(connection)
			Expect(len(result)).To(Equal(1))

			Expect(result[oid]).To(Equal("public.some_type"))
		})
		It("Returns empty map when no tables OF type exist", func() {
			testhelper.AssertQueryRuns(connection, "CREATE TABLE public.some_table (i int, j int)")
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.some_table")

			result := backup.GetTableType(connection)
			Expect(len(result)).To(Equal(0))
		})
	})

	Describe("GetUnloggedTables", func() {
		It("Returns a map when an UNLOGGED table exists", func() {
			testutils.SkipIfBefore6(connection)
			testhelper.AssertQueryRuns(connection, "CREATE UNLOGGED TABLE public.some_table(i int)")
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.some_table")
			oid := testutils.OidFromObjectName(connection, "public", "some_table", backup.TYPE_RELATION)

			result := backup.GetUnloggedTables(connection)
			Expect(len(result)).To(Equal(1))

			Expect(result[oid]).To(BeTrue())
		})
		It("Returns empty map when no UNLOGGED tables exist", func() {
			testhelper.AssertQueryRuns(connection, "CREATE TABLE public.some_table (i int, j int)")
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.some_table")

			result := backup.GetUnloggedTables(connection)
			Expect(len(result)).To(Equal(0))
		})
	})

	Describe("GetTableStorageOptions", func() {
		It("returns an empty string when no table storage options exist ", func() {
			testhelper.AssertQueryRuns(connection, "CREATE TABLE public.simple_table(i int)")
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.simple_table")
			oid := testutils.OidFromObjectName(connection, "public", "simple_table", backup.TYPE_RELATION)

			result := backup.GetTableStorageOptions(connection)[oid]

			Expect(result).To(Equal(""))
		})
		It("returns a value for storage options of a table ", func() {
			testhelper.AssertQueryRuns(connection, "CREATE TABLE public.ao_table(i int) with (appendonly=true)")
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.ao_table")
			oid := testutils.OidFromObjectName(connection, "public", "ao_table", backup.TYPE_RELATION)

			result := backup.GetTableStorageOptions(connection)[oid]

			Expect(result).To(Equal("appendonly=true"))
		})
	})
	Describe("GetAllSequenceRelations", func() {
		It("returns a slice of all sequences", func() {
			testhelper.AssertQueryRuns(connection, "CREATE SEQUENCE public.my_sequence START 10")
			defer testhelper.AssertQueryRuns(connection, "DROP SEQUENCE public.my_sequence")
			testhelper.AssertQueryRuns(connection, "COMMENT ON SEQUENCE public.my_sequence IS 'this is a sequence comment'")

			testhelper.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
			defer testhelper.AssertQueryRuns(connection, "DROP SCHEMA testschema CASCADE")
			testhelper.AssertQueryRuns(connection, "CREATE SEQUENCE testschema.my_sequence2")

			sequences := backup.GetAllSequenceRelations(connection)

			mySequence := backup.Relation{Schema: "public", Name: "my_sequence"}
			mySequence2 := backup.Relation{Schema: "testschema", Name: "my_sequence2"}

			Expect(len(sequences)).To(Equal(2))
			structmatcher.ExpectStructsToMatchExcluding(&mySequence, &sequences[0], "SchemaOid", "Oid")
			structmatcher.ExpectStructsToMatchExcluding(&mySequence2, &sequences[1], "SchemaOid", "Oid")
		})
		It("returns a slice of all sequences in a specific schema", func() {
			testhelper.AssertQueryRuns(connection, "CREATE SEQUENCE public.my_sequence START 10")
			defer testhelper.AssertQueryRuns(connection, "DROP SEQUENCE public.my_sequence")

			testhelper.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
			defer testhelper.AssertQueryRuns(connection, "DROP SCHEMA testschema CASCADE")
			testhelper.AssertQueryRuns(connection, "CREATE SEQUENCE testschema.my_sequence")
			mySequence := backup.Relation{Schema: "testschema", Name: "my_sequence"}

			backup.SetIncludeSchemas([]string{"testschema"})
			sequences := backup.GetAllSequenceRelations(connection)

			Expect(len(sequences)).To(Equal(1))
			structmatcher.ExpectStructsToMatchExcluding(&mySequence, &sequences[0], "SchemaOid", "Oid")
		})
		It("does not return sequences owned by included tables", func() {
			testhelper.AssertQueryRuns(connection, "CREATE SEQUENCE public.my_sequence START 10")

			testhelper.AssertQueryRuns(connection, "CREATE TABLE public.seq_table(i int)")
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.seq_table")
			testhelper.AssertQueryRuns(connection, "ALTER SEQUENCE public.my_sequence OWNED BY public.seq_table.i")

			backup.SetIncludeRelations([]string{"public.seq_table"})
			sequences := backup.GetAllSequenceRelations(connection)

			Expect(len(sequences)).To(Equal(0))
		})
		It("returns sequences owned by excluded tables if the sequence is not excluded", func() {
			testhelper.AssertQueryRuns(connection, "CREATE SEQUENCE public.my_sequence START 10")

			testhelper.AssertQueryRuns(connection, "CREATE TABLE public.seq_table(i int)")
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.seq_table")
			testhelper.AssertQueryRuns(connection, "ALTER SEQUENCE public.my_sequence OWNED BY public.seq_table.i")
			mySequence := backup.Relation{Schema: "public", Name: "my_sequence"}

			backup.SetExcludeRelations([]string{"public.seq_table"})
			sequences := backup.GetAllSequenceRelations(connection)

			Expect(len(sequences)).To(Equal(1))
			structmatcher.ExpectStructsToMatchExcluding(&mySequence, &sequences[0], "SchemaOid", "Oid")
		})
		It("does not return an excluded sequence", func() {
			testhelper.AssertQueryRuns(connection, "CREATE SEQUENCE public.sequence1 START 10")
			defer testhelper.AssertQueryRuns(connection, "DROP SEQUENCE public.sequence1")
			testhelper.AssertQueryRuns(connection, "CREATE SEQUENCE public.sequence2 START 10")
			defer testhelper.AssertQueryRuns(connection, "DROP SEQUENCE public.sequence2")

			sequence2 := backup.Relation{Schema: "public", Name: "sequence2"}

			backup.SetExcludeRelations([]string{"public.sequence1"})
			sequences := backup.GetAllSequenceRelations(connection)

			Expect(len(sequences)).To(Equal(1))
			structmatcher.ExpectStructsToMatchExcluding(&sequence2, &sequences[0], "SchemaOid", "Oid")
		})
		It("returns only the included sequence", func() {
			testhelper.AssertQueryRuns(connection, "CREATE SEQUENCE public.sequence1 START 10")
			defer testhelper.AssertQueryRuns(connection, "DROP SEQUENCE public.sequence1")
			testhelper.AssertQueryRuns(connection, "CREATE SEQUENCE public.sequence2 START 10")
			defer testhelper.AssertQueryRuns(connection, "DROP SEQUENCE public.sequence2")

			sequence1 := backup.Relation{Schema: "public", Name: "sequence1"}

			backup.SetIncludeRelations([]string{"public.sequence1"})
			sequences := backup.GetAllSequenceRelations(connection)

			Expect(len(sequences)).To(Equal(1))
			structmatcher.ExpectStructsToMatchExcluding(&sequence1, &sequences[0], "SchemaOid", "Oid")
		})
	})
	Describe("GetSequenceDefinition", func() {
		It("returns sequence information for sequence with default values", func() {
			testhelper.AssertQueryRuns(connection, "CREATE SEQUENCE public.my_sequence")
			defer testhelper.AssertQueryRuns(connection, "DROP SEQUENCE public.my_sequence")

			resultSequenceDef := backup.GetSequenceDefinition(connection, "public.my_sequence")

			expectedSequence := backup.SequenceDefinition{Name: "my_sequence", LastVal: 1, Increment: 1, MaxVal: math.MaxInt64, MinVal: 1, CacheVal: 1}
			if connection.Version.Before("5") {
				expectedSequence.LogCnt = 1 // In GPDB 4.3, sequence log count is one-indexed
			}
			if connection.Version.AtLeast("6") {
				expectedSequence.StartVal = 1
			}

			structmatcher.ExpectStructsToMatch(&expectedSequence, &resultSequenceDef)
		})
		It("returns sequence information for a complex sequence", func() {
			testhelper.AssertQueryRuns(connection, "CREATE TABLE public.with_sequence(a int, b char(20))")
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.with_sequence")
			testhelper.AssertQueryRuns(connection,
				"CREATE SEQUENCE public.my_sequence INCREMENT BY 5 MINVALUE 20 MAXVALUE 1000 START 100 OWNED BY public.with_sequence.a")
			defer testhelper.AssertQueryRuns(connection, "DROP SEQUENCE public.my_sequence")
			testhelper.AssertQueryRuns(connection, "INSERT INTO public.with_sequence VALUES (nextval('public.my_sequence'), 'acme')")
			testhelper.AssertQueryRuns(connection, "INSERT INTO public.with_sequence VALUES (nextval('public.my_sequence'), 'beta')")

			resultSequenceDef := backup.GetSequenceDefinition(connection, "public.my_sequence")

			expectedSequence := backup.SequenceDefinition{Name: "my_sequence", LastVal: 105, Increment: 5, MaxVal: 1000, MinVal: 20, CacheVal: 1, IsCycled: false, IsCalled: true}
			if connection.Version.Before("5") {
				expectedSequence.LogCnt = 32 // In GPDB 4.3, sequence log count is one-indexed
			} else {
				expectedSequence.LogCnt = 31 // In GPDB 5, sequence log count is zero-indexed
			}
			if connection.Version.AtLeast("6") {
				expectedSequence.StartVal = 100
			}

			structmatcher.ExpectStructsToMatch(&expectedSequence, &resultSequenceDef)
		})
	})
	Describe("GetSequenceOwnerMap", func() {
		It("returns sequence information for sequences owned by columns", func() {
			testhelper.AssertQueryRuns(connection, "CREATE TABLE public.without_sequence(a int, b char(20));")
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.without_sequence")
			testhelper.AssertQueryRuns(connection, "CREATE TABLE public.with_sequence(a int, b char(20));")
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.with_sequence")
			testhelper.AssertQueryRuns(connection, "CREATE SEQUENCE public.my_sequence OWNED BY public.with_sequence.a;")
			defer testhelper.AssertQueryRuns(connection, "DROP SEQUENCE public.my_sequence")

			sequenceOwnerTables, sequenceOwnerColumns := backup.GetSequenceColumnOwnerMap(connection)

			Expect(len(sequenceOwnerTables)).To(Equal(1))
			Expect(len(sequenceOwnerColumns)).To(Equal(1))
			Expect(sequenceOwnerTables["public.my_sequence"]).To(Equal("public.with_sequence"))
			Expect(sequenceOwnerColumns["public.my_sequence"]).To(Equal("public.with_sequence.a"))
		})
		It("does not return sequence owner columns if the owning table is not backed up", func() {
			testhelper.AssertQueryRuns(connection, "CREATE TABLE public.my_table(a int, b char(20));")
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.my_table")
			testhelper.AssertQueryRuns(connection, "CREATE SEQUENCE public.my_sequence OWNED BY public.my_table.a;")
			defer testhelper.AssertQueryRuns(connection, "DROP SEQUENCE public.my_sequence")

			backup.SetExcludeRelations([]string{"public.my_table"})
			sequenceOwnerTables, sequenceOwnerColumns := backup.GetSequenceColumnOwnerMap(connection)
			Expect(len(sequenceOwnerTables)).To(Equal(0))
			Expect(len(sequenceOwnerColumns)).To(Equal(0))

			backup.SetExcludeRelations([]string{})
			backup.SetIncludeRelations([]string{"public.my_sequence"})
			sequenceOwnerTables, sequenceOwnerColumns = backup.GetSequenceColumnOwnerMap(connection)
			Expect(len(sequenceOwnerTables)).To(Equal(0))
			Expect(len(sequenceOwnerColumns)).To(Equal(0))

			backup.SetIncludeRelations([]string{"public.my_sequence", "public.my_table"})
			sequenceOwnerTables, sequenceOwnerColumns = backup.GetSequenceColumnOwnerMap(connection)
			Expect(len(sequenceOwnerTables)).To(Equal(1))
			Expect(len(sequenceOwnerColumns)).To(Equal(1))

			backup.SetIncludeRelations([]string{"public.my_table"})
			sequenceOwnerTables, sequenceOwnerColumns = backup.GetSequenceColumnOwnerMap(connection)
			Expect(len(sequenceOwnerTables)).To(Equal(1))
			Expect(len(sequenceOwnerColumns)).To(Equal(1))
		})
	})
	Describe("GetAllSequences", func() {
		It("returns a slice of definitions for all sequences", func() {
			testhelper.AssertQueryRuns(connection, "CREATE SEQUENCE public.seq_one START 3")
			defer testhelper.AssertQueryRuns(connection, "DROP SEQUENCE public.seq_one")
			testhelper.AssertQueryRuns(connection, "COMMENT ON SEQUENCE public.seq_one IS 'this is a sequence comment'")
			startValOne := int64(0)
			startValTwo := int64(0)
			if connection.Version.AtLeast("6") {
				startValOne = 3
				startValTwo = 7
			}

			testhelper.AssertQueryRuns(connection, "CREATE SEQUENCE public.seq_two START 7")
			defer testhelper.AssertQueryRuns(connection, "DROP SEQUENCE public.seq_two")

			seqOneRelation := backup.Relation{Schema: "public", Name: "seq_one"}

			seqOneDef := backup.SequenceDefinition{Name: "seq_one", LastVal: 3, Increment: 1, MaxVal: math.MaxInt64, MinVal: 1, CacheVal: 1, StartVal: startValOne}
			seqTwoRelation := backup.Relation{Schema: "public", Name: "seq_two"}
			seqTwoDef := backup.SequenceDefinition{Name: "seq_two", LastVal: 7, Increment: 1, MaxVal: math.MaxInt64, MinVal: 1, CacheVal: 1, StartVal: startValTwo}
			if connection.Version.Before("5") {
				seqOneDef.LogCnt = 1 // In GPDB 4.3, sequence log count is one-indexed
				seqTwoDef.LogCnt = 1
			}

			results := backup.GetAllSequences(connection, map[string]string{})

			structmatcher.ExpectStructsToMatchExcluding(&seqOneRelation, &results[0].Relation, "SchemaOid", "Oid")
			structmatcher.ExpectStructsToMatchExcluding(&seqOneDef, &results[0].SequenceDefinition)
			structmatcher.ExpectStructsToMatchExcluding(&seqTwoRelation, &results[1].Relation, "SchemaOid", "Oid")
			structmatcher.ExpectStructsToMatchExcluding(&seqTwoDef, &results[1].SequenceDefinition)
		})
	})
	Describe("GetViews", func() {
		It("returns a slice for a basic view", func() {
			testhelper.AssertQueryRuns(connection, "CREATE VIEW public.simpleview AS SELECT rolname FROM pg_roles")
			defer testhelper.AssertQueryRuns(connection, "DROP VIEW public.simpleview")

			results := backup.GetViews(connection)

			viewDef := backup.View{Oid: 1, Schema: "public", Name: "simpleview", Definition: "SELECT pg_roles.rolname FROM pg_roles;", DependsUpon: nil}

			Expect(len(results)).To(Equal(1))
			structmatcher.ExpectStructsToMatchExcluding(&viewDef, &results[0], "Oid")
		})
		It("returns a slice for view in a specific schema", func() {
			testhelper.AssertQueryRuns(connection, "CREATE VIEW public.simpleview AS SELECT rolname FROM pg_roles")
			defer testhelper.AssertQueryRuns(connection, "DROP VIEW public.simpleview")
			testhelper.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
			defer testhelper.AssertQueryRuns(connection, "DROP SCHEMA testschema")
			testhelper.AssertQueryRuns(connection, "CREATE VIEW testschema.simpleview AS SELECT rolname FROM pg_roles")
			defer testhelper.AssertQueryRuns(connection, "DROP VIEW testschema.simpleview")
			backup.SetIncludeSchemas([]string{"testschema"})

			results := backup.GetViews(connection)

			viewDef := backup.View{Oid: 1, Schema: "testschema", Name: "simpleview", Definition: "SELECT pg_roles.rolname FROM pg_roles;", DependsUpon: nil}

			Expect(len(results)).To(Equal(1))
			structmatcher.ExpectStructsToMatchExcluding(&viewDef, &results[0], "Oid")
		})
	})
	Describe("ConstructTableDependencies", func() {
		child := backup.Relation{Schema: "public", Name: "child"}
		childOne := backup.Relation{Schema: "public", Name: "child_one"}
		childTwo := backup.Relation{Schema: "public", Name: "child_two"}

		tableDefs := map[uint32]backup.TableDefinition{}
		It("constructs dependencies correctly if there is one table dependent on one table", func() {
			testhelper.AssertQueryRuns(connection, "CREATE TABLE public.parent(i int)")
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.parent")
			testhelper.AssertQueryRuns(connection, "CREATE TABLE public.child() INHERITS (public.parent)")
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.child")

			child.Oid = testutils.OidFromObjectName(connection, "public", "child", backup.TYPE_RELATION)
			tables := []backup.Relation{child}

			tables = backup.ConstructTableDependencies(connection, tables, tableDefs, false)

			Expect(len(tables)).To(Equal(1))
			Expect(len(tables[0].DependsUpon)).To(Equal(1))
			Expect(tables[0].DependsUpon[0]).To(Equal("public.parent"))
			Expect(len(tables[0].Inherits)).To(Equal(1))
			Expect(tables[0].Inherits[0]).To(Equal("public.parent"))
		})
		It("constructs dependencies correctly if there are two tables dependent on one table", func() {
			testhelper.AssertQueryRuns(connection, "CREATE TABLE public.parent(i int)")
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.parent")
			testhelper.AssertQueryRuns(connection, "CREATE TABLE public.child_one() INHERITS (public.parent)")
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.child_one")
			testhelper.AssertQueryRuns(connection, "CREATE TABLE public.child_two() INHERITS (public.parent)")
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.child_two")

			childOne.Oid = testutils.OidFromObjectName(connection, "public", "child_one", backup.TYPE_RELATION)
			childTwo.Oid = testutils.OidFromObjectName(connection, "public", "child_two", backup.TYPE_RELATION)
			tables := []backup.Relation{childOne, childTwo}

			tables = backup.ConstructTableDependencies(connection, tables, tableDefs, false)

			Expect(len(tables)).To(Equal(2))
			Expect(len(tables[0].DependsUpon)).To(Equal(1))
			Expect(tables[0].DependsUpon[0]).To(Equal("public.parent"))
			Expect(len(tables[0].Inherits)).To(Equal(1))
			Expect(tables[0].Inherits[0]).To(Equal("public.parent"))
			Expect(len(tables[1].DependsUpon)).To(Equal(1))
			Expect(tables[1].DependsUpon[0]).To(Equal("public.parent"))
			Expect(len(tables[1].Inherits)).To(Equal(1))
			Expect(tables[1].Inherits[0]).To(Equal("public.parent"))
		})
		It("constructs dependencies correctly if there is one table dependent on two tables", func() {
			testhelper.AssertQueryRuns(connection, "CREATE TABLE public.parent_one(i int)")
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.parent_one")
			testhelper.AssertQueryRuns(connection, "CREATE TABLE public.parent_two(j int)")
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.parent_two")
			testhelper.AssertQueryRuns(connection, "CREATE TABLE public.child() INHERITS (public.parent_one, public.parent_two)")
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.child")

			child.Oid = testutils.OidFromObjectName(connection, "public", "child", backup.TYPE_RELATION)
			tables := []backup.Relation{child}

			tables = backup.ConstructTableDependencies(connection, tables, tableDefs, false)

			sort.Strings(tables[0].DependsUpon)
			sort.Strings(tables[0].Inherits)
			Expect(len(tables)).To(Equal(1))
			Expect(len(tables[0].DependsUpon)).To(Equal(2))
			Expect(tables[0].DependsUpon[0]).To(Equal("public.parent_one"))
			Expect(tables[0].DependsUpon[1]).To(Equal("public.parent_two"))
			Expect(len(tables[0].Inherits)).To(Equal(2))
			Expect(tables[0].Inherits[0]).To(Equal("public.parent_one"))
			Expect(tables[0].Inherits[1]).To(Equal("public.parent_two"))
		})
		It("constructs dependencies correctly if there are no table dependencies", func() {
			tables := []backup.Relation{}
			tables = backup.ConstructTableDependencies(connection, tables, tableDefs, false)
			Expect(len(tables)).To(Equal(0))
		})
		It("constructs dependencies correctly if there are no table dependencies while filtering", func() {
			tables := []backup.Relation{}
			tables = backup.ConstructTableDependencies(connection, tables, tableDefs, true)
			Expect(len(tables)).To(Equal(0))
		})
		It("constructs dependencies correctly if there are two dependent tables but one is not in the backup set", func() {
			testhelper.AssertQueryRuns(connection, "CREATE TABLE public.parent(i int)")
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.parent")
			testhelper.AssertQueryRuns(connection, "CREATE TABLE public.child_one() INHERITS (public.parent)")
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.child_one")
			testhelper.AssertQueryRuns(connection, "CREATE TABLE public.child_two() INHERITS (public.parent)")
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.child_two")

			childOne.Oid = testutils.OidFromObjectName(connection, "public", "child_one", backup.TYPE_RELATION)
			tables := []backup.Relation{childOne}

			tables = backup.ConstructTableDependencies(connection, tables, tableDefs, true)

			Expect(len(tables)).To(Equal(1))
			Expect(len(tables[0].DependsUpon)).To(Equal(0))
			Expect(len(tables[0].Inherits)).To(Equal(1))
			Expect(tables[0].Inherits[0]).To(Equal("public.parent"))
		})
		It("does not record a dependency of an external leaf partition on a parent table", func() {
			testhelper.AssertQueryRuns(connection, `CREATE TABLE public.partition_table (id int, gender char(1))
DISTRIBUTED BY (id)
PARTITION BY LIST (gender)
( PARTITION girls VALUES ('F'),
  PARTITION boys VALUES ('M'),
  DEFAULT PARTITION other );`)
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.partition_table")
			testhelper.AssertQueryRuns(connection, `CREATE EXTERNAL WEB TABLE public.partition_table_ext_part_ (like public.partition_table_1_prt_girls)
EXECUTE 'echo -e "2\n1"' on host
FORMAT 'csv';`)
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.partition_table_ext_part_")
			testhelper.AssertQueryRuns(connection, `ALTER TABLE public.partition_table EXCHANGE PARTITION girls WITH TABLE public.partition_table_ext_part_ WITHOUT VALIDATION;`)

			partition := backup.Relation{Schema: "public", Name: "partition_table_ext_part_"}

			partition.Oid = testutils.OidFromObjectName(connection, "public", "partition_table_ext_part_", backup.TYPE_RELATION)
			tables := []backup.Relation{partition}
			partTableDefs := map[uint32]backup.TableDefinition{partition.Oid: {IsExternal: true, PartitionType: "l"}}

			tables = backup.ConstructTableDependencies(connection, tables, partTableDefs, false)

			Expect(len(tables)).To(Equal(1))
			Expect(len(tables[0].DependsUpon)).To(Equal(0))
			Expect(len(tables[0].Inherits)).To(Equal(0))
		})
		It("constructs dependencies correctly if there is one table dependent on one protocol", func() {
			testhelper.AssertQueryRuns(connection, `CREATE FUNCTION read_from_s3() RETURNS integer
    AS '$libdir/gps3ext.so', 's3_import'
    LANGUAGE c STABLE NO SQL;`)
			defer testhelper.AssertQueryRuns(connection, "DROP FUNCTION read_from_s3()")
			testhelper.AssertQueryRuns(connection, `CREATE PROTOCOL s3 (readfunc = 'read_from_s3'); `)
			defer testhelper.AssertQueryRuns(connection, "DROP PROTOCOL s3")
			testhelper.AssertQueryRuns(connection, `CREATE EXTERNAL TABLE public.ext_tbl (
	 i int
 ) LOCATION (
	 's3://192.168.0.1'
 ) ON ALL
 FORMAT 'csv' (delimiter E',' null E'' escape E'"' quote E'"')
 OPTIONS ()
 ENCODING 'UTF8';`)

			defer testhelper.AssertQueryRuns(connection, "DROP EXTERNAL TABLE public.ext_tbl")
			child.Oid = testutils.OidFromObjectName(connection, "public", "ext_tbl", backup.TYPE_RELATION)
			tables := []backup.Relation{child}

			tables = backup.ConstructTableDependencies(connection, tables, tableDefs, false)

			Expect(len(tables)).To(Equal(1))
			Expect(len(tables[0].DependsUpon)).To(Equal(1))
			Expect(tables[0].DependsUpon[0]).To(Equal("s3"))
			Expect(len(tables[0].Inherits)).To(Equal(0))
		})
	})
	Describe("ConstructViewDependencies", func() {
		It("constructs dependencies correctly for a view that depends on two other views", func() {
			testhelper.AssertQueryRuns(connection, "CREATE VIEW public.parent1 AS SELECT relname FROM pg_class")
			defer testhelper.AssertQueryRuns(connection, "DROP VIEW public.parent1")
			testhelper.AssertQueryRuns(connection, "CREATE VIEW public.parent2 AS SELECT relname FROM pg_class")
			defer testhelper.AssertQueryRuns(connection, "DROP VIEW public.parent2")
			testhelper.AssertQueryRuns(connection, "CREATE VIEW public.child AS (SELECT * FROM public.parent1 UNION SELECT * FROM public.parent2)")
			defer testhelper.AssertQueryRuns(connection, "DROP VIEW public.child")

			childView := backup.View{}
			childView.Oid = testutils.OidFromObjectName(connection, "public", "child", backup.TYPE_RELATION)
			views := []backup.View{childView}

			views = backup.ConstructViewDependencies(connection, views)

			Expect(len(views)).To(Equal(1))
			Expect(len(views[0].DependsUpon)).To(Equal(2))
			Expect(views[0].DependsUpon[0]).To(Equal("public.parent1"))
			Expect(views[0].DependsUpon[1]).To(Equal("public.parent2"))
		})
	})
})
