package integration

import (
	"database/sql"
	"fmt"

	"github.com/greenplum-db/gp-common-go-libs/structmatcher"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/options"
	"github.com/greenplum-db/gpbackup/testutils"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup integration tests", func() {
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
			testhelper.AssertQueryRuns(connectionPool, createStmt)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.summer_sales")

			parent := testutils.OidFromObjectName(connectionPool, "public", "summer_sales", backup.TYPE_RELATION)
			intermediate1 := testutils.OidFromObjectName(connectionPool, "public", "summer_sales_1_prt_outlying_years", backup.TYPE_RELATION)
			leaf11 := testutils.OidFromObjectName(connectionPool, "public", "summer_sales_1_prt_outlying_years_2_prt_2", backup.TYPE_RELATION)
			leaf12 := testutils.OidFromObjectName(connectionPool, "public", "summer_sales_1_prt_outlying_years_2_prt_3", backup.TYPE_RELATION)
			leaf13 := testutils.OidFromObjectName(connectionPool, "public", "summer_sales_1_prt_outlying_years_2_prt_other_months", backup.TYPE_RELATION)
			intermediate2 := testutils.OidFromObjectName(connectionPool, "public", "summer_sales_1_prt_2", backup.TYPE_RELATION)
			leaf21 := testutils.OidFromObjectName(connectionPool, "public", "summer_sales_1_prt_2_2_prt_2", backup.TYPE_RELATION)
			leaf22 := testutils.OidFromObjectName(connectionPool, "public", "summer_sales_1_prt_2_2_prt_3", backup.TYPE_RELATION)
			leaf23 := testutils.OidFromObjectName(connectionPool, "public", "summer_sales_1_prt_2_2_prt_other_months", backup.TYPE_RELATION)
			intermediate3 := testutils.OidFromObjectName(connectionPool, "public", "summer_sales_1_prt_3", backup.TYPE_RELATION)
			leaf31 := testutils.OidFromObjectName(connectionPool, "public", "summer_sales_1_prt_3_2_prt_2", backup.TYPE_RELATION)
			leaf32 := testutils.OidFromObjectName(connectionPool, "public", "summer_sales_1_prt_3_2_prt_3", backup.TYPE_RELATION)
			leaf33 := testutils.OidFromObjectName(connectionPool, "public", "summer_sales_1_prt_3_2_prt_other_months", backup.TYPE_RELATION)
			partTableMap := backup.GetPartitionTableMap(connectionPool)

			Expect(partTableMap).To(HaveLen(13))
			structmatcher.ExpectStructsToMatch(partTableMap[parent], &backup.PartitionLevelInfo{Oid: parent, Level: "p", RootName: ""})
			structmatcher.ExpectStructsToMatch(partTableMap[intermediate1], &backup.PartitionLevelInfo{Oid: intermediate1, Level: "i", RootName: "summer_sales"})
			structmatcher.ExpectStructsToMatch(partTableMap[intermediate2], &backup.PartitionLevelInfo{Oid: intermediate2, Level: "i", RootName: "summer_sales"})
			structmatcher.ExpectStructsToMatch(partTableMap[intermediate3], &backup.PartitionLevelInfo{Oid: intermediate3, Level: "i", RootName: "summer_sales"})
			structmatcher.ExpectStructsToMatch(partTableMap[leaf11], &backup.PartitionLevelInfo{Oid: leaf11, Level: "l", RootName: "summer_sales"})
			structmatcher.ExpectStructsToMatch(partTableMap[leaf12], &backup.PartitionLevelInfo{Oid: leaf12, Level: "l", RootName: "summer_sales"})
			structmatcher.ExpectStructsToMatch(partTableMap[leaf13], &backup.PartitionLevelInfo{Oid: leaf13, Level: "l", RootName: "summer_sales"})
			structmatcher.ExpectStructsToMatch(partTableMap[leaf21], &backup.PartitionLevelInfo{Oid: leaf21, Level: "l", RootName: "summer_sales"})
			structmatcher.ExpectStructsToMatch(partTableMap[leaf22], &backup.PartitionLevelInfo{Oid: leaf22, Level: "l", RootName: "summer_sales"})
			structmatcher.ExpectStructsToMatch(partTableMap[leaf23], &backup.PartitionLevelInfo{Oid: leaf23, Level: "l", RootName: "summer_sales"})
			structmatcher.ExpectStructsToMatch(partTableMap[leaf31], &backup.PartitionLevelInfo{Oid: leaf31, Level: "l", RootName: "summer_sales"})
			structmatcher.ExpectStructsToMatch(partTableMap[leaf32], &backup.PartitionLevelInfo{Oid: leaf32, Level: "l", RootName: "summer_sales"})
			structmatcher.ExpectStructsToMatch(partTableMap[leaf33], &backup.PartitionLevelInfo{Oid: leaf33, Level: "l", RootName: "summer_sales"})
		})
	})
	Describe("GetColumnDefinitions", func() {
		It("returns table attribute information for a heap table", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.atttable(a float, b text, c text NOT NULL, d int DEFAULT(5), e text)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.atttable")
			testhelper.AssertQueryRuns(connectionPool, "COMMENT ON COLUMN public.atttable.a IS 'att comment'")
			testhelper.AssertQueryRuns(connectionPool, "ALTER TABLE public.atttable DROP COLUMN b")
			testhelper.AssertQueryRuns(connectionPool, "ALTER TABLE public.atttable ALTER COLUMN e SET STORAGE PLAIN")
			oid := testutils.OidFromObjectName(connectionPool, "public", "atttable", backup.TYPE_RELATION)
			privileges := sql.NullString{String: "", Valid: false}
			if connectionPool.Version.AtLeast("6") {
				testhelper.AssertQueryRuns(connectionPool, "GRANT SELECT (c, d) ON TABLE public.atttable TO testrole")
				privileges = sql.NullString{String: "testrole=r/testrole", Valid: true}
			}
			tableAtts := backup.GetColumnDefinitions(connectionPool)[oid]

			columnA := backup.ColumnDefinition{Oid: 0, Num: 1, Name: "a", NotNull: false, HasDefault: false, Type: "double precision", Encoding: "", StatTarget: -1, StorageType: "", DefaultVal: "", Comment: "att comment"}
			columnC := backup.ColumnDefinition{Oid: 0, Num: 3, Name: "c", NotNull: true, HasDefault: false, Type: "text", Encoding: "", StatTarget: -1, StorageType: "", DefaultVal: "", Comment: "", Privileges: privileges}
			columnD := backup.ColumnDefinition{Oid: 0, Num: 4, Name: "d", NotNull: false, HasDefault: true, Type: "integer", Encoding: "", StatTarget: -1, StorageType: "", DefaultVal: "(5)", Comment: "", Privileges: privileges}
			columnE := backup.ColumnDefinition{Oid: 0, Num: 5, Name: "e", NotNull: false, HasDefault: false, Type: "text", Encoding: "", StatTarget: -1, StorageType: "PLAIN", DefaultVal: "", Comment: ""}

			Expect(tableAtts).To(HaveLen(4))

			structmatcher.ExpectStructsToMatchExcluding(&columnA, &tableAtts[0], "Oid")
			structmatcher.ExpectStructsToMatchExcluding(&columnC, &tableAtts[1], "Oid")
			structmatcher.ExpectStructsToMatchExcluding(&columnD, &tableAtts[2], "Oid")
			structmatcher.ExpectStructsToMatchExcluding(&columnE, &tableAtts[3], "Oid")
		})
		It("returns table attributes including encoding for a column oriented table", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.co_atttable(a float, b text ENCODING(blocksize=65536)) WITH (appendonly=true, orientation=column)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.co_atttable")
			oid := testutils.OidFromObjectName(connectionPool, "public", "co_atttable", backup.TYPE_RELATION)
			tableAtts := backup.GetColumnDefinitions(connectionPool)[oid]

			columnA := backup.ColumnDefinition{Oid: 0, Num: 1, Name: "a", NotNull: false, HasDefault: false, Type: "double precision", Encoding: "compresstype=none,blocksize=32768,compresslevel=0", StatTarget: -1, StorageType: "", DefaultVal: "", Comment: ""}
			columnB := backup.ColumnDefinition{Oid: 0, Num: 2, Name: "b", NotNull: false, HasDefault: false, Type: "text", Encoding: "blocksize=65536,compresstype=none,compresslevel=0", StatTarget: -1, StorageType: "", DefaultVal: "", Comment: ""}

			Expect(tableAtts).To(HaveLen(2))

			structmatcher.ExpectStructsToMatchExcluding(&columnA, &tableAtts[0], "Oid")
			structmatcher.ExpectStructsToMatchExcluding(&columnB, &tableAtts[1], "Oid")
		})
		It("returns an empty attribute array for a table with no columns", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.nocol_atttable()")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.nocol_atttable")
			oid := testutils.OidFromObjectName(connectionPool, "public", "nocol_atttable", backup.TYPE_RELATION)

			tableAtts := backup.GetColumnDefinitions(connectionPool)[oid]

			Expect(tableAtts).To(BeEmpty())
		})
		It("returns table attributes with options only applicable to coordinator", func() {
			testutils.SkipIfBefore6(connectionPool)
			testhelper.AssertQueryRuns(connectionPool, "CREATE COLLATION public.some_coll (lc_collate = 'POSIX', lc_ctype = 'POSIX')")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP COLLATION public.some_coll")
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.atttable(i character(8) COLLATE public.some_coll)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.atttable")
			testhelper.AssertQueryRuns(connectionPool, "ALTER TABLE ONLY public.atttable ALTER COLUMN i SET (n_distinct=1);")
			testhelper.AssertQueryRuns(connectionPool, "SECURITY LABEL FOR dummy ON COLUMN public.atttable.i IS 'unclassified';")
			oid := testutils.OidFromObjectName(connectionPool, "public", "atttable", backup.TYPE_RELATION)
			tableAtts := backup.GetColumnDefinitions(connectionPool)[oid]

			columnA := backup.ColumnDefinition{Oid: 0, Num: 1, Name: "i", NotNull: false, HasDefault: false, Type: "character(8)", Encoding: "", StatTarget: -1, StorageType: "", DefaultVal: "", Comment: "", Options: "n_distinct=1", Collation: "public.some_coll", SecurityLabelProvider: "dummy", SecurityLabel: "unclassified"}

			Expect(tableAtts).To(HaveLen(1))

			structmatcher.ExpectStructsToMatchExcluding(&columnA, &tableAtts[0], "Oid")
		})
		It("returns table attributes with foreign data options", func() {
			testutils.SkipIfBefore6(connectionPool)
			testhelper.AssertQueryRuns(connectionPool, "CREATE FOREIGN DATA WRAPPER dummy;")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP FOREIGN DATA WRAPPER dummy")
			testhelper.AssertQueryRuns(connectionPool, "CREATE SERVER sc FOREIGN DATA WRAPPER dummy;")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SERVER sc")
			testhelper.AssertQueryRuns(connectionPool, `CREATE FOREIGN TABLE public.ft1 (
	c1 integer OPTIONS (param1 'val1', param2 'val2') NOT NULL
) SERVER sc ;`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP FOREIGN TABLE public.ft1")

			oid := testutils.OidFromObjectName(connectionPool, "public", "ft1", backup.TYPE_RELATION)
			tableAtts := backup.GetColumnDefinitions(connectionPool)[oid]

			Expect(tableAtts).To(HaveLen(1))
			column1 := backup.ColumnDefinition{Oid: 0, Num: 1, Name: "c1", NotNull: true, HasDefault: false, Type: "integer", StatTarget: -1, FdwOptions: "param1 'val1', param2 'val2'"}
			structmatcher.ExpectStructsToMatchExcluding(column1, &tableAtts[0], "Oid")
		})
		It("handles table with gin index, when index's attribute's collname and namespace are null", func() {
			testutils.SkipIfBefore6(connectionPool)

			testhelper.AssertQueryRuns(connectionPool, `
CREATE TABLE public.test_tsvector (
    t text,
    a tsvector,
    distkey integer
) DISTRIBUTED BY (distkey)
`)
			testhelper.AssertQueryRuns(connectionPool, `CREATE INDEX wowidx ON public.test_tsvector USING gin (a)`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP table public.test_tsvector cascade")

			oid := testutils.OidFromObjectName(connectionPool, "public", "test_tsvector", backup.TYPE_RELATION)
			tableAtts := backup.GetColumnDefinitions(connectionPool)[oid]

			Expect(tableAtts).To(HaveLen(3))
		})
		It("gets the correct collation schema", func() {
			testutils.SkipIfBefore6(connectionPool)

			testhelper.AssertQueryRuns(connectionPool, `CREATE SCHEMA myschema;`)
			defer testhelper.AssertQueryRuns(connectionPool, `DROP SCHEMA myschema`)
			testhelper.AssertQueryRuns(connectionPool, `CREATE COLLATION myschema.mycoll (lc_collate = 'C', lc_ctype = 'C')`)
			defer testhelper.AssertQueryRuns(connectionPool, `DROP COLLATION myschema.mycoll`)
			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE public.foo(i text COLLATE myschema.mycoll)`)
			defer testhelper.AssertQueryRuns(connectionPool, `DROP TABLE public.foo`)

			oid := testutils.OidFromObjectName(connectionPool, "public", "foo", backup.TYPE_RELATION)
			tableAtts := backup.GetColumnDefinitions(connectionPool)[oid]

			Expect(tableAtts).To(HaveLen(1))
			Expect(tableAtts[0].Collation).To(Equal("myschema.mycoll"))
		})
	})
	Describe("GetDistributionPolicies", func() {
		It("returns distribution policy info for a table DISTRIBUTED RANDOMLY", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.dist_random(a int, b text) DISTRIBUTED RANDOMLY")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.dist_random")
			oid := testutils.OidFromObjectName(connectionPool, "public", "dist_random", backup.TYPE_RELATION)

			distPolicies := backup.GetDistributionPolicies(connectionPool)[oid]

			Expect(distPolicies).To(Equal("DISTRIBUTED RANDOMLY"))
		})
		It("returns distribution policy info for a table DISTRIBUTED BY one column", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.dist_one(a int, b text) DISTRIBUTED BY (a)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.dist_one")
			oid := testutils.OidFromObjectName(connectionPool, "public", "dist_one", backup.TYPE_RELATION)

			distPolicies := backup.GetDistributionPolicies(connectionPool)[oid]

			Expect(distPolicies).To(Equal("DISTRIBUTED BY (a)"))
		})
		It("returns distribution policy info for a table DISTRIBUTED BY two columns", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.dist_two(a int, b text) DISTRIBUTED BY (a, b)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.dist_two")
			oid := testutils.OidFromObjectName(connectionPool, "public", "dist_two", backup.TYPE_RELATION)

			distPolicies := backup.GetDistributionPolicies(connectionPool)[oid]

			Expect(distPolicies).To(Equal("DISTRIBUTED BY (a, b)"))
		})
		It("returns distribution policy info for a table DISTRIBUTED BY a custom operator", func() {
			testutils.SkipIfBefore6(connectionPool)
			testhelper.AssertQueryRuns(connectionPool, `
				CREATE OPERATOR FAMILY public.abs_int_hash_ops USING hash;

				CREATE FUNCTION public.abseq(integer, integer) RETURNS boolean
				    LANGUAGE plpgsql IMMUTABLE STRICT NO SQL
				    AS $_$
				  begin return abs($1) = abs($2); end;
				$_$;

				CREATE OPERATOR public.|=| (
				    PROCEDURE = public.abseq,
				    LEFTARG = integer,
				    RIGHTARG = integer,
				    COMMUTATOR = OPERATOR(public.|=|),
				    MERGES,
				    HASHES
				);
				CREATE FUNCTION public.abshashfunc(integer) RETURNS integer
				    LANGUAGE plpgsql IMMUTABLE STRICT NO SQL
				    AS $_$
				  begin return abs($1); end;
				$_$;


				-- operator class dec
				CREATE OPERATOR CLASS public.abs_int_hash_ops
				    FOR TYPE integer USING hash FAMILY public.abs_int_hash_ops AS
				    OPERATOR 1 public.|=|(integer,integer) ,
				    FUNCTION 1 (integer, integer) public.abshashfunc(integer);


				-- table ft object
				CREATE TABLE public.abs_opclass_test (
				    i integer,
				    t text,
				    j integer
				) DISTRIBUTED BY (i public.abs_int_hash_ops, t, j public.abs_int_hash_ops);
			`)

			defer testhelper.AssertQueryRuns(connectionPool, `
				DROP TABLE IF EXISTS public.abs_opclass_test;
				DROP OPERATOR CLASS IF EXISTS public.abs_int_hash_ops USING hash;
				DROP FUNCTION IF EXISTS public.abshashfunc(integer);
				DROP OPERATOR IF EXISTS public.|=|(integer, integer);
				DROP FUNCTION IF EXISTS public.abseq(integer, integer);
				DROP OPERATOR FAMILY IF EXISTS public.abs_int_hash_ops USING hash;
			`)

			oid := testutils.OidFromObjectName(connectionPool, "public", "abs_opclass_test", backup.TYPE_RELATION)

			distPolicies := backup.GetDistributionPolicies(connectionPool)[oid]

			Expect(distPolicies).To(Equal("DISTRIBUTED BY (i public.abs_int_hash_ops, t, j public.abs_int_hash_ops)"))
		})
		It("returns distribution policy info for a table DISTRIBUTED BY column name as keyword", func() {
			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE public.dist_one(a int, "group" text) DISTRIBUTED BY ("group")`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.dist_one")
			oid := testutils.OidFromObjectName(connectionPool, "public", "dist_one", backup.TYPE_RELATION)

			distPolicies := backup.GetDistributionPolicies(connectionPool)[oid]

			Expect(distPolicies).To(Equal(`DISTRIBUTED BY ("group")`))
		})
		It("returns distribution policy info for a table DISTRIBUTED BY multiple columns in correct order", func() {
			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE public.dist_one (id int, memo varchar(20), dt date, col1 varchar(20)) DISTRIBUTED BY (memo, dt, id, col1); `)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.dist_one")
			oid := testutils.OidFromObjectName(connectionPool, "public", "dist_one", backup.TYPE_RELATION)

			distPolicies := backup.GetDistributionPolicies(connectionPool)[oid]

			Expect(distPolicies).To(Equal(`DISTRIBUTED BY (memo, dt, id, col1)`))
		})
		It("returns distribution policy info for a table DISTRIBUTED REPLICATED", func() {
			testutils.SkipIfBefore6(connectionPool)
			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE public.dist_one(a int, "group" text) DISTRIBUTED REPLICATED`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.dist_one")
			oid := testutils.OidFromObjectName(connectionPool, "public", "dist_one", backup.TYPE_RELATION)

			distPolicies := backup.GetDistributionPolicies(connectionPool)[oid]

			Expect(distPolicies).To(Equal(`DISTRIBUTED REPLICATED`))
		})
	})
	Describe("GetPartitionDefinitions", func() {
		var partitionPartFalseExpectation = "false "
		BeforeEach(func() {
			// GPDB 7+ does not have pg_get_partition_def()
			if connectionPool.Version.AtLeast("7") {
				Skip("Test is not applicable to GPDB 7+")
			}

			if connectionPool.Version.AtLeast("6") {
				partitionPartFalseExpectation = "'false'"
			}
		})
		It("returns empty string when no partition exists", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.simple_table(i int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.simple_table")
			oid := testutils.OidFromObjectName(connectionPool, "public", "simple_table", backup.TYPE_RELATION)

			result, _ := backup.GetPartitionDetails(connectionPool)

			Expect(result[oid]).To(Equal(""))
		})
		It("returns a value for a partition definition", func() {
			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE public.part_table (id int, rank int, year int, gender
char(1), count int )
DISTRIBUTED BY (id)
PARTITION BY LIST (gender)
( PARTITION girls VALUES ('F'),
  PARTITION boys VALUES ('M'),
  DEFAULT PARTITION other );
			`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.part_table")
			oid := testutils.OidFromObjectName(connectionPool, "public", "part_table", backup.TYPE_RELATION)

			result, _ := backup.GetPartitionDetails(connectionPool)

			// The spacing is very specific here and is output from the postgres function
			expectedResult := fmt.Sprintf(`PARTITION BY LIST(gender) `+`
          (
          PARTITION girls VALUES('F') WITH (tablename='part_table_1_prt_girls', appendonly=%[1]s), `+`
          PARTITION boys VALUES('M') WITH (tablename='part_table_1_prt_boys', appendonly=%[1]s), `+`
          DEFAULT PARTITION other  WITH (tablename='part_table_1_prt_other', appendonly=%[1]s)
          )`, partitionPartFalseExpectation)
			Expect(result[oid]).To(Equal(expectedResult))
		})
		PIt("returns a value for a partition definition for a table with a negative partition value", func() {
			// Pend test until this fix makes it into an RC
			// this test exercises a corner case bug that was fixed in GPDB6:
			// https://github.com/greenplum-db/gpdb/pull/13330
			testutils.SkipIfBefore6(connectionPool)
			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE public.part_table (id int)
DISTRIBUTED BY (id)
PARTITION BY LIST (id)
( PARTITION negative VALUES (-1, 1),
  PARTITION zero VALUES (0),
  DEFAULT PARTITION other );
			`)
			oid := testutils.OidFromObjectName(connectionPool, "public", "part_table", backup.TYPE_RELATION)

			_ = backupCmdFlags.Set(options.INCLUDE_RELATION, "public.part_table")

			results, _ := backup.GetPartitionDetails(connectionPool)
			Expect(results).To(HaveLen(1))
			result := results[oid]

			testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.part_table")

			// ensure that the partition definition generated is valid SQL
			newTableQuery := fmt.Sprintf(`CREATE TABLE public.part_table2 (id int)
DISTRIBUTED BY (id)
%s`, result)
			testhelper.AssertQueryRuns(connectionPool, newTableQuery)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.part_table2")
		})
		It("returns a value for a partition definition for a specific table", func() {
			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE public.part_table (id int, rank int, year int, gender
char(1), count int )
DISTRIBUTED BY (id)
PARTITION BY LIST (gender)
( PARTITION girls VALUES ('F'),
  PARTITION boys VALUES ('M'),
  DEFAULT PARTITION other );
			`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.part_table")
			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE public.part_table2 (id int, rank int, year int, gender
char(1), count int )
DISTRIBUTED BY (id)
PARTITION BY LIST (gender)
( PARTITION girls VALUES ('F'),
  PARTITION boys VALUES ('M'),
  DEFAULT PARTITION other );
			`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.part_table2")
			oid := testutils.OidFromObjectName(connectionPool, "public", "part_table", backup.TYPE_RELATION)

			_ = backupCmdFlags.Set(options.INCLUDE_RELATION, "public.part_table")

			results, _ := backup.GetPartitionDetails(connectionPool)
			Expect(results).To(HaveLen(1))
			result := results[oid]

			// The spacing is very specific here and is output from the postgres function
			expectedResult := fmt.Sprintf(`PARTITION BY LIST(gender) `+`
          (
          PARTITION girls VALUES('F') WITH (tablename='part_table_1_prt_girls', appendonly=%[1]s), `+`
          PARTITION boys VALUES('M') WITH (tablename='part_table_1_prt_boys', appendonly=%[1]s), `+`
          DEFAULT PARTITION other  WITH (tablename='part_table_1_prt_other', appendonly=%[1]s)
          )`, partitionPartFalseExpectation)
			Expect(result).To(Equal(expectedResult))
		})
		It("returns a value for a partition definition in a specific schema", func() {
			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE public.part_table (id int, rank int, year int, gender
char(1), count int )
DISTRIBUTED BY (id)
PARTITION BY LIST (gender)
( PARTITION girls VALUES ('F'),
  PARTITION boys VALUES ('M'),
  DEFAULT PARTITION other );
			`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.part_table")
			testhelper.AssertQueryRuns(connectionPool, "CREATE SCHEMA testschema")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA testschema CASCADE")
			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE testschema.part_table (id int, rank int, year int, gender
char(1), count int )
DISTRIBUTED BY (id)
PARTITION BY LIST (gender)
( PARTITION girls VALUES ('F'),
  PARTITION boys VALUES ('M'),
  DEFAULT PARTITION other );
			`)
			oid := testutils.OidFromObjectName(connectionPool, "testschema", "part_table", backup.TYPE_RELATION)

			_ = backupCmdFlags.Set(options.INCLUDE_SCHEMA, "testschema")

			results, _ := backup.GetPartitionDetails(connectionPool)
			Expect(results).To(HaveLen(1))
			result := results[oid]

			// The spacing is very specific here and is output from the postgres function
			expectedResult := fmt.Sprintf(`PARTITION BY LIST(gender) `+`
          (
          PARTITION girls VALUES('F') WITH (tablename='part_table_1_prt_girls', appendonly=%[1]s), `+`
          PARTITION boys VALUES('M') WITH (tablename='part_table_1_prt_boys', appendonly=%[1]s), `+`
          DEFAULT PARTITION other  WITH (tablename='part_table_1_prt_other', appendonly=%[1]s)
          )`, partitionPartFalseExpectation)
			Expect(result).To(Equal(expectedResult))
		})
	})
	Describe("GetPartitionTemplates", func() {
		BeforeEach(func() {
			// GPDB 7+ does not have pg_get_partition_template_def()
			if connectionPool.Version.AtLeast("7") {
				Skip("Test is not applicable to GPDB 7+")
			}
		})
		It("returns empty string when no partition definition template exists", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.simple_table(i int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.simple_table")
			oid := testutils.OidFromObjectName(connectionPool, "public", "simple_table", backup.TYPE_RELATION)

			_, result := backup.GetPartitionDetails(connectionPool)

			Expect(result[oid]).To(Equal(""))
		})
		It("returns a value for a subpartition template", func() {
			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE public.part_table (trans_id int, date date, amount decimal(9,2), region text)
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
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.part_table")
			oid := testutils.OidFromObjectName(connectionPool, "public", "part_table", backup.TYPE_RELATION)

			_, result := backup.GetPartitionDetails(connectionPool)

			/*
			 * The spacing is very specific here and is output from the postgres function
			 * The only difference between the below statements is spacing
			 */
			expectedResult := ""
			if connectionPool.Version.Before("6") {
				expectedResult = `ALTER TABLE public.part_table 
SET SUBPARTITION TEMPLATE  
          (
          SUBPARTITION usa VALUES('usa') WITH (tablename='part_table'), 
          SUBPARTITION asia VALUES('asia') WITH (tablename='part_table'), 
          SUBPARTITION europe VALUES('europe') WITH (tablename='part_table'), 
          DEFAULT SUBPARTITION other_regions  WITH (tablename='part_table')
          )
`
			} else {
				expectedResult = `ALTER TABLE public.part_table 
SET SUBPARTITION TEMPLATE 
          (
          SUBPARTITION usa VALUES('usa') WITH (tablename='part_table'), 
          SUBPARTITION asia VALUES('asia') WITH (tablename='part_table'), 
          SUBPARTITION europe VALUES('europe') WITH (tablename='part_table'), 
          DEFAULT SUBPARTITION other_regions  WITH (tablename='part_table')
          )
`
			}

			Expect(result[oid]).To(Equal(expectedResult))
		})
		It("returns a value for a subpartition template for a specific table", func() {
			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE public.part_table (trans_id int, date date, amount decimal(9,2), region text)
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
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.part_table")
			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE public.part_table2 (trans_id int, date date, amount decimal(9,2), region text)
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
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.part_table2")
			oid := testutils.OidFromObjectName(connectionPool, "public", "part_table", backup.TYPE_RELATION)

			_ = backupCmdFlags.Set(options.INCLUDE_RELATION, "public.part_table")

			_, results := backup.GetPartitionDetails(connectionPool)
			Expect(results).To(HaveLen(1))
			result := results[oid]

			/*
			 * The spacing is very specific here and is output from the postgres function
			 * The only difference between the below statements is spacing
			 */
			expectedResult := ""
			if connectionPool.Version.Before("6") {
				expectedResult = `ALTER TABLE public.part_table 
SET SUBPARTITION TEMPLATE  
          (
          SUBPARTITION usa VALUES('usa') WITH (tablename='part_table'), 
          SUBPARTITION asia VALUES('asia') WITH (tablename='part_table'), 
          SUBPARTITION europe VALUES('europe') WITH (tablename='part_table'), 
          DEFAULT SUBPARTITION other_regions  WITH (tablename='part_table')
          )
`
			} else {
				expectedResult = `ALTER TABLE public.part_table 
SET SUBPARTITION TEMPLATE 
          (
          SUBPARTITION usa VALUES('usa') WITH (tablename='part_table'), 
          SUBPARTITION asia VALUES('asia') WITH (tablename='part_table'), 
          SUBPARTITION europe VALUES('europe') WITH (tablename='part_table'), 
          DEFAULT SUBPARTITION other_regions  WITH (tablename='part_table')
          )
`
			}
			Expect(result).To(Equal(expectedResult))

		})
		It("returns a value for a subpartition template in a specific schema", func() {
			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE public.part_table (trans_id int, date date, amount decimal(9,2), region text)
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
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.part_table")
			testhelper.AssertQueryRuns(connectionPool, "CREATE SCHEMA testschema")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA testschema CASCADE")
			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE testschema.part_table (trans_id int, date date, amount decimal(9,2), region text)
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
			oid := testutils.OidFromObjectName(connectionPool, "testschema", "part_table", backup.TYPE_RELATION)

			_ = backupCmdFlags.Set(options.INCLUDE_SCHEMA, "testschema")

			_, results := backup.GetPartitionDetails(connectionPool)
			Expect(results).To(HaveLen(1))
			result := results[oid]

			/*
			 * The spacing is very specific here and is output from the postgres function
			 * The only difference between the below statements is spacing
			 */
			expectedResult := ""
			if connectionPool.Version.Before("6") {
				expectedResult = `ALTER TABLE testschema.part_table 
SET SUBPARTITION TEMPLATE  
          (
          SUBPARTITION usa VALUES('usa') WITH (tablename='part_table'), 
          SUBPARTITION asia VALUES('asia') WITH (tablename='part_table'), 
          SUBPARTITION europe VALUES('europe') WITH (tablename='part_table'), 
          DEFAULT SUBPARTITION other_regions  WITH (tablename='part_table')
          )
`
			} else {
				expectedResult = `ALTER TABLE testschema.part_table 
SET SUBPARTITION TEMPLATE 
          (
          SUBPARTITION usa VALUES('usa') WITH (tablename='part_table'), 
          SUBPARTITION asia VALUES('asia') WITH (tablename='part_table'), 
          SUBPARTITION europe VALUES('europe') WITH (tablename='part_table'), 
          DEFAULT SUBPARTITION other_regions  WITH (tablename='part_table')
          )
`
			}

			Expect(result).To(Equal(expectedResult))
		})
	})
	Describe("GetTableType", func() {
		It("Returns a map when a table OF type exists", func() {
			testutils.SkipIfBefore6(connectionPool)
			testhelper.AssertQueryRuns(connectionPool, "CREATE TYPE public.some_type AS (a text, b numeric)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TYPE public.some_type")
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.some_table OF public.some_type (PRIMARY KEY (a), b WITH OPTIONS DEFAULT 1000)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.some_table")
			oid := testutils.OidFromObjectName(connectionPool, "public", "some_table", backup.TYPE_RELATION)

			result := backup.GetTableType(connectionPool)
			Expect(result).To(HaveLen(1))

			Expect(result[oid]).To(Equal("public.some_type"))
		})
		It("Returns empty map when no tables OF type exist", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.some_table (i int, j int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.some_table")

			result := backup.GetTableType(connectionPool)
			Expect(result).To(BeEmpty())
		})
	})

	Describe("GetUnloggedTables", func() {
		It("Returns a map when an UNLOGGED table exists", func() {
			testutils.SkipIfBefore6(connectionPool)
			testhelper.AssertQueryRuns(connectionPool, "CREATE UNLOGGED TABLE public.some_table(i int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.some_table")
			oid := testutils.OidFromObjectName(connectionPool, "public", "some_table", backup.TYPE_RELATION)

			result := backup.GetUnloggedTables(connectionPool)
			Expect(result).To(HaveLen(1))

			Expect(result[oid]).To(BeTrue())
		})
		It("Returns empty map when no UNLOGGED tables exist", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.some_table (i int, j int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.some_table")

			result := backup.GetUnloggedTables(connectionPool)
			Expect(result).To(BeEmpty())
		})
	})

	Describe("GetForeignTableDefinitions", func() {
		It("Returns a map when a FOREIGN table exists", func() {
			testutils.SkipIfBefore6(connectionPool)
			testhelper.AssertQueryRuns(connectionPool, "CREATE FOREIGN DATA WRAPPER dummy;")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP FOREIGN DATA WRAPPER dummy")
			testhelper.AssertQueryRuns(connectionPool, "CREATE SERVER sc FOREIGN DATA WRAPPER dummy;")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SERVER sc")
			testhelper.AssertQueryRuns(connectionPool, `CREATE FOREIGN TABLE public.ft1 (
	c1 integer OPTIONS (param1 'val1') NOT NULL,
	c3 date
) SERVER sc OPTIONS (delimiter ',', quote '"');`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP FOREIGN TABLE public.ft1")
			oid := testutils.OidFromObjectName(connectionPool, "public", "ft1", backup.TYPE_RELATION)
			result := backup.GetForeignTableDefinitions(connectionPool)
			expectedResult := backup.ForeignTableDefinition{Oid: oid, Options: "delimiter ',',    quote '\"'", Server: "sc"}
			Expect(result).To(HaveLen(1))
			Expect(result[oid]).To(Equal(expectedResult))
		})
		It("Returns an empty map when no FOREIGN table exists", func() {
			testutils.SkipIfBefore6(connectionPool)
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.some_table (i int, j int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.some_table")

			result := backup.GetForeignTableDefinitions(connectionPool)
			Expect(result).To(BeEmpty())
		})
	})
	Describe("GetForeignTableRelations", func() {
		It("Returns a list with FOREIGN table", func() {
			testutils.SkipIfBefore6(connectionPool)
			testhelper.AssertQueryRuns(connectionPool, "CREATE FOREIGN DATA WRAPPER dummy;")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP FOREIGN DATA WRAPPER dummy")
			testhelper.AssertQueryRuns(connectionPool, "CREATE SERVER sc FOREIGN DATA WRAPPER dummy;")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SERVER sc")
			testhelper.AssertQueryRuns(connectionPool, `CREATE FOREIGN TABLE public.ft1 (
	c1 integer OPTIONS (param1 'val1') NOT NULL,
	c3 date
) SERVER sc OPTIONS (delimiter ',', quote '"');`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP FOREIGN TABLE public.ft1")
			result := backup.GetForeignTableRelations(connectionPool)
			Expect(result).To(HaveLen(1))
			Expect(result[0].Schema).To(Equal("public"))
			Expect(result[0].Name).To(Equal("ft1"))
		})
		It("Returns an empty list when no FOREIGN table exits", func() {
			testutils.SkipIfBefore6(connectionPool)
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.some_table (i int, j int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.some_table")
			result := backup.GetForeignTableRelations(connectionPool)
			Expect(result).To(HaveLen(0))
		})
	})
	Describe("GetTableStorageOptions", func() {
		It("returns an empty string when no table storage options exist ", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.simple_table(i int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.simple_table")
			oid := testutils.OidFromObjectName(connectionPool, "public", "simple_table", backup.TYPE_RELATION)

			_, result := backup.GetTableStorage(connectionPool)

			Expect(result[oid]).To(Equal(""))
		})
		It("returns a value for storage options of a table ", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.ao_table(i int) with (appendonly=true)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.ao_table")
			oid := testutils.OidFromObjectName(connectionPool, "public", "ao_table", backup.TYPE_RELATION)

			_, result := backup.GetTableStorage(connectionPool)

			if connectionPool.Version.Before("7") {
				Expect(result[oid]).To(Equal("appendonly=true"))
			} else {
				// For GPDB 7+, storage options no longer contain appendonly and orientation,
				// instead populates with default compression values
				Expect(result[oid]).To(Equal("blocksize=32768, compresslevel=0, compresstype=none, checksum=true"))
			}
		})
	})
	Describe("GetTableInheritance", func() {
		child := backup.Relation{Schema: "public", Name: "child"}
		childOne := backup.Relation{Schema: "public", Name: "child_one"}
		childTwo := backup.Relation{Schema: "public", Name: "child_two"}
		parent := backup.Relation{Schema: "public", Name: "parent"}
		parentOne := backup.Relation{Schema: "public", Name: "parent_one"}
		parentTwo := backup.Relation{Schema: "public", Name: "parent_two"}

		It("constructs dependencies correctly if there is one table dependent on one table", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.parent(i int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.parent")
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.child() INHERITS (public.parent)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.child")

			child.Oid = testutils.OidFromObjectName(connectionPool, "public", "child", backup.TYPE_RELATION)
			tables := []backup.Relation{child, parent}

			inheritanceMap := backup.GetTableInheritance(connectionPool, tables)

			Expect(inheritanceMap).To(HaveLen(1))
			Expect(inheritanceMap[child.Oid]).To(ConsistOf("public.parent"))
		})
		It("constructs dependencies correctly if there are two tables dependent on one table", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.parent(i int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.parent")
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.child_one() INHERITS (public.parent)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.child_one")
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.child_two() INHERITS (public.parent)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.child_two")

			childOne.Oid = testutils.OidFromObjectName(connectionPool, "public", "child_one", backup.TYPE_RELATION)
			childTwo.Oid = testutils.OidFromObjectName(connectionPool, "public", "child_two", backup.TYPE_RELATION)
			tables := []backup.Relation{parent, childOne, childTwo}

			inheritanceMap := backup.GetTableInheritance(connectionPool, tables)

			Expect(inheritanceMap).To(HaveLen(2))
			Expect(inheritanceMap[childOne.Oid]).To(ConsistOf("public.parent"))
			Expect(inheritanceMap[childTwo.Oid]).To(ConsistOf("public.parent"))
		})
		It("constructs dependencies correctly if there is one table dependent on two tables", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.parent_one(i int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.parent_one")
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.parent_two(j int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.parent_two")
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.child() INHERITS (public.parent_one, public.parent_two)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.child")

			child.Oid = testutils.OidFromObjectName(connectionPool, "public", "child", backup.TYPE_RELATION)
			tables := []backup.Relation{parentOne, parentTwo, child}

			inheritanceMap := backup.GetTableInheritance(connectionPool, tables)

			Expect(inheritanceMap).To(HaveLen(1))
			Expect(inheritanceMap[child.Oid]).To(Equal([]string{"public.parent_one", "public.parent_two"}))
		})
		It("constructs dependencies correctly if there are no table dependencies", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.parent(i int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.parent")
			tables := make([]backup.Relation, 0)

			inheritanceMap := backup.GetTableInheritance(connectionPool, tables)

			Expect(inheritanceMap).To(BeEmpty())
		})
		It("constructs dependencies correctly if there are no table dependencies while filtering", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.parent(i int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.parent")

			_ = backupCmdFlags.Set(options.INCLUDE_RELATION, "public.parent")
			tables := make([]backup.Relation, 0)

			inheritanceMap := backup.GetTableInheritance(connectionPool, tables)

			Expect(inheritanceMap).To(BeEmpty())
		})
		It("constructs dependencies correctly if there are two dependent tables but one is not in the backup set", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.parent(i int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.parent")
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.child_one() INHERITS (public.parent)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.child_one")
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.child_two() INHERITS (public.parent)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.child_two")

			_ = backupCmdFlags.Set(options.INCLUDE_RELATION, "public.child_one")
			childOne.Oid = testutils.OidFromObjectName(connectionPool, "public", "child_one", backup.TYPE_RELATION)
			tables := []backup.Relation{childOne}

			inheritanceMap := backup.GetTableInheritance(connectionPool, tables)

			Expect(inheritanceMap).To(HaveLen(1))
			Expect(inheritanceMap[childOne.Oid]).To(ConsistOf("public.parent"))
		})
		It("does not record a dependency of an external leaf partition on a parent table", func() {
			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE public.partition_table (id int, gender char(1))
		DISTRIBUTED BY (id)
		PARTITION BY LIST (gender)
		( PARTITION girls VALUES ('F'),
		  PARTITION boys VALUES ('M'),
		  DEFAULT PARTITION other );`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.partition_table")
			testhelper.AssertQueryRuns(connectionPool, `CREATE EXTERNAL WEB TABLE public.partition_table_ext_part_ (like public.partition_table_1_prt_girls)
		EXECUTE 'echo -e "2\n1"' on host
		FORMAT 'csv';`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.partition_table_ext_part_")
			testhelper.AssertQueryRuns(connectionPool, `ALTER TABLE public.partition_table EXCHANGE PARTITION girls WITH TABLE public.partition_table_ext_part_ WITHOUT VALIDATION;`)

			partition := backup.Relation{Schema: "public", Name: "partition_table_ext_part_"}

			partition.Oid = testutils.OidFromObjectName(connectionPool, "public", "partition_table_ext_part_", backup.TYPE_RELATION)
			tables := []backup.Relation{partition}

			inheritanceMap := backup.GetTableInheritance(connectionPool, tables)

			Expect(inheritanceMap).To(Not(HaveKey(partition.Oid)))
		})
	})
	Describe("GetTableReplicaIdentity", func() {
		It("Returns a map of oid to replica identity with default", func() {
			testutils.SkipIfBefore6(connectionPool)
			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE public.test_table()`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.test_table")

			oid := testutils.OidFromObjectName(connectionPool, "public", "test_table", backup.TYPE_RELATION)
			result := backup.GetTableReplicaIdentity(connectionPool)
			Expect(result[oid]).To(Equal("d"))
		})
		It("Returns a map of oid to replica identity with full", func() {
			testutils.SkipIfBefore6(connectionPool)
			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE public.test_table()`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.test_table")

			testhelper.AssertQueryRuns(connectionPool, `ALTER TABLE public.test_table REPLICA IDENTITY FULL;`)
			oid := testutils.OidFromObjectName(connectionPool, "public", "test_table", backup.TYPE_RELATION)
			result := backup.GetTableReplicaIdentity(connectionPool)
			Expect(result[oid]).To(Equal("f"))
		})
		It("Returns a map of oid to replica identity with nothing", func() {
			testutils.SkipIfBefore6(connectionPool)
			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE public.test_table()`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.test_table")

			testhelper.AssertQueryRuns(connectionPool, `ALTER TABLE public.test_table REPLICA IDENTITY NOTHING;`)
			oid := testutils.OidFromObjectName(connectionPool, "public", "test_table", backup.TYPE_RELATION)
			result := backup.GetTableReplicaIdentity(connectionPool)
			Expect(result[oid]).To(Equal("n"))
		})
	})
	Describe("GetPartitionAlteredSchema", func() {
		BeforeEach(func() {
			// For GPDB 7+, leaf partitions have their own DDL which will have the correct namespace
			if connectionPool.Version.AtLeast("7") {
				Skip("Test is not applicable to GPDB 7+")
			}
		})
		It("Returns a map of table oid to array of child partitions with different schemas", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE SCHEMA testschema")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA testschema")
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.foopart(a int, b int) PARTITION BY RANGE(a) (START(1) END (4) EVERY(1))")
			testhelper.AssertQueryRuns(connectionPool, "ALTER TABLE public.foopart_1_prt_1 SET SCHEMA testschema")
			testhelper.AssertQueryRuns(connectionPool, "ALTER TABLE public.foopart_1_prt_2 SET SCHEMA testschema")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.foopart")

			oid := testutils.OidFromObjectName(connectionPool, "public", "foopart", backup.TYPE_RELATION)
			result := backup.GetPartitionAlteredSchema(connectionPool)

			expectedAlteredPartitions := []backup.AlteredPartitionRelation{
				{OldSchema: "public", NewSchema: "testschema", Name: "foopart_1_prt_1"},
				{OldSchema: "public", NewSchema: "testschema", Name: "foopart_1_prt_2"},
			}

			Expect(result[oid]).To(ConsistOf(expectedAlteredPartitions))
		})
	})
	Describe("GetAttachedPartitionInfo", func() {
		It("Returns a map of table oid to attach partition info", func() {
			testutils.SkipIfBefore7(connectionPool)
			testhelper.AssertQueryRuns(connectionPool, "CREATE SCHEMA testschema")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA testschema")
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE testschema.foopart(a integer, b integer) PARTITION BY RANGE (b) DISTRIBUTED BY (a)")
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE testschema.foopart_1_prt_1 (a integer, b integer) DISTRIBUTED BY (a)")
			testhelper.AssertQueryRuns(connectionPool, "ALTER TABLE ONLY testschema.foopart ATTACH PARTITION testschema.foopart_1_prt_1 FOR VALUES FROM (1) TO (2)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE testschema.foopart")

			oid := testutils.OidFromObjectName(connectionPool, "testschema", "foopart_1_prt_1", backup.TYPE_RELATION)
			result := backup.GetAttachPartitionInfo(connectionPool)

			expectedAttachPartitionInfo := backup.AttachPartitionInfo{Oid: oid, Relname: "testschema.foopart_1_prt_1", Parent: "testschema.foopart", Expr: "FOR VALUES FROM (1) TO (2)"}

			structmatcher.ExpectStructsToMatch(result[oid], &expectedAttachPartitionInfo)
		})
	})
})
