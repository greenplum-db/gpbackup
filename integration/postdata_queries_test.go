package integration

import (
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup integration tests", func() {
	Describe("ConstructImplicitIndexNames", func() {
		It("returns an empty map if there are no implicit indexes", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE simple_table(i int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE simple_table")

			indexNameSet := backup.ConstructImplicitIndexNames(connection)

			Expect(indexNameSet.Length()).To(Equal(0))
		})
		It("returns a map of all implicit indexes", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE simple_table(i int UNIQUE)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE simple_table")

			indexNameSet := backup.ConstructImplicitIndexNames(connection)

			Expect(indexNameSet.Length()).To(Equal(1))
			Expect(indexNameSet.MatchesFilter("public.simple_table_i_key")).To(BeFalse()) // False because it is an exclude set
		})
	})
	Describe("GetIndex", func() {
		var indexNameSet *utils.FilterSet
		BeforeEach(func() {
			indexNameSet = utils.NewExcludeSet([]string{})
		})
		It("returns no slice when no index exists", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE simple_table(i int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE simple_table")

			results := backup.GetIndexes(connection, indexNameSet)

			Expect(len(results)).To(Equal(0))
		})
		It("returns a slice of multiple indexes", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE simple_table(i int, j int, k int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE simple_table")
			testutils.AssertQueryRuns(connection, "CREATE INDEX simple_table_idx1 ON simple_table(i)")
			defer testutils.AssertQueryRuns(connection, "DROP INDEX simple_table_idx1")
			testutils.AssertQueryRuns(connection, "CREATE INDEX simple_table_idx2 ON simple_table(j)")
			defer testutils.AssertQueryRuns(connection, "DROP INDEX simple_table_idx2")

			index1 := backup.QuerySimpleDefinition{Oid: 0, Name: "simple_table_idx1", OwningSchema: "public", OwningTable: "simple_table", Tablespace: "", Def: "CREATE INDEX simple_table_idx1 ON simple_table USING btree (i)"}
			index2 := backup.QuerySimpleDefinition{Oid: 1, Name: "simple_table_idx2", OwningSchema: "public", OwningTable: "simple_table", Tablespace: "", Def: "CREATE INDEX simple_table_idx2 ON simple_table USING btree (j)"}

			results := backup.GetIndexes(connection, indexNameSet)

			Expect(len(results)).To(Equal(2))
			results[0].Oid = testutils.OidFromObjectName(connection, "", "simple_table_idx1", backup.TYPE_INDEX)
			results[1].Oid = testutils.OidFromObjectName(connection, "", "simple_table_idx2", backup.TYPE_INDEX)

			testutils.ExpectStructsToMatchExcluding(&index1, &results[0], "Oid")
			testutils.ExpectStructsToMatchExcluding(&index2, &results[1], "Oid")
		})
		It("returns a slice of multiple indexes, excluding implicit indexes", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE simple_table(i int UNIQUE, j int, k int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE simple_table")
			testutils.AssertQueryRuns(connection, "CREATE INDEX simple_table_idx1 ON simple_table(i)")
			defer testutils.AssertQueryRuns(connection, "DROP INDEX simple_table_idx1")
			testutils.AssertQueryRuns(connection, "CREATE INDEX simple_table_idx2 ON simple_table(j)")
			defer testutils.AssertQueryRuns(connection, "DROP INDEX simple_table_idx2")
			indexNameSet.Add("public.simple_table_i_key")

			index1 := backup.QuerySimpleDefinition{Oid: 0, Name: "simple_table_idx1", OwningSchema: "public", OwningTable: "simple_table", Tablespace: "", Def: "CREATE INDEX simple_table_idx1 ON simple_table USING btree (i)"}
			index2 := backup.QuerySimpleDefinition{Oid: 1, Name: "simple_table_idx2", OwningSchema: "public", OwningTable: "simple_table", Tablespace: "", Def: "CREATE INDEX simple_table_idx2 ON simple_table USING btree (j)"}

			results := backup.GetIndexes(connection, indexNameSet)

			Expect(len(results)).To(Equal(2))
			testutils.ExpectStructsToMatchExcluding(&index1, &results[0], "Oid")
			testutils.ExpectStructsToMatchExcluding(&index2, &results[1], "Oid")
		})
		It("returns a slice of indexes for only partition parent tables", func() {
			testutils.AssertQueryRuns(connection, `CREATE TABLE part (id int, date date, amt decimal(10,2)) DISTRIBUTED BY (id)
PARTITION BY RANGE (date)
      (PARTITION Jan08 START (date '2008-01-01') INCLUSIVE ,
      PARTITION Feb08 START (date '2008-02-01') INCLUSIVE ,
      PARTITION Mar08 START (date '2008-03-01') INCLUSIVE
      END (date '2008-04-01') EXCLUSIVE);
`)
			defer testutils.AssertQueryRuns(connection, "DROP TABLE part")
			testutils.AssertQueryRuns(connection, "CREATE INDEX part_idx ON part(id)")
			defer testutils.AssertQueryRuns(connection, "DROP INDEX part_idx")

			index1 := backup.QuerySimpleDefinition{Oid: 0, Name: "part_idx", OwningSchema: "public", OwningTable: "part", Tablespace: "", Def: "CREATE INDEX part_idx ON part USING btree (id)"}

			results := backup.GetIndexes(connection, indexNameSet)

			Expect(len(results)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&index1, &results[0], "Oid")
		})
		It("returns a slice containing an index in a non-default tablespace", func() {
			if connection.Version.Before("6") {
				testutils.AssertQueryRuns(connection, "CREATE TABLESPACE test_tablespace FILESPACE test_dir")
			} else {
				testutils.AssertQueryRuns(connection, "CREATE TABLESPACE test_tablespace LOCATION '/tmp/test_dir'")
			}
			defer testutils.AssertQueryRuns(connection, "DROP TABLESPACE test_tablespace")
			testutils.AssertQueryRuns(connection, "CREATE TABLE simple_table(i int, j int, k int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE simple_table")
			testutils.AssertQueryRuns(connection, "CREATE INDEX simple_table_idx ON simple_table(i) TABLESPACE test_tablespace")
			defer testutils.AssertQueryRuns(connection, "DROP INDEX simple_table_idx")

			index1 := backup.QuerySimpleDefinition{Oid: 0, Name: "simple_table_idx", OwningSchema: "public", OwningTable: "simple_table", Tablespace: "test_tablespace", Def: "CREATE INDEX simple_table_idx ON simple_table USING btree (i)"}

			results := backup.GetIndexes(connection, indexNameSet)

			Expect(len(results)).To(Equal(1))
			results[0].Oid = testutils.OidFromObjectName(connection, "", "simple_table_idx", backup.TYPE_INDEX)

			testutils.ExpectStructsToMatchExcluding(&index1, &results[0], "Oid")
		})
		It("returns a slice for an index in specific schema", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE simple_table(i int, j int, k int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE simple_table")
			testutils.AssertQueryRuns(connection, "CREATE INDEX simple_table_idx1 ON simple_table(i)")
			defer testutils.AssertQueryRuns(connection, "DROP INDEX simple_table_idx1")
			testutils.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
			defer testutils.AssertQueryRuns(connection, "DROP SCHEMA testschema")
			testutils.AssertQueryRuns(connection, "CREATE TABLE testschema.simple_table(i int, j int, k int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE testschema.simple_table")
			testutils.AssertQueryRuns(connection, "CREATE INDEX simple_table_idx1 ON testschema.simple_table(i)")
			defer testutils.AssertQueryRuns(connection, "DROP INDEX testschema.simple_table_idx1")
			backup.SetIncludeSchemas([]string{"testschema"})

			index1 := backup.QuerySimpleDefinition{Oid: 0, Name: "simple_table_idx1", OwningSchema: "testschema", OwningTable: "simple_table", Tablespace: "", Def: "CREATE INDEX simple_table_idx1 ON testschema.simple_table USING btree (i)"}

			results := backup.GetIndexes(connection, indexNameSet)

			Expect(len(results)).To(Equal(1))
			results[0].Oid = testutils.OidFromObjectName(connection, "", "simple_table_idx1", backup.TYPE_INDEX)

			testutils.ExpectStructsToMatchExcluding(&index1, &results[0], "Oid")
		})
		It("returns a slice for an index used for clustering", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE simple_table(i int, j int, k int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE simple_table")
			testutils.AssertQueryRuns(connection, "CREATE INDEX simple_table_idx1 ON simple_table(i)")
			defer testutils.AssertQueryRuns(connection, "DROP INDEX simple_table_idx1")
			testutils.AssertQueryRuns(connection, "ALTER TABLE public.simple_table CLUSTER ON simple_table_idx1")

			index1 := backup.QuerySimpleDefinition{Oid: 0, Name: "simple_table_idx1", OwningSchema: "public", OwningTable: "simple_table", Tablespace: "", Def: "CREATE INDEX simple_table_idx1 ON simple_table USING btree (i)", IsClustered: true}

			results := backup.GetIndexes(connection, indexNameSet)

			Expect(len(results)).To(Equal(1))
			results[0].Oid = testutils.OidFromObjectName(connection, "", "simple_table_idx1", backup.TYPE_INDEX)

			testutils.ExpectStructsToMatchExcluding(&index1, &results[0], "Oid")
		})
	})
	Describe("GetRules", func() {
		It("returns no slice when no rule exists", func() {
			results := backup.GetRules(connection)

			Expect(len(results)).To(Equal(0))
		})
		It("returns a slice of multiple rules", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE rule_table1(i int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE rule_table1")
			testutils.AssertQueryRuns(connection, "CREATE TABLE rule_table2(i int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE rule_table2")
			testutils.AssertQueryRuns(connection, "CREATE RULE double_insert AS ON INSERT TO rule_table1 DO INSERT INTO rule_table2 (i) VALUES (1)")
			defer testutils.AssertQueryRuns(connection, "DROP RULE double_insert ON rule_table1")
			testutils.AssertQueryRuns(connection, "CREATE RULE update_notify AS ON UPDATE TO rule_table1 DO NOTIFY rule_table1")
			defer testutils.AssertQueryRuns(connection, "DROP RULE update_notify ON rule_table1")
			testutils.AssertQueryRuns(connection, "COMMENT ON RULE update_notify ON rule_table1 IS 'This is a rule comment.'")

			rule1 := backup.QuerySimpleDefinition{Oid: 0, Name: "double_insert", OwningSchema: "public", OwningTable: "rule_table1", Tablespace: "", Def: "CREATE RULE double_insert AS ON INSERT TO rule_table1 DO INSERT INTO rule_table2 (i) VALUES (1);"}
			rule2 := backup.QuerySimpleDefinition{Oid: 1, Name: "update_notify", OwningSchema: "public", OwningTable: "rule_table1", Tablespace: "", Def: "CREATE RULE update_notify AS ON UPDATE TO rule_table1 DO NOTIFY rule_table1;"}

			results := backup.GetRules(connection)

			Expect(len(results)).To(Equal(2))
			testutils.ExpectStructsToMatchExcluding(&rule1, &results[0], "Oid")
			testutils.ExpectStructsToMatchExcluding(&rule2, &results[1], "Oid")
		})
		It("returns a slice of rules for a specific schema", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE rule_table1(i int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE rule_table1")
			testutils.AssertQueryRuns(connection, "CREATE RULE double_insert AS ON INSERT TO rule_table1 DO INSERT INTO rule_table1 (i) VALUES (1)")
			defer testutils.AssertQueryRuns(connection, "DROP RULE double_insert ON rule_table1")
			testutils.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
			defer testutils.AssertQueryRuns(connection, "DROP SCHEMA testschema")
			testutils.AssertQueryRuns(connection, "CREATE TABLE testschema.rule_table1(i int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE testschema.rule_table1")
			testutils.AssertQueryRuns(connection, "CREATE RULE double_insert AS ON INSERT TO testschema.rule_table1 DO INSERT INTO testschema.rule_table1 (i) VALUES (1)")
			defer testutils.AssertQueryRuns(connection, "DROP RULE double_insert ON testschema.rule_table1")
			backup.SetIncludeSchemas([]string{"testschema"})

			rule1 := backup.QuerySimpleDefinition{Oid: 0, Name: "double_insert", OwningSchema: "testschema", OwningTable: "rule_table1", Tablespace: "", Def: "CREATE RULE double_insert AS ON INSERT TO testschema.rule_table1 DO INSERT INTO testschema.rule_table1 (i) VALUES (1);"}

			results := backup.GetRules(connection)

			Expect(len(results)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&rule1, &results[0], "Oid")
		})
	})
	Describe("GetTriggers", func() {
		It("returns no slice when no trigger exists", func() {
			results := backup.GetTriggers(connection)

			Expect(len(results)).To(Equal(0))
		})
		It("returns a slice of multiple triggers", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE trigger_table1(i int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE trigger_table1")
			testutils.AssertQueryRuns(connection, "CREATE TABLE trigger_table2(j int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE trigger_table2")
			testutils.AssertQueryRuns(connection, "CREATE TRIGGER sync_trigger_table1 AFTER INSERT OR DELETE OR UPDATE ON trigger_table1 FOR EACH STATEMENT EXECUTE PROCEDURE flatfile_update_trigger()")
			defer testutils.AssertQueryRuns(connection, "DROP TRIGGER sync_trigger_table1 ON trigger_table1")
			testutils.AssertQueryRuns(connection, "CREATE TRIGGER sync_trigger_table2 AFTER INSERT OR DELETE OR UPDATE ON trigger_table2 FOR EACH STATEMENT EXECUTE PROCEDURE flatfile_update_trigger()")
			defer testutils.AssertQueryRuns(connection, "DROP TRIGGER sync_trigger_table2 ON trigger_table2")
			testutils.AssertQueryRuns(connection, "COMMENT ON TRIGGER sync_trigger_table2 ON trigger_table2 IS 'This is a trigger comment.'")

			trigger1 := backup.QuerySimpleDefinition{Oid: 0, Name: "sync_trigger_table1", OwningSchema: "public", OwningTable: "trigger_table1", Tablespace: "", Def: "CREATE TRIGGER sync_trigger_table1 AFTER INSERT OR DELETE OR UPDATE ON trigger_table1 FOR EACH STATEMENT EXECUTE PROCEDURE flatfile_update_trigger()"}
			trigger2 := backup.QuerySimpleDefinition{Oid: 1, Name: "sync_trigger_table2", OwningSchema: "public", OwningTable: "trigger_table2", Tablespace: "", Def: "CREATE TRIGGER sync_trigger_table2 AFTER INSERT OR DELETE OR UPDATE ON trigger_table2 FOR EACH STATEMENT EXECUTE PROCEDURE flatfile_update_trigger()"}

			results := backup.GetTriggers(connection)

			Expect(len(results)).To(Equal(2))
			testutils.ExpectStructsToMatchExcluding(&trigger1, &results[0], "Oid")
			testutils.ExpectStructsToMatchExcluding(&trigger2, &results[1], "Oid")
		})
		It("does not include constraint triggers", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE trigger_table1(i int PRIMARY KEY)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE trigger_table1")
			testutils.AssertQueryRuns(connection, "CREATE TABLE trigger_table2(j int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE trigger_table2")
			testutils.AssertQueryRuns(connection, "ALTER TABLE trigger_table2 ADD CONSTRAINT fkc FOREIGN KEY (j) REFERENCES trigger_table1 (i) ON UPDATE RESTRICT ON DELETE RESTRICT")

			results := backup.GetTriggers(connection)

			Expect(len(results)).To(Equal(0))
		})
		It("returns a slice of triggers for a specific schema", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLE trigger_table1(i int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE trigger_table1")
			testutils.AssertQueryRuns(connection, "CREATE TRIGGER sync_trigger_table1 AFTER INSERT OR DELETE OR UPDATE ON trigger_table1 FOR EACH STATEMENT EXECUTE PROCEDURE flatfile_update_trigger()")
			defer testutils.AssertQueryRuns(connection, "DROP TRIGGER sync_trigger_table1 ON trigger_table1")
			testutils.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
			defer testutils.AssertQueryRuns(connection, "DROP SCHEMA testschema")
			testutils.AssertQueryRuns(connection, "CREATE TABLE testschema.trigger_table1(i int)")
			defer testutils.AssertQueryRuns(connection, "DROP TABLE testschema.trigger_table1")
			testutils.AssertQueryRuns(connection, "CREATE TRIGGER sync_trigger_table1 AFTER INSERT OR DELETE OR UPDATE ON testschema.trigger_table1 FOR EACH STATEMENT EXECUTE PROCEDURE flatfile_update_trigger()")
			defer testutils.AssertQueryRuns(connection, "DROP TRIGGER sync_trigger_table1 ON testschema.trigger_table1")
			backup.SetIncludeSchemas([]string{"testschema"})

			trigger1 := backup.QuerySimpleDefinition{Oid: 0, Name: "sync_trigger_table1", OwningSchema: "testschema", OwningTable: "trigger_table1", Tablespace: "", Def: "CREATE TRIGGER sync_trigger_table1 AFTER INSERT OR DELETE OR UPDATE ON testschema.trigger_table1 FOR EACH STATEMENT EXECUTE PROCEDURE flatfile_update_trigger()"}

			results := backup.GetTriggers(connection)

			Expect(len(results)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&trigger1, &results[0], "Oid")
		})
	})
})
