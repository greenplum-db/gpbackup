package integration

import (
	"github.com/greenplum-db/gp-common-go-libs/structmatcher"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup integration tests", func() {
	Describe("ConstructImplicitIndexNames", func() {
		It("returns an empty string if there are no implicit indexes", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.simple_table(i int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.simple_table")

			indexNameSet := backup.ConstructImplicitIndexNames(connectionPool)

			Expect(indexNameSet).To(Equal(""))
		})
		It("returns a string of all implicit indexes", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.simple_table(i int UNIQUE)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.simple_table")

			indexNameSet := backup.ConstructImplicitIndexNames(connectionPool)

			Expect(indexNameSet).To(Equal("'public.simple_table_i_key'"))
		})
	})
	Describe("GetIndex", func() {
		It("returns no slice when no index exists", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.simple_table(i int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.simple_table")

			results := backup.GetIndexes(connectionPool)

			Expect(results).To(BeEmpty())
		})
		It("returns a slice of multiple indexes", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.simple_table(i int, j int, k int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.simple_table")
			testhelper.AssertQueryRuns(connectionPool, "CREATE INDEX simple_table_idx1 ON public.simple_table(i)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP INDEX public.simple_table_idx1")
			testhelper.AssertQueryRuns(connectionPool, "CREATE INDEX simple_table_idx2 ON public.simple_table(j)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP INDEX public.simple_table_idx2")

			index1 := backup.IndexDefinition{Oid: 0, Name: "simple_table_idx1", OwningSchema: "public", OwningTable: "simple_table", Def: "CREATE INDEX simple_table_idx1 ON public.simple_table USING btree (i)"}
			index2 := backup.IndexDefinition{Oid: 1, Name: "simple_table_idx2", OwningSchema: "public", OwningTable: "simple_table", Def: "CREATE INDEX simple_table_idx2 ON public.simple_table USING btree (j)"}

			results := backup.GetIndexes(connectionPool)

			Expect(results).To(HaveLen(2))
			results[0].Oid = testutils.OidFromObjectName(connectionPool, "", "simple_table_idx1", backup.TYPE_INDEX)
			results[1].Oid = testutils.OidFromObjectName(connectionPool, "", "simple_table_idx2", backup.TYPE_INDEX)

			structmatcher.ExpectStructsToMatchExcluding(&index1, &results[0], "Oid")
			structmatcher.ExpectStructsToMatchExcluding(&index2, &results[1], "Oid")
		})
		It("returns a slice of multiple indexes, including implicit indexes created by constraints", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.simple_table(i int, j int, k int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.simple_table CASCADE")
			testhelper.AssertQueryRuns(connectionPool, "ALTER TABLE public.simple_table ADD CONSTRAINT test_constraint UNIQUE (i, k)")
			testhelper.AssertQueryRuns(connectionPool, "CREATE UNIQUE INDEX simple_table_idx1 ON public.simple_table(i)")
			testhelper.AssertQueryRuns(connectionPool, "CREATE INDEX simple_table_idx2 ON public.simple_table(j)")

			index1 := backup.IndexDefinition{Oid: 0, Name: "simple_table_idx1", OwningSchema: "public", OwningTable: "simple_table", Def: "CREATE UNIQUE INDEX simple_table_idx1 ON public.simple_table USING btree (i)"}
			index2 := backup.IndexDefinition{Oid: 1, Name: "simple_table_idx2", OwningSchema: "public", OwningTable: "simple_table", Def: "CREATE INDEX simple_table_idx2 ON public.simple_table USING btree (j)"}

			results := backup.GetIndexes(connectionPool)
			supportsConstraint := make([]backup.IndexDefinition, 0)
			userIndex := make([]backup.IndexDefinition, 0)
			for _, indexDef := range results {
				if indexDef.SupportsConstraint {
					supportsConstraint = append(supportsConstraint, indexDef)
				} else {
					userIndex = append(userIndex, indexDef)
				}
			}

			Expect(userIndex).To(HaveLen(2))
			Expect(supportsConstraint).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&index1, &userIndex[0], "Oid")
			structmatcher.ExpectStructsToMatchExcluding(&index2, &userIndex[1], "Oid")
		})
		It("returns a slice of indexes for only partition parent tables", func() {
			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE public.part (id int, date date, amt decimal(10,2)) DISTRIBUTED BY (id)
PARTITION BY RANGE (date)
      (PARTITION Jan08 START (date '2008-01-01') INCLUSIVE ,
      PARTITION Feb08 START (date '2008-02-01') INCLUSIVE ,
      PARTITION Mar08 START (date '2008-03-01') INCLUSIVE
      END (date '2008-04-01') EXCLUSIVE);
`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.part")
			testhelper.AssertQueryRuns(connectionPool, "CREATE INDEX part_idx ON public.part(id)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP INDEX public.part_idx")

			index1 := backup.IndexDefinition{Oid: 0, Name: "part_idx", OwningSchema: "public", OwningTable: "part", Def: "CREATE INDEX part_idx ON public.part USING btree (id)"}

			results := backup.GetIndexes(connectionPool)

			Expect(results).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&index1, &results[0], "Oid")
		})
		It("returns a slice containing an index in a non-default tablespace", func() {
			if connectionPool.Version.Before("6") {
				testhelper.AssertQueryRuns(connectionPool, "CREATE TABLESPACE test_tablespace FILESPACE test_dir")
			} else {
				testhelper.AssertQueryRuns(connectionPool, "CREATE TABLESPACE test_tablespace LOCATION '/tmp/test_dir'")
			}
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLESPACE test_tablespace")
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.simple_table(i int, j int, k int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.simple_table")
			testhelper.AssertQueryRuns(connectionPool, "CREATE INDEX simple_table_idx ON public.simple_table(i) TABLESPACE test_tablespace")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP INDEX public.simple_table_idx")

			index1 := backup.IndexDefinition{Oid: 0, Name: "simple_table_idx", OwningSchema: "public", OwningTable: "simple_table", Tablespace: "test_tablespace", Def: "CREATE INDEX simple_table_idx ON public.simple_table USING btree (i)"}

			results := backup.GetIndexes(connectionPool)

			Expect(results).To(HaveLen(1))
			results[0].Oid = testutils.OidFromObjectName(connectionPool, "", "simple_table_idx", backup.TYPE_INDEX)

			structmatcher.ExpectStructsToMatchExcluding(&index1, &results[0], "Oid")
		})
		It("returns a slice for an index in specific schema", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.simple_table(i int, j int, k int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.simple_table")
			testhelper.AssertQueryRuns(connectionPool, "CREATE INDEX simple_table_idx1 ON public.simple_table(i)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP INDEX public.simple_table_idx1")
			testhelper.AssertQueryRuns(connectionPool, "CREATE SCHEMA testschema")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA testschema")
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE testschema.simple_table(i int, j int, k int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE testschema.simple_table")
			testhelper.AssertQueryRuns(connectionPool, "CREATE INDEX simple_table_idx1 ON testschema.simple_table(i)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP INDEX testschema.simple_table_idx1")
			backupCmdFlags.Set(utils.INCLUDE_SCHEMA, "testschema")

			index1 := backup.IndexDefinition{Oid: 0, Name: "simple_table_idx1", OwningSchema: "testschema", OwningTable: "simple_table", Def: "CREATE INDEX simple_table_idx1 ON testschema.simple_table USING btree (i)"}

			results := backup.GetIndexes(connectionPool)

			Expect(results).To(HaveLen(1))
			results[0].Oid = testutils.OidFromObjectName(connectionPool, "", "simple_table_idx1", backup.TYPE_INDEX)

			structmatcher.ExpectStructsToMatchExcluding(&index1, &results[0], "Oid")
		})
		It("returns a slice of indexes belonging to filtered tables", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.simple_table(i int, j int, k int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.simple_table")
			testhelper.AssertQueryRuns(connectionPool, "CREATE INDEX simple_table_idx1 ON public.simple_table(i)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP INDEX public.simple_table_idx1")
			testhelper.AssertQueryRuns(connectionPool, "CREATE SCHEMA testschema")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA testschema")
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE testschema.simple_table(i int, j int, k int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE testschema.simple_table")
			testhelper.AssertQueryRuns(connectionPool, "CREATE INDEX simple_table_idx1 ON testschema.simple_table(i)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP INDEX testschema.simple_table_idx1")
			backupCmdFlags.Set(utils.INCLUDE_RELATION, "testschema.simple_table")

			index1 := backup.IndexDefinition{Oid: 0, Name: "simple_table_idx1", OwningSchema: "testschema", OwningTable: "simple_table", Def: "CREATE INDEX simple_table_idx1 ON testschema.simple_table USING btree (i)"}

			results := backup.GetIndexes(connectionPool)

			Expect(results).To(HaveLen(1))
			results[0].Oid = testutils.OidFromObjectName(connectionPool, "", "simple_table_idx1", backup.TYPE_INDEX)

			structmatcher.ExpectStructsToMatchExcluding(&index1, &results[0], "Oid")
		})
		It("returns a slice for an index used for clustering", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.simple_table(i int, j int, k int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.simple_table")
			testhelper.AssertQueryRuns(connectionPool, "CREATE INDEX simple_table_idx1 ON public.simple_table(i)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP INDEX public.simple_table_idx1")
			testhelper.AssertQueryRuns(connectionPool, "ALTER TABLE public.simple_table CLUSTER ON simple_table_idx1")

			index1 := backup.IndexDefinition{Oid: 0, Name: "simple_table_idx1", OwningSchema: "public", OwningTable: "simple_table", Def: "CREATE INDEX simple_table_idx1 ON public.simple_table USING btree (i)", IsClustered: true}

			results := backup.GetIndexes(connectionPool)

			Expect(results).To(HaveLen(1))
			results[0].Oid = testutils.OidFromObjectName(connectionPool, "", "simple_table_idx1", backup.TYPE_INDEX)

			structmatcher.ExpectStructsToMatchExcluding(&index1, &results[0], "Oid")
		})
	})
	Describe("GetRules", func() {
		var (
			ruleDef1 string
			ruleDef2 string
			rule1    backup.RuleDefinition
		)
		BeforeEach(func() {
			if connectionPool.Version.Before("6") {
				ruleDef1 = "CREATE RULE double_insert AS ON INSERT TO public.rule_table1 DO INSERT INTO public.rule_table1 (i) VALUES (1);"
				ruleDef2 = "CREATE RULE update_notify AS ON UPDATE TO public.rule_table1 DO NOTIFY rule_table1;"
			} else {
				ruleDef1 = "CREATE RULE double_insert AS\n    ON INSERT TO public.rule_table1 DO  INSERT INTO public.rule_table1 (i)\n  VALUES (1);"
				ruleDef2 = "CREATE RULE update_notify AS\n    ON UPDATE TO public.rule_table1 DO\n NOTIFY rule_table1;"
			}
			rule1 = backup.RuleDefinition{Oid: 0, Name: "double_insert", OwningSchema: "public", OwningTable: "rule_table1", Def: ruleDef1}
		})
		It("returns no slice when no rule exists", func() {
			results := backup.GetRules(connectionPool)

			Expect(results).To(BeEmpty())
		})
		It("returns a slice of multiple rules", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.rule_table1(i int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.rule_table1")
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.rule_table2(i int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.rule_table2")
			testhelper.AssertQueryRuns(connectionPool, "CREATE RULE double_insert AS ON INSERT TO public.rule_table1 DO INSERT INTO public.rule_table1 (i) VALUES (1)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP RULE double_insert ON public.rule_table1")
			testhelper.AssertQueryRuns(connectionPool, "CREATE RULE update_notify AS ON UPDATE TO public.rule_table1 DO NOTIFY rule_table1")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP RULE update_notify ON public.rule_table1")

			rule2 := backup.RuleDefinition{Oid: 1, Name: "update_notify", OwningSchema: "public", OwningTable: "rule_table1", Def: ruleDef2}

			results := backup.GetRules(connectionPool)

			Expect(results).To(HaveLen(2))
			structmatcher.ExpectStructsToMatchExcluding(&rule1, &results[0], "Oid")
			structmatcher.ExpectStructsToMatchExcluding(&rule2, &results[1], "Oid")
		})
		It("returns a slice of rules for a specific schema", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.rule_table1(i int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.rule_table1")
			testhelper.AssertQueryRuns(connectionPool, "CREATE RULE double_insert AS ON INSERT TO public.rule_table1 DO INSERT INTO public.rule_table1 (i) VALUES (1)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP RULE double_insert ON public.rule_table1")
			testhelper.AssertQueryRuns(connectionPool, "CREATE SCHEMA testschema")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA testschema")
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE testschema.rule_table1(i int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE testschema.rule_table1")
			testhelper.AssertQueryRuns(connectionPool, "CREATE RULE double_insert AS ON INSERT TO testschema.rule_table1 DO INSERT INTO testschema.rule_table1 (i) VALUES (1)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP RULE double_insert ON testschema.rule_table1")
			backupCmdFlags.Set(utils.INCLUDE_SCHEMA, "public")

			results := backup.GetRules(connectionPool)

			Expect(results).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&rule1, &results[0], "Oid")
		})
		It("returns a slice of rules belonging to filtered tables", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.rule_table1(i int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.rule_table1")
			testhelper.AssertQueryRuns(connectionPool, "CREATE RULE double_insert AS ON INSERT TO public.rule_table1 DO INSERT INTO public.rule_table1 (i) VALUES (1)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP RULE double_insert ON public.rule_table1")
			testhelper.AssertQueryRuns(connectionPool, "CREATE SCHEMA testschema")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA testschema")
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE testschema.rule_table1(i int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE testschema.rule_table1")
			testhelper.AssertQueryRuns(connectionPool, "CREATE RULE double_insert AS ON INSERT TO testschema.rule_table1 DO INSERT INTO testschema.rule_table1 (i) VALUES (1)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP RULE double_insert ON testschema.rule_table1")
			backupCmdFlags.Set(utils.INCLUDE_RELATION, "public.rule_table1")

			results := backup.GetRules(connectionPool)

			Expect(results).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&rule1, &results[0], "Oid")
		})
	})
	Describe("GetTriggers", func() {
		It("returns no slice when no trigger exists", func() {
			results := backup.GetTriggers(connectionPool)

			Expect(results).To(BeEmpty())
		})
		It("returns a slice of multiple triggers", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.trigger_table1(i int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.trigger_table1")
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.trigger_table2(j int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.trigger_table2")
			testhelper.AssertQueryRuns(connectionPool, `CREATE TRIGGER sync_trigger_table1 AFTER INSERT OR DELETE OR UPDATE ON public.trigger_table1 FOR EACH STATEMENT EXECUTE PROCEDURE "RI_FKey_check_ins"()`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TRIGGER sync_trigger_table1 ON public.trigger_table1")
			testhelper.AssertQueryRuns(connectionPool, `CREATE TRIGGER sync_trigger_table2 AFTER INSERT OR DELETE OR UPDATE ON public.trigger_table2 FOR EACH STATEMENT EXECUTE PROCEDURE "RI_FKey_check_ins"()`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TRIGGER sync_trigger_table2 ON public.trigger_table2")

			trigger1 := backup.TriggerDefinition{Oid: 0, Name: "sync_trigger_table1", OwningSchema: "public", OwningTable: "trigger_table1", Def: `CREATE TRIGGER sync_trigger_table1 AFTER INSERT OR DELETE OR UPDATE ON public.trigger_table1 FOR EACH STATEMENT EXECUTE PROCEDURE "RI_FKey_check_ins"()`}
			trigger2 := backup.TriggerDefinition{Oid: 1, Name: "sync_trigger_table2", OwningSchema: "public", OwningTable: "trigger_table2", Def: `CREATE TRIGGER sync_trigger_table2 AFTER INSERT OR DELETE OR UPDATE ON public.trigger_table2 FOR EACH STATEMENT EXECUTE PROCEDURE "RI_FKey_check_ins"()`}

			results := backup.GetTriggers(connectionPool)

			Expect(results).To(HaveLen(2))
			structmatcher.ExpectStructsToMatchExcluding(&trigger1, &results[0], "Oid")
			structmatcher.ExpectStructsToMatchExcluding(&trigger2, &results[1], "Oid")
		})
		It("does not include constraint triggers", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.trigger_table1(i int PRIMARY KEY)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.trigger_table1")
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.trigger_table2(j int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.trigger_table2")
			testhelper.AssertQueryRuns(connectionPool, "ALTER TABLE public.trigger_table2 ADD CONSTRAINT fkc FOREIGN KEY (j) REFERENCES public.trigger_table1 (i) ON UPDATE RESTRICT ON DELETE RESTRICT")

			results := backup.GetTriggers(connectionPool)

			Expect(results).To(BeEmpty())
		})
		It("returns a slice of triggers for a specific schema", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.trigger_table1(i int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.trigger_table1")
			testhelper.AssertQueryRuns(connectionPool, `CREATE TRIGGER sync_trigger_table1 AFTER INSERT OR DELETE OR UPDATE ON public.trigger_table1 FOR EACH STATEMENT EXECUTE PROCEDURE "RI_FKey_check_ins"()`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TRIGGER sync_trigger_table1 ON public.trigger_table1")
			testhelper.AssertQueryRuns(connectionPool, "CREATE SCHEMA testschema")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA testschema")
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE testschema.trigger_table1(i int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE testschema.trigger_table1")
			testhelper.AssertQueryRuns(connectionPool, `CREATE TRIGGER sync_trigger_table1 AFTER INSERT OR DELETE OR UPDATE ON testschema.trigger_table1 FOR EACH STATEMENT EXECUTE PROCEDURE "RI_FKey_check_ins"()`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TRIGGER sync_trigger_table1 ON testschema.trigger_table1")
			backupCmdFlags.Set(utils.INCLUDE_SCHEMA, "testschema")

			trigger1 := backup.TriggerDefinition{Oid: 0, Name: "sync_trigger_table1", OwningSchema: "testschema", OwningTable: "trigger_table1", Def: `CREATE TRIGGER sync_trigger_table1 AFTER INSERT OR DELETE OR UPDATE ON testschema.trigger_table1 FOR EACH STATEMENT EXECUTE PROCEDURE "RI_FKey_check_ins"()`}

			results := backup.GetTriggers(connectionPool)

			Expect(results).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&trigger1, &results[0], "Oid")
		})
		It("returns a slice of triggers belonging to filtered tables", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.trigger_table1(i int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.trigger_table1")
			testhelper.AssertQueryRuns(connectionPool, `CREATE TRIGGER sync_trigger_table1 AFTER INSERT OR DELETE OR UPDATE ON public.trigger_table1 FOR EACH STATEMENT EXECUTE PROCEDURE "RI_FKey_check_ins"()`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TRIGGER sync_trigger_table1 ON public.trigger_table1")
			testhelper.AssertQueryRuns(connectionPool, "CREATE SCHEMA testschema")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA testschema")
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE testschema.trigger_table1(i int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE testschema.trigger_table1")
			testhelper.AssertQueryRuns(connectionPool, `CREATE TRIGGER sync_trigger_table1 AFTER INSERT OR DELETE OR UPDATE ON testschema.trigger_table1 FOR EACH STATEMENT EXECUTE PROCEDURE "RI_FKey_check_ins"()`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TRIGGER sync_trigger_table1 ON testschema.trigger_table1")
			backupCmdFlags.Set(utils.INCLUDE_RELATION, "testschema.trigger_table1")

			trigger1 := backup.TriggerDefinition{Oid: 0, Name: "sync_trigger_table1", OwningSchema: "testschema", OwningTable: "trigger_table1", Def: `CREATE TRIGGER sync_trigger_table1 AFTER INSERT OR DELETE OR UPDATE ON testschema.trigger_table1 FOR EACH STATEMENT EXECUTE PROCEDURE "RI_FKey_check_ins"()`}

			results := backup.GetTriggers(connectionPool)

			Expect(results).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&trigger1, &results[0], "Oid")
		})
	})
	Describe("GetEventTriggers", func() {
		BeforeEach(func() {
			testutils.SkipIfBefore6(connectionPool)
			testhelper.AssertQueryRuns(connectionPool, `CREATE FUNCTION abort_any_command()
RETURNS event_trigger LANGUAGE plpgsql
AS $$ BEGIN RAISE EXCEPTION 'exception'; END; $$;`)
		})
		AfterEach(func() {
			testhelper.AssertQueryRuns(connectionPool, `DROP FUNCTION abort_any_command()`)
		})
		It("returns no slice when no event trigger exists", func() {
			results := backup.GetEventTriggers(connectionPool)

			Expect(results).To(BeEmpty())
		})
		It("returns a slice of multiple event triggers ", func() {

			testhelper.AssertQueryRuns(connectionPool, "CREATE EVENT TRIGGER testeventtrigger1 ON ddl_command_start EXECUTE PROCEDURE abort_any_command();")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP EVENT TRIGGER testeventtrigger1")
			testhelper.AssertQueryRuns(connectionPool, "CREATE EVENT TRIGGER testeventtrigger2 ON ddl_command_start EXECUTE PROCEDURE abort_any_command();")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP EVENT TRIGGER testeventtrigger2")

			results := backup.GetEventTriggers(connectionPool)

			eventTrigger1 := backup.EventTrigger{Oid: 1, Name: "testeventtrigger1", Event: "ddl_command_start", FunctionName: "abort_any_command", Enabled: "O"}
			eventTrigger2 := backup.EventTrigger{Oid: 1, Name: "testeventtrigger2", Event: "ddl_command_start", FunctionName: "abort_any_command", Enabled: "O"}

			Expect(results).To(HaveLen(2))
			structmatcher.ExpectStructsToMatchExcluding(&eventTrigger1, &results[0], "Oid")
			structmatcher.ExpectStructsToMatchExcluding(&eventTrigger2, &results[1], "Oid")

		})
		It("returns a slice of event trigger with a filter tag", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE EVENT TRIGGER testeventtrigger1 ON ddl_command_start WHEN TAG IN ('DROP FUNCTION') EXECUTE PROCEDURE abort_any_command();")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP EVENT TRIGGER testeventtrigger1")

			results := backup.GetEventTriggers(connectionPool)

			eventTrigger1 := backup.EventTrigger{Oid: 1, Name: "testeventtrigger1", Event: "ddl_command_start", FunctionName: "abort_any_command", Enabled: "O", EventTags: `'DROP FUNCTION'`}

			Expect(results).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&eventTrigger1, &results[0], "Oid")

		})
		It("returns a slice of event trigger with multiple filter tags", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE EVENT TRIGGER testeventtrigger1 ON ddl_command_start WHEN TAG IN ('DROP FUNCTION', 'DROP TABLE') EXECUTE PROCEDURE abort_any_command();")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP EVENT TRIGGER testeventtrigger1")

			results := backup.GetEventTriggers(connectionPool)

			eventTrigger1 := backup.EventTrigger{Oid: 1, Name: "testeventtrigger1", Event: "ddl_command_start", FunctionName: "abort_any_command", Enabled: "O", EventTags: `'DROP FUNCTION', 'DROP TABLE'`}

			Expect(results).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&eventTrigger1, &results[0], "Oid")

		})
	})
})
