package integration

import (
	"database/sql"

	"github.com/greenplum-db/gp-common-go-libs/structmatcher"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup integration create statement tests", func() {
	BeforeEach(func() {
		tocfile, backupfile = testutils.InitializeTestTOC(buffer, "predata")
	})
	Describe("PrintCreateIndexStatements", func() {
		var (
			indexMetadataMap backup.MetadataMap
			index            backup.IndexDefinition
		)
		BeforeEach(func() {
			indexMetadataMap = backup.MetadataMap{}
			index = backup.IndexDefinition{
				Oid:          0,
				Name:         "index1",
				OwningSchema: "public",
				OwningTable:  "testtable",
				Def:          sql.NullString{String: "CREATE INDEX index1 ON public.testtable USING btree (i)", Valid: true},
			}
		})
		It("creates a basic index", func() {
			indexes := []backup.IndexDefinition{{Oid: 0, Name: "index1", OwningSchema: "public", OwningTable: "testtable", Def: sql.NullString{String: "CREATE INDEX index1 ON public.testtable USING btree (i)", Valid: true}}}
			backup.PrintCreateIndexStatements(backupfile, tocfile, indexes, indexMetadataMap)

			//Create table whose columns we can index
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.testtable(i int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.testtable")

			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			resultIndexes := backup.GetIndexes(connectionPool)
			Expect(resultIndexes).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&resultIndexes[0], &indexes[0], "Oid")
		})
		It("creates an index used for clustering", func() {
			indexes := []backup.IndexDefinition{{Oid: 0, Name: "index1", OwningSchema: "public", OwningTable: "testtable", Def: sql.NullString{String: "CREATE INDEX index1 ON public.testtable USING btree (i)", Valid: true}, IsClustered: true}}
			backup.PrintCreateIndexStatements(backupfile, tocfile, indexes, indexMetadataMap)

			//Create table whose columns we can index
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.testtable(i int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.testtable")

			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			resultIndexes := backup.GetIndexes(connectionPool)
			Expect(resultIndexes).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&resultIndexes[0], &indexes[0], "Oid")
		})
		It("creates an index with a comment", func() {
			indexes := []backup.IndexDefinition{{Oid: 1, Name: "index1", OwningSchema: "public", OwningTable: "testtable", Def: sql.NullString{String: "CREATE INDEX index1 ON public.testtable USING btree (i)", Valid: true}}}
			indexMetadataMap = testutils.DefaultMetadataMap("INDEX", false, false, true, false)
			indexMetadata := indexMetadataMap[indexes[0].GetUniqueID()]
			backup.PrintCreateIndexStatements(backupfile, tocfile, indexes, indexMetadataMap)

			//Create table whose columns we can index
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.testtable(i int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.testtable")

			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			resultIndexes := backup.GetIndexes(connectionPool)
			resultMetadataMap := backup.GetCommentsForObjectType(connectionPool, backup.TYPE_INDEX)
			resultMetadata := resultMetadataMap[resultIndexes[0].GetUniqueID()]
			Expect(resultIndexes).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&resultIndexes[0], &indexes[0], "Oid")
			structmatcher.ExpectStructsToMatch(&resultMetadata, &indexMetadata)
		})
		It("creates an index in a non-default tablespace", func() {
			if connectionPool.Version.Before("6") {
				testhelper.AssertQueryRuns(connectionPool, "CREATE TABLESPACE test_tablespace FILESPACE test_dir")
			} else {
				testhelper.AssertQueryRuns(connectionPool, "CREATE TABLESPACE test_tablespace LOCATION '/tmp/test_dir'")
			}
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLESPACE test_tablespace")
			indexes := []backup.IndexDefinition{{Oid: 0, Name: "index1", OwningSchema: "public", OwningTable: "testtable", Tablespace: "test_tablespace", Def: sql.NullString{String: "CREATE INDEX index1 ON public.testtable USING btree (i)", Valid: true}}}
			backup.PrintCreateIndexStatements(backupfile, tocfile, indexes, indexMetadataMap)

			//Create table whose columns we can index
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.testtable(i int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.testtable")

			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			resultIndexes := backup.GetIndexes(connectionPool)
			Expect(resultIndexes).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&resultIndexes[0], &indexes[0], "Oid")
		})
		It("creates a unique index used as replica identity", func() {
			testutils.SkipIfBefore6(connectionPool)
			index.Def = sql.NullString{String: "CREATE UNIQUE INDEX index1 ON public.testtable USING btree (i)", Valid: true}
			index.IsReplicaIdentity = true
			indexes := []backup.IndexDefinition{index}
			backup.PrintCreateIndexStatements(backupfile, tocfile, indexes, indexMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.testtable(i int NOT NULL)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.testtable")

			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			resultIndexes := backup.GetIndexes(connectionPool)
			Expect(resultIndexes).To(HaveLen(1))
			resultIndex := resultIndexes[0]
			structmatcher.ExpectStructsToMatchExcluding(&resultIndex, &index, "Oid")
		})
		It("creates an index with statistics on expression columns", func() {
			testutils.SkipIfBefore7(connectionPool)
			indexes := []backup.IndexDefinition{{Oid: 0, Name: "testtable_index", OwningSchema: "public", OwningTable: "testtable", Def: sql.NullString{String: "CREATE INDEX testtable_index ON public.testtable USING btree (i, ((i + 1)), ((j * 2)))", Valid: true}, StatisticsColumns: "2,3", StatisticsValues: "5000,600"}}
			backup.PrintCreateIndexStatements(backupfile, tocfile, indexes, indexMetadataMap)

			//Create table whose columns we can index
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.testtable(i int, j int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.testtable")

			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			resultIndexes := backup.GetIndexes(connectionPool)
			Expect(resultIndexes).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&resultIndexes[0], &indexes[0], "Oid")
		})
		It("creates a parition index and attaches it to the parent index", func() {
			testutils.SkipIfBefore7(connectionPool)

			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.foopart_new (a integer, b integer) PARTITION BY RANGE (b) DISTRIBUTED BY (a)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.foopart_new")
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.foopart_new_p1 (a integer, b integer) DISTRIBUTED BY (a)")
			testhelper.AssertQueryRuns(connectionPool, "ALTER TABLE ONLY public.foopart_new ATTACH PARTITION public.foopart_new_p1 FOR VALUES FROM (0) TO (1)")
			testhelper.AssertQueryRuns(connectionPool, "CREATE INDEX fooidx ON ONLY public.foopart_new USING btree (b)")

			partitionIndex := backup.IndexDefinition{Oid: 0, Name: "foopart_new_p1_b_idx", OwningSchema: "public", OwningTable: "foopart_new_p1", Def: sql.NullString{String: "CREATE INDEX foopart_new_p1_b_idx ON public.foopart_new_p1 USING btree (b)", Valid: true}, ParentIndexFQN: "public.fooidx"}

			indexes := []backup.IndexDefinition{partitionIndex}
			backup.PrintCreateIndexStatements(backupfile, tocfile, indexes, indexMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			partitionIndex.Oid = testutils.OidFromObjectName(connectionPool, "", "foopart_new_p1_b_idx", backup.TYPE_INDEX)
			partitionIndex.ParentIndex = testutils.OidFromObjectName(connectionPool, "", "fooidx", backup.TYPE_INDEX)

			resultIndexes := backup.GetIndexes(connectionPool)
			Expect(resultIndexes).To(HaveLen(2))
			resultIndex := resultIndexes[1]

			structmatcher.ExpectStructsToMatch(&resultIndex, &partitionIndex)
		})
	})
	Describe("PrintCreateRuleStatements", func() {
		var (
			ruleMetadataMap backup.MetadataMap
			ruleDef         string
		)
		BeforeEach(func() {
			ruleMetadataMap = backup.MetadataMap{}
			if connectionPool.Version.Before("6") {
				ruleDef = "CREATE RULE update_notify AS ON UPDATE TO public.testtable DO NOTIFY testtable;"
			} else {
				ruleDef = "CREATE RULE update_notify AS\n    ON UPDATE TO public.testtable DO\n NOTIFY testtable;"
			}
		})
		It("creates a basic rule", func() {
			rules := []backup.RuleDefinition{{Oid: 0, Name: "update_notify", OwningSchema: "public", OwningTable: "testtable", Def: sql.NullString{String: ruleDef, Valid: true}}}
			backup.PrintCreateRuleStatements(backupfile, tocfile, rules, ruleMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.testtable(i int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.testtable")

			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			resultRules := backup.GetRules(connectionPool)
			Expect(resultRules).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&resultRules[0], &rules[0], "Oid")
		})
		It("creates a rule with a comment", func() {
			rules := []backup.RuleDefinition{{Oid: 1, Name: "update_notify", OwningSchema: "public", OwningTable: "testtable", Def: sql.NullString{String: ruleDef, Valid: true}}}
			ruleMetadataMap = testutils.DefaultMetadataMap("RULE", false, false, true, false)
			ruleMetadata := ruleMetadataMap[rules[0].GetUniqueID()]
			backup.PrintCreateRuleStatements(backupfile, tocfile, rules, ruleMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.testtable(i int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.testtable")

			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			rules[0].Oid = testutils.OidFromObjectName(connectionPool, "", "update_notify", backup.TYPE_RULE)
			resultRules := backup.GetRules(connectionPool)
			resultMetadataMap := backup.GetCommentsForObjectType(connectionPool, backup.TYPE_RULE)
			resultMetadata := resultMetadataMap[resultRules[0].GetUniqueID()]
			Expect(resultRules).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&resultRules[0], &rules[0], "Oid")
			structmatcher.ExpectStructsToMatch(&resultMetadata, &ruleMetadata)
		})
	})
	Describe("PrintCreateTriggerStatements", func() {
		var (
			triggerMetadataMap backup.MetadataMap
		)
		BeforeEach(func() {
			triggerMetadataMap = backup.MetadataMap{}
		})
		It("creates a basic trigger", func() {
			triggerDef := `CREATE TRIGGER sync_testtable AFTER INSERT OR DELETE OR UPDATE ON public.testtable FOR EACH STATEMENT EXECUTE PROCEDURE "RI_FKey_check_ins"()`
			if connectionPool.Version.AtLeast("7") {
				triggerDef = `CREATE TRIGGER sync_testtable AFTER INSERT OR DELETE OR UPDATE ON public.testtable FOR EACH ROW EXECUTE FUNCTION "RI_FKey_check_ins"()`
			}
			triggers := []backup.TriggerDefinition{{Oid: 0, Name: "sync_testtable", OwningSchema: "public", OwningTable: "testtable", Def: sql.NullString{String: triggerDef, Valid: true}}}
			backup.PrintCreateTriggerStatements(backupfile, tocfile, triggers, triggerMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.testtable(i int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.testtable")

			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			resultTriggers := backup.GetTriggers(connectionPool)
			Expect(resultTriggers).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&resultTriggers[0], &triggers[0], "Oid")
		})
		It("creates a trigger with a comment", func() {
			triggerDef := `CREATE TRIGGER sync_testtable AFTER INSERT OR DELETE OR UPDATE ON public.testtable FOR EACH STATEMENT EXECUTE PROCEDURE "RI_FKey_check_ins"()`
			if connectionPool.Version.AtLeast("7") {
				triggerDef = `CREATE TRIGGER sync_testtable AFTER INSERT OR DELETE OR UPDATE ON public.testtable FOR EACH ROW EXECUTE FUNCTION "RI_FKey_check_ins"()`
			}
			triggers := []backup.TriggerDefinition{{Oid: 1, Name: "sync_testtable", OwningSchema: "public", OwningTable: "testtable", Def: sql.NullString{String: triggerDef, Valid: true}}}
			triggerMetadataMap = testutils.DefaultMetadataMap("RULE", false, false, true, false)
			triggerMetadata := triggerMetadataMap[triggers[0].GetUniqueID()]
			backup.PrintCreateTriggerStatements(backupfile, tocfile, triggers, triggerMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.testtable(i int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.testtable")

			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			triggers[0].Oid = testutils.OidFromObjectName(connectionPool, "", "sync_testtable", backup.TYPE_TRIGGER)
			resultTriggers := backup.GetTriggers(connectionPool)
			resultMetadataMap := backup.GetCommentsForObjectType(connectionPool, backup.TYPE_TRIGGER)
			resultMetadata := resultMetadataMap[resultTriggers[0].GetUniqueID()]
			Expect(resultTriggers).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&resultTriggers[0], &triggers[0], "Oid")
			structmatcher.ExpectStructsToMatch(&resultMetadata, &triggerMetadata)
		})
	})
	Describe("PrintCreateEventTriggerStatements", func() {
		BeforeEach(func() {
			testutils.SkipIfBefore6(connectionPool)
			testhelper.AssertQueryRuns(connectionPool, `CREATE FUNCTION abort_any_command()
RETURNS event_trigger LANGUAGE plpgsql
AS $$ BEGIN RAISE EXCEPTION 'exception'; END; $$;`)
		})
		AfterEach(func() {
			testhelper.AssertQueryRuns(connectionPool, `DROP FUNCTION abort_any_command()`)
		})
		It("creates a basic event trigger", func() {
			eventTriggers := []backup.EventTrigger{{Oid: 1, Name: "testeventtrigger1", Event: "ddl_command_start", FunctionName: "abort_any_command", Enabled: "O"}}
			eventTriggerMetadataMap := backup.MetadataMap{}
			backup.PrintCreateEventTriggerStatements(backupfile, tocfile, eventTriggers, eventTriggerMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP EVENT TRIGGER testeventtrigger1")

			results := backup.GetEventTriggers(connectionPool)

			Expect(results).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&eventTriggers[0], &results[0], "Oid")
		})
		It("creates an event trigger with multiple filter tags", func() {
			eventTriggers := []backup.EventTrigger{{Oid: 1, Name: "testeventtrigger1", Event: "ddl_command_start", FunctionName: "abort_any_command", Enabled: "O", EventTags: `'DROP FUNCTION', 'DROP TABLE'`}}
			eventTriggerMetadataMap := backup.MetadataMap{}
			backup.PrintCreateEventTriggerStatements(backupfile, tocfile, eventTriggers, eventTriggerMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP EVENT TRIGGER testeventtrigger1")

			results := backup.GetEventTriggers(connectionPool)

			Expect(results).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&eventTriggers[0], &results[0], "Oid")
		})
		It("creates an event trigger with a single filter tag and enable option", func() {
			eventTriggers := []backup.EventTrigger{{Oid: 1, Name: "testeventtrigger1", Event: "ddl_command_start", FunctionName: "abort_any_command", Enabled: "R", EventTags: `'DROP FUNCTION'`}}
			eventTriggerMetadataMap := backup.MetadataMap{}
			backup.PrintCreateEventTriggerStatements(backupfile, tocfile, eventTriggers, eventTriggerMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP EVENT TRIGGER testeventtrigger1")

			results := backup.GetEventTriggers(connectionPool)

			Expect(results).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&eventTriggers[0], &results[0], "Oid")
		})
		It("creates an event trigger with comment, security label, and owner", func() {
			eventTriggers := []backup.EventTrigger{{Oid: 1, Name: "test_event_trigger", Event: "ddl_command_start", FunctionName: "abort_any_command", Enabled: "O"}}
			eventTriggerMetadataMap := testutils.DefaultMetadataMap("EVENT TRIGGER", false, true, true, includeSecurityLabels)
			eventTriggerMetadata := eventTriggerMetadataMap[eventTriggers[0].GetUniqueID()]

			backup.PrintCreateEventTriggerStatements(backupfile, tocfile, []backup.EventTrigger{eventTriggers[0]}, eventTriggerMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP EVENT TRIGGER test_event_trigger")

			resultEventTriggers := backup.GetEventTriggers(connectionPool)
			resultMetadataMap := backup.GetMetadataForObjectType(connectionPool, backup.TYPE_EVENTTRIGGER)

			Expect(resultEventTriggers).To(HaveLen(1))
			uniqueID := testutils.UniqueIDFromObjectName(connectionPool, "", "test_event_trigger", backup.TYPE_EVENTTRIGGER)
			resultMetadata := resultMetadataMap[uniqueID]
			structmatcher.ExpectStructsToMatchExcluding(&eventTriggers[0], &resultEventTriggers[0], "Oid")
			structmatcher.ExpectStructsToMatch(&eventTriggerMetadata, &resultMetadata)

		})
	})
})
