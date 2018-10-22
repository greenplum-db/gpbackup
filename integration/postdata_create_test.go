package integration

import (
	"github.com/greenplum-db/gp-common-go-libs/structmatcher"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup integration create statement tests", func() {
	BeforeEach(func() {
		toc, backupfile = testutils.InitializeTestTOC(buffer, "predata")
	})
	Describe("PrintCreateIndexStatements", func() {
		var (
			indexMetadataMap backup.MetadataMap
		)
		BeforeEach(func() {
			indexMetadataMap = backup.MetadataMap{}
		})
		It("creates a basic index", func() {
			indexes := []backup.IndexDefinition{{Oid: 0, Name: "index1", OwningSchema: "public", OwningTable: "testtable", Def: "CREATE INDEX index1 ON public.testtable USING btree (i)"}}
			backup.PrintCreateIndexStatements(backupfile, toc, indexes, indexMetadataMap)

			//Create table whose columns we can index
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.testtable(i int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.testtable")

			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			resultIndexes := backup.GetIndexes(connectionPool)
			Expect(resultIndexes).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&resultIndexes[0], &indexes[0], "Oid")
		})
		It("creates an index used for clustering", func() {
			indexes := []backup.IndexDefinition{{Oid: 0, Name: "index1", OwningSchema: "public", OwningTable: "testtable", Def: "CREATE INDEX index1 ON public.testtable USING btree (i)", IsClustered: true}}
			backup.PrintCreateIndexStatements(backupfile, toc, indexes, indexMetadataMap)

			//Create table whose columns we can index
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.testtable(i int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.testtable")

			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			resultIndexes := backup.GetIndexes(connectionPool)
			Expect(resultIndexes).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&resultIndexes[0], &indexes[0], "Oid")
		})
		It("creates an index with a comment", func() {
			indexes := []backup.IndexDefinition{{Oid: 1, Name: "index1", OwningSchema: "public", OwningTable: "testtable", Def: "CREATE INDEX index1 ON public.testtable USING btree (i)"}}
			indexMetadataMap = testutils.DefaultMetadataMap("INDEX", false, false, true)
			indexMetadata := indexMetadataMap[indexes[0].GetUniqueID()]
			backup.PrintCreateIndexStatements(backupfile, toc, indexes, indexMetadataMap)

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
			indexes := []backup.IndexDefinition{{Oid: 0, Name: "index1", OwningSchema: "public", OwningTable: "testtable", Tablespace: "test_tablespace", Def: "CREATE INDEX index1 ON public.testtable USING btree (i)"}}
			backup.PrintCreateIndexStatements(backupfile, toc, indexes, indexMetadataMap)

			//Create table whose columns we can index
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.testtable(i int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.testtable")

			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			resultIndexes := backup.GetIndexes(connectionPool)
			Expect(resultIndexes).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&resultIndexes[0], &indexes[0], "Oid")
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
			rules := []backup.QuerySimpleDefinition{{ClassID: backup.PG_REWRITE_OID, Oid: 0, Name: "update_notify", OwningSchema: "public", OwningTable: "testtable", Def: ruleDef}}
			backup.PrintCreateRuleStatements(backupfile, toc, rules, ruleMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.testtable(i int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.testtable")

			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			resultRules := backup.GetRules(connectionPool)
			Expect(resultRules).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&resultRules[0], &rules[0], "Oid")
		})
		It("creates a rule with a comment", func() {
			rules := []backup.QuerySimpleDefinition{{ClassID: backup.PG_REWRITE_OID, Oid: 1, Name: "update_notify", OwningSchema: "public", OwningTable: "testtable", Def: ruleDef}}
			ruleMetadataMap = testutils.DefaultMetadataMap("RULE", false, false, true)
			ruleMetadata := ruleMetadataMap[rules[0].GetUniqueID()]
			backup.PrintCreateRuleStatements(backupfile, toc, rules, ruleMetadataMap)

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
			triggers := []backup.QuerySimpleDefinition{{ClassID: backup.PG_TRIGGER_OID, Oid: 0, Name: "sync_testtable", OwningSchema: "public", OwningTable: "testtable", Def: `CREATE TRIGGER sync_testtable AFTER INSERT OR DELETE OR UPDATE ON public.testtable FOR EACH STATEMENT EXECUTE PROCEDURE "RI_FKey_check_ins"()`}}
			backup.PrintCreateTriggerStatements(backupfile, toc, triggers, triggerMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.testtable(i int)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.testtable")

			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			resultTriggers := backup.GetTriggers(connectionPool)
			Expect(resultTriggers).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&resultTriggers[0], &triggers[0], "Oid")
		})
		It("creates a trigger with a comment", func() {
			triggers := []backup.QuerySimpleDefinition{{ClassID: backup.PG_TRIGGER_OID, Oid: 1, Name: "sync_testtable", OwningSchema: "public", OwningTable: "testtable", Def: `CREATE TRIGGER sync_testtable AFTER INSERT OR DELETE OR UPDATE ON public.testtable FOR EACH STATEMENT EXECUTE PROCEDURE "RI_FKey_check_ins"()`}}
			triggerMetadataMap = testutils.DefaultMetadataMap("RULE", false, false, true)
			triggerMetadata := triggerMetadataMap[triggers[0].GetUniqueID()]
			backup.PrintCreateTriggerStatements(backupfile, toc, triggers, triggerMetadataMap)

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
			backup.PrintCreateEventTriggerStatements(backupfile, toc, eventTriggers, eventTriggerMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP EVENT TRIGGER testeventtrigger1")

			results := backup.GetEventTriggers(connectionPool)

			Expect(results).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&eventTriggers[0], &results[0], "Oid")
		})
		It("creates an event trigger with multiple filter tags", func() {
			eventTriggers := []backup.EventTrigger{{Oid: 1, Name: "testeventtrigger1", Event: "ddl_command_start", FunctionName: "abort_any_command", Enabled: "O", EventTags: `'DROP FUNCTION', 'DROP TABLE'`}}
			eventTriggerMetadataMap := backup.MetadataMap{}
			backup.PrintCreateEventTriggerStatements(backupfile, toc, eventTriggers, eventTriggerMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP EVENT TRIGGER testeventtrigger1")

			results := backup.GetEventTriggers(connectionPool)

			Expect(results).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&eventTriggers[0], &results[0], "Oid")
		})
		It("creates an event trigger with a single filter tag and enable option", func() {
			eventTriggers := []backup.EventTrigger{{Oid: 1, Name: "testeventtrigger1", Event: "ddl_command_start", FunctionName: "abort_any_command", Enabled: "R", EventTags: `'DROP FUNCTION'`}}
			eventTriggerMetadataMap := backup.MetadataMap{}
			backup.PrintCreateEventTriggerStatements(backupfile, toc, eventTriggers, eventTriggerMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP EVENT TRIGGER testeventtrigger1")

			results := backup.GetEventTriggers(connectionPool)

			Expect(results).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&eventTriggers[0], &results[0], "Oid")
		})
		It("creates an event trigger with comment and owner", func() {
			eventTriggers := []backup.EventTrigger{{Oid: 1, Name: "test_event_trigger", Event: "ddl_command_start", FunctionName: "abort_any_command", Enabled: "O"}}
			eventTriggerMetadataMap := testutils.DefaultMetadataMap("EVENT TRIGGER", false, true, true)
			eventTriggerMetadata := eventTriggerMetadataMap[eventTriggers[0].GetUniqueID()]

			backup.PrintCreateEventTriggerStatements(backupfile, toc, []backup.EventTrigger{eventTriggers[0]}, eventTriggerMetadataMap)

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
