package backup_test

import (
	"database/sql"
	"fmt"

	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/toc"

	. "github.com/onsi/ginkgo/v2"
)

var _ = Describe("backup/postdata tests", func() {
	var emptyMetadataMap = backup.MetadataMap{}
	BeforeEach(func() {
		tocfile, backupfile = testutils.InitializeTestTOC(buffer, "postdata")
	})
	Context("PrintCreateIndexStatements", func() {
		var index backup.IndexDefinition
		BeforeEach(func() {
			index = backup.IndexDefinition{Oid: 1, Name: "testindex", OwningSchema: "public", OwningTable: "testtable", Def: sql.NullString{String: "CREATE INDEX testindex ON public.testtable USING btree(i)", Valid: true}}
		})
		It("can print a basic index", func() {
			indexes := []backup.IndexDefinition{index}
			backup.PrintCreateIndexStatements(backupfile, tocfile, indexes, emptyMetadataMap)
			testutils.ExpectEntry(tocfile.PostdataEntries, 0, "public", "public.testtable", "testindex", toc.OBJ_INDEX)
			testutils.AssertBufferContents(tocfile.PostdataEntries, buffer, `CREATE INDEX testindex ON public.testtable USING btree(i);`)
		})
		It("can print an index used for clustering", func() {
			index.IsClustered = true
			indexes := []backup.IndexDefinition{index}
			backup.PrintCreateIndexStatements(backupfile, tocfile, indexes, emptyMetadataMap)
			testutils.ExpectEntry(tocfile.PostdataEntries, 0, "public", "public.testtable", "testindex", toc.OBJ_INDEX)
			testutils.AssertBufferContents(tocfile.PostdataEntries, buffer, "CREATE INDEX testindex ON public.testtable USING btree(i);",
				"ALTER TABLE public.testtable CLUSTER ON testindex;")
		})
		It("can print an index with a tablespace", func() {
			index.Tablespace = "test_tablespace"
			indexes := []backup.IndexDefinition{index}
			backup.PrintCreateIndexStatements(backupfile, tocfile, indexes, emptyMetadataMap)
			testutils.AssertBufferContents(tocfile.PostdataEntries, buffer, "CREATE INDEX testindex ON public.testtable USING btree(i);",
				"ALTER INDEX public.testindex SET TABLESPACE test_tablespace;")
		})
		It("can print an index with a comment", func() {
			indexes := []backup.IndexDefinition{index}
			indexMetadataMap := testutils.DefaultMetadataMap(toc.OBJ_INDEX, false, false, true, false)
			backup.PrintCreateIndexStatements(backupfile, tocfile, indexes, indexMetadataMap)
			testutils.AssertBufferContents(tocfile.PostdataEntries, buffer, "CREATE INDEX testindex ON public.testtable USING btree(i);",
				"COMMENT ON INDEX public.testindex IS 'This is an index comment.';")
		})
		It("can print an index that is a replica identity", func() {
			index.IsReplicaIdentity = true
			indexes := []backup.IndexDefinition{index}
			backup.PrintCreateIndexStatements(backupfile, tocfile, indexes, emptyMetadataMap)
			testutils.ExpectEntry(tocfile.PostdataEntries, 0, "public", "public.testtable", "testindex", toc.OBJ_INDEX)
			testutils.AssertBufferContents(tocfile.PostdataEntries, buffer,
				"CREATE INDEX testindex ON public.testtable USING btree(i);",
				"ALTER TABLE public.testtable REPLICA IDENTITY USING INDEX testindex;",
			)
		})
		It("can print an index of a partition that is attached to a parent partition index", func() {
			testutils.SkipIfBefore7(connectionPool)

			parentIndex := backup.IndexDefinition{Oid: 2, Name: "parenttestindex", OwningSchema: "public", OwningTable: "parenttesttable", Def: sql.NullString{String: "CREATE INDEX parenttestindex ON public.parenttesttable USING btree(i)", Valid: true}}
			index.ParentIndexFQN = "public.parenttestindex"
			indexes := []backup.IndexDefinition{parentIndex, index}
			backup.PrintCreateIndexStatements(backupfile, tocfile, indexes, emptyMetadataMap)
			testutils.ExpectEntry(tocfile.PostdataEntries, 0, "public", "public.parenttesttable", "parenttestindex", toc.OBJ_INDEX)
			testutils.ExpectEntry(tocfile.PostdataEntries, 1, "public", "public.testtable", "testindex", toc.OBJ_INDEX)
			testutils.ExpectEntry(tocfile.PostdataEntries, 2, "public", "public.testindex", "testindex", "INDEX METADATA")
			testutils.AssertBufferContents(tocfile.PostdataEntries, buffer,
				"CREATE INDEX parenttestindex ON public.parenttesttable USING btree(i);",
				"CREATE INDEX testindex ON public.testtable USING btree(i);",
				"ALTER INDEX public.parenttestindex ATTACH PARTITION public.testindex;",
			)
		})
	})
	Context("PrintCreateRuleStatements", func() {
		rule := backup.RuleDefinition{Oid: 1, Name: "testrule", OwningSchema: "public", OwningTable: "testtable", Def: sql.NullString{String: "CREATE RULE update_notify AS ON UPDATE TO testtable DO NOTIFY testtable;", Valid: true}}
		It("can print a basic rule", func() {
			rules := []backup.RuleDefinition{rule}
			emptyMetadataMap := backup.MetadataMap{}
			backup.PrintCreateRuleStatements(backupfile, tocfile, rules, emptyMetadataMap)
			testutils.ExpectEntry(tocfile.PostdataEntries, 0, "public", "public.testtable", "testrule", toc.OBJ_RULE)
			testutils.AssertBufferContents(tocfile.PostdataEntries, buffer, "CREATE RULE update_notify AS ON UPDATE TO testtable DO NOTIFY testtable;")
		})
		It("can print a rule with a comment", func() {
			rules := []backup.RuleDefinition{rule}
			ruleMetadataMap := testutils.DefaultMetadataMap(toc.OBJ_RULE, false, false, true, false)
			backup.PrintCreateRuleStatements(backupfile, tocfile, rules, ruleMetadataMap)
			testutils.AssertBufferContents(tocfile.PostdataEntries, buffer, "CREATE RULE update_notify AS ON UPDATE TO testtable DO NOTIFY testtable;",
				"COMMENT ON RULE testrule ON public.testtable IS 'This is a rule comment.';")
		})
	})
	Context("PrintCreateTriggerStatements", func() {
		trigger := backup.TriggerDefinition{Oid: 1, Name: "testtrigger", OwningSchema: "public", OwningTable: "testtable", Def: sql.NullString{String: "CREATE TRIGGER sync_testtable AFTER INSERT OR DELETE OR UPDATE ON testtable FOR EACH STATEMENT EXECUTE PROCEDURE flatfile_update_trigger()", Valid: true}}
		It("can print a basic trigger", func() {
			triggers := []backup.TriggerDefinition{trigger}
			emptyMetadataMap := backup.MetadataMap{}
			backup.PrintCreateTriggerStatements(backupfile, tocfile, triggers, emptyMetadataMap)
			testutils.ExpectEntry(tocfile.PostdataEntries, 0, "public", "public.testtable", "testtrigger", toc.OBJ_TRIGGER)
			testutils.AssertBufferContents(tocfile.PostdataEntries, buffer, "CREATE TRIGGER sync_testtable AFTER INSERT OR DELETE OR UPDATE ON testtable FOR EACH STATEMENT EXECUTE PROCEDURE flatfile_update_trigger();")
		})
		It("can print a trigger with a comment", func() {
			triggers := []backup.TriggerDefinition{trigger}
			triggerMetadataMap := testutils.DefaultMetadataMap(toc.OBJ_TRIGGER, false, false, true, false)
			backup.PrintCreateTriggerStatements(backupfile, tocfile, triggers, triggerMetadataMap)
			testutils.AssertBufferContents(tocfile.PostdataEntries, buffer, "CREATE TRIGGER sync_testtable AFTER INSERT OR DELETE OR UPDATE ON testtable FOR EACH STATEMENT EXECUTE PROCEDURE flatfile_update_trigger();",
				"COMMENT ON TRIGGER testtrigger ON public.testtable IS 'This is a trigger comment.';")
		})
	})
	Context("PrintCreateEventTriggerStatements", func() {
		var evTrigExecReplacement string
		BeforeEach(func() {
			if connectionPool.Version.AtLeast("7") {
				evTrigExecReplacement = toc.OBJ_FUNCTION
			} else {
				evTrigExecReplacement = toc.OBJ_PROCEDURE
			}
		})
		It("can print a basic event trigger", func() {
			eventTrigger := backup.EventTrigger{Oid: 1, Name: "testeventtrigger", Event: "ddl_command_start", FunctionName: "abort_any_command", Enabled: "O"}
			eventTriggers := []backup.EventTrigger{eventTrigger}
			emptyMetadataMap := backup.MetadataMap{}
			backup.PrintCreateEventTriggerStatements(backupfile, tocfile, eventTriggers, emptyMetadataMap)
			testutils.ExpectEntry(tocfile.PostdataEntries, 0, "", "", "testeventtrigger", toc.OBJ_EVENT_TRIGGER)
			testutils.AssertBufferContents(tocfile.PostdataEntries, buffer, fmt.Sprintf(`CREATE EVENT TRIGGER testeventtrigger
ON ddl_command_start
EXECUTE %s abort_any_command();`, evTrigExecReplacement))
		})
		It("can print a basic event trigger with a comment", func() {
			eventTrigger := backup.EventTrigger{Oid: 1, Name: "testeventtrigger", Event: "ddl_command_start", FunctionName: "abort_any_command", Enabled: "O"}
			eventTriggers := []backup.EventTrigger{eventTrigger}
			eventTriggerMetadataMap := testutils.DefaultMetadataMap(toc.OBJ_EVENT_TRIGGER, false, false, true, false)
			backup.PrintCreateEventTriggerStatements(backupfile, tocfile, eventTriggers, eventTriggerMetadataMap)
			testutils.ExpectEntry(tocfile.PostdataEntries, 0, "", "", "testeventtrigger", toc.OBJ_EVENT_TRIGGER)
			testutils.AssertBufferContents(tocfile.PostdataEntries, buffer, fmt.Sprintf(`CREATE EVENT TRIGGER testeventtrigger
ON ddl_command_start
EXECUTE %s abort_any_command();`, evTrigExecReplacement), `COMMENT ON EVENT TRIGGER testeventtrigger IS 'This is an event trigger comment.';`)
		})
		It("can print a basic event trigger with an owner", func() {
			eventTrigger := backup.EventTrigger{Oid: 1, Name: "testeventtrigger", Event: "ddl_command_start", FunctionName: "abort_any_command", Enabled: "O"}
			eventTriggers := []backup.EventTrigger{eventTrigger}
			eventTriggerMetadataMap := testutils.DefaultMetadataMap(toc.OBJ_EVENT_TRIGGER, false, true, false, false)
			backup.PrintCreateEventTriggerStatements(backupfile, tocfile, eventTriggers, eventTriggerMetadataMap)
			testutils.ExpectEntry(tocfile.PostdataEntries, 0, "", "", "testeventtrigger", toc.OBJ_EVENT_TRIGGER)
			testutils.AssertBufferContents(tocfile.PostdataEntries, buffer, fmt.Sprintf(`CREATE EVENT TRIGGER testeventtrigger
ON ddl_command_start
EXECUTE %s abort_any_command();`, evTrigExecReplacement), `ALTER EVENT TRIGGER testeventtrigger OWNER TO testrole;`)
		})
		It("can print a basic event trigger with a security label", func() {
			eventTrigger := backup.EventTrigger{Oid: 1, Name: "testeventtrigger", Event: "ddl_command_start", FunctionName: "abort_any_command", Enabled: "O"}
			eventTriggers := []backup.EventTrigger{eventTrigger}
			eventTriggerMetadataMap := testutils.DefaultMetadataMap(toc.OBJ_EVENT_TRIGGER, false, false, false, true)
			backup.PrintCreateEventTriggerStatements(backupfile, tocfile, eventTriggers, eventTriggerMetadataMap)
			testutils.ExpectEntry(tocfile.PostdataEntries, 0, "", "", "testeventtrigger", toc.OBJ_EVENT_TRIGGER)
			testutils.AssertBufferContents(tocfile.PostdataEntries, buffer, fmt.Sprintf(`CREATE EVENT TRIGGER testeventtrigger
ON ddl_command_start
EXECUTE %s abort_any_command();`, evTrigExecReplacement), `SECURITY LABEL FOR dummy ON EVENT TRIGGER testeventtrigger IS 'unclassified';`)
		})
		It("can print an event trigger with filter variables", func() {
			eventTrigger := backup.EventTrigger{Oid: 1, Name: "testeventtrigger", Event: "ddl_command_start", FunctionName: "abort_any_command", Enabled: "O", EventTags: `'DROP FUNCTION','DROP TABLE'`}
			eventTriggers := []backup.EventTrigger{eventTrigger}
			emptyMetadataMap := backup.MetadataMap{}
			backup.PrintCreateEventTriggerStatements(backupfile, tocfile, eventTriggers, emptyMetadataMap)
			testutils.ExpectEntry(tocfile.PostdataEntries, 0, "", "", "testeventtrigger", toc.OBJ_EVENT_TRIGGER)
			testutils.AssertBufferContents(tocfile.PostdataEntries, buffer, fmt.Sprintf(`CREATE EVENT TRIGGER testeventtrigger
ON ddl_command_start
WHEN TAG IN ('DROP FUNCTION','DROP TABLE')
EXECUTE %s abort_any_command();`, evTrigExecReplacement))
		})
		It("can print an event trigger with filter variables with enable option DISABLE", func() {
			eventTrigger := backup.EventTrigger{Oid: 1, Name: "testeventtrigger", Event: "ddl_command_start", FunctionName: "abort_any_command", Enabled: "D"}
			eventTriggers := []backup.EventTrigger{eventTrigger}
			emptyMetadataMap := backup.MetadataMap{}
			backup.PrintCreateEventTriggerStatements(backupfile, tocfile, eventTriggers, emptyMetadataMap)
			testutils.ExpectEntry(tocfile.PostdataEntries, 0, "", "", "testeventtrigger", toc.OBJ_EVENT_TRIGGER)
			testutils.AssertBufferContents(tocfile.PostdataEntries, buffer, fmt.Sprintf(`CREATE EVENT TRIGGER testeventtrigger
ON ddl_command_start
EXECUTE %s abort_any_command();`, evTrigExecReplacement), `ALTER EVENT TRIGGER testeventtrigger DISABLE;`)
		})
		It("can print an event trigger with filter variables with enable option ENABLE", func() {
			eventTrigger := backup.EventTrigger{Oid: 1, Name: "testeventtrigger", Event: "ddl_command_start", FunctionName: "abort_any_command", Enabled: ""}
			eventTriggers := []backup.EventTrigger{eventTrigger}
			emptyMetadataMap := backup.MetadataMap{}
			backup.PrintCreateEventTriggerStatements(backupfile, tocfile, eventTriggers, emptyMetadataMap)
			testutils.ExpectEntry(tocfile.PostdataEntries, 0, "", "", "testeventtrigger", toc.OBJ_EVENT_TRIGGER)
			testutils.AssertBufferContents(tocfile.PostdataEntries, buffer, fmt.Sprintf(`CREATE EVENT TRIGGER testeventtrigger
ON ddl_command_start
EXECUTE %s abort_any_command();`, evTrigExecReplacement), `ALTER EVENT TRIGGER testeventtrigger ENABLE;`)
		})
		It("can print an event trigger with filter variables with enable option ENABLE REPLICA", func() {
			eventTrigger := backup.EventTrigger{Oid: 1, Name: "testeventtrigger", Event: "ddl_command_start", FunctionName: "abort_any_command", Enabled: "R"}
			eventTriggers := []backup.EventTrigger{eventTrigger}
			emptyMetadataMap := backup.MetadataMap{}
			backup.PrintCreateEventTriggerStatements(backupfile, tocfile, eventTriggers, emptyMetadataMap)
			testutils.ExpectEntry(tocfile.PostdataEntries, 0, "", "", "testeventtrigger", toc.OBJ_EVENT_TRIGGER)
			testutils.AssertBufferContents(tocfile.PostdataEntries, buffer, fmt.Sprintf(`CREATE EVENT TRIGGER testeventtrigger
ON ddl_command_start
EXECUTE %s abort_any_command();`, evTrigExecReplacement), `ALTER EVENT TRIGGER testeventtrigger ENABLE REPLICA;`)
		})
		It("can print an event trigger with filter variables with enable option ENABLE ALWAYS", func() {
			eventTrigger := backup.EventTrigger{Oid: 1, Name: "testeventtrigger", Event: "ddl_command_start", FunctionName: "abort_any_command", Enabled: "A"}
			eventTriggers := []backup.EventTrigger{eventTrigger}
			emptyMetadataMap := backup.MetadataMap{}
			backup.PrintCreateEventTriggerStatements(backupfile, tocfile, eventTriggers, emptyMetadataMap)
			testutils.ExpectEntry(tocfile.PostdataEntries, 0, "", "", "testeventtrigger", toc.OBJ_EVENT_TRIGGER)
			testutils.AssertBufferContents(tocfile.PostdataEntries, buffer, fmt.Sprintf(`CREATE EVENT TRIGGER testeventtrigger
ON ddl_command_start
EXECUTE %s abort_any_command();`, evTrigExecReplacement), `ALTER EVENT TRIGGER testeventtrigger ENABLE ALWAYS;`)
		})
	})
	Context("PrintCreateExtendedStatistics", func() {
		statExt := backup.StatisticExt{Oid: 1, Name: "test_stat", Namespace: "public", Owner: "username", TableSchema: "myschema", TableName: "mytable", Definition: "CREATE STATISTICS public.test_stat (dependencies) ON a, b FROM myschema.mytable"}
		It("can print an extended statistics", func() {
			stats := []backup.StatisticExt{statExt}
			emptyMetadataMap := backup.MetadataMap{}
			backup.PrintCreateExtendedStatistics(backupfile, tocfile, stats, emptyMetadataMap)
			testutils.ExpectEntry(tocfile.PostdataEntries, 0, "public", "myschema.mytable", "test_stat", toc.OBJ_STATISTICS)
			testutils.AssertBufferContents(tocfile.PostdataEntries, buffer, `CREATE STATISTICS public.test_stat (dependencies) ON a, b FROM myschema.mytable;`)
		})
	})
})
