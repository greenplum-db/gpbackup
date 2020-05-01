package backup_test

import (
	"database/sql"

	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"

	. "github.com/onsi/ginkgo"
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
			testutils.ExpectEntry(tocfile.PostdataEntries, 0, "public", "public.testtable", "testindex", "INDEX")
			testutils.AssertBufferContents(tocfile.PostdataEntries, buffer, `CREATE INDEX testindex ON public.testtable USING btree(i);`)
		})
		It("can print an index used for clustering", func() {
			index.IsClustered = true
			indexes := []backup.IndexDefinition{index}
			backup.PrintCreateIndexStatements(backupfile, tocfile, indexes, emptyMetadataMap)
			testutils.ExpectEntry(tocfile.PostdataEntries, 0, "public", "public.testtable", "testindex", "INDEX")
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
			indexMetadataMap := testutils.DefaultMetadataMap("INDEX", false, false, true, false)
			backup.PrintCreateIndexStatements(backupfile, tocfile, indexes, indexMetadataMap)
			testutils.AssertBufferContents(tocfile.PostdataEntries, buffer, "CREATE INDEX testindex ON public.testtable USING btree(i);",
				"COMMENT ON INDEX public.testindex IS 'This is an index comment.';")
		})
		It("can print an index that is a replica identity", func() {
			index.IsReplicaIdentity = true
			indexes := []backup.IndexDefinition{index}
			backup.PrintCreateIndexStatements(backupfile, tocfile, indexes, emptyMetadataMap)
			testutils.ExpectEntry(tocfile.PostdataEntries, 0, "public", "public.testtable", "testindex", "INDEX")
			testutils.AssertBufferContents(tocfile.PostdataEntries, buffer,
				"CREATE INDEX testindex ON public.testtable USING btree(i);",
				"ALTER TABLE public.testtable REPLICA IDENTITY USING INDEX testindex;",
			)
		})
	})
	Context("PrintCreateRuleStatements", func() {
		rule := backup.RuleDefinition{Oid: 1, Name: "testrule", OwningSchema: "public", OwningTable: "testtable", Def: sql.NullString{String: "CREATE RULE update_notify AS ON UPDATE TO testtable DO NOTIFY testtable;", Valid: true}}
		It("can print a basic rule", func() {
			rules := []backup.RuleDefinition{rule}
			emptyMetadataMap := backup.MetadataMap{}
			backup.PrintCreateRuleStatements(backupfile, tocfile, rules, emptyMetadataMap)
			testutils.ExpectEntry(tocfile.PostdataEntries, 0, "public", "public.testtable", "testrule", "RULE")
			testutils.AssertBufferContents(tocfile.PostdataEntries, buffer, "CREATE RULE update_notify AS ON UPDATE TO testtable DO NOTIFY testtable;")
		})
		It("can print a rule with a comment", func() {
			rules := []backup.RuleDefinition{rule}
			ruleMetadataMap := testutils.DefaultMetadataMap("RULE", false, false, true, false)
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
			testutils.ExpectEntry(tocfile.PostdataEntries, 0, "public", "public.testtable", "testtrigger", "TRIGGER")
			testutils.AssertBufferContents(tocfile.PostdataEntries, buffer, "CREATE TRIGGER sync_testtable AFTER INSERT OR DELETE OR UPDATE ON testtable FOR EACH STATEMENT EXECUTE PROCEDURE flatfile_update_trigger();")
		})
		It("can print a trigger with a comment", func() {
			triggers := []backup.TriggerDefinition{trigger}
			triggerMetadataMap := testutils.DefaultMetadataMap("TRIGGER", false, false, true, false)
			backup.PrintCreateTriggerStatements(backupfile, tocfile, triggers, triggerMetadataMap)
			testutils.AssertBufferContents(tocfile.PostdataEntries, buffer, "CREATE TRIGGER sync_testtable AFTER INSERT OR DELETE OR UPDATE ON testtable FOR EACH STATEMENT EXECUTE PROCEDURE flatfile_update_trigger();",
				"COMMENT ON TRIGGER testtrigger ON public.testtable IS 'This is a trigger comment.';")
		})
	})
	Context("PrintCreateEventTriggerStatements", func() {
		It("can print a basic event trigger", func() {
			eventTrigger := backup.EventTrigger{Oid: 1, Name: "testeventtrigger", Event: "ddl_command_start", FunctionName: "abort_any_command", Enabled: "O"}
			eventTriggers := []backup.EventTrigger{eventTrigger}
			emptyMetadataMap := backup.MetadataMap{}
			backup.PrintCreateEventTriggerStatements(backupfile, tocfile, eventTriggers, emptyMetadataMap)
			testutils.ExpectEntry(tocfile.PostdataEntries, 0, "", "", "testeventtrigger", "EVENT TRIGGER")
			testutils.AssertBufferContents(tocfile.PostdataEntries, buffer, `CREATE EVENT TRIGGER testeventtrigger
ON ddl_command_start
EXECUTE PROCEDURE abort_any_command();`)
		})
		It("can print a basic event trigger with a comment", func() {
			eventTrigger := backup.EventTrigger{Oid: 1, Name: "testeventtrigger", Event: "ddl_command_start", FunctionName: "abort_any_command", Enabled: "O"}
			eventTriggers := []backup.EventTrigger{eventTrigger}
			eventTriggerMetadataMap := testutils.DefaultMetadataMap("EVENT TRIGGER", false, false, true, false)
			backup.PrintCreateEventTriggerStatements(backupfile, tocfile, eventTriggers, eventTriggerMetadataMap)
			testutils.ExpectEntry(tocfile.PostdataEntries, 0, "", "", "testeventtrigger", "EVENT TRIGGER")
			testutils.AssertBufferContents(tocfile.PostdataEntries, buffer, `CREATE EVENT TRIGGER testeventtrigger
ON ddl_command_start
EXECUTE PROCEDURE abort_any_command();`, `COMMENT ON EVENT TRIGGER testeventtrigger IS 'This is an event trigger comment.';`)
		})
		It("can print a basic event trigger with an owner", func() {
			eventTrigger := backup.EventTrigger{Oid: 1, Name: "testeventtrigger", Event: "ddl_command_start", FunctionName: "abort_any_command", Enabled: "O"}
			eventTriggers := []backup.EventTrigger{eventTrigger}
			eventTriggerMetadataMap := testutils.DefaultMetadataMap("EVENT TRIGGER", false, true, false, false)
			backup.PrintCreateEventTriggerStatements(backupfile, tocfile, eventTriggers, eventTriggerMetadataMap)
			testutils.ExpectEntry(tocfile.PostdataEntries, 0, "", "", "testeventtrigger", "EVENT TRIGGER")
			testutils.AssertBufferContents(tocfile.PostdataEntries, buffer, `CREATE EVENT TRIGGER testeventtrigger
ON ddl_command_start
EXECUTE PROCEDURE abort_any_command();`, `ALTER EVENT TRIGGER testeventtrigger OWNER TO testrole;`)
		})
		It("can print a basic event trigger with a security label", func() {
			eventTrigger := backup.EventTrigger{Oid: 1, Name: "testeventtrigger", Event: "ddl_command_start", FunctionName: "abort_any_command", Enabled: "O"}
			eventTriggers := []backup.EventTrigger{eventTrigger}
			eventTriggerMetadataMap := testutils.DefaultMetadataMap("EVENT TRIGGER", false, false, false, true)
			backup.PrintCreateEventTriggerStatements(backupfile, tocfile, eventTriggers, eventTriggerMetadataMap)
			testutils.ExpectEntry(tocfile.PostdataEntries, 0, "", "", "testeventtrigger", "EVENT TRIGGER")
			testutils.AssertBufferContents(tocfile.PostdataEntries, buffer, `CREATE EVENT TRIGGER testeventtrigger
ON ddl_command_start
EXECUTE PROCEDURE abort_any_command();`, `SECURITY LABEL FOR dummy ON EVENT TRIGGER testeventtrigger IS 'unclassified';`)
		})
		It("can print an event trigger with filter variables", func() {
			eventTrigger := backup.EventTrigger{Oid: 1, Name: "testeventtrigger", Event: "ddl_command_start", FunctionName: "abort_any_command", Enabled: "O", EventTags: `'DROP FUNCTION','DROP TABLE'`}
			eventTriggers := []backup.EventTrigger{eventTrigger}
			emptyMetadataMap := backup.MetadataMap{}
			backup.PrintCreateEventTriggerStatements(backupfile, tocfile, eventTriggers, emptyMetadataMap)
			testutils.ExpectEntry(tocfile.PostdataEntries, 0, "", "", "testeventtrigger", "EVENT TRIGGER")
			testutils.AssertBufferContents(tocfile.PostdataEntries, buffer, `CREATE EVENT TRIGGER testeventtrigger
ON ddl_command_start
WHEN TAG IN ('DROP FUNCTION','DROP TABLE')
EXECUTE PROCEDURE abort_any_command();`)
		})
		It("can print an event trigger with filter variables with enable option DISABLE", func() {
			eventTrigger := backup.EventTrigger{Oid: 1, Name: "testeventtrigger", Event: "ddl_command_start", FunctionName: "abort_any_command", Enabled: "D"}
			eventTriggers := []backup.EventTrigger{eventTrigger}
			emptyMetadataMap := backup.MetadataMap{}
			backup.PrintCreateEventTriggerStatements(backupfile, tocfile, eventTriggers, emptyMetadataMap)
			testutils.ExpectEntry(tocfile.PostdataEntries, 0, "", "", "testeventtrigger", "EVENT TRIGGER")
			testutils.AssertBufferContents(tocfile.PostdataEntries, buffer, `CREATE EVENT TRIGGER testeventtrigger
ON ddl_command_start
EXECUTE PROCEDURE abort_any_command();`, `ALTER EVENT TRIGGER testeventtrigger DISABLE;`)
		})
		It("can print an event trigger with filter variables with enable option ENABLE", func() {
			eventTrigger := backup.EventTrigger{Oid: 1, Name: "testeventtrigger", Event: "ddl_command_start", FunctionName: "abort_any_command", Enabled: ""}
			eventTriggers := []backup.EventTrigger{eventTrigger}
			emptyMetadataMap := backup.MetadataMap{}
			backup.PrintCreateEventTriggerStatements(backupfile, tocfile, eventTriggers, emptyMetadataMap)
			testutils.ExpectEntry(tocfile.PostdataEntries, 0, "", "", "testeventtrigger", "EVENT TRIGGER")
			testutils.AssertBufferContents(tocfile.PostdataEntries, buffer, `CREATE EVENT TRIGGER testeventtrigger
ON ddl_command_start
EXECUTE PROCEDURE abort_any_command();`, `ALTER EVENT TRIGGER testeventtrigger ENABLE;`)
		})
		It("can print an event trigger with filter variables with enable option ENABLE REPLICA", func() {
			eventTrigger := backup.EventTrigger{Oid: 1, Name: "testeventtrigger", Event: "ddl_command_start", FunctionName: "abort_any_command", Enabled: "R"}
			eventTriggers := []backup.EventTrigger{eventTrigger}
			emptyMetadataMap := backup.MetadataMap{}
			backup.PrintCreateEventTriggerStatements(backupfile, tocfile, eventTriggers, emptyMetadataMap)
			testutils.ExpectEntry(tocfile.PostdataEntries, 0, "", "", "testeventtrigger", "EVENT TRIGGER")
			testutils.AssertBufferContents(tocfile.PostdataEntries, buffer, `CREATE EVENT TRIGGER testeventtrigger
ON ddl_command_start
EXECUTE PROCEDURE abort_any_command();`, `ALTER EVENT TRIGGER testeventtrigger ENABLE REPLICA;`)
		})
		It("can print an event trigger with filter variables with enable option ENABLE ALWAYS", func() {
			eventTrigger := backup.EventTrigger{Oid: 1, Name: "testeventtrigger", Event: "ddl_command_start", FunctionName: "abort_any_command", Enabled: "A"}
			eventTriggers := []backup.EventTrigger{eventTrigger}
			emptyMetadataMap := backup.MetadataMap{}
			backup.PrintCreateEventTriggerStatements(backupfile, tocfile, eventTriggers, emptyMetadataMap)
			testutils.ExpectEntry(tocfile.PostdataEntries, 0, "", "", "testeventtrigger", "EVENT TRIGGER")
			testutils.AssertBufferContents(tocfile.PostdataEntries, buffer, `CREATE EVENT TRIGGER testeventtrigger
ON ddl_command_start
EXECUTE PROCEDURE abort_any_command();`, `ALTER EVENT TRIGGER testeventtrigger ENABLE ALWAYS;`)
		})
	})
})
