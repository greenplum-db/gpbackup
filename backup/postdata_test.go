package backup_test

import (
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"

	. "github.com/onsi/ginkgo"
)

var _ = Describe("backup/postdata tests", func() {
	BeforeEach(func() {
		toc, backupfile = testutils.InitializeTestTOC(buffer, "postdata")
	})
	Context("PrintCreateIndexStatements", func() {
		It("can print a basic index", func() {
			indexes := []backup.QuerySimpleDefinition{{Oid: 1, Name: "testindex", OwningSchema: "public", OwningTable: "testtable", Tablespace: "", Def: "CREATE INDEX testindex ON public.testtable USING btree(i)"}}
			emptyMetadataMap := backup.MetadataMap{}
			backup.PrintCreateIndexStatements(backupfile, toc, indexes, emptyMetadataMap)
			testutils.ExpectEntry(toc.PostdataEntries, 0, "public", "public.testtable", "testindex", "INDEX")
			testutils.AssertBufferContents(toc.PostdataEntries, buffer, `CREATE INDEX testindex ON public.testtable USING btree(i);`)
		})
		It("can print an index with a tablespace", func() {
			indexes := []backup.QuerySimpleDefinition{{Oid: 1, Name: "testindex", OwningSchema: "public", OwningTable: "testtable", Tablespace: "test_tablespace", Def: "CREATE INDEX testindex ON public.testtable USING btree(i)"}}
			emptyMetadataMap := backup.MetadataMap{}
			backup.PrintCreateIndexStatements(backupfile, toc, indexes, emptyMetadataMap)
			testutils.AssertBufferContents(toc.PostdataEntries, buffer, `CREATE INDEX testindex ON public.testtable USING btree(i);
ALTER INDEX testindex SET TABLESPACE test_tablespace;`)
		})
		It("can print an index with a comment", func() {
			indexes := []backup.QuerySimpleDefinition{{Oid: 1, Name: "testindex", OwningSchema: "public", OwningTable: "testtable", Tablespace: "", Def: "CREATE INDEX testindex ON public.testtable USING btree(i)"}}
			indexMetadataMap := backup.MetadataMap{1: {Comment: "This is an index comment."}}
			backup.PrintCreateIndexStatements(backupfile, toc, indexes, indexMetadataMap)
			testutils.AssertBufferContents(toc.PostdataEntries, buffer, `CREATE INDEX testindex ON public.testtable USING btree(i);

COMMENT ON INDEX testindex IS 'This is an index comment.';`)
		})
	})
	Context("PrintCreateRuleStatements", func() {
		It("can print a basic rule", func() {
			rules := []backup.QuerySimpleDefinition{{Oid: 1, Name: "testrule", OwningSchema: "public", OwningTable: "testtable", Tablespace: "", Def: "CREATE RULE update_notify AS ON UPDATE TO testtable DO NOTIFY testtable;"}}
			emptyMetadataMap := backup.MetadataMap{}
			backup.PrintCreateRuleStatements(backupfile, toc, rules, emptyMetadataMap)
			testutils.ExpectEntry(toc.PostdataEntries, 0, "public", "public.testtable", "testrule", "RULE")
			testutils.AssertBufferContents(toc.PostdataEntries, buffer, `CREATE RULE update_notify AS ON UPDATE TO testtable DO NOTIFY testtable;`)
		})
		It("can print a rule with a comment", func() {
			rules := []backup.QuerySimpleDefinition{{Oid: 1, Name: "testrule", OwningSchema: "public", OwningTable: "testtable", Tablespace: "", Def: "CREATE RULE update_notify AS ON UPDATE TO testtable DO NOTIFY testtable;"}}
			ruleMetadataMap := backup.MetadataMap{1: {Comment: "This is a rule comment."}}
			backup.PrintCreateRuleStatements(backupfile, toc, rules, ruleMetadataMap)
			testutils.AssertBufferContents(toc.PostdataEntries, buffer, `CREATE RULE update_notify AS ON UPDATE TO testtable DO NOTIFY testtable;

COMMENT ON RULE testrule ON public.testtable IS 'This is a rule comment.';`)
		})
	})
	Context("PrintCreateTriggerStatements", func() {
		It("can print a basic trigger", func() {
			triggers := []backup.QuerySimpleDefinition{{Oid: 1, Name: "testtrigger", OwningSchema: "public", OwningTable: "testtable", Tablespace: "", Def: "CREATE TRIGGER sync_testtable AFTER INSERT OR DELETE OR UPDATE ON testtable FOR EACH STATEMENT EXECUTE PROCEDURE flatfile_update_trigger()"}}
			emptyMetadataMap := backup.MetadataMap{}
			backup.PrintCreateTriggerStatements(backupfile, toc, triggers, emptyMetadataMap)
			testutils.ExpectEntry(toc.PostdataEntries, 0, "public", "public.testtable", "testtrigger", "TRIGGER")
			testutils.AssertBufferContents(toc.PostdataEntries, buffer, `CREATE TRIGGER sync_testtable AFTER INSERT OR DELETE OR UPDATE ON testtable FOR EACH STATEMENT EXECUTE PROCEDURE flatfile_update_trigger();`)
		})
		It("can print a trigger with a comment", func() {
			triggers := []backup.QuerySimpleDefinition{{Oid: 1, Name: "testtrigger", OwningSchema: "public", OwningTable: "testtable", Tablespace: "", Def: "CREATE TRIGGER sync_testtable AFTER INSERT OR DELETE OR UPDATE ON testtable FOR EACH STATEMENT EXECUTE PROCEDURE flatfile_update_trigger()"}}
			triggerMetadataMap := backup.MetadataMap{1: {Comment: "This is a trigger comment."}}
			backup.PrintCreateTriggerStatements(backupfile, toc, triggers, triggerMetadataMap)
			testutils.AssertBufferContents(toc.PostdataEntries, buffer, `CREATE TRIGGER sync_testtable AFTER INSERT OR DELETE OR UPDATE ON testtable FOR EACH STATEMENT EXECUTE PROCEDURE flatfile_update_trigger();

COMMENT ON TRIGGER testtrigger ON public.testtable IS 'This is a trigger comment.';`)
		})
	})
})
