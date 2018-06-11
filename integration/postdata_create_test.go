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
			testhelper.AssertQueryRuns(connection, "CREATE TABLE public.testtable(i int)")
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.testtable")

			testhelper.AssertQueryRuns(connection, buffer.String())

			resultIndexes := backup.GetIndexes(connection)
			Expect(len(resultIndexes)).To(Equal(1))
			structmatcher.ExpectStructsToMatchExcluding(&resultIndexes[0], &indexes[0], "Oid")
		})
		It("creates an index used for clustering", func() {
			indexes := []backup.IndexDefinition{{Oid: 0, Name: "index1", OwningSchema: "public", OwningTable: "testtable", Def: "CREATE INDEX index1 ON public.testtable USING btree (i)", IsClustered: true}}
			backup.PrintCreateIndexStatements(backupfile, toc, indexes, indexMetadataMap)

			//Create table whose columns we can index
			testhelper.AssertQueryRuns(connection, "CREATE TABLE public.testtable(i int)")
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.testtable")

			testhelper.AssertQueryRuns(connection, buffer.String())

			resultIndexes := backup.GetIndexes(connection)
			Expect(len(resultIndexes)).To(Equal(1))
			structmatcher.ExpectStructsToMatchExcluding(&resultIndexes[0], &indexes[0], "Oid")
		})
		It("creates an index with a comment", func() {
			indexes := []backup.IndexDefinition{{Oid: 1, Name: "index1", OwningSchema: "public", OwningTable: "testtable", Def: "CREATE INDEX index1 ON public.testtable USING btree (i)"}}
			indexMetadataMap = testutils.DefaultMetadataMap("INDEX", false, false, true)
			indexMetadata := indexMetadataMap[1]
			backup.PrintCreateIndexStatements(backupfile, toc, indexes, indexMetadataMap)

			//Create table whose columns we can index
			testhelper.AssertQueryRuns(connection, "CREATE TABLE public.testtable(i int)")
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.testtable")

			testhelper.AssertQueryRuns(connection, buffer.String())

			indexes[0].Oid = testutils.OidFromObjectName(connection, "", "index1", backup.TYPE_INDEX)
			resultIndexes := backup.GetIndexes(connection)
			resultMetadataMap := backup.GetCommentsForObjectType(connection, backup.TYPE_INDEX)
			resultMetadata := resultMetadataMap[indexes[0].Oid]
			Expect(len(resultIndexes)).To(Equal(1))
			structmatcher.ExpectStructsToMatchExcluding(&resultIndexes[0], &indexes[0], "Oid")
			structmatcher.ExpectStructsToMatch(&resultMetadata, &indexMetadata)
		})
		It("creates an index in a non-default tablespace", func() {
			if connection.Version.Before("6") {
				testhelper.AssertQueryRuns(connection, "CREATE TABLESPACE test_tablespace FILESPACE test_dir")
			} else {
				testhelper.AssertQueryRuns(connection, "CREATE TABLESPACE test_tablespace LOCATION '/tmp/test_dir'")
			}
			defer testhelper.AssertQueryRuns(connection, "DROP TABLESPACE test_tablespace")
			indexes := []backup.IndexDefinition{{Oid: 0, Name: "index1", OwningSchema: "public", OwningTable: "testtable", Tablespace: "test_tablespace", Def: "CREATE INDEX index1 ON public.testtable USING btree (i)"}}
			backup.PrintCreateIndexStatements(backupfile, toc, indexes, indexMetadataMap)

			//Create table whose columns we can index
			testhelper.AssertQueryRuns(connection, "CREATE TABLE public.testtable(i int)")
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.testtable")

			testhelper.AssertQueryRuns(connection, buffer.String())

			resultIndexes := backup.GetIndexes(connection)
			Expect(len(resultIndexes)).To(Equal(1))
			structmatcher.ExpectStructsToMatchExcluding(&resultIndexes[0], &indexes[0], "Oid")
		})
	})
	Describe("PrintCreateRuleStatements", func() {
		var (
			ruleMetadataMap backup.MetadataMap
		)
		BeforeEach(func() {
			ruleMetadataMap = backup.MetadataMap{}
		})
		It("creates a basic rule", func() {
			rules := []backup.QuerySimpleDefinition{{Oid: 0, Name: "update_notify", OwningSchema: "public", OwningTable: "testtable", Def: "CREATE RULE update_notify AS ON UPDATE TO public.testtable DO NOTIFY testtable;"}}
			backup.PrintCreateRuleStatements(backupfile, toc, rules, ruleMetadataMap)

			testhelper.AssertQueryRuns(connection, "CREATE TABLE public.testtable(i int)")
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.testtable")

			testhelper.AssertQueryRuns(connection, buffer.String())

			resultRules := backup.GetRules(connection)
			Expect(len(resultRules)).To(Equal(1))
			structmatcher.ExpectStructsToMatchExcluding(&resultRules[0], &rules[0], "Oid")
		})
		It("creates a rule with a comment", func() {
			rules := []backup.QuerySimpleDefinition{{Oid: 1, Name: "update_notify", OwningSchema: "public", OwningTable: "testtable", Def: "CREATE RULE update_notify AS ON UPDATE TO public.testtable DO NOTIFY testtable;"}}
			ruleMetadataMap = testutils.DefaultMetadataMap("RULE", false, false, true)
			ruleMetadata := ruleMetadataMap[1]
			backup.PrintCreateRuleStatements(backupfile, toc, rules, ruleMetadataMap)

			testhelper.AssertQueryRuns(connection, "CREATE TABLE public.testtable(i int)")
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.testtable")

			testhelper.AssertQueryRuns(connection, buffer.String())

			rules[0].Oid = testutils.OidFromObjectName(connection, "", "update_notify", backup.TYPE_RULE)
			resultRules := backup.GetRules(connection)
			resultMetadataMap := backup.GetCommentsForObjectType(connection, backup.TYPE_RULE)
			resultMetadata := resultMetadataMap[rules[0].Oid]
			Expect(len(resultRules)).To(Equal(1))
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
			triggers := []backup.QuerySimpleDefinition{{Oid: 0, Name: "sync_testtable", OwningSchema: "public", OwningTable: "testtable", Def: `CREATE TRIGGER sync_testtable AFTER INSERT OR DELETE OR UPDATE ON public.testtable FOR EACH STATEMENT EXECUTE PROCEDURE "RI_FKey_check_ins"()`}}
			backup.PrintCreateTriggerStatements(backupfile, toc, triggers, triggerMetadataMap)

			testhelper.AssertQueryRuns(connection, "CREATE TABLE public.testtable(i int)")
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.testtable")

			testhelper.AssertQueryRuns(connection, buffer.String())

			resultTriggers := backup.GetTriggers(connection)
			Expect(len(resultTriggers)).To(Equal(1))
			structmatcher.ExpectStructsToMatchExcluding(&resultTriggers[0], &triggers[0], "Oid")
		})
		It("creates a trigger with a comment", func() {
			triggers := []backup.QuerySimpleDefinition{{Oid: 1, Name: "sync_testtable", OwningSchema: "public", OwningTable: "testtable", Def: `CREATE TRIGGER sync_testtable AFTER INSERT OR DELETE OR UPDATE ON public.testtable FOR EACH STATEMENT EXECUTE PROCEDURE "RI_FKey_check_ins"()`}}
			triggerMetadataMap = testutils.DefaultMetadataMap("RULE", false, false, true)
			triggerMetadata := triggerMetadataMap[1]
			backup.PrintCreateTriggerStatements(backupfile, toc, triggers, triggerMetadataMap)

			testhelper.AssertQueryRuns(connection, "CREATE TABLE public.testtable(i int)")
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.testtable")

			testhelper.AssertQueryRuns(connection, buffer.String())

			triggers[0].Oid = testutils.OidFromObjectName(connection, "", "sync_testtable", backup.TYPE_TRIGGER)
			resultTriggers := backup.GetTriggers(connection)
			resultMetadataMap := backup.GetCommentsForObjectType(connection, backup.TYPE_TRIGGER)
			resultMetadata := resultMetadataMap[triggers[0].Oid]
			Expect(len(resultTriggers)).To(Equal(1))
			structmatcher.ExpectStructsToMatchExcluding(&resultTriggers[0], &triggers[0], "Oid")
			structmatcher.ExpectStructsToMatch(&resultMetadata, &triggerMetadata)
		})
	})
})
