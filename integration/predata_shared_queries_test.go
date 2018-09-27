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
	Describe("GetAllUserSchemas", func() {
		It("returns user schema information", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE SCHEMA bar")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA bar")
			schemas := backup.GetAllUserSchemas(connectionPool)

			schemaBar := backup.Schema{Oid: 0, Name: "bar"}
			schemaPublic := backup.Schema{Oid: 2200, Name: "public"}

			Expect(schemas).To(HaveLen(2))
			structmatcher.ExpectStructsToMatchExcluding(&schemaBar, &schemas[0], "Oid")
			structmatcher.ExpectStructsToMatchExcluding(&schemaPublic, &schemas[1], "Owner")
		})

		It("returns schema information for single specific schema", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE SCHEMA bar")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA bar")
			backupCmdFlags.Set(utils.INCLUDE_SCHEMA, "bar")

			schemas := backup.GetAllUserSchemas(connectionPool)

			schemaBar := backup.Schema{Oid: 0, Name: "bar"}

			Expect(schemas).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&schemaBar, &schemas[0], "Oid")

		})

		It("returns schema information for multiple specific schemas", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE SCHEMA bar")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA bar")
			backupCmdFlags.Set(utils.INCLUDE_SCHEMA, "bar,public")
			schemas := backup.GetAllUserSchemas(connectionPool)

			schemaBar := backup.Schema{Oid: 0, Name: "bar"}
			schemaPublic := backup.Schema{Oid: 2200, Name: "public"}

			Expect(schemas).To(HaveLen(2))
			structmatcher.ExpectStructsToMatchExcluding(&schemaBar, &schemas[0], "Oid")
			structmatcher.ExpectStructsToMatchExcluding(&schemaPublic, &schemas[1], "Owner")

		})
	})
	Describe("GetConstraints", func() {
		var (
			uniqueConstraint         = backup.Constraint{Oid: 0, Schema: "public", Name: "uniq2", ConType: "u", ConDef: "UNIQUE (a, b)", OwningObject: "public.constraints_table", IsDomainConstraint: false, IsPartitionParent: false}
			fkConstraint             = backup.Constraint{Oid: 0, Schema: "public", Name: "fk1", ConType: "f", ConDef: "FOREIGN KEY (b) REFERENCES public.constraints_table(b)", OwningObject: "public.constraints_other_table", IsDomainConstraint: false, IsPartitionParent: false}
			pkConstraint             = backup.Constraint{Oid: 0, Schema: "public", Name: "pk1", ConType: "p", ConDef: "PRIMARY KEY (b)", OwningObject: "public.constraints_table", IsDomainConstraint: false, IsPartitionParent: false}
			checkConstraint          = backup.Constraint{Oid: 0, Schema: "public", Name: "check1", ConType: "c", ConDef: "CHECK (a <> 42)", OwningObject: "public.constraints_table", IsDomainConstraint: false, IsPartitionParent: false}
			partitionCheckConstraint = backup.Constraint{Oid: 0, Schema: "public", Name: "check1", ConType: "c", ConDef: "CHECK (id <> 0)", OwningObject: "public.part", IsDomainConstraint: false, IsPartitionParent: true}
			domainConstraint         = backup.Constraint{Oid: 0, Schema: "public", Name: "check1", ConType: "c", ConDef: "CHECK (VALUE <> 42)", OwningObject: "public.constraint_domain", IsDomainConstraint: true, IsPartitionParent: false}
		)
		Context("No constraints", func() {
			It("returns an empty constraint array for a table with no constraints", func() {
				testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.no_constraints_table(a int, b text)")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.no_constraints_table")

				constraints := backup.GetConstraints(connectionPool)

				Expect(constraints).To(BeEmpty())
			})
		})
		Context("One constraint", func() {
			It("returns a constraint array for a table with one UNIQUE constraint and a comment", func() {
				testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.constraints_table(a int, b text, c float)")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.constraints_table")
				testhelper.AssertQueryRuns(connectionPool, "ALTER TABLE ONLY public.constraints_table ADD CONSTRAINT uniq2 UNIQUE (a, b)")
				testhelper.AssertQueryRuns(connectionPool, "COMMENT ON CONSTRAINT uniq2 ON public.constraints_table IS 'this is a constraint comment'")

				constraints := backup.GetConstraints(connectionPool)

				Expect(constraints).To(HaveLen(1))
				structmatcher.ExpectStructsToMatchExcluding(&constraints[0], &uniqueConstraint, "Oid")
			})
			It("returns a constraint array for a table with one PRIMARY KEY constraint and a comment", func() {
				testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.constraints_table(a int, b text, c float)")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.constraints_table")
				testhelper.AssertQueryRuns(connectionPool, "ALTER TABLE ONLY public.constraints_table ADD CONSTRAINT pk1 PRIMARY KEY (b)")
				testhelper.AssertQueryRuns(connectionPool, "COMMENT ON CONSTRAINT pk1 ON public.constraints_table IS 'this is a constraint comment'")

				constraints := backup.GetConstraints(connectionPool)

				Expect(constraints).To(HaveLen(1))
				structmatcher.ExpectStructsToMatchExcluding(&constraints[0], &pkConstraint, "Oid")
			})
			It("returns a constraint array for a table with one FOREIGN KEY constraint", func() {
				testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.constraints_table(a int, b text, c float)")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.constraints_table CASCADE")
				testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.constraints_other_table(b text)")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.constraints_other_table CASCADE")
				testhelper.AssertQueryRuns(connectionPool, "ALTER TABLE ONLY public.constraints_table ADD CONSTRAINT pk1 PRIMARY KEY (b)")
				testhelper.AssertQueryRuns(connectionPool, "ALTER TABLE ONLY public.constraints_other_table ADD CONSTRAINT fk1 FOREIGN KEY (b) REFERENCES public.constraints_table(b)")

				constraints := backup.GetConstraints(connectionPool)

				Expect(constraints).To(HaveLen(2))
				structmatcher.ExpectStructsToMatchExcluding(&constraints[0], &fkConstraint, "Oid")
				structmatcher.ExpectStructsToMatchExcluding(&constraints[1], &pkConstraint, "Oid")
			})
			It("returns a constraint array for a table with one CHECK constraint", func() {
				testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.constraints_table(a int, b text, c float)")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.constraints_table")
				testhelper.AssertQueryRuns(connectionPool, "ALTER TABLE ONLY public.constraints_table ADD CONSTRAINT check1 CHECK (a <> 42)")

				constraints := backup.GetConstraints(connectionPool)

				Expect(constraints).To(HaveLen(1))
				structmatcher.ExpectStructsToMatchExcluding(&constraints[0], &checkConstraint, "Oid")
			})
			It("returns a constraint array for a parent partition table with one CHECK constraint", func() {
				testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE public.part (id int, date date, amt decimal(10,2) default 0.0) DISTRIBUTED BY (id)
PARTITION BY RANGE (date)
      (PARTITION Jan08 START (date '2008-01-01') INCLUSIVE ,
      PARTITION Feb08 START (date '2008-02-01') INCLUSIVE ,
      PARTITION Mar08 START (date '2008-03-01') INCLUSIVE
      END (date '2008-04-01') EXCLUSIVE);`)
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.part")
				testhelper.AssertQueryRuns(connectionPool, "ALTER TABLE public.part ADD CONSTRAINT check1 CHECK (id <> 0)")

				constraints := backup.GetConstraints(connectionPool)

				Expect(constraints).To(HaveLen(1))
				structmatcher.ExpectStructsToMatchExcluding(&constraints[0], &partitionCheckConstraint, "Oid")
			})
			It("returns a constraint array for a domain", func() {
				testhelper.AssertQueryRuns(connectionPool, "CREATE DOMAIN public.constraint_domain AS int")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP DOMAIN public.constraint_domain")
				testhelper.AssertQueryRuns(connectionPool, "ALTER DOMAIN public.constraint_domain ADD CONSTRAINT check1 CHECK (VALUE <> 42)")

				constraints := backup.GetConstraints(connectionPool)

				Expect(constraints).To(HaveLen(1))
				structmatcher.ExpectStructsToMatchExcluding(&constraints[0], &domainConstraint, "Oid")
			})
			It("does not return a constraint array for a table that inherits a constraint from another table", func() {
				testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.constraints_table(a int, b text, c float)")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.constraints_table")
				testhelper.AssertQueryRuns(connectionPool, "ALTER TABLE ONLY public.constraints_table ADD CONSTRAINT check1 CHECK (a <> 42)")

				testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.constraints_child_table(a int, b text, c float) INHERITS (public.constraints_table)")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.constraints_child_table")

				constraints := backup.GetConstraints(connectionPool)

				Expect(constraints).To(HaveLen(1))
				structmatcher.ExpectStructsToMatchExcluding(&constraints[0], &checkConstraint, "Oid")
			})
			It("returns a constraint array for a table that inherits from another table and has an additional constraint", func() {
				testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.parent_table(a int, b text, c float)")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.parent_table")

				testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.constraints_table(a int, b text, c float) INHERITS (public.parent_table)")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.constraints_table")

				testhelper.AssertQueryRuns(connectionPool, "ALTER TABLE ONLY public.constraints_table ADD CONSTRAINT check1 CHECK (a <> 42)")

				constraints := backup.GetConstraints(connectionPool)

				Expect(constraints).To(HaveLen(1))
				structmatcher.ExpectStructsToMatchExcluding(&constraints[0], &checkConstraint, "Oid")
			})
			It("returns a constraint array for a table in a specific schema", func() {
				testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.constraints_table(a int, b text, c float)")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.constraints_table")
				testhelper.AssertQueryRuns(connectionPool, "ALTER TABLE ONLY public.constraints_table ADD CONSTRAINT uniq2 UNIQUE (a, b)")
				testhelper.AssertQueryRuns(connectionPool, "CREATE SCHEMA testschema")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA testschema")
				testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE testschema.constraints_table(a int, b text, c float)")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE testschema.constraints_table")
				testhelper.AssertQueryRuns(connectionPool, "ALTER TABLE ONLY testschema.constraints_table ADD CONSTRAINT uniq2 UNIQUE (a, b)")
				backupCmdFlags.Set(utils.INCLUDE_SCHEMA, "testschema")
				constraintInSchema := backup.Constraint{Oid: 0, Schema: "testschema", Name: "uniq2", ConType: "u", ConDef: "UNIQUE (a, b)", OwningObject: "testschema.constraints_table", IsDomainConstraint: false, IsPartitionParent: false}

				constraints := backup.GetConstraints(connectionPool)

				Expect(constraints).To(HaveLen(1))
				structmatcher.ExpectStructsToMatchExcluding(&constraints[0], &constraintInSchema, "Oid")
			})
			It("returns a constraint array for only the tables included in the backup set", func() {
				testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.constraints_table(a int, b text, c float)")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.constraints_table")
				testhelper.AssertQueryRuns(connectionPool, "ALTER TABLE ONLY public.constraints_table ADD CONSTRAINT uniq2 UNIQUE (a, b)")
				testhelper.AssertQueryRuns(connectionPool, "COMMENT ON CONSTRAINT uniq2 ON public.constraints_table IS 'this is a constraint comment'")
				testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.other_table(d bool, e float)")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.other_table")

				constraintsOid := testutils.OidFromObjectName(connectionPool, "public", "constraints_table", backup.TYPE_RELATION)
				otherOid := testutils.OidFromObjectName(connectionPool, "public", "other_table", backup.TYPE_RELATION)
				tables := []backup.Relation{{Oid: constraintsOid, Schema: "public", Name: "constraints_table"}}
				constraints := backup.GetConstraints(connectionPool, tables...)
				Expect(constraints).To(HaveLen(1))
				structmatcher.ExpectStructsToMatchExcluding(&constraints[0], &uniqueConstraint, "Oid")

				tables = []backup.Relation{{Oid: otherOid, Schema: "public", Name: "other_table"}}
				constraints = backup.GetConstraints(connectionPool, tables...)
				Expect(constraints).To(BeEmpty())
			})
			It("returns a constraint array without contraints on tables in the exclude set", func() {
				testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.constraints_table(a int, b text, c float)")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.constraints_table")
				testhelper.AssertQueryRuns(connectionPool, "ALTER TABLE ONLY public.constraints_table ADD CONSTRAINT uniq2 UNIQUE (a, b)")
				testhelper.AssertQueryRuns(connectionPool, "COMMENT ON CONSTRAINT uniq2 ON public.constraints_table IS 'this is a constraint comment'")
				testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.other_table(d bool, e float)")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.other_table")

				backupCmdFlags.Set(utils.EXCLUDE_RELATION, "public.other_table")
				defer backupCmdFlags.Set(utils.EXCLUDE_RELATION, "")
				constraints := backup.GetConstraints(connectionPool)
				Expect(constraints).To(HaveLen(1))
				structmatcher.ExpectStructsToMatchExcluding(&constraints[0], &uniqueConstraint, "Oid")

				backupCmdFlags.Set(utils.EXCLUDE_RELATION, "public.constraints_table")
				constraints = backup.GetConstraints(connectionPool)
				Expect(constraints).To(BeEmpty())
			})
		})
		Context("Multiple constraints", func() {
			It("returns a constraint array for a table with multiple constraints", func() {
				testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.constraints_table(a int, b text, c float) DISTRIBUTED BY (b)")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.constraints_table CASCADE")
				testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.constraints_other_table(b text)")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.constraints_other_table CASCADE")
				testhelper.AssertQueryRuns(connectionPool, "ALTER TABLE ONLY public.constraints_table ADD CONSTRAINT uniq2 UNIQUE (a, b)")
				testhelper.AssertQueryRuns(connectionPool, "COMMENT ON CONSTRAINT uniq2 ON public.constraints_table IS 'this is a constraint comment'")
				testhelper.AssertQueryRuns(connectionPool, "ALTER TABLE ONLY public.constraints_table ADD CONSTRAINT pk1 PRIMARY KEY (b)")
				testhelper.AssertQueryRuns(connectionPool, "COMMENT ON CONSTRAINT pk1 ON public.constraints_table IS 'this is a constraint comment'")
				testhelper.AssertQueryRuns(connectionPool, "ALTER TABLE ONLY public.constraints_other_table ADD CONSTRAINT fk1 FOREIGN KEY (b) REFERENCES public.constraints_table(b)")
				testhelper.AssertQueryRuns(connectionPool, "ALTER TABLE ONLY public.constraints_table ADD CONSTRAINT check1 CHECK (a <> 42)")

				constraints := backup.GetConstraints(connectionPool)

				Expect(constraints).To(HaveLen(4))
				structmatcher.ExpectStructsToMatchExcluding(&constraints[0], &checkConstraint, "Oid")
				structmatcher.ExpectStructsToMatchExcluding(&constraints[1], &fkConstraint, "Oid")
				structmatcher.ExpectStructsToMatchExcluding(&constraints[2], &pkConstraint, "Oid")
				structmatcher.ExpectStructsToMatchExcluding(&constraints[3], &uniqueConstraint, "Oid")
			})
		})
	})
	Describe("GetMetadataForObjectType", func() {
		Context("default metadata for all objects of one type", func() {
			It("returns a slice of metadata with modified privileges", func() {
				testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.foo(i int)")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.foo")
				testhelper.AssertQueryRuns(connectionPool, "REVOKE DELETE ON TABLE public.foo FROM testrole")
				testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.bar(i int)")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.bar")
				testhelper.AssertQueryRuns(connectionPool, "REVOKE ALL ON TABLE public.bar FROM testrole")
				testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.baz(i int)")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.baz")
				testhelper.AssertQueryRuns(connectionPool, "GRANT ALL ON TABLE public.baz TO anothertestrole")

				resultMetadataMap := backup.GetMetadataForObjectType(connectionPool, backup.TYPE_RELATION)

				fooUniqueID := testutils.UniqueIDFromObjectName(connectionPool, "public", "foo", backup.TYPE_RELATION)
				barUniqueID := testutils.UniqueIDFromObjectName(connectionPool, "public", "bar", backup.TYPE_RELATION)
				bazUniqueID := testutils.UniqueIDFromObjectName(connectionPool, "public", "baz", backup.TYPE_RELATION)
				expectedFoo := backup.ObjectMetadata{Privileges: []backup.ACL{testutils.DefaultACLWithout("testrole", "TABLE", "DELETE")}, Owner: "testrole"}
				expectedBar := backup.ObjectMetadata{Privileges: []backup.ACL{{Grantee: "GRANTEE"}}, Owner: "testrole"}
				expectedBaz := backup.ObjectMetadata{Privileges: []backup.ACL{testutils.DefaultACLForType("anothertestrole", "TABLE"), testutils.DefaultACLForType("testrole", "TABLE")}, Owner: "testrole"}
				Expect(resultMetadataMap).To(HaveLen(3))
				resultFoo := resultMetadataMap[fooUniqueID]
				resultBar := resultMetadataMap[barUniqueID]
				resultBaz := resultMetadataMap[bazUniqueID]
				structmatcher.ExpectStructsToMatch(&resultFoo, &expectedFoo)
				structmatcher.ExpectStructsToMatch(&resultBar, &expectedBar)
				structmatcher.ExpectStructsToMatch(&resultBaz, &expectedBaz)
			})
			It("returns a slice of default metadata for a database", func() {
				testhelper.AssertQueryRuns(connectionPool, "GRANT ALL ON DATABASE testdb TO anothertestrole")
				defer testhelper.AssertQueryRuns(connectionPool, "REVOKE ALL ON DATABASE testdb FROM anothertestRole")
				testhelper.AssertQueryRuns(connectionPool, "COMMENT ON DATABASE testdb IS 'This is a database comment.'")
				expectedMetadata := backup.ObjectMetadata{Privileges: []backup.ACL{
					{Grantee: "", Temporary: true, Connect: true},
					{Grantee: "anothertestrole", Create: true, Temporary: true, Connect: true},
				}, Owner: "anothertestrole", Comment: "This is a database comment."}

				resultMetadataMap := backup.GetMetadataForObjectType(connectionPool, backup.TYPE_DATABASE)

				uniqueID := testutils.UniqueIDFromObjectName(connectionPool, "", "testdb", backup.TYPE_DATABASE)
				resultMetadata := resultMetadataMap[uniqueID]
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of metadata with the owner in quotes", func() {
				testhelper.AssertQueryRuns(connectionPool, `CREATE ROLE "Role1"`)
				defer testhelper.AssertQueryRuns(connectionPool, `DROP ROLE "Role1"`)
				testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.testtable(i int)")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.testtable")
				testhelper.AssertQueryRuns(connectionPool, `ALTER TABLE public.testtable OWNER TO "Role1"`)

				resultMetadataMap := backup.GetMetadataForObjectType(connectionPool, backup.TYPE_RELATION)

				uniqueID := testutils.UniqueIDFromObjectName(connectionPool, "public", "testtable", backup.TYPE_RELATION)
				expectedMetadata := backup.ObjectMetadata{Privileges: []backup.ACL{}, Owner: `"Role1"`}
				Expect(resultMetadataMap).To(HaveLen(1))
				resultMetadata := resultMetadataMap[uniqueID]
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for a table", func() {
				testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.testtable(i int)")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.testtable")
				testhelper.AssertQueryRuns(connectionPool, "GRANT ALL ON TABLE public.testtable TO testrole")
				testhelper.AssertQueryRuns(connectionPool, "COMMENT ON TABLE public.testtable IS 'This is a table comment.'")

				resultMetadataMap := backup.GetMetadataForObjectType(connectionPool, backup.TYPE_RELATION)

				uniqueID := testutils.UniqueIDFromObjectName(connectionPool, "public", "testtable", backup.TYPE_RELATION)
				expectedMetadata := testutils.DefaultMetadata("TABLE", true, true, true)
				Expect(resultMetadataMap).To(HaveLen(1))
				resultMetadata := resultMetadataMap[uniqueID]
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for a sequence", func() {
				testhelper.AssertQueryRuns(connectionPool, "CREATE SEQUENCE public.testsequence")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP SEQUENCE public.testsequence")
				testhelper.AssertQueryRuns(connectionPool, "GRANT ALL ON SEQUENCE public.testsequence TO testrole")
				testhelper.AssertQueryRuns(connectionPool, "COMMENT ON SEQUENCE public.testsequence IS 'This is a sequence comment.'")

				resultMetadataMap := backup.GetMetadataForObjectType(connectionPool, backup.TYPE_RELATION)

				uniqueID := testutils.UniqueIDFromObjectName(connectionPool, "public", "testsequence", backup.TYPE_RELATION)
				expectedMetadata := testutils.DefaultMetadata("SEQUENCE", true, true, true)
				Expect(resultMetadataMap).To(HaveLen(1))
				resultMetadata := resultMetadataMap[uniqueID]
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for a function", func() {
				testhelper.AssertQueryRuns(connectionPool, `CREATE FUNCTION public.add(integer, integer) RETURNS integer
AS 'SELECT $1 + $2'
LANGUAGE SQL`)
				defer testhelper.AssertQueryRuns(connectionPool, "DROP FUNCTION public.add(integer, integer)")
				testhelper.AssertQueryRuns(connectionPool, "GRANT ALL ON FUNCTION public.add(integer, integer) TO testrole")
				testhelper.AssertQueryRuns(connectionPool, "REVOKE ALL ON FUNCTION public.add(integer, integer) FROM PUBLIC")
				testhelper.AssertQueryRuns(connectionPool, "COMMENT ON FUNCTION public.add(integer, integer) IS 'This is a function comment.'")

				resultMetadataMap := backup.GetMetadataForObjectType(connectionPool, backup.TYPE_FUNCTION)

				uniqueID := testutils.UniqueIDFromObjectName(connectionPool, "public", "add", backup.TYPE_FUNCTION)
				expectedMetadata := testutils.DefaultMetadata("FUNCTION", true, true, true)
				Expect(resultMetadataMap).To(HaveLen(1))
				resultMetadata := resultMetadataMap[uniqueID]
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for a view", func() {
				testhelper.AssertQueryRuns(connectionPool, `CREATE VIEW public.testview AS SELECT * FROM pg_class`)
				defer testhelper.AssertQueryRuns(connectionPool, "DROP VIEW public.testview")
				testhelper.AssertQueryRuns(connectionPool, "GRANT ALL ON public.testview TO testrole")
				testhelper.AssertQueryRuns(connectionPool, "COMMENT ON VIEW public.testview IS 'This is a view comment.'")

				resultMetadataMap := backup.GetMetadataForObjectType(connectionPool, backup.TYPE_RELATION)

				uniqueID := testutils.UniqueIDFromObjectName(connectionPool, "public", "testview", backup.TYPE_RELATION)
				expectedMetadata := testutils.DefaultMetadata("VIEW", true, true, true)
				Expect(resultMetadataMap).To(HaveLen(1))
				resultMetadata := resultMetadataMap[uniqueID]
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for a schema", func() {
				testhelper.AssertQueryRuns(connectionPool, `CREATE SCHEMA testschema`)
				defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA testschema")
				testhelper.AssertQueryRuns(connectionPool, "GRANT ALL ON SCHEMA testschema TO testrole")
				testhelper.AssertQueryRuns(connectionPool, "COMMENT ON SCHEMA testschema IS 'This is a schema comment.'")

				resultMetadataMap := backup.GetMetadataForObjectType(connectionPool, backup.TYPE_SCHEMA)

				uniqueID := testutils.UniqueIDFromObjectName(connectionPool, "", "testschema", backup.TYPE_SCHEMA)
				expectedMetadata := testutils.DefaultMetadata("SCHEMA", true, true, true)
				resultMetadata := resultMetadataMap[uniqueID]
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for an aggregate", func() {
				testhelper.AssertQueryRuns(connectionPool, `
			CREATE FUNCTION public.mysfunc_accum(numeric, numeric, numeric)
			   RETURNS numeric
			   AS 'select $1 + $2 + $3'
			   LANGUAGE SQL
			   IMMUTABLE
			   RETURNS NULL ON NULL INPUT;
			`)
				defer testhelper.AssertQueryRuns(connectionPool, "DROP FUNCTION public.mysfunc_accum(numeric, numeric, numeric)")
				testhelper.AssertQueryRuns(connectionPool, `
			CREATE FUNCTION public.mypre_accum(numeric, numeric)
			   RETURNS numeric
			   AS 'select $1 + $2'
			   LANGUAGE SQL
			   IMMUTABLE
			   RETURNS NULL ON NULL INPUT;
			`)
				defer testhelper.AssertQueryRuns(connectionPool, "DROP FUNCTION public.mypre_accum(numeric, numeric)")
				testhelper.AssertQueryRuns(connectionPool, `CREATE AGGREGATE public.agg_prefunc(numeric, numeric) (
	SFUNC = public.mysfunc_accum,
	STYPE = numeric,
	PREFUNC = public.mypre_accum,
	INITCOND = '0'
);`)
				defer testhelper.AssertQueryRuns(connectionPool, "DROP AGGREGATE public.agg_prefunc(numeric, numeric)")
				testhelper.AssertQueryRuns(connectionPool, "COMMENT ON AGGREGATE public.agg_prefunc(numeric, numeric) IS 'This is an aggregate comment.'")

				resultMetadataMap := backup.GetMetadataForObjectType(connectionPool, backup.TYPE_AGGREGATE)

				uniqueID := testutils.UniqueIDFromObjectName(connectionPool, "", "agg_prefunc", backup.TYPE_AGGREGATE)
				expectedMetadata := testutils.DefaultMetadata("AGGREGATE", false, true, true)
				resultMetadata := resultMetadataMap[uniqueID]
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for a type", func() {
				testhelper.AssertQueryRuns(connectionPool, `CREATE TYPE public.testtype AS (name text, num numeric)`)
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TYPE public.testtype")
				testhelper.AssertQueryRuns(connectionPool, "COMMENT ON TYPE public.testtype IS 'This is a type comment.'")

				resultMetadataMap := backup.GetMetadataForObjectType(connectionPool, backup.TYPE_TYPE)

				typeUniqueID := testutils.UniqueIDFromObjectName(connectionPool, "", "testtype", backup.TYPE_TYPE)
				expectedMetadata := testutils.DefaultMetadata("TYPE", false, true, true)
				resultMetadata := resultMetadataMap[typeUniqueID]
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for a domain", func() {
				testhelper.AssertQueryRuns(connectionPool, `CREATE DOMAIN public.domain_type AS numeric`)
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TYPE public.domain_type")
				testhelper.AssertQueryRuns(connectionPool, "COMMENT ON DOMAIN public.domain_type IS 'This is a domain comment.'")

				resultMetadataMap := backup.GetMetadataForObjectType(connectionPool, backup.TYPE_TYPE)

				uniqueID := testutils.UniqueIDFromObjectName(connectionPool, "", "domain_type", backup.TYPE_TYPE)
				expectedMetadata := testutils.DefaultMetadata("DOMAIN", false, true, true)
				resultMetadata := resultMetadataMap[uniqueID]
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for an external protocol", func() {
				testhelper.AssertQueryRuns(connectionPool, `CREATE OR REPLACE FUNCTION public.read_from_s3() RETURNS integer AS '$libdir/gps3ext.so', 's3_import' LANGUAGE C STABLE;`)
				defer testhelper.AssertQueryRuns(connectionPool, "DROP FUNCTION public.read_from_s3()")
				testhelper.AssertQueryRuns(connectionPool, `CREATE TRUSTED PROTOCOL s3_read (readfunc = public.read_from_s3);`)
				defer testhelper.AssertQueryRuns(connectionPool, "DROP PROTOCOL s3_read")
				testhelper.AssertQueryRuns(connectionPool, "GRANT ALL ON PROTOCOL s3_read TO testrole")

				resultMetadataMap := backup.GetMetadataForObjectType(connectionPool, backup.TYPE_PROTOCOL)

				uniqueID := testutils.UniqueIDFromObjectName(connectionPool, "", "s3_read", backup.TYPE_PROTOCOL)
				expectedMetadata := testutils.DefaultMetadata("PROTOCOL", true, true, false)
				resultMetadata := resultMetadataMap[uniqueID]
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for a tablespace", func() {
				if connectionPool.Version.Before("6") {
					testhelper.AssertQueryRuns(connectionPool, "CREATE TABLESPACE test_tablespace FILESPACE test_dir")
				} else {
					testhelper.AssertQueryRuns(connectionPool, "CREATE TABLESPACE test_tablespace LOCATION '/tmp/test_dir'")
				}
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLESPACE test_tablespace")
				testhelper.AssertQueryRuns(connectionPool, "COMMENT ON TABLESPACE test_tablespace IS 'This is a tablespace comment.'")
				testhelper.AssertQueryRuns(connectionPool, "GRANT ALL ON TABLESPACE test_tablespace TO testrole")

				resultMetadataMap := backup.GetMetadataForObjectType(connectionPool, backup.TYPE_TABLESPACE)

				uniqueID := testutils.UniqueIDFromObjectName(connectionPool, "", "test_tablespace", backup.TYPE_TABLESPACE)
				expectedMetadata := testutils.DefaultMetadata("TABLESPACE", true, true, true)
				resultMetadata := resultMetadataMap[uniqueID]
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for an operator", func() {
				testhelper.AssertQueryRuns(connectionPool, "CREATE OPERATOR public.#### (LEFTARG = bigint, PROCEDURE = numeric_fac)")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP OPERATOR public.#### (bigint, NONE)")

				testhelper.AssertQueryRuns(connectionPool, "COMMENT ON OPERATOR public.#### (bigint, NONE) IS 'This is an operator comment.'")

				resultMetadataMap := backup.GetMetadataForObjectType(connectionPool, backup.TYPE_OPERATOR)

				uniqueID := testutils.UniqueIDFromObjectName(connectionPool, "", "####", backup.TYPE_OPERATOR)
				expectedMetadata := testutils.DefaultMetadata("OPERATOR", false, true, true)
				resultMetadata := resultMetadataMap[uniqueID]
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for an operator family", func() {
				testutils.SkipIfBefore5(connectionPool)
				testhelper.AssertQueryRuns(connectionPool, "CREATE OPERATOR FAMILY public.testfam USING hash")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP OPERATOR FAMILY public.testfam USING hash")

				testhelper.AssertQueryRuns(connectionPool, "COMMENT ON OPERATOR FAMILY public.testfam USING hash IS 'This is an operator family comment.'")

				resultMetadataMap := backup.GetMetadataForObjectType(connectionPool, backup.TYPE_OPERATORFAMILY)

				uniqueID := testutils.UniqueIDFromObjectName(connectionPool, "public", "testfam", backup.TYPE_OPERATORFAMILY)
				expectedMetadata := testutils.DefaultMetadata("OPERATOR FAMILY", false, true, true)
				resultMetadata := resultMetadataMap[uniqueID]
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for an operator class", func() {
				testhelper.AssertQueryRuns(connectionPool, "CREATE OPERATOR CLASS public.testclass FOR TYPE int USING hash AS STORAGE int")
				if connectionPool.Version.Before("5") {
					defer testhelper.AssertQueryRuns(connectionPool, "DROP OPERATOR CLASS public.testclass USING hash")
				} else {
					defer testhelper.AssertQueryRuns(connectionPool, "DROP OPERATOR FAMILY public.testclass USING hash")
				}
				testhelper.AssertQueryRuns(connectionPool, "COMMENT ON OPERATOR CLASS public.testclass USING hash IS 'This is an operator class comment.'")

				resultMetadataMap := backup.GetMetadataForObjectType(connectionPool, backup.TYPE_OPERATORCLASS)

				uniqueID := testutils.UniqueIDFromObjectName(connectionPool, "public", "testclass", backup.TYPE_OPERATORCLASS)
				expectedMetadata := testutils.DefaultMetadata("OPERATOR CLASS", false, true, true)
				resultMetadata := resultMetadataMap[uniqueID]
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for a text search dictionary", func() {
				testutils.SkipIfBefore5(connectionPool)
				testhelper.AssertQueryRuns(connectionPool, "CREATE TEXT SEARCH DICTIONARY public.testdictionary(TEMPLATE = snowball, LANGUAGE = 'russian', STOPWORDS = 'russian');")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TEXT SEARCH DICTIONARY public.testdictionary")
				testhelper.AssertQueryRuns(connectionPool, "COMMENT ON TEXT SEARCH DICTIONARY public.testdictionary IS 'This is a text search dictionary comment.'")

				resultMetadataMap := backup.GetMetadataForObjectType(connectionPool, backup.TYPE_TSDICTIONARY)

				Expect(resultMetadataMap).To(HaveLen(1))
				uniqueID := testutils.UniqueIDFromObjectName(connectionPool, "public", "testdictionary", backup.TYPE_TSDICTIONARY)
				dictionaryMetadata := testutils.DefaultMetadata("TEXT SEARCH DICTIONARY", false, true, true)
				resultMetadata := resultMetadataMap[uniqueID]
				structmatcher.ExpectStructsToMatch(&dictionaryMetadata, &resultMetadata)
			})
			It("returns a slice of default metadata for a text search configuration", func() {
				testutils.SkipIfBefore5(connectionPool)
				resultMetadataMap := backup.GetMetadataForObjectType(connectionPool, backup.TYPE_TSCONFIGURATION)
				configurationMetadata := testutils.DefaultMetadata("TEXT SEARCH CONFIGURATION", false, true, true)

				testhelper.AssertQueryRuns(connectionPool, `CREATE TEXT SEARCH CONFIGURATION public.testconfiguration (PARSER = pg_catalog."default");`)
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TEXT SEARCH CONFIGURATION public.testconfiguration")
				testhelper.AssertQueryRuns(connectionPool, "COMMENT ON TEXT SEARCH CONFIGURATION public.testconfiguration IS 'This is a text search configuration comment.'")

				uniqueID := testutils.UniqueIDFromObjectName(connectionPool, "public", "testconfiguration", backup.TYPE_TSCONFIGURATION)
				resultMetadataMap = backup.GetMetadataForObjectType(connectionPool, backup.TYPE_TSCONFIGURATION)

				Expect(resultMetadataMap).To(HaveLen(1))
				resultMetadata := resultMetadataMap[uniqueID]
				structmatcher.ExpectStructsToMatch(&configurationMetadata, &resultMetadata)
			})
			It("returns a slice of default metadata for a foreign data wrapper", func() {
				testutils.SkipIfBefore6(connectionPool)
				testhelper.AssertQueryRuns(connectionPool, "CREATE FOREIGN DATA WRAPPER foreignwrapper")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP FOREIGN DATA WRAPPER foreignwrapper")

				testhelper.AssertQueryRuns(connectionPool, "GRANT ALL ON FOREIGN DATA WRAPPER foreignwrapper TO testrole")

				resultMetadataMap := backup.GetMetadataForObjectType(connectionPool, backup.TYPE_FOREIGNDATAWRAPPER)

				uniqueID := testutils.UniqueIDFromObjectName(connectionPool, "", "foreignwrapper", backup.TYPE_FOREIGNDATAWRAPPER)
				expectedMetadata := testutils.DefaultMetadata("FOREIGN DATA WRAPPER", true, true, false)
				resultMetadata := resultMetadataMap[uniqueID]
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for a foreign server", func() {
				testutils.SkipIfBefore6(connectionPool)
				testhelper.AssertQueryRuns(connectionPool, "CREATE FOREIGN DATA WRAPPER foreignwrapper")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP FOREIGN DATA WRAPPER foreignwrapper CASCADE")
				testhelper.AssertQueryRuns(connectionPool, "CREATE SERVER foreignserver FOREIGN DATA WRAPPER foreignwrapper")

				testhelper.AssertQueryRuns(connectionPool, "GRANT ALL ON FOREIGN SERVER foreignserver TO testrole")

				resultMetadataMap := backup.GetMetadataForObjectType(connectionPool, backup.TYPE_FOREIGNSERVER)

				uniqueID := testutils.UniqueIDFromObjectName(connectionPool, "", "foreignserver", backup.TYPE_FOREIGNSERVER)
				expectedMetadata := testutils.DefaultMetadata("FOREIGN SERVER", true, true, false)
				resultMetadata := resultMetadataMap[uniqueID]
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for a collation", func() {
				testutils.SkipIfBefore6(connectionPool)
				testhelper.AssertQueryRuns(connectionPool, `CREATE COLLATION public.some_coll (lc_collate = 'POSIX', lc_ctype = 'POSIX');`)
				defer testhelper.AssertQueryRuns(connectionPool, "DROP COLLATION public.some_coll")
				testhelper.AssertQueryRuns(connectionPool, "COMMENT ON COLLATION public.some_coll IS 'This is a collation comment.'")

				resultMetadataMap := backup.GetMetadataForObjectType(connectionPool, backup.TYPE_COLLATION)

				Expect(resultMetadataMap).To(HaveLen(1))
				uniqueID := testutils.UniqueIDFromObjectName(connectionPool, "public", "some_coll", backup.TYPE_COLLATION)
				collationMetadata := testutils.DefaultMetadata("COLLATION", false, true, true)
				resultMetadata := resultMetadataMap[uniqueID]
				structmatcher.ExpectStructsToMatch(&collationMetadata, &resultMetadata)
			})
			It("returns a slice of default metadata for an event trigger", func() {
				testutils.SkipIfBefore6(connectionPool)
				testhelper.AssertQueryRuns(connectionPool, `CREATE FUNCTION abort_any_command()
RETURNS event_trigger LANGUAGE plpgsql
AS $$ BEGIN RAISE EXCEPTION 'exception'; END; $$;`)
				defer testhelper.AssertQueryRuns(connectionPool, `DROP FUNCTION abort_any_command()`)
				testhelper.AssertQueryRuns(connectionPool, "CREATE EVENT TRIGGER test_event_trigger ON ddl_command_start EXECUTE PROCEDURE abort_any_command();")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP EVENT TRIGGER test_event_trigger")

				testhelper.AssertQueryRuns(connectionPool, "COMMENT ON EVENT TRIGGER test_event_trigger IS 'This is an event trigger comment.'")

				resultMetadataMap := backup.GetMetadataForObjectType(connectionPool, backup.TYPE_EVENTTRIGGER)

				Expect(resultMetadataMap).To(HaveLen(1))
				uniqueID := testutils.UniqueIDFromObjectName(connectionPool, "", "test_event_trigger", backup.TYPE_EVENTTRIGGER)
				eventTriggerMetadata := testutils.DefaultMetadata("EVENT TRIGGER", false, true, true)
				resultMetadata := resultMetadataMap[uniqueID]
				structmatcher.ExpectStructsToMatch(&eventTriggerMetadata, &resultMetadata)
			})
		})
		Context("metadata for objects in a specific schema", func() {
			It("returns a slice of default metadata for a table in a specific schema", func() {
				testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.testtable(i int)")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.testtable")
				testhelper.AssertQueryRuns(connectionPool, "CREATE SCHEMA testschema")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA testschema")
				testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE testschema.testtable(i int)")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE testschema.testtable")
				testhelper.AssertQueryRuns(connectionPool, "GRANT ALL ON TABLE testschema.testtable TO testrole")
				testhelper.AssertQueryRuns(connectionPool, "COMMENT ON TABLE testschema.testtable IS 'This is a table comment.'")
				backupCmdFlags.Set(utils.INCLUDE_SCHEMA, "testschema")

				resultMetadataMap := backup.GetMetadataForObjectType(connectionPool, backup.TYPE_RELATION)

				uniqueID := testutils.UniqueIDFromObjectName(connectionPool, "testschema", "testtable", backup.TYPE_RELATION)
				expectedMetadata := testutils.DefaultMetadata("TABLE", true, true, true)
				Expect(resultMetadataMap).To(HaveLen(1))
				resultMetadata := resultMetadataMap[uniqueID]
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for a table not in a specific schema", func() {
				testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.testtable(i int)")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.testtable")
				testhelper.AssertQueryRuns(connectionPool, "CREATE SCHEMA testschema")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA testschema")
				testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE testschema.testtable(i int)")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE testschema.testtable")
				testhelper.AssertQueryRuns(connectionPool, "GRANT ALL ON TABLE testschema.testtable TO testrole")
				testhelper.AssertQueryRuns(connectionPool, "COMMENT ON TABLE testschema.testtable IS 'This is a table comment.'")
				backupCmdFlags.Set(utils.EXCLUDE_SCHEMA, "public")

				resultMetadataMap := backup.GetMetadataForObjectType(connectionPool, backup.TYPE_RELATION)

				uniqueID := testutils.UniqueIDFromObjectName(connectionPool, "testschema", "testtable", backup.TYPE_RELATION)
				expectedMetadata := testutils.DefaultMetadata("TABLE", true, true, true)
				Expect(resultMetadataMap).To(HaveLen(1))
				resultMetadata := resultMetadataMap[uniqueID]
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for a function in a specific schema", func() {
				testhelper.AssertQueryRuns(connectionPool, `CREATE FUNCTION public.add(integer, integer) RETURNS integer
AS 'SELECT $1 + $2'
LANGUAGE SQL`)
				defer testhelper.AssertQueryRuns(connectionPool, "DROP FUNCTION public.add(integer, integer)")
				testhelper.AssertQueryRuns(connectionPool, "CREATE SCHEMA testschema")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA testschema")
				testhelper.AssertQueryRuns(connectionPool, `CREATE FUNCTION testschema.add(integer, integer) RETURNS integer
AS 'SELECT $1 + $2'
LANGUAGE SQL`)
				defer testhelper.AssertQueryRuns(connectionPool, "DROP FUNCTION testschema.add(integer, integer)")
				testhelper.AssertQueryRuns(connectionPool, "GRANT ALL ON FUNCTION testschema.add(integer, integer) TO testrole")
				testhelper.AssertQueryRuns(connectionPool, "REVOKE ALL ON FUNCTION testschema.add(integer, integer) FROM PUBLIC")
				testhelper.AssertQueryRuns(connectionPool, "COMMENT ON FUNCTION testschema.add(integer, integer) IS 'This is a function comment.'")

				backupCmdFlags.Set(utils.INCLUDE_SCHEMA, "testschema")
				resultMetadataMap := backup.GetMetadataForObjectType(connectionPool, backup.TYPE_FUNCTION)

				uniqueID := testutils.UniqueIDFromObjectName(connectionPool, "testschema", "add", backup.TYPE_FUNCTION)
				expectedMetadata := testutils.DefaultMetadata("FUNCTION", true, true, true)
				Expect(resultMetadataMap).To(HaveLen(1))
				resultMetadata := resultMetadataMap[uniqueID]
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for a view in a specific schema", func() {
				testhelper.AssertQueryRuns(connectionPool, `CREATE VIEW public.testview AS SELECT * FROM pg_class`)
				defer testhelper.AssertQueryRuns(connectionPool, "DROP VIEW public.testview")
				testhelper.AssertQueryRuns(connectionPool, "CREATE SCHEMA testschema")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA testschema")
				testhelper.AssertQueryRuns(connectionPool, `CREATE VIEW testschema.testview AS SELECT * FROM pg_class`)
				defer testhelper.AssertQueryRuns(connectionPool, "DROP VIEW testschema.testview")
				testhelper.AssertQueryRuns(connectionPool, "GRANT ALL ON testschema.testview TO testrole")
				testhelper.AssertQueryRuns(connectionPool, "COMMENT ON VIEW testschema.testview IS 'This is a view comment.'")

				backupCmdFlags.Set(utils.INCLUDE_SCHEMA, "testschema")
				resultMetadataMap := backup.GetMetadataForObjectType(connectionPool, backup.TYPE_RELATION)

				uniqueID := testutils.UniqueIDFromObjectName(connectionPool, "testschema", "testview", backup.TYPE_RELATION)
				expectedMetadata := testutils.DefaultMetadata("VIEW", true, true, true)
				Expect(resultMetadataMap).To(HaveLen(1))
				resultMetadata := resultMetadataMap[uniqueID]
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for an aggregate in a specific schema", func() {
				testhelper.AssertQueryRuns(connectionPool, `
			CREATE FUNCTION public.mysfunc_accum(numeric, numeric, numeric)
			   RETURNS numeric
			   AS 'select $1 + $2 + $3'
			   LANGUAGE SQL
			   IMMUTABLE
			   RETURNS NULL ON NULL INPUT;
			`)
				defer testhelper.AssertQueryRuns(connectionPool, "DROP FUNCTION public.mysfunc_accum(numeric, numeric, numeric)")
				testhelper.AssertQueryRuns(connectionPool, `
			CREATE FUNCTION public.mypre_accum(numeric, numeric)
			   RETURNS numeric
			   AS 'select $1 + $2'
			   LANGUAGE SQL
			   IMMUTABLE
			   RETURNS NULL ON NULL INPUT;
			`)
				defer testhelper.AssertQueryRuns(connectionPool, "DROP FUNCTION public.mypre_accum(numeric, numeric)")
				testhelper.AssertQueryRuns(connectionPool, `CREATE AGGREGATE public.agg_prefunc(numeric, numeric) (
	SFUNC = public.mysfunc_accum,
	STYPE = numeric,
	PREFUNC = public.mypre_accum,
	INITCOND = '0'
);`)
				defer testhelper.AssertQueryRuns(connectionPool, "DROP AGGREGATE public.agg_prefunc(numeric, numeric)")
				testhelper.AssertQueryRuns(connectionPool, "COMMENT ON AGGREGATE public.agg_prefunc(numeric, numeric) IS 'This is an aggregate comment.'")
				testhelper.AssertQueryRuns(connectionPool, "CREATE SCHEMA testschema")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA testschema")
				testhelper.AssertQueryRuns(connectionPool, `CREATE AGGREGATE testschema.agg_prefunc(numeric, numeric) (
	SFUNC = public.mysfunc_accum,
	STYPE = numeric,
	PREFUNC = public.mypre_accum,
	INITCOND = '0'
);`)
				defer testhelper.AssertQueryRuns(connectionPool, "DROP AGGREGATE testschema.agg_prefunc(numeric, numeric)")
				testhelper.AssertQueryRuns(connectionPool, "COMMENT ON AGGREGATE testschema.agg_prefunc(numeric, numeric) IS 'This is an aggregate comment.'")

				backupCmdFlags.Set(utils.INCLUDE_SCHEMA, "testschema")
				resultMetadataMap := backup.GetMetadataForObjectType(connectionPool, backup.TYPE_AGGREGATE)

				uniqueID := testutils.UniqueIDFromObjectName(connectionPool, "testschema", "agg_prefunc", backup.TYPE_AGGREGATE)
				expectedMetadata := testutils.DefaultMetadata("AGGREGATE", false, true, true)
				Expect(resultMetadataMap).To(HaveLen(1))
				resultMetadata := resultMetadataMap[uniqueID]
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for a type in a specific schema", func() {
				testhelper.AssertQueryRuns(connectionPool, `CREATE TYPE public.testtype`)
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TYPE public.testtype")
				testhelper.AssertQueryRuns(connectionPool, "CREATE SCHEMA testschema")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA testschema")
				testhelper.AssertQueryRuns(connectionPool, `CREATE TYPE testschema.testtype AS (name text)`)
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TYPE testschema.testtype")
				testhelper.AssertQueryRuns(connectionPool, "COMMENT ON TYPE testschema.testtype IS 'This is a type comment.'")

				backupCmdFlags.Set(utils.INCLUDE_SCHEMA, "testschema")
				resultMetadataMap := backup.GetMetadataForObjectType(connectionPool, backup.TYPE_TYPE)

				uniqueID := testutils.UniqueIDFromObjectName(connectionPool, "testschema", "testtype", backup.TYPE_TYPE)
				expectedMetadata := testutils.DefaultMetadata("TYPE", false, true, true)
				resultMetadata := resultMetadataMap[uniqueID]
				if connectionPool.Version.Before("5") {
					// In 4.3, creating testtype does not generate a "_testtype" entry in pg_type
					Expect(resultMetadataMap).To(HaveLen(1))
				} else {
					// In 5, creating testtype generates 2 entries in pg_type, "testtype" and "_testtype"
					Expect(resultMetadataMap).To(HaveLen(2))
				}
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for an operator in a specific schema", func() {
				testhelper.AssertQueryRuns(connectionPool, "CREATE OPERATOR public.#### (LEFTARG = bigint, PROCEDURE = numeric_fac)")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP OPERATOR public.#### (bigint, NONE)")
				testhelper.AssertQueryRuns(connectionPool, "CREATE SCHEMA testschema")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA testschema")
				testhelper.AssertQueryRuns(connectionPool, "CREATE OPERATOR testschema.#### (LEFTARG = bigint, PROCEDURE = numeric_fac)")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP OPERATOR testschema.#### (bigint, NONE)")
				testhelper.AssertQueryRuns(connectionPool, "COMMENT ON OPERATOR testschema.#### (bigint, NONE) IS 'This is an operator comment.'")

				backupCmdFlags.Set(utils.INCLUDE_SCHEMA, "testschema")
				resultMetadataMap := backup.GetMetadataForObjectType(connectionPool, backup.TYPE_OPERATOR)

				Expect(resultMetadataMap).To(HaveLen(1))
				uniqueID := testutils.UniqueIDFromObjectName(connectionPool, "testschema", "####", backup.TYPE_OPERATOR)
				expectedMetadata := testutils.DefaultMetadata("OPERATOR", false, true, true)
				resultMetadata := resultMetadataMap[uniqueID]
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for an operator family in a specific schema", func() {
				testutils.SkipIfBefore5(connectionPool)
				testhelper.AssertQueryRuns(connectionPool, "CREATE OPERATOR FAMILY public.testfam USING hash")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP OPERATOR FAMILY public.testfam USING hash")
				testhelper.AssertQueryRuns(connectionPool, "CREATE SCHEMA testschema")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA testschema")
				testhelper.AssertQueryRuns(connectionPool, "CREATE OPERATOR FAMILY testschema.testfam USING hash")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP OPERATOR FAMILY testschema.testfam USING hash")
				testhelper.AssertQueryRuns(connectionPool, "COMMENT ON OPERATOR FAMILY testschema.testfam USING hash IS 'This is an operator family comment.'")

				backupCmdFlags.Set(utils.INCLUDE_SCHEMA, "testschema")
				resultMetadataMap := backup.GetMetadataForObjectType(connectionPool, backup.TYPE_OPERATORFAMILY)

				Expect(resultMetadataMap).To(HaveLen(1))
				uniqueID := testutils.UniqueIDFromObjectName(connectionPool, "testschema", "testfam", backup.TYPE_OPERATORFAMILY)
				expectedMetadata := testutils.DefaultMetadata("OPERATOR FAMILY", false, true, true)
				resultMetadata := resultMetadataMap[uniqueID]
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for an operator class in a specific schema", func() {
				testhelper.AssertQueryRuns(connectionPool, "CREATE OPERATOR CLASS public.testclass FOR TYPE int4 USING hash AS STORAGE int4")
				if connectionPool.Version.Before("5") {
					defer testhelper.AssertQueryRuns(connectionPool, "DROP OPERATOR CLASS public.testclass USING hash CASCADE")
				} else {
					defer testhelper.AssertQueryRuns(connectionPool, "DROP OPERATOR FAMILY public.testclass USING hash CASCADE")
				}
				testhelper.AssertQueryRuns(connectionPool, "CREATE SCHEMA testschema")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA testschema")
				testhelper.AssertQueryRuns(connectionPool, "CREATE OPERATOR CLASS testschema.testclass FOR TYPE int4 USING hash AS STORAGE int4")
				if connectionPool.Version.Before("5") {
					defer testhelper.AssertQueryRuns(connectionPool, "DROP OPERATOR CLASS testschema.testclass USING hash CASCADE")
				} else {
					defer testhelper.AssertQueryRuns(connectionPool, "DROP OPERATOR FAMILY testschema.testclass USING hash CASCADE")
				}
				testhelper.AssertQueryRuns(connectionPool, "COMMENT ON OPERATOR CLASS testschema.testclass USING hash IS 'This is an operator class comment.'")

				backupCmdFlags.Set(utils.INCLUDE_SCHEMA, "testschema")
				resultMetadataMap := backup.GetMetadataForObjectType(connectionPool, backup.TYPE_OPERATORCLASS)

				Expect(resultMetadataMap).To(HaveLen(1))
				uniqueID := testutils.UniqueIDFromObjectName(connectionPool, "testschema", "testclass", backup.TYPE_OPERATORCLASS)
				expectedMetadata := testutils.DefaultMetadata("OPERATOR CLASS", false, true, true)
				resultMetadata := resultMetadataMap[uniqueID]
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for a text search dictionary in a specific schema", func() {
				testutils.SkipIfBefore5(connectionPool)
				testhelper.AssertQueryRuns(connectionPool, "CREATE TEXT SEARCH DICTIONARY public.testdictionary(TEMPLATE = snowball, LANGUAGE = 'russian', STOPWORDS = 'russian');")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TEXT SEARCH DICTIONARY public.testdictionary")
				testhelper.AssertQueryRuns(connectionPool, "CREATE SCHEMA testschema")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA testschema")
				testhelper.AssertQueryRuns(connectionPool, "CREATE TEXT SEARCH DICTIONARY testschema.testdictionary(TEMPLATE = snowball, LANGUAGE = 'russian', STOPWORDS = 'russian');")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TEXT SEARCH DICTIONARY testschema.testdictionary")
				testhelper.AssertQueryRuns(connectionPool, "COMMENT ON TEXT SEARCH DICTIONARY testschema.testdictionary IS 'This is a text search dictionary comment.'")

				backupCmdFlags.Set(utils.INCLUDE_SCHEMA, "testschema")
				resultMetadataMap := backup.GetMetadataForObjectType(connectionPool, backup.TYPE_TSDICTIONARY)

				Expect(resultMetadataMap).To(HaveLen(1))
				uniqueID := testutils.UniqueIDFromObjectName(connectionPool, "testschema", "testdictionary", backup.TYPE_TSDICTIONARY)
				dictionaryMetadata := testutils.DefaultMetadata("TEXT SEARCH DICTIONARY", false, true, true)
				resultMetadata := resultMetadataMap[uniqueID]
				structmatcher.ExpectStructsToMatch(&dictionaryMetadata, &resultMetadata)
			})
			It("returns a slice of default metadata for a text search configuration in a specific schema", func() {
				testutils.SkipIfBefore5(connectionPool)
				resultMetadataMap := backup.GetMetadataForObjectType(connectionPool, backup.TYPE_TSCONFIGURATION)
				configurationMetadata := testutils.DefaultMetadata("TEXT SEARCH CONFIGURATION", false, true, true)

				testhelper.AssertQueryRuns(connectionPool, `CREATE TEXT SEARCH CONFIGURATION public.testconfiguration (PARSER = pg_catalog."default");`)
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TEXT SEARCH CONFIGURATION public.testconfiguration")
				testhelper.AssertQueryRuns(connectionPool, "CREATE SCHEMA testschema")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA testschema")
				testhelper.AssertQueryRuns(connectionPool, `CREATE TEXT SEARCH CONFIGURATION testschema.testconfiguration (PARSER = pg_catalog."default");`)
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TEXT SEARCH CONFIGURATION testschema.testconfiguration")
				testhelper.AssertQueryRuns(connectionPool, "COMMENT ON TEXT SEARCH CONFIGURATION testschema.testconfiguration IS 'This is a text search configuration comment.'")

				backupCmdFlags.Set(utils.INCLUDE_SCHEMA, "testschema")
				resultMetadataMap = backup.GetMetadataForObjectType(connectionPool, backup.TYPE_TSCONFIGURATION)

				Expect(resultMetadataMap).To(HaveLen(1))
				uniqueID := testutils.UniqueIDFromObjectName(connectionPool, "testschema", "testconfiguration", backup.TYPE_TSCONFIGURATION)
				resultMetadata := resultMetadataMap[uniqueID]
				structmatcher.ExpectStructsToMatch(&configurationMetadata, &resultMetadata)
			})
			It("returns a slice of default metadata for a collation in a specific schema", func() {
				testutils.SkipIfBefore6(connectionPool)
				testhelper.AssertQueryRuns(connectionPool, `CREATE COLLATION public.some_coll (lc_collate = 'POSIX', lc_ctype = 'POSIX');`)
				defer testhelper.AssertQueryRuns(connectionPool, "DROP COLLATION public.some_coll")
				testhelper.AssertQueryRuns(connectionPool, "CREATE SCHEMA testschema")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA testschema")
				testhelper.AssertQueryRuns(connectionPool, `CREATE COLLATION testschema.some_coll (lc_collate = 'POSIX', lc_ctype = 'POSIX');`)
				defer testhelper.AssertQueryRuns(connectionPool, "DROP COLLATION testschema.some_coll")
				testhelper.AssertQueryRuns(connectionPool, "COMMENT ON COLLATION testschema.some_coll IS 'This is a collation comment.'")

				backupCmdFlags.Set(utils.INCLUDE_SCHEMA, "testschema")
				resultMetadataMap := backup.GetMetadataForObjectType(connectionPool, backup.TYPE_COLLATION)

				Expect(resultMetadataMap).To(HaveLen(1))
				uniqueID := testutils.UniqueIDFromObjectName(connectionPool, "testschema", "some_coll", backup.TYPE_COLLATION)
				collationMetadata := testutils.DefaultMetadata("COLLATION", false, true, true)
				resultMetadata := resultMetadataMap[uniqueID]
				structmatcher.ExpectStructsToMatch(&collationMetadata, &resultMetadata)
			})
		})
	})
	Describe("GetCommentsForObjectType", func() {
		Context("comments for all objects of one type", func() {
			It("returns a slice of default metadata for an index", func() {
				resultMetadataMap := backup.GetCommentsForObjectType(connectionPool, backup.TYPE_INDEX)
				numIndexes := len(resultMetadataMap)

				testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE public.testtable(i int)`)
				testhelper.AssertQueryRuns(connectionPool, `CREATE INDEX testindex ON public.testtable USING btree(i)`)
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.testtable")
				testhelper.AssertQueryRuns(connectionPool, "COMMENT ON INDEX public.testindex IS 'This is an index comment.'")

				resultMetadataMap = backup.GetCommentsForObjectType(connectionPool, backup.TYPE_INDEX)

				uniqueID := testutils.UniqueIDFromObjectName(connectionPool, "", "testindex", backup.TYPE_INDEX)
				expectedMetadata := testutils.DefaultMetadata("INDEX", false, false, true)

				Expect(resultMetadataMap).To(HaveLen(numIndexes + 1))
				resultMetadata := resultMetadataMap[uniqueID]
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for a rule", func() {
				resultMetadataMap := backup.GetCommentsForObjectType(connectionPool, backup.TYPE_RULE)
				numRules := len(resultMetadataMap)

				testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE public.testtable(i int)`)
				testhelper.AssertQueryRuns(connectionPool, `CREATE RULE update_notify AS ON UPDATE TO public.testtable DO NOTIFY testtable`)
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.testtable")
				testhelper.AssertQueryRuns(connectionPool, "COMMENT ON RULE update_notify IS 'This is a rule comment.'")

				resultMetadataMap = backup.GetCommentsForObjectType(connectionPool, backup.TYPE_RULE)

				uniqueID := testutils.UniqueIDFromObjectName(connectionPool, "", "update_notify", backup.TYPE_RULE)
				expectedMetadata := testutils.DefaultMetadata("RULE", false, false, true)

				Expect(resultMetadataMap).To(HaveLen(numRules + 1))
				resultMetadata := resultMetadataMap[uniqueID]
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for a trigger", func() {
				resultMetadataMap := backup.GetCommentsForObjectType(connectionPool, backup.TYPE_TRIGGER)
				numTriggers := len(resultMetadataMap)

				testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE public.testtable(i int)`)
				testhelper.AssertQueryRuns(connectionPool, `CREATE TRIGGER sync_testtable AFTER INSERT OR DELETE OR UPDATE ON public.testtable FOR EACH STATEMENT EXECUTE PROCEDURE "RI_FKey_check_ins"()`)
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.testtable")
				testhelper.AssertQueryRuns(connectionPool, "COMMENT ON TRIGGER sync_testtable ON public.testtable IS 'This is a trigger comment.'")

				resultMetadataMap = backup.GetCommentsForObjectType(connectionPool, backup.TYPE_TRIGGER)

				uniqueID := testutils.UniqueIDFromObjectName(connectionPool, "", "sync_testtable", backup.TYPE_TRIGGER)
				expectedMetadata := testutils.DefaultMetadata("TRIGGER", false, false, true)

				Expect(resultMetadataMap).To(HaveLen(numTriggers + 1))
				resultMetadata := resultMetadataMap[uniqueID]
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for a cast in 4.3", func() {
				testutils.SkipIfNot4(connectionPool)
				resultMetadataMap := backup.GetCommentsForObjectType(connectionPool, backup.TYPE_CAST)
				numCasts := len(resultMetadataMap)

				testhelper.AssertQueryRuns(connectionPool, "CREATE FUNCTION public.casttotext(bool) RETURNS text STRICT IMMUTABLE LANGUAGE PLPGSQL AS $$ BEGIN IF $1 IS TRUE THEN RETURN 'true'; ELSE RETURN 'false'; END IF; END; $$;")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP FUNCTION public.casttotext(bool) CASCADE")
				testhelper.AssertQueryRuns(connectionPool, "CREATE CAST (bool AS text) WITH FUNCTION public.casttotext(bool) AS ASSIGNMENT")
				testhelper.AssertQueryRuns(connectionPool, "COMMENT ON CAST (bool AS text) IS 'This is a cast comment.'")

				resultMetadataMap = backup.GetCommentsForObjectType(connectionPool, backup.TYPE_CAST)

				boolOid := testutils.OidFromObjectName(connectionPool, "", "bool", backup.TYPE_TYPE)
				textOid := testutils.OidFromObjectName(connectionPool, "", "text", backup.TYPE_TYPE)
				oid := testutils.OidFromCast(connectionPool, boolOid, textOid)
				expectedMetadata := testutils.DefaultMetadata("CAST", false, false, true)

				Expect(resultMetadataMap).To(HaveLen(numCasts + 1))
				resultMetadata := resultMetadataMap[backup.UniqueID{ClassID: backup.PG_CAST_OID, Oid: oid}]
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for a cast in 5", func() {
				testutils.SkipIfBefore5(connectionPool)
				resultMetadataMap := backup.GetCommentsForObjectType(connectionPool, backup.TYPE_CAST)
				numCasts := len(resultMetadataMap)

				testhelper.AssertQueryRuns(connectionPool, `CREATE FUNCTION public.casttoint(text) RETURNS integer STRICT IMMUTABLE LANGUAGE SQL AS 'SELECT cast($1 as integer);'`)
				defer testhelper.AssertQueryRuns(connectionPool, "DROP FUNCTION public.casttoint(text) CASCADE")
				testhelper.AssertQueryRuns(connectionPool, "CREATE CAST (text AS int) WITH FUNCTION public.casttoint(text) AS ASSIGNMENT;")
				testhelper.AssertQueryRuns(connectionPool, "COMMENT ON CAST (text AS int) IS 'This is a cast comment.'")

				resultMetadataMap = backup.GetCommentsForObjectType(connectionPool, backup.TYPE_CAST)

				textOid := testutils.OidFromObjectName(connectionPool, "", "text", backup.TYPE_TYPE)
				intOid := testutils.OidFromObjectName(connectionPool, "", "int4", backup.TYPE_TYPE)
				oid := testutils.OidFromCast(connectionPool, textOid, intOid)
				expectedMetadata := testutils.DefaultMetadata("CAST", false, false, true)

				Expect(resultMetadataMap).To(HaveLen(numCasts + 1))
				resultMetadata := resultMetadataMap[backup.UniqueID{ClassID: backup.PG_CAST_OID, Oid: oid}]
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for a resource queue", func() {
				resultMetadataMap := backup.GetCommentsForObjectType(connectionPool, backup.TYPE_RESOURCEQUEUE)
				numResQueues := len(resultMetadataMap)

				testhelper.AssertQueryRuns(connectionPool, `CREATE RESOURCE QUEUE res_queue WITH (MAX_COST=32.8);`)
				defer testhelper.AssertQueryRuns(connectionPool, "DROP RESOURCE QUEUE res_queue")
				testhelper.AssertQueryRuns(connectionPool, "COMMENT ON RESOURCE QUEUE res_queue IS 'This is a resource queue comment.'")

				resultMetadataMap = backup.GetCommentsForObjectType(connectionPool, backup.TYPE_RESOURCEQUEUE)

				uniqueID := testutils.UniqueIDFromObjectName(connectionPool, "", "res_queue", backup.TYPE_RESOURCEQUEUE)
				expectedMetadata := testutils.DefaultMetadata("RESOURCE QUEUE", false, false, true)

				Expect(resultMetadataMap).To(HaveLen(numResQueues + 1))
				resultMetadata := resultMetadataMap[uniqueID]
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for a role", func() {
				resultMetadataMap := backup.GetCommentsForObjectType(connectionPool, backup.TYPE_ROLE)
				numRoles := len(resultMetadataMap)

				testhelper.AssertQueryRuns(connectionPool, `CREATE ROLE testuser`)
				defer testhelper.AssertQueryRuns(connectionPool, "DROP ROLE testuser")
				testhelper.AssertQueryRuns(connectionPool, "COMMENT ON ROLE testuser IS 'This is a role comment.'")

				resultMetadataMap = backup.GetCommentsForObjectType(connectionPool, backup.TYPE_ROLE)

				uniqueID := testutils.UniqueIDFromObjectName(connectionPool, "", "testuser", backup.TYPE_ROLE)
				expectedMetadata := testutils.DefaultMetadata("ROLE", false, false, true)

				Expect(resultMetadataMap).To(HaveLen(numRoles + 1))
				resultMetadata := resultMetadataMap[uniqueID]
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for a text search parser", func() {
				testutils.SkipIfBefore5(connectionPool)
				parserMetadata := testutils.DefaultMetadata("TEXT SEARCH PARSER", false, false, true)

				testhelper.AssertQueryRuns(connectionPool, "CREATE TEXT SEARCH PARSER public.testparser(START = prsd_start, GETTOKEN = prsd_nexttoken, END = prsd_end, LEXTYPES = prsd_lextype);")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TEXT SEARCH PARSER public.testparser")
				testhelper.AssertQueryRuns(connectionPool, "COMMENT ON TEXT SEARCH PARSER public.testparser IS 'This is a text search parser comment.'")

				uniqueID := testutils.UniqueIDFromObjectName(connectionPool, "public", "testparser", backup.TYPE_TSPARSER)
				resultMetadataMap := backup.GetCommentsForObjectType(connectionPool, backup.TYPE_TSPARSER)

				Expect(resultMetadataMap).To(HaveLen(1))
				resultMetadata := resultMetadataMap[uniqueID]
				structmatcher.ExpectStructsToMatch(&parserMetadata, &resultMetadata)
			})
			It("returns a slice of default metadata for a text search template", func() {
				testutils.SkipIfBefore5(connectionPool)
				templateMetadata := testutils.DefaultMetadata("TEXT SEARCH TEMPLATE", false, false, true)

				testhelper.AssertQueryRuns(connectionPool, "CREATE TEXT SEARCH TEMPLATE public.testtemplate(LEXIZE = dsimple_lexize);")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TEXT SEARCH TEMPLATE public.testtemplate")
				testhelper.AssertQueryRuns(connectionPool, "COMMENT ON TEXT SEARCH TEMPLATE public.testtemplate IS 'This is a text search template comment.'")

				uniqueID := testutils.UniqueIDFromObjectName(connectionPool, "public", "testtemplate", backup.TYPE_TSTEMPLATE)
				resultMetadataMap := backup.GetCommentsForObjectType(connectionPool, backup.TYPE_TSTEMPLATE)

				Expect(resultMetadataMap).To(HaveLen(1))
				resultMetadata := resultMetadataMap[uniqueID]
				structmatcher.ExpectStructsToMatch(&templateMetadata, &resultMetadata)
			})
			It("returns a slice of default metadata for an extension", func() {
				testutils.SkipIfBefore5(connectionPool)
				extensionMetadata := testutils.DefaultMetadata("EXTENSION", false, false, true)

				testhelper.AssertQueryRuns(connectionPool, "CREATE EXTENSION plperl;")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP EXTENSION plperl")
				testhelper.AssertQueryRuns(connectionPool, "COMMENT ON EXTENSION plperl IS 'This is an extension comment.'")

				uniqueID := testutils.UniqueIDFromObjectName(connectionPool, "", "plperl", backup.TYPE_EXTENSION)
				resultMetadataMap := backup.GetCommentsForObjectType(connectionPool, backup.TYPE_EXTENSION)

				Expect(resultMetadataMap).To(HaveLen(1))
				resultMetadata := resultMetadataMap[uniqueID]
				structmatcher.ExpectStructsToMatch(&extensionMetadata, &resultMetadata)
			})
		})
		Context("comments for objects in a specific schema", func() {
			It("returns a slice of default metadata for an index in a specific schema", func() {

				testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE public.testtable(i int)`)
				testhelper.AssertQueryRuns(connectionPool, `CREATE INDEX testindex ON public.testtable USING btree(i)`)
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.testtable")
				testhelper.AssertQueryRuns(connectionPool, "CREATE SCHEMA testschema")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA testschema")
				testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE testschema.testtable(i int)`)
				testhelper.AssertQueryRuns(connectionPool, `CREATE INDEX testindex1 ON testschema.testtable USING btree(i)`)
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE testschema.testtable")
				testhelper.AssertQueryRuns(connectionPool, "COMMENT ON INDEX testschema.testindex1 IS 'This is an index comment.'")

				backupCmdFlags.Set(utils.INCLUDE_SCHEMA, "testschema")
				resultMetadataMap := backup.GetCommentsForObjectType(connectionPool, backup.TYPE_INDEX)

				uniqueID := testutils.UniqueIDFromObjectName(connectionPool, "", "testindex1", backup.TYPE_INDEX)
				expectedMetadata := testutils.DefaultMetadata("INDEX", false, false, true)

				resultMetadata := resultMetadataMap[uniqueID]
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for a constraint in a specific schema", func() {
				testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE public.testtable(i int UNIQUE)`)
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.testtable")
				testhelper.AssertQueryRuns(connectionPool, "COMMENT ON CONSTRAINT testtable_i_key ON public.testtable IS 'This is a constraint comment.'")

				testhelper.AssertQueryRuns(connectionPool, "CREATE SCHEMA testschema")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA testschema")
				testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE testschema.testtable(i int UNIQUE)`)
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE testschema.testtable")
				testhelper.AssertQueryRuns(connectionPool, "COMMENT ON CONSTRAINT testtable_i_key ON testschema.testtable IS 'This is a constraint comment.'")
				backupCmdFlags.Set(utils.INCLUDE_SCHEMA, "testschema")

				resultMetadataMap := backup.GetCommentsForObjectType(connectionPool, backup.TYPE_CONSTRAINT)

				uniqueID := testutils.UniqueIDFromObjectName(connectionPool, "testschema", "testtable_i_key", backup.TYPE_CONSTRAINT)
				expectedMetadata := testutils.DefaultMetadata("CONSTRAINT", false, false, true)

				Expect(resultMetadataMap).To(HaveLen(1))
				resultMetadata := resultMetadataMap[uniqueID]
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for a text search parser in a specific schema", func() {
				testutils.SkipIfBefore5(connectionPool)
				parserMetadata := testutils.DefaultMetadata("TEXT SEARCH PARSER", false, false, true)

				testhelper.AssertQueryRuns(connectionPool, "CREATE TEXT SEARCH PARSER public.testparser(START = prsd_start, GETTOKEN = prsd_nexttoken, END = prsd_end, LEXTYPES = prsd_lextype);")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TEXT SEARCH PARSER public.testparser")
				testhelper.AssertQueryRuns(connectionPool, "CREATE SCHEMA testschema")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA testschema")
				testhelper.AssertQueryRuns(connectionPool, "CREATE TEXT SEARCH PARSER testschema.testparser(START = prsd_start, GETTOKEN = prsd_nexttoken, END = prsd_end, LEXTYPES = prsd_lextype);")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TEXT SEARCH PARSER testschema.testparser")
				testhelper.AssertQueryRuns(connectionPool, "COMMENT ON TEXT SEARCH PARSER testschema.testparser IS 'This is a text search parser comment.'")

				uniqueID := testutils.UniqueIDFromObjectName(connectionPool, "testschema", "testparser", backup.TYPE_TSPARSER)
				backupCmdFlags.Set(utils.INCLUDE_SCHEMA, "testschema")
				resultMetadataMap := backup.GetCommentsForObjectType(connectionPool, backup.TYPE_TSPARSER)

				Expect(resultMetadataMap).To(HaveLen(1))
				resultMetadata := resultMetadataMap[uniqueID]
				structmatcher.ExpectStructsToMatch(&parserMetadata, &resultMetadata)
			})
			It("returns a slice of default metadata for a text search template in a specific schema", func() {
				testutils.SkipIfBefore5(connectionPool)
				templateMetadata := testutils.DefaultMetadata("TEXT SEARCH TEMPLATE", false, false, true)

				testhelper.AssertQueryRuns(connectionPool, "CREATE TEXT SEARCH TEMPLATE public.testtemplate(LEXIZE = dsimple_lexize);")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TEXT SEARCH TEMPLATE public.testtemplate")
				testhelper.AssertQueryRuns(connectionPool, "CREATE SCHEMA testschema")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA testschema")
				testhelper.AssertQueryRuns(connectionPool, "CREATE TEXT SEARCH TEMPLATE testschema.testtemplate(LEXIZE = dsimple_lexize);")
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TEXT SEARCH TEMPLATE testschema.testtemplate")
				testhelper.AssertQueryRuns(connectionPool, "COMMENT ON TEXT SEARCH TEMPLATE testschema.testtemplate IS 'This is a text search template comment.'")

				uniqueID := testutils.UniqueIDFromObjectName(connectionPool, "testschema", "testtemplate", backup.TYPE_TSTEMPLATE)
				backupCmdFlags.Set(utils.INCLUDE_SCHEMA, "testschema")
				resultMetadataMap := backup.GetCommentsForObjectType(connectionPool, backup.TYPE_TSTEMPLATE)

				Expect(resultMetadataMap).To(HaveLen(1))
				resultMetadata := resultMetadataMap[uniqueID]
				structmatcher.ExpectStructsToMatch(&templateMetadata, &resultMetadata)
			})
		})
	})
})
