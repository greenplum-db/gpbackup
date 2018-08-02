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
			testhelper.AssertQueryRuns(connection, "CREATE SCHEMA bar")
			defer testhelper.AssertQueryRuns(connection, "DROP SCHEMA bar")
			schemas := backup.GetAllUserSchemas(connection)

			schemaBar := backup.Schema{Oid: 0, Name: "bar"}
			schemaPublic := backup.Schema{Oid: 2200, Name: "public"}

			Expect(len(schemas)).To(Equal(2))
			structmatcher.ExpectStructsToMatchExcluding(&schemaBar, &schemas[0], "Oid")
			structmatcher.ExpectStructsToMatchExcluding(&schemaPublic, &schemas[1], "Owner")
		})

		It("returns schema information for single specific schema", func() {
			testhelper.AssertQueryRuns(connection, "CREATE SCHEMA bar")
			defer testhelper.AssertQueryRuns(connection, "DROP SCHEMA bar")
			cmdFlags.Set(utils.INCLUDE_SCHEMA, "bar")

			schemas := backup.GetAllUserSchemas(connection)

			schemaBar := backup.Schema{Oid: 0, Name: "bar"}

			Expect(len(schemas)).To(Equal(1))
			structmatcher.ExpectStructsToMatchExcluding(&schemaBar, &schemas[0], "Oid")

		})

		It("returns schema information for multiple specific schemas", func() {
			testhelper.AssertQueryRuns(connection, "CREATE SCHEMA bar")
			defer testhelper.AssertQueryRuns(connection, "DROP SCHEMA bar")
			cmdFlags.Set(utils.INCLUDE_SCHEMA, "bar,public")
			schemas := backup.GetAllUserSchemas(connection)

			schemaBar := backup.Schema{Oid: 0, Name: "bar"}
			schemaPublic := backup.Schema{Oid: 2200, Name: "public"}

			Expect(len(schemas)).To(Equal(2))
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
				testhelper.AssertQueryRuns(connection, "CREATE TABLE public.no_constraints_table(a int, b text)")
				defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.no_constraints_table")

				constraints := backup.GetConstraints(connection)

				Expect(len(constraints)).To(Equal(0))
			})
		})
		Context("One constraint", func() {
			It("returns a constraint array for a table with one UNIQUE constraint and a comment", func() {
				testhelper.AssertQueryRuns(connection, "CREATE TABLE public.constraints_table(a int, b text, c float)")
				defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.constraints_table")
				testhelper.AssertQueryRuns(connection, "ALTER TABLE ONLY public.constraints_table ADD CONSTRAINT uniq2 UNIQUE (a, b)")
				testhelper.AssertQueryRuns(connection, "COMMENT ON CONSTRAINT uniq2 ON public.constraints_table IS 'this is a constraint comment'")

				constraints := backup.GetConstraints(connection)

				Expect(len(constraints)).To(Equal(1))
				structmatcher.ExpectStructsToMatchExcluding(&constraints[0], &uniqueConstraint, "Oid")
			})
			It("returns a constraint array for a table with one PRIMARY KEY constraint and a comment", func() {
				testhelper.AssertQueryRuns(connection, "CREATE TABLE public.constraints_table(a int, b text, c float)")
				defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.constraints_table")
				testhelper.AssertQueryRuns(connection, "ALTER TABLE ONLY public.constraints_table ADD CONSTRAINT pk1 PRIMARY KEY (b)")
				testhelper.AssertQueryRuns(connection, "COMMENT ON CONSTRAINT pk1 ON public.constraints_table IS 'this is a constraint comment'")

				constraints := backup.GetConstraints(connection)

				Expect(len(constraints)).To(Equal(1))
				structmatcher.ExpectStructsToMatchExcluding(&constraints[0], &pkConstraint, "Oid")
			})
			It("returns a constraint array for a table with one FOREIGN KEY constraint", func() {
				testhelper.AssertQueryRuns(connection, "CREATE TABLE public.constraints_table(a int, b text, c float)")
				defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.constraints_table CASCADE")
				testhelper.AssertQueryRuns(connection, "CREATE TABLE public.constraints_other_table(b text)")
				defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.constraints_other_table CASCADE")
				testhelper.AssertQueryRuns(connection, "ALTER TABLE ONLY public.constraints_table ADD CONSTRAINT pk1 PRIMARY KEY (b)")
				testhelper.AssertQueryRuns(connection, "ALTER TABLE ONLY public.constraints_other_table ADD CONSTRAINT fk1 FOREIGN KEY (b) REFERENCES public.constraints_table(b)")

				constraints := backup.GetConstraints(connection)

				Expect(len(constraints)).To(Equal(2))
				structmatcher.ExpectStructsToMatchExcluding(&constraints[0], &fkConstraint, "Oid")
				structmatcher.ExpectStructsToMatchExcluding(&constraints[1], &pkConstraint, "Oid")
			})
			It("returns a constraint array for a table with one CHECK constraint", func() {
				testhelper.AssertQueryRuns(connection, "CREATE TABLE public.constraints_table(a int, b text, c float)")
				defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.constraints_table")
				testhelper.AssertQueryRuns(connection, "ALTER TABLE ONLY public.constraints_table ADD CONSTRAINT check1 CHECK (a <> 42)")

				constraints := backup.GetConstraints(connection)

				Expect(len(constraints)).To(Equal(1))
				structmatcher.ExpectStructsToMatchExcluding(&constraints[0], &checkConstraint, "Oid")
			})
			It("returns a constraint array for a parent partition table with one CHECK constraint", func() {
				testhelper.AssertQueryRuns(connection, `CREATE TABLE public.part (id int, date date, amt decimal(10,2) default 0.0) DISTRIBUTED BY (id)
PARTITION BY RANGE (date)
      (PARTITION Jan08 START (date '2008-01-01') INCLUSIVE ,
      PARTITION Feb08 START (date '2008-02-01') INCLUSIVE ,
      PARTITION Mar08 START (date '2008-03-01') INCLUSIVE
      END (date '2008-04-01') EXCLUSIVE);`)
				defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.part")
				testhelper.AssertQueryRuns(connection, "ALTER TABLE public.part ADD CONSTRAINT check1 CHECK (id <> 0)")

				constraints := backup.GetConstraints(connection)

				Expect(len(constraints)).To(Equal(1))
				structmatcher.ExpectStructsToMatchExcluding(&constraints[0], &partitionCheckConstraint, "Oid")
			})
			It("returns a constraint array for a domain", func() {
				testhelper.AssertQueryRuns(connection, "CREATE DOMAIN public.constraint_domain AS int")
				defer testhelper.AssertQueryRuns(connection, "DROP DOMAIN public.constraint_domain")
				testhelper.AssertQueryRuns(connection, "ALTER DOMAIN public.constraint_domain ADD CONSTRAINT check1 CHECK (VALUE <> 42)")

				constraints := backup.GetConstraints(connection)

				Expect(len(constraints)).To(Equal(1))
				structmatcher.ExpectStructsToMatchExcluding(&constraints[0], &domainConstraint, "Oid")
			})
			It("does not return a constraint array for a table that inherits a constraint from another table", func() {
				testhelper.AssertQueryRuns(connection, "CREATE TABLE public.constraints_table(a int, b text, c float)")
				defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.constraints_table")
				testhelper.AssertQueryRuns(connection, "ALTER TABLE ONLY public.constraints_table ADD CONSTRAINT check1 CHECK (a <> 42)")

				testhelper.AssertQueryRuns(connection, "CREATE TABLE public.constraints_child_table(a int, b text, c float) INHERITS (public.constraints_table)")
				defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.constraints_child_table")

				constraints := backup.GetConstraints(connection)

				Expect(len(constraints)).To(Equal(1))
				structmatcher.ExpectStructsToMatchExcluding(&constraints[0], &checkConstraint, "Oid")
			})
			It("returns a constraint array for a table that inherits from another table and has an additional constraint", func() {
				testhelper.AssertQueryRuns(connection, "CREATE TABLE public.parent_table(a int, b text, c float)")
				defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.parent_table")

				testhelper.AssertQueryRuns(connection, "CREATE TABLE public.constraints_table(a int, b text, c float) INHERITS (public.parent_table)")
				defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.constraints_table")

				testhelper.AssertQueryRuns(connection, "ALTER TABLE ONLY public.constraints_table ADD CONSTRAINT check1 CHECK (a <> 42)")

				constraints := backup.GetConstraints(connection)

				Expect(len(constraints)).To(Equal(1))
				structmatcher.ExpectStructsToMatchExcluding(&constraints[0], &checkConstraint, "Oid")
			})
			It("returns a constraint array for a table in a specific schema", func() {
				testhelper.AssertQueryRuns(connection, "CREATE TABLE public.constraints_table(a int, b text, c float)")
				defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.constraints_table")
				testhelper.AssertQueryRuns(connection, "ALTER TABLE ONLY public.constraints_table ADD CONSTRAINT uniq2 UNIQUE (a, b)")
				testhelper.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
				defer testhelper.AssertQueryRuns(connection, "DROP SCHEMA testschema")
				testhelper.AssertQueryRuns(connection, "CREATE TABLE testschema.constraints_table(a int, b text, c float)")
				defer testhelper.AssertQueryRuns(connection, "DROP TABLE testschema.constraints_table")
				testhelper.AssertQueryRuns(connection, "ALTER TABLE ONLY testschema.constraints_table ADD CONSTRAINT uniq2 UNIQUE (a, b)")
				cmdFlags.Set(utils.INCLUDE_SCHEMA, "testschema")
				constraintInSchema := backup.Constraint{Oid: 0, Schema: "testschema", Name: "uniq2", ConType: "u", ConDef: "UNIQUE (a, b)", OwningObject: "testschema.constraints_table", IsDomainConstraint: false, IsPartitionParent: false}

				constraints := backup.GetConstraints(connection)

				Expect(len(constraints)).To(Equal(1))
				structmatcher.ExpectStructsToMatchExcluding(&constraints[0], &constraintInSchema, "Oid")
			})
			It("returns a constraint array for only the tables included in the backup set", func() {
				testhelper.AssertQueryRuns(connection, "CREATE TABLE public.constraints_table(a int, b text, c float)")
				defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.constraints_table")
				testhelper.AssertQueryRuns(connection, "ALTER TABLE ONLY public.constraints_table ADD CONSTRAINT uniq2 UNIQUE (a, b)")
				testhelper.AssertQueryRuns(connection, "COMMENT ON CONSTRAINT uniq2 ON public.constraints_table IS 'this is a constraint comment'")
				testhelper.AssertQueryRuns(connection, "CREATE TABLE public.other_table(d bool, e float)")
				defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.other_table")

				constraintsOid := testutils.OidFromObjectName(connection, "public", "constraints_table", backup.TYPE_RELATION)
				otherOid := testutils.OidFromObjectName(connection, "public", "other_table", backup.TYPE_RELATION)
				tables := []backup.Relation{{Oid: constraintsOid, Schema: "public", Name: "constraints_table"}}
				constraints := backup.GetConstraints(connection, tables...)
				Expect(len(constraints)).To(Equal(1))
				structmatcher.ExpectStructsToMatchExcluding(&constraints[0], &uniqueConstraint, "Oid")

				tables = []backup.Relation{{Oid: otherOid, Schema: "public", Name: "other_table"}}
				constraints = backup.GetConstraints(connection, tables...)
				Expect(len(constraints)).To(Equal(0))
			})
			It("returns a constraint array without contraints on tables in the exclude set", func() {
				testhelper.AssertQueryRuns(connection, "CREATE TABLE public.constraints_table(a int, b text, c float)")
				defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.constraints_table")
				testhelper.AssertQueryRuns(connection, "ALTER TABLE ONLY public.constraints_table ADD CONSTRAINT uniq2 UNIQUE (a, b)")
				testhelper.AssertQueryRuns(connection, "COMMENT ON CONSTRAINT uniq2 ON public.constraints_table IS 'this is a constraint comment'")
				testhelper.AssertQueryRuns(connection, "CREATE TABLE public.other_table(d bool, e float)")
				defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.other_table")

				cmdFlags.Set(utils.EXCLUDE_RELATION, "public.other_table")
				defer cmdFlags.Set(utils.EXCLUDE_RELATION, "")
				constraints := backup.GetConstraints(connection)
				Expect(len(constraints)).To(Equal(1))
				structmatcher.ExpectStructsToMatchExcluding(&constraints[0], &uniqueConstraint, "Oid")

				cmdFlags.Set(utils.EXCLUDE_RELATION, "public.constraints_table")
				constraints = backup.GetConstraints(connection)
				Expect(len(constraints)).To(Equal(0))
			})
		})
		Context("Multiple constraints", func() {
			It("returns a constraint array for a table with multiple constraints", func() {
				testhelper.AssertQueryRuns(connection, "CREATE TABLE public.constraints_table(a int, b text, c float) DISTRIBUTED BY (b)")
				defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.constraints_table CASCADE")
				testhelper.AssertQueryRuns(connection, "CREATE TABLE public.constraints_other_table(b text)")
				defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.constraints_other_table CASCADE")
				testhelper.AssertQueryRuns(connection, "ALTER TABLE ONLY public.constraints_table ADD CONSTRAINT uniq2 UNIQUE (a, b)")
				testhelper.AssertQueryRuns(connection, "COMMENT ON CONSTRAINT uniq2 ON public.constraints_table IS 'this is a constraint comment'")
				testhelper.AssertQueryRuns(connection, "ALTER TABLE ONLY public.constraints_table ADD CONSTRAINT pk1 PRIMARY KEY (b)")
				testhelper.AssertQueryRuns(connection, "COMMENT ON CONSTRAINT pk1 ON public.constraints_table IS 'this is a constraint comment'")
				testhelper.AssertQueryRuns(connection, "ALTER TABLE ONLY public.constraints_other_table ADD CONSTRAINT fk1 FOREIGN KEY (b) REFERENCES public.constraints_table(b)")
				testhelper.AssertQueryRuns(connection, "ALTER TABLE ONLY public.constraints_table ADD CONSTRAINT check1 CHECK (a <> 42)")

				constraints := backup.GetConstraints(connection)

				Expect(len(constraints)).To(Equal(4))
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
				testhelper.AssertQueryRuns(connection, "CREATE TABLE public.foo(i int)")
				defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.foo")
				testhelper.AssertQueryRuns(connection, "REVOKE DELETE ON TABLE public.foo FROM testrole")
				testhelper.AssertQueryRuns(connection, "CREATE TABLE public.bar(i int)")
				defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.bar")
				testhelper.AssertQueryRuns(connection, "REVOKE ALL ON TABLE public.bar FROM testrole")
				testhelper.AssertQueryRuns(connection, "CREATE TABLE public.baz(i int)")
				defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.baz")
				testhelper.AssertQueryRuns(connection, "GRANT ALL ON TABLE public.baz TO anothertestrole")

				resultMetadataMap := backup.GetMetadataForObjectType(connection, backup.TYPE_RELATION)

				fooOid := testutils.OidFromObjectName(connection, "public", "foo", backup.TYPE_RELATION)
				barOid := testutils.OidFromObjectName(connection, "public", "bar", backup.TYPE_RELATION)
				bazOid := testutils.OidFromObjectName(connection, "public", "baz", backup.TYPE_RELATION)
				expectedFoo := backup.ObjectMetadata{Privileges: []backup.ACL{testutils.DefaultACLWithout("testrole", "TABLE", "DELETE")}, Owner: "testrole"}
				expectedBar := backup.ObjectMetadata{Privileges: []backup.ACL{{Grantee: "GRANTEE"}}, Owner: "testrole"}
				expectedBaz := backup.ObjectMetadata{Privileges: []backup.ACL{testutils.DefaultACLForType("anothertestrole", "TABLE"), testutils.DefaultACLForType("testrole", "TABLE")}, Owner: "testrole"}
				Expect(len(resultMetadataMap)).To(Equal(3))
				resultFoo := resultMetadataMap[fooOid]
				resultBar := resultMetadataMap[barOid]
				resultBaz := resultMetadataMap[bazOid]
				structmatcher.ExpectStructsToMatch(&resultFoo, &expectedFoo)
				structmatcher.ExpectStructsToMatch(&resultBar, &expectedBar)
				structmatcher.ExpectStructsToMatch(&resultBaz, &expectedBaz)
			})
			It("returns a slice of default metadata for a database", func() {
				testhelper.AssertQueryRuns(connection, "GRANT ALL ON DATABASE testdb TO anothertestrole")
				defer testhelper.AssertQueryRuns(connection, "REVOKE ALL ON DATABASE testdb FROM anothertestRole")
				testhelper.AssertQueryRuns(connection, "COMMENT ON DATABASE testdb IS 'This is a database comment.'")
				expectedMetadata := backup.ObjectMetadata{Privileges: []backup.ACL{
					{Grantee: "", Temporary: true, Connect: true},
					{Grantee: "anothertestrole", Create: true, Temporary: true, Connect: true},
				}, Owner: "anothertestrole", Comment: "This is a database comment."}

				resultMetadataMap := backup.GetMetadataForObjectType(connection, backup.TYPE_DATABASE)

				oid := testutils.OidFromObjectName(connection, "", "testdb", backup.TYPE_DATABASE)
				resultMetadata := resultMetadataMap[oid]
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for a table", func() {
				testhelper.AssertQueryRuns(connection, "CREATE TABLE public.testtable(i int)")
				defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.testtable")
				testhelper.AssertQueryRuns(connection, "GRANT ALL ON TABLE public.testtable TO testrole")
				testhelper.AssertQueryRuns(connection, "COMMENT ON TABLE public.testtable IS 'This is a table comment.'")

				resultMetadataMap := backup.GetMetadataForObjectType(connection, backup.TYPE_RELATION)

				oid := testutils.OidFromObjectName(connection, "public", "testtable", backup.TYPE_RELATION)
				expectedMetadata := testutils.DefaultMetadataMap("TABLE", true, true, true)[1]
				Expect(len(resultMetadataMap)).To(Equal(1))
				resultMetadata := resultMetadataMap[oid]
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for a sequence", func() {
				testhelper.AssertQueryRuns(connection, "CREATE SEQUENCE public.testsequence")
				defer testhelper.AssertQueryRuns(connection, "DROP SEQUENCE public.testsequence")
				testhelper.AssertQueryRuns(connection, "GRANT ALL ON SEQUENCE public.testsequence TO testrole")
				testhelper.AssertQueryRuns(connection, "COMMENT ON SEQUENCE public.testsequence IS 'This is a sequence comment.'")

				resultMetadataMap := backup.GetMetadataForObjectType(connection, backup.TYPE_RELATION)

				oid := testutils.OidFromObjectName(connection, "public", "testsequence", backup.TYPE_RELATION)
				expectedMetadata := testutils.DefaultMetadataMap("SEQUENCE", true, true, true)[1]
				Expect(len(resultMetadataMap)).To(Equal(1))
				resultMetadata := resultMetadataMap[oid]
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for a function", func() {
				testhelper.AssertQueryRuns(connection, `CREATE FUNCTION public.add(integer, integer) RETURNS integer
AS 'SELECT $1 + $2'
LANGUAGE SQL`)
				defer testhelper.AssertQueryRuns(connection, "DROP FUNCTION public.add(integer, integer)")
				testhelper.AssertQueryRuns(connection, "GRANT ALL ON FUNCTION public.add(integer, integer) TO testrole")
				testhelper.AssertQueryRuns(connection, "REVOKE ALL ON FUNCTION public.add(integer, integer) FROM PUBLIC")
				testhelper.AssertQueryRuns(connection, "COMMENT ON FUNCTION public.add(integer, integer) IS 'This is a function comment.'")

				resultMetadataMap := backup.GetMetadataForObjectType(connection, backup.TYPE_FUNCTION)

				oid := testutils.OidFromObjectName(connection, "public", "add", backup.TYPE_FUNCTION)
				expectedMetadata := testutils.DefaultMetadataMap("FUNCTION", true, true, true)[1]
				Expect(len(resultMetadataMap)).To(Equal(1))
				resultMetadata := resultMetadataMap[oid]
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for a view", func() {
				testhelper.AssertQueryRuns(connection, `CREATE VIEW public.testview AS SELECT * FROM pg_class`)
				defer testhelper.AssertQueryRuns(connection, "DROP VIEW public.testview")
				testhelper.AssertQueryRuns(connection, "GRANT ALL ON public.testview TO testrole")
				testhelper.AssertQueryRuns(connection, "COMMENT ON VIEW public.testview IS 'This is a view comment.'")

				resultMetadataMap := backup.GetMetadataForObjectType(connection, backup.TYPE_RELATION)

				oid := testutils.OidFromObjectName(connection, "public", "testview", backup.TYPE_RELATION)
				expectedMetadata := testutils.DefaultMetadataMap("VIEW", true, true, true)[1]
				Expect(len(resultMetadataMap)).To(Equal(1))
				resultMetadata := resultMetadataMap[oid]
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for a schema", func() {
				testhelper.AssertQueryRuns(connection, `CREATE SCHEMA testschema`)
				defer testhelper.AssertQueryRuns(connection, "DROP SCHEMA testschema")
				testhelper.AssertQueryRuns(connection, "GRANT ALL ON SCHEMA testschema TO testrole")
				testhelper.AssertQueryRuns(connection, "COMMENT ON SCHEMA testschema IS 'This is a schema comment.'")

				resultMetadataMap := backup.GetMetadataForObjectType(connection, backup.TYPE_SCHEMA)

				oid := testutils.OidFromObjectName(connection, "", "testschema", backup.TYPE_SCHEMA)
				expectedMetadata := testutils.DefaultMetadataMap("SCHEMA", true, true, true)[1]
				resultMetadata := resultMetadataMap[oid]
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for an aggregate", func() {
				testhelper.AssertQueryRuns(connection, `
			CREATE FUNCTION public.mysfunc_accum(numeric, numeric, numeric)
			   RETURNS numeric
			   AS 'select $1 + $2 + $3'
			   LANGUAGE SQL
			   IMMUTABLE
			   RETURNS NULL ON NULL INPUT;
			`)
				defer testhelper.AssertQueryRuns(connection, "DROP FUNCTION public.mysfunc_accum(numeric, numeric, numeric)")
				testhelper.AssertQueryRuns(connection, `
			CREATE FUNCTION public.mypre_accum(numeric, numeric)
			   RETURNS numeric
			   AS 'select $1 + $2'
			   LANGUAGE SQL
			   IMMUTABLE
			   RETURNS NULL ON NULL INPUT;
			`)
				defer testhelper.AssertQueryRuns(connection, "DROP FUNCTION public.mypre_accum(numeric, numeric)")
				testhelper.AssertQueryRuns(connection, `CREATE AGGREGATE public.agg_prefunc(numeric, numeric) (
	SFUNC = public.mysfunc_accum,
	STYPE = numeric,
	PREFUNC = public.mypre_accum,
	INITCOND = '0'
);`)
				defer testhelper.AssertQueryRuns(connection, "DROP AGGREGATE public.agg_prefunc(numeric, numeric)")
				testhelper.AssertQueryRuns(connection, "COMMENT ON AGGREGATE public.agg_prefunc(numeric, numeric) IS 'This is an aggregate comment.'")

				resultMetadataMap := backup.GetMetadataForObjectType(connection, backup.TYPE_AGGREGATE)

				oid := testutils.OidFromObjectName(connection, "", "agg_prefunc", backup.TYPE_AGGREGATE)
				expectedMetadata := testutils.DefaultMetadataMap("AGGREGATE", false, true, true)[1]
				resultMetadata := resultMetadataMap[oid]
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for a type", func() {
				testhelper.AssertQueryRuns(connection, `CREATE TYPE public.testtype AS (name text, num numeric)`)
				defer testhelper.AssertQueryRuns(connection, "DROP TYPE public.testtype")
				testhelper.AssertQueryRuns(connection, "COMMENT ON TYPE public.testtype IS 'This is a type comment.'")

				resultMetadataMap := backup.GetMetadataForObjectType(connection, backup.TYPE_TYPE)

				oid := testutils.OidFromObjectName(connection, "", "testtype", backup.TYPE_TYPE)
				expectedMetadata := testutils.DefaultMetadataMap("TYPE", false, true, true)[1]
				resultMetadata := resultMetadataMap[oid]
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for a domain", func() {
				testhelper.AssertQueryRuns(connection, `CREATE DOMAIN public.domain_type AS numeric`)
				defer testhelper.AssertQueryRuns(connection, "DROP TYPE public.domain_type")
				testhelper.AssertQueryRuns(connection, "COMMENT ON DOMAIN public.domain_type IS 'This is a domain comment.'")

				resultMetadataMap := backup.GetMetadataForObjectType(connection, backup.TYPE_TYPE)

				oid := testutils.OidFromObjectName(connection, "", "domain_type", backup.TYPE_TYPE)
				expectedMetadata := testutils.DefaultMetadataMap("DOMAIN", false, true, true)[1]
				resultMetadata := resultMetadataMap[oid]
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for an external protocol", func() {
				if connection.Version.Is("6") {
					Skip("ACL bug currently in master, skipping test until bug is fixed")
					// See https://github.com/greenplum-db/gpdb/issues/5382 for details.
				}
				testhelper.AssertQueryRuns(connection, `CREATE OR REPLACE FUNCTION public.read_from_s3() RETURNS integer AS '$libdir/gps3ext.so', 's3_import' LANGUAGE C STABLE;`)
				defer testhelper.AssertQueryRuns(connection, "DROP FUNCTION public.read_from_s3()")
				testhelper.AssertQueryRuns(connection, `CREATE TRUSTED PROTOCOL s3_read (readfunc = public.read_from_s3);`)
				defer testhelper.AssertQueryRuns(connection, "DROP PROTOCOL s3_read")
				testhelper.AssertQueryRuns(connection, "GRANT ALL ON PROTOCOL s3_read TO testrole")

				resultMetadataMap := backup.GetMetadataForObjectType(connection, backup.TYPE_PROTOCOL)

				oid := testutils.OidFromObjectName(connection, "", "s3_read", backup.TYPE_PROTOCOL)
				expectedMetadata := testutils.DefaultMetadataMap("PROTOCOL", true, true, false)[1]
				resultMetadata := resultMetadataMap[oid]
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for a tablespace", func() {
				if connection.Version.Before("6") {
					testhelper.AssertQueryRuns(connection, "CREATE TABLESPACE test_tablespace FILESPACE test_dir")
				} else {
					testhelper.AssertQueryRuns(connection, "CREATE TABLESPACE test_tablespace LOCATION '/tmp/test_dir'")
				}
				defer testhelper.AssertQueryRuns(connection, "DROP TABLESPACE test_tablespace")
				testhelper.AssertQueryRuns(connection, "COMMENT ON TABLESPACE test_tablespace IS 'This is a tablespace comment.'")
				testhelper.AssertQueryRuns(connection, "GRANT ALL ON TABLESPACE test_tablespace TO testrole")

				resultMetadataMap := backup.GetMetadataForObjectType(connection, backup.TYPE_TABLESPACE)

				oid := testutils.OidFromObjectName(connection, "", "test_tablespace", backup.TYPE_TABLESPACE)
				expectedMetadata := testutils.DefaultMetadataMap("TABLESPACE", true, true, true)[1]
				resultMetadata := resultMetadataMap[oid]
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for an operator", func() {
				testhelper.AssertQueryRuns(connection, "CREATE OPERATOR public.#### (LEFTARG = bigint, PROCEDURE = numeric_fac)")
				defer testhelper.AssertQueryRuns(connection, "DROP OPERATOR public.#### (bigint, NONE)")

				testhelper.AssertQueryRuns(connection, "COMMENT ON OPERATOR public.#### (bigint, NONE) IS 'This is an operator comment.'")

				resultMetadataMap := backup.GetMetadataForObjectType(connection, backup.TYPE_OPERATOR)

				oid := testutils.OidFromObjectName(connection, "", "####", backup.TYPE_OPERATOR)
				expectedMetadata := testutils.DefaultMetadataMap("OPERATOR", false, true, true)[1]
				resultMetadata := resultMetadataMap[oid]
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for an operator family", func() {
				testutils.SkipIfBefore5(connection)
				testhelper.AssertQueryRuns(connection, "CREATE OPERATOR FAMILY public.testfam USING hash")
				defer testhelper.AssertQueryRuns(connection, "DROP OPERATOR FAMILY public.testfam USING hash")

				testhelper.AssertQueryRuns(connection, "COMMENT ON OPERATOR FAMILY public.testfam USING hash IS 'This is an operator family comment.'")

				resultMetadataMap := backup.GetMetadataForObjectType(connection, backup.TYPE_OPERATORFAMILY)

				oid := testutils.OidFromObjectName(connection, "public", "testfam", backup.TYPE_OPERATORFAMILY)
				expectedMetadata := testutils.DefaultMetadataMap("OPERATOR FAMILY", false, true, true)[1]
				resultMetadata := resultMetadataMap[oid]
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for an operator class", func() {
				testhelper.AssertQueryRuns(connection, "CREATE OPERATOR CLASS public.testclass FOR TYPE int USING hash AS STORAGE int")
				if connection.Version.Before("5") {
					defer testhelper.AssertQueryRuns(connection, "DROP OPERATOR CLASS public.testclass USING hash")
				} else {
					defer testhelper.AssertQueryRuns(connection, "DROP OPERATOR FAMILY public.testclass USING hash")
				}
				testhelper.AssertQueryRuns(connection, "COMMENT ON OPERATOR CLASS public.testclass USING hash IS 'This is an operator class comment.'")

				resultMetadataMap := backup.GetMetadataForObjectType(connection, backup.TYPE_OPERATORCLASS)

				oid := testutils.OidFromObjectName(connection, "public", "testclass", backup.TYPE_OPERATORCLASS)
				expectedMetadata := testutils.DefaultMetadataMap("OPERATOR CLASS", false, true, true)[1]
				resultMetadata := resultMetadataMap[oid]
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for a text search dictionary", func() {
				testutils.SkipIfBefore5(connection)
				testhelper.AssertQueryRuns(connection, "CREATE TEXT SEARCH DICTIONARY public.testdictionary(TEMPLATE = snowball, LANGUAGE = 'russian', STOPWORDS = 'russian');")
				defer testhelper.AssertQueryRuns(connection, "DROP TEXT SEARCH DICTIONARY public.testdictionary")
				testhelper.AssertQueryRuns(connection, "COMMENT ON TEXT SEARCH DICTIONARY public.testdictionary IS 'This is a text search dictionary comment.'")

				resultMetadataMap := backup.GetMetadataForObjectType(connection, backup.TYPE_TSDICTIONARY)

				Expect(len(resultMetadataMap)).To(Equal(1))
				oid := testutils.OidFromObjectName(connection, "public", "testdictionary", backup.TYPE_TSDICTIONARY)
				dictionaryMetadataMap := testutils.DefaultMetadataMap("TEXT SEARCH DICTIONARY", false, true, true)
				dictionaryMetadata := dictionaryMetadataMap[1]
				resultMetadata := resultMetadataMap[oid]
				structmatcher.ExpectStructsToMatch(&dictionaryMetadata, &resultMetadata)
			})
			It("returns a slice of default metadata for a text search configuration", func() {
				testutils.SkipIfBefore5(connection)
				resultMetadataMap := backup.GetMetadataForObjectType(connection, backup.TYPE_TSCONFIGURATION)
				configurationMetadataMap := testutils.DefaultMetadataMap("TEXT SEARCH CONFIGURATION", false, true, true)
				configurationMetadata := configurationMetadataMap[1]

				testhelper.AssertQueryRuns(connection, `CREATE TEXT SEARCH CONFIGURATION public.testconfiguration (PARSER = pg_catalog."default");`)
				defer testhelper.AssertQueryRuns(connection, "DROP TEXT SEARCH CONFIGURATION public.testconfiguration")
				testhelper.AssertQueryRuns(connection, "COMMENT ON TEXT SEARCH CONFIGURATION public.testconfiguration IS 'This is a text search configuration comment.'")

				oid := testutils.OidFromObjectName(connection, "public", "testconfiguration", backup.TYPE_TSCONFIGURATION)
				resultMetadataMap = backup.GetMetadataForObjectType(connection, backup.TYPE_TSCONFIGURATION)

				Expect(len(resultMetadataMap)).To(Equal(1))
				resultMetadata := resultMetadataMap[oid]
				structmatcher.ExpectStructsToMatch(&configurationMetadata, &resultMetadata)
			})
			It("returns a slice of default metadata for a foreign data wrapper", func() {
				testutils.SkipIfBefore6(connection)
				testhelper.AssertQueryRuns(connection, "CREATE FOREIGN DATA WRAPPER foreignwrapper")
				defer testhelper.AssertQueryRuns(connection, "DROP FOREIGN DATA WRAPPER foreignwrapper")

				testhelper.AssertQueryRuns(connection, "GRANT ALL ON FOREIGN DATA WRAPPER foreignwrapper TO testrole")

				resultMetadataMap := backup.GetMetadataForObjectType(connection, backup.TYPE_FOREIGNDATAWRAPPER)

				oid := testutils.OidFromObjectName(connection, "", "foreignwrapper", backup.TYPE_FOREIGNDATAWRAPPER)
				expectedMetadata := testutils.DefaultMetadataMap("FOREIGN DATA WRAPPER", true, true, false)[1]
				resultMetadata := resultMetadataMap[oid]
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for a foreign server", func() {
				testutils.SkipIfBefore6(connection)
				testhelper.AssertQueryRuns(connection, "CREATE FOREIGN DATA WRAPPER foreignwrapper")
				defer testhelper.AssertQueryRuns(connection, "DROP FOREIGN DATA WRAPPER foreignwrapper CASCADE")
				testhelper.AssertQueryRuns(connection, "CREATE SERVER foreignserver FOREIGN DATA WRAPPER foreignwrapper")

				testhelper.AssertQueryRuns(connection, "GRANT ALL ON FOREIGN SERVER foreignserver TO testrole")

				resultMetadataMap := backup.GetMetadataForObjectType(connection, backup.TYPE_FOREIGNSERVER)

				oid := testutils.OidFromObjectName(connection, "", "foreignserver", backup.TYPE_FOREIGNSERVER)
				expectedMetadata := testutils.DefaultMetadataMap("FOREIGN SERVER", true, true, false)[1]
				resultMetadata := resultMetadataMap[oid]
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for a collation", func() {
				testutils.SkipIfBefore6(connection)
				testhelper.AssertQueryRuns(connection, `CREATE COLLATION public.some_coll (lc_collate = 'POSIX', lc_ctype = 'POSIX');`)
				defer testhelper.AssertQueryRuns(connection, "DROP COLLATION public.some_coll")
				testhelper.AssertQueryRuns(connection, "COMMENT ON COLLATION public.some_coll IS 'This is a collation comment.'")

				resultMetadataMap := backup.GetMetadataForObjectType(connection, backup.TYPE_COLLATION)

				Expect(len(resultMetadataMap)).To(Equal(1))
				oid := testutils.OidFromObjectName(connection, "public", "some_coll", backup.TYPE_COLLATION)
				collationMetadataMap := testutils.DefaultMetadataMap("COLLATION", false, true, true)
				collationMetadata := collationMetadataMap[1]
				resultMetadata := resultMetadataMap[oid]
				structmatcher.ExpectStructsToMatch(&collationMetadata, &resultMetadata)
			})
		})
		Context("metadata for objects in a specific schema", func() {
			It("returns a slice of default metadata for a table in a specific schema", func() {
				testhelper.AssertQueryRuns(connection, "CREATE TABLE public.testtable(i int)")
				defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.testtable")
				testhelper.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
				defer testhelper.AssertQueryRuns(connection, "DROP SCHEMA testschema")
				testhelper.AssertQueryRuns(connection, "CREATE TABLE testschema.testtable(i int)")
				defer testhelper.AssertQueryRuns(connection, "DROP TABLE testschema.testtable")
				testhelper.AssertQueryRuns(connection, "GRANT ALL ON TABLE testschema.testtable TO testrole")
				testhelper.AssertQueryRuns(connection, "COMMENT ON TABLE testschema.testtable IS 'This is a table comment.'")
				cmdFlags.Set(utils.INCLUDE_SCHEMA, "testschema")

				resultMetadataMap := backup.GetMetadataForObjectType(connection, backup.TYPE_RELATION)

				oid := testutils.OidFromObjectName(connection, "testschema", "testtable", backup.TYPE_RELATION)
				expectedMetadata := testutils.DefaultMetadataMap("TABLE", true, true, true)[1]
				Expect(len(resultMetadataMap)).To(Equal(1))
				resultMetadata := resultMetadataMap[oid]
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for a table not in a specific schema", func() {
				testhelper.AssertQueryRuns(connection, "CREATE TABLE public.testtable(i int)")
				defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.testtable")
				testhelper.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
				defer testhelper.AssertQueryRuns(connection, "DROP SCHEMA testschema")
				testhelper.AssertQueryRuns(connection, "CREATE TABLE testschema.testtable(i int)")
				defer testhelper.AssertQueryRuns(connection, "DROP TABLE testschema.testtable")
				testhelper.AssertQueryRuns(connection, "GRANT ALL ON TABLE testschema.testtable TO testrole")
				testhelper.AssertQueryRuns(connection, "COMMENT ON TABLE testschema.testtable IS 'This is a table comment.'")
				cmdFlags.Set(utils.EXCLUDE_SCHEMA, "public")

				resultMetadataMap := backup.GetMetadataForObjectType(connection, backup.TYPE_RELATION)

				oid := testutils.OidFromObjectName(connection, "testschema", "testtable", backup.TYPE_RELATION)
				expectedMetadata := testutils.DefaultMetadataMap("TABLE", true, true, true)[1]
				Expect(len(resultMetadataMap)).To(Equal(1))
				resultMetadata := resultMetadataMap[oid]
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for a function in a specific schema", func() {
				testhelper.AssertQueryRuns(connection, `CREATE FUNCTION public.add(integer, integer) RETURNS integer
AS 'SELECT $1 + $2'
LANGUAGE SQL`)
				defer testhelper.AssertQueryRuns(connection, "DROP FUNCTION public.add(integer, integer)")
				testhelper.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
				defer testhelper.AssertQueryRuns(connection, "DROP SCHEMA testschema")
				testhelper.AssertQueryRuns(connection, `CREATE FUNCTION testschema.add(integer, integer) RETURNS integer
AS 'SELECT $1 + $2'
LANGUAGE SQL`)
				defer testhelper.AssertQueryRuns(connection, "DROP FUNCTION testschema.add(integer, integer)")
				testhelper.AssertQueryRuns(connection, "GRANT ALL ON FUNCTION testschema.add(integer, integer) TO testrole")
				testhelper.AssertQueryRuns(connection, "REVOKE ALL ON FUNCTION testschema.add(integer, integer) FROM PUBLIC")
				testhelper.AssertQueryRuns(connection, "COMMENT ON FUNCTION testschema.add(integer, integer) IS 'This is a function comment.'")

				cmdFlags.Set(utils.INCLUDE_SCHEMA, "testschema")
				resultMetadataMap := backup.GetMetadataForObjectType(connection, backup.TYPE_FUNCTION)

				oid := testutils.OidFromObjectName(connection, "testschema", "add", backup.TYPE_FUNCTION)
				expectedMetadata := testutils.DefaultMetadataMap("FUNCTION", true, true, true)[1]
				Expect(len(resultMetadataMap)).To(Equal(1))
				resultMetadata := resultMetadataMap[oid]
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for a view in a specific schema", func() {
				testhelper.AssertQueryRuns(connection, `CREATE VIEW public.testview AS SELECT * FROM pg_class`)
				defer testhelper.AssertQueryRuns(connection, "DROP VIEW public.testview")
				testhelper.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
				defer testhelper.AssertQueryRuns(connection, "DROP SCHEMA testschema")
				testhelper.AssertQueryRuns(connection, `CREATE VIEW testschema.testview AS SELECT * FROM pg_class`)
				defer testhelper.AssertQueryRuns(connection, "DROP VIEW testschema.testview")
				testhelper.AssertQueryRuns(connection, "GRANT ALL ON testschema.testview TO testrole")
				testhelper.AssertQueryRuns(connection, "COMMENT ON VIEW testschema.testview IS 'This is a view comment.'")

				cmdFlags.Set(utils.INCLUDE_SCHEMA, "testschema")
				resultMetadataMap := backup.GetMetadataForObjectType(connection, backup.TYPE_RELATION)

				oid := testutils.OidFromObjectName(connection, "testschema", "testview", backup.TYPE_RELATION)
				expectedMetadata := testutils.DefaultMetadataMap("VIEW", true, true, true)[1]
				Expect(len(resultMetadataMap)).To(Equal(1))
				resultMetadata := resultMetadataMap[oid]
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for an aggregate in a specific schema", func() {
				testhelper.AssertQueryRuns(connection, `
			CREATE FUNCTION public.mysfunc_accum(numeric, numeric, numeric)
			   RETURNS numeric
			   AS 'select $1 + $2 + $3'
			   LANGUAGE SQL
			   IMMUTABLE
			   RETURNS NULL ON NULL INPUT;
			`)
				defer testhelper.AssertQueryRuns(connection, "DROP FUNCTION public.mysfunc_accum(numeric, numeric, numeric)")
				testhelper.AssertQueryRuns(connection, `
			CREATE FUNCTION public.mypre_accum(numeric, numeric)
			   RETURNS numeric
			   AS 'select $1 + $2'
			   LANGUAGE SQL
			   IMMUTABLE
			   RETURNS NULL ON NULL INPUT;
			`)
				defer testhelper.AssertQueryRuns(connection, "DROP FUNCTION public.mypre_accum(numeric, numeric)")
				testhelper.AssertQueryRuns(connection, `CREATE AGGREGATE public.agg_prefunc(numeric, numeric) (
	SFUNC = public.mysfunc_accum,
	STYPE = numeric,
	PREFUNC = public.mypre_accum,
	INITCOND = '0'
);`)
				defer testhelper.AssertQueryRuns(connection, "DROP AGGREGATE public.agg_prefunc(numeric, numeric)")
				testhelper.AssertQueryRuns(connection, "COMMENT ON AGGREGATE public.agg_prefunc(numeric, numeric) IS 'This is an aggregate comment.'")
				testhelper.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
				defer testhelper.AssertQueryRuns(connection, "DROP SCHEMA testschema")
				testhelper.AssertQueryRuns(connection, `CREATE AGGREGATE testschema.agg_prefunc(numeric, numeric) (
	SFUNC = public.mysfunc_accum,
	STYPE = numeric,
	PREFUNC = public.mypre_accum,
	INITCOND = '0'
);`)
				defer testhelper.AssertQueryRuns(connection, "DROP AGGREGATE testschema.agg_prefunc(numeric, numeric)")
				testhelper.AssertQueryRuns(connection, "COMMENT ON AGGREGATE testschema.agg_prefunc(numeric, numeric) IS 'This is an aggregate comment.'")

				cmdFlags.Set(utils.INCLUDE_SCHEMA, "testschema")
				resultMetadataMap := backup.GetMetadataForObjectType(connection, backup.TYPE_AGGREGATE)

				oid := testutils.OidFromObjectName(connection, "testschema", "agg_prefunc", backup.TYPE_AGGREGATE)
				expectedMetadata := testutils.DefaultMetadataMap("AGGREGATE", false, true, true)[1]
				Expect(len(resultMetadataMap)).To(Equal(1))
				resultMetadata := resultMetadataMap[oid]
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for a type in a specific schema", func() {
				testhelper.AssertQueryRuns(connection, `CREATE TYPE public.testtype`)
				defer testhelper.AssertQueryRuns(connection, "DROP TYPE public.testtype")
				testhelper.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
				defer testhelper.AssertQueryRuns(connection, "DROP SCHEMA testschema")
				testhelper.AssertQueryRuns(connection, `CREATE TYPE testschema.testtype AS (name text)`)
				defer testhelper.AssertQueryRuns(connection, "DROP TYPE testschema.testtype")
				testhelper.AssertQueryRuns(connection, "COMMENT ON TYPE testschema.testtype IS 'This is a type comment.'")

				cmdFlags.Set(utils.INCLUDE_SCHEMA, "testschema")
				resultMetadataMap := backup.GetMetadataForObjectType(connection, backup.TYPE_TYPE)

				oid := testutils.OidFromObjectName(connection, "testschema", "testtype", backup.TYPE_TYPE)
				expectedMetadata := testutils.DefaultMetadataMap("TYPE", false, true, true)[1]
				resultMetadata := resultMetadataMap[oid]
				if connection.Version.Before("5") {
					// In 4.3, creating testtype does not generate a "_testtype" entry in pg_type
					Expect(len(resultMetadataMap)).To(Equal(1))
				} else {
					// In 5, creating testtype generates 2 entries in pg_type, "testtype" and "_testtype"
					Expect(len(resultMetadataMap)).To(Equal(2))
				}
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for an operator in a specific schema", func() {
				testhelper.AssertQueryRuns(connection, "CREATE OPERATOR public.#### (LEFTARG = bigint, PROCEDURE = numeric_fac)")
				defer testhelper.AssertQueryRuns(connection, "DROP OPERATOR public.#### (bigint, NONE)")
				testhelper.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
				defer testhelper.AssertQueryRuns(connection, "DROP SCHEMA testschema")
				testhelper.AssertQueryRuns(connection, "CREATE OPERATOR testschema.#### (LEFTARG = bigint, PROCEDURE = numeric_fac)")
				defer testhelper.AssertQueryRuns(connection, "DROP OPERATOR testschema.#### (bigint, NONE)")
				testhelper.AssertQueryRuns(connection, "COMMENT ON OPERATOR testschema.#### (bigint, NONE) IS 'This is an operator comment.'")

				cmdFlags.Set(utils.INCLUDE_SCHEMA, "testschema")
				resultMetadataMap := backup.GetMetadataForObjectType(connection, backup.TYPE_OPERATOR)

				Expect(len(resultMetadataMap)).To(Equal(1))
				oid := testutils.OidFromObjectName(connection, "testschema", "####", backup.TYPE_OPERATOR)
				expectedMetadata := testutils.DefaultMetadataMap("OPERATOR", false, true, true)[1]
				resultMetadata := resultMetadataMap[oid]
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for an operator family in a specific schema", func() {
				testutils.SkipIfBefore5(connection)
				testhelper.AssertQueryRuns(connection, "CREATE OPERATOR FAMILY public.testfam USING hash")
				defer testhelper.AssertQueryRuns(connection, "DROP OPERATOR FAMILY public.testfam USING hash")
				testhelper.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
				defer testhelper.AssertQueryRuns(connection, "DROP SCHEMA testschema")
				testhelper.AssertQueryRuns(connection, "CREATE OPERATOR FAMILY testschema.testfam USING hash")
				defer testhelper.AssertQueryRuns(connection, "DROP OPERATOR FAMILY testschema.testfam USING hash")
				testhelper.AssertQueryRuns(connection, "COMMENT ON OPERATOR FAMILY testschema.testfam USING hash IS 'This is an operator family comment.'")

				cmdFlags.Set(utils.INCLUDE_SCHEMA, "testschema")
				resultMetadataMap := backup.GetMetadataForObjectType(connection, backup.TYPE_OPERATORFAMILY)

				Expect(len(resultMetadataMap)).To(Equal(1))
				oid := testutils.OidFromObjectName(connection, "testschema", "testfam", backup.TYPE_OPERATORFAMILY)
				expectedMetadata := testutils.DefaultMetadataMap("OPERATOR FAMILY", false, true, true)[1]
				resultMetadata := resultMetadataMap[oid]
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for an operator class in a specific schema", func() {
				testhelper.AssertQueryRuns(connection, "CREATE OPERATOR CLASS public.testclass FOR TYPE int4 USING hash AS STORAGE int4")
				if connection.Version.Before("5") {
					defer testhelper.AssertQueryRuns(connection, "DROP OPERATOR CLASS public.testclass USING hash CASCADE")
				} else {
					defer testhelper.AssertQueryRuns(connection, "DROP OPERATOR FAMILY public.testclass USING hash CASCADE")
				}
				testhelper.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
				defer testhelper.AssertQueryRuns(connection, "DROP SCHEMA testschema")
				testhelper.AssertQueryRuns(connection, "CREATE OPERATOR CLASS testschema.testclass FOR TYPE int4 USING hash AS STORAGE int4")
				if connection.Version.Before("5") {
					defer testhelper.AssertQueryRuns(connection, "DROP OPERATOR CLASS testschema.testclass USING hash CASCADE")
				} else {
					defer testhelper.AssertQueryRuns(connection, "DROP OPERATOR FAMILY testschema.testclass USING hash CASCADE")
				}
				testhelper.AssertQueryRuns(connection, "COMMENT ON OPERATOR CLASS testschema.testclass USING hash IS 'This is an operator class comment.'")

				cmdFlags.Set(utils.INCLUDE_SCHEMA, "testschema")
				resultMetadataMap := backup.GetMetadataForObjectType(connection, backup.TYPE_OPERATORCLASS)

				Expect(len(resultMetadataMap)).To(Equal(1))
				oid := testutils.OidFromObjectName(connection, "testschema", "testclass", backup.TYPE_OPERATORCLASS)
				expectedMetadata := testutils.DefaultMetadataMap("OPERATOR CLASS", false, true, true)[1]
				resultMetadata := resultMetadataMap[oid]
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for a text search dictionary in a specific schema", func() {
				testutils.SkipIfBefore5(connection)
				testhelper.AssertQueryRuns(connection, "CREATE TEXT SEARCH DICTIONARY public.testdictionary(TEMPLATE = snowball, LANGUAGE = 'russian', STOPWORDS = 'russian');")
				defer testhelper.AssertQueryRuns(connection, "DROP TEXT SEARCH DICTIONARY public.testdictionary")
				testhelper.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
				defer testhelper.AssertQueryRuns(connection, "DROP SCHEMA testschema")
				testhelper.AssertQueryRuns(connection, "CREATE TEXT SEARCH DICTIONARY testschema.testdictionary(TEMPLATE = snowball, LANGUAGE = 'russian', STOPWORDS = 'russian');")
				defer testhelper.AssertQueryRuns(connection, "DROP TEXT SEARCH DICTIONARY testschema.testdictionary")
				testhelper.AssertQueryRuns(connection, "COMMENT ON TEXT SEARCH DICTIONARY testschema.testdictionary IS 'This is a text search dictionary comment.'")

				cmdFlags.Set(utils.INCLUDE_SCHEMA, "testschema")
				resultMetadataMap := backup.GetMetadataForObjectType(connection, backup.TYPE_TSDICTIONARY)

				Expect(len(resultMetadataMap)).To(Equal(1))
				oid := testutils.OidFromObjectName(connection, "testschema", "testdictionary", backup.TYPE_TSDICTIONARY)
				dictionaryMetadataMap := testutils.DefaultMetadataMap("TEXT SEARCH DICTIONARY", false, true, true)
				dictionaryMetadata := dictionaryMetadataMap[1]
				resultMetadata := resultMetadataMap[oid]
				structmatcher.ExpectStructsToMatch(&dictionaryMetadata, &resultMetadata)
			})
			It("returns a slice of default metadata for a text search configuration in a specific schema", func() {
				testutils.SkipIfBefore5(connection)
				resultMetadataMap := backup.GetMetadataForObjectType(connection, backup.TYPE_TSCONFIGURATION)
				configurationMetadataMap := testutils.DefaultMetadataMap("TEXT SEARCH CONFIGURATION", false, true, true)
				configurationMetadata := configurationMetadataMap[1]

				testhelper.AssertQueryRuns(connection, `CREATE TEXT SEARCH CONFIGURATION public.testconfiguration (PARSER = pg_catalog."default");`)
				defer testhelper.AssertQueryRuns(connection, "DROP TEXT SEARCH CONFIGURATION public.testconfiguration")
				testhelper.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
				defer testhelper.AssertQueryRuns(connection, "DROP SCHEMA testschema")
				testhelper.AssertQueryRuns(connection, `CREATE TEXT SEARCH CONFIGURATION testschema.testconfiguration (PARSER = pg_catalog."default");`)
				defer testhelper.AssertQueryRuns(connection, "DROP TEXT SEARCH CONFIGURATION testschema.testconfiguration")
				testhelper.AssertQueryRuns(connection, "COMMENT ON TEXT SEARCH CONFIGURATION testschema.testconfiguration IS 'This is a text search configuration comment.'")

				cmdFlags.Set(utils.INCLUDE_SCHEMA, "testschema")
				resultMetadataMap = backup.GetMetadataForObjectType(connection, backup.TYPE_TSCONFIGURATION)

				Expect(len(resultMetadataMap)).To(Equal(1))
				oid := testutils.OidFromObjectName(connection, "testschema", "testconfiguration", backup.TYPE_TSCONFIGURATION)
				resultMetadata := resultMetadataMap[oid]
				structmatcher.ExpectStructsToMatch(&configurationMetadata, &resultMetadata)
			})
			It("returns a slice of default metadata for a collation in a specific schema", func() {
				testutils.SkipIfBefore6(connection)
				testhelper.AssertQueryRuns(connection, `CREATE COLLATION public.some_coll (lc_collate = 'POSIX', lc_ctype = 'POSIX');`)
				defer testhelper.AssertQueryRuns(connection, "DROP COLLATION public.some_coll")
				testhelper.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
				defer testhelper.AssertQueryRuns(connection, "DROP SCHEMA testschema")
				testhelper.AssertQueryRuns(connection, `CREATE COLLATION testschema.some_coll (lc_collate = 'POSIX', lc_ctype = 'POSIX');`)
				defer testhelper.AssertQueryRuns(connection, "DROP COLLATION testschema.some_coll")
				testhelper.AssertQueryRuns(connection, "COMMENT ON COLLATION testschema.some_coll IS 'This is a collation comment.'")

				cmdFlags.Set(utils.INCLUDE_SCHEMA, "testschema")
				resultMetadataMap := backup.GetMetadataForObjectType(connection, backup.TYPE_COLLATION)

				Expect(len(resultMetadataMap)).To(Equal(1))
				oid := testutils.OidFromObjectName(connection, "testschema", "some_coll", backup.TYPE_COLLATION)
				collationMetadataMap := testutils.DefaultMetadataMap("COLLATION", false, true, true)
				collationMetadata := collationMetadataMap[1]
				resultMetadata := resultMetadataMap[oid]
				structmatcher.ExpectStructsToMatch(&collationMetadata, &resultMetadata)
			})
		})
	})
	Describe("GetCommentsForObjectType", func() {
		Context("comments for all objects of one type", func() {
			It("returns a slice of default metadata for an index", func() {
				resultMetadataMap := backup.GetCommentsForObjectType(connection, backup.TYPE_INDEX)
				numIndexes := len(resultMetadataMap)

				testhelper.AssertQueryRuns(connection, `CREATE TABLE public.testtable(i int)`)
				testhelper.AssertQueryRuns(connection, `CREATE INDEX testindex ON public.testtable USING btree(i)`)
				defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.testtable")
				testhelper.AssertQueryRuns(connection, "COMMENT ON INDEX public.testindex IS 'This is an index comment.'")

				resultMetadataMap = backup.GetCommentsForObjectType(connection, backup.TYPE_INDEX)

				oid := testutils.OidFromObjectName(connection, "", "testindex", backup.TYPE_INDEX)
				expectedMetadataMap := testutils.DefaultMetadataMap("INDEX", false, false, true)
				expectedMetadata := expectedMetadataMap[1]

				Expect(len(resultMetadataMap)).To(Equal(numIndexes + 1))
				resultMetadata := resultMetadataMap[oid]
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for a rule", func() {
				resultMetadataMap := backup.GetCommentsForObjectType(connection, backup.TYPE_RULE)
				numRules := len(resultMetadataMap)

				testhelper.AssertQueryRuns(connection, `CREATE TABLE public.testtable(i int)`)
				testhelper.AssertQueryRuns(connection, `CREATE RULE update_notify AS ON UPDATE TO public.testtable DO NOTIFY testtable`)
				defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.testtable")
				testhelper.AssertQueryRuns(connection, "COMMENT ON RULE update_notify IS 'This is a rule comment.'")

				resultMetadataMap = backup.GetCommentsForObjectType(connection, backup.TYPE_RULE)

				oid := testutils.OidFromObjectName(connection, "", "update_notify", backup.TYPE_RULE)
				expectedMetadataMap := testutils.DefaultMetadataMap("RULE", false, false, true)
				expectedMetadata := expectedMetadataMap[1]

				Expect(len(resultMetadataMap)).To(Equal(numRules + 1))
				resultMetadata := resultMetadataMap[oid]
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for a trigger", func() {
				resultMetadataMap := backup.GetCommentsForObjectType(connection, backup.TYPE_TRIGGER)
				numTriggers := len(resultMetadataMap)

				testhelper.AssertQueryRuns(connection, `CREATE TABLE public.testtable(i int)`)
				testhelper.AssertQueryRuns(connection, `CREATE TRIGGER sync_testtable AFTER INSERT OR DELETE OR UPDATE ON public.testtable FOR EACH STATEMENT EXECUTE PROCEDURE "RI_FKey_check_ins"()`)
				defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.testtable")
				testhelper.AssertQueryRuns(connection, "COMMENT ON TRIGGER sync_testtable ON public.testtable IS 'This is a trigger comment.'")

				resultMetadataMap = backup.GetCommentsForObjectType(connection, backup.TYPE_TRIGGER)

				oid := testutils.OidFromObjectName(connection, "", "sync_testtable", backup.TYPE_TRIGGER)
				expectedMetadataMap := testutils.DefaultMetadataMap("TRIGGER", false, false, true)
				expectedMetadata := expectedMetadataMap[1]

				Expect(len(resultMetadataMap)).To(Equal(numTriggers + 1))
				resultMetadata := resultMetadataMap[oid]
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for a cast in 4.3", func() {
				testutils.SkipIfNot4(connection)
				resultMetadataMap := backup.GetCommentsForObjectType(connection, backup.TYPE_CAST)
				numCasts := len(resultMetadataMap)

				testhelper.AssertQueryRuns(connection, "CREATE FUNCTION public.casttotext(bool) RETURNS text STRICT IMMUTABLE LANGUAGE PLPGSQL AS $$ BEGIN IF $1 IS TRUE THEN RETURN 'true'; ELSE RETURN 'false'; END IF; END; $$;")
				defer testhelper.AssertQueryRuns(connection, "DROP FUNCTION public.casttotext(bool) CASCADE")
				testhelper.AssertQueryRuns(connection, "CREATE CAST (bool AS text) WITH FUNCTION public.casttotext(bool) AS ASSIGNMENT")
				testhelper.AssertQueryRuns(connection, "COMMENT ON CAST (bool AS text) IS 'This is a cast comment.'")

				resultMetadataMap = backup.GetCommentsForObjectType(connection, backup.TYPE_CAST)

				boolOid := testutils.OidFromObjectName(connection, "", "bool", backup.TYPE_TYPE)
				textOid := testutils.OidFromObjectName(connection, "", "text", backup.TYPE_TYPE)
				oid := testutils.OidFromCast(connection, boolOid, textOid)
				expectedMetadataMap := testutils.DefaultMetadataMap("CAST", false, false, true)
				expectedMetadata := expectedMetadataMap[1]

				Expect(len(resultMetadataMap)).To(Equal(numCasts + 1))
				resultMetadata := resultMetadataMap[oid]
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for a cast in 5", func() {
				testutils.SkipIfBefore5(connection)
				resultMetadataMap := backup.GetCommentsForObjectType(connection, backup.TYPE_CAST)
				numCasts := len(resultMetadataMap)

				testhelper.AssertQueryRuns(connection, `CREATE FUNCTION public.casttoint(text) RETURNS integer STRICT IMMUTABLE LANGUAGE SQL AS 'SELECT cast($1 as integer);'`)
				defer testhelper.AssertQueryRuns(connection, "DROP FUNCTION public.casttoint(text) CASCADE")
				testhelper.AssertQueryRuns(connection, "CREATE CAST (text AS int) WITH FUNCTION public.casttoint(text) AS ASSIGNMENT;")
				testhelper.AssertQueryRuns(connection, "COMMENT ON CAST (text AS int) IS 'This is a cast comment.'")

				resultMetadataMap = backup.GetCommentsForObjectType(connection, backup.TYPE_CAST)

				textOid := testutils.OidFromObjectName(connection, "", "text", backup.TYPE_TYPE)
				intOid := testutils.OidFromObjectName(connection, "", "int4", backup.TYPE_TYPE)
				oid := testutils.OidFromCast(connection, textOid, intOid)
				expectedMetadataMap := testutils.DefaultMetadataMap("CAST", false, false, true)
				expectedMetadata := expectedMetadataMap[1]

				Expect(len(resultMetadataMap)).To(Equal(numCasts + 1))
				resultMetadata := resultMetadataMap[oid]
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for a resource queue", func() {
				resultMetadataMap := backup.GetCommentsForObjectType(connection, backup.TYPE_RESOURCEQUEUE)
				numResQueues := len(resultMetadataMap)

				testhelper.AssertQueryRuns(connection, `CREATE RESOURCE QUEUE res_queue WITH (MAX_COST=32.8);`)
				defer testhelper.AssertQueryRuns(connection, "DROP RESOURCE QUEUE res_queue")
				testhelper.AssertQueryRuns(connection, "COMMENT ON RESOURCE QUEUE res_queue IS 'This is a resource queue comment.'")

				resultMetadataMap = backup.GetCommentsForObjectType(connection, backup.TYPE_RESOURCEQUEUE)

				oid := testutils.OidFromObjectName(connection, "", "res_queue", backup.TYPE_RESOURCEQUEUE)
				expectedMetadataMap := testutils.DefaultMetadataMap("RESOURCE QUEUE", false, false, true)
				expectedMetadata := expectedMetadataMap[1]

				Expect(len(resultMetadataMap)).To(Equal(numResQueues + 1))
				resultMetadata := resultMetadataMap[oid]
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for a role", func() {
				resultMetadataMap := backup.GetCommentsForObjectType(connection, backup.TYPE_ROLE)
				numRoles := len(resultMetadataMap)

				testhelper.AssertQueryRuns(connection, `CREATE ROLE testuser`)
				defer testhelper.AssertQueryRuns(connection, "DROP ROLE testuser")
				testhelper.AssertQueryRuns(connection, "COMMENT ON ROLE testuser IS 'This is a role comment.'")

				resultMetadataMap = backup.GetCommentsForObjectType(connection, backup.TYPE_ROLE)

				oid := testutils.OidFromObjectName(connection, "", "testuser", backup.TYPE_ROLE)
				expectedMetadataMap := testutils.DefaultMetadataMap("ROLE", false, false, true)
				expectedMetadata := expectedMetadataMap[1]

				Expect(len(resultMetadataMap)).To(Equal(numRoles + 1))
				resultMetadata := resultMetadataMap[oid]
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for a text search parser", func() {
				testutils.SkipIfBefore5(connection)
				parserMetadataMap := testutils.DefaultMetadataMap("TEXT SEARCH PARSER", false, false, true)
				parserMetadata := parserMetadataMap[1]

				testhelper.AssertQueryRuns(connection, "CREATE TEXT SEARCH PARSER public.testparser(START = prsd_start, GETTOKEN = prsd_nexttoken, END = prsd_end, LEXTYPES = prsd_lextype);")
				defer testhelper.AssertQueryRuns(connection, "DROP TEXT SEARCH PARSER public.testparser")
				testhelper.AssertQueryRuns(connection, "COMMENT ON TEXT SEARCH PARSER public.testparser IS 'This is a text search parser comment.'")

				oid := testutils.OidFromObjectName(connection, "public", "testparser", backup.TYPE_TSPARSER)
				resultMetadataMap := backup.GetCommentsForObjectType(connection, backup.TYPE_TSPARSER)

				Expect(len(resultMetadataMap)).To(Equal(1))
				resultMetadata := resultMetadataMap[oid]
				structmatcher.ExpectStructsToMatch(&parserMetadata, &resultMetadata)
			})
			It("returns a slice of default metadata for a text search template", func() {
				testutils.SkipIfBefore5(connection)
				templateMetadataMap := testutils.DefaultMetadataMap("TEXT SEARCH TEMPLATE", false, false, true)
				templateMetadata := templateMetadataMap[1]

				testhelper.AssertQueryRuns(connection, "CREATE TEXT SEARCH TEMPLATE public.testtemplate(LEXIZE = dsimple_lexize);")
				defer testhelper.AssertQueryRuns(connection, "DROP TEXT SEARCH TEMPLATE public.testtemplate")
				testhelper.AssertQueryRuns(connection, "COMMENT ON TEXT SEARCH TEMPLATE public.testtemplate IS 'This is a text search template comment.'")

				oid := testutils.OidFromObjectName(connection, "public", "testtemplate", backup.TYPE_TSTEMPLATE)
				resultMetadataMap := backup.GetCommentsForObjectType(connection, backup.TYPE_TSTEMPLATE)

				Expect(len(resultMetadataMap)).To(Equal(1))
				resultMetadata := resultMetadataMap[oid]
				structmatcher.ExpectStructsToMatch(&templateMetadata, &resultMetadata)
			})
			It("returns a slice of default metadata for an extension", func() {
				testutils.SkipIfBefore5(connection)
				extensionMetadataMap := testutils.DefaultMetadataMap("EXTENSION", false, false, true)
				extensionMetadata := extensionMetadataMap[1]

				testhelper.AssertQueryRuns(connection, "CREATE EXTENSION plperl;")
				defer testhelper.AssertQueryRuns(connection, "DROP EXTENSION plperl")
				testhelper.AssertQueryRuns(connection, "COMMENT ON EXTENSION plperl IS 'This is an extension comment.'")

				oid := testutils.OidFromObjectName(connection, "", "plperl", backup.TYPE_EXTENSION)
				resultMetadataMap := backup.GetCommentsForObjectType(connection, backup.TYPE_EXTENSION)

				Expect(len(resultMetadataMap)).To(Equal(1))
				resultMetadata := resultMetadataMap[oid]
				structmatcher.ExpectStructsToMatch(&extensionMetadata, &resultMetadata)
			})
		})
		Context("comments for objects in a specific schema", func() {
			It("returns a slice of default metadata for an index in a specific schema", func() {

				testhelper.AssertQueryRuns(connection, `CREATE TABLE public.testtable(i int)`)
				testhelper.AssertQueryRuns(connection, `CREATE INDEX testindex ON public.testtable USING btree(i)`)
				defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.testtable")
				testhelper.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
				defer testhelper.AssertQueryRuns(connection, "DROP SCHEMA testschema")
				testhelper.AssertQueryRuns(connection, `CREATE TABLE testschema.testtable(i int)`)
				testhelper.AssertQueryRuns(connection, `CREATE INDEX testindex1 ON testschema.testtable USING btree(i)`)
				defer testhelper.AssertQueryRuns(connection, "DROP TABLE testschema.testtable")
				testhelper.AssertQueryRuns(connection, "COMMENT ON INDEX testschema.testindex1 IS 'This is an index comment.'")

				cmdFlags.Set(utils.INCLUDE_SCHEMA, "testschema")
				resultMetadataMap := backup.GetCommentsForObjectType(connection, backup.TYPE_INDEX)

				oid := testutils.OidFromObjectName(connection, "", "testindex1", backup.TYPE_INDEX)
				expectedMetadataMap := testutils.DefaultMetadataMap("INDEX", false, false, true)
				expectedMetadata := expectedMetadataMap[1]

				resultMetadata := resultMetadataMap[oid]
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for a constraint in a specific schema", func() {
				testhelper.AssertQueryRuns(connection, `CREATE TABLE public.testtable(i int UNIQUE)`)
				defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.testtable")
				testhelper.AssertQueryRuns(connection, "COMMENT ON CONSTRAINT testtable_i_key ON public.testtable IS 'This is a constraint comment.'")

				testhelper.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
				defer testhelper.AssertQueryRuns(connection, "DROP SCHEMA testschema")
				testhelper.AssertQueryRuns(connection, `CREATE TABLE testschema.testtable(i int UNIQUE)`)
				defer testhelper.AssertQueryRuns(connection, "DROP TABLE testschema.testtable")
				testhelper.AssertQueryRuns(connection, "COMMENT ON CONSTRAINT testtable_i_key ON testschema.testtable IS 'This is a constraint comment.'")
				cmdFlags.Set(utils.INCLUDE_SCHEMA, "testschema")

				resultMetadataMap := backup.GetCommentsForObjectType(connection, backup.TYPE_CONSTRAINT)

				oid := testutils.OidFromObjectName(connection, "testschema", "testtable_i_key", backup.TYPE_CONSTRAINT)
				expectedMetadataMap := testutils.DefaultMetadataMap("CONSTRAINT", false, false, true)
				expectedMetadata := expectedMetadataMap[1]

				Expect(len(resultMetadataMap)).To(Equal(1))
				resultMetadata := resultMetadataMap[oid]
				structmatcher.ExpectStructsToMatchExcluding(&expectedMetadata, &resultMetadata, "Oid")
			})
			It("returns a slice of default metadata for a text search parser in a specific schema", func() {
				testutils.SkipIfBefore5(connection)
				parserMetadataMap := testutils.DefaultMetadataMap("TEXT SEARCH PARSER", false, false, true)
				parserMetadata := parserMetadataMap[1]

				testhelper.AssertQueryRuns(connection, "CREATE TEXT SEARCH PARSER public.testparser(START = prsd_start, GETTOKEN = prsd_nexttoken, END = prsd_end, LEXTYPES = prsd_lextype);")
				defer testhelper.AssertQueryRuns(connection, "DROP TEXT SEARCH PARSER public.testparser")
				testhelper.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
				defer testhelper.AssertQueryRuns(connection, "DROP SCHEMA testschema")
				testhelper.AssertQueryRuns(connection, "CREATE TEXT SEARCH PARSER testschema.testparser(START = prsd_start, GETTOKEN = prsd_nexttoken, END = prsd_end, LEXTYPES = prsd_lextype);")
				defer testhelper.AssertQueryRuns(connection, "DROP TEXT SEARCH PARSER testschema.testparser")
				testhelper.AssertQueryRuns(connection, "COMMENT ON TEXT SEARCH PARSER testschema.testparser IS 'This is a text search parser comment.'")

				oid := testutils.OidFromObjectName(connection, "testschema", "testparser", backup.TYPE_TSPARSER)
				cmdFlags.Set(utils.INCLUDE_SCHEMA, "testschema")
				resultMetadataMap := backup.GetCommentsForObjectType(connection, backup.TYPE_TSPARSER)

				Expect(len(resultMetadataMap)).To(Equal(1))
				resultMetadata := resultMetadataMap[oid]
				structmatcher.ExpectStructsToMatch(&parserMetadata, &resultMetadata)
			})
			It("returns a slice of default metadata for a text search template in a specific schema", func() {
				testutils.SkipIfBefore5(connection)
				templateMetadataMap := testutils.DefaultMetadataMap("TEXT SEARCH TEMPLATE", false, false, true)
				templateMetadata := templateMetadataMap[1]

				testhelper.AssertQueryRuns(connection, "CREATE TEXT SEARCH TEMPLATE public.testtemplate(LEXIZE = dsimple_lexize);")
				defer testhelper.AssertQueryRuns(connection, "DROP TEXT SEARCH TEMPLATE public.testtemplate")
				testhelper.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
				defer testhelper.AssertQueryRuns(connection, "DROP SCHEMA testschema")
				testhelper.AssertQueryRuns(connection, "CREATE TEXT SEARCH TEMPLATE testschema.testtemplate(LEXIZE = dsimple_lexize);")
				defer testhelper.AssertQueryRuns(connection, "DROP TEXT SEARCH TEMPLATE testschema.testtemplate")
				testhelper.AssertQueryRuns(connection, "COMMENT ON TEXT SEARCH TEMPLATE testschema.testtemplate IS 'This is a text search template comment.'")

				oid := testutils.OidFromObjectName(connection, "testschema", "testtemplate", backup.TYPE_TSTEMPLATE)
				cmdFlags.Set(utils.INCLUDE_SCHEMA, "testschema")
				resultMetadataMap := backup.GetCommentsForObjectType(connection, backup.TYPE_TSTEMPLATE)

				Expect(len(resultMetadataMap)).To(Equal(1))
				resultMetadata := resultMetadataMap[oid]
				structmatcher.ExpectStructsToMatch(&templateMetadata, &resultMetadata)
			})
		})
	})
})
