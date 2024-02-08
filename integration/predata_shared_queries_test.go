package integration

import (
	"database/sql"

	"github.com/greenplum-db/gp-common-go-libs/structmatcher"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/options"
	"github.com/greenplum-db/gpbackup/testutils"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup integration tests", func() {
	Describe("GetAllUserSchemas", func() {
		var partitionAlteredSchemas map[string]bool
		BeforeEach(func() {
			partitionAlteredSchemas = make(map[string]bool)
		})
		It("returns user schema information", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE SCHEMA bar")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA bar")
			schemas := backup.GetAllUserSchemas(connectionPool, partitionAlteredSchemas)

			schemaBar := backup.Schema{Oid: 0, Name: "bar"}
			schemaPublic := backup.Schema{Oid: 2200, Name: "public"}

			Expect(schemas).To(HaveLen(2))
			structmatcher.ExpectStructsToMatchExcluding(&schemaBar, &schemas[0], "Oid")
			structmatcher.ExpectStructsToMatchExcluding(&schemaPublic, &schemas[1], "Owner")
		})

		It("returns schema information for single specific schema", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE SCHEMA bar")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA bar")
			_ = backupCmdFlags.Set(options.INCLUDE_SCHEMA, "bar")

			schemas := backup.GetAllUserSchemas(connectionPool, partitionAlteredSchemas)

			schemaBar := backup.Schema{Oid: 0, Name: "bar"}

			Expect(schemas).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&schemaBar, &schemas[0], "Oid")

		})

		It("returns schema information for multiple specific schemas", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE SCHEMA bar")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA bar")
			_ = backupCmdFlags.Set(options.INCLUDE_SCHEMA, "bar")
			_ = backupCmdFlags.Set(options.INCLUDE_SCHEMA, "public")
			schemas := backup.GetAllUserSchemas(connectionPool, partitionAlteredSchemas)

			schemaBar := backup.Schema{Oid: 0, Name: "bar"}
			schemaPublic := backup.Schema{Oid: 2200, Name: "public"}

			Expect(schemas).To(HaveLen(2))
			structmatcher.ExpectStructsToMatchExcluding(&schemaBar, &schemas[0], "Oid")
			structmatcher.ExpectStructsToMatchExcluding(&schemaPublic, &schemas[1], "Owner")

		})

		It("returns schema information for filtered schemas with altered partition schema exceptions", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE SCHEMA bar")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA bar")
			_ = backupCmdFlags.Set(options.INCLUDE_SCHEMA, "public")
			partitionAlteredSchemas["bar"] = true
			schemas := backup.GetAllUserSchemas(connectionPool, partitionAlteredSchemas)

			schemaBar := backup.Schema{Oid: 0, Name: "bar"}
			schemaPublic := backup.Schema{Oid: 2200, Name: "public"}

			Expect(schemas).To(HaveLen(2))
			structmatcher.ExpectStructsToMatchExcluding(&schemaBar, &schemas[0], "Oid")
			structmatcher.ExpectStructsToMatchExcluding(&schemaPublic, &schemas[1], "Owner")

		})
	})
	Describe("GetConstraints", func() {
		var (
			uniqueConstraint         backup.Constraint
			fkConstraint             backup.Constraint
			pkConstraint             backup.Constraint
			checkConstraint          backup.Constraint
			partitionCheckConstraint backup.Constraint
			domainConstraint         backup.Constraint
			constraintInSchema       backup.Constraint
		)

		BeforeEach(func() {
			uniqueConstraint = backup.Constraint{Oid: 0, Schema: "public", Name: "uniq2", ConType: "u", Def: sql.NullString{String: "UNIQUE (a, b)", Valid: true}, OwningObject: "public.constraints_table", IsDomainConstraint: false, IsPartitionParent: false}
			fkConstraint = backup.Constraint{Oid: 0, Schema: "public", Name: "fk1", ConType: "f", Def: sql.NullString{String: "FOREIGN KEY (b) REFERENCES public.constraints_table(b)", Valid: true}, OwningObject: "public.constraints_other_table", IsDomainConstraint: false, IsPartitionParent: false}
			pkConstraint = backup.Constraint{Oid: 0, Schema: "public", Name: "pk1", ConType: "p", Def: sql.NullString{String: "PRIMARY KEY (b)", Valid: true}, OwningObject: "public.constraints_table", IsDomainConstraint: false, IsPartitionParent: false}
			checkConstraint = backup.Constraint{Oid: 0, Schema: "public", Name: "check1", ConType: "c", Def: sql.NullString{String: "CHECK (a <> 42)", Valid: true}, OwningObject: "public.constraints_table", IsDomainConstraint: false, IsPartitionParent: false}
			partitionCheckConstraint = backup.Constraint{Oid: 0, Schema: "public", Name: "check1", ConType: "c", Def: sql.NullString{String: "CHECK (id <> 0)", Valid: true}, OwningObject: "public.part", IsDomainConstraint: false, IsPartitionParent: true}
			domainConstraint = backup.Constraint{Oid: 0, Schema: "public", Name: "check1", ConType: "c", Def: sql.NullString{String: "CHECK (VALUE <> 42)", Valid: true}, OwningObject: "public.constraint_domain", IsDomainConstraint: true, IsPartitionParent: false}
			constraintInSchema = backup.Constraint{Oid: 0, Schema: "testschema", Name: "uniq2", ConType: "u", Def: sql.NullString{String: "UNIQUE (a, b)", Valid: true}, OwningObject: "testschema.constraints_table", IsDomainConstraint: false, IsPartitionParent: false}

			if connectionPool.Version.AtLeast("6") {
				uniqueConstraint.ConIsLocal = true
				fkConstraint.ConIsLocal = true
				pkConstraint.ConIsLocal = true
				checkConstraint.ConIsLocal = true
				partitionCheckConstraint.ConIsLocal = true
				domainConstraint.ConIsLocal = true
				constraintInSchema.ConIsLocal = true
			}
		})
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
				// In GPDB 6+, we no longer allow the distributed key to implicitly change when creating a unique index.
				if connectionPool.Version.Before("6") {
					testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.constraints_table(a int, b text, c float)")
				} else {
					testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.constraints_table(a int, b text, c float) DISTRIBUTED BY (b)")
				}
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.constraints_table")
				testhelper.AssertQueryRuns(connectionPool, "ALTER TABLE ONLY public.constraints_table ADD CONSTRAINT pk1 PRIMARY KEY (b)")
				testhelper.AssertQueryRuns(connectionPool, "COMMENT ON CONSTRAINT pk1 ON public.constraints_table IS 'this is a constraint comment'")

				constraints := backup.GetConstraints(connectionPool)

				Expect(constraints).To(HaveLen(1))
				structmatcher.ExpectStructsToMatchExcluding(&constraints[0], &pkConstraint, "Oid")
			})
			It("returns a constraint array for a table with one FOREIGN KEY constraint", func() {
				// In GPDB 6+, we no longer allow the distributed key to implicitly change when creating a unique index.
				if connectionPool.Version.Before("6") {
					testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.constraints_table(a int, b text, c float)")
				} else {
					testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.constraints_table(a int, b text, c float) DISTRIBUTED BY (b)")
				}
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
				_ = backupCmdFlags.Set(options.INCLUDE_SCHEMA, "testschema")

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

				_ = backupCmdFlags.Set(options.EXCLUDE_RELATION, "public.other_table")
				defer backupCmdFlags.Set(options.EXCLUDE_RELATION, "")
				constraints := backup.GetConstraints(connectionPool)
				Expect(constraints).To(HaveLen(1))
				structmatcher.ExpectStructsToMatchExcluding(&constraints[0], &uniqueConstraint, "Oid")

				_ = backupCmdFlags.Set(options.EXCLUDE_RELATION, "public.constraints_table")
				backup.SetFilterRelationClause("")
				opts, err := options.NewOptions(backupCmdFlags)
				Expect(err).NotTo(HaveOccurred())
				backup.ValidateAndProcessFilterLists(opts)
				constraints = backup.GetConstraints(connectionPool)
				Expect(constraints).To(BeEmpty())
			})
		})
		It("returns a constraint array for a table with multiple constraints", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.constraints_table(a int, b text, c float) DISTRIBUTED BY (b)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.constraints_table CASCADE")
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.constraints_other_table(a int, b text)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.constraints_other_table CASCADE")
			testhelper.AssertQueryRuns(connectionPool, "ALTER TABLE ONLY public.constraints_table ADD CONSTRAINT uniq2 UNIQUE (a, b)")
			testhelper.AssertQueryRuns(connectionPool, "COMMENT ON CONSTRAINT uniq2 ON public.constraints_table IS 'this is a constraint comment'")
			testhelper.AssertQueryRuns(connectionPool, "ALTER TABLE ONLY public.constraints_table ADD CONSTRAINT pk1 PRIMARY KEY (a,b)")
			testhelper.AssertQueryRuns(connectionPool, "COMMENT ON CONSTRAINT pk1 ON public.constraints_table IS 'this is a constraint comment'")
			testhelper.AssertQueryRuns(connectionPool, "ALTER TABLE ONLY public.constraints_other_table ADD CONSTRAINT fk1 FOREIGN KEY (a,b) REFERENCES public.constraints_table(a,b)")
			testhelper.AssertQueryRuns(connectionPool, "ALTER TABLE ONLY public.constraints_table ADD CONSTRAINT check1 CHECK (a <> 42)")

			constraints := backup.GetConstraints(connectionPool)

			Expect(constraints).To(HaveLen(4))
			structmatcher.ExpectStructsToMatchExcluding(&constraints[0], &checkConstraint, "Oid")

			fkConstraint = backup.Constraint{Oid: 0, Schema: "public", Name: "fk1", ConType: "f", Def: sql.NullString{String: "FOREIGN KEY (a, b) REFERENCES public.constraints_table(a, b)", Valid: true}, OwningObject: "public.constraints_other_table", IsDomainConstraint: false, IsPartitionParent: false}
			if connectionPool.Version.AtLeast("6") {
				fkConstraint.ConIsLocal = true
			}
			structmatcher.ExpectStructsToMatchExcluding(&constraints[1], &fkConstraint, "Oid")

			pkConstraint = backup.Constraint{Oid: 0, Schema: "public", Name: "pk1", ConType: "p", Def: sql.NullString{String: "PRIMARY KEY (a, b)", Valid: true}, OwningObject: "public.constraints_table", IsDomainConstraint: false, IsPartitionParent: false}
			if connectionPool.Version.AtLeast("6") {
				pkConstraint.ConIsLocal = true
			}
			structmatcher.ExpectStructsToMatchExcluding(&constraints[2], &pkConstraint, "Oid")
			structmatcher.ExpectStructsToMatchExcluding(&constraints[3], &uniqueConstraint, "Oid")
		})
		It("returns a constraint array for a table with PRIMARY KEY constraint that includes non-key columns", func() {
			testutils.SkipIfBefore7(connectionPool)
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.table_with_constr (a int NOT NULL, b int NOT NULL, c int, d int) DISTRIBUTED BY (a);")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.table_with_constr")
			testhelper.AssertQueryRuns(connectionPool, "ALTER TABLE ONLY public.table_with_constr ADD CONSTRAINT table_with_constr_pkey PRIMARY KEY (a, b) INCLUDE (c, d);")

			constraints := backup.GetConstraints(connectionPool)
			expectedConstraint := backup.Constraint{Oid: 0, Schema: "public", Name: "table_with_constr_pkey", ConType: "p", Def: sql.NullString{String: "PRIMARY KEY (a, b) INCLUDE (c, d)", Valid: true}, ConIsLocal: true, OwningObject: "public.table_with_constr", IsDomainConstraint: false, IsPartitionParent: false}

			Expect(constraints).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&constraints[0], &expectedConstraint, "Oid")
		})
	})
	Describe("GetAccessMethods", func() {
		It("returns information for user defined access methods", func() {
			testutils.SkipIfBefore7(connectionPool)

			testhelper.AssertQueryRuns(connectionPool, "CREATE ACCESS METHOD test_tableam_table TYPE TABLE HANDLER heap_tableam_handler;")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP ACCESS METHOD test_tableam_table;")

			testhelper.AssertQueryRuns(connectionPool, "CREATE ACCESS METHOD test_tableam_index TYPE INDEX HANDLER gisthandler;")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP ACCESS METHOD test_tableam_index;")

			accessMethods := backup.GetAccessMethods(connectionPool)

			accessMethodTable := backup.AccessMethod{Oid: 1, Name: "test_tableam_table", Handler: "heap_tableam_handler", Type: "t"}
			accessMethodIndex := backup.AccessMethod{Oid: 2, Name: "test_tableam_index", Handler: "gisthandler", Type: "i"}

			Expect(accessMethods).To(HaveLen(2))
			structmatcher.ExpectStructsToMatchExcluding(&accessMethodTable, &accessMethods[0], "Oid")
			structmatcher.ExpectStructsToMatchExcluding(&accessMethodIndex, &accessMethods[1], "Oid")

		})
	})
})
