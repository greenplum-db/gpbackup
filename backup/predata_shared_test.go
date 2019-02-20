package backup_test

import (
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"

	. "github.com/onsi/ginkgo"
)

var _ = Describe("backup/predata_shared tests", func() {
	BeforeEach(func() {
		toc, backupfile = testutils.InitializeTestTOC(buffer, "predata")
	})
	Describe("PrintConstraintStatements", func() {
		var (
			uniqueOne        backup.Constraint
			uniqueTwo        backup.Constraint
			primarySingle    backup.Constraint
			primaryComposite backup.Constraint
			foreignOne       backup.Constraint
			foreignTwo       backup.Constraint
			emptyMetadataMap backup.MetadataMap
		)
		BeforeEach(func() {
			uniqueOne = backup.Constraint{Oid: 1, Name: "tablename_i_key", ConType: "u", ConDef: "UNIQUE (i)", OwningObject: "public.tablename", IsDomainConstraint: false, IsPartitionParent: false}
			uniqueTwo = backup.Constraint{Oid: 0, Name: "tablename_j_key", ConType: "u", ConDef: "UNIQUE (j)", OwningObject: "public.tablename", IsDomainConstraint: false, IsPartitionParent: false}
			primarySingle = backup.Constraint{Oid: 0, Name: "tablename_pkey", ConType: "p", ConDef: "PRIMARY KEY (i)", OwningObject: "public.tablename", IsDomainConstraint: false, IsPartitionParent: false}
			primaryComposite = backup.Constraint{Oid: 0, Name: "tablename_pkey", ConType: "p", ConDef: "PRIMARY KEY (i, j)", OwningObject: "public.tablename", IsDomainConstraint: false, IsPartitionParent: false}
			foreignOne = backup.Constraint{Oid: 0, Name: "tablename_i_fkey", ConType: "f", ConDef: "FOREIGN KEY (i) REFERENCES other_tablename(a)", OwningObject: "public.tablename", IsDomainConstraint: false, IsPartitionParent: false}
			foreignTwo = backup.Constraint{Oid: 0, Name: "tablename_j_fkey", ConType: "f", ConDef: "FOREIGN KEY (j) REFERENCES other_tablename(b)", OwningObject: "public.tablename", IsDomainConstraint: false, IsPartitionParent: false}
			emptyMetadataMap = backup.MetadataMap{}
		})

		Context("No constraints", func() {
			It("doesn't print anything", func() {
				constraints := []backup.Constraint{}
				backup.PrintConstraintStatements(backupfile, toc, constraints, emptyMetadataMap)
				testhelper.NotExpectRegexp(buffer, `CONSTRAINT`)
			})
		})
		Context("Constraints involving different columns", func() {
			It("prints an ADD CONSTRAINT statement for one UNIQUE constraint with a comment", func() {
				constraints := []backup.Constraint{uniqueOne}
				constraintMetadataMap := testutils.DefaultMetadataMap("CONSTRAINT", false, false, true, false)
				backup.PrintConstraintStatements(backupfile, toc, constraints, constraintMetadataMap)
				testutils.ExpectEntry(toc.PostdataEntries, 0, "", "public.tablename", "tablename_i_key", "CONSTRAINT")
				testutils.AssertBufferContents(toc.PostdataEntries, buffer, "ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_key UNIQUE (i);", "COMMENT ON CONSTRAINT tablename_i_key ON public.tablename IS 'This is a constraint comment.';")
			})
			It("prints an ADD CONSTRAINT statement for one UNIQUE constraint", func() {
				constraints := []backup.Constraint{uniqueOne}
				backup.PrintConstraintStatements(backupfile, toc, constraints, emptyMetadataMap)
				testutils.AssertBufferContents(toc.PostdataEntries, buffer, `ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_key UNIQUE (i);`)
			})
			It("prints ADD CONSTRAINT statements for two UNIQUE constraints", func() {
				constraints := []backup.Constraint{uniqueOne, uniqueTwo}
				backup.PrintConstraintStatements(backupfile, toc, constraints, emptyMetadataMap)
				testutils.AssertBufferContents(toc.PostdataEntries, buffer,
					`ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_key UNIQUE (i);`,
					`ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_j_key UNIQUE (j);`)
			})
			It("prints an ADD CONSTRAINT statement for one PRIMARY KEY constraint on one column", func() {
				constraints := []backup.Constraint{primarySingle}
				backup.PrintConstraintStatements(backupfile, toc, constraints, emptyMetadataMap)
				testutils.AssertBufferContents(toc.PostdataEntries, buffer, `ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_pkey PRIMARY KEY (i);`)
			})
			It("prints an ADD CONSTRAINT statement for one composite PRIMARY KEY constraint on two columns", func() {
				constraints := []backup.Constraint{primaryComposite}
				backup.PrintConstraintStatements(backupfile, toc, constraints, emptyMetadataMap)
				testutils.AssertBufferContents(toc.PostdataEntries, buffer, `ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_pkey PRIMARY KEY (i, j);`)
			})
			It("prints an ADD CONSTRAINT statement for one FOREIGN KEY constraint", func() {
				constraints := []backup.Constraint{foreignOne}
				backup.PrintConstraintStatements(backupfile, toc, constraints, emptyMetadataMap)
				testutils.AssertBufferContents(toc.PostdataEntries, buffer, `ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_fkey FOREIGN KEY (i) REFERENCES other_tablename(a);`)
			})
			It("prints ADD CONSTRAINT statements for two FOREIGN KEY constraints", func() {
				constraints := []backup.Constraint{foreignOne, foreignTwo}
				backup.PrintConstraintStatements(backupfile, toc, constraints, emptyMetadataMap)
				testutils.AssertBufferContents(toc.PostdataEntries, buffer,
					`ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_fkey FOREIGN KEY (i) REFERENCES other_tablename(a);`,
					`ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_j_fkey FOREIGN KEY (j) REFERENCES other_tablename(b);`)
			})
			It("prints ADD CONSTRAINT statements for one UNIQUE constraint and one FOREIGN KEY constraint", func() {
				constraints := []backup.Constraint{foreignTwo, uniqueOne}
				backup.PrintConstraintStatements(backupfile, toc, constraints, emptyMetadataMap)
				testutils.AssertBufferContents(toc.PostdataEntries, buffer,
					`ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_key UNIQUE (i);`,
					`ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_j_fkey FOREIGN KEY (j) REFERENCES other_tablename(b);`)
			})
			It("prints ADD CONSTRAINT statements for one PRIMARY KEY constraint and one FOREIGN KEY constraint", func() {
				constraints := []backup.Constraint{foreignTwo, primarySingle}
				backup.PrintConstraintStatements(backupfile, toc, constraints, emptyMetadataMap)
				testutils.AssertBufferContents(toc.PostdataEntries, buffer,
					`ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_pkey PRIMARY KEY (i);`,
					`ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_j_fkey FOREIGN KEY (j) REFERENCES other_tablename(b);`)
			})
			It("prints ADD CONSTRAINT statements for one two-column composite PRIMARY KEY constraint and one FOREIGN KEY constraint", func() {
				constraints := []backup.Constraint{foreignTwo, primaryComposite}
				backup.PrintConstraintStatements(backupfile, toc, constraints, emptyMetadataMap)
				testutils.AssertBufferContents(toc.PostdataEntries, buffer,
					`ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_pkey PRIMARY KEY (i, j);`,
					`ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_j_fkey FOREIGN KEY (j) REFERENCES other_tablename(b);`)
			})
		})
		Context("Constraints involving the same column", func() {
			It("prints ADD CONSTRAINT statements for one UNIQUE constraint and one FOREIGN KEY constraint", func() {
				constraints := []backup.Constraint{foreignOne, uniqueOne}
				backup.PrintConstraintStatements(backupfile, toc, constraints, emptyMetadataMap)
				testutils.AssertBufferContents(toc.PostdataEntries, buffer,
					`ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_key UNIQUE (i);`,
					`ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_fkey FOREIGN KEY (i) REFERENCES other_tablename(a);`)
			})
			It("prints ADD CONSTRAINT statements for one PRIMARY KEY constraint and one FOREIGN KEY constraint", func() {
				constraints := []backup.Constraint{foreignOne, primarySingle}
				backup.PrintConstraintStatements(backupfile, toc, constraints, emptyMetadataMap)
				testutils.AssertBufferContents(toc.PostdataEntries, buffer,
					`ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_pkey PRIMARY KEY (i);`,
					`ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_fkey FOREIGN KEY (i) REFERENCES other_tablename(a);`)
			})
			It("prints ADD CONSTRAINT statements for a two-column composite PRIMARY KEY constraint and one FOREIGN KEY constraint", func() {
				constraints := []backup.Constraint{foreignOne, primaryComposite}
				backup.PrintConstraintStatements(backupfile, toc, constraints, emptyMetadataMap)
				testutils.AssertBufferContents(toc.PostdataEntries, buffer,
					`ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_pkey PRIMARY KEY (i, j);`,
					`ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_fkey FOREIGN KEY (i) REFERENCES other_tablename(a);`)
			})
			It("doesn't print an ADD CONSTRAINT statement for domain check constraint", func() {
				domainCheckConstraint := backup.Constraint{Oid: 0, Name: "check1", ConType: "c", ConDef: "CHECK (VALUE <> 42::numeric)", OwningObject: "public.domain1", IsDomainConstraint: true, IsPartitionParent: false}
				constraints := []backup.Constraint{domainCheckConstraint}
				backup.PrintConstraintStatements(backupfile, toc, constraints, emptyMetadataMap)
				testhelper.NotExpectRegexp(buffer, `ALTER DOMAIN`)
			})
			It("prints an ADD CONSTRAINT statement for a parent partition table", func() {
				uniqueOne.IsPartitionParent = true
				constraints := []backup.Constraint{uniqueOne}
				backup.PrintConstraintStatements(backupfile, toc, constraints, emptyMetadataMap)
				testutils.AssertBufferContents(toc.PostdataEntries, buffer, `ALTER TABLE public.tablename ADD CONSTRAINT tablename_i_key UNIQUE (i);`)
			})
			It("prints an ADD CONSTRAINT [name] CHECK statement without keyword ONLY for a table with descendants (another table inherits it)", func() {
				checkConstraint := backup.Constraint{Oid: 0, Name: "check1", ConType: "c", ConDef: "CHECK (VALUE <> 42::numeric)", OwningObject: "public.tablename", IsDomainConstraint: false, IsPartitionParent: false, ConIsLocal: true}
				constraints := []backup.Constraint{checkConstraint}
				backup.PrintConstraintStatements(backupfile, toc, constraints, emptyMetadataMap)
				testutils.AssertBufferContents(toc.PostdataEntries, buffer, `ALTER TABLE public.tablename ADD CONSTRAINT check1 CHECK (VALUE <> 42::numeric);`)
			})
		})
	})
	Describe("PrintCreateSchemaStatements", func() {
		It("can print a basic schema", func() {
			schemas := []backup.Schema{{Oid: 0, Name: "schemaname"}}
			emptyMetadataMap := backup.MetadataMap{}

			backup.PrintCreateSchemaStatements(backupfile, toc, schemas, emptyMetadataMap)
			testutils.ExpectEntry(toc.PredataEntries, 0, "schemaname", "", "schemaname", "SCHEMA")
			testutils.AssertBufferContents(toc.PredataEntries, buffer, "CREATE SCHEMA schemaname;")
		})
		It("can print a schema with privileges, an owner, security label, and a comment", func() {
			schemas := []backup.Schema{{Oid: 1, Name: "schemaname"}}
			schemaMetadataMap := testutils.DefaultMetadataMap("SCHEMA", true, true, true, true)

			backup.PrintCreateSchemaStatements(backupfile, toc, schemas, schemaMetadataMap)
			expectedStatements := []string{"CREATE SCHEMA schemaname;",
				"COMMENT ON SCHEMA schemaname IS 'This is a schema comment.';",
				"ALTER SCHEMA schemaname OWNER TO testrole;",
				`REVOKE ALL ON SCHEMA schemaname FROM PUBLIC;
REVOKE ALL ON SCHEMA schemaname FROM testrole;
GRANT ALL ON SCHEMA schemaname TO testrole;`,
				"SECURITY LABEL FOR dummy ON SCHEMA schemaname IS 'unclassified';"}
			testutils.AssertBufferContents(toc.PredataEntries, buffer, expectedStatements...)
		})
	})
})
