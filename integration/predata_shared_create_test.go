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
		tocfile, backupfile = testutils.InitializeTestTOC(buffer, "predata")
	})
	Describe("PrintCreateSchemaStatements", func() {
		var partitionAlteredSchemas map[string]bool
		BeforeEach(func() {
			partitionAlteredSchemas = make(map[string]bool)
		})
		It("creates a non public schema", func() {
			schemas := []backup.Schema{{Oid: 0, Name: "test_schema"}}
			schemaMetadata := testutils.DefaultMetadataMap("SCHEMA", true, true, true, includeSecurityLabels)

			backup.PrintCreateSchemaStatements(backupfile, tocfile, schemas, schemaMetadata)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA test_schema")

			resultSchemas := backup.GetAllUserSchemas(connectionPool, partitionAlteredSchemas)

			Expect(resultSchemas).To(HaveLen(2))
			Expect(resultSchemas[0].Name).To(Equal("public"))

			structmatcher.ExpectStructsToMatchExcluding(&schemas[0], &resultSchemas[1], "Oid")
		})

		It("modifies the public schema", func() {
			schemas := []backup.Schema{{Oid: 2200, Name: "public"}}
			schemaMetadata := testutils.DefaultMetadataMap("SCHEMA", true, true, true, includeSecurityLabels)

			backup.PrintCreateSchemaStatements(backupfile, tocfile, schemas, schemaMetadata)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "ALTER SCHEMA public OWNER TO anothertestrole")
			defer testhelper.AssertQueryRuns(connectionPool, "COMMENT ON SCHEMA public IS 'standard public schema'")

			resultSchemas := backup.GetAllUserSchemas(connectionPool, partitionAlteredSchemas)

			Expect(resultSchemas).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&schemas[0], &resultSchemas[0])
		})
	})
	Describe("PrintConstraintStatements", func() {
		var (
			uniqueConstraint         backup.Constraint
			pkConstraint             backup.Constraint
			fkConstraint             backup.Constraint
			checkConstraint          backup.Constraint
			partitionCheckConstraint backup.Constraint
			conMetadataMap           backup.MetadataMap
		)
		BeforeEach(func() {
			uniqueConstraint = backup.Constraint{Oid: 0, Schema: "public", Name: "uniq2", ConType: "u", ConDef: "UNIQUE (a, b)", OwningObject: "public.testtable", IsDomainConstraint: false, IsPartitionParent: false}
			pkConstraint = backup.Constraint{Oid: 0, Schema: "public", Name: "constraints_other_table_pkey", ConType: "p", ConDef: "PRIMARY KEY (b)", OwningObject: "public.constraints_other_table", IsDomainConstraint: false, IsPartitionParent: false}
			fkConstraint = backup.Constraint{Oid: 0, Schema: "public", Name: "fk1", ConType: "f", ConDef: "FOREIGN KEY (b) REFERENCES public.constraints_other_table(b)", OwningObject: "public.testtable", IsDomainConstraint: false, IsPartitionParent: false}
			checkConstraint = backup.Constraint{Oid: 0, Schema: "public", Name: "check1", ConType: "c", ConDef: "CHECK (a <> 42)", OwningObject: "public.testtable", IsDomainConstraint: false, IsPartitionParent: false}
			partitionCheckConstraint = backup.Constraint{Oid: 0, Schema: "public", Name: "check1", ConType: "c", ConDef: "CHECK (id <> 0)", OwningObject: "public.part", IsDomainConstraint: false, IsPartitionParent: true}
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.testtable(a int, b text) DISTRIBUTED BY (b)")
			conMetadataMap = backup.MetadataMap{}

			if connectionPool.Version.AtLeast("6") {
				uniqueConstraint.ConIsLocal = true
				pkConstraint.ConIsLocal = true
				fkConstraint.ConIsLocal = true
				checkConstraint.ConIsLocal = true
				partitionCheckConstraint.ConIsLocal = true
			}
		})
		AfterEach(func() {
			testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.testtable CASCADE")
		})
		It("creates a unique constraint", func() {
			constraints := []backup.Constraint{uniqueConstraint}
			backup.PrintConstraintStatements(backupfile, tocfile, constraints, conMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			resultConstraints := backup.GetConstraints(connectionPool)

			Expect(resultConstraints).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&uniqueConstraint, &resultConstraints[0], "Oid")
		})
		It("creates a primary key constraint", func() {
			constraints := []backup.Constraint{pkConstraint}
			backup.PrintConstraintStatements(backupfile, tocfile, constraints, conMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.constraints_other_table(b text)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.constraints_other_table CASCADE")
			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			resultConstraints := backup.GetConstraints(connectionPool)

			Expect(resultConstraints).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&pkConstraint, &resultConstraints[0], "Oid")
		})
		It("creates a foreign key constraint", func() {
			constraints := []backup.Constraint{fkConstraint}
			backup.PrintConstraintStatements(backupfile, tocfile, constraints, conMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.constraints_other_table(b text PRIMARY KEY)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.constraints_other_table CASCADE")
			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			resultConstraints := backup.GetConstraints(connectionPool)

			Expect(resultConstraints).To(HaveLen(2))
			structmatcher.ExpectStructsToMatchExcluding(&pkConstraint, &resultConstraints[0], "Oid")
			structmatcher.ExpectStructsToMatchExcluding(&fkConstraint, &resultConstraints[1], "Oid")
		})
		It("creates a check constraint", func() {
			constraints := []backup.Constraint{checkConstraint}
			backup.PrintConstraintStatements(backupfile, tocfile, constraints, conMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			resultConstraints := backup.GetConstraints(connectionPool)

			Expect(resultConstraints).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&checkConstraint, &resultConstraints[0], "Oid")
		})
		It("creates multiple constraints on one table", func() {
			constraints := []backup.Constraint{checkConstraint, uniqueConstraint, fkConstraint}
			backup.PrintConstraintStatements(backupfile, tocfile, constraints, conMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.constraints_other_table(b text PRIMARY KEY)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.constraints_other_table CASCADE")
			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			resultConstraints := backup.GetConstraints(connectionPool)

			Expect(resultConstraints).To(HaveLen(4))
			structmatcher.ExpectStructsToMatchExcluding(&checkConstraint, &resultConstraints[0], "Oid")
			structmatcher.ExpectStructsToMatchExcluding(&pkConstraint, &resultConstraints[1], "Oid")
			structmatcher.ExpectStructsToMatchExcluding(&fkConstraint, &resultConstraints[2], "Oid")
			structmatcher.ExpectStructsToMatchExcluding(&uniqueConstraint, &resultConstraints[3], "Oid")
		})
		It("doesn't create a check constraint on a domain", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE DOMAIN public.domain1 AS numeric")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP DOMAIN public.domain1")
			domainCheckConstraint := backup.Constraint{Oid: 0, Name: "check1", ConType: "c", ConDef: "CHECK (VALUE <> 42::numeric)", OwningObject: "public.domain1", IsDomainConstraint: true, IsPartitionParent: false}
			constraints := []backup.Constraint{domainCheckConstraint}
			backup.PrintConstraintStatements(backupfile, tocfile, constraints, conMetadataMap)

			Expect(buffer.String()).To(Equal(""))
		})
		It("creates a check constraint on a parent partition table", func() {
			constraints := []backup.Constraint{partitionCheckConstraint}
			backup.PrintConstraintStatements(backupfile, tocfile, constraints, conMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE public.part (id int, year int)
DISTRIBUTED BY (id)
PARTITION BY RANGE (year)
( START (2007) END (2008) EVERY (1),
  DEFAULT PARTITION extra ); `)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.part CASCADE")
			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			resultConstraints := backup.GetConstraints(connectionPool)

			Expect(resultConstraints).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&partitionCheckConstraint, &resultConstraints[0], "Oid")
		})
	})
	Describe("GUC-printing functions", func() {
		gucs := backup.SessionGUCs{ClientEncoding: "UTF8"}
		Describe("PrintSessionGUCs", func() {
			It("prints the default session GUCs", func() {
				backup.PrintSessionGUCs(backupfile, tocfile, gucs)

				//We just want to check that these queries run successfully, no setup required
				testhelper.AssertQueryRuns(connectionPool, buffer.String())
			})

		})
	})
})
