package integration

import (
	"database/sql"

	"github.com/greenplum-db/gp-common-go-libs/structmatcher"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/toc"

	. "github.com/onsi/ginkgo/v2"
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
			schemaMetadata := testutils.DefaultMetadataMap(toc.OBJ_SCHEMA, true, true, true, includeSecurityLabels)

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
			schemaMetadata := testutils.DefaultMetadataMap(toc.OBJ_SCHEMA, true, true, true, includeSecurityLabels)

			backup.PrintCreateSchemaStatements(backupfile, tocfile, schemas, schemaMetadata)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "ALTER SCHEMA public OWNER TO anothertestrole")
			defer testhelper.AssertQueryRuns(connectionPool, "COMMENT ON SCHEMA public IS 'standard public schema'")

			resultSchemas := backup.GetAllUserSchemas(connectionPool, partitionAlteredSchemas)

			Expect(resultSchemas).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&schemas[0], &resultSchemas[0])
		})
	})
	Describe("PrintConstraintStatement", func() {
		var (
			uniqueConstraint          backup.Constraint
			pkConstraint              backup.Constraint
			fkConstraint              backup.Constraint
			checkConstraint           backup.Constraint
			partitionCheckConstraint  backup.Constraint
			partitionChildConstraint  backup.Constraint
			partitionIntmdConstraint  backup.Constraint
			partitionParentConstraint backup.Constraint
			objectMetadata            backup.MetadataMap
		)
		BeforeEach(func() {
			uniqueConstraint = backup.Constraint{Oid: 0, Schema: "public", Name: "uniq2", ConType: "u", Def: sql.NullString{String: "UNIQUE (a, b)", Valid: true}, OwningObject: "public.testtable", IsDomainConstraint: false, IsPartitionParent: false}
			pkConstraint = backup.Constraint{Oid: 0, Schema: "public", Name: "constraints_other_table_pkey", ConType: "p", Def: sql.NullString{String: "PRIMARY KEY (b)", Valid: true}, OwningObject: "public.constraints_other_table", IsDomainConstraint: false, IsPartitionParent: false}
			fkConstraint = backup.Constraint{Oid: 0, Schema: "public", Name: "fk1", ConType: "f", Def: sql.NullString{String: "FOREIGN KEY (b) REFERENCES public.constraints_other_table(b)", Valid: true}, OwningObject: "public.testtable", IsDomainConstraint: false, IsPartitionParent: false}
			checkConstraint = backup.Constraint{Oid: 0, Schema: "public", Name: "check1", ConType: "c", Def: sql.NullString{String: "CHECK (a <> 42)", Valid: true}, OwningObject: "public.testtable", IsDomainConstraint: false, IsPartitionParent: false}
			partitionCheckConstraint = backup.Constraint{Oid: 0, Schema: "public", Name: "check1", ConType: "c", Def: sql.NullString{String: "CHECK (id <> 0)", Valid: true}, OwningObject: "public.part", IsDomainConstraint: false, IsPartitionParent: true}
			partitionIntmdConstraint = backup.Constraint{Oid: 0, Schema: "public", Name: "id_unique", ConType: "u", Def: sql.NullString{String: "UNIQUE (id)", Valid: true}, OwningObject: "public.part_one", IsDomainConstraint: false, IsPartitionParent: true}
			partitionChildConstraint = backup.Constraint{Oid: 0, Schema: "public", Name: "id_unique", ConType: "u", Def: sql.NullString{String: "UNIQUE (id)", Valid: true}, OwningObject: "public.part_one", IsDomainConstraint: false, IsPartitionParent: false}
			partitionParentConstraint = backup.Constraint{Oid: 0, Schema: "public", Name: "check_year", ConType: "c", Def: sql.NullString{String: "CHECK (year < 3000)", Valid: true}, OwningObject: "public.part", IsDomainConstraint: false, IsPartitionParent: true}
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.testtable(a int, b text) DISTRIBUTED BY (b)")
			objectMetadata = testutils.DefaultMetadataMap(toc.OBJ_CONSTRAINT, false, false, false, false)

			if connectionPool.Version.AtLeast("6") {
				uniqueConstraint.ConIsLocal = true
				pkConstraint.ConIsLocal = true
				fkConstraint.ConIsLocal = true
				checkConstraint.ConIsLocal = true
				partitionCheckConstraint.ConIsLocal = true
				partitionIntmdConstraint.ConIsLocal = true
				partitionChildConstraint.ConIsLocal = true
				partitionParentConstraint.ConIsLocal = true
			}
		})
		AfterEach(func() {
			testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.testtable CASCADE")
		})
		It("creates a unique constraint", func() {
			constraints := []backup.Constraint{uniqueConstraint}
			backup.PrintConstraintStatements(backupfile, tocfile, constraints, objectMetadata)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			resultConstraints := backup.GetConstraints(connectionPool)

			Expect(resultConstraints).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&uniqueConstraint, &resultConstraints[0], "Oid")
		})
		It("creates a primary key constraint", func() {
			constraints := []backup.Constraint{pkConstraint}
			backup.PrintConstraintStatements(backupfile, tocfile, constraints, objectMetadata)

			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.constraints_other_table(b text)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.constraints_other_table CASCADE")
			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			resultConstraints := backup.GetConstraints(connectionPool)

			Expect(resultConstraints).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&pkConstraint, &resultConstraints[0], "Oid")
		})
		It("creates a foreign key constraint", func() {
			constraints := []backup.Constraint{fkConstraint}
			backup.PrintConstraintStatements(backupfile, tocfile, constraints, objectMetadata)

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
			backup.PrintConstraintStatements(backupfile, tocfile, constraints, objectMetadata)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			resultConstraints := backup.GetConstraints(connectionPool)

			Expect(resultConstraints).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&checkConstraint, &resultConstraints[0], "Oid")
		})
		It("creates multiple constraints on one table", func() {
			constraints := []backup.Constraint{checkConstraint, uniqueConstraint, fkConstraint}
			backup.PrintConstraintStatements(backupfile, tocfile, constraints, objectMetadata)

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
		It("manages order of primary and foreign key constraints", func() {
			constraints := []backup.Constraint{fkConstraint, uniqueConstraint, pkConstraint}
			backup.PrintConstraintStatements(backupfile, tocfile, constraints, objectMetadata)

			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.constraints_other_table(b text)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.constraints_other_table CASCADE")
			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			resultConstraints := backup.GetConstraints(connectionPool)

			Expect(resultConstraints).To(HaveLen(3))
			structmatcher.ExpectStructsToMatchExcluding(&pkConstraint, &resultConstraints[0], "Oid")
			structmatcher.ExpectStructsToMatchExcluding(&fkConstraint, &resultConstraints[1], "Oid")
			structmatcher.ExpectStructsToMatchExcluding(&uniqueConstraint, &resultConstraints[2], "Oid")
		})
		It("creates a check constraint on a parent partition table", func() {
			constraints := []backup.Constraint{partitionCheckConstraint}
			backup.PrintConstraintStatements(backupfile, tocfile, constraints, objectMetadata)

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
		It("creates a unique constraint on an intermediate partition table", func() {
			// TODO -- this seems like it should work on 6.  See about flexing the syntax below to 6-supported
			testutils.SkipIfBefore7(connectionPool)
			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE public.part (id int, year int)
DISTRIBUTED BY (id)
PARTITION BY RANGE (year);`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.part CASCADE")

			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE public.part_one PARTITION OF public.part
FOR VALUES FROM (2007) TO (2010) PARTITION BY RANGE (id);`)
			testhelper.AssertQueryRuns(connectionPool, `ALTER TABLE public.part_one ADD CONSTRAINT id_unique UNIQUE (id);`)

			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE public.part_two PARTITION OF public.part_one
FOR VALUES FROM (0) TO (100);`)

			resultConstraints := backup.GetConstraints(connectionPool)
			Expect(resultConstraints).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&partitionIntmdConstraint, &resultConstraints[0], "Oid")
		})
		It("creates a unique constraint on a child partition table", func() {
			// TODO -- this seems like it should work on 6.  See about flexing the syntax below to 6-supported
			testutils.SkipIfBefore7(connectionPool)
			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE public.part (id int, year int)
DISTRIBUTED BY (id)
PARTITION BY RANGE (year);`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.part CASCADE")

			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE public.part_one PARTITION OF public.part
FOR VALUES FROM (2007) TO (2010);`)
			testhelper.AssertQueryRuns(connectionPool, `ALTER TABLE public.part_one ADD CONSTRAINT id_unique UNIQUE (id);`)

			resultConstraints := backup.GetConstraints(connectionPool)
			Expect(resultConstraints).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&partitionChildConstraint, &resultConstraints[0], "Oid")
		})
		It("creates two unique constraints, one each  on parent and child partition tables", func() {
			// TODO -- this seems like it should work on 6.  See about flexing the syntax below to 6-supported
			testutils.SkipIfBefore7(connectionPool)
			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE public.part (id int, year int)
DISTRIBUTED BY (id)
PARTITION BY RANGE (year);`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.part CASCADE")
			testhelper.AssertQueryRuns(connectionPool, `ALTER TABLE public.part ADD CONSTRAINT check_year CHECK (year < 3000);`)

			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE public.part_one PARTITION OF public.part
FOR VALUES FROM (2007) TO (2010);`)
			testhelper.AssertQueryRuns(connectionPool, `ALTER TABLE public.part_one ADD CONSTRAINT id_unique UNIQUE (id);`)

			resultConstraints := backup.GetConstraints(connectionPool)
			Expect(resultConstraints).To(HaveLen(2))
			if resultConstraints[0].Name == "check_year" {
				structmatcher.ExpectStructsToMatchExcluding(&partitionParentConstraint, &resultConstraints[0], "Oid")
				structmatcher.ExpectStructsToMatchExcluding(&partitionChildConstraint, &resultConstraints[1], "Oid")
			} else {
				structmatcher.ExpectStructsToMatchExcluding(&partitionParentConstraint, &resultConstraints[1], "Oid")
				structmatcher.ExpectStructsToMatchExcluding(&partitionChildConstraint, &resultConstraints[0], "Oid")
			}

		})
	})
	Describe("PrintAccessMethodStatement", func() {
		It("creates user defined access method for tables and indexes", func() {
			testutils.SkipIfBefore7(connectionPool)

			accessMethodTable := backup.AccessMethod{Oid: 1, Name: "test_tableam_table", Handler: "heap_tableam_handler", Type: "t"}
			accessMethodIndex := backup.AccessMethod{Oid: 2, Name: "test_tableam_index", Handler: "gisthandler", Type: "i"}
			accessMethods := []backup.AccessMethod{accessMethodTable, accessMethodIndex}
			backup.PrintAccessMethodStatements(backupfile, tocfile, accessMethods, backup.MetadataMap{})

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP ACCESS METHOD test_tableam_table;")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP ACCESS METHOD test_tableam_index;")

			resultAccessMethods := backup.GetAccessMethods(connectionPool)
			Expect(resultAccessMethods).To(HaveLen(2))
			structmatcher.ExpectStructsToMatchExcluding(&resultAccessMethods[0], &accessMethodTable, "Oid")
			structmatcher.ExpectStructsToMatchExcluding(&resultAccessMethods[1], &accessMethodIndex, "Oid")
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
