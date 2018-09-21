package backup_test

import (
	"database/sql"

	"github.com/greenplum-db/gp-common-go-libs/structmatcher"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
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
				constraintMetadataMap := testutils.DefaultMetadataMap("CONSTRAINT", false, false, true)
				backup.PrintConstraintStatements(backupfile, toc, constraints, constraintMetadataMap)
				testutils.ExpectEntry(toc.PredataEntries, 0, "", "public.tablename", "tablename_i_key", "CONSTRAINT")
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_key UNIQUE (i);


COMMENT ON CONSTRAINT tablename_i_key ON public.tablename IS 'This is a constraint comment.';`)
			})
			It("prints an ADD CONSTRAINT statement for one UNIQUE constraint", func() {
				constraints := []backup.Constraint{uniqueOne}
				backup.PrintConstraintStatements(backupfile, toc, constraints, emptyMetadataMap)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_key UNIQUE (i);`)
			})
			It("prints ADD CONSTRAINT statements for two UNIQUE constraints", func() {
				constraints := []backup.Constraint{uniqueOne, uniqueTwo}
				backup.PrintConstraintStatements(backupfile, toc, constraints, emptyMetadataMap)
				testutils.AssertBufferContents(toc.PredataEntries, buffer,
					`ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_key UNIQUE (i);`,
					`ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_j_key UNIQUE (j);`)
			})
			It("prints an ADD CONSTRAINT statement for one PRIMARY KEY constraint on one column", func() {
				constraints := []backup.Constraint{primarySingle}
				backup.PrintConstraintStatements(backupfile, toc, constraints, emptyMetadataMap)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_pkey PRIMARY KEY (i);`)
			})
			It("prints an ADD CONSTRAINT statement for one composite PRIMARY KEY constraint on two columns", func() {
				constraints := []backup.Constraint{primaryComposite}
				backup.PrintConstraintStatements(backupfile, toc, constraints, emptyMetadataMap)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_pkey PRIMARY KEY (i, j);`)
			})
			It("prints an ADD CONSTRAINT statement for one FOREIGN KEY constraint", func() {
				constraints := []backup.Constraint{foreignOne}
				backup.PrintConstraintStatements(backupfile, toc, constraints, emptyMetadataMap)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_fkey FOREIGN KEY (i) REFERENCES other_tablename(a);`)
			})
			It("prints ADD CONSTRAINT statements for two FOREIGN KEY constraints", func() {
				constraints := []backup.Constraint{foreignOne, foreignTwo}
				backup.PrintConstraintStatements(backupfile, toc, constraints, emptyMetadataMap)
				testutils.AssertBufferContents(toc.PredataEntries, buffer,
					`ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_fkey FOREIGN KEY (i) REFERENCES other_tablename(a);`,
					`ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_j_fkey FOREIGN KEY (j) REFERENCES other_tablename(b);`)
			})
			It("prints ADD CONSTRAINT statements for one UNIQUE constraint and one FOREIGN KEY constraint", func() {
				constraints := []backup.Constraint{foreignTwo, uniqueOne}
				backup.PrintConstraintStatements(backupfile, toc, constraints, emptyMetadataMap)
				testutils.AssertBufferContents(toc.PredataEntries, buffer,
					`ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_key UNIQUE (i);`,
					`ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_j_fkey FOREIGN KEY (j) REFERENCES other_tablename(b);`)
			})
			It("prints ADD CONSTRAINT statements for one PRIMARY KEY constraint and one FOREIGN KEY constraint", func() {
				constraints := []backup.Constraint{foreignTwo, primarySingle}
				backup.PrintConstraintStatements(backupfile, toc, constraints, emptyMetadataMap)
				testutils.AssertBufferContents(toc.PredataEntries, buffer,
					`ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_pkey PRIMARY KEY (i);`,
					`ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_j_fkey FOREIGN KEY (j) REFERENCES other_tablename(b);`)
			})
			It("prints ADD CONSTRAINT statements for one two-column composite PRIMARY KEY constraint and one FOREIGN KEY constraint", func() {
				constraints := []backup.Constraint{foreignTwo, primaryComposite}
				backup.PrintConstraintStatements(backupfile, toc, constraints, emptyMetadataMap)
				testutils.AssertBufferContents(toc.PredataEntries, buffer,
					`ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_pkey PRIMARY KEY (i, j);`,
					`ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_j_fkey FOREIGN KEY (j) REFERENCES other_tablename(b);`)
			})
		})
		Context("Constraints involving the same column", func() {
			It("prints ADD CONSTRAINT statements for one UNIQUE constraint and one FOREIGN KEY constraint", func() {
				constraints := []backup.Constraint{foreignOne, uniqueOne}
				backup.PrintConstraintStatements(backupfile, toc, constraints, emptyMetadataMap)
				testutils.AssertBufferContents(toc.PredataEntries, buffer,
					`ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_key UNIQUE (i);`,
					`ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_fkey FOREIGN KEY (i) REFERENCES other_tablename(a);`)
			})
			It("prints ADD CONSTRAINT statements for one PRIMARY KEY constraint and one FOREIGN KEY constraint", func() {
				constraints := []backup.Constraint{foreignOne, primarySingle}
				backup.PrintConstraintStatements(backupfile, toc, constraints, emptyMetadataMap)
				testutils.AssertBufferContents(toc.PredataEntries, buffer,
					`ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_pkey PRIMARY KEY (i);`,
					`ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_fkey FOREIGN KEY (i) REFERENCES other_tablename(a);`)
			})
			It("prints ADD CONSTRAINT statements for a two-column composite PRIMARY KEY constraint and one FOREIGN KEY constraint", func() {
				constraints := []backup.Constraint{foreignOne, primaryComposite}
				backup.PrintConstraintStatements(backupfile, toc, constraints, emptyMetadataMap)
				testutils.AssertBufferContents(toc.PredataEntries, buffer,
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
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `ALTER TABLE public.tablename ADD CONSTRAINT tablename_i_key UNIQUE (i);`)
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
		It("can print a schema with privileges, an owner, and a comment", func() {
			schemas := []backup.Schema{{Oid: 1, Name: "schemaname"}}
			schemaMetadataMap := testutils.DefaultMetadataMap("SCHEMA", true, true, true)

			backup.PrintCreateSchemaStatements(backupfile, toc, schemas, schemaMetadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE SCHEMA schemaname;

COMMENT ON SCHEMA schemaname IS 'This is a schema comment.';


ALTER SCHEMA schemaname OWNER TO testrole;


REVOKE ALL ON SCHEMA schemaname FROM PUBLIC;
REVOKE ALL ON SCHEMA schemaname FROM testrole;
GRANT ALL ON SCHEMA schemaname TO testrole;`)
		})
	})
	Describe("SchemaFromString", func() {
		It("can parse an unquoted string", func() {
			testString := `schemaname`
			newSchema := backup.SchemaFromString(testString)
			Expect(newSchema.Oid).To(Equal(uint32(0)))
			Expect(newSchema.Name).To(Equal(`schemaname`))
		})
		It("can parse a quoted string", func() {
			testString := `"schema,name"`
			newSchema := backup.SchemaFromString(testString)
			Expect(newSchema.Oid).To(Equal(uint32(0)))
			Expect(newSchema.Name).To(Equal(`schema,name`))
		})
		It("panics if given an invalid string", func() {
			testString := `schema.name`
			defer testhelper.ShouldPanicWithMessage(`schema.name is not a valid identifier`)
			backup.SchemaFromString(testString)
		})
	})
	Describe("GetUniqueSchemas", func() {
		alphabeticalAFoo := backup.Relation{SchemaOid: 1, Oid: 0, Schema: "otherschema", Name: "foo"}
		alphabeticalABar := backup.Relation{SchemaOid: 1, Oid: 0, Schema: "otherschema", Name: "bar"}
		schemaOther := backup.Schema{Oid: 2, Name: "otherschema"}
		alphabeticalBFoo := backup.Relation{SchemaOid: 2, Oid: 0, Schema: "public", Name: "foo"}
		alphabeticalBBar := backup.Relation{SchemaOid: 2, Oid: 0, Schema: "public", Name: "bar"}
		schemaPublic := backup.Schema{Oid: 1, Name: "public"}
		schemas := []backup.Schema{schemaOther, schemaPublic}

		It("has multiple tables in a single schema", func() {
			tables := []backup.Relation{alphabeticalAFoo, alphabeticalABar}
			uniqueSchemas := backup.GetUniqueSchemas(schemas, tables)
			Expect(uniqueSchemas).To(Equal([]backup.Schema{schemaPublic}))
		})
		It("has multiple schemas, each with multiple tables", func() {
			tables := []backup.Relation{alphabeticalBFoo, alphabeticalBBar, alphabeticalAFoo, alphabeticalABar}
			uniqueSchemas := backup.GetUniqueSchemas(schemas, tables)
			Expect(uniqueSchemas).To(Equal([]backup.Schema{schemaOther, schemaPublic}))
		})
		It("has no tables", func() {
			tables := []backup.Relation{}
			uniqueSchemas := backup.GetUniqueSchemas(schemas, tables)
			Expect(uniqueSchemas).To(Equal([]backup.Schema{}))
		})
	})
	Describe("PrintObjectMetadata", func() {
		hasAllPrivileges := testutils.DefaultACLForType("anothertestrole", "TABLE")
		hasMostPrivileges := testutils.DefaultACLForType("testrole", "TABLE")
		hasMostPrivileges.Trigger = false
		hasSinglePrivilege := backup.ACL{Grantee: "", Trigger: true}
		hasAllPrivilegesWithGrant := testutils.DefaultACLForTypeWithGrant("anothertestrole", "TABLE")
		hasMostPrivilegesWithGrant := testutils.DefaultACLForTypeWithGrant("testrole", "TABLE")
		hasMostPrivilegesWithGrant.TriggerWithGrant = false
		hasSinglePrivilegeWithGrant := backup.ACL{Grantee: "", TriggerWithGrant: true}
		privileges := []backup.ACL{hasAllPrivileges, hasMostPrivileges, hasSinglePrivilege}
		privilegesWithGrant := []backup.ACL{hasAllPrivilegesWithGrant, hasMostPrivilegesWithGrant, hasSinglePrivilegeWithGrant}
		It("prints a block with a table comment", func() {
			tableMetadata := backup.ObjectMetadata{Comment: "This is a table comment."}
			backup.PrintObjectMetadata(backupfile, tableMetadata, "public.tablename", "TABLE")
			testhelper.ExpectRegexp(buffer, `

COMMENT ON TABLE public.tablename IS 'This is a table comment.';`)
		})
		It("prints a block with a table comment with special characters", func() {
			tableMetadata := backup.ObjectMetadata{Comment: `This is a ta'ble 1+=;,./\>,<@\\n^comment.`}
			backup.PrintObjectMetadata(backupfile, tableMetadata, "public.tablename", "TABLE")
			testhelper.ExpectRegexp(buffer, `

COMMENT ON TABLE public.tablename IS 'This is a ta''ble 1+=;,./\>,<@\\n^comment.';`)
		})
		It("prints an ALTER TABLE ... OWNER TO statement to set the table owner", func() {
			tableMetadata := backup.ObjectMetadata{Owner: "testrole"}
			backup.PrintObjectMetadata(backupfile, tableMetadata, "public.tablename", "TABLE")
			testhelper.ExpectRegexp(buffer, `

ALTER TABLE public.tablename OWNER TO testrole;`)
		})
		It("prints a block of REVOKE and GRANT statements", func() {
			tableMetadata := backup.ObjectMetadata{Privileges: privileges}
			backup.PrintObjectMetadata(backupfile, tableMetadata, "public.tablename", "TABLE")
			testhelper.ExpectRegexp(buffer, `

REVOKE ALL ON TABLE public.tablename FROM PUBLIC;
GRANT ALL ON TABLE public.tablename TO anothertestrole;
GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES ON TABLE public.tablename TO testrole;
GRANT TRIGGER ON TABLE public.tablename TO PUBLIC;`)
		})
		It("prints a block of REVOKE and GRANT statements WITH GRANT OPTION", func() {
			tableMetadata := backup.ObjectMetadata{Privileges: privilegesWithGrant}
			backup.PrintObjectMetadata(backupfile, tableMetadata, "public.tablename", "TABLE")
			testhelper.ExpectRegexp(buffer, `

REVOKE ALL ON TABLE public.tablename FROM PUBLIC;
GRANT ALL ON TABLE public.tablename TO anothertestrole WITH GRANT OPTION;
GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES ON TABLE public.tablename TO testrole WITH GRANT OPTION;
GRANT TRIGGER ON TABLE public.tablename TO PUBLIC WITH GRANT OPTION;`)
		})
		It("prints a block of REVOKE and GRANT statements, some with WITH GRANT OPTION, some without", func() {
			tableMetadata := backup.ObjectMetadata{Privileges: []backup.ACL{hasAllPrivileges, hasMostPrivilegesWithGrant}}
			backup.PrintObjectMetadata(backupfile, tableMetadata, "public.tablename", "TABLE")
			testhelper.ExpectRegexp(buffer, `

REVOKE ALL ON TABLE public.tablename FROM PUBLIC;
GRANT ALL ON TABLE public.tablename TO anothertestrole;
GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES ON TABLE public.tablename TO testrole WITH GRANT OPTION;`)
		})
		It("prints both an ALTER TABLE ... OWNER TO statement and a table comment", func() {
			tableMetadata := backup.ObjectMetadata{Comment: "This is a table comment.", Owner: "testrole"}
			backup.PrintObjectMetadata(backupfile, tableMetadata, "public.tablename", "TABLE")
			testhelper.ExpectRegexp(buffer, `

COMMENT ON TABLE public.tablename IS 'This is a table comment.';


ALTER TABLE public.tablename OWNER TO testrole;`)
		})
		It("prints both a block of REVOKE and GRANT statements and an ALTER TABLE ... OWNER TO statement", func() {
			tableMetadata := backup.ObjectMetadata{Privileges: privileges, Owner: "testrole"}
			backup.PrintObjectMetadata(backupfile, tableMetadata, "public.tablename", "TABLE")
			testhelper.ExpectRegexp(buffer, `

ALTER TABLE public.tablename OWNER TO testrole;


REVOKE ALL ON TABLE public.tablename FROM PUBLIC;
REVOKE ALL ON TABLE public.tablename FROM testrole;
GRANT ALL ON TABLE public.tablename TO anothertestrole;
GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES ON TABLE public.tablename TO testrole;
GRANT TRIGGER ON TABLE public.tablename TO PUBLIC;`)
		})
		It("prints both a block of REVOKE and GRANT statements and a table comment", func() {
			tableMetadata := backup.ObjectMetadata{Privileges: privileges, Comment: "This is a table comment."}
			backup.PrintObjectMetadata(backupfile, tableMetadata, "public.tablename", "TABLE")
			testhelper.ExpectRegexp(buffer, `

COMMENT ON TABLE public.tablename IS 'This is a table comment.';


REVOKE ALL ON TABLE public.tablename FROM PUBLIC;
GRANT ALL ON TABLE public.tablename TO anothertestrole;
GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES ON TABLE public.tablename TO testrole;
GRANT TRIGGER ON TABLE public.tablename TO PUBLIC;`)
		})
		It("prints REVOKE and GRANT statements, an ALTER TABLE ... OWNER TO statement, and comments", func() {
			tableMetadata := backup.ObjectMetadata{Privileges: privileges, Owner: "testrole", Comment: "This is a table comment."}
			backup.PrintObjectMetadata(backupfile, tableMetadata, "public.tablename", "TABLE")
			testhelper.ExpectRegexp(buffer, `

COMMENT ON TABLE public.tablename IS 'This is a table comment.';


ALTER TABLE public.tablename OWNER TO testrole;


REVOKE ALL ON TABLE public.tablename FROM PUBLIC;
REVOKE ALL ON TABLE public.tablename FROM testrole;
GRANT ALL ON TABLE public.tablename TO anothertestrole;
GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES ON TABLE public.tablename TO testrole;
GRANT TRIGGER ON TABLE public.tablename TO PUBLIC;`)
		})
		It("prints SERVER for ALTER and FOREIGN SERVER for GRANT/REVOKE for a foreign server", func() {
			serverPrivileges := testutils.DefaultACLForType("testrole", "FOREIGN SERVER")
			serverMetadata := backup.ObjectMetadata{Privileges: []backup.ACL{serverPrivileges}, Owner: "testrole"}
			backup.PrintObjectMetadata(backupfile, serverMetadata, "foreignserver", "FOREIGN SERVER")
			testhelper.ExpectRegexp(buffer, `

ALTER SERVER foreignserver OWNER TO testrole;


REVOKE ALL ON FOREIGN SERVER foreignserver FROM PUBLIC;
REVOKE ALL ON FOREIGN SERVER foreignserver FROM testrole;
GRANT ALL ON FOREIGN SERVER foreignserver TO testrole;`)
		})
		Context("Views and sequences have owners", func() {
			objectMetadata := backup.ObjectMetadata{Owner: "testrole"}
			AfterEach(func() {
				testhelper.SetDBVersion(connectionPool, "5.1.0")
			})
			It("prints an ALTER TABLE ... OWNER TO statement to set the owner for a sequence if version < 6", func() {
				testhelper.SetDBVersion(connectionPool, "5.0.0")
				backup.PrintObjectMetadata(backupfile, objectMetadata, "public.sequencename", "SEQUENCE")
				testhelper.ExpectRegexp(buffer, `

ALTER TABLE public.sequencename OWNER TO testrole;`)
			})
			It("prints an ALTER TABLE ... OWNER TO statement to set the owner for a view if version < 6", func() {
				testhelper.SetDBVersion(connectionPool, "5.0.0")
				backup.PrintObjectMetadata(backupfile, objectMetadata, "public.viewname", "VIEW")
				testhelper.ExpectRegexp(buffer, `

ALTER TABLE public.viewname OWNER TO testrole;`)
			})
			It("prints an ALTER SEQUENCE ... OWNER TO statement to set the owner for a sequence if version >= 6", func() {
				testhelper.SetDBVersion(connectionPool, "6.0.0")
				backup.PrintObjectMetadata(backupfile, objectMetadata, "public.sequencename", "SEQUENCE")
				testhelper.ExpectRegexp(buffer, `

ALTER SEQUENCE public.sequencename OWNER TO testrole;`)
			})
			It("prints an ALTER VIEW ... OWNER TO statement to set the owner for a view if version >= 6", func() {
				testhelper.SetDBVersion(connectionPool, "6.0.0")
				backup.PrintObjectMetadata(backupfile, objectMetadata, "public.viewname", "VIEW")
				testhelper.ExpectRegexp(buffer, `

ALTER VIEW public.viewname OWNER TO testrole;`)
			})
		})
	})
	Describe("ConstructMetadataMap", func() {
		object1A := backup.MetadataQueryStruct{Oid: 1, Privileges: sql.NullString{String: "gpadmin=r/gpadmin", Valid: true}, Kind: "", Owner: "testrole", Comment: ""}
		object1B := backup.MetadataQueryStruct{Oid: 1, Privileges: sql.NullString{String: "testrole=r/testrole", Valid: true}, Kind: "", Owner: "testrole", Comment: ""}
		object2 := backup.MetadataQueryStruct{Oid: 2, Privileges: sql.NullString{String: "testrole=r/testrole", Valid: true}, Kind: "", Owner: "testrole", Comment: "this is a comment"}
		objectDefaultKind := backup.MetadataQueryStruct{Oid: 3, Privileges: sql.NullString{String: "", Valid: false}, Kind: "Default", Owner: "testrole", Comment: ""}
		objectEmptyKind := backup.MetadataQueryStruct{Oid: 4, Privileges: sql.NullString{String: "", Valid: false}, Kind: "Empty", Owner: "testrole", Comment: ""}
		var metadataList []backup.MetadataQueryStruct
		BeforeEach(func() {
			rolnames := sqlmock.NewRows([]string{"rolename", "quotedrolename"}).
				AddRow("gpadmin", "gpadmin").
				AddRow("testrole", "testrole")
			mock.ExpectQuery("SELECT rolname (.*)").
				WillReturnRows(rolnames)
			metadataList = []backup.MetadataQueryStruct{}
		})
		It("No objects", func() {
			metadataMap := backup.ConstructMetadataMap(metadataList)
			Expect(metadataMap).To(BeEmpty())
		})
		It("One object", func() {
			metadataList = []backup.MetadataQueryStruct{object2}
			metadataMap := backup.ConstructMetadataMap(metadataList)
			expectedObjectMetadata := backup.ObjectMetadata{Privileges: []backup.ACL{{Grantee: "testrole", Select: true}}, Owner: "testrole", Comment: "this is a comment"}
			Expect(metadataMap).To(HaveLen(1))
			Expect(metadataMap[2]).To(Equal(expectedObjectMetadata))
		})
		It("One object with two ACL entries", func() {
			metadataList = []backup.MetadataQueryStruct{object1A, object1B}
			metadataMap := backup.ConstructMetadataMap(metadataList)
			expectedObjectMetadata := backup.ObjectMetadata{Privileges: []backup.ACL{{Grantee: "gpadmin", Select: true}, {Grantee: "testrole", Select: true}}, Owner: "testrole"}
			Expect(metadataMap).To(HaveLen(1))
			Expect(metadataMap[1]).To(Equal(expectedObjectMetadata))
		})
		It("Multiple objects", func() {
			metadataList = []backup.MetadataQueryStruct{object1A, object1B, object2}
			metadataMap := backup.ConstructMetadataMap(metadataList)
			expectedObjectMetadataOne := backup.ObjectMetadata{Privileges: []backup.ACL{{Grantee: "gpadmin", Select: true}, {Grantee: "testrole", Select: true}}, Owner: "testrole"}
			expectedObjectMetadataTwo := backup.ObjectMetadata{Privileges: []backup.ACL{{Grantee: "testrole", Select: true}}, Owner: "testrole", Comment: "this is a comment"}
			Expect(metadataMap).To(HaveLen(2))
			Expect(metadataMap[1]).To(Equal(expectedObjectMetadataOne))
			Expect(metadataMap[2]).To(Equal(expectedObjectMetadataTwo))
		})
		It("Default Kind", func() {
			metadataList = []backup.MetadataQueryStruct{objectDefaultKind}
			metadataMap := backup.ConstructMetadataMap(metadataList)
			expectedObjectMetadata := backup.ObjectMetadata{Privileges: []backup.ACL{}, Owner: "testrole"}
			Expect(metadataMap).To(HaveLen(1))
			Expect(metadataMap[3]).To(Equal(expectedObjectMetadata))
		})
		It("'Empty' Kind", func() {
			metadataList = []backup.MetadataQueryStruct{objectEmptyKind}
			metadataMap := backup.ConstructMetadataMap(metadataList)
			expectedObjectMetadata := backup.ObjectMetadata{Privileges: []backup.ACL{{Grantee: "GRANTEE"}}, Owner: "testrole"}
			Expect(metadataMap).To(HaveLen(1))
			Expect(metadataMap[4]).To(Equal(expectedObjectMetadata))
		})
	})
	Describe("ParseACL", func() {
		var quotedRoleNames map[string]string
		BeforeEach(func() {
			quotedRoleNames = map[string]string{
				"testrole":  "testrole",
				"Test|role": `"Test|role"`,
			}
		})
		It("parses an ACL string representing default privileges", func() {
			aclStr := ""
			result := backup.ParseACL(aclStr, quotedRoleNames)
			Expect(result).To(BeNil())
		})
		It("parses an ACL string representing no privileges", func() {
			aclStr := "GRANTEE=/GRANTOR"
			expected := backup.ACL{Grantee: "GRANTEE"}
			result := backup.ParseACL(aclStr, quotedRoleNames)
			structmatcher.ExpectStructsToMatch(&expected, result)
		})
		It("parses an ACL string containing a role with multiple privileges", func() {
			aclStr := "testrole=arwdDxt/gpadmin"
			expected := testutils.DefaultACLForType("testrole", "TABLE")
			result := backup.ParseACL(aclStr, quotedRoleNames)
			structmatcher.ExpectStructsToMatch(&expected, result)
		})
		It("parses an ACL string containing a role with one privilege", func() {
			aclStr := "testrole=a/gpadmin"
			expected := backup.ACL{Grantee: "testrole", Insert: true}
			result := backup.ParseACL(aclStr, quotedRoleNames)
			structmatcher.ExpectStructsToMatch(&expected, result)
		})
		It("parses an ACL string containing a role name with special characters", func() {
			aclStr := `"Test|role"=a/gpadmin`
			expected := backup.ACL{Grantee: `"Test|role"`, Insert: true}
			result := backup.ParseACL(aclStr, quotedRoleNames)
			structmatcher.ExpectStructsToMatch(&expected, result)
		})
		It("parses an ACL string containing a role with some privileges with GRANT and some without including GRANT", func() {
			aclStr := "testrole=ar*w*d*tXUCTc/gpadmin"
			expected := backup.ACL{Grantee: "testrole", Insert: true, SelectWithGrant: true, UpdateWithGrant: true,
				DeleteWithGrant: true, Trigger: true, Execute: true, Usage: true, Create: true, Temporary: true, Connect: true}
			result := backup.ParseACL(aclStr, quotedRoleNames)
			structmatcher.ExpectStructsToMatch(&expected, result)
		})
		It("parses an ACL string containing a role with all privileges including GRANT", func() {
			aclStr := "testrole=a*D*x*t*X*U*C*T*c*/gpadmin"
			expected := backup.ACL{Grantee: "testrole", InsertWithGrant: true, TruncateWithGrant: true, ReferencesWithGrant: true,
				TriggerWithGrant: true, ExecuteWithGrant: true, UsageWithGrant: true, CreateWithGrant: true, TemporaryWithGrant: true, ConnectWithGrant: true}
			result := backup.ParseACL(aclStr, quotedRoleNames)
			structmatcher.ExpectStructsToMatch(&expected, result)
		})
		It("parses an ACL string granting privileges to PUBLIC", func() {
			aclStr := "=a/gpadmin"
			expected := backup.ACL{Grantee: "", Insert: true}
			result := backup.ParseACL(aclStr, quotedRoleNames)
			structmatcher.ExpectStructsToMatch(&expected, result)
		})
	})
	Describe("PrintDependentObjectStatements", func() {
		var (
			objects      []backup.Sortable
			metadataMap  backup.MetadataMap
			tableDefsMap map[uint32]backup.TableDefinition
		)
		BeforeEach(func() {
			objects = []backup.Sortable{
				backup.Function{Oid: 1, Schema: "public", Name: "function", FunctionBody: "SELECT $1 + $2",
					Arguments: "integer, integer", IdentArgs: "integer, integer", ResultType: "integer", Language: "sql"},
				backup.Type{Oid: 2, Schema: "public", Name: "base", Type: "b", Input: "typin", Output: "typout", Category: "U"},
				backup.Type{Oid: 3, Schema: "public", Name: "composite", Type: "c", Attributes: []backup.Attribute{{Name: "foo", Type: "integer"}}, Category: "U"},
				backup.Type{Oid: 4, Schema: "public", Name: "domain", Type: "d", BaseType: "numeric", Category: "U"},
				backup.Relation{Oid: 5, Schema: "public", Name: "relation"},
				backup.ExternalProtocol{Oid: 6, Name: "ext_protocol", Trusted: true, ReadFunction: 2, WriteFunction: 1, Validator: 0,
					FuncMap: map[uint32]string{
						1: "public.write_to_s3",
						2: "public.read_from_s3",
					},
				},
			}
			metadataMap = backup.MetadataMap{
				1: backup.ObjectMetadata{Comment: "function"},
				2: backup.ObjectMetadata{Comment: "base type"},
				3: backup.ObjectMetadata{Comment: "composite type"},
				4: backup.ObjectMetadata{Comment: "domain"},
				5: backup.ObjectMetadata{Comment: "relation"},
				6: backup.ObjectMetadata{Comment: "protocol"},
			}
			tableDefsMap = map[uint32]backup.TableDefinition{
				5: {DistPolicy: "DISTRIBUTED RANDOMLY", ColumnDefs: []backup.ColumnDefinition{}},
			}
		})
		It("prints create statements for dependent types, functions, protocols, and tables (domain has a constraint)", func() {
			constraints := []backup.Constraint{
				{Name: "check_constraint", ConDef: "CHECK (VALUE > 2)", OwningObject: "public.domain"},
			}
			backup.PrintDependentObjectStatements(backupfile, toc, objects, metadataMap, tableDefsMap, constraints)
			testhelper.ExpectRegexp(buffer, `
CREATE FUNCTION public.function(integer, integer) RETURNS integer AS
$_$SELECT $1 + $2$_$
LANGUAGE sql;


COMMENT ON FUNCTION public.function(integer, integer) IS 'function';


CREATE TYPE public.base (
	INPUT = typin,
	OUTPUT = typout
);


COMMENT ON TYPE public.base IS 'base type';


CREATE TYPE public.composite AS (
	foo integer
);

COMMENT ON TYPE public.composite IS 'composite type';

CREATE DOMAIN public.domain AS numeric
	CONSTRAINT check_constraint CHECK (VALUE > 2);


COMMENT ON DOMAIN public.domain IS 'domain';


CREATE TABLE public.relation (
) DISTRIBUTED RANDOMLY;


COMMENT ON TABLE public.relation IS 'relation';


CREATE TRUSTED PROTOCOL ext_protocol (readfunc = public.read_from_s3, writefunc = public.write_to_s3);


COMMENT ON PROTOCOL ext_protocol IS 'protocol';
`)
		})
		It("prints create statements for dependent types, functions, protocols, and tables (no domain constraint)", func() {
			constraints := []backup.Constraint{}
			backup.PrintDependentObjectStatements(backupfile, toc, objects, metadataMap, tableDefsMap, constraints)
			testhelper.ExpectRegexp(buffer, `
CREATE FUNCTION public.function(integer, integer) RETURNS integer AS
$_$SELECT $1 + $2$_$
LANGUAGE sql;


COMMENT ON FUNCTION public.function(integer, integer) IS 'function';


CREATE TYPE public.base (
	INPUT = typin,
	OUTPUT = typout
);


COMMENT ON TYPE public.base IS 'base type';


CREATE TYPE public.composite AS (
	foo integer
);

COMMENT ON TYPE public.composite IS 'composite type';

CREATE DOMAIN public.domain AS numeric;


COMMENT ON DOMAIN public.domain IS 'domain';


CREATE TABLE public.relation (
) DISTRIBUTED RANDOMLY;


COMMENT ON TABLE public.relation IS 'relation';


CREATE TRUSTED PROTOCOL ext_protocol (readfunc = public.read_from_s3, writefunc = public.write_to_s3);


COMMENT ON PROTOCOL ext_protocol IS 'protocol';
`)
		})
	})
})
