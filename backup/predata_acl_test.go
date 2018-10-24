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

var _ = Describe("backup/predata_acl tests", func() {
	BeforeEach(func() {
		toc, backupfile = testutils.InitializeTestTOC(buffer, "predata")
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
		object1A := backup.MetadataQueryStruct{UniqueID: backup.UniqueID{Oid: 1}, Privileges: sql.NullString{String: "gpadmin=r/gpadmin", Valid: true}, Kind: "", Owner: "testrole", Comment: ""}
		object1B := backup.MetadataQueryStruct{UniqueID: backup.UniqueID{Oid: 1}, Privileges: sql.NullString{String: "testrole=r/testrole", Valid: true}, Kind: "", Owner: "testrole", Comment: ""}
		object2 := backup.MetadataQueryStruct{UniqueID: backup.UniqueID{Oid: 2}, Privileges: sql.NullString{String: "testrole=r/testrole", Valid: true}, Kind: "", Owner: "testrole", Comment: "this is a comment"}
		objectDefaultKind := backup.MetadataQueryStruct{UniqueID: backup.UniqueID{Oid: 3}, Privileges: sql.NullString{String: "", Valid: false}, Kind: "Default", Owner: "testrole", Comment: ""}
		objectEmptyKind := backup.MetadataQueryStruct{UniqueID: backup.UniqueID{Oid: 4}, Privileges: sql.NullString{String: "", Valid: false}, Kind: "Empty", Owner: "testrole", Comment: ""}
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
			Expect(metadataMap[backup.UniqueID{Oid: 2}]).To(Equal(expectedObjectMetadata))
		})
		It("One object with two ACL entries", func() {
			metadataList = []backup.MetadataQueryStruct{object1A, object1B}
			metadataMap := backup.ConstructMetadataMap(metadataList)
			expectedObjectMetadata := backup.ObjectMetadata{Privileges: []backup.ACL{{Grantee: "gpadmin", Select: true}, {Grantee: "testrole", Select: true}}, Owner: "testrole"}
			Expect(metadataMap).To(HaveLen(1))
			Expect(metadataMap[backup.UniqueID{Oid: 1}]).To(Equal(expectedObjectMetadata))
		})
		It("Multiple objects", func() {
			metadataList = []backup.MetadataQueryStruct{object1A, object1B, object2}
			metadataMap := backup.ConstructMetadataMap(metadataList)
			expectedObjectMetadataOne := backup.ObjectMetadata{Privileges: []backup.ACL{{Grantee: "gpadmin", Select: true}, {Grantee: "testrole", Select: true}}, Owner: "testrole"}
			expectedObjectMetadataTwo := backup.ObjectMetadata{Privileges: []backup.ACL{{Grantee: "testrole", Select: true}}, Owner: "testrole", Comment: "this is a comment"}
			Expect(metadataMap).To(HaveLen(2))
			Expect(metadataMap[backup.UniqueID{Oid: 1}]).To(Equal(expectedObjectMetadataOne))
			Expect(metadataMap[backup.UniqueID{Oid: 2}]).To(Equal(expectedObjectMetadataTwo))
		})
		It("Default Kind", func() {
			metadataList = []backup.MetadataQueryStruct{objectDefaultKind}
			metadataMap := backup.ConstructMetadataMap(metadataList)
			expectedObjectMetadata := backup.ObjectMetadata{Privileges: []backup.ACL{}, Owner: "testrole"}
			Expect(metadataMap).To(HaveLen(1))
			Expect(metadataMap[backup.UniqueID{Oid: 3}]).To(Equal(expectedObjectMetadata))
		})
		It("'Empty' Kind", func() {
			metadataList = []backup.MetadataQueryStruct{objectEmptyKind}
			metadataMap := backup.ConstructMetadataMap(metadataList)
			expectedObjectMetadata := backup.ObjectMetadata{Privileges: []backup.ACL{{Grantee: "GRANTEE"}}, Owner: "testrole"}
			Expect(metadataMap).To(HaveLen(1))
			Expect(metadataMap[backup.UniqueID{Oid: 4}]).To(Equal(expectedObjectMetadata))
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
})
