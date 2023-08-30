package integration

import (
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
		testutils.SkipIfBefore6(connectionPool)
	})
	Describe("PrintDefaultPrivilegesStatements", func() {
		It("create default privileges for a table", func() {
			privs := []backup.ACL{{Grantee: "", Select: true}, testutils.DefaultACLForType("testrole", toc.OBJ_TABLE)}
			defaultPrivileges := []backup.DefaultPrivileges{{Schema: "", Privileges: privs, ObjectType: "r", Owner: "testrole"}}

			backup.PrintDefaultPrivilegesStatements(backupfile, tocfile, defaultPrivileges)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "ALTER DEFAULT PRIVILEGES FOR ROLE testrole REVOKE ALL ON TABLES FROM PUBLIC;")

			resultPrivileges := backup.GetDefaultPrivileges(connectionPool)

			Expect(resultPrivileges).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&defaultPrivileges[0], &resultPrivileges[0], "Oid")
		})
		It("create default privileges for a sequence with grant option in schema", func() {
			privs := []backup.ACL{{Grantee: "testrole", SelectWithGrant: true}}
			defaultPrivileges := []backup.DefaultPrivileges{{Schema: "", Privileges: privs, ObjectType: "S", Owner: "testrole"}}

			backup.PrintDefaultPrivilegesStatements(backupfile, tocfile, defaultPrivileges)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			// Both of these statements are required to remove the entry from the pg_default_acl catalog table, otherwise it will pollute other tests
			defer testhelper.AssertQueryRuns(connectionPool, "ALTER DEFAULT PRIVILEGES FOR ROLE testrole GRANT ALL ON SEQUENCES TO testrole;")
			defer testhelper.AssertQueryRuns(connectionPool, "ALTER DEFAULT PRIVILEGES FOR ROLE testrole REVOKE ALL ON SEQUENCES FROM testrole;")

			resultPrivileges := backup.GetDefaultPrivileges(connectionPool)

			Expect(resultPrivileges).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&defaultPrivileges[0], &resultPrivileges[0], "Oid")
		})
	})
})
