package integration

import (
	"regexp"

	"github.com/greenplum-db/gp-common-go-libs/structmatcher"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
)

var _ = Describe("backup integration create statement tests", func() {
	BeforeEach(func() {
		toc, backupfile = testutils.InitializeTestTOC(buffer, "predata")
	})
	Describe("PrintDatabaseGUCs", func() {
		defaultOidGUC := "SET default_with_oids TO 'true'"
		searchPathGUC := "SET search_path TO pg_catalog, public"
		defaultStorageGUC := "SET gp_default_storage_options TO 'appendonly=true, compresslevel=6, orientation=row, compresstype=none'"
		It("creates database GUCs with correct quoting", func() {
			gucs := []string{defaultOidGUC, searchPathGUC, defaultStorageGUC}

			backup.PrintDatabaseGUCs(backupfile, toc, gucs, "testdb")
			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "ALTER DATABASE testdb RESET default_with_oids")
			defer testhelper.AssertQueryRuns(connectionPool, "ALTER DATABASE testdb RESET search_path")
			defer testhelper.AssertQueryRuns(connectionPool, "ALTER DATABASE testdb RESET gp_default_storage_options")
			resultGUCs := backup.GetDatabaseGUCs(connectionPool)
			Expect(resultGUCs).To(Equal(gucs))
		})
	})
	Describe("PrintCreateResourceQueueStatements", func() {
		It("creates a basic resource queue with a comment", func() {
			basicQueue := backup.ResourceQueue{Oid: 1, Name: `"basicQueue"`, ActiveStatements: -1, MaxCost: "32.80", CostOvercommit: false, MinCost: "0.00", Priority: "medium", MemoryLimit: "-1"}
			resQueueMetadataMap := testutils.DefaultMetadataMap("RESOURCE QUEUE", false, false, true)
			resQueueMetadata := resQueueMetadataMap[basicQueue.GetUniqueID()]

			backup.PrintCreateResourceQueueStatements(backupfile, toc, []backup.ResourceQueue{basicQueue}, resQueueMetadataMap)

			// CREATE RESOURCE QUEUE statements can not be part of a multi-command statement, so
			// feed the CREATE RESOURCE QUEUE and COMMENT ON statements separately.
			hunks := regexp.MustCompile(";\n\n").Split(buffer.String(), 2)
			testhelper.AssertQueryRuns(connectionPool, hunks[0])
			defer testhelper.AssertQueryRuns(connectionPool, `DROP RESOURCE QUEUE "basicQueue"`)
			testhelper.AssertQueryRuns(connectionPool, hunks[1])

			resultResourceQueues := backup.GetResourceQueues(connectionPool)
			resQueueUniqueID := testutils.UniqueIDFromObjectName(connectionPool, "", "basicQueue", backup.TYPE_RESOURCEQUEUE)
			resultMetadataMap := backup.GetCommentsForObjectType(connectionPool, backup.TYPE_RESOURCEQUEUE)
			resultMetadata := resultMetadataMap[resQueueUniqueID]
			structmatcher.ExpectStructsToMatch(&resultMetadata, &resQueueMetadata)

			for _, resultQueue := range resultResourceQueues {
				if resultQueue.Name == `"basicQueue"` {
					structmatcher.ExpectStructsToMatchExcluding(&basicQueue, &resultQueue, "Oid")
					return
				}
			}
		})
		It("creates a resource queue with all attributes", func() {
			everythingQueue := backup.ResourceQueue{Oid: 1, Name: `"everythingQueue"`, ActiveStatements: 7, MaxCost: "32.80", CostOvercommit: true, MinCost: "22.80", Priority: "low", MemoryLimit: "2GB"}
			emptyMetadataMap := map[backup.UniqueID]backup.ObjectMetadata{}

			backup.PrintCreateResourceQueueStatements(backupfile, toc, []backup.ResourceQueue{everythingQueue}, emptyMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, `DROP RESOURCE QUEUE "everythingQueue"`)

			resultResourceQueues := backup.GetResourceQueues(connectionPool)

			for _, resultQueue := range resultResourceQueues {
				if resultQueue.Name == `"everythingQueue"` {
					structmatcher.ExpectStructsToMatchExcluding(&everythingQueue, &resultQueue, "Oid")
					return
				}
			}
			Fail("Could not find everythingQueue")
		})
	})
	Describe("PrintCreateResourceGroupStatements", func() {
		BeforeEach(func() {
			testutils.SkipIfBefore5(connectionPool)
		})
		It("creates a basic resource group", func() {
			someGroup := backup.ResourceGroup{Oid: 1, Name: "some_group", CPURateLimit: 10, MemoryLimit: 20, Concurrency: 15, MemorySharedQuota: 25, MemorySpillRatio: 30, MemoryAuditor: 0, Cpuset: "-1"}
			emptyMetadataMap := map[backup.UniqueID]backup.ObjectMetadata{}

			backup.PrintCreateResourceGroupStatements(backupfile, toc, []backup.ResourceGroup{someGroup}, emptyMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, `DROP RESOURCE GROUP some_group`)

			resultResourceGroups := backup.GetResourceGroups(connectionPool)

			for _, resultGroup := range resultResourceGroups {
				if resultGroup.Name == "some_group" {
					structmatcher.ExpectStructsToMatchExcluding(&someGroup, &resultGroup, "Oid")
					return
				}
			}
			Fail("Could not find some_group")
		})
		It("alters a default resource group", func() {
			defaultGroup := backup.ResourceGroup{Oid: 1, Name: "default_group", CPURateLimit: 10, MemoryLimit: 20, Concurrency: 15, MemorySharedQuota: 25, MemorySpillRatio: 30, MemoryAuditor: 0, Cpuset: "-1"}
			emptyMetadataMap := map[backup.UniqueID]backup.ObjectMetadata{}

			backup.PrintCreateResourceGroupStatements(backupfile, toc, []backup.ResourceGroup{defaultGroup}, emptyMetadataMap)

			hunks := regexp.MustCompile(";\n\n").Split(buffer.String(), 5)
			for i := 0; i < 5; i++ {
				testhelper.AssertQueryRuns(connectionPool, hunks[i])
			}
			resultResourceGroups := backup.GetResourceGroups(connectionPool)

			for _, resultGroup := range resultResourceGroups {
				if resultGroup.Name == "default_group" {
					structmatcher.ExpectStructsToMatchExcluding(&defaultGroup, &resultGroup, "Oid")
					return
				}
			}
			Fail("Could not find default_group")
		})
	})
	Describe("PrintCreateRoleStatements", func() {
		var role1 backup.Role
		BeforeEach(func() {
			role1 = backup.Role{
				Oid:             1,
				Name:            "role1",
				Super:           true,
				Inherit:         false,
				CreateRole:      false,
				CreateDB:        false,
				CanLogin:        false,
				ConnectionLimit: -1,
				Password:        "",
				ValidUntil:      "",
				ResQueue:        "pg_default",
				ResGroup:        "default_group",
				Createrexthttp:  false,
				Createrextgpfd:  false,
				Createwextgpfd:  false,
				Createrexthdfs:  false,
				Createwexthdfs:  false,
				TimeConstraints: nil,
			}
		})
		emptyConfigMap := map[string][]backup.RoleGUC{}
		emptyMetadataMap := backup.MetadataMap{}
		It("creates a basic role", func() {
			if connectionPool.Version.Before("5") {
				role1.ResGroup = ""
			}

			backup.PrintCreateRoleStatements(backupfile, toc, []backup.Role{role1}, emptyConfigMap, emptyMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, `DROP ROLE "role1"`)
			role1.Oid = testutils.OidFromObjectName(connectionPool, "", "role1", backup.TYPE_ROLE)

			resultRoles := backup.GetRoles(connectionPool)
			for _, role := range resultRoles {
				if role.Name == "role1" {
					structmatcher.ExpectStructsToMatch(&role1, role)
					return
				}
			}
			Fail("Role 'role1' was not found")
		})
		It("creates a basic role with user GUCs set", func() {
			if connectionPool.Version.Before("5") {
				role1.ResGroup = ""
			}
			roleConfigMap := map[string][]backup.RoleGUC{
				"role1": {
					{RoleName: "role1", Config: "SET gp_default_storage_options TO 'appendonly=true, compresslevel=6, orientation=row, compresstype=none'"},
					{RoleName: "role1", Config: "SET search_path TO public"}},
			}

			backup.PrintCreateRoleStatements(backupfile, toc, []backup.Role{role1}, roleConfigMap, emptyMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, `DROP ROLE "role1"`)

			resultGUCs := backup.GetRoleGUCs(connectionPool)

			Expect(resultGUCs["role1"]).To(ConsistOf(roleConfigMap["role1"]))
		})
		It("creates a basic role with db specific user GUCs set", func() {
			testutils.SkipIfBefore6(connectionPool)

			roleConfigMap := map[string][]backup.RoleGUC{
				"role1": {
					{RoleName: "role1", DbName: "testdb", Config: "SET gp_default_storage_options TO 'appendonly=true, compresslevel=6, orientation=row, compresstype=none'"},
					{RoleName: "role1", DbName: "testdb", Config: "SET search_path TO public"}},
			}

			backup.PrintCreateRoleStatements(backupfile, toc, []backup.Role{role1}, roleConfigMap, emptyMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, `DROP ROLE "role1"`)

			resultGUCs := backup.GetRoleGUCs(connectionPool)

			Expect(resultGUCs["role1"]).To(ConsistOf(roleConfigMap["role1"]))
		})
		It("creates a role with all attributes", func() {
			role1 := backup.Role{
				Oid:             1,
				Name:            "role1",
				Super:           false,
				Inherit:         true,
				CreateRole:      true,
				CreateDB:        true,
				CanLogin:        true,
				ConnectionLimit: 4,
				Password:        "md5a8b2c77dfeba4705f29c094592eb3369",
				ValidUntil:      "2099-01-01 08:00:00-00",
				ResQueue:        "pg_default",
				ResGroup:        "default_group",
				Createrexthttp:  true,
				Createrextgpfd:  true,
				Createwextgpfd:  true,
				Createrexthdfs:  true,
				Createwexthdfs:  true,
				TimeConstraints: []backup.TimeConstraint{
					{
						Oid:       0,
						StartDay:  0,
						StartTime: "13:30:00",
						EndDay:    3,
						EndTime:   "14:30:00",
					}, {
						Oid:       0,
						StartDay:  5,
						StartTime: "00:00:00",
						EndDay:    5,
						EndTime:   "24:00:00",
					},
				},
			}
			if connectionPool.Version.Before("5") {
				role1.ResGroup = ""
			}
			metadataMap := testutils.DefaultMetadataMap("ROLE", false, false, true)

			backup.PrintCreateRoleStatements(backupfile, toc, []backup.Role{role1}, emptyConfigMap, metadataMap)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, `DROP ROLE "role1"`)
			role1.Oid = testutils.OidFromObjectName(connectionPool, "", "role1", backup.TYPE_ROLE)

			resultRoles := backup.GetRoles(connectionPool)
			for _, role := range resultRoles {
				if role.Name == "role1" {
					structmatcher.ExpectStructsToMatchExcluding(&role1, role, "TimeConstraints.Oid")
					return
				}
			}
			Fail("Role 'role1' was not found")
		})
		It("creates a role with replication", func() {
			testutils.SkipIfBefore6(connectionPool)

			role1.Replication = true
			backup.PrintCreateRoleStatements(backupfile, toc, []backup.Role{role1}, emptyConfigMap, emptyMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, `DROP ROLE "role1"`)
			role1.Oid = testutils.OidFromObjectName(connectionPool, "", "role1", backup.TYPE_ROLE)

			resultRoles := backup.GetRoles(connectionPool)
			for _, role := range resultRoles {
				if role.Name == "role1" {
					structmatcher.ExpectStructsToMatch(&role1, role)
					return
				}
			}
			Fail("Role 'role1' was not found")
		})
	})
	Describe("PrintRoleMembershipStatements", func() {
		BeforeEach(func() {
			testhelper.AssertQueryRuns(connectionPool, `CREATE ROLE usergroup`)
			testhelper.AssertQueryRuns(connectionPool, `CREATE ROLE testuser`)
		})
		AfterEach(func() {
			defer testhelper.AssertQueryRuns(connectionPool, `DROP ROLE usergroup`)
			defer testhelper.AssertQueryRuns(connectionPool, `DROP ROLE testuser`)
		})
		It("grants a role without ADMIN OPTION", func() {
			numRoleMembers := len(backup.GetRoleMembers(connectionPool))
			expectedRoleMember := backup.RoleMember{Role: "usergroup", Member: "testuser", Grantor: "testrole", IsAdmin: false}
			backup.PrintRoleMembershipStatements(backupfile, toc, []backup.RoleMember{expectedRoleMember})

			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			resultRoleMembers := backup.GetRoleMembers(connectionPool)
			Expect(resultRoleMembers).To(HaveLen(numRoleMembers + 1))
			for _, roleMember := range resultRoleMembers {
				if roleMember.Role == "usergroup" {
					structmatcher.ExpectStructsToMatch(&expectedRoleMember, &roleMember)
					return
				}
			}
			Fail("Role 'testuser' is not a member of role 'usergroup'")
		})
		It("grants a role WITH ADMIN OPTION", func() {
			numRoleMembers := len(backup.GetRoleMembers(connectionPool))
			expectedRoleMember := backup.RoleMember{Role: "usergroup", Member: "testuser", Grantor: "testrole", IsAdmin: true}
			backup.PrintRoleMembershipStatements(backupfile, toc, []backup.RoleMember{expectedRoleMember})

			testhelper.AssertQueryRuns(connectionPool, buffer.String())

			resultRoleMembers := backup.GetRoleMembers(connectionPool)
			Expect(resultRoleMembers).To(HaveLen(numRoleMembers + 1))
			for _, roleMember := range resultRoleMembers {
				if roleMember.Role == "usergroup" {
					structmatcher.ExpectStructsToMatch(&expectedRoleMember, &roleMember)
					return
				}
			}
			Fail("Role 'testuser' is not a member of role 'usergroup'")
		})
	})
	Describe("PrintCreateTablespaceStatements", func() {
		var expectedTablespace backup.Tablespace
		BeforeEach(func() {
			if connectionPool.Version.AtLeast("6") {
				expectedTablespace = backup.Tablespace{Oid: 1, Tablespace: "test_tablespace", FileLocation: "'/tmp/test_dir'", SegmentLocations: []string{}}
			} else {
				expectedTablespace = backup.Tablespace{Oid: 1, Tablespace: "test_tablespace", FileLocation: "test_dir"}
			}
		})
		It("creates a basic tablespace", func() {
			numTablespaces := len(backup.GetTablespaces(connectionPool))
			emptyMetadataMap := backup.MetadataMap{}
			backup.PrintCreateTablespaceStatements(backupfile, toc, []backup.Tablespace{expectedTablespace}, emptyMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLESPACE test_tablespace")

			resultTablespaces := backup.GetTablespaces(connectionPool)
			Expect(resultTablespaces).To(HaveLen(numTablespaces + 1))
			for _, tablespace := range resultTablespaces {
				if tablespace.Tablespace == "test_tablespace" {
					structmatcher.ExpectStructsToMatchExcluding(&expectedTablespace, &tablespace, "Oid")
					return
				}
			}
			Fail("Tablespace 'test_tablespace' was not created")
		})
		It("creates a basic tablespace with different filespace locations", func() {
			testutils.SkipIfBefore6(connectionPool)

			expectedTablespace = backup.Tablespace{
				Oid: 1, Tablespace: "test_tablespace", FileLocation: "'/tmp/test_dir'",
				SegmentLocations: []string{"content0='/tmp/test_dir1'", "content1='/tmp/test_dir2'"},
			}
			numTablespaces := len(backup.GetTablespaces(connectionPool))
			emptyMetadataMap := backup.MetadataMap{}
			backup.PrintCreateTablespaceStatements(backupfile, toc, []backup.Tablespace{expectedTablespace}, emptyMetadataMap)

			testhelper.AssertQueryRuns(connectionPool, buffer.String())
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLESPACE test_tablespace")

			resultTablespaces := backup.GetTablespaces(connectionPool)
			Expect(resultTablespaces).To(HaveLen(numTablespaces + 1))
			for _, tablespace := range resultTablespaces {
				if tablespace.Tablespace == "test_tablespace" {
					structmatcher.ExpectStructsToMatchExcluding(&expectedTablespace, &tablespace, "Oid")
					return
				}
			}
			Fail("Tablespace 'test_tablespace' was not created")
		})
		It("creates a tablespace with permissions, an owner, and a comment", func() {
			numTablespaces := len(backup.GetTablespaces(connectionPool))
			tablespaceMetadataMap := testutils.DefaultMetadataMap("TABLESPACE", true, true, true)
			tablespaceMetadata := tablespaceMetadataMap[expectedTablespace.GetUniqueID()]
			backup.PrintCreateTablespaceStatements(backupfile, toc, []backup.Tablespace{expectedTablespace}, tablespaceMetadataMap)

			if connectionPool.Version.AtLeast("6") {
				/*
				 * In GPDB 6 and later, a CREATE TABLESPACE statement can't be run in a multi-command string
				 * with other statements, so we execute it separately from the metadata statements.
				 */
				gbuffer := gbytes.BufferWithBytes([]byte(buffer.String()))
				entries, _ := testutils.SliceBufferByEntries(toc.GlobalEntries, gbuffer)
				create, metadata := entries[0], entries[1]
				testhelper.AssertQueryRuns(connectionPool, create)
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLESPACE test_tablespace")
				testhelper.AssertQueryRuns(connectionPool, metadata)
			} else {
				testhelper.AssertQueryRuns(connectionPool, buffer.String())
				defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLESPACE test_tablespace")
			}

			resultTablespaces := backup.GetTablespaces(connectionPool)
			resultMetadataMap := backup.GetMetadataForObjectType(connectionPool, backup.TYPE_TABLESPACE)
			resultMetadata := resultMetadataMap[resultTablespaces[0].GetUniqueID()]
			structmatcher.ExpectStructsToMatchExcluding(&tablespaceMetadata, &resultMetadata, "Oid")
			Expect(resultTablespaces).To(HaveLen(numTablespaces + 1))
			for _, tablespace := range resultTablespaces {
				if tablespace.Tablespace == "test_tablespace" {
					structmatcher.ExpectStructsToMatchExcluding(&expectedTablespace, &tablespace, "Oid")
					return
				}
			}
			Fail("Tablespace 'test_tablespace' was not created")
		})
	})
})
