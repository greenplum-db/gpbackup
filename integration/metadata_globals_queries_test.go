package integration

import (
	"sort"

	"github.com/greenplum-db/gp-common-go-libs/structmatcher"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup integration tests", func() {
	Describe("GetSessionGUCs", func() {
		It("returns a slice of values for session level GUCs", func() {
			/*
			 * We shouldn't need to run any setup queries, because we're using
			 * the default values of these GUCs.
			 */
			results := backup.GetSessionGUCs(connection)
			Expect(results.ClientEncoding).To(Equal("UTF8"))
		})
	})
	Describe("GetDatabaseGUCs", func() {
		It("returns a slice of values for database level GUCs", func() {
			testhelper.AssertQueryRuns(connection, "ALTER DATABASE testdb SET default_with_oids TO true")
			defer testhelper.AssertQueryRuns(connection, "ALTER DATABASE testdb RESET default_with_oids")
			testhelper.AssertQueryRuns(connection, "ALTER DATABASE testdb SET search_path TO public,pg_catalog")
			defer testhelper.AssertQueryRuns(connection, "ALTER DATABASE testdb RESET search_path")
			testhelper.AssertQueryRuns(connection, "ALTER DATABASE testdb SET lc_time TO 'C'")
			defer testhelper.AssertQueryRuns(connection, "ALTER DATABASE testdb RESET lc_time")
			results := backup.GetDatabaseGUCs(connection)
			Expect(len(results)).To(Equal(3))
			Expect(results[0]).To(Equal(`SET default_with_oids TO 'true'`))
			Expect(results[1]).To(Equal("SET search_path TO public, pg_catalog"))
			Expect(results[2]).To(Equal(`SET lc_time TO 'C'`))
		})
	})
	Describe("GetDatabaseInfo", func() {
		It("returns a database info struct", func() {
			if connection.Version.Before("6") {
				testhelper.AssertQueryRuns(connection, "CREATE TABLESPACE test_tablespace FILESPACE test_dir")
			} else {
				testhelper.AssertQueryRuns(connection, "CREATE TABLESPACE test_tablespace LOCATION '/tmp/test_dir'")
			}
			defer testhelper.AssertQueryRuns(connection, "DROP TABLESPACE test_tablespace")

			result := backup.GetDatabaseInfo(connection)

			testdbExpected := backup.Database{Oid: 0, Name: "testdb", Tablespace: "pg_default", Encoding: "UTF8"}
			structmatcher.ExpectStructsToMatchExcluding(&testdbExpected, &result, "Oid", "Collate", "CType")
		})
	})
	Describe("GetResourceQueues", func() {
		It("returns a slice for a resource queue with only ACTIVE_STATEMENTS", func() {
			testhelper.AssertQueryRuns(connection, `CREATE RESOURCE QUEUE "statementsQueue" WITH (ACTIVE_STATEMENTS=7);`)
			defer testhelper.AssertQueryRuns(connection, `DROP RESOURCE QUEUE "statementsQueue"`)

			results := backup.GetResourceQueues(connection)

			statementsQueue := backup.ResourceQueue{Oid: 1, Name: `"statementsQueue"`, ActiveStatements: 7, MaxCost: "-1.00", CostOvercommit: false, MinCost: "0.00", Priority: "medium", MemoryLimit: "-1"}

			//Since resource queues are global, we can't be sure this is the only one
			for _, resultQueue := range results {
				if resultQueue.Name == `"statementsQueue"` {
					structmatcher.ExpectStructsToMatchExcluding(&statementsQueue, &resultQueue, "Oid")
					return
				}
			}
			Fail("Resource queue 'statementsQueue' was not found.")
		})
		It("returns a slice for a resource queue with only MAX_COST", func() {
			testhelper.AssertQueryRuns(connection, `CREATE RESOURCE QUEUE "maxCostQueue" WITH (MAX_COST=32.8);`)
			defer testhelper.AssertQueryRuns(connection, `DROP RESOURCE QUEUE "maxCostQueue"`)

			results := backup.GetResourceQueues(connection)

			maxCostQueue := backup.ResourceQueue{Oid: 1, Name: `"maxCostQueue"`, ActiveStatements: -1, MaxCost: "32.80", CostOvercommit: false, MinCost: "0.00", Priority: "medium", MemoryLimit: "-1"}

			for _, resultQueue := range results {
				if resultQueue.Name == `"maxCostQueue"` {
					structmatcher.ExpectStructsToMatchExcluding(&maxCostQueue, &resultQueue, "Oid")
					return
				}
			}
			Fail("Resource queue 'maxCostQueue' was not found.")
		})
		It("returns a slice for a resource queue with everything", func() {
			testhelper.AssertQueryRuns(connection, `CREATE RESOURCE QUEUE "everyQueue" WITH (ACTIVE_STATEMENTS=7, MAX_COST=3e+4, COST_OVERCOMMIT=TRUE, MIN_COST=22.53, PRIORITY=LOW, MEMORY_LIMIT='2GB');`)
			defer testhelper.AssertQueryRuns(connection, `DROP RESOURCE QUEUE "everyQueue"`)

			results := backup.GetResourceQueues(connection)

			everyQueue := backup.ResourceQueue{Oid: 1, Name: `"everyQueue"`, ActiveStatements: 7, MaxCost: "30000.00", CostOvercommit: true, MinCost: "22.53", Priority: "low", MemoryLimit: "2GB"}

			for _, resultQueue := range results {
				if resultQueue.Name == `"everyQueue"` {
					structmatcher.ExpectStructsToMatchExcluding(&everyQueue, &resultQueue, "Oid")
					return
				}
			}
			Fail("Resource queue 'everyQueue' was not found.")
		})

	})
	Describe("GetResourceGroups", func() {
		BeforeEach(func() {
			testutils.SkipIfBefore5(connection)
		})
		It("returns a slice for a resource group with everything", func() {
			testhelper.AssertQueryRuns(connection, `CREATE RESOURCE GROUP "someGroup" WITH (CPU_RATE_LIMIT=10, MEMORY_LIMIT=20, MEMORY_SHARED_QUOTA=25, MEMORY_SPILL_RATIO=30, CONCURRENCY=15);`)
			defer testhelper.AssertQueryRuns(connection, `DROP RESOURCE GROUP "someGroup"`)

			results := backup.GetResourceGroups(connection)

			someGroup := backup.ResourceGroup{Oid: 1, Name: `"someGroup"`, CPURateLimit: 10, MemoryLimit: 20, Concurrency: 15, MemorySharedQuota: 25, MemorySpillRatio: 30, MemoryAuditor: 0, Cpuset: "-1"}

			for _, resultGroup := range results {
				if resultGroup.Name == `"someGroup"` {
					structmatcher.ExpectStructsToMatchExcluding(&someGroup, &resultGroup, "Oid")
					return
				}
			}
			Fail("Resource group 'someGroup' was not found.")
		})
		It("returns a slice for a resource group with memory_auditor=vmtracker", func() {
			testhelper.AssertQueryRuns(connection, `CREATE RESOURCE GROUP "someGroup" WITH (CPU_RATE_LIMIT=10, MEMORY_LIMIT=20, MEMORY_SHARED_QUOTA=25, MEMORY_SPILL_RATIO=30, CONCURRENCY=0, MEMORY_AUDITOR=vmtracker);`)
			defer testhelper.AssertQueryRuns(connection, `DROP RESOURCE GROUP "someGroup"`)

			results := backup.GetResourceGroups(connection)

			someGroup := backup.ResourceGroup{Oid: 1, Name: `"someGroup"`, CPURateLimit: 10, MemoryLimit: 20, Concurrency: 0, MemorySharedQuota: 25, MemorySpillRatio: 30, MemoryAuditor: 0, Cpuset: "-1"}

			for _, resultGroup := range results {
				if resultGroup.Name == `"someGroup"` {
					structmatcher.ExpectStructsToMatchExcluding(&someGroup, &resultGroup, "Oid")
					return
				}
			}
			Fail("Resource group 'someGroup' was not found.")
		})
	})
	Describe("GetDatabaseRoles", func() {
		It("returns a role with default properties", func() {
			testhelper.AssertQueryRuns(connection, "CREATE ROLE role1 SUPERUSER NOINHERIT")
			defer testhelper.AssertQueryRuns(connection, "DROP ROLE role1")

			results := backup.GetRoles(connection)

			roleOid := testutils.OidFromObjectName(connection, "", "role1", backup.TYPE_ROLE)
			expectedRole := backup.Role{
				Oid:             roleOid,
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
				ResGroup:        "admin_group",
				Createrexthttp:  false,
				Createrextgpfd:  false,
				Createwextgpfd:  false,
				Createrexthdfs:  false,
				Createwexthdfs:  false,
				TimeConstraints: nil,
			}
			if connection.Version.Before("5") {
				expectedRole.ResGroup = ""
			}
			for _, role := range results {
				if role.Name == "role1" {
					structmatcher.ExpectStructsToMatch(&expectedRole, role)
					return
				}
			}
			Fail("Role 'role1' was not found")
		})
		It("returns a role with all properties specified", func() {
			testhelper.AssertQueryRuns(connection, "CREATE ROLE role1")
			defer testhelper.AssertQueryRuns(connection, "DROP ROLE role1")
			testhelper.AssertQueryRuns(connection, `
ALTER ROLE role1 WITH NOSUPERUSER INHERIT CREATEROLE CREATEDB LOGIN
CONNECTION LIMIT 4 PASSWORD 'swordfish' VALID UNTIL '2099-01-01 00:00:00-08'
CREATEEXTTABLE (protocol='http')
CREATEEXTTABLE (protocol='gpfdist', type='readable')
CREATEEXTTABLE (protocol='gpfdist', type='writable')
CREATEEXTTABLE (protocol='gphdfs', type='readable')
CREATEEXTTABLE (protocol='gphdfs', type='writable')`)
			testhelper.AssertQueryRuns(connection, "ALTER ROLE role1 DENY BETWEEN DAY 'Sunday' TIME '1:30 PM' AND DAY 'Wednesday' TIME '14:30:00'")
			testhelper.AssertQueryRuns(connection, "ALTER ROLE role1 DENY DAY 'Friday'")
			testhelper.AssertQueryRuns(connection, "COMMENT ON ROLE role1 IS 'this is a role comment'")

			results := backup.GetRoles(connection)

			roleOid := testutils.OidFromObjectName(connection, "", "role1", backup.TYPE_ROLE)
			expectedRole := backup.Role{
				Oid:             roleOid,
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

			if connection.Version.Before("5") {
				expectedRole.ResGroup = ""
			}

			for _, role := range results {
				if role.Name == "role1" {
					structmatcher.ExpectStructsToMatchExcluding(&expectedRole, role, "TimeConstraints.Oid")
					return
				}
			}
			Fail("Role 'role1' was not found")
		})
	})
	Describe("GetRoleMembers", func() {
		BeforeEach(func() {
			testhelper.AssertQueryRuns(connection, `CREATE ROLE usergroup`)
			testhelper.AssertQueryRuns(connection, `CREATE ROLE testuser`)
		})
		AfterEach(func() {
			defer testhelper.AssertQueryRuns(connection, `DROP ROLE usergroup`)
			defer testhelper.AssertQueryRuns(connection, `DROP ROLE testuser`)
		})
		It("returns a role without ADMIN OPTION", func() {
			testhelper.AssertQueryRuns(connection, "GRANT usergroup TO testuser")
			expectedRoleMember := backup.RoleMember{Role: "usergroup", Member: "testuser", Grantor: "testrole", IsAdmin: false}

			roleMembers := backup.GetRoleMembers(connection)

			for _, roleMember := range roleMembers {
				if roleMember.Role == "usergroup" {
					structmatcher.ExpectStructsToMatch(&expectedRoleMember, &roleMember)
					return
				}
			}
			Fail("Role 'testuser' is not a member of role 'usergroup'")
		})
		It("returns a role WITH ADMIN OPTION", func() {
			testhelper.AssertQueryRuns(connection, "GRANT usergroup TO testuser WITH ADMIN OPTION GRANTED BY testrole")
			expectedRoleMember := backup.RoleMember{Role: "usergroup", Member: "testuser", Grantor: "testrole", IsAdmin: true}

			roleMembers := backup.GetRoleMembers(connection)

			for _, roleMember := range roleMembers {
				if roleMember.Role == "usergroup" {
					structmatcher.ExpectStructsToMatch(&expectedRoleMember, &roleMember)
					return
				}
			}
			Fail("Role 'testuser' is not a member of role 'usergroup'")
		})
		It("returns properly quoted roles in GRANT statement", func() {
			testhelper.AssertQueryRuns(connection, `CREATE ROLE "1testrole" SUPERUSER`)
			defer testhelper.AssertQueryRuns(connection, `DROP ROLE "1testrole"`)
			testhelper.AssertQueryRuns(connection, `SET ROLE "1testrole"`)
			defer testhelper.AssertQueryRuns(connection, `SET ROLE testrole`)
			testhelper.AssertQueryRuns(connection, `CREATE ROLE "1usergroup"`)
			defer testhelper.AssertQueryRuns(connection, `DROP ROLE "1usergroup"`)
			testhelper.AssertQueryRuns(connection, `CREATE ROLE "1testuser"`)
			defer testhelper.AssertQueryRuns(connection, `DROP ROLE "1testuser"`)
			testhelper.AssertQueryRuns(connection, `GRANT "1usergroup" TO "1testuser"`)
			expectedRoleMember := backup.RoleMember{Role: `"1usergroup"`, Member: `"1testuser"`, Grantor: `"1testrole"`, IsAdmin: false}

			roleMembers := backup.GetRoleMembers(connection)

			for _, roleMember := range roleMembers {
				if roleMember.Role == `"1usergroup"` {
					structmatcher.ExpectStructsToMatch(&expectedRoleMember, &roleMember)
					return
				}
			}
			Fail(`Role "1testuser" is not a member of role "1usergroup"`)
		})
	})
	Describe("GetRoleGUCs", func() {
		It("returns a slice of values for user level GUCs", func() {
			testhelper.AssertQueryRuns(connection, "CREATE ROLE role1 SUPERUSER NOINHERIT")
			defer testhelper.AssertQueryRuns(connection, "DROP ROLE role1")
			testhelper.AssertQueryRuns(connection, "ALTER ROLE role1 SET search_path TO public")
			testhelper.AssertQueryRuns(connection, "ALTER ROLE role1 SET client_min_messages TO 'info'")
			testhelper.AssertQueryRuns(connection, "ALTER ROLE role1 SET gp_default_storage_options TO 'appendonly=true, compresslevel=6, orientation=row, compresstype=none'")
			results := backup.GetRoleGUCs(connection)
			Expect(len(results)).To(Equal(1))
			roleConfig := results["role1"]
			sort.Strings(roleConfig)
			Expect(roleConfig[0]).To(Equal(`SET client_min_messages TO 'info'`))
			Expect(roleConfig[1]).To(Equal(`SET gp_default_storage_options TO 'appendonly=true, compresslevel=6, orientation=row, compresstype=none'`))
			Expect(roleConfig[2]).To(Equal(`SET search_path TO public`))
		})
	})
	Describe("GetTablespaces", func() {
		It("returns a tablespace", func() {
			var expectedTablespace backup.Tablespace
			if connection.Version.Before("6") {
				testhelper.AssertQueryRuns(connection, "CREATE TABLESPACE test_tablespace FILESPACE test_dir")
				expectedTablespace = backup.Tablespace{Oid: 0, Tablespace: "test_tablespace", FileLocation: "test_dir"}
			} else {
				testhelper.AssertQueryRuns(connection, "CREATE TABLESPACE test_tablespace LOCATION '/tmp/test_dir'")
				expectedTablespace = backup.Tablespace{Oid: 0, Tablespace: "test_tablespace", FileLocation: "'/tmp/test_dir'", SegmentLocations: []string{}}
			}
			defer testhelper.AssertQueryRuns(connection, "DROP TABLESPACE test_tablespace")

			resultTablespaces := backup.GetTablespaces(connection)

			for _, tablespace := range resultTablespaces {
				if tablespace.Tablespace == "test_tablespace" {
					structmatcher.ExpectStructsToMatchExcluding(&expectedTablespace, &tablespace, "Oid")
					return
				}
			}
			Fail("Tablespace 'test_tablespace' was not created")
		})
		It("returns a tablespace with segment locations", func() {
			testutils.SkipIfBefore6(connection)

			testhelper.AssertQueryRuns(connection, "CREATE TABLESPACE test_tablespace LOCATION '/tmp/test_dir' OPTIONS (content0 '/tmp/test_dir1')")
			expectedTablespace := backup.Tablespace{
				Oid: 0, Tablespace: "test_tablespace", FileLocation: "'/tmp/test_dir'",
				SegmentLocations: []string{"content0 '/tmp/test_dir1'"},
			}

			defer testhelper.AssertQueryRuns(connection, "DROP TABLESPACE test_tablespace")

			resultTablespaces := backup.GetTablespaces(connection)

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
