package integration

import (
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
			Expect(results.DefaultWithOids).To(Equal("off"))
		})
	})
	Describe("GetDatabaseGUCs", func() {
		It("returns a slice of values for database level GUCs", func() {
			testutils.AssertQueryRuns(connection, "ALTER DATABASE testdb SET default_with_oids TO true")
			defer testutils.AssertQueryRuns(connection, "ALTER DATABASE testdb SET default_with_oids TO false")
			testutils.AssertQueryRuns(connection, "ALTER DATABASE testdb SET search_path TO public,pg_catalog")
			defer testutils.AssertQueryRuns(connection, "ALTER DATABASE testdb SET search_path TO pg_catalog,public")
			testutils.AssertQueryRuns(connection, "ALTER DATABASE testdb SET lc_time TO 'C'")
			results := backup.GetDatabaseGUCs(connection)
			Expect(len(results)).To(Equal(3))
			Expect(results[0]).To(Equal(`SET default_with_oids TO "true"`))
			Expect(results[1]).To(Equal("SET search_path TO public, pg_catalog"))
			Expect(results[2]).To(Equal(`SET lc_time TO "C"`))
		})
	})
	Describe("GetDatabaseInfo", func() {
		It("returns a database info struct", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLESPACE test_tablespace FILESPACE test_filespace")
			defer testutils.AssertQueryRuns(connection, "DROP TABLESPACE test_tablespace")

			result := backup.GetDatabaseInfo(connection)

			testdbExpected := backup.Database{}
			if connection.Version.AtLeast("6") {
				testdbExpected = backup.Database{Oid: 0, Name: "testdb", Tablespace: "pg_default", Encoding: "UTF8", Collate: "en_US.utf-8", CType: "en_US.utf-8"}
			} else {
				testdbExpected = backup.Database{Oid: 0, Name: "testdb", Tablespace: "pg_default", Encoding: "UTF8", Collate: "", CType: ""}
			}
			testutils.ExpectStructsToMatchExcluding(&testdbExpected, &result, "Oid")
		})
	})
	Describe("GetResourceQueues", func() {
		It("returns a slice for a resource queue with only ACTIVE_STATEMENTS", func() {
			testutils.AssertQueryRuns(connection, `CREATE RESOURCE QUEUE "statementsQueue" WITH (ACTIVE_STATEMENTS=7);`)
			defer testutils.AssertQueryRuns(connection, `DROP RESOURCE QUEUE "statementsQueue"`)

			results := backup.GetResourceQueues(connection)

			statementsQueue := backup.ResourceQueue{Oid: 1, Name: `"statementsQueue"`, ActiveStatements: 7, MaxCost: "-1.00", CostOvercommit: false, MinCost: "0.00", Priority: "medium", MemoryLimit: "-1"}

			//Since resource queues are global, we can't be sure this is the only one
			for _, resultQueue := range results {
				if resultQueue.Name == `"statementsQueue"` {
					testutils.ExpectStructsToMatchExcluding(&statementsQueue, &resultQueue, "Oid")
					return
				}
			}
			Fail("Resource queue 'statementsQueue' was not found.")
		})
		It("returns a slice for a resource queue with only MAX_COST", func() {
			testutils.AssertQueryRuns(connection, `CREATE RESOURCE QUEUE "maxCostQueue" WITH (MAX_COST=32.8);`)
			defer testutils.AssertQueryRuns(connection, `DROP RESOURCE QUEUE "maxCostQueue"`)

			results := backup.GetResourceQueues(connection)

			maxCostQueue := backup.ResourceQueue{Oid: 1, Name: `"maxCostQueue"`, ActiveStatements: -1, MaxCost: "32.80", CostOvercommit: false, MinCost: "0.00", Priority: "medium", MemoryLimit: "-1"}

			for _, resultQueue := range results {
				if resultQueue.Name == `"maxCostQueue"` {
					testutils.ExpectStructsToMatchExcluding(&maxCostQueue, &resultQueue, "Oid")
					return
				}
			}
			Fail("Resource queue 'maxCostQueue' was not found.")
		})
		It("returns a slice for a resource queue with everything", func() {
			testutils.AssertQueryRuns(connection, `CREATE RESOURCE QUEUE "everyQueue" WITH (ACTIVE_STATEMENTS=7, MAX_COST=3e+4, COST_OVERCOMMIT=TRUE, MIN_COST=22.53, PRIORITY=LOW, MEMORY_LIMIT='2GB');`)
			defer testutils.AssertQueryRuns(connection, `DROP RESOURCE QUEUE "everyQueue"`)

			results := backup.GetResourceQueues(connection)

			everyQueue := backup.ResourceQueue{Oid: 1, Name: `"everyQueue"`, ActiveStatements: 7, MaxCost: "30000.00", CostOvercommit: true, MinCost: "22.53", Priority: "low", MemoryLimit: "2GB"}

			for _, resultQueue := range results {
				if resultQueue.Name == `"everyQueue"` {
					testutils.ExpectStructsToMatchExcluding(&everyQueue, &resultQueue, "Oid")
					return
				}
			}
			Fail("Resource queue 'everyQueue' was not found.")
		})

	})
	Describe("GetResourceGroups", func() {
		BeforeEach(func() {
			testutils.SkipIf4(connection)
		})
		It("returns a slice for a resource group with everything", func() {
			testutils.AssertQueryRuns(connection, `CREATE RESOURCE GROUP "someGroup" WITH (CPU_RATE_LIMIT=10, MEMORY_LIMIT=20, MEMORY_SHARED_QUOTA=25, MEMORY_SPILL_RATIO=30, CONCURRENCY=15);`)
			defer testutils.AssertQueryRuns(connection, `DROP RESOURCE GROUP "someGroup"`)

			results := backup.GetResourceGroups(connection)

			someGroup := backup.ResourceGroup{Oid: 1, Name: `"someGroup"`, CPURateLimit: 10, MemoryLimit: 20, Concurrency: 15, MemorySharedQuota: 25, MemorySpillRatio: 30}

			for _, resultGroup := range results {
				if resultGroup.Name == `"someGroup"` {
					testutils.ExpectStructsToMatchExcluding(&someGroup, &resultGroup, "Oid")
					return
				}
			}
			Fail("Resource group 'someGroup' was not found.")
		})
	})
	Describe("GetDatabaseRoles", func() {
		It("returns a role with default properties", func() {
			testutils.AssertQueryRuns(connection, "CREATE ROLE role1 SUPERUSER NOINHERIT")
			defer testutils.AssertQueryRuns(connection, "DROP ROLE role1")

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
					testutils.ExpectStructsToMatch(&expectedRole, role)
					return
				}
			}
			Fail("Role 'role1' was not found")
		})
		It("returns a role with all properties specified", func() {
			testutils.AssertQueryRuns(connection, "CREATE ROLE role1")
			defer testutils.AssertQueryRuns(connection, "DROP ROLE role1")
			testutils.AssertQueryRuns(connection, `
ALTER ROLE role1 WITH NOSUPERUSER INHERIT CREATEROLE CREATEDB LOGIN
CONNECTION LIMIT 4 PASSWORD 'swordfish' VALID UNTIL '2099-01-01 00:00:00-08'
CREATEEXTTABLE (protocol='http')
CREATEEXTTABLE (protocol='gpfdist', type='readable')
CREATEEXTTABLE (protocol='gpfdist', type='writable')
CREATEEXTTABLE (protocol='gphdfs', type='readable')
CREATEEXTTABLE (protocol='gphdfs', type='writable')`)
			testutils.AssertQueryRuns(connection, "ALTER ROLE role1 DENY BETWEEN DAY 'Sunday' TIME '1:30 PM' AND DAY 'Wednesday' TIME '14:30:00'")
			testutils.AssertQueryRuns(connection, "ALTER ROLE role1 DENY DAY 'Friday'")
			testutils.AssertQueryRuns(connection, "COMMENT ON ROLE role1 IS 'this is a role comment'")

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
					testutils.ExpectStructsToMatchExcluding(&expectedRole, role, "TimeConstraints.Oid")
					return
				}
			}
			Fail("Role 'role1' was not found")
		})
	})
	Describe("GetRoleMembers", func() {
		BeforeEach(func() {
			testutils.AssertQueryRuns(connection, `CREATE ROLE usergroup`)
			testutils.AssertQueryRuns(connection, `CREATE ROLE testuser`)
		})
		AfterEach(func() {
			defer testutils.AssertQueryRuns(connection, `DROP ROLE usergroup`)
			defer testutils.AssertQueryRuns(connection, `DROP ROLE testuser`)
		})
		It("returns a role without ADMIN OPTION", func() {
			testutils.AssertQueryRuns(connection, "GRANT usergroup TO testuser")
			expectedRoleMember := backup.RoleMember{Role: "usergroup", Member: "testuser", Grantor: "testrole", IsAdmin: false}

			roleMembers := backup.GetRoleMembers(connection)

			for _, roleMember := range roleMembers {
				if roleMember.Role == "usergroup" {
					testutils.ExpectStructsToMatch(&expectedRoleMember, &roleMember)
					return
				}
			}
			Fail("Role 'testuser' is not a member of role 'usergroup'")
		})
		It("returns a role WITH ADMIN OPTION", func() {
			testutils.AssertQueryRuns(connection, "GRANT usergroup TO testuser WITH ADMIN OPTION GRANTED BY testrole")
			expectedRoleMember := backup.RoleMember{Role: "usergroup", Member: "testuser", Grantor: "testrole", IsAdmin: true}

			roleMembers := backup.GetRoleMembers(connection)

			for _, roleMember := range roleMembers {
				if roleMember.Role == "usergroup" {
					testutils.ExpectStructsToMatch(&expectedRoleMember, &roleMember)
					return
				}
			}
			Fail("Role 'testuser' is not a member of role 'usergroup'")
		})
	})
	Describe("GetTablespaces", func() {
		It("returns a tablespace", func() {
			testutils.AssertQueryRuns(connection, "CREATE TABLESPACE test_tablespace FILESPACE test_filespace")
			defer testutils.AssertQueryRuns(connection, "DROP TABLESPACE test_tablespace")
			expectedTablespace := backup.Tablespace{Oid: 0, Tablespace: "test_tablespace", Filespace: "test_filespace"}

			resultTablespaces := backup.GetTablespaces(connection)

			for _, tablespace := range resultTablespaces {
				if tablespace.Tablespace == "test_tablespace" {
					testutils.ExpectStructsToMatchExcluding(&expectedTablespace, &tablespace, "Oid")
					return
				}
			}
			Fail("Tablespace 'test_tablespace' was not created")
		})
	})
})
