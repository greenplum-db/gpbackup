package integration

import (
	"fmt"

	"github.com/greenplum-db/gp-common-go-libs/structmatcher"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup integration tests", func() {
	Describe("GetSessionGUCs", func() {
		It("returns a slice of values for session level GUCs", func() {
			/*
			 * We shouldn't need to run any setup queries, because we're using
			 * the default values of these GUCs.
			 */
			results := backup.GetSessionGUCs(connectionPool)
			Expect(results.ClientEncoding).To(Equal("UTF8"))
		})
	})
	Describe("GetDatabaseGUCs", func() {
		It("returns a slice of values for database level GUCs", func() {
			testhelper.AssertQueryRuns(connectionPool, "ALTER DATABASE testdb SET enable_nestloop TO true")
			defer testhelper.AssertQueryRuns(connectionPool, "ALTER DATABASE testdb RESET enable_nestloop")
			testhelper.AssertQueryRuns(connectionPool, "ALTER DATABASE testdb SET search_path TO public,pg_catalog")
			defer testhelper.AssertQueryRuns(connectionPool, "ALTER DATABASE testdb RESET search_path")
			testhelper.AssertQueryRuns(connectionPool, "ALTER DATABASE testdb SET lc_time TO 'C'")
			defer testhelper.AssertQueryRuns(connectionPool, "ALTER DATABASE testdb RESET lc_time")
			results := backup.GetDatabaseGUCs(connectionPool)
			Expect(results).To(HaveLen(3))
			Expect(results[0]).To(Equal(`SET enable_nestloop TO 'true'`))
			Expect(results[1]).To(Equal("SET search_path TO public, pg_catalog"))
			Expect(results[2]).To(Equal(`SET lc_time TO 'C'`))
		})
		It("only gets GUCs that are non role specific", func() {
			testutils.SkipIfBefore6(connectionPool)
			testhelper.AssertQueryRuns(connectionPool, "ALTER ROLE testrole IN DATABASE testdb SET enable_nestloop TO true")
			defer testhelper.AssertQueryRuns(connectionPool, "ALTER ROLE testrole IN DATABASE testdb RESET enable_nestloop")
			testhelper.AssertQueryRuns(connectionPool, "ALTER DATABASE testdb SET enable_nestloop TO false")
			defer testhelper.AssertQueryRuns(connectionPool, "ALTER DATABASE testdb RESET enable_nestloop")
			results := backup.GetDatabaseGUCs(connectionPool)
			Expect(results).To(HaveLen(1))
			Expect(results[0]).To(Equal(`SET enable_nestloop TO 'false'`))
		})
	})
	Describe("GetDefaultDatabaseEncodingInfo", func() {
		It("queries default values from template0", func() {
			result := backup.GetDefaultDatabaseEncodingInfo(connectionPool)

			Expect(result.Name).To(Equal("template0"))
			Expect(result.Encoding).To(Equal("UTF8"))
			if connectionPool.Version.AtLeast("6") {
				/*
				 * These values are slightly different between mac and linux
				 * so we use a regexp to match them
				 */
				Expect(result.Collate).To(MatchRegexp("en_US.(utf|UTF)-?8"))
				Expect(result.CType).To(MatchRegexp("en_US.(utf|UTF)-?8"))
			}
		})
	})
	Describe("GetDatabaseInfo", func() {
		It("returns a database info struct for a basic database", func() {
			result := backup.GetDatabaseInfo(connectionPool)

			testdbExpected := backup.Database{Oid: 0, Name: "testdb", Tablespace: "pg_default", Encoding: "UTF8"}
			structmatcher.ExpectStructsToMatchExcluding(&testdbExpected, &result, "Oid", "Collate", "CType")
		})
		It("returns a database info struct for a complex database", func() {
			var expectedDB backup.Database
			if connectionPool.Version.Before("6") {
				testhelper.AssertQueryRuns(connectionPool, "CREATE DATABASE create_test_db ENCODING 'UTF8' TEMPLATE template0")
				expectedDB = backup.Database{Oid: 1, Name: "create_test_db", Tablespace: "pg_default", Encoding: "UTF8", Collate: "", CType: ""}
			} else {
				testhelper.AssertQueryRuns(connectionPool, "CREATE DATABASE create_test_db ENCODING 'UTF8' LC_COLLATE 'en_US.utf-8' LC_CTYPE 'en_US.utf-8' TEMPLATE template0")
				expectedDB = backup.Database{Oid: 1, Name: "create_test_db", Tablespace: "pg_default", Encoding: "UTF8", Collate: "en_US.utf-8", CType: "en_US.utf-8"}
			}
			defer testhelper.AssertQueryRuns(connectionPool, "DROP DATABASE create_test_db")

			connectionPool.DBName = `create_test_db`
			result := backup.GetDatabaseInfo(connectionPool)
			connectionPool.DBName = `testdb`

			structmatcher.ExpectStructsToMatchExcluding(&expectedDB, &result, "Oid")
		})
		It("returns a database info struct if database contains single quote", func() {
			testhelper.AssertQueryRuns(connectionPool, `CREATE DATABASE "test'db"`)
			defer testhelper.AssertQueryRuns(connectionPool, `DROP DATABASE "test'db"`)
			connectionPool.DBName = `test'db`
			result := backup.GetDatabaseInfo(connectionPool)
			connectionPool.DBName = `testdb`

			testdbExpected := backup.Database{Oid: 0, Name: `"test'db"`, Tablespace: "pg_default", Encoding: "UTF8"}
			structmatcher.ExpectStructsToMatchExcluding(&testdbExpected, &result, "Oid", "Collate", "CType")
		})
	})
	Describe("GetResourceQueues", func() {
		It("returns a slice for a resource queue with only ACTIVE_STATEMENTS", func() {
			testhelper.AssertQueryRuns(connectionPool, `CREATE RESOURCE QUEUE "statementsQueue" WITH (ACTIVE_STATEMENTS=7);`)
			defer testhelper.AssertQueryRuns(connectionPool, `DROP RESOURCE QUEUE "statementsQueue"`)

			results := backup.GetResourceQueues(connectionPool)

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
			testhelper.AssertQueryRuns(connectionPool, `CREATE RESOURCE QUEUE "maxCostQueue" WITH (MAX_COST=32.8);`)
			defer testhelper.AssertQueryRuns(connectionPool, `DROP RESOURCE QUEUE "maxCostQueue"`)

			results := backup.GetResourceQueues(connectionPool)

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
			testhelper.AssertQueryRuns(connectionPool, `CREATE RESOURCE QUEUE "everyQueue" WITH (ACTIVE_STATEMENTS=7, MAX_COST=3e+4, COST_OVERCOMMIT=TRUE, MIN_COST=22.53, PRIORITY=LOW, MEMORY_LIMIT='2GB');`)
			defer testhelper.AssertQueryRuns(connectionPool, `DROP RESOURCE QUEUE "everyQueue"`)

			results := backup.GetResourceQueues(connectionPool)

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
		It("returns a slice for a resource group with everything", func() {
			if connectionPool.Version.Before("7") {
				testhelper.AssertQueryRuns(connectionPool, `CREATE RESOURCE GROUP "someGroup" WITH (CPU_RATE_LIMIT=10, MEMORY_LIMIT=20, MEMORY_SHARED_QUOTA=25, MEMORY_SPILL_RATIO=30, CONCURRENCY=15);`)
				defer testhelper.AssertQueryRuns(connectionPool, `DROP RESOURCE GROUP "someGroup"`)

				results := backup.GetResourceGroups[backup.ResourceGroupBefore7](connectionPool)

				someGroup := backup.ResourceGroupBefore7{ResourceGroup: backup.ResourceGroup{Oid: 1, Name: `"someGroup"`, Concurrency: "15", Cpuset: "-1"},
					CPURateLimit: "10", MemoryLimit: "20", MemorySharedQuota: "25", MemorySpillRatio: "30", MemoryAuditor: "0"}

				for _, resultGroup := range results {
					if resultGroup.Name == `"someGroup"` {
						structmatcher.ExpectStructsToMatchExcluding(&someGroup, &resultGroup, "ResourceGroup.Oid")
						return
					}
				}
			} else { // GPDB7+
				testhelper.AssertQueryRuns(connectionPool, `CREATE RESOURCE GROUP someGroup WITH (CPU_MAX_PERCENT=10, CPU_WEIGHT=100, CONCURRENCY=15);`)
				defer testhelper.AssertQueryRuns(connectionPool, `DROP RESOURCE GROUP someGroup`)

				results := backup.GetResourceGroups[backup.ResourceGroupAtLeast7](connectionPool)

				someGroup := backup.ResourceGroupAtLeast7{ResourceGroup: backup.ResourceGroup{Oid: 1, Name: `somegroup`, Concurrency: "15", Cpuset: "-1"},
					CpuMaxPercent: "10", CpuWeight: "100"}

				for _, resultGroup := range results {
					if resultGroup.Name == `somegroup` {
						structmatcher.ExpectStructsToMatchExcluding(&someGroup, &resultGroup, "ResourceGroup.Oid")
						return
					}
				}
			}
			Fail("Resource group 'someGroup' was not found.")
		})
		It("returns a slice for a resource group with memory_auditor=vmtracker", func() {
			if connectionPool.Version.AtLeast("7") {
				Skip("Test only applicable to GPDB6 and earlier")
			}
			testhelper.AssertQueryRuns(connectionPool, `CREATE RESOURCE GROUP "someGroup" WITH (CPU_RATE_LIMIT=10, MEMORY_LIMIT=20, MEMORY_SHARED_QUOTA=25, MEMORY_SPILL_RATIO=30, CONCURRENCY=0, MEMORY_AUDITOR=vmtracker);`)
			defer testhelper.AssertQueryRuns(connectionPool, `DROP RESOURCE GROUP "someGroup"`)

			results := backup.GetResourceGroups[backup.ResourceGroupBefore7](connectionPool)

			someGroup := backup.ResourceGroupBefore7{ResourceGroup: backup.ResourceGroup{Oid: 1, Name: `"someGroup"`, Concurrency: "0", Cpuset: "-1"},
				CPURateLimit: "10", MemoryLimit: "20", MemorySharedQuota: "25", MemorySpillRatio: "30", MemoryAuditor: "0"}

			for _, resultGroup := range results {
				if resultGroup.Name == `"someGroup"` {
					structmatcher.ExpectStructsToMatchExcluding(&someGroup, &resultGroup, "ResourceGroup.Oid")
					return
				}
			}
			Fail("Resource group 'someGroup' was not found.")
		})
		It("returns a resource group with defaults", func() {
			if connectionPool.Version.Before("7") {
				testhelper.AssertQueryRuns(connectionPool, `CREATE RESOURCE GROUP "someGroup" WITH (CPU_RATE_LIMIT=10, MEMORY_LIMIT=20);`)
				defer testhelper.AssertQueryRuns(connectionPool, `DROP RESOURCE GROUP "someGroup"`)

				results := backup.GetResourceGroups[backup.ResourceGroupBefore7](connectionPool)

				expectedDefaults := backup.ResourceGroupBefore7{ResourceGroup: backup.ResourceGroup{Oid: 1, Name: `"someGroup"`, Concurrency: concurrencyDefault, Cpuset: cpuSetDefault},
					CPURateLimit: "10", MemoryLimit: "20", MemorySharedQuota: memSharedDefault, MemorySpillRatio: memSpillDefault, MemoryAuditor: memAuditDefault}

				for _, resultGroup := range results {
					if resultGroup.Name == `"someGroup"` {
						structmatcher.ExpectStructsToMatchExcluding(&expectedDefaults, &resultGroup, "ResourceGroup.Oid")
						return
					}
				}
			} else {
				testhelper.AssertQueryRuns(connectionPool, `CREATE RESOURCE GROUP someGroup WITH (CPU_MAX_PERCENT=10, CPU_WEIGHT=100);`)
				defer testhelper.AssertQueryRuns(connectionPool, `DROP RESOURCE GROUP someGroup`)

				results := backup.GetResourceGroups[backup.ResourceGroupAtLeast7](connectionPool)

				expectedDefaults := backup.ResourceGroupAtLeast7{ResourceGroup: backup.ResourceGroup{Oid: 1, Name: `somegroup`, Concurrency: concurrencyDefault, Cpuset: cpuSetDefault},
					CpuMaxPercent: "10", CpuWeight: "100"}

				for _, resultGroup := range results {
					if resultGroup.Name == `somegroup` {
						structmatcher.ExpectStructsToMatchExcluding(&expectedDefaults, &resultGroup, "ResourceGroup.Oid")
						return
					}
				}
			}
			Fail("Resource group 'someGroup' was not found.")
		})
	})
	Describe("GetDatabaseRoles", func() {
		It("returns a role with default properties", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE ROLE role1 SUPERUSER NOINHERIT")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP ROLE role1")

			results := backup.GetRoles(connectionPool)

			roleOid := testutils.OidFromObjectName(connectionPool, "", "role1", backup.TYPE_ROLE)
			expectedRole := backup.Role{
				Oid:             roleOid,
				Name:            "role1",
				Super:           true,
				Inherit:         false,
				CreateRole:      false,
				CreateDB:        false,
				CanLogin:        false,
				Replication:     false,
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
			for _, role := range results {
				if role.Name == "role1" {
					structmatcher.ExpectStructsToMatch(&expectedRole, role)
					return
				}
			}
			Fail("Role 'role1' was not found")
		})
		It("returns a role with all properties specified", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE ROLE role1")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP ROLE role1")
			setupQuery := `
ALTER ROLE role1 WITH NOSUPERUSER INHERIT CREATEROLE CREATEDB LOGIN
CONNECTION LIMIT 4 PASSWORD 'swordfish' VALID UNTIL '2099-01-01 00:00:00-08'
CREATEEXTTABLE (protocol='http')
CREATEEXTTABLE (protocol='gpfdist', type='readable')
CREATEEXTTABLE (protocol='gpfdist', type='writable')`
			if connectionPool.Version.Before("6") {
				setupQuery += `
CREATEEXTTABLE (protocol='gphdfs', type='readable')
CREATEEXTTABLE (protocol='gphdfs', type='writable')`
			}
			testhelper.AssertQueryRuns(connectionPool, setupQuery)
			testhelper.AssertQueryRuns(connectionPool, "ALTER ROLE role1 DENY BETWEEN DAY 'Sunday' TIME '1:30 PM' AND DAY 'Wednesday' TIME '14:30:00'")
			testhelper.AssertQueryRuns(connectionPool, "ALTER ROLE role1 DENY DAY 'Friday'")
			testhelper.AssertQueryRuns(connectionPool, "COMMENT ON ROLE role1 IS 'this is a role comment'")

			results := backup.GetRoles(connectionPool)

			roleOid := testutils.OidFromObjectName(connectionPool, "", "role1", backup.TYPE_ROLE)
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
				ResGroup:        "",
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

			expectedRole.ResGroup = "default_group"

			if connectionPool.Version.AtLeast("6") {
				expectedRole.Createrexthdfs = false
				expectedRole.Createwexthdfs = false
			}

			for _, role := range results {
				if role.Name == "role1" {
					structmatcher.ExpectStructsToMatchExcluding(&expectedRole, role, "TimeConstraints.Oid")
					return
				}
			}
			Fail("Role 'role1' was not found")
		})
		It("returns a role with replication", func() {
			testutils.SkipIfBefore6(connectionPool)

			testhelper.AssertQueryRuns(connectionPool, "CREATE ROLE role1 WITH SUPERUSER NOINHERIT REPLICATION")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP ROLE role1")

			results := backup.GetRoles(connectionPool)

			roleOid := testutils.OidFromObjectName(connectionPool, "", "role1", backup.TYPE_ROLE)
			expectedRole := backup.Role{
				Oid:             roleOid,
				Name:            "role1",
				Super:           true,
				Inherit:         false,
				CreateRole:      false,
				CreateDB:        false,
				CanLogin:        false,
				Replication:     true,
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

			for _, role := range results {
				if role.Name == "role1" {
					structmatcher.ExpectStructsToMatch(&expectedRole, role)
					return
				}
			}
			Fail("Role 'role1' was not found")
		})
		It("returns a role with inf/-inf ValidUntil field", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE ROLE role1inf WITH VALID UNTIL 'infinity' SUPERUSER NOINHERIT")
			testhelper.AssertQueryRuns(connectionPool, "CREATE ROLE role1neginf WITH VALID UNTIL '-infinity' SUPERUSER NOINHERIT")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP ROLE role1inf")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP ROLE role1neginf")

			results := backup.GetRoles(connectionPool)

			expectedRoleInf := backup.Role{
				Oid:             0,
				Name:            "role1inf",
				Super:           true,
				Inherit:         false,
				CreateRole:      false,
				CreateDB:        false,
				CanLogin:        false,
				Replication:     false,
				ConnectionLimit: -1,
				Password:        "",
				ValidUntil:      "infinity",
				ResQueue:        "pg_default",
				ResGroup:        "",
				Createrexthttp:  false,
				Createrextgpfd:  false,
				Createwextgpfd:  false,
				Createrexthdfs:  false,
				Createwexthdfs:  false,
				TimeConstraints: nil,
			}

			expectedRoleNegInf := backup.Role{
				Oid:             0,
				Name:            "role1neginf",
				Super:           true,
				Inherit:         false,
				CreateRole:      false,
				CreateDB:        false,
				CanLogin:        false,
				Replication:     false,
				ConnectionLimit: -1,
				Password:        "",
				ValidUntil:      "-infinity",
				ResQueue:        "pg_default",
				ResGroup:        "",
				Createrexthttp:  false,
				Createrextgpfd:  false,
				Createwextgpfd:  false,
				Createrexthdfs:  false,
				Createwexthdfs:  false,
				TimeConstraints: nil,
			}
			var foundRoles int
			for _, role := range results {
				if role.Name == "role1inf" {
					structmatcher.ExpectStructsToMatchExcluding(&expectedRoleInf, role, "Oid", "ResGroup")
					foundRoles++
				} else if role.Name == "role1neginf" {
					structmatcher.ExpectStructsToMatchExcluding(&expectedRoleNegInf, role, "Oid", "ResGroup")
					foundRoles++
				}
			}
			if foundRoles != 2 {
				Fail("Role 'role1inf' or 'role1neginf' was not found")
			}
		})
		It("handles implicit cast of timestamp to text", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE ROLE role1 SUPERUSER NOINHERIT")
			testhelper.AssertQueryRuns(connectionPool, "CREATE OR REPLACE FUNCTION pg_catalog.text(timestamp without time zone) RETURNS text STRICT IMMUTABLE LANGUAGE SQL AS 'SELECT textin(timestamp_out($1));';")
			testhelper.AssertQueryRuns(connectionPool, "CREATE CAST (timestamp without time zone AS text) WITH FUNCTION pg_catalog.text(timestamp without time zone) AS IMPLICIT;")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP FUNCTION pg_catalog.text(timestamp without time zone) CASCADE;")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP ROLE role1")

			results := backup.GetRoles(connectionPool)

			roleOid := testutils.OidFromObjectName(connectionPool, "", "role1", backup.TYPE_ROLE)
			expectedRole := backup.Role{
				Oid:             roleOid,
				Name:            "role1",
				Super:           true,
				Inherit:         false,
				CreateRole:      false,
				CreateDB:        false,
				CanLogin:        false,
				Replication:     false,
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
			for _, role := range results {
				if role.Name == "role1" {
					structmatcher.ExpectStructsToMatch(&expectedRole, role)
					return
				}
			}
			Fail("Role 'role1' was not found")

		})
	})
	Describe("GetRoleMembers", func() {
		BeforeEach(func() {
			testhelper.AssertQueryRuns(connectionPool, `CREATE ROLE usergroup`)
			testhelper.AssertQueryRuns(connectionPool, `CREATE ROLE testuser`)
		})
		AfterEach(func() {
			defer testhelper.AssertQueryRuns(connectionPool, `DROP ROLE usergroup`)
			defer testhelper.AssertQueryRuns(connectionPool, `DROP ROLE testuser`)
		})
		It("returns a role without ADMIN OPTION", func() {
			testhelper.AssertQueryRuns(connectionPool, "GRANT usergroup TO testuser")
			expectedRoleMember := backup.RoleMember{Role: "usergroup", Member: "testuser", Grantor: "testrole", IsAdmin: false}

			roleMembers := backup.GetRoleMembers(connectionPool)

			for _, roleMember := range roleMembers {
				if roleMember.Role == "usergroup" {
					structmatcher.ExpectStructsToMatch(&expectedRoleMember, &roleMember)
					return
				}
			}
			Fail("Role 'testuser' is not a member of role 'usergroup'")
		})
		It("returns a role WITH ADMIN OPTION", func() {
			testhelper.AssertQueryRuns(connectionPool, "GRANT usergroup TO testuser WITH ADMIN OPTION GRANTED BY testrole")
			expectedRoleMember := backup.RoleMember{Role: "usergroup", Member: "testuser", Grantor: "testrole", IsAdmin: true}

			roleMembers := backup.GetRoleMembers(connectionPool)

			for _, roleMember := range roleMembers {
				if roleMember.Role == "usergroup" {
					structmatcher.ExpectStructsToMatch(&expectedRoleMember, &roleMember)
					return
				}
			}
			Fail("Role 'testuser' is not a member of role 'usergroup'")
		})
		It("returns properly quoted roles in GRANT statement", func() {
			testhelper.AssertQueryRuns(connectionPool, `CREATE ROLE "1testrole" SUPERUSER`)
			defer testhelper.AssertQueryRuns(connectionPool, `DROP ROLE "1testrole"`)
			testhelper.AssertQueryRuns(connectionPool, `SET ROLE "1testrole"`)
			defer testhelper.AssertQueryRuns(connectionPool, `SET ROLE testrole`)
			testhelper.AssertQueryRuns(connectionPool, `CREATE ROLE "1usergroup"`)
			defer testhelper.AssertQueryRuns(connectionPool, `DROP ROLE "1usergroup"`)
			testhelper.AssertQueryRuns(connectionPool, `CREATE ROLE "1testuser"`)
			defer testhelper.AssertQueryRuns(connectionPool, `DROP ROLE "1testuser"`)
			testhelper.AssertQueryRuns(connectionPool, `GRANT "1usergroup" TO "1testuser"`)
			expectedRoleMember := backup.RoleMember{Role: `"1usergroup"`, Member: `"1testuser"`, Grantor: `"1testrole"`, IsAdmin: false}

			roleMembers := backup.GetRoleMembers(connectionPool)

			for _, roleMember := range roleMembers {
				if roleMember.Role == `"1usergroup"` {
					structmatcher.ExpectStructsToMatch(&expectedRoleMember, &roleMember)
					return
				}
			}
			Fail(`Role "1testuser" is not a member of role "1usergroup"`)
		})
		It("handles dropped granter", func() {
			testhelper.AssertQueryRuns(connectionPool, `CREATE ROLE testdropgranter_role`)
			defer testhelper.AssertQueryRuns(connectionPool, `DROP ROLE testdropgranter_role`)
			testhelper.AssertQueryRuns(connectionPool, `CREATE ROLE testdropgranter_member`)
			defer testhelper.AssertQueryRuns(connectionPool, `DROP ROLE testdropgranter_member`)
			testhelper.AssertQueryRuns(connectionPool, `CREATE ROLE testdropgranter_granter`)
			testhelper.AssertQueryRuns(connectionPool, `GRANT testdropgranter_role TO testdropgranter_member GRANTED BY testdropgranter_granter`)
			testhelper.AssertQueryRuns(connectionPool, `DROP ROLE testdropgranter_granter`)
			expectedRoleMember := backup.RoleMember{Role: `testdropgranter_role`, Member: `testdropgranter_member`, Grantor: ``, IsAdmin: false}

			roleMember := backup.GetRoleMembers(connectionPool)
			Expect(len(roleMember)).To(Equal(1))
			structmatcher.ExpectStructsToMatch(&expectedRoleMember, &roleMember[0])
		})
		It("handles implicit cast of oid to text", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE OR REPLACE FUNCTION pg_catalog.text(oid) RETURNS text STRICT IMMUTABLE LANGUAGE SQL AS 'SELECT textin(oidout($1));';")
			testhelper.AssertQueryRuns(connectionPool, "CREATE CAST (oid AS text) WITH FUNCTION pg_catalog.text(oid) AS IMPLICIT;")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP FUNCTION pg_catalog.text(oid) CASCADE;")
			testhelper.AssertQueryRuns(connectionPool, "GRANT usergroup TO testuser")
			expectedRoleMember := backup.RoleMember{Role: "usergroup", Member: "testuser", Grantor: "testrole", IsAdmin: false}

			roleMembers := backup.GetRoleMembers(connectionPool)

			for _, roleMember := range roleMembers {
				if roleMember.Role == "usergroup" {
					structmatcher.ExpectStructsToMatch(&expectedRoleMember, &roleMember)
					return
				}
			}
			Fail("Role 'testuser' is not a member of role 'usergroup'")
		})
	})
	Describe("GetRoleGUCs", func() {
		It("returns a slice of values for user level GUCs", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE ROLE role1 SUPERUSER NOINHERIT")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP ROLE role1")
			testhelper.AssertQueryRuns(connectionPool, "ALTER ROLE role1 SET search_path TO public")
			testhelper.AssertQueryRuns(connectionPool, "ALTER ROLE role1 SET client_min_messages TO 'info'")

			defaultStorageOptionsString := "appendonly=true, compresslevel=6, orientation=row, compresstype=none"
			if connectionPool.Version.AtLeast("7") {
				defaultStorageOptionsString = "compresslevel=6, compresstype=none"
			}
			testhelper.AssertQueryRuns(connectionPool, fmt.Sprintf("ALTER ROLE role1 SET gp_default_storage_options TO '%s'", defaultStorageOptionsString))

			results := backup.GetRoleGUCs(connectionPool)
			roleConfig := results["role1"]

			Expect(roleConfig).To(HaveLen(3))
			expectedRoleConfig := []backup.RoleGUC{
				{RoleName: "role1", Config: `SET client_min_messages TO 'info'`},
				{RoleName: "role1", Config: fmt.Sprintf(`SET gp_default_storage_options TO '%s'`, defaultStorageOptionsString)},
				{RoleName: "role1", Config: `SET search_path TO public`}}

			Expect(roleConfig).To(ConsistOf(expectedRoleConfig))
		})
		It("returns a slice of values for db specific user level GUCs", func() {
			testutils.SkipIfBefore6(connectionPool)

			testhelper.AssertQueryRuns(connectionPool, "CREATE ROLE role1 SUPERUSER NOINHERIT")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP ROLE role1")
			testhelper.AssertQueryRuns(connectionPool, "ALTER ROLE role1 IN DATABASE testdb SET search_path TO public")
			testhelper.AssertQueryRuns(connectionPool, "ALTER ROLE role1 IN DATABASE testdb SET client_min_messages TO 'info'")

			results := backup.GetRoleGUCs(connectionPool)
			roleConfig := results["role1"]

			Expect(roleConfig).To(HaveLen(2))
			expectedRoleConfig := []backup.RoleGUC{
				{RoleName: "role1", DbName: "testdb", Config: `SET client_min_messages TO 'info'`},
				{RoleName: "role1", DbName: "testdb", Config: `SET search_path TO public`}}

			Expect(roleConfig).To(ConsistOf(expectedRoleConfig))
		})
	})
	Describe("GetTablespaces", func() {
		It("returns a tablespace", func() {
			var expectedTablespace backup.Tablespace
			if connectionPool.Version.Before("6") {
				testhelper.AssertQueryRuns(connectionPool, "CREATE TABLESPACE test_tablespace FILESPACE test_dir")
				expectedTablespace = backup.Tablespace{Oid: 0, Tablespace: "test_tablespace", FileLocation: "test_dir"}
			} else {
				testhelper.AssertQueryRuns(connectionPool, "CREATE TABLESPACE test_tablespace LOCATION '/tmp/test_dir'")
				expectedTablespace = backup.Tablespace{Oid: 0, Tablespace: "test_tablespace", FileLocation: "'/tmp/test_dir'", SegmentLocations: []string{}}
			}
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLESPACE test_tablespace")

			resultTablespaces := backup.GetTablespaces(connectionPool)

			for _, tablespace := range resultTablespaces {
				if tablespace.Tablespace == "test_tablespace" {
					structmatcher.ExpectStructsToMatchExcluding(&expectedTablespace, &tablespace, "Oid")
					return
				}
			}
			Fail("Tablespace 'test_tablespace' was not created")
		})
		It("returns a tablespace with segment locations and options", func() {
			testutils.SkipIfBefore6(connectionPool)

			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLESPACE test_tablespace LOCATION '/tmp/test_dir' WITH (content0='/tmp/test_dir1', seq_page_cost=123)")
			expectedTablespace := backup.Tablespace{
				Oid: 0, Tablespace: "test_tablespace", FileLocation: "'/tmp/test_dir'",
				SegmentLocations: []string{"content0='/tmp/test_dir1'"},
				Options:          "seq_page_cost=123",
			}

			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLESPACE test_tablespace")

			resultTablespaces := backup.GetTablespaces(connectionPool)

			for _, tablespace := range resultTablespaces {
				if tablespace.Tablespace == "test_tablespace" {
					structmatcher.ExpectStructsToMatchExcluding(&expectedTablespace, &tablespace, "Oid")
					return
				}
			}
			Fail("Tablespace 'test_tablespace' was not created")
		})
		It("handles implicit cast of oid to text", func() {
			testutils.SkipIfBefore6(connectionPool)

			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLESPACE test_tablespace LOCATION '/tmp/test_dir' WITH (content0='/tmp/test_dir1', seq_page_cost=123)")
			testhelper.AssertQueryRuns(connectionPool, "CREATE OR REPLACE FUNCTION pg_catalog.text(oid) RETURNS text STRICT IMMUTABLE LANGUAGE SQL AS 'SELECT textin(oidout($1));';")
			testhelper.AssertQueryRuns(connectionPool, "CREATE CAST (oid AS text) WITH FUNCTION pg_catalog.text(oid) AS IMPLICIT;")
			expectedTablespace := backup.Tablespace{
				Oid: 0, Tablespace: "test_tablespace", FileLocation: "'/tmp/test_dir'",
				SegmentLocations: []string{"content0='/tmp/test_dir1'"},
				Options:          "seq_page_cost=123",
			}

			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLESPACE test_tablespace")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP FUNCTION pg_catalog.text(oid) CASCADE;")

			resultTablespaces := backup.GetTablespaces(connectionPool)

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
