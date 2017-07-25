package backup_test

import (
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"

	. "github.com/onsi/ginkgo"
	"github.com/onsi/gomega/gbytes"
)

var _ = Describe("backup/metadata_globals tests", func() {
	buffer := gbytes.NewBuffer()

	BeforeEach(func() {
		buffer = gbytes.BufferWithBytes([]byte(""))
	})
	Describe("PrintSessionGUCs", func() {
		It("prints session GUCs", func() {
			gucs := backup.QuerySessionGUCs{"UTF8", "on", "false"}

			backup.PrintSessionGUCs(buffer, gucs)
			testutils.ExpectRegexp(buffer, `SET statement_timeout = 0;
SET check_function_bodies = false;
SET client_min_messages = error;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET default_with_oids = false`)
		})
	})
	Describe("PrintCreateDatabaseStatement", func() {
		It("prints a basic CREATE DATABASE statement", func() {
			dbs := []backup.QueryDatabaseName{}
			emptyMetadataMap := backup.MetadataMap{}
			backup.PrintCreateDatabaseStatement(buffer, "testdb", dbs, emptyMetadataMap)
			testutils.ExpectRegexp(buffer, `CREATE DATABASE testdb;`)
		})
		It("prints a CREATE DATABASE statement with privileges, an owner, and a comment", func() {
			dbs := []backup.QueryDatabaseName{{1, "testdb"}, {2, "otherdb"}}
			dbMetadataMap := testutils.DefaultMetadataMap("DATABASE", true, true, true)
			backup.PrintCreateDatabaseStatement(buffer, "testdb", dbs, dbMetadataMap)
			testutils.ExpectRegexp(buffer, `CREATE DATABASE testdb;

COMMENT ON DATABASE testdb IS 'This is a database comment.';


ALTER DATABASE testdb OWNER TO testrole;


REVOKE ALL ON DATABASE testdb FROM PUBLIC;
REVOKE ALL ON DATABASE testdb FROM testrole;
GRANT ALL ON DATABASE testdb TO testrole;`)
		})
		It("prints a CREATE DATABASE statement with privileges for testdb and only prints privileges for otherdb", func() {
			dbs := []backup.QueryDatabaseName{{1, "testdb"}, {2, "otherdb"}}
			dbMetadataMap := testutils.DefaultMetadataMap("DATABASE", true, true, true)
			dbMetadataMap[2] = backup.ObjectMetadata{Privileges: []backup.ACL{{Grantee: "testrole", Create: true}}}
			backup.PrintCreateDatabaseStatement(buffer, "testdb", dbs, dbMetadataMap)
			testutils.ExpectRegexp(buffer, `CREATE DATABASE testdb;

COMMENT ON DATABASE testdb IS 'This is a database comment.';


ALTER DATABASE testdb OWNER TO testrole;


REVOKE ALL ON DATABASE testdb FROM PUBLIC;
REVOKE ALL ON DATABASE testdb FROM testrole;
GRANT ALL ON DATABASE testdb TO testrole;

REVOKE ALL ON DATABASE otherdb FROM PUBLIC;
GRANT CREATE ON DATABASE otherdb TO testrole;`)
		})
	})
	Describe("PrintDatabaseGUCs", func() {
		dbname := "testdb"
		defaultOidGUC := "SET default_with_oids TO 'true'"
		searchPathGUC := "SET search_path TO 'pg_catalog, public'"
		defaultStorageGUC := "SET gp_default_storage_options TO 'appendonly=true,blocksize=32768'"

		It("prints single database GUC", func() {
			gucs := []string{defaultOidGUC}

			backup.PrintDatabaseGUCs(buffer, gucs, dbname)
			testutils.ExpectRegexp(buffer, `ALTER DATABASE testdb SET default_with_oids TO 'true';`)
		})
		It("prints multiple database GUCs", func() {
			gucs := []string{defaultOidGUC, searchPathGUC, defaultStorageGUC}

			backup.PrintDatabaseGUCs(buffer, gucs, dbname)
			testutils.ExpectRegexp(buffer, `ALTER DATABASE testdb SET default_with_oids TO 'true';
ALTER DATABASE testdb SET search_path TO 'pg_catalog, public';
ALTER DATABASE testdb SET gp_default_storage_options TO 'appendonly=true,blocksize=32768';`)
		})
	})
	Describe("PrintCreateResourceQueueStatements", func() {
		var emptyResQueueMetadata = map[uint32]backup.ObjectMetadata{}
		It("prints resource queues", func() {
			someQueue := backup.QueryResourceQueue{1, "some_queue", 1, "-1.00", false, "0.00", "medium", "-1"}
			maxCostQueue := backup.QueryResourceQueue{1, "someMaxCostQueue", -1, "99.9", true, "0.00", "medium", "-1"}
			resQueues := []backup.QueryResourceQueue{someQueue, maxCostQueue}

			backup.PrintCreateResourceQueueStatements(buffer, resQueues, emptyResQueueMetadata)
			testutils.ExpectRegexp(buffer, `CREATE RESOURCE QUEUE some_queue WITH (ACTIVE_STATEMENTS=1);

CREATE RESOURCE QUEUE "someMaxCostQueue" WITH (MAX_COST=99.9, COST_OVERCOMMIT=TRUE);`)
		})
		It("prints a resource queue with active statements and max cost", func() {
			someActiveMaxCostQueue := backup.QueryResourceQueue{1, "someActiveMaxCostQueue", 5, "62.03", false, "0.00", "medium", "-1"}
			resQueues := []backup.QueryResourceQueue{someActiveMaxCostQueue}

			backup.PrintCreateResourceQueueStatements(buffer, resQueues, emptyResQueueMetadata)
			testutils.ExpectRegexp(buffer, `CREATE RESOURCE QUEUE "someActiveMaxCostQueue" WITH (ACTIVE_STATEMENTS=5, MAX_COST=62.03);`)
		})
		It("prints a resource queue with active statements and max cost", func() {
			everythingQueue := backup.QueryResourceQueue{1, "everythingQueue", 7, "32.80", true, "1.34", "low", "2GB"}
			resQueues := []backup.QueryResourceQueue{everythingQueue}

			backup.PrintCreateResourceQueueStatements(buffer, resQueues, emptyResQueueMetadata)
			testutils.ExpectRegexp(buffer, `CREATE RESOURCE QUEUE "everythingQueue" WITH (ACTIVE_STATEMENTS=7, MAX_COST=32.80, COST_OVERCOMMIT=TRUE, MIN_COST=1.34, PRIORITY=LOW, MEMORY_LIMIT='2GB');`)
		})
		It("prints a resource queue with a comment", func() {
			commentQueue := backup.QueryResourceQueue{1, "commentQueue", 1, "-1.00", false, "0.00", "medium", "-1"}
			resQueues := []backup.QueryResourceQueue{commentQueue}
			resQueueMetadata := testutils.DefaultMetadataMap("RESOURCE QUEUE", false, false, true)

			backup.PrintCreateResourceQueueStatements(buffer, resQueues, resQueueMetadata)
			testutils.ExpectRegexp(buffer, `CREATE RESOURCE QUEUE "commentQueue" WITH (ACTIVE_STATEMENTS=1);

COMMENT ON RESOURCE QUEUE "commentQueue" IS 'This is a resource queue comment.'`)
		})
		It("prints ALTER statement for pg_default resource queue", func() {
			pg_default := backup.QueryResourceQueue{1, "pg_default", 1, "-1.00", false, "0.00", "medium", "-1"}
			resQueues := []backup.QueryResourceQueue{pg_default}

			backup.PrintCreateResourceQueueStatements(buffer, resQueues, emptyResQueueMetadata)
			testutils.ExpectRegexp(buffer, `ALTER RESOURCE QUEUE pg_default WITH (ACTIVE_STATEMENTS=1);`)
		})
	})
	Describe("PrintCreateRoleStatements", func() {
		testrole1 := backup.QueryRole{
			Oid:             1,
			Name:            "testrole1",
			Super:           false,
			Inherit:         false,
			CreateRole:      false,
			CreateDB:        false,
			CanLogin:        false,
			ConnectionLimit: -1,
			Password:        "",
			ValidUntil:      "",
			ResQueue:        "pg_default",
			Createrexthttp:  false,
			Createrextgpfd:  false,
			Createwextgpfd:  false,
			Createrexthdfs:  false,
			Createwexthdfs:  false,
			TimeConstraints: []backup.TimeConstraint{},
		}

		testrole2 := backup.QueryRole{
			Oid:             1,
			Name:            "testRole2",
			Super:           true,
			Inherit:         true,
			CreateRole:      true,
			CreateDB:        true,
			CanLogin:        true,
			ConnectionLimit: 4,
			Password:        "md5a8b2c77dfeba4705f29c094592eb3369",
			ValidUntil:      "2099-01-01 00:00:00-08",
			ResQueue:        "testQueue",
			Createrexthttp:  true,
			Createrextgpfd:  true,
			Createwextgpfd:  true,
			Createrexthdfs:  true,
			Createwexthdfs:  true,
			TimeConstraints: []backup.TimeConstraint{
				{
					StartDay:  0,
					StartTime: "13:30:00",
					EndDay:    3,
					EndTime:   "14:30:00",
				}, {
					StartDay:  5,
					StartTime: "00:00:00",
					EndDay:    5,
					EndTime:   "24:00:00",
				},
			},
		}
		It("prints basic role", func() {
			roleMetadataMap := testutils.DefaultMetadataMap("ROLE", false, false, true)
			backup.PrintCreateRoleStatements(buffer, []backup.QueryRole{testrole1}, roleMetadataMap)

			testutils.ExpectRegexp(buffer, `CREATE ROLE testrole1;
ALTER ROLE testrole1 WITH NOSUPERUSER NOINHERIT NOCREATEROLE NOCREATEDB NOLOGIN RESOURCE QUEUE pg_default;

COMMENT ON ROLE testrole1 IS 'This is a role comment.';`)
		})
		It("prints roles with non-defaults", func() {
			roleMetadataMap := testutils.DefaultMetadataMap("ROLE", false, false, true)
			backup.PrintCreateRoleStatements(buffer, []backup.QueryRole{testrole2}, roleMetadataMap)

			testutils.ExpectRegexp(buffer, `CREATE ROLE "testRole2";
ALTER ROLE "testRole2" WITH SUPERUSER INHERIT CREATEROLE CREATEDB LOGIN CONNECTION LIMIT 4 PASSWORD 'md5a8b2c77dfeba4705f29c094592eb3369' VALID UNTIL '2099-01-01 00:00:00-08' RESOURCE QUEUE "testQueue" CREATEEXTTABLE (protocol='http') CREATEEXTTABLE (protocol='gpfdist', type='readable') CREATEEXTTABLE (protocol='gpfdist', type='writable') CREATEEXTTABLE (protocol='gphdfs', type='readable') CREATEEXTTABLE (protocol='gphdfs', type='writable');
ALTER ROLE "testRole2" DENY BETWEEN DAY 0 TIME '13:30:00' AND DAY 3 TIME '14:30:00';
ALTER ROLE "testRole2" DENY BETWEEN DAY 5 TIME '00:00:00' AND DAY 5 TIME '24:00:00';

COMMENT ON ROLE "testRole2" IS 'This is a role comment.';`)
		})
		It("prints multiple roles", func() {
			emptyMetadataMap := backup.MetadataMap{}
			backup.PrintCreateRoleStatements(buffer, []backup.QueryRole{testrole1, testrole2}, emptyMetadataMap)

			testutils.ExpectRegexp(buffer, `CREATE ROLE testrole1;
ALTER ROLE testrole1 WITH NOSUPERUSER NOINHERIT NOCREATEROLE NOCREATEDB NOLOGIN RESOURCE QUEUE pg_default;

CREATE ROLE "testRole2";
ALTER ROLE "testRole2" WITH SUPERUSER INHERIT CREATEROLE CREATEDB LOGIN CONNECTION LIMIT 4 PASSWORD 'md5a8b2c77dfeba4705f29c094592eb3369' VALID UNTIL '2099-01-01 00:00:00-08' RESOURCE QUEUE "testQueue" CREATEEXTTABLE (protocol='http') CREATEEXTTABLE (protocol='gpfdist', type='readable') CREATEEXTTABLE (protocol='gpfdist', type='writable') CREATEEXTTABLE (protocol='gphdfs', type='readable') CREATEEXTTABLE (protocol='gphdfs', type='writable');
ALTER ROLE "testRole2" DENY BETWEEN DAY 0 TIME '13:30:00' AND DAY 3 TIME '14:30:00';
ALTER ROLE "testRole2" DENY BETWEEN DAY 5 TIME '00:00:00' AND DAY 5 TIME '24:00:00';`)
		})
	})
	Describe("PrintRoleMembershipStatements", func() {
		roleWith := backup.QueryRoleMember{"group", "rolewith", "grantor", true}
		roleWithout := backup.QueryRoleMember{"group", "rolewithout", "grantor", false}
		It("prints a role without ADMIN OPTION", func() {
			backup.PrintRoleMembershipStatements(buffer, []backup.QueryRoleMember{roleWithout})
			testutils.ExpectRegexp(buffer, `GRANT group TO rolewithout GRANTED BY grantor;`)
		})
		It("prints a role WITH ADMIN OPTION", func() {
			backup.PrintRoleMembershipStatements(buffer, []backup.QueryRoleMember{roleWith})
			testutils.ExpectRegexp(buffer, `GRANT group TO rolewith WITH ADMIN OPTION GRANTED BY grantor;`)
		})
		It("prints multiple roles", func() {
			backup.PrintRoleMembershipStatements(buffer, []backup.QueryRoleMember{roleWith, roleWithout})
			testutils.ExpectRegexp(buffer, `GRANT group TO rolewith WITH ADMIN OPTION GRANTED BY grantor;
GRANT group TO rolewithout GRANTED BY grantor;`)
		})
	})
	Describe("PrintCreateTablespaceStatements", func() {
		expectedTablespace := backup.QueryTablespace{1, "test_tablespace", "test_filespace"}
		It("prints a basic tablespace", func() {
			emptyMetadataMap := backup.MetadataMap{}
			backup.PrintCreateTablespaceStatements(buffer, []backup.QueryTablespace{expectedTablespace}, emptyMetadataMap)
			testutils.ExpectRegexp(buffer, `CREATE TABLESPACE test_tablespace FILESPACE test_filespace;`)
		})
		It("prints a tablespace with privileges, an owner, and a comment", func() {
			tablespaceMetadataMap := testutils.DefaultMetadataMap("TABLESPACE", true, true, true)
			backup.PrintCreateTablespaceStatements(buffer, []backup.QueryTablespace{expectedTablespace}, tablespaceMetadataMap)
			testutils.ExpectRegexp(buffer, `CREATE TABLESPACE test_tablespace FILESPACE test_filespace;

COMMENT ON TABLESPACE test_tablespace IS 'This is a tablespace comment.';


ALTER TABLESPACE test_tablespace OWNER TO testrole;


REVOKE ALL ON TABLESPACE test_tablespace FROM PUBLIC;
REVOKE ALL ON TABLESPACE test_tablespace FROM testrole;
GRANT ALL ON TABLESPACE test_tablespace TO testrole;`)
		})
	})
})
