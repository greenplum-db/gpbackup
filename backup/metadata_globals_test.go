package backup_test

import (
	"fmt"

	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/toc"

	. "github.com/onsi/ginkgo/v2"
)

var _ = Describe("backup/metadata_globals tests", func() {
	emptyDB := backup.Database{}
	BeforeEach(func() {
		tocfile, backupfile = testutils.InitializeTestTOC(buffer, "global")
	})
	Describe("PrintSessionGUCs", func() {
		It("prints session GUCs", func() {
			gucs := backup.SessionGUCs{ClientEncoding: "UTF8"}

			backup.PrintSessionGUCs(backupfile, tocfile, gucs)
			testhelper.ExpectRegexp(buffer, `SET client_encoding = 'UTF8';
`)
		})
	})
	Describe("PrintCreateDatabaseStatement", func() {
		It("prints a basic CREATE DATABASE statement", func() {
			db := backup.Database{Oid: 1, Name: "testdb", Tablespace: "pg_default"}
			emptyMetadataMap := backup.MetadataMap{}
			backup.PrintCreateDatabaseStatement(backupfile, tocfile, emptyDB, db, emptyMetadataMap)
			testutils.ExpectEntry(tocfile.GlobalEntries, 0, "", "", "testdb", toc.OBJ_DATABASE)
			testutils.AssertBufferContents(tocfile.GlobalEntries, buffer, `CREATE DATABASE testdb TEMPLATE template0;`)
		})
		It("prints a CREATE DATABASE statement for a reserved keyword named database", func() {
			db := backup.Database{Oid: 1, Name: `"table"`, Tablespace: "pg_default"}
			emptyMetadataMap := backup.MetadataMap{}
			backup.PrintCreateDatabaseStatement(backupfile, tocfile, emptyDB, db, emptyMetadataMap)
			testutils.ExpectEntry(tocfile.GlobalEntries, 0, "", "", `"table"`, toc.OBJ_DATABASE)
			testutils.AssertBufferContents(tocfile.GlobalEntries, buffer, `CREATE DATABASE "table" TEMPLATE template0;`)
		})
		It("prints a CREATE DATABASE statement with privileges, an owner, security label, and a comment", func() {
			db := backup.Database{Oid: 1, Name: "testdb", Tablespace: "pg_default"}
			dbMetadataMap := testutils.DefaultMetadataMap(toc.OBJ_DATABASE, true, true, true, true)
			dbMetadata := dbMetadataMap[db.GetUniqueID()]
			dbMetadata.Privileges[0].Create = false
			dbMetadataMap[db.GetUniqueID()] = dbMetadata
			backup.PrintCreateDatabaseStatement(backupfile, tocfile, emptyDB, db, dbMetadataMap)
			expectedStatements := []string{
				`CREATE DATABASE testdb TEMPLATE template0;`,
				`COMMENT ON DATABASE testdb IS 'This is a database comment.';`,
				`ALTER DATABASE testdb OWNER TO testrole;`,
				`REVOKE ALL ON DATABASE testdb FROM PUBLIC;
REVOKE ALL ON DATABASE testdb FROM testrole;
GRANT TEMPORARY,CONNECT ON DATABASE testdb TO testrole;`,
				`SECURITY LABEL FOR dummy ON DATABASE testdb IS 'unclassified';`}
			testutils.AssertBufferContents(tocfile.GlobalEntries, buffer, expectedStatements...)
		})
		It("prints a CREATE DATABASE statement with all modifiers", func() {
			db := backup.Database{Oid: 1, Name: "testdb", Tablespace: "test_tablespace", Encoding: "UTF8", Collate: "en_US.utf-8", CType: "en_US.utf-8"}
			emptyMetadataMap := backup.MetadataMap{}
			backup.PrintCreateDatabaseStatement(backupfile, tocfile, emptyDB, db, emptyMetadataMap)
			testutils.AssertBufferContents(tocfile.GlobalEntries, buffer, `CREATE DATABASE testdb TEMPLATE template0 TABLESPACE test_tablespace ENCODING 'UTF8' LC_COLLATE 'en_US.utf-8' LC_CTYPE 'en_US.utf-8';`)
		})
		It("does not print encoding information if it is the same as defaults", func() {
			defaultDB := backup.Database{Oid: 0, Name: "", Tablespace: "", Encoding: "UTF8", Collate: "en_US.utf-8", CType: "en_US.utf-8"}
			db := backup.Database{Oid: 1, Name: "testdb", Tablespace: "test_tablespace", Encoding: "UTF8", Collate: "en_US.utf-8", CType: "en_US.utf-8"}
			emptyMetadataMap := backup.MetadataMap{}
			backup.PrintCreateDatabaseStatement(backupfile, tocfile, defaultDB, db, emptyMetadataMap)
			testutils.AssertBufferContents(tocfile.GlobalEntries, buffer, `CREATE DATABASE testdb TEMPLATE template0 TABLESPACE test_tablespace;`)
		})
	})
	Describe("PrintDatabaseGUCs", func() {
		dbname := "testdb"
		defaultOidGUC := "SET default_with_oids TO 'true'"
		searchPathGUC := "SET search_path TO pg_catalog, public"
		defaultStorageGUC := "SET gp_default_storage_options TO 'appendonly=true,blocksize=32768'"

		It("prints single database GUC", func() {
			gucs := []string{defaultOidGUC}

			backup.PrintDatabaseGUCs(backupfile, tocfile, gucs, dbname)
			testutils.ExpectEntry(tocfile.GlobalEntries, 0, "", "", "testdb", toc.OBJ_DATABASE_GUC)
			testutils.AssertBufferContents(tocfile.GlobalEntries, buffer, `ALTER DATABASE testdb SET default_with_oids TO 'true';`)
		})
		It("prints multiple database GUCs", func() {
			gucs := []string{defaultOidGUC, searchPathGUC, defaultStorageGUC}

			backup.PrintDatabaseGUCs(backupfile, tocfile, gucs, dbname)
			testutils.AssertBufferContents(tocfile.GlobalEntries, buffer,
				`ALTER DATABASE testdb SET default_with_oids TO 'true';`,
				`ALTER DATABASE testdb SET search_path TO pg_catalog, public;`,
				`ALTER DATABASE testdb SET gp_default_storage_options TO 'appendonly=true,blocksize=32768';`)
		})
	})
	Describe("PrintCreateResourceQueueStatements", func() {
		var emptyResQueueMetadata = backup.MetadataMap{}
		It("prints resource queues", func() {
			someQueue := backup.ResourceQueue{Oid: 1, Name: "some_queue", ActiveStatements: 1, MaxCost: "-1.00", CostOvercommit: false, MinCost: "0.00", Priority: "medium", MemoryLimit: "-1"}
			maxCostQueue := backup.ResourceQueue{Oid: 1, Name: `"someMaxCostQueue"`, ActiveStatements: -1, MaxCost: "99.9", CostOvercommit: true, MinCost: "0.00", Priority: "medium", MemoryLimit: "-1"}
			resQueues := []backup.ResourceQueue{someQueue, maxCostQueue}

			backup.PrintCreateResourceQueueStatements(backupfile, tocfile, resQueues, emptyResQueueMetadata)
			testutils.ExpectEntry(tocfile.GlobalEntries, 0, "", "", "some_queue", toc.OBJ_RESOURCE_QUEUE)
			testutils.AssertBufferContents(tocfile.GlobalEntries, buffer,
				`CREATE RESOURCE QUEUE some_queue WITH (ACTIVE_STATEMENTS=1);`,
				`CREATE RESOURCE QUEUE "someMaxCostQueue" WITH (MAX_COST=99.9, COST_OVERCOMMIT=TRUE);`)
		})
		It("prints a resource queue with active statements and max cost", func() {
			someActiveMaxCostQueue := backup.ResourceQueue{Oid: 1, Name: `"someActiveMaxCostQueue"`, ActiveStatements: 5, MaxCost: "62.03", CostOvercommit: false, MinCost: "0.00", Priority: "medium", MemoryLimit: "-1"}
			resQueues := []backup.ResourceQueue{someActiveMaxCostQueue}

			backup.PrintCreateResourceQueueStatements(backupfile, tocfile, resQueues, emptyResQueueMetadata)
			testutils.AssertBufferContents(tocfile.GlobalEntries, buffer, `CREATE RESOURCE QUEUE "someActiveMaxCostQueue" WITH (ACTIVE_STATEMENTS=5, MAX_COST=62.03);`)
		})
		It("prints a resource queue with all properties", func() {
			everythingQueue := backup.ResourceQueue{Oid: 1, Name: `"everythingQueue"`, ActiveStatements: 7, MaxCost: "32.80", CostOvercommit: true, MinCost: "1.34", Priority: "low", MemoryLimit: "2GB"}
			resQueues := []backup.ResourceQueue{everythingQueue}

			backup.PrintCreateResourceQueueStatements(backupfile, tocfile, resQueues, emptyResQueueMetadata)
			testutils.AssertBufferContents(tocfile.GlobalEntries, buffer, `CREATE RESOURCE QUEUE "everythingQueue" WITH (ACTIVE_STATEMENTS=7, MAX_COST=32.80, COST_OVERCOMMIT=TRUE, MIN_COST=1.34, PRIORITY=LOW, MEMORY_LIMIT='2GB');`)
		})
		It("prints a resource queue with a comment", func() {
			commentQueue := backup.ResourceQueue{Oid: 1, Name: `"commentQueue"`, ActiveStatements: 1, MaxCost: "-1.00", CostOvercommit: false, MinCost: "0.00", Priority: "medium", MemoryLimit: "-1"}
			resQueues := []backup.ResourceQueue{commentQueue}
			resQueueMetadata := testutils.DefaultMetadataMap(toc.OBJ_RESOURCE_QUEUE, false, false, true, false)

			backup.PrintCreateResourceQueueStatements(backupfile, tocfile, resQueues, resQueueMetadata)
			testutils.AssertBufferContents(tocfile.GlobalEntries, buffer, `CREATE RESOURCE QUEUE "commentQueue" WITH (ACTIVE_STATEMENTS=1);`,
				`COMMENT ON RESOURCE QUEUE "commentQueue" IS 'This is a resource queue comment.';`)
		})
		It("prints ALTER statement for pg_default resource queue", func() {
			pgDefault := backup.ResourceQueue{Oid: 1, Name: "pg_default", ActiveStatements: 1, MaxCost: "-1.00", CostOvercommit: false, MinCost: "0.00", Priority: "medium", MemoryLimit: "-1"}
			resQueues := []backup.ResourceQueue{pgDefault}

			backup.PrintCreateResourceQueueStatements(backupfile, tocfile, resQueues, emptyResQueueMetadata)
			testutils.AssertBufferContents(tocfile.GlobalEntries, buffer, `ALTER RESOURCE QUEUE pg_default WITH (ACTIVE_STATEMENTS=1);`)
		})
	})
	Describe("PrintCreateResourceGroupStatements", func() {
		var emptyResGroupMetadata = backup.MetadataMap{}
		It("prints resource groups", func() {
			testhelper.SetDBVersion(connectionPool, "5.9.0")
			someGroup := backup.ResourceGroupBefore7{ResourceGroup: backup.ResourceGroup{Oid: 1, Name: "some_group", Concurrency: "15"}, CPURateLimit: "10", MemoryLimit: "20", MemorySharedQuota: "25", MemorySpillRatio: "30"}
			someGroup2 := backup.ResourceGroupBefore7{ResourceGroup: backup.ResourceGroup{Oid: 2, Name: "some_group2", Concurrency: "25"}, CPURateLimit: "20", MemoryLimit: "30", MemorySharedQuota: "35", MemorySpillRatio: "10"}
			resGroups := []backup.ResourceGroupBefore7{someGroup, someGroup2}

			backup.PrintCreateResourceGroupStatementsBefore7(backupfile, tocfile, resGroups, emptyResGroupMetadata)
			testutils.ExpectEntry(tocfile.GlobalEntries, 0, "", "", "some_group", toc.OBJ_RESOURCE_GROUP)
			testutils.AssertBufferContents(tocfile.GlobalEntries, buffer,
				`CREATE RESOURCE GROUP some_group WITH (CPU_RATE_LIMIT=10, MEMORY_AUDITOR=vmtracker, MEMORY_LIMIT=20, MEMORY_SHARED_QUOTA=25, MEMORY_SPILL_RATIO=30, CONCURRENCY=15);`,
				`CREATE RESOURCE GROUP some_group2 WITH (CPU_RATE_LIMIT=20, MEMORY_AUDITOR=vmtracker, MEMORY_LIMIT=30, MEMORY_SHARED_QUOTA=35, MEMORY_SPILL_RATIO=10, CONCURRENCY=25);`)
		})
		It("prints ALTER statement for default_group resource group", func() {
			defaultGroup := backup.ResourceGroupBefore7{ResourceGroup: backup.ResourceGroup{Oid: 1, Name: "default_group", Concurrency: "15"}, CPURateLimit: "10", MemoryLimit: "20", MemorySharedQuota: "25", MemorySpillRatio: "30"}
			resGroups := []backup.ResourceGroupBefore7{defaultGroup}

			backup.PrintCreateResourceGroupStatementsBefore7(backupfile, tocfile, resGroups, emptyResGroupMetadata)
			testutils.ExpectEntry(tocfile.GlobalEntries, 0, "", "", "default_group", toc.OBJ_RESOURCE_GROUP)
			testutils.AssertBufferContents(tocfile.GlobalEntries, buffer,
				`ALTER RESOURCE GROUP default_group SET MEMORY_LIMIT 20;`,
				`ALTER RESOURCE GROUP default_group SET MEMORY_SHARED_QUOTA 25;`,
				`ALTER RESOURCE GROUP default_group SET MEMORY_SPILL_RATIO 30;`,
				`ALTER RESOURCE GROUP default_group SET CONCURRENCY 15;`,
				`ALTER RESOURCE GROUP default_group SET CPU_RATE_LIMIT 10;`)
		})
		It("prints memory_auditor resource groups", func() {
			testhelper.SetDBVersion(connectionPool, "5.8.0")
			someGroup := backup.ResourceGroupBefore7{ResourceGroup: backup.ResourceGroup{Oid: 1, Name: "some_group", Concurrency: "15"}, CPURateLimit: "10", MemoryLimit: "20", MemorySharedQuota: "25", MemorySpillRatio: "30"}
			someGroup2 := backup.ResourceGroupBefore7{ResourceGroup: backup.ResourceGroup{Oid: 2, Name: "some_group2", Concurrency: "0"}, CPURateLimit: "10", MemoryLimit: "30", MemorySharedQuota: "35", MemorySpillRatio: "10", MemoryAuditor: "1"}
			someGroup3 := backup.ResourceGroupBefore7{ResourceGroup: backup.ResourceGroup{Oid: 3, Name: "some_group3", Concurrency: "25"}, CPURateLimit: "10", MemoryLimit: "30", MemorySharedQuota: "35", MemorySpillRatio: "10", MemoryAuditor: "0"}
			resGroups := []backup.ResourceGroupBefore7{someGroup, someGroup2, someGroup3}

			backup.PrintCreateResourceGroupStatementsBefore7(backupfile, tocfile, resGroups, emptyResGroupMetadata)
			testutils.ExpectEntry(tocfile.GlobalEntries, 0, "", "", "some_group", toc.OBJ_RESOURCE_GROUP)
			testutils.AssertBufferContents(tocfile.GlobalEntries, buffer,
				`CREATE RESOURCE GROUP some_group WITH (CPU_RATE_LIMIT=10, MEMORY_AUDITOR=vmtracker, MEMORY_LIMIT=20, MEMORY_SHARED_QUOTA=25, MEMORY_SPILL_RATIO=30, CONCURRENCY=15);`,
				`CREATE RESOURCE GROUP some_group2 WITH (CPU_RATE_LIMIT=10, MEMORY_AUDITOR=cgroup, MEMORY_LIMIT=30, MEMORY_SHARED_QUOTA=35, MEMORY_SPILL_RATIO=10, CONCURRENCY=0);`,
				`CREATE RESOURCE GROUP some_group3 WITH (CPU_RATE_LIMIT=10, MEMORY_AUDITOR=vmtracker, MEMORY_LIMIT=30, MEMORY_SHARED_QUOTA=35, MEMORY_SPILL_RATIO=10, CONCURRENCY=25);`)
		})
		It("prints cpuset resource groups", func() {
			testhelper.SetDBVersion(connectionPool, "5.9.0")
			someGroup := backup.ResourceGroupBefore7{ResourceGroup: backup.ResourceGroup{Oid: 1, Name: "some_group", Concurrency: "15"}, CPURateLimit: "10", MemoryLimit: "20", MemorySharedQuota: "25", MemorySpillRatio: "30"}
			someGroup2 := backup.ResourceGroupBefore7{ResourceGroup: backup.ResourceGroup{Oid: 2, Name: "some_group2", Concurrency: "25", Cpuset: "0-3"}, CPURateLimit: "-1", MemoryLimit: "30", MemorySharedQuota: "35", MemorySpillRatio: "10"}
			resGroups := []backup.ResourceGroupBefore7{someGroup, someGroup2}

			backup.PrintCreateResourceGroupStatementsBefore7(backupfile, tocfile, resGroups, emptyResGroupMetadata)
			testutils.ExpectEntry(tocfile.GlobalEntries, 0, "", "", "some_group", toc.OBJ_RESOURCE_GROUP)
			testutils.AssertBufferContents(tocfile.GlobalEntries, buffer,
				`CREATE RESOURCE GROUP some_group WITH (CPU_RATE_LIMIT=10, MEMORY_AUDITOR=vmtracker, MEMORY_LIMIT=20, MEMORY_SHARED_QUOTA=25, MEMORY_SPILL_RATIO=30, CONCURRENCY=15);`,
				`CREATE RESOURCE GROUP some_group2 WITH (CPUSET='0-3', MEMORY_AUDITOR=vmtracker, MEMORY_LIMIT=30, MEMORY_SHARED_QUOTA=35, MEMORY_SPILL_RATIO=10, CONCURRENCY=25);`)
		})
		// Test for GPDB5x, 'retired' in 6X+ #temp5xResGroup
		It("prints memory_spill_ratio resource groups in new syntax", func() {
			testhelper.SetDBVersion(connectionPool, "5.2.0")

			defaultGroup := backup.ResourceGroupBefore7{ResourceGroup: backup.ResourceGroup{Oid: 1, Name: "default_group", Concurrency: "15"}, CPURateLimit: "10", MemoryLimit: "20", MemorySharedQuota: "25", MemorySpillRatio: "30 MB"}
			adminGroup := backup.ResourceGroupBefore7{ResourceGroup: backup.ResourceGroup{Oid: 2, Name: "admin_group", Concurrency: "15"}, CPURateLimit: "10", MemoryLimit: "20", MemorySharedQuota: "25", MemorySpillRatio: "30"}
			someGroup := backup.ResourceGroupBefore7{ResourceGroup: backup.ResourceGroup{Oid: 3, Name: "some_group", Concurrency: "25"}, CPURateLimit: "20", MemoryLimit: "30", MemorySharedQuota: "35", MemorySpillRatio: "40 MB"}
			someGroup2 := backup.ResourceGroupBefore7{ResourceGroup: backup.ResourceGroup{Oid: 4, Name: "some_group2", Concurrency: "25"}, CPURateLimit: "20", MemoryLimit: "30", MemorySharedQuota: "35", MemorySpillRatio: "40"}
			resGroups := []backup.ResourceGroupBefore7{defaultGroup, adminGroup, someGroup, someGroup2}

			backup.PrintCreateResourceGroupStatementsBefore7(backupfile, tocfile, resGroups, emptyResGroupMetadata)
			testutils.ExpectEntry(tocfile.GlobalEntries, 0, "", "", "default_group", toc.OBJ_RESOURCE_GROUP)
			testutils.AssertBufferContents(tocfile.GlobalEntries, buffer,
				`ALTER RESOURCE GROUP default_group SET MEMORY_LIMIT 20;`,
				`ALTER RESOURCE GROUP default_group SET MEMORY_SHARED_QUOTA 25;`,
				`ALTER RESOURCE GROUP default_group SET MEMORY_SPILL_RATIO '30 MB';`,
				`ALTER RESOURCE GROUP default_group SET CONCURRENCY 15;`,
				`ALTER RESOURCE GROUP default_group SET CPU_RATE_LIMIT 10;`,
				`ALTER RESOURCE GROUP admin_group SET MEMORY_LIMIT 20;`,
				`ALTER RESOURCE GROUP admin_group SET MEMORY_SHARED_QUOTA 25;`,
				`ALTER RESOURCE GROUP admin_group SET MEMORY_SPILL_RATIO 30;`,
				`ALTER RESOURCE GROUP admin_group SET CONCURRENCY 15;`,
				`ALTER RESOURCE GROUP admin_group SET CPU_RATE_LIMIT 10;`,
				`CREATE RESOURCE GROUP some_group WITH (CPU_RATE_LIMIT=20, MEMORY_LIMIT=30, MEMORY_SHARED_QUOTA=35, MEMORY_SPILL_RATIO='40 MB', CONCURRENCY=25);`,
				`CREATE RESOURCE GROUP some_group2 WITH (CPU_RATE_LIMIT=20, MEMORY_LIMIT=30, MEMORY_SHARED_QUOTA=35, MEMORY_SPILL_RATIO=40, CONCURRENCY=25);`)
		})
		It("prints correct CREATE RESOURCE GROUP syntax for old resource groups on GPDB 5.8", func() {
			// Memory Auditor reslimittype was added in GPDB 5.8. Make sure the older resource group object will have the proper default.
			testhelper.SetDBVersion(connectionPool, "5.8.0")
			resGroup52 := backup.ResourceGroupBefore7{ResourceGroup: backup.ResourceGroup{Oid: 3, Name: "resGroup52", Concurrency: "25"}, CPURateLimit: "20", MemoryLimit: "30", MemorySharedQuota: "35", MemorySpillRatio: "40"}
			resGroup58 := backup.ResourceGroupBefore7{ResourceGroup: backup.ResourceGroup{Oid: 4, Name: "resGroup58", Concurrency: "25"}, CPURateLimit: "20", MemoryLimit: "30", MemorySharedQuota: "35", MemorySpillRatio: "40", MemoryAuditor: "1"}
			resGroups := []backup.ResourceGroupBefore7{resGroup52, resGroup58}

			backup.PrintCreateResourceGroupStatementsBefore7(backupfile, tocfile, resGroups, emptyResGroupMetadata)
			testutils.AssertBufferContents(tocfile.GlobalEntries, buffer,
				`CREATE RESOURCE GROUP resGroup52 WITH (CPU_RATE_LIMIT=20, MEMORY_AUDITOR=vmtracker, MEMORY_LIMIT=30, MEMORY_SHARED_QUOTA=35, MEMORY_SPILL_RATIO=40, CONCURRENCY=25);`,
				`CREATE RESOURCE GROUP resGroup58 WITH (CPU_RATE_LIMIT=20, MEMORY_AUDITOR=cgroup, MEMORY_LIMIT=30, MEMORY_SHARED_QUOTA=35, MEMORY_SPILL_RATIO=40, CONCURRENCY=25);`)
		})
		It("prints correct CREATE RESOURCE GROUP syntax for old resource groups on GPDB 5.9", func() {
			// Cpuset reslimittype was added in GPDB 5.9. Make sure the older resource group objects
			// will have the proper default. In this case, you either have cpu_rate_limit or cpuset.
			testhelper.SetDBVersion(connectionPool, "5.9.0")
			resGroup52 := backup.ResourceGroupBefore7{ResourceGroup: backup.ResourceGroup{Oid: 3, Name: "resGroup52", Concurrency: "25"}, CPURateLimit: "20", MemoryLimit: "30", MemorySharedQuota: "35", MemorySpillRatio: "40"}
			resGroup58 := backup.ResourceGroupBefore7{ResourceGroup: backup.ResourceGroup{Oid: 4, Name: "resGroup58", Concurrency: "25"}, CPURateLimit: "20", MemoryLimit: "30", MemorySharedQuota: "35", MemorySpillRatio: "40", MemoryAuditor: "1"}
			resGroup59 := backup.ResourceGroupBefore7{ResourceGroup: backup.ResourceGroup{Oid: 5, Name: "resGroup59", Concurrency: "25", Cpuset: "1"}, CPURateLimit: "-1", MemoryLimit: "30", MemorySharedQuota: "35", MemorySpillRatio: "40", MemoryAuditor: "1"}
			resGroups := []backup.ResourceGroupBefore7{resGroup52, resGroup58, resGroup59}

			backup.PrintCreateResourceGroupStatementsBefore7(backupfile, tocfile, resGroups, emptyResGroupMetadata)
			testutils.AssertBufferContents(tocfile.GlobalEntries, buffer,
				`CREATE RESOURCE GROUP resGroup52 WITH (CPU_RATE_LIMIT=20, MEMORY_AUDITOR=vmtracker, MEMORY_LIMIT=30, MEMORY_SHARED_QUOTA=35, MEMORY_SPILL_RATIO=40, CONCURRENCY=25);`,
				`CREATE RESOURCE GROUP resGroup58 WITH (CPU_RATE_LIMIT=20, MEMORY_AUDITOR=cgroup, MEMORY_LIMIT=30, MEMORY_SHARED_QUOTA=35, MEMORY_SPILL_RATIO=40, CONCURRENCY=25);`,
				`CREATE RESOURCE GROUP resGroup59 WITH (CPUSET='1', MEMORY_AUDITOR=cgroup, MEMORY_LIMIT=30, MEMORY_SHARED_QUOTA=35, MEMORY_SPILL_RATIO=40, CONCURRENCY=25);`)
		})
	})
	Describe("PrintResetResourceGroupStatements", func() {
		It("prints prepare resource groups", func() {
			backup.PrintResetResourceGroupStatements(backupfile, tocfile)
			testutils.ExpectEntry(tocfile.GlobalEntries, 0, "", "", "admin_group", toc.OBJ_RESOURCE_GROUP)
			if connectionPool.Version.Before("7") {
				testutils.AssertBufferContents(tocfile.GlobalEntries, buffer,
					`ALTER RESOURCE GROUP admin_group SET CPU_RATE_LIMIT 1;`,
					`ALTER RESOURCE GROUP admin_group SET MEMORY_LIMIT 1;`,
					`ALTER RESOURCE GROUP default_group SET CPU_RATE_LIMIT 1;`,
					`ALTER RESOURCE GROUP default_group SET MEMORY_LIMIT 1;`)
			} else { // GPDB7+
				testutils.AssertBufferContents(tocfile.GlobalEntries, buffer,
					`ALTER RESOURCE GROUP admin_group SET CPU_MAX_PERCENT 1;`,
					`ALTER RESOURCE GROUP admin_group SET CPU_WEIGHT 100;`,
					`ALTER RESOURCE GROUP default_group SET CPU_MAX_PERCENT 1;`,
					`ALTER RESOURCE GROUP default_group SET CPU_WEIGHT 100;`,
					`ALTER RESOURCE GROUP system_group SET CPU_MAX_PERCENT 1;`,
					`ALTER RESOURCE GROUP system_group SET CPU_WEIGHT 100;`)

			}
		})
	})
	Describe("PrintCreateRoleStatements", func() {
		testrole1 := backup.Role{
			Oid:             1,
			Name:            "testrole1",
			Super:           false,
			Inherit:         false,
			CreateRole:      false,
			CreateDB:        false,
			CanLogin:        false,
			Replication:     false,
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
			TimeConstraints: []backup.TimeConstraint{},
		}

		testrole2 := backup.Role{
			Oid:             1,
			Name:            `"testRole2"`,
			Super:           true,
			Inherit:         true,
			CreateRole:      true,
			CreateDB:        true,
			CanLogin:        true,
			Replication:     true,
			ConnectionLimit: 4,
			Password:        "md5a8b2c77dfeba4705f29c094592eb3369",
			ValidUntil:      "2099-01-01 00:00:00-08",
			ResQueue:        `"testQueue"`,
			ResGroup:        `"testGroup"`,
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

		getResourceGroupReplace := func() (string, string) {

			resourceGroupReplace1 := ` RESOURCE GROUP default_group`
			resourceGroupReplace2 := `RESOURCE GROUP "testGroup" `
			return resourceGroupReplace1, resourceGroupReplace2
		}

		It("prints basic role", func() {
			roleMetadataMap := testutils.DefaultMetadataMap(toc.OBJ_ROLE, false, false, true, false)
			backup.PrintCreateRoleStatements(backupfile, tocfile, []backup.Role{testrole1}, roleMetadataMap)

			resourceGroupReplace, _ := getResourceGroupReplace()
			testutils.ExpectEntry(tocfile.GlobalEntries, 0, "", "", "testrole1", toc.OBJ_ROLE)
			testutils.AssertBufferContents(tocfile.GlobalEntries, buffer, fmt.Sprintf(`CREATE ROLE testrole1;
ALTER ROLE testrole1 WITH NOSUPERUSER NOINHERIT NOCREATEROLE NOCREATEDB NOLOGIN RESOURCE QUEUE pg_default%s;`, resourceGroupReplace),
				`COMMENT ON ROLE testrole1 IS 'This is a role comment.';`)
		})
		It("prints roles with non-defaults and security label", func() {
			roleMetadataMap := testutils.DefaultMetadataMap(toc.OBJ_ROLE, false, false, true, true)
			backup.PrintCreateRoleStatements(backupfile, tocfile, []backup.Role{testrole2}, roleMetadataMap)

			_, resourceGroupReplace := getResourceGroupReplace()
			expectedStatements := []string{
				fmt.Sprintf(`CREATE ROLE "testRole2";
ALTER ROLE "testRole2" WITH SUPERUSER INHERIT CREATEROLE CREATEDB LOGIN REPLICATION CONNECTION LIMIT 4 PASSWORD 'md5a8b2c77dfeba4705f29c094592eb3369' VALID UNTIL '2099-01-01 00:00:00-08' RESOURCE QUEUE "testQueue" %sCREATEEXTTABLE (protocol='http') CREATEEXTTABLE (protocol='gpfdist', type='readable') CREATEEXTTABLE (protocol='gpfdist', type='writable') CREATEEXTTABLE (protocol='gphdfs', type='readable') CREATEEXTTABLE (protocol='gphdfs', type='writable');`, resourceGroupReplace),
				`ALTER ROLE "testRole2" DENY BETWEEN DAY 0 TIME '13:30:00' AND DAY 3 TIME '14:30:00';`,
				`ALTER ROLE "testRole2" DENY BETWEEN DAY 5 TIME '00:00:00' AND DAY 5 TIME '24:00:00';`,
				`COMMENT ON ROLE "testRole2" IS 'This is a role comment.';`,
				`SECURITY LABEL FOR dummy ON ROLE "testRole2" IS 'unclassified';`}
			testutils.AssertBufferContents(tocfile.GlobalEntries, buffer, expectedStatements...)

		})
		It("prints multiple roles", func() {
			emptyMetadataMap := backup.MetadataMap{}
			backup.PrintCreateRoleStatements(backupfile, tocfile, []backup.Role{testrole1, testrole2}, emptyMetadataMap)

			resourceGroupReplace1, resourceGroupReplace2 := getResourceGroupReplace()
			expectedStatements := []string{
				fmt.Sprintf(`CREATE ROLE testrole1;
ALTER ROLE testrole1 WITH NOSUPERUSER NOINHERIT NOCREATEROLE NOCREATEDB NOLOGIN RESOURCE QUEUE pg_default%s;`, resourceGroupReplace1),
				fmt.Sprintf(`CREATE ROLE "testRole2";
ALTER ROLE "testRole2" WITH SUPERUSER INHERIT CREATEROLE CREATEDB LOGIN REPLICATION CONNECTION LIMIT 4 PASSWORD 'md5a8b2c77dfeba4705f29c094592eb3369' VALID UNTIL '2099-01-01 00:00:00-08' RESOURCE QUEUE "testQueue" %sCREATEEXTTABLE (protocol='http') CREATEEXTTABLE (protocol='gpfdist', type='readable') CREATEEXTTABLE (protocol='gpfdist', type='writable') CREATEEXTTABLE (protocol='gphdfs', type='readable') CREATEEXTTABLE (protocol='gphdfs', type='writable');`, resourceGroupReplace2),
				`ALTER ROLE "testRole2" DENY BETWEEN DAY 0 TIME '13:30:00' AND DAY 3 TIME '14:30:00';`,
				`ALTER ROLE "testRole2" DENY BETWEEN DAY 5 TIME '00:00:00' AND DAY 5 TIME '24:00:00';`}
			testutils.AssertBufferContents(tocfile.GlobalEntries, buffer, expectedStatements...)
		})
	})
	Describe("PrintRoleMembershipStatements", func() {
		roleWith := backup.RoleMember{Role: "group", Member: "rolewith", Grantor: "grantor", IsAdmin: true}
		roleWithout := backup.RoleMember{Role: "group", Member: "rolewithout", Grantor: "grantor", IsAdmin: false}
		roleWithoutGrantor := backup.RoleMember{Role: "group", Member: "rolewithoutgrantor", Grantor: "", IsAdmin: false}
		It("prints a role without ADMIN OPTION", func() {
			backup.PrintRoleMembershipStatements(backupfile, tocfile, []backup.RoleMember{roleWithout})
			testutils.ExpectEntry(tocfile.GlobalEntries, 0, "", "", "rolewithout", toc.OBJ_ROLE_GRANT)
			testutils.AssertBufferContents(tocfile.GlobalEntries, buffer, `GRANT group TO rolewithout GRANTED BY grantor;`)
		})
		It("prints a role WITH ADMIN OPTION", func() {
			backup.PrintRoleMembershipStatements(backupfile, tocfile, []backup.RoleMember{roleWith})
			testutils.AssertBufferContents(tocfile.GlobalEntries, buffer, `GRANT group TO rolewith WITH ADMIN OPTION GRANTED BY grantor;`)
		})
		It("prints multiple roles", func() {
			backup.PrintRoleMembershipStatements(backupfile, tocfile, []backup.RoleMember{roleWith, roleWithout})
			testutils.AssertBufferContents(tocfile.GlobalEntries, buffer,
				`GRANT group TO rolewith WITH ADMIN OPTION GRANTED BY grantor;`,
				`GRANT group TO rolewithout GRANTED BY grantor;`)
		})
		It("prints a role without a grantor", func() {
			backup.PrintRoleMembershipStatements(backupfile, tocfile, []backup.RoleMember{roleWithoutGrantor})
			testutils.AssertBufferContents(tocfile.GlobalEntries, buffer, `GRANT group TO rolewithoutgrantor;`)
		})
	})
	Describe("PrintRoleGUCStatements", func() {
		It("Prints guc statements for a role", func() {
			roleConfigMap := map[string][]backup.RoleGUC{
				"testrole1": {
					{RoleName: "testrole1", Config: "SET search_path TO public"},
					{RoleName: "testrole1", DbName: "testdb", Config: "SET client_min_messages TO 'error'"},
					{RoleName: "testrole1", Config: "SET gp_default_storage_options TO 'appendonly=true, compresslevel=6, orientation=row, compresstype=none'"}},
			}
			backup.PrintRoleGUCStatements(backupfile, tocfile, roleConfigMap)

			testutils.ExpectEntry(tocfile.GlobalEntries, 0, "", "", "testrole1", toc.OBJ_ROLE_GUC)
			testutils.AssertBufferContents(tocfile.GlobalEntries, buffer, `ALTER ROLE testrole1 SET search_path TO public;`,
				`ALTER ROLE testrole1 IN DATABASE testdb SET client_min_messages TO 'error';`,
				`ALTER ROLE testrole1 SET gp_default_storage_options TO 'appendonly=true, compresslevel=6, orientation=row, compresstype=none';`)
		})
	})
	Describe("PrintCreateTablespaceStatements", func() {
		expectedTablespace := backup.Tablespace{Oid: 1, Tablespace: "test_tablespace", FileLocation: "test_filespace"}
		It("prints a basic tablespace with a filespace", func() {
			emptyMetadataMap := backup.MetadataMap{}
			backup.PrintCreateTablespaceStatements(backupfile, tocfile, []backup.Tablespace{expectedTablespace}, emptyMetadataMap)
			testutils.ExpectEntry(tocfile.GlobalEntries, 0, "", "", "test_tablespace", toc.OBJ_TABLESPACE)
			testutils.AssertBufferContents(tocfile.GlobalEntries, buffer, `CREATE TABLESPACE test_tablespace FILESPACE test_filespace;`)
		})
		It("prints a tablespace with privileges, an owner, security label, and a comment", func() {
			tablespaceMetadataMap := testutils.DefaultMetadataMap(toc.OBJ_TABLESPACE, true, true, true, true)
			backup.PrintCreateTablespaceStatements(backupfile, tocfile, []backup.Tablespace{expectedTablespace}, tablespaceMetadataMap)
			expectedStatements := []string{
				`CREATE TABLESPACE test_tablespace FILESPACE test_filespace;`,
				`COMMENT ON TABLESPACE test_tablespace IS 'This is a tablespace comment.';`,
				`ALTER TABLESPACE test_tablespace OWNER TO testrole;`,
				`REVOKE ALL ON TABLESPACE test_tablespace FROM PUBLIC;
REVOKE ALL ON TABLESPACE test_tablespace FROM testrole;
GRANT ALL ON TABLESPACE test_tablespace TO testrole;`,
				`SECURITY LABEL FOR dummy ON TABLESPACE test_tablespace IS 'unclassified';`}
			testutils.AssertBufferContents(tocfile.GlobalEntries, buffer, expectedStatements...)

		})
		It("prints a tablespace with no per-segment tablespaces", func() {
			expectedTablespace := backup.Tablespace{
				Oid: 1, Tablespace: "test_tablespace", FileLocation: "'/data/dir'",
				SegmentLocations: []string{},
			}
			emptyMetadataMap := backup.MetadataMap{}
			backup.PrintCreateTablespaceStatements(backupfile, tocfile, []backup.Tablespace{expectedTablespace}, emptyMetadataMap)
			testutils.ExpectEntry(tocfile.GlobalEntries, 0, "", "", "test_tablespace", toc.OBJ_TABLESPACE)
			testutils.AssertBufferContents(tocfile.GlobalEntries, buffer, `CREATE TABLESPACE test_tablespace LOCATION '/data/dir';`)
		})
		It("prints a tablespace with per-segment tablespaces", func() {
			expectedTablespace := backup.Tablespace{
				Oid: 1, Tablespace: "test_tablespace", FileLocation: "'/data/dir'",
				SegmentLocations: []string{"content1='/data/dir1'", "content2='/data/dir2'", "content3='/data/dir3'"},
			}
			emptyMetadataMap := backup.MetadataMap{}
			backup.PrintCreateTablespaceStatements(backupfile, tocfile, []backup.Tablespace{expectedTablespace}, emptyMetadataMap)
			testutils.ExpectEntry(tocfile.GlobalEntries, 0, "", "", "test_tablespace", toc.OBJ_TABLESPACE)
			testutils.AssertBufferContents(tocfile.GlobalEntries, buffer, `CREATE TABLESPACE test_tablespace LOCATION '/data/dir'
	WITH (content1='/data/dir1', content2='/data/dir2', content3='/data/dir3');`)
		})
		It("prints a tablespace with options", func() {
			expectedTablespace := backup.Tablespace{
				Oid: 1, Tablespace: "test_tablespace", FileLocation: "'/data/dir'",
				SegmentLocations: []string{},
				Options:          "param1=val1, param2=val2",
			}
			emptyMetadataMap := backup.MetadataMap{}
			backup.PrintCreateTablespaceStatements(backupfile, tocfile, []backup.Tablespace{expectedTablespace}, emptyMetadataMap)
			testutils.ExpectEntry(tocfile.GlobalEntries, 0, "", "", "test_tablespace", toc.OBJ_TABLESPACE)
			testutils.ExpectEntry(tocfile.GlobalEntries, 1, "", "", "test_tablespace", toc.OBJ_TABLESPACE)
			expectedStatements := []string{`CREATE TABLESPACE test_tablespace LOCATION '/data/dir';`,
				`ALTER TABLESPACE test_tablespace SET (param1=val1, param2=val2);`}
			testutils.AssertBufferContents(tocfile.GlobalEntries, buffer, expectedStatements...)
		})
	})
})
