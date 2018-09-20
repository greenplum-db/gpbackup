package integration

import (
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup integration tests", func() {
	Describe("GetDependencies", func() {
		It("correctly constructs table inheritance dependencies", func() {
			testhelper.AssertQueryRuns(connection, "CREATE TABLE public.foo(i int, j text, k bool)")
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.foo")
			testhelper.AssertQueryRuns(connection, "CREATE TABLE public.bar(m int) inherits (public.foo)")
			defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.bar")

			oidFoo := testutils.OidFromObjectName(connection, "public", "foo", backup.TYPE_RELATION)
			oidBar := testutils.OidFromObjectName(connection, "public", "bar", backup.TYPE_RELATION)
			fooEntry := backup.DepEntry{Classid: backup.PG_CLASS_OID, Objid: oidFoo}
			barEntry := backup.DepEntry{Classid: backup.PG_CLASS_OID, Objid: oidBar}
			backupSet := map[backup.DepEntry]bool{fooEntry: true, barEntry: true}

			deps := backup.GetDependencies(connection, backupSet)

			Expect(deps).To(HaveLen(1))
			Expect(deps[barEntry]).To(HaveLen(1))
			Expect(deps[barEntry]).To(HaveKey(fooEntry))
		})
		It("constructs dependencies correctly for a table dependent on a protocol", func() {
			testhelper.AssertQueryRuns(connection, `CREATE FUNCTION public.read_from_s3() RETURNS integer
		AS '$libdir/gps3ext.so', 's3_import'
		LANGUAGE c STABLE NO SQL;`)
			defer testhelper.AssertQueryRuns(connection, "DROP FUNCTION public.read_from_s3()")
			testhelper.AssertQueryRuns(connection, `CREATE PROTOCOL s3 (readfunc = public.read_from_s3);`)
			defer testhelper.AssertQueryRuns(connection, "DROP PROTOCOL s3")
			testhelper.AssertQueryRuns(connection, `CREATE EXTERNAL TABLE public.ext_tbl (
		i int
		) LOCATION (
		's3://192.168.0.1'
		)
		FORMAT 'csv' (delimiter E',' null E'' escape E'"' quote E'"')
		ENCODING 'UTF8';`)
			defer testhelper.AssertQueryRuns(connection, "DROP EXTERNAL TABLE public.ext_tbl")

			tableOid := testutils.OidFromObjectName(connection, "public", "ext_tbl", backup.TYPE_RELATION)
			protocolOid := testutils.OidFromObjectName(connection, "", "s3", backup.TYPE_PROTOCOL)
			functionOid := testutils.OidFromObjectName(connection, "public", "read_from_s3", backup.TYPE_FUNCTION)

			tableEntry := backup.DepEntry{Classid: backup.PG_CLASS_OID, Objid: tableOid}
			protocolEntry := backup.DepEntry{Classid: backup.PG_EXTPROTOCOL_OID, Objid: protocolOid}
			functionEntry := backup.DepEntry{Classid: backup.PG_PROC_OID, Objid: functionOid}
			backupSet := map[backup.DepEntry]bool{tableEntry: true, protocolEntry: true, functionEntry: true}

			deps := backup.GetDependencies(connection, backupSet)
			if connection.Version.Is("4") {
				tables := backup.GetAllUserTables(connection)
				tableDefs := backup.ConstructDefinitionsForTables(connection, tables)
				protocols := backup.GetExternalProtocols(connection)
				backup.AddProtocolDependenciesForGPDB4(deps, tables, tableDefs, protocols)
			}

			Expect(deps).To(HaveLen(2))
			Expect(deps[tableEntry]).To(HaveLen(1))
			Expect(deps[tableEntry]).To(HaveKey(protocolEntry))
			Expect(deps[protocolEntry]).To(HaveLen(1))
			Expect(deps[protocolEntry]).To(HaveKey(functionEntry))
		})
		It("constructs dependencies correctly for a view that depends on two other views", func() {
			testhelper.AssertQueryRuns(connection, "CREATE VIEW public.parent1 AS SELECT relname FROM pg_class")
			defer testhelper.AssertQueryRuns(connection, "DROP VIEW public.parent1")
			testhelper.AssertQueryRuns(connection, "CREATE VIEW public.parent2 AS SELECT relname FROM pg_class")
			defer testhelper.AssertQueryRuns(connection, "DROP VIEW public.parent2")
			testhelper.AssertQueryRuns(connection, "CREATE VIEW public.child AS (SELECT * FROM public.parent1 UNION SELECT * FROM public.parent2)")
			defer testhelper.AssertQueryRuns(connection, "DROP VIEW public.child")

			parent1Oid := testutils.OidFromObjectName(connection, "public", "parent1", backup.TYPE_RELATION)
			parent2Oid := testutils.OidFromObjectName(connection, "public", "parent2", backup.TYPE_RELATION)
			childOid := testutils.OidFromObjectName(connection, "public", "child", backup.TYPE_RELATION)

			parent1Entry := backup.DepEntry{Classid: backup.PG_CLASS_OID, Objid: parent1Oid}
			parent2Entry := backup.DepEntry{Classid: backup.PG_CLASS_OID, Objid: parent2Oid}
			childEntry := backup.DepEntry{Classid: backup.PG_CLASS_OID, Objid: childOid}
			backupSet := map[backup.DepEntry]bool{parent1Entry: true, parent2Entry: true, childEntry: true}

			deps := backup.GetDependencies(connection, backupSet)

			Expect(deps).To(HaveLen(1))
			Expect(deps[childEntry]).To(HaveLen(2))
			Expect(deps[childEntry]).To(HaveKey(parent1Entry))
			Expect(deps[childEntry]).To(HaveKey(parent2Entry))
		})
		Describe("function dependencies", func() {
			var compositeEntry backup.DepEntry
			BeforeEach(func() {
				testhelper.AssertQueryRuns(connection, "CREATE TYPE public.composite_ints AS (one integer, two integer)")
				compositeOid := testutils.OidFromObjectName(connection, "public", "composite_ints", backup.TYPE_TYPE)
				compositeEntry = backup.DepEntry{Classid: backup.PG_TYPE_OID, Objid: compositeOid}
			})
			AfterEach(func() {
				testhelper.AssertQueryRuns(connection, "DROP TYPE public.composite_ints CASCADE")
			})
			It("constructs dependencies correctly for a function dependent on a user-defined type in the arguments", func() {
				testhelper.AssertQueryRuns(connection, "CREATE FUNCTION public.add(public.composite_ints) RETURNS integer STRICT IMMUTABLE LANGUAGE SQL AS 'SELECT ($1.one + $1.two);'")
				defer testhelper.AssertQueryRuns(connection, "DROP FUNCTION public.add(public.composite_ints)")

				functionOid := testutils.OidFromObjectName(connection, "public", "add", backup.TYPE_FUNCTION)
				funcEntry := backup.DepEntry{Classid: backup.PG_PROC_OID, Objid: functionOid}
				backupSet := map[backup.DepEntry]bool{funcEntry: true, compositeEntry: true}

				functionDeps := backup.GetDependencies(connection, backupSet)

				Expect(functionDeps).To(HaveLen(1))
				Expect(functionDeps[funcEntry]).To(HaveLen(1))
				Expect(functionDeps[funcEntry]).To(HaveKey(compositeEntry))
			})
			It("constructs dependencies correctly for a function dependent on a user-defined type in the return type", func() {
				testhelper.AssertQueryRuns(connection, "CREATE FUNCTION public.compose(integer, integer) RETURNS public.composite_ints STRICT IMMUTABLE LANGUAGE PLPGSQL AS 'DECLARE comp public.composite_ints; BEGIN SELECT $1, $2 INTO comp; RETURN comp; END;';")
				defer testhelper.AssertQueryRuns(connection, "DROP FUNCTION public.compose(integer, integer)")

				functionOid := testutils.OidFromObjectName(connection, "public", "compose", backup.TYPE_FUNCTION)
				funcEntry := backup.DepEntry{Classid: backup.PG_PROC_OID, Objid: functionOid}
				backupSet := map[backup.DepEntry]bool{funcEntry: true, compositeEntry: true}

				functionDeps := backup.GetDependencies(connection, backupSet)

				Expect(functionDeps).To(HaveLen(1))
				Expect(functionDeps[funcEntry]).To(HaveLen(1))
				Expect(functionDeps[funcEntry]).To(HaveKey(compositeEntry))
			})
			It("constructs dependencies correctly for a function dependent on an implicit array type", func() {
				testhelper.AssertQueryRuns(connection, "CREATE TYPE public.base_type")
				defer testhelper.AssertQueryRuns(connection, "DROP TYPE public.base_type CASCADE")
				testhelper.AssertQueryRuns(connection, "CREATE FUNCTION public.base_fn_in(cstring) RETURNS public.base_type AS 'boolin' LANGUAGE internal")
				testhelper.AssertQueryRuns(connection, "CREATE FUNCTION public.base_fn_out(public.base_type) RETURNS cstring AS 'boolout' LANGUAGE internal")
				testhelper.AssertQueryRuns(connection, "CREATE TYPE public.base_type(INPUT=public.base_fn_in, OUTPUT=public.base_fn_out)")
				testhelper.AssertQueryRuns(connection, "CREATE FUNCTION public.compose(public.base_type[], public.composite_ints) RETURNS public.composite_ints STRICT IMMUTABLE LANGUAGE PLPGSQL AS 'DECLARE comp public.composite_ints; BEGIN SELECT $1[0].one+$2.one, $1[0].two+$2.two INTO comp; RETURN comp; END;';")
				defer testhelper.AssertQueryRuns(connection, "DROP FUNCTION public.compose(public.base_type[], public.composite_ints)")

				functionOid := testutils.OidFromObjectName(connection, "public", "compose", backup.TYPE_FUNCTION)
				funcEntry := backup.DepEntry{Classid: backup.PG_PROC_OID, Objid: functionOid}
				baseOid := testutils.OidFromObjectName(connection, "public", "base_type", backup.TYPE_TYPE)
				baseEntry := backup.DepEntry{Classid: backup.PG_TYPE_OID, Objid: baseOid}
				backupSet := map[backup.DepEntry]bool{funcEntry: true, compositeEntry: true, baseEntry: true}

				functionDeps := backup.GetDependencies(connection, backupSet)

				Expect(functionDeps).To(HaveLen(1))
				Expect(functionDeps[funcEntry]).To(HaveLen(2))
				Expect(functionDeps[funcEntry]).To(HaveKey(compositeEntry))
				Expect(functionDeps[funcEntry]).To(HaveKey(baseEntry))
			})
		})
		Describe("type dependencies", func() {
			var (
				baseOid   uint32
				baseEntry backup.DepEntry
			)
			BeforeEach(func() {
				testhelper.AssertQueryRuns(connection, "CREATE TYPE public.base_type")
				testhelper.AssertQueryRuns(connection, "CREATE FUNCTION public.base_fn_in(cstring) RETURNS public.base_type AS 'boolin' LANGUAGE internal")
				testhelper.AssertQueryRuns(connection, "CREATE FUNCTION public.base_fn_out(public.base_type) RETURNS cstring AS 'boolout' LANGUAGE internal")
				testhelper.AssertQueryRuns(connection, "CREATE TYPE public.base_type(INPUT=public.base_fn_in, OUTPUT=public.base_fn_out)")

				baseOid = testutils.OidFromObjectName(connection, "public", "base_type", backup.TYPE_TYPE)
				baseEntry = backup.DepEntry{Classid: backup.PG_TYPE_OID, Objid: baseOid}
			})
			AfterEach(func() {
				testhelper.AssertQueryRuns(connection, "DROP TYPE public.base_type CASCADE")
			})
			It("constructs domain dependencies on user-defined types", func() {
				testhelper.AssertQueryRuns(connection, "CREATE DOMAIN public.parent_domain AS integer")
				defer testhelper.AssertQueryRuns(connection, "DROP DOMAIN public.parent_domain")
				testhelper.AssertQueryRuns(connection, "CREATE DOMAIN public.domain_type AS public.parent_domain")
				defer testhelper.AssertQueryRuns(connection, "DROP DOMAIN public.domain_type")

				domainOid := testutils.OidFromObjectName(connection, "public", "parent_domain", backup.TYPE_TYPE)
				domain2Oid := testutils.OidFromObjectName(connection, "public", "domain_type", backup.TYPE_TYPE)

				domainEntry := backup.DepEntry{Classid: backup.PG_TYPE_OID, Objid: domainOid}
				domain2Entry := backup.DepEntry{Classid: backup.PG_TYPE_OID, Objid: domain2Oid}
				backupSet := map[backup.DepEntry]bool{domainEntry: true, domain2Entry: true}

				deps := backup.GetDependencies(connection, backupSet)

				Expect(deps).To(HaveLen(1))
				Expect(deps[domain2Entry]).To(HaveLen(1))
				Expect(deps[domain2Entry]).To(HaveKey(domainEntry))
			})

			It("constructs dependencies correctly for a function/base type dependency loop", func() {
				baseInOid := testutils.OidFromObjectName(connection, "public", "base_fn_in", backup.TYPE_FUNCTION)
				baseOutOid := testutils.OidFromObjectName(connection, "public", "base_fn_out", backup.TYPE_FUNCTION)

				baseInEntry := backup.DepEntry{Classid: backup.PG_PROC_OID, Objid: baseInOid}
				baseOutEntry := backup.DepEntry{Classid: backup.PG_PROC_OID, Objid: baseOutOid}
				backupSet := map[backup.DepEntry]bool{baseEntry: true, baseInEntry: true, baseOutEntry: true}

				deps := backup.GetDependencies(connection, backupSet)

				Expect(deps).To(HaveLen(1))
				Expect(deps[baseEntry]).To(HaveLen(2))
				Expect(deps[baseEntry]).To(HaveKey(baseInEntry))
				Expect(deps[baseEntry]).To(HaveKey(baseOutEntry))
			})
			It("constructs dependencies correctly for a composite type dependent on one user-defined type", func() {
				testhelper.AssertQueryRuns(connection, "CREATE TYPE public.comp_type AS (base public.base_type, builtin integer)")
				defer testhelper.AssertQueryRuns(connection, "DROP TYPE public.comp_type")

				compositeOid := testutils.OidFromObjectName(connection, "public", "comp_type", backup.TYPE_TYPE)
				compositeEntry := backup.DepEntry{Classid: backup.PG_TYPE_OID, Objid: compositeOid}
				backupSet := map[backup.DepEntry]bool{baseEntry: true, compositeEntry: true}

				deps := backup.GetDependencies(connection, backupSet)

				Expect(deps).To(HaveLen(1))
				Expect(deps[compositeEntry]).To(HaveLen(1))
				Expect(deps[compositeEntry]).To(HaveKey(baseEntry))
			})
			It("constructs dependencies correctly for a composite type dependent on multiple user-defined types", func() {
				testhelper.AssertQueryRuns(connection, "CREATE TYPE public.base_type2")
				defer testhelper.AssertQueryRuns(connection, "DROP TYPE public.base_type2 CASCADE")
				testhelper.AssertQueryRuns(connection, "CREATE FUNCTION public.base_fn_in2(cstring) RETURNS public.base_type2 AS 'boolin' LANGUAGE internal")
				testhelper.AssertQueryRuns(connection, "CREATE FUNCTION public.base_fn_out2(public.base_type2) RETURNS cstring AS 'boolout' LANGUAGE internal")
				testhelper.AssertQueryRuns(connection, "CREATE TYPE public.base_type2(INPUT=public.base_fn_in2, OUTPUT=public.base_fn_out2)")

				testhelper.AssertQueryRuns(connection, "CREATE TYPE public.comp_type AS (base public.base_type, base2 public.base_type2)")
				defer testhelper.AssertQueryRuns(connection, "DROP TYPE public.comp_type")

				base2Oid := testutils.OidFromObjectName(connection, "public", "base_type2", backup.TYPE_TYPE)
				base2Entry := backup.DepEntry{Classid: backup.PG_TYPE_OID, Objid: base2Oid}
				compositeOid := testutils.OidFromObjectName(connection, "public", "comp_type", backup.TYPE_TYPE)
				compositeEntry := backup.DepEntry{Classid: backup.PG_TYPE_OID, Objid: compositeOid}
				backupSet := map[backup.DepEntry]bool{baseEntry: true, base2Entry: true, compositeEntry: true}

				deps := backup.GetDependencies(connection, backupSet)

				Expect(deps).To(HaveLen(1))
				Expect(deps[compositeEntry]).To(HaveLen(2))
				Expect(deps[compositeEntry]).To(HaveKey(baseEntry))
				Expect(deps[compositeEntry]).To(HaveKey(base2Entry))
			})
			It("constructs dependencies correctly for a composite type dependent on the same user-defined type multiple times", func() {
				testhelper.AssertQueryRuns(connection, "CREATE TYPE public.comp_type AS (base public.base_type, base2 public.base_type)")
				defer testhelper.AssertQueryRuns(connection, "DROP TYPE public.comp_type")

				compositeOid := testutils.OidFromObjectName(connection, "public", "comp_type", backup.TYPE_TYPE)
				compositeEntry := backup.DepEntry{Classid: backup.PG_TYPE_OID, Objid: compositeOid}
				backupSet := map[backup.DepEntry]bool{baseEntry: true, compositeEntry: true}

				deps := backup.GetDependencies(connection, backupSet)

				Expect(deps).To(HaveLen(1))
				Expect(deps[compositeEntry]).To(HaveLen(1))
				Expect(deps[compositeEntry]).To(HaveKey(baseEntry))
			})
			It("constructs dependencies correctly for a composite type dependent on a table", func() {
				testhelper.AssertQueryRuns(connection, "CREATE TABLE public.my_table(i int)")
				defer testhelper.AssertQueryRuns(connection, "DROP TABLE public.my_table")
				testhelper.AssertQueryRuns(connection, "CREATE TYPE public.my_type AS (type1 public.my_table)")
				defer testhelper.AssertQueryRuns(connection, "DROP TYPE public.my_type")

				tableOid := testutils.OidFromObjectName(connection, "public", "my_table", backup.TYPE_RELATION)
				typeOid := testutils.OidFromObjectName(connection, "public", "my_type", backup.TYPE_TYPE)

				tableEntry := backup.DepEntry{Classid: backup.PG_CLASS_OID, Objid: tableOid}
				typeEntry := backup.DepEntry{Classid: backup.PG_TYPE_OID, Objid: typeOid}
				backupSet := map[backup.DepEntry]bool{tableEntry: true, typeEntry: true}

				deps := backup.GetDependencies(connection, backupSet)

				Expect(deps).To(HaveLen(1))
				Expect(deps[typeEntry]).To(HaveLen(1))
				Expect(deps[typeEntry]).To(HaveKey(tableEntry))
			})
		})
	})
})
