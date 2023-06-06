package integration

import (
	"database/sql"
	"fmt"

	"github.com/greenplum-db/gp-common-go-libs/structmatcher"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/options"
	"github.com/greenplum-db/gpbackup/testutils"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup integration tests", func() {
	Describe("GetFunctions", func() {
		var prokindValue string
		var plannerSupportValue string
		var proparallelValue string
		BeforeEach(func() {
			testutils.SkipIfBefore5(connectionPool)
			if connectionPool.Version.AtLeast("7") {
				prokindValue = "f"
				plannerSupportValue = "-"
				proparallelValue = "u"
			} else {
				prokindValue = ""
				plannerSupportValue = ""
				proparallelValue = ""
			}
		})
		It("returns a slice of functions", func() {
			testhelper.AssertQueryRuns(connectionPool, `CREATE FUNCTION public.add(integer, integer) RETURNS integer
AS 'SELECT $1 + $2'
LANGUAGE SQL`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP FUNCTION public.add(integer, integer)")
			testhelper.AssertQueryRuns(connectionPool, `
CREATE FUNCTION public.append(integer, integer) RETURNS SETOF record
AS 'SELECT ($1, $2)'
LANGUAGE SQL
SECURITY DEFINER
STRICT
STABLE
COST 200
ROWS 200
SET search_path = pg_temp
MODIFIES SQL DATA
`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP FUNCTION public.append(integer, integer)")
			testhelper.AssertQueryRuns(connectionPool, "COMMENT ON FUNCTION public.append(integer, integer) IS 'this is a function comment'")

			results := backup.GetFunctions(connectionPool)

			addFunction := backup.Function{
				Schema: "public", Name: "add", Kind: prokindValue, ReturnsSet: false, FunctionBody: "SELECT $1 + $2",
				BinaryPath: "", Arguments: sql.NullString{String: "integer, integer", Valid: true},
				IdentArgs:  sql.NullString{String: "integer, integer", Valid: true},
				ResultType: sql.NullString{String: "integer", Valid: true},
				Volatility: "v", IsStrict: false, IsSecurityDefiner: false, PlannerSupport: plannerSupportValue, Config: "", Cost: 100, NumRows: 0,
				DataAccess: "c", Language: "sql", ExecLocation: "a", Parallel: proparallelValue}
			appendFunction := backup.Function{
				Schema: "public", Name: "append", Kind: prokindValue, ReturnsSet: true, FunctionBody: "SELECT ($1, $2)",
				BinaryPath: "", Arguments: sql.NullString{String: "integer, integer", Valid: true},
				IdentArgs:  sql.NullString{String: "integer, integer", Valid: true},
				ResultType: sql.NullString{String: "SETOF record", Valid: true},
				Volatility: "s", IsStrict: true, IsSecurityDefiner: true, PlannerSupport: plannerSupportValue, Config: `SET search_path TO 'pg_temp'`, Cost: 200,
				NumRows: 200, DataAccess: "m", Language: "sql", ExecLocation: "a", Parallel: proparallelValue}
			if connectionPool.Version.AtLeast("7") {
				addFunction.DataAccess = ""
				appendFunction.DataAccess = ""
			}

			Expect(results).To(HaveLen(2))
			structmatcher.ExpectStructsToMatchExcluding(&results[0], &addFunction, "Oid")
			structmatcher.ExpectStructsToMatchExcluding(&results[1], &appendFunction, "Oid")
		})
		It("returns a slice of functions in a specific schema", func() {
			testhelper.AssertQueryRuns(connectionPool, `CREATE FUNCTION public.add(integer, integer) RETURNS integer
AS 'SELECT $1 + $2'
LANGUAGE SQL`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP FUNCTION public.add(integer, integer)")
			testhelper.AssertQueryRuns(connectionPool, "CREATE SCHEMA testschema")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA testschema")
			testhelper.AssertQueryRuns(connectionPool, `CREATE FUNCTION testschema.add(integer, integer) RETURNS integer
AS 'SELECT $1 + $2'
LANGUAGE SQL`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP FUNCTION testschema.add(integer, integer)")

			_ = backupCmdFlags.Set(options.INCLUDE_SCHEMA, "testschema")
			results := backup.GetFunctions(connectionPool)

			addFunction := backup.Function{
				Schema: "testschema", Name: "add", Kind: prokindValue, ReturnsSet: false, FunctionBody: "SELECT $1 + $2",
				BinaryPath: "", Arguments: sql.NullString{String: "integer, integer", Valid: true},
				IdentArgs:  sql.NullString{String: "integer, integer", Valid: true},
				ResultType: sql.NullString{String: "integer", Valid: true},
				Volatility: "v", IsStrict: false, IsSecurityDefiner: false, PlannerSupport: plannerSupportValue, Config: "", Cost: 100, NumRows: 0, DataAccess: "c",
				Language: "sql", ExecLocation: "a", Parallel: proparallelValue}
			if connectionPool.Version.AtLeast("7") {
				addFunction.DataAccess = ""
			}

			Expect(results).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&results[0], &addFunction, "Oid")
		})
		It("returns a window function", func() {
			testutils.SkipIfBefore6(connectionPool)
			if connectionPool.Version.AtLeast("7") {
				// GPDB7 only allows set-returning functions to execute on coordinator
				testhelper.AssertQueryRuns(connectionPool, `CREATE FUNCTION public.add(integer, integer) RETURNS SETOF integer
AS 'SELECT $1 + $2'
LANGUAGE SQL WINDOW`)
			} else {
				testhelper.AssertQueryRuns(connectionPool, `CREATE FUNCTION public.add(integer, integer) RETURNS integer
AS 'SELECT $1 + $2'
LANGUAGE SQL WINDOW`)
			}
			defer testhelper.AssertQueryRuns(connectionPool, "DROP FUNCTION public.add(integer, integer)")

			results := backup.GetFunctions(connectionPool)

			var windowFunction backup.Function
			if connectionPool.Version.AtLeast("7") {
				windowFunction = backup.Function{
					Schema: "public", Name: "add", ReturnsSet: true, FunctionBody: "SELECT $1 + $2",
					BinaryPath: "", Arguments: sql.NullString{String: "integer, integer", Valid: true},
					IdentArgs:  sql.NullString{String: "integer, integer", Valid: true},
					ResultType: sql.NullString{String: "SETOF integer", Valid: true},
					Volatility: "v", IsStrict: false, IsSecurityDefiner: false, PlannerSupport: plannerSupportValue,
					Config: "", Cost: 100, NumRows: 1000, DataAccess: "", Language: "sql", Kind: "w", ExecLocation: "a",
					Parallel: proparallelValue, IsWindow: true}
			} else {
				windowFunction = backup.Function{
					Schema: "public", Name: "add", ReturnsSet: false, FunctionBody: "SELECT $1 + $2",
					BinaryPath: "", Arguments: sql.NullString{String: "integer, integer", Valid: true},
					IdentArgs:  sql.NullString{String: "integer, integer", Valid: true},
					ResultType: sql.NullString{String: "integer", Valid: true},
					Volatility: "v", IsStrict: false, IsSecurityDefiner: false,
					PlannerSupport: plannerSupportValue, Config: "", Cost: 100, NumRows: 0, DataAccess: "c",
					Language: "sql", IsWindow: true, ExecLocation: "a", Parallel: proparallelValue}
			}
			Expect(results).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&results[0], &windowFunction, "Oid")
		})
		It("returns a function to execute on coordinator and all segments", func() {
			testutils.SkipIfBefore6(connectionPool)

			if connectionPool.Version.Is("6") {
				testhelper.AssertQueryRuns(connectionPool, `CREATE FUNCTION public.srf_on_coordinator(integer, integer) RETURNS integer
AS 'SELECT $1 + $2'
LANGUAGE SQL WINDOW
EXECUTE ON MASTER;`)
				testhelper.AssertQueryRuns(connectionPool, `CREATE FUNCTION public.srf_on_all_segments(integer, integer) RETURNS integer
AS 'SELECT $1 + $2'
LANGUAGE SQL WINDOW
EXECUTE ON ALL SEGMENTS;`)
			} else {
				testhelper.AssertQueryRuns(connectionPool, `CREATE FUNCTION public.srf_on_coordinator(integer, integer) RETURNS SETOF integer
AS 'SELECT $1 + $2'
LANGUAGE SQL WINDOW
EXECUTE ON COORDINATOR;`)
				testhelper.AssertQueryRuns(connectionPool, `CREATE FUNCTION public.srf_on_all_segments(integer, integer) RETURNS SETOF integer
AS 'SELECT $1 + $2'
LANGUAGE SQL WINDOW
EXECUTE ON ALL SEGMENTS;`)
			}

			defer testhelper.AssertQueryRuns(connectionPool, "DROP FUNCTION public.srf_on_coordinator(integer, integer)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP FUNCTION public.srf_on_all_segments(integer, integer)")

			results := backup.GetFunctions(connectionPool)
			var prokindValue string
			if connectionPool.Version.AtLeast("7") {
				prokindValue = "w"
			} else {
				prokindValue = ""
			}

			srfOnCoordinatorFunction := backup.Function{
				Schema: "public", Name: "srf_on_coordinator", Kind: prokindValue, ReturnsSet: false, FunctionBody: "SELECT $1 + $2",
				BinaryPath: "", Arguments: sql.NullString{String: "integer, integer", Valid: true},
				IdentArgs:  sql.NullString{String: "integer, integer", Valid: true},
				ResultType: sql.NullString{String: "integer", Valid: true},
				Volatility: "v", IsStrict: false, IsSecurityDefiner: false,
				PlannerSupport: plannerSupportValue, Config: "", Cost: 100, NumRows: 0, DataAccess: "c",
				Language: "sql", IsWindow: true, ExecLocation: "m", Parallel: proparallelValue}
			if connectionPool.Version.AtLeast("7") {
				srfOnCoordinatorFunction.ExecLocation = "c"

				// GPDB7 only allows set-returning functions to execute on coordinator
				srfOnCoordinatorFunction.ReturnsSet = true
				srfOnCoordinatorFunction.NumRows = 1000
				srfOnCoordinatorFunction.ResultType = sql.NullString{String: "SETOF integer", Valid: true}
				srfOnCoordinatorFunction.DataAccess = ""
			}

			srfOnAllSegmentsFunction := backup.Function{
				Schema: "public", Name: "srf_on_all_segments", Kind: prokindValue, ReturnsSet: false, FunctionBody: "SELECT $1 + $2",
				BinaryPath: "", Arguments: sql.NullString{String: "integer, integer", Valid: true},
				IdentArgs:  sql.NullString{String: "integer, integer", Valid: true},
				ResultType: sql.NullString{String: "integer", Valid: true},
				Volatility: "v", IsStrict: false, IsSecurityDefiner: false,
				PlannerSupport: plannerSupportValue, Config: "", Cost: 100, NumRows: 0, DataAccess: "c",
				Language: "sql", IsWindow: true, ExecLocation: "s", Parallel: proparallelValue}
			if connectionPool.Version.AtLeast("7") {
				// GPDB7 only allows set-returning functions to execute on all segments
				srfOnAllSegmentsFunction.ReturnsSet = true
				srfOnAllSegmentsFunction.NumRows = 1000
				srfOnAllSegmentsFunction.ResultType = sql.NullString{String: "SETOF integer", Valid: true}
				srfOnAllSegmentsFunction.DataAccess = ""
			}

			Expect(results).To(HaveLen(2))
			structmatcher.ExpectStructsToMatchExcluding(&results[0], &srfOnAllSegmentsFunction, "Oid")
			structmatcher.ExpectStructsToMatchExcluding(&results[1], &srfOnCoordinatorFunction, "Oid")
		})
		It("returns a function to execute on initplan", func() {
			if connectionPool.Version.Before("6.5") {
				Skip("Test only applicable to GPDB6.5 and above")
			}

			if connectionPool.Version.AtLeast("7") {
				// GPDB7 only allows set-returning functions to execute on coordinator
				testhelper.AssertQueryRuns(connectionPool, `CREATE FUNCTION public.srf_on_initplan(integer, integer) RETURNS SETOF integer
AS 'SELECT $1 + $2'
LANGUAGE SQL WINDOW
EXECUTE ON INITPLAN;`)
			} else {
				testhelper.AssertQueryRuns(connectionPool, `CREATE FUNCTION public.srf_on_initplan(integer, integer) RETURNS integer
AS 'SELECT $1 + $2'
LANGUAGE SQL WINDOW
EXECUTE ON INITPLAN;`)
			}
			defer testhelper.AssertQueryRuns(connectionPool, "DROP FUNCTION public.srf_on_initplan(integer, integer)")

			results := backup.GetFunctions(connectionPool)

			var srfOnInitplan backup.Function
			if connectionPool.Version.AtLeast("7") {
				// GPDB7 only allows set-returning functions to execute on coordinator
				srfOnInitplan = backup.Function{
					Schema: "public", Name: "srf_on_initplan", ReturnsSet: true, FunctionBody: "SELECT $1 + $2",
					BinaryPath: "", Arguments: sql.NullString{String: "integer, integer", Valid: true},
					IdentArgs:  sql.NullString{String: "integer, integer", Valid: true},
					ResultType: sql.NullString{String: "SETOF integer", Valid: true},
					Volatility: "v", IsStrict: false, IsSecurityDefiner: false, Config: "", Cost: 100, NumRows: 1000, DataAccess: "",
					PlannerSupport: plannerSupportValue, Language: "sql", IsWindow: true, ExecLocation: "i",
					Parallel: proparallelValue, Kind: "w"}
			} else {
				srfOnInitplan = backup.Function{
					Schema: "public", Name: "srf_on_initplan", ReturnsSet: false, FunctionBody: "SELECT $1 + $2",
					BinaryPath: "", Arguments: sql.NullString{String: "integer, integer", Valid: true},
					IdentArgs:  sql.NullString{String: "integer, integer", Valid: true},
					ResultType: sql.NullString{String: "integer", Valid: true},
					Volatility: "v", IsStrict: false, IsSecurityDefiner: false, Config: "", Cost: 100, NumRows: 0, DataAccess: "c",
					PlannerSupport: plannerSupportValue, Language: "sql", IsWindow: true, ExecLocation: "i",
					Parallel: proparallelValue}
			}
			Expect(results).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&results[0], &srfOnInitplan, "Oid")
		})
		It("returns a function with LEAKPROOF", func() {
			testutils.SkipIfBefore6(connectionPool)
			testhelper.AssertQueryRuns(connectionPool, `
CREATE FUNCTION public.append(integer, integer) RETURNS SETOF record
AS 'SELECT ($1, $2)'
LANGUAGE SQL
SECURITY DEFINER
STRICT
LEAKPROOF
STABLE
COST 200
ROWS 200
SET search_path = pg_temp
MODIFIES SQL DATA
`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP FUNCTION public.append(integer, integer)")

			results := backup.GetFunctions(connectionPool)

			appendFunction := backup.Function{
				Schema: "public", Name: "append", Kind: prokindValue, ReturnsSet: true, FunctionBody: "SELECT ($1, $2)",
				BinaryPath: "", Arguments: sql.NullString{String: "integer, integer", Valid: true},
				IdentArgs:  sql.NullString{String: "integer, integer", Valid: true},
				ResultType: sql.NullString{String: "SETOF record", Valid: true},
				Volatility: "s", IsStrict: true, IsLeakProof: true, IsSecurityDefiner: true,
				PlannerSupport: plannerSupportValue, Config: `SET search_path TO 'pg_temp'`, Cost: 200,
				NumRows: 200, DataAccess: "m", Language: "sql", ExecLocation: "a", Parallel: proparallelValue}
			if connectionPool.Version.AtLeast("7") {
				appendFunction.DataAccess = ""
			}

			Expect(results).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&results[0], &appendFunction, "Oid")
		})
		It("does not return range type constructor functions", func() {
			testutils.SkipIfBefore6(connectionPool)
			testhelper.AssertQueryRuns(connectionPool, "CREATE TYPE public.textrange AS RANGE (SUBTYPE = pg_catalog.text)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TYPE public.textrange")

			results := backup.GetFunctions(connectionPool)

			Expect(results).To(HaveLen(0))
		})
		It("returns a function that has quotes in Config", func() {
			testhelper.AssertQueryRuns(connectionPool, `
	CREATE FUNCTION public.myfunc(integer) RETURNS text
	LANGUAGE plpgsql NO SQL
		SET work_mem TO '1MB'
	AS $_$
	begin
		set work_mem = '2MB';
		perform 1/$1;
		return current_setting('work_mem');
	end $_$;
`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP FUNCTION public.myfunc(integer)")

			results := backup.GetFunctions(connectionPool)
			appendFunction := backup.Function{
				Schema: "public", Name: "myfunc", Kind: prokindValue, ReturnsSet: false, FunctionBody: `
	begin
		set work_mem = '2MB';
		perform 1/$1;
		return current_setting('work_mem');
	end `,
				BinaryPath: "", Arguments: sql.NullString{String: "integer", Valid: true},
				IdentArgs:  sql.NullString{String: "integer", Valid: true},
				ResultType: sql.NullString{String: "text", Valid: true},
				Volatility: "v", IsStrict: false, IsLeakProof: false, IsSecurityDefiner: false,
				PlannerSupport: plannerSupportValue, Config: "SET work_mem TO '1MB'", Cost: 100,
				NumRows: 0, DataAccess: "n", Language: "plpgsql", ExecLocation: "a", Parallel: proparallelValue}
			if connectionPool.Version.AtLeast("7") {
				appendFunction.DataAccess = ""
			}
			Expect(results).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&results[0], &appendFunction, "Oid")
		})
		It("returns a function that sets a GUC with a string array value with quoted items", func() {
			testhelper.AssertQueryRuns(connectionPool, `CREATE SCHEMA "abc""def"`)

			testhelper.AssertQueryRuns(connectionPool, `
	CREATE FUNCTION public.myfunc(integer) RETURNS text
    LANGUAGE plpgsql NO SQL
    SET search_path TO "$user", public, "abc""def"
    AS $_$
    begin
        set work_mem = '2MB';
        perform 1/$1;
        return current_setting('work_mem');
    end $_$;
`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP FUNCTION public.myfunc(integer)")
			defer testhelper.AssertQueryRuns(connectionPool, `DROP SCHEMA "abc""def"`)

			results := backup.GetFunctions(connectionPool)

			appendFunction := backup.Function{
				Schema: "public", Name: "myfunc", Kind: prokindValue, ReturnsSet: false, FunctionBody: `
    begin
        set work_mem = '2MB';
        perform 1/$1;
        return current_setting('work_mem');
    end `,
				BinaryPath: "", Arguments: sql.NullString{String: "integer", Valid: true},
				IdentArgs:  sql.NullString{String: "integer", Valid: true},
				ResultType: sql.NullString{String: "text", Valid: true},
				Volatility: "v", IsStrict: false, IsLeakProof: false, IsSecurityDefiner: false,
				PlannerSupport: plannerSupportValue, Config: `SET search_path TO '$user', 'public', 'abc"def'`, Cost: 100,
				NumRows: 0, DataAccess: "n", Language: "plpgsql", ExecLocation: "a", Parallel: proparallelValue}
			if connectionPool.Version.AtLeast("7") {
				appendFunction.DataAccess = ""
			}

			Expect(results).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&results[0], &appendFunction, "Oid")
		})
		It("includes stored procedures in the result", func() {
			testutils.SkipIfBefore7(connectionPool)

			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE public.tbl (n int);`)
			defer testhelper.AssertQueryRuns(connectionPool, `DROP TABLE public.tbl;`)

			testhelper.AssertQueryRuns(connectionPool, `
CREATE PROCEDURE
public.insert_data(a integer, b integer) LANGUAGE SQL AS $$
INSERT INTO public.tbl VALUES (a);
INSERT INTO public.tbl VALUES (b);
$$;`)
			defer testhelper.AssertQueryRuns(connectionPool, `DROP PROCEDURE public.insert_data(a integer, b integer);`)

			testhelper.AssertQueryRuns(connectionPool, `
CREATE PROCEDURE
public.insert_more_data(a integer, b integer) LANGUAGE SQL AS $$
INSERT INTO public.tbl VALUES (a);
INSERT INTO public.tbl VALUES (b);
$$;`)
			defer testhelper.AssertQueryRuns(connectionPool, `DROP PROCEDURE public.insert_more_data(a integer, b integer);`)

			results := backup.GetFunctions(connectionPool)

			firstProcedure := backup.Function{
				Schema: "public", Name: "insert_data", Kind: "p", ReturnsSet: false, FunctionBody: `
INSERT INTO public.tbl VALUES (a);
INSERT INTO public.tbl VALUES (b);
`,
				BinaryPath: "", Arguments: sql.NullString{String: "a integer, b integer", Valid: true},
				IdentArgs:  sql.NullString{String: "a integer, b integer", Valid: true},
				ResultType: sql.NullString{String: "", Valid: false},
				Volatility: "v", IsSecurityDefiner: false, PlannerSupport: plannerSupportValue,
				Config: "", Cost: 100, NumRows: 0, DataAccess: "c",
				Language: "sql", ExecLocation: "a", Parallel: proparallelValue}
			secondProcedure := backup.Function{
				Schema: "public", Name: "insert_more_data", Kind: "p", ReturnsSet: false, FunctionBody: `
INSERT INTO public.tbl VALUES (a);
INSERT INTO public.tbl VALUES (b);
`,
				BinaryPath: "", Arguments: sql.NullString{String: "a integer, b integer", Valid: true},
				IdentArgs:  sql.NullString{String: "a integer, b integer", Valid: true},
				ResultType: sql.NullString{String: "", Valid: false},
				Volatility: "v", IsSecurityDefiner: false, PlannerSupport: plannerSupportValue,
				Config: "", Cost: 100, NumRows: 0, DataAccess: "c",
				Language: "sql", ExecLocation: "a", Parallel: proparallelValue}
			if connectionPool.Version.AtLeast("7") {
				firstProcedure.DataAccess = ""
				secondProcedure.DataAccess = ""
			}

			Expect(results).To(HaveLen(2))
			structmatcher.ExpectStructsToMatchExcluding(&results[0], &firstProcedure, "Oid")
			structmatcher.ExpectStructsToMatchExcluding(&results[1], &secondProcedure, "Oid")
		})
	})
	Describe("GetFunctions4", func() {
		BeforeEach(func() {
			testutils.SkipIfNot4(connectionPool)
		})
		It("returns a slice of functions", func() {
			testhelper.AssertQueryRuns(connectionPool, `CREATE FUNCTION public.add(numeric, integer) RETURNS numeric
AS 'SELECT $1 + $2'
LANGUAGE SQL`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP FUNCTION public.add(numeric, integer)")
			testhelper.AssertQueryRuns(connectionPool, `
CREATE FUNCTION public.append(float, integer) RETURNS SETOF record
AS 'SELECT ($1, $2)'
LANGUAGE SQL
SECURITY DEFINER
STRICT
STABLE
`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP FUNCTION public.append(float, integer)")
			testhelper.AssertQueryRuns(connectionPool, "COMMENT ON FUNCTION public.append(float, integer) IS 'this is a function comment'")
			testhelper.AssertQueryRuns(connectionPool, `CREATE FUNCTION public."specChar"(t text, "precision" double precision) RETURNS double precision AS $$BEGIN RETURN precision + 1; END;$$ LANGUAGE PLPGSQL;`)
			defer testhelper.AssertQueryRuns(connectionPool, `DROP FUNCTION public."specChar"(text, double precision)`)

			results := backup.GetFunctions4(connectionPool)

			addFunction := backup.Function{
				Schema: "public", Name: "add", ReturnsSet: false, FunctionBody: "SELECT $1 + $2", BinaryPath: "",
				Volatility: "v", IsStrict: false, IsSecurityDefiner: false, NumRows: 0, Language: "sql", ExecLocation: "a"}
			appendFunction := backup.Function{
				Schema: "public", Name: "append", ReturnsSet: true, FunctionBody: "SELECT ($1, $2)", BinaryPath: "",
				Volatility: "s", IsStrict: true, IsSecurityDefiner: true, Language: "sql", ExecLocation: "a"}
			specCharFunction := backup.Function{
				Schema: "public", Name: `"specChar"`, ReturnsSet: false, FunctionBody: "BEGIN RETURN precision + 1; END;", BinaryPath: "",
				Volatility: "v", IsStrict: false, IsSecurityDefiner: false, NumRows: 0, Language: "plpgsql", ExecLocation: "a"}

			Expect(results).To(HaveLen(3))
			structmatcher.ExpectStructsToMatchExcluding(&results[0], &addFunction, "Oid")
			structmatcher.ExpectStructsToMatchExcluding(&results[1], &appendFunction, "Oid")
			structmatcher.ExpectStructsToMatchExcluding(&results[2], &specCharFunction, "Oid")
		})
		It("returns a slice of functions in a specific schema", func() {
			testhelper.AssertQueryRuns(connectionPool, `CREATE FUNCTION public.add(numeric, integer) RETURNS numeric
AS 'SELECT $1 + $2'
LANGUAGE SQL`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP FUNCTION public.add(numeric, integer)")
			testhelper.AssertQueryRuns(connectionPool, "CREATE SCHEMA testschema")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA testschema")
			testhelper.AssertQueryRuns(connectionPool, `CREATE FUNCTION testschema.add(float, integer) RETURNS float
AS 'SELECT $1 + $2'
LANGUAGE SQL`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP FUNCTION testschema.add(float, integer)")

			addFunction := backup.Function{
				Schema: "testschema", Name: "add", ReturnsSet: false, FunctionBody: "SELECT $1 + $2", BinaryPath: "",
				Volatility: "v", IsStrict: false, IsSecurityDefiner: false, Language: "sql", ExecLocation: "a"}
			_ = backupCmdFlags.Set(options.INCLUDE_SCHEMA, "testschema")
			results := backup.GetFunctions4(connectionPool)

			Expect(results).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&results[0], &addFunction, "Oid")
		})
	})
	Describe("GetAggregates", func() {
		BeforeEach(func() {
			testhelper.AssertQueryRuns(connectionPool, `
CREATE FUNCTION public.mysfunc_accum(numeric, numeric, numeric)
   RETURNS numeric
   AS 'select $1 + $2 + $3'
   LANGUAGE SQL
   IMMUTABLE;
`)
			testhelper.AssertQueryRuns(connectionPool, `
CREATE FUNCTION public.mypre_accum(numeric, numeric)
   RETURNS numeric
   AS 'select $1 + $2'
   LANGUAGE SQL
   IMMUTABLE
   RETURNS NULL ON NULL INPUT;
`)
		})
		AfterEach(func() {
			testhelper.AssertQueryRuns(connectionPool, "DROP FUNCTION public.mysfunc_accum(numeric, numeric, numeric)")
			testhelper.AssertQueryRuns(connectionPool, "DROP FUNCTION public.mypre_accum(numeric, numeric)")
		})
		It("creates an aggregate with sortop in pg_catalog", func() {
			testhelper.AssertQueryRuns(connectionPool, `
CREATE FUNCTION public.ascii_larger(prev character, curr character) RETURNS character
AS $$
begin if (prev ~>~ curr) then return prev; end if; return curr; end; $$
LANGUAGE plpgsql IMMUTABLE NO SQL;`)
			transitionOid := testutils.OidFromObjectName(connectionPool, "public", "ascii_larger", backup.TYPE_FUNCTION)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP FUNCTION public.ascii_larger(character, character)")

			testhelper.AssertQueryRuns(connectionPool, `
CREATE AGGREGATE public.ascii_max(character) (
SFUNC = public.ascii_larger,
STYPE = character,
SORTOP = ~>~ );`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP AGGREGATE public.ascii_max(character)")

			resultAggregates := backup.GetAggregates(connectionPool)
			aggregateDef := backup.Aggregate{
				Schema: "public", Name: "ascii_max", Arguments: sql.NullString{String: "character", Valid: true},
				IdentArgs:          sql.NullString{String: "character", Valid: true},
				TransitionFunction: transitionOid, FinalFunction: 0, SortOperator: "~>~", SortOperatorSchema: "pg_catalog", TransitionDataType: "character",
				InitialValue: "", InitValIsNull: true, MInitValIsNull: true, IsOrdered: false,
			}
			if connectionPool.Version.AtLeast("7") {
				aggregateDef.Kind = "n"
				aggregateDef.Finalmodify = "r"
				aggregateDef.Mfinalmodify = "r"
				aggregateDef.Parallel = "u"
			}

			Expect(resultAggregates).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&resultAggregates[0], &aggregateDef, "Oid")
		})
		It("returns a slice of aggregates", func() {
			testhelper.AssertQueryRuns(connectionPool, `
CREATE AGGREGATE public.agg_prefunc(numeric, numeric) (
	SFUNC = public.mysfunc_accum,
	STYPE = numeric,
	PREFUNC = public.mypre_accum,
	INITCOND = 0 );
`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP AGGREGATE public.agg_prefunc(numeric, numeric)")

			transitionOid := testutils.OidFromObjectName(connectionPool, "public", "mysfunc_accum", backup.TYPE_FUNCTION)
			prelimOid := testutils.OidFromObjectName(connectionPool, "public", "mypre_accum", backup.TYPE_FUNCTION)

			result := backup.GetAggregates(connectionPool)

			aggregateDef := backup.Aggregate{
				Schema: "public", Name: "agg_prefunc", Arguments: sql.NullString{String: "numeric, numeric", Valid: true},
				IdentArgs: sql.NullString{String: "numeric, numeric", Valid: true}, TransitionFunction: transitionOid, PreliminaryFunction: prelimOid,
				FinalFunction: 0, SortOperator: "", TransitionDataType: "numeric", InitialValue: "0", MInitValIsNull: true, IsOrdered: false,
			}
			if connectionPool.Version.AtLeast("6") {
				aggregateDef.PreliminaryFunction = 0
				aggregateDef.CombineFunction = prelimOid
			}
			if connectionPool.Version.AtLeast("7") {
				aggregateDef.Kind = "n"
				aggregateDef.Finalmodify = "r"
				aggregateDef.Mfinalmodify = "r"
				aggregateDef.Parallel = "u"
			}

			Expect(result).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&result[0], &aggregateDef, "Oid")
		})
		It("returns a slice of aggregates in a specific schema", func() {
			testhelper.AssertQueryRuns(connectionPool, `
CREATE AGGREGATE public.agg_prefunc(numeric, numeric) (
	SFUNC = public.mysfunc_accum,
	STYPE = numeric,
	PREFUNC = public.mypre_accum,
	INITCOND = 0 );
`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP AGGREGATE public.agg_prefunc(numeric, numeric)")
			testhelper.AssertQueryRuns(connectionPool, "CREATE SCHEMA testschema")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA testschema")
			testhelper.AssertQueryRuns(connectionPool, `
CREATE AGGREGATE testschema.agg_prefunc(numeric, numeric) (
	SFUNC = public.mysfunc_accum,
	STYPE = numeric,
	PREFUNC = public.mypre_accum,
	INITCOND = 0 );
`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP AGGREGATE testschema.agg_prefunc(numeric, numeric)")

			transitionOid := testutils.OidFromObjectName(connectionPool, "public", "mysfunc_accum", backup.TYPE_FUNCTION)
			prelimOid := testutils.OidFromObjectName(connectionPool, "public", "mypre_accum", backup.TYPE_FUNCTION)
			aggregateDef := backup.Aggregate{
				Schema: "testschema", Name: "agg_prefunc", Arguments: sql.NullString{String: "numeric, numeric", Valid: true},
				IdentArgs: sql.NullString{String: "numeric, numeric", Valid: true}, TransitionFunction: transitionOid, PreliminaryFunction: prelimOid,
				FinalFunction: 0, SortOperator: "", TransitionDataType: "numeric", InitialValue: "0", MInitValIsNull: true, IsOrdered: false,
			}
			if connectionPool.Version.AtLeast("6") {
				aggregateDef.PreliminaryFunction = 0
				aggregateDef.CombineFunction = prelimOid
			}
			if connectionPool.Version.AtLeast("7") {
				aggregateDef.Kind = "n"
				aggregateDef.Finalmodify = "r"
				aggregateDef.Mfinalmodify = "r"
				aggregateDef.Parallel = "u"
			}
			_ = backupCmdFlags.Set(options.INCLUDE_SCHEMA, "testschema")

			result := backup.GetAggregates(connectionPool)

			Expect(result).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&result[0], &aggregateDef, "Oid")
		})
		It("returns a slice for a hypothetical ordered-set aggregate", func() {
			testutils.SkipIfBefore6(connectionPool)

			testhelper.AssertQueryRuns(connectionPool, `
CREATE AGGREGATE public.agg_hypo_ord (VARIADIC "any" ORDER BY VARIADIC "any")
(
	SFUNC = pg_catalog.ordered_set_transition_multi,
	STYPE = internal,
	FINALFUNC = pg_catalog.rank_final,
	FINALFUNC_EXTRA,
	HYPOTHETICAL
);`)
			defer testhelper.AssertQueryRuns(connectionPool, `DROP AGGREGATE public.agg_hypo_ord(VARIADIC "any" ORDER BY VARIADIC "any")`)

			transitionOid := testutils.OidFromObjectName(connectionPool, "pg_catalog", "ordered_set_transition_multi", backup.TYPE_FUNCTION)
			finalOid := testutils.OidFromObjectName(connectionPool, "pg_catalog", "rank_final", backup.TYPE_FUNCTION)

			result := backup.GetAggregates(connectionPool)

			aggregateDef := backup.Aggregate{
				Schema: "public", Name: "agg_hypo_ord", Arguments: sql.NullString{String: `VARIADIC "any" ORDER BY VARIADIC "any"`, Valid: true},
				IdentArgs: sql.NullString{String: `VARIADIC "any" ORDER BY VARIADIC "any"`, Valid: true}, TransitionFunction: transitionOid,
				FinalFunction: finalOid, TransitionDataType: "internal", InitValIsNull: true, MInitValIsNull: true, FinalFuncExtra: true, Hypothetical: true,
			}
			if connectionPool.Version.AtLeast("7") {
				aggregateDef.Hypothetical = false
				aggregateDef.Kind = "h"
				aggregateDef.Finalmodify = "w"
				aggregateDef.Mfinalmodify = "w"
				aggregateDef.Parallel = "u"
			}

			Expect(result).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&result[0], &aggregateDef, "Oid")
		})
		It("returns a slice of aggregates with a combine function and transition data size", func() {
			testutils.SkipIfBefore6(connectionPool)
			testhelper.AssertQueryRuns(connectionPool, `
CREATE AGGREGATE public.agg_combinefunc(numeric, numeric) (
	SFUNC = public.mysfunc_accum,
	STYPE = numeric,
	SSPACE = 1000,
	COMBINEFUNC = public.mypre_accum,
	INITCOND = 0 );
`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP AGGREGATE public.agg_combinefunc(numeric, numeric)")

			transitionOid := testutils.OidFromObjectName(connectionPool, "public", "mysfunc_accum", backup.TYPE_FUNCTION)
			combineOid := testutils.OidFromObjectName(connectionPool, "public", "mypre_accum", backup.TYPE_FUNCTION)

			result := backup.GetAggregates(connectionPool)

			aggregateDef := backup.Aggregate{
				Schema: "public", Name: "agg_combinefunc", Arguments: sql.NullString{String: "numeric, numeric", Valid: true},
				IdentArgs: sql.NullString{String: "numeric, numeric", Valid: true}, TransitionFunction: transitionOid, CombineFunction: combineOid,
				FinalFunction: 0, SortOperator: "", TransitionDataType: "numeric", TransitionDataSize: 1000,
				InitialValue: "0", MInitValIsNull: true, IsOrdered: false,
			}
			if connectionPool.Version.AtLeast("7") {
				aggregateDef.Kind = "n"
				aggregateDef.Finalmodify = "r"
				aggregateDef.Mfinalmodify = "r"
				aggregateDef.Parallel = "u"
			}

			Expect(result).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&result[0], &aggregateDef, "Oid")
		})
		It("returns a slice of aggregates with serial/deserial functions", func() {
			testutils.SkipIfBefore6(connectionPool)
			testhelper.AssertQueryRuns(connectionPool, `
CREATE AGGREGATE public.myavg (numeric) (
	stype = internal,
	sfunc = numeric_avg_accum,
	finalfunc = numeric_avg,
	serialfunc = numeric_avg_serialize,
	deserialfunc = numeric_avg_deserialize);
`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP AGGREGATE public.myavg(numeric)")

			serialOid := testutils.OidFromObjectName(connectionPool, "pg_catalog", "numeric_avg_serialize", backup.TYPE_FUNCTION)
			deserialOid := testutils.OidFromObjectName(connectionPool, "pg_catalog", "numeric_avg_deserialize", backup.TYPE_FUNCTION)

			result := backup.GetAggregates(connectionPool)

			aggregateDef := backup.Aggregate{
				Schema: "public", Name: "myavg", Arguments: sql.NullString{String: "numeric", Valid: true},
				IdentArgs: sql.NullString{String: "numeric", Valid: true}, SerialFunction: serialOid, DeserialFunction: deserialOid,
				FinalFunction: 0, SortOperator: "", TransitionDataType: "internal",
				IsOrdered: false, InitValIsNull: true, MInitValIsNull: true,
			}
			if connectionPool.Version.AtLeast("7") {
				aggregateDef.Kind = "n"
				aggregateDef.Finalmodify = "r"
				aggregateDef.Mfinalmodify = "r"
				aggregateDef.Parallel = "u"
			}

			Expect(result).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&result[0], &aggregateDef, "Oid", "TransitionFunction", "FinalFunction")
		})
		It("returns a slice of aggregates with moving attributes", func() {
			testutils.SkipIfBefore6(connectionPool)
			testhelper.AssertQueryRuns(connectionPool, `
CREATE AGGREGATE public.moving_agg(numeric,numeric) (
	SFUNC = public.mysfunc_accum,
	STYPE = numeric,
	MSFUNC = public.mysfunc_accum,
	MINVFUNC = public.mysfunc_accum,
	MSTYPE = numeric,
	MSSPACE = 100,
	MFINALFUNC = public.mysfunc_accum,
	MFINALFUNC_EXTRA,
	MINITCOND = 0
	);
`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP AGGREGATE public.moving_agg(numeric, numeric)")

			sfuncOid := testutils.OidFromObjectName(connectionPool, "public", "mysfunc_accum", backup.TYPE_FUNCTION)

			result := backup.GetAggregates(connectionPool)

			aggregateDef := backup.Aggregate{
				Schema: "public", Name: "moving_agg", Arguments: sql.NullString{String: "numeric, numeric", Valid: true},
				IdentArgs: sql.NullString{String: "numeric, numeric", Valid: true}, TransitionFunction: sfuncOid, TransitionDataType: "numeric",
				InitValIsNull: true, MTransitionFunction: sfuncOid, MInverseTransitionFunction: sfuncOid,
				MTransitionDataType: "numeric", MTransitionDataSize: 100, MFinalFunction: sfuncOid,
				MFinalFuncExtra: true, MInitialValue: "0", MInitValIsNull: false,
			}
			if connectionPool.Version.AtLeast("7") {
				aggregateDef.Kind = "n"
				aggregateDef.Finalmodify = "r"
				aggregateDef.Mfinalmodify = "r"
				aggregateDef.Parallel = "u"
			}

			Expect(result).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&result[0], &aggregateDef, "Oid")
		})
	})
	Describe("GetFunctionOidToInfoMap", func() {
		It("returns map containing function information", func() {
			result := backup.GetFunctionOidToInfoMap(connectionPool)
			initialLength := len(result)
			testhelper.AssertQueryRuns(connectionPool, `CREATE FUNCTION public.add(integer, integer) RETURNS integer
AS 'SELECT $1 + $2'
LANGUAGE SQL`)
			defer testhelper.AssertQueryRuns(connectionPool, "DROP FUNCTION public.add(integer, integer)")

			result = backup.GetFunctionOidToInfoMap(connectionPool)
			oid := testutils.OidFromObjectName(connectionPool, "public", "add", backup.TYPE_FUNCTION)
			Expect(result).To(HaveLen(initialLength + 1))
			Expect(result[oid].QualifiedName).To(Equal("public.add"))
			Expect(result[oid].Arguments.String).To(Equal("integer, integer"))
			Expect(result[oid].IsInternal).To(BeFalse())
		})
		It("returns a map containing an internal function", func() {
			result := backup.GetFunctionOidToInfoMap(connectionPool)

			oid := testutils.OidFromObjectName(connectionPool, "pg_catalog", "boolin", backup.TYPE_FUNCTION)
			Expect(result[oid].QualifiedName).To(Equal("pg_catalog.boolin"))
			Expect(result[oid].IsInternal).To(BeTrue())
		})
	})
	Describe("GetCasts", func() {
		It("returns a slice for a basic cast with a function in 4.3", func() {
			testutils.SkipIfNot4(connectionPool)
			testhelper.AssertQueryRuns(connectionPool, "CREATE FUNCTION public.casttotext(bool) RETURNS pg_catalog.text STRICT IMMUTABLE LANGUAGE PLPGSQL AS $$ BEGIN IF $1 IS TRUE THEN RETURN 'true'; ELSE RETURN 'false'; END IF; END; $$;")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP FUNCTION public.casttotext(bool)")
			testhelper.AssertQueryRuns(connectionPool, "CREATE CAST (bool AS text) WITH FUNCTION public.casttotext(bool) AS ASSIGNMENT")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP CAST (bool AS text)")

			results := backup.GetCasts(connectionPool)

			castDef := backup.Cast{Oid: 0, SourceTypeFQN: "pg_catalog.bool", TargetTypeFQN: "pg_catalog.text", FunctionSchema: "public", FunctionName: "casttotext", FunctionArgs: "boolean", CastContext: "a", CastMethod: "f"}

			Expect(results).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&castDef, &results[0], "Oid", "FunctionOid")
		})
		It("returns a slice for a basic cast with a function in 5 and 6", func() {
			testutils.SkipIfBefore5(connectionPool)
			testhelper.AssertQueryRuns(connectionPool, "CREATE FUNCTION public.casttoint(text) RETURNS integer STRICT IMMUTABLE LANGUAGE SQL AS 'SELECT cast($1 as integer);'")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP FUNCTION public.casttoint(text)")
			testhelper.AssertQueryRuns(connectionPool, "CREATE CAST (text AS integer) WITH FUNCTION public.casttoint(text) AS ASSIGNMENT")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP CAST (text AS int4)")

			results := backup.GetCasts(connectionPool)

			castDef := backup.Cast{Oid: 0, SourceTypeFQN: "pg_catalog.text", TargetTypeFQN: "pg_catalog.int4", FunctionSchema: "public", FunctionName: "casttoint", FunctionArgs: "text", CastContext: "a", CastMethod: "f"}

			Expect(results).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&castDef, &results[0], "Oid")
		})
		It("returns a slice for a basic cast without a function", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE FUNCTION public.cast_in(cstring) RETURNS public.casttesttype AS $$textin$$ LANGUAGE internal STRICT NO SQL")
			testhelper.AssertQueryRuns(connectionPool, "CREATE FUNCTION public.cast_out(public.casttesttype) RETURNS cstring AS $$textout$$ LANGUAGE internal STRICT NO SQL")
			testhelper.AssertQueryRuns(connectionPool, "CREATE TYPE public.casttesttype (INTERNALLENGTH = variable, INPUT = public.cast_in, OUTPUT = public.cast_out)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TYPE public.casttesttype CASCADE")
			testhelper.AssertQueryRuns(connectionPool, "CREATE CAST (text AS public.casttesttype) WITHOUT FUNCTION AS IMPLICIT")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP CAST (text AS public.casttesttype)")

			results := backup.GetCasts(connectionPool)

			castDef := backup.Cast{Oid: 0, SourceTypeFQN: "pg_catalog.text", TargetTypeFQN: "public.casttesttype", FunctionSchema: "", FunctionName: "", FunctionArgs: "", CastContext: "i", CastMethod: "b"}

			Expect(results).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&castDef, &results[0], "Oid")
		})
		It("returns a slice of casts with the source and target types in a different schema", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE SCHEMA testschema1")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA testschema1")
			testhelper.AssertQueryRuns(connectionPool, "CREATE FUNCTION cast_in(cstring) RETURNS testschema1.casttesttype AS $$textin$$ LANGUAGE internal STRICT NO SQL")
			testhelper.AssertQueryRuns(connectionPool, "CREATE FUNCTION cast_out(testschema1.casttesttype) RETURNS cstring AS $$textout$$ LANGUAGE internal STRICT NO SQL")
			testhelper.AssertQueryRuns(connectionPool, "CREATE TYPE testschema1.casttesttype (INTERNALLENGTH = variable, INPUT = cast_in, OUTPUT = cast_out)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TYPE testschema1.casttesttype CASCADE")

			testhelper.AssertQueryRuns(connectionPool, "CREATE CAST (text AS testschema1.casttesttype) WITHOUT FUNCTION AS IMPLICIT")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP CAST (text AS testschema1.casttesttype)")
			testhelper.AssertQueryRuns(connectionPool, "CREATE CAST (testschema1.casttesttype AS text) WITHOUT FUNCTION AS IMPLICIT")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP CAST (testschema1.casttesttype AS text)")

			results := backup.GetCasts(connectionPool)

			castDefTarget := backup.Cast{Oid: 0, SourceTypeFQN: "pg_catalog.text", TargetTypeFQN: "testschema1.casttesttype", FunctionSchema: "", FunctionName: "", FunctionArgs: "", CastContext: "i", CastMethod: "b"}
			castDefSource := backup.Cast{Oid: 0, SourceTypeFQN: "testschema1.casttesttype", TargetTypeFQN: "pg_catalog.text", FunctionSchema: "", FunctionName: "", FunctionArgs: "", CastContext: "i", CastMethod: "b"}

			Expect(results).To(HaveLen(2))
			structmatcher.ExpectStructsToMatchExcluding(&castDefTarget, &results[0], "Oid")
			structmatcher.ExpectStructsToMatchExcluding(&castDefSource, &results[1], "Oid")
		})
		It("returns a slice for an inout cast", func() {
			testutils.SkipIfBefore6(connectionPool)
			testhelper.AssertQueryRuns(connectionPool, "CREATE TYPE public.custom_numeric AS (i numeric)")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TYPE public.custom_numeric")
			testhelper.AssertQueryRuns(connectionPool, "CREATE CAST (varchar AS public.custom_numeric) WITH INOUT")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP CAST (varchar AS public.custom_numeric)")

			results := backup.GetCasts(connectionPool)

			castDef := backup.Cast{Oid: 0, SourceTypeFQN: `pg_catalog."varchar"`, TargetTypeFQN: "public.custom_numeric", FunctionSchema: "", FunctionName: "", FunctionArgs: "", CastContext: "e", CastMethod: "i"}

			Expect(results).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&castDef, &results[0], "Oid")
		})
	})
	Describe("GetExtensions", func() {
		It("returns a slice of extension", func() {
			testutils.SkipIfBefore5(connectionPool)
			testhelper.AssertQueryRuns(connectionPool, "CREATE EXTENSION plperl")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP EXTENSION plperl")

			results := backup.GetExtensions(connectionPool)

			if connectionPool.Version.Before("7") {
				Expect(results).To(HaveLen(1))
			} else {
				// gp_toolkit is installed by default as an extension in GPDB7+
				Expect(results).To(HaveLen(2))
			}

			plperlDef := backup.Extension{Oid: 0, Name: "plperl", Schema: "pg_catalog"}
			for _, result := range results {
				if result.Name == "plperl" {
					structmatcher.ExpectStructsToMatchExcluding(&plperlDef, &results[0], "Oid")
				}
			}
		})
	})
	Describe("GetProceduralLanguages", func() {
		It("returns a slice of procedural languages", func() {
			plpythonString := "plpython"
			if connectionPool.Version.AtLeast("7") {
				plpythonString = "plpython3"
			}

			testhelper.AssertQueryRuns(connectionPool, fmt.Sprintf("CREATE LANGUAGE %su", plpythonString))
			defer testhelper.AssertQueryRuns(connectionPool, fmt.Sprintf("DROP LANGUAGE %su", plpythonString))

			pythonHandlerOid := testutils.OidFromObjectName(connectionPool, "pg_catalog", fmt.Sprintf("%s_call_handler", plpythonString), backup.TYPE_FUNCTION)

			expectedPlpythonInfo := backup.ProceduralLanguage{Oid: 1, Name: fmt.Sprintf("%su", plpythonString), Owner: "testrole", IsPl: true, PlTrusted: false, Handler: pythonHandlerOid, Inline: 0, Validator: 0}
			if connectionPool.Version.AtLeast("5") {
				pythonInlineOid := testutils.OidFromObjectName(connectionPool, "pg_catalog", fmt.Sprintf("%s_inline_handler", plpythonString), backup.TYPE_FUNCTION)
				expectedPlpythonInfo.Inline = pythonInlineOid
			}
			if connectionPool.Version.AtLeast("6") {
				expectedPlpythonInfo.Validator = testutils.OidFromObjectName(connectionPool, "pg_catalog", fmt.Sprintf("%s_validator", plpythonString), backup.TYPE_FUNCTION)
			}

			resultProcLangs := backup.GetProceduralLanguages(connectionPool)

			Expect(resultProcLangs).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&expectedPlpythonInfo, &resultProcLangs[0], "Oid", "Owner")
		})
	})
	Describe("GetConversions", func() {
		It("returns a slice of conversions", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE CONVERSION public.testconv FOR 'LATIN1' TO 'MULE_INTERNAL' FROM latin1_to_mic")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP CONVERSION public.testconv")

			expectedConversion := backup.Conversion{Oid: 0, Schema: "public", Name: "testconv", ForEncoding: "LATIN1", ToEncoding: "MULE_INTERNAL", ConversionFunction: "pg_catalog.latin1_to_mic", IsDefault: false}

			resultConversions := backup.GetConversions(connectionPool)

			Expect(resultConversions).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&expectedConversion, &resultConversions[0], "Oid")
		})
		It("returns a slice of conversions in a specific schema", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE CONVERSION public.testconv FOR 'LATIN1' TO 'MULE_INTERNAL' FROM latin1_to_mic")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP CONVERSION public.testconv")
			testhelper.AssertQueryRuns(connectionPool, "CREATE SCHEMA testschema")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP SCHEMA testschema")
			testhelper.AssertQueryRuns(connectionPool, "CREATE CONVERSION testschema.testconv FOR 'LATIN1' TO 'MULE_INTERNAL' FROM latin1_to_mic")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP CONVERSION testschema.testconv")

			expectedConversion := backup.Conversion{Oid: 0, Schema: "testschema", Name: "testconv", ForEncoding: "LATIN1", ToEncoding: "MULE_INTERNAL", ConversionFunction: "pg_catalog.latin1_to_mic", IsDefault: false}

			_ = backupCmdFlags.Set(options.INCLUDE_SCHEMA, "testschema")
			resultConversions := backup.GetConversions(connectionPool)

			Expect(resultConversions).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&expectedConversion, &resultConversions[0], "Oid")
		})
	})
	Describe("GetTransforms", func() {
		BeforeEach(func() {
			testutils.SkipIfBefore7(connectionPool)
		})
		It("returns a slice of transfroms", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE TRANSFORM FOR pg_catalog.int4 LANGUAGE c (FROM SQL WITH FUNCTION numeric_support(internal), TO SQL WITH FUNCTION int4recv(internal));")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TRANSFORM FOR int4 LANGUAGE c")

			fromSQLFuncOid := testutils.OidFromObjectName(connectionPool, "pg_catalog", "numeric_support", backup.TYPE_FUNCTION)
			toSQLFuncOid := testutils.OidFromObjectName(connectionPool, "pg_catalog", "int4recv", backup.TYPE_FUNCTION)

			expectedTransforms := backup.Transform{TypeNamespace: "pg_catalog", TypeName: "int4", LanguageName: "c", FromSQLFunc: fromSQLFuncOid, ToSQLFunc: toSQLFuncOid}

			resultTransforms := backup.GetTransforms(connectionPool)

			Expect(resultTransforms).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&expectedTransforms, &resultTransforms[0], "Oid")
		})
	})
	Describe("GetForeignDataWrappers", func() {
		BeforeEach(func() {
			testutils.SkipIfBefore6(connectionPool)
		})
		It("returns a slice of foreign data wrappers", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE FOREIGN DATA WRAPPER foreigndatawrapper")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP FOREIGN DATA WRAPPER foreigndatawrapper")

			expectedForeignDataWrapper := backup.ForeignDataWrapper{Oid: 0, Name: "foreigndatawrapper"}

			resultForeignDataWrapper := backup.GetForeignDataWrappers(connectionPool)

			Expect(resultForeignDataWrapper).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&expectedForeignDataWrapper, &resultForeignDataWrapper[0], "Oid")
		})
		It("returns a slice of foreign data wrappers with a validator", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE FOREIGN DATA WRAPPER foreigndatawrapper VALIDATOR postgresql_fdw_validator")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP FOREIGN DATA WRAPPER foreigndatawrapper")

			validatorOid := testutils.OidFromObjectName(connectionPool, "pg_catalog", "postgresql_fdw_validator", backup.TYPE_FUNCTION)
			expectedForeignDataWrapper := backup.ForeignDataWrapper{Oid: 0, Name: "foreigndatawrapper", Validator: validatorOid}

			resultForeignDataWrapper := backup.GetForeignDataWrappers(connectionPool)

			Expect(resultForeignDataWrapper).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&expectedForeignDataWrapper, &resultForeignDataWrapper[0], "Oid")
		})
		It("returns a slice of foreign data wrappers with options", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE FOREIGN DATA WRAPPER foreigndatawrapper OPTIONS (dbname 'testdb', debug 'true')")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP FOREIGN DATA WRAPPER foreigndatawrapper")

			expectedForeignDataWrapper := backup.ForeignDataWrapper{Oid: 0, Name: "foreigndatawrapper", Options: "dbname 'testdb', debug 'true'"}

			resultForeignDataWrappers := backup.GetForeignDataWrappers(connectionPool)

			Expect(resultForeignDataWrappers).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&expectedForeignDataWrapper, &resultForeignDataWrappers[0], "Oid")
		})
	})
	Describe("GetForeignServers", func() {
		BeforeEach(func() {
			testutils.SkipIfBefore6(connectionPool)
			testhelper.AssertQueryRuns(connectionPool, "CREATE FOREIGN DATA WRAPPER foreigndatawrapper")
		})
		AfterEach(func() {
			testhelper.AssertQueryRuns(connectionPool, "DROP FOREIGN DATA WRAPPER foreigndatawrapper CASCADE")
		})
		It("returns a slice of foreign servers", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE SERVER foreignserver FOREIGN DATA WRAPPER foreigndatawrapper")

			expectedServer := backup.ForeignServer{Oid: 1, Name: "foreignserver", ForeignDataWrapper: "foreigndatawrapper"}

			resultServers := backup.GetForeignServers(connectionPool)

			Expect(resultServers).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&expectedServer, &resultServers[0], "Oid")
		})
		It("returns a slice of foreign servers with a type and version", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE SERVER foreignserver TYPE 'mytype' VERSION 'myversion' FOREIGN DATA WRAPPER foreigndatawrapper")

			expectedServer := backup.ForeignServer{Oid: 1, Name: "foreignserver", Type: "mytype", Version: "myversion", ForeignDataWrapper: "foreigndatawrapper"}

			resultServers := backup.GetForeignServers(connectionPool)

			Expect(resultServers).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&expectedServer, &resultServers[0], "Oid")
		})
		It("returns a slice of foreign servers with options", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE SERVER foreignserver FOREIGN DATA WRAPPER foreigndatawrapper OPTIONS (dbname 'testdb', host 'localhost')")

			expectedServer := backup.ForeignServer{Oid: 1, Name: "foreignserver", ForeignDataWrapper: "foreigndatawrapper", Options: "dbname 'testdb', host 'localhost'"}

			resultServers := backup.GetForeignServers(connectionPool)

			Expect(resultServers).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&expectedServer, &resultServers[0], "Oid")
		})
	})
	Describe("GetUserMappings", func() {
		BeforeEach(func() {
			testutils.SkipIfBefore6(connectionPool)
			testhelper.AssertQueryRuns(connectionPool, "CREATE FOREIGN DATA WRAPPER foreigndatawrapper")
			testhelper.AssertQueryRuns(connectionPool, "CREATE SERVER foreignserver FOREIGN DATA WRAPPER foreigndatawrapper")
		})
		AfterEach(func() {
			testhelper.AssertQueryRuns(connectionPool, "DROP FOREIGN DATA WRAPPER foreigndatawrapper CASCADE")
		})
		It("returns a slice of user mappings", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE USER MAPPING FOR testrole SERVER foreignserver")

			expectedMapping := backup.UserMapping{Oid: 1, User: "testrole", Server: "foreignserver"}

			resultMappings := backup.GetUserMappings(connectionPool)

			Expect(resultMappings).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&expectedMapping, &resultMappings[0], "Oid")
		})
		It("returns a slice of user mappings with options", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE USER MAPPING FOR public SERVER foreignserver OPTIONS (dbname 'testdb', host 'localhost')")

			expectedMapping := backup.UserMapping{Oid: 1, User: "public", Server: "foreignserver", Options: "dbname 'testdb', host 'localhost'"}

			resultMappings := backup.GetUserMappings(connectionPool)

			Expect(resultMappings).To(HaveLen(1))
			structmatcher.ExpectStructsToMatchExcluding(&expectedMapping, &resultMappings[0], "Oid")
		})
		It("returns a slice of user mappings in sorted order", func() {
			testhelper.AssertQueryRuns(connectionPool, "CREATE USER MAPPING FOR testrole SERVER foreignserver")
			testhelper.AssertQueryRuns(connectionPool, "CREATE USER MAPPING FOR anothertestrole SERVER foreignserver")

			expectedMapping := []backup.UserMapping{
				{Oid: 1, User: "anothertestrole", Server: "foreignserver"},
				{Oid: 1, User: "testrole", Server: "foreignserver"},
			}

			resultMappings := backup.GetUserMappings(connectionPool)

			Expect(resultMappings).To(HaveLen(2))
			for idx := range expectedMapping {
				structmatcher.ExpectStructsToMatchExcluding(&expectedMapping[idx], &resultMappings[idx], "Oid")
			}
		})
	})
})
