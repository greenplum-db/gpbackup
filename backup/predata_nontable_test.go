package backup_test

import (
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
)

var _ = Describe("backup/predata tests", func() {
	buffer := gbytes.NewBuffer()

	BeforeEach(func() {
		buffer = gbytes.BufferWithBytes([]byte(""))
	})
	Describe("ProcessConstraints", func() {
		testTable := utils.BasicRelation("public", "tablename")
		uniqueOne := backup.QueryConstraint{"tablename_i_key", "u", "UNIQUE (i)", ""}
		uniqueTwo := backup.QueryConstraint{"tablename_j_key", "u", "UNIQUE (j)", ""}
		primarySingle := backup.QueryConstraint{"tablename_pkey", "p", "PRIMARY KEY (i)", ""}
		primaryComposite := backup.QueryConstraint{"tablename_pkey", "p", "PRIMARY KEY (i, j)", ""}
		foreignOne := backup.QueryConstraint{"tablename_i_fkey", "f", "FOREIGN KEY (i) REFERENCES other_tablename(a)", ""}
		foreignTwo := backup.QueryConstraint{"tablename_j_fkey", "f", "FOREIGN KEY (j) REFERENCES other_tablename(b)", ""}
		commentOne := backup.QueryConstraint{"tablename_i_key", "u", "UNIQUE (i)", "This is a constraint comment."}

		Context("No ALTER TABLE statements", func() {
			It("returns an empty slice", func() {
				constraints := []backup.QueryConstraint{}
				cons, fkCons := backup.ProcessConstraints(testTable, constraints)
				Expect(len(cons)).To(Equal(0))
				Expect(len(fkCons)).To(Equal(0))
			})
		})
		Context("ALTER TABLE statements involving different columns", func() {
			It("returns a slice containing one UNIQUE constraint", func() {
				constraints := []backup.QueryConstraint{uniqueOne}
				cons, fkCons := backup.ProcessConstraints(testTable, constraints)
				Expect(len(cons)).To(Equal(1))
				Expect(len(fkCons)).To(Equal(0))
				Expect(cons[0]).To(Equal("\n\nALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_key UNIQUE (i);"))
			})
			It("returns a slice containing two UNIQUE constraints", func() {
				constraints := []backup.QueryConstraint{uniqueOne, uniqueTwo}
				cons, fkCons := backup.ProcessConstraints(testTable, constraints)
				Expect(len(cons)).To(Equal(2))
				Expect(len(fkCons)).To(Equal(0))
				Expect(cons[0]).To(Equal("\n\nALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_key UNIQUE (i);"))
				Expect(cons[1]).To(Equal("\n\nALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_j_key UNIQUE (j);"))
			})
			It("returns a slice containing PRIMARY KEY constraint on one column", func() {
				constraints := []backup.QueryConstraint{primarySingle}
				cons, fkCons := backup.ProcessConstraints(testTable, constraints)
				Expect(len(cons)).To(Equal(1))
				Expect(len(fkCons)).To(Equal(0))
				Expect(cons[0]).To(Equal("\n\nALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_pkey PRIMARY KEY (i);"))
			})
			It("returns a slice containing composite PRIMARY KEY constraint on two columns", func() {
				constraints := []backup.QueryConstraint{primaryComposite}
				cons, fkCons := backup.ProcessConstraints(testTable, constraints)
				Expect(len(cons)).To(Equal(1))
				Expect(len(fkCons)).To(Equal(0))
				Expect(cons[0]).To(Equal("\n\nALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_pkey PRIMARY KEY (i, j);"))
			})
			It("returns a slice containing one FOREIGN KEY constraint", func() {
				constraints := []backup.QueryConstraint{foreignOne}
				cons, fkCons := backup.ProcessConstraints(testTable, constraints)
				Expect(len(cons)).To(Equal(0))
				Expect(len(fkCons)).To(Equal(1))
				Expect(fkCons[0]).To(Equal("\n\nALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_fkey FOREIGN KEY (i) REFERENCES other_tablename(a);"))
			})
			It("returns a slice containing two FOREIGN KEY constraints", func() {
				constraints := []backup.QueryConstraint{foreignOne, foreignTwo}
				cons, fkCons := backup.ProcessConstraints(testTable, constraints)
				Expect(len(cons)).To(Equal(0))
				Expect(len(fkCons)).To(Equal(2))
				Expect(fkCons[0]).To(Equal("\n\nALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_fkey FOREIGN KEY (i) REFERENCES other_tablename(a);"))
				Expect(fkCons[1]).To(Equal("\n\nALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_j_fkey FOREIGN KEY (j) REFERENCES other_tablename(b);"))
			})
			It("returns a slice containing one UNIQUE constraint and one FOREIGN KEY constraint", func() {
				constraints := []backup.QueryConstraint{uniqueOne, foreignTwo}
				cons, fkCons := backup.ProcessConstraints(testTable, constraints)
				Expect(len(cons)).To(Equal(1))
				Expect(len(fkCons)).To(Equal(1))
				Expect(cons[0]).To(Equal("\n\nALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_key UNIQUE (i);"))
				Expect(fkCons[0]).To(Equal("\n\nALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_j_fkey FOREIGN KEY (j) REFERENCES other_tablename(b);"))
			})
			It("returns a slice containing one PRIMARY KEY constraint and one FOREIGN KEY constraint", func() {
				constraints := []backup.QueryConstraint{primarySingle, foreignTwo}
				cons, fkCons := backup.ProcessConstraints(testTable, constraints)
				Expect(len(cons)).To(Equal(1))
				Expect(len(fkCons)).To(Equal(1))
				Expect(cons[0]).To(Equal("\n\nALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_pkey PRIMARY KEY (i);"))
				Expect(fkCons[0]).To(Equal("\n\nALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_j_fkey FOREIGN KEY (j) REFERENCES other_tablename(b);"))
			})
			It("returns a slice containing a two-column composite PRIMARY KEY constraint and one FOREIGN KEY constraint", func() {
				constraints := []backup.QueryConstraint{primaryComposite, foreignTwo}
				cons, fkCons := backup.ProcessConstraints(testTable, constraints)
				Expect(len(cons)).To(Equal(1))
				Expect(len(fkCons)).To(Equal(1))
				Expect(cons[0]).To(Equal("\n\nALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_pkey PRIMARY KEY (i, j);"))
				Expect(fkCons[0]).To(Equal("\n\nALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_j_fkey FOREIGN KEY (j) REFERENCES other_tablename(b);"))
			})
			It("returns a slice containing one UNIQUE constraint with a comment and one without", func() {
				constraints := []backup.QueryConstraint{commentOne, uniqueTwo}
				cons, _ := backup.ProcessConstraints(testTable, constraints)
				Expect(len(cons)).To(Equal(2))
				Expect(cons[0]).To(Equal(`

ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_key UNIQUE (i);

COMMENT ON CONSTRAINT tablename_i_key ON public.tablename IS 'This is a constraint comment.';`))
				Expect(cons[1]).To(Equal("\n\nALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_j_key UNIQUE (j);"))
			})
		})
		Context("ALTER TABLE statements involving the same column", func() {
			It("returns a slice containing one UNIQUE constraint and one FOREIGN KEY constraint", func() {
				constraints := []backup.QueryConstraint{uniqueOne, foreignOne}
				cons, fkCons := backup.ProcessConstraints(testTable, constraints)
				Expect(len(cons)).To(Equal(1))
				Expect(len(fkCons)).To(Equal(1))
				Expect(cons[0]).To(Equal("\n\nALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_key UNIQUE (i);"))
				Expect(fkCons[0]).To(Equal("\n\nALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_fkey FOREIGN KEY (i) REFERENCES other_tablename(a);"))
			})
			It("returns a slice containing one PRIMARY KEY constraint and one FOREIGN KEY constraint", func() {
				constraints := []backup.QueryConstraint{primarySingle, foreignOne}
				cons, fkCons := backup.ProcessConstraints(testTable, constraints)
				Expect(len(cons)).To(Equal(1))
				Expect(len(fkCons)).To(Equal(1))
				Expect(cons[0]).To(Equal("\n\nALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_pkey PRIMARY KEY (i);"))
				Expect(fkCons[0]).To(Equal("\n\nALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_fkey FOREIGN KEY (i) REFERENCES other_tablename(a);"))
			})
			It("returns a slice containing a two-column composite PRIMARY KEY constraint and one FOREIGN KEY constraint", func() {
				constraints := []backup.QueryConstraint{primaryComposite, foreignOne}
				cons, fkCons := backup.ProcessConstraints(testTable, constraints)
				Expect(len(cons)).To(Equal(1))
				Expect(len(fkCons)).To(Equal(1))
				Expect(cons[0]).To(Equal("\n\nALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_pkey PRIMARY KEY (i, j);"))
				Expect(fkCons[0]).To(Equal("\n\nALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_fkey FOREIGN KEY (i) REFERENCES other_tablename(a);"))
			})
		})
	})
	Describe("PrintCreateSequenceStatements", func() {
		baseSequence := utils.BasicRelation("public", "seq_name")
		commentSequence := utils.Relation{0, 0, "public", "seq_name", "This is a sequence comment.", ""}
		ownerSequence := utils.Relation{0, 0, "public", "seq_name", "", "testrole"}
		seqDefault := backup.SequenceDefinition{baseSequence, backup.QuerySequenceDefinition{"seq_name", 7, 1, 9223372036854775807, 1, 5, 42, false, true}}
		seqNegIncr := backup.SequenceDefinition{baseSequence, backup.QuerySequenceDefinition{"seq_name", 7, -1, -1, -9223372036854775807, 5, 42, false, true}}
		seqMaxPos := backup.SequenceDefinition{baseSequence, backup.QuerySequenceDefinition{"seq_name", 7, 1, 100, 1, 5, 42, false, true}}
		seqMinPos := backup.SequenceDefinition{baseSequence, backup.QuerySequenceDefinition{"seq_name", 7, 1, 9223372036854775807, 10, 5, 42, false, true}}
		seqMaxNeg := backup.SequenceDefinition{baseSequence, backup.QuerySequenceDefinition{"seq_name", 7, -1, -10, -9223372036854775807, 5, 42, false, true}}
		seqMinNeg := backup.SequenceDefinition{baseSequence, backup.QuerySequenceDefinition{"seq_name", 7, -1, -1, -100, 5, 42, false, true}}
		seqCycle := backup.SequenceDefinition{baseSequence, backup.QuerySequenceDefinition{"seq_name", 7, 1, 9223372036854775807, 1, 5, 42, true, true}}
		seqStart := backup.SequenceDefinition{baseSequence, backup.QuerySequenceDefinition{"seq_name", 7, 1, 9223372036854775807, 1, 5, 42, false, false}}
		seqComment := backup.SequenceDefinition{commentSequence, backup.QuerySequenceDefinition{"seq_name", 7, 1, 9223372036854775807, 1, 5, 42, false, true}}
		seqOwner := backup.SequenceDefinition{ownerSequence, backup.QuerySequenceDefinition{"seq_name", 7, 1, 9223372036854775807, 1, 5, 42, false, true}}
		emptyOwnerMap := make(map[string]string, 0)

		It("can print a sequence with all default options", func() {
			sequences := []backup.SequenceDefinition{seqDefault}
			backup.PrintCreateSequenceStatements(buffer, sequences, emptyOwnerMap)
			testutils.ExpectRegexp(buffer, `CREATE SEQUENCE public.seq_name
	INCREMENT BY 1
	NO MAXVALUE
	NO MINVALUE
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, true);`)
		})
		It("can print a decreasing sequence", func() {
			sequences := []backup.SequenceDefinition{seqNegIncr}
			backup.PrintCreateSequenceStatements(buffer, sequences, emptyOwnerMap)
			testutils.ExpectRegexp(buffer, `CREATE SEQUENCE public.seq_name
	INCREMENT BY -1
	NO MAXVALUE
	NO MINVALUE
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, true);`)
		})
		It("can print an increasing sequence with a maximum value", func() {
			sequences := []backup.SequenceDefinition{seqMaxPos}
			backup.PrintCreateSequenceStatements(buffer, sequences, emptyOwnerMap)
			testutils.ExpectRegexp(buffer, `CREATE SEQUENCE public.seq_name
	INCREMENT BY 1
	MAXVALUE 100
	NO MINVALUE
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, true);`)
		})
		It("can print an increasing sequence with a minimum value", func() {
			sequences := []backup.SequenceDefinition{seqMinPos}
			backup.PrintCreateSequenceStatements(buffer, sequences, emptyOwnerMap)
			testutils.ExpectRegexp(buffer, `CREATE SEQUENCE public.seq_name
	INCREMENT BY 1
	NO MAXVALUE
	MINVALUE 10
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, true);`)
		})
		It("can print a decreasing sequence with a maximum value", func() {
			sequences := []backup.SequenceDefinition{seqMaxNeg}
			backup.PrintCreateSequenceStatements(buffer, sequences, emptyOwnerMap)
			testutils.ExpectRegexp(buffer, `CREATE SEQUENCE public.seq_name
	INCREMENT BY -1
	MAXVALUE -10
	NO MINVALUE
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, true);`)
		})
		It("can print a decreasing sequence with a minimum value", func() {
			sequences := []backup.SequenceDefinition{seqMinNeg}
			backup.PrintCreateSequenceStatements(buffer, sequences, emptyOwnerMap)
			testutils.ExpectRegexp(buffer, `CREATE SEQUENCE public.seq_name
	INCREMENT BY -1
	NO MAXVALUE
	MINVALUE -100
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, true);`)
		})
		It("can print a sequence that cycles", func() {
			sequences := []backup.SequenceDefinition{seqCycle}
			backup.PrintCreateSequenceStatements(buffer, sequences, emptyOwnerMap)
			testutils.ExpectRegexp(buffer, `CREATE SEQUENCE public.seq_name
	INCREMENT BY 1
	NO MAXVALUE
	NO MINVALUE
	CACHE 5
	CYCLE;

SELECT pg_catalog.setval('public.seq_name', 7, true);`)
		})
		It("can print a sequence with a start value", func() {
			sequences := []backup.SequenceDefinition{seqStart}
			backup.PrintCreateSequenceStatements(buffer, sequences, emptyOwnerMap)
			testutils.ExpectRegexp(buffer, `CREATE SEQUENCE public.seq_name
	START WITH 7
	INCREMENT BY 1
	NO MAXVALUE
	NO MINVALUE
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, false);`)
		})
		It("can print a sequence with a comment", func() {
			sequences := []backup.SequenceDefinition{seqComment}
			backup.PrintCreateSequenceStatements(buffer, sequences, emptyOwnerMap)
			testutils.ExpectRegexp(buffer, `CREATE SEQUENCE public.seq_name
	INCREMENT BY 1
	NO MAXVALUE
	NO MINVALUE
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, true);


COMMENT ON SEQUENCE public.seq_name IS 'This is a sequence comment.';`)
		})
		It("can print a sequence with an owner", func() {
			sequences := []backup.SequenceDefinition{seqOwner}
			backup.PrintCreateSequenceStatements(buffer, sequences, emptyOwnerMap)
			testutils.ExpectRegexp(buffer, `CREATE SEQUENCE public.seq_name
	INCREMENT BY 1
	NO MAXVALUE
	NO MINVALUE
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, true);


ALTER TABLE public.seq_name OWNER TO testrole;`)
		})
		It("can print a sequence with an owning column", func() {
			sequences := []backup.SequenceDefinition{seqOwner}
			ownerMap := map[string]string{"public.seq_name": "tablename.col_one"}
			backup.PrintCreateSequenceStatements(buffer, sequences, ownerMap)
			testutils.ExpectRegexp(buffer, `CREATE SEQUENCE public.seq_name
	INCREMENT BY 1
	NO MAXVALUE
	NO MINVALUE
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, true);


ALTER TABLE public.seq_name OWNER TO testrole;


ALTER SEQUENCE public.seq_name OWNED BY tablename.col_one`)
		})
	})
	Describe("PrintCreateSchemaStatements", func() {
		It("can print schema with comments", func() {
			schemas := []utils.Schema{utils.Schema{0, "schema_with_comments", "This is a comment.", ""}}

			backup.PrintCreateSchemaStatements(buffer, schemas)
			testutils.ExpectRegexp(buffer, `CREATE SCHEMA schema_with_comments;
COMMENT ON SCHEMA schema_with_comments IS 'This is a comment.';`)
		})
		It("can print schema with no comments", func() {
			schemas := []utils.Schema{utils.BasicSchema("schema_with_no_comments")}

			backup.PrintCreateSchemaStatements(buffer, schemas)
			testutils.ExpectRegexp(buffer, `CREATE SCHEMA schema_with_no_comments;`)
		})
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
	Describe("PrintCreateLanguageStatements", func() {
		plUntrustedHandlerOnly := backup.QueryProceduralLanguage{"plpythonu", "testrole", true, false, 4, 0, 0, "", ""}
		plAllFields := backup.QueryProceduralLanguage{"plpgsql", "testrole", true, true, 1, 2, 3, "", ""}
		plComment := backup.QueryProceduralLanguage{"plpythonu", "testrole", true, false, 4, 0, 0, "", "language comment"}
		funcInfoMap := map[uint32]backup.FunctionInfo{
			1: backup.FunctionInfo{QualifiedName: "pg_catalog.plpgsql_call_handler", Arguments: ""},
			2: backup.FunctionInfo{QualifiedName: "pg_catalog.plpgsql_inline_handler", Arguments: "internal"},
			3: backup.FunctionInfo{QualifiedName: "pg_catalog.plpgsql_validator", Arguments: "oid"},
			4: backup.FunctionInfo{QualifiedName: "pg_catalog.plpython_call_handler", Arguments: ""},
		}

		It("prints untrusted language with a handler only", func() {
			langs := []backup.QueryProceduralLanguage{plUntrustedHandlerOnly}

			backup.PrintCreateLanguageStatements(buffer, langs, funcInfoMap)
			testutils.ExpectRegexp(buffer, `CREATE PROCEDURAL LANGUAGE plpythonu;
ALTER FUNCTION pg_catalog.plpython_call_handler() OWNER TO testrole;
ALTER LANGUAGE plpythonu OWNER TO testrole;`)
		})
		It("prints trusted language with handler, inline, validator, and comments", func() {
			langs := []backup.QueryProceduralLanguage{plAllFields}

			backup.PrintCreateLanguageStatements(buffer, langs, funcInfoMap)
			testutils.ExpectRegexp(buffer, `CREATE TRUSTED PROCEDURAL LANGUAGE plpgsql;
ALTER FUNCTION pg_catalog.plpgsql_call_handler() OWNER TO testrole;
ALTER FUNCTION pg_catalog.plpgsql_inline_handler(internal) OWNER TO testrole;
ALTER FUNCTION pg_catalog.plpgsql_validator(oid) OWNER TO testrole;
ALTER LANGUAGE plpgsql OWNER TO testrole;`)
		})
		It("prints multiple create language statements", func() {
			langs := []backup.QueryProceduralLanguage{plUntrustedHandlerOnly, plAllFields}

			backup.PrintCreateLanguageStatements(buffer, langs, funcInfoMap)
			testutils.ExpectRegexp(buffer, `CREATE PROCEDURAL LANGUAGE plpythonu;
ALTER FUNCTION pg_catalog.plpython_call_handler() OWNER TO testrole;
ALTER LANGUAGE plpythonu OWNER TO testrole;


CREATE TRUSTED PROCEDURAL LANGUAGE plpgsql;
ALTER FUNCTION pg_catalog.plpgsql_call_handler() OWNER TO testrole;
ALTER FUNCTION pg_catalog.plpgsql_inline_handler(internal) OWNER TO testrole;
ALTER FUNCTION pg_catalog.plpgsql_validator(oid) OWNER TO testrole;
ALTER LANGUAGE plpgsql OWNER TO testrole;`)
		})
		It("prints language with comment", func() {
			langs := []backup.QueryProceduralLanguage{plComment}

			backup.PrintCreateLanguageStatements(buffer, langs, funcInfoMap)
			testutils.ExpectRegexp(buffer, `CREATE PROCEDURAL LANGUAGE plpythonu;
ALTER FUNCTION pg_catalog.plpython_call_handler() OWNER TO testrole;
ALTER LANGUAGE plpythonu OWNER TO testrole;

COMMENT ON LANGUAGE plpythonu IS 'language comment'`)
		})
	})
	Describe("Functions involved in printing CREATE FUNCTION statements", func() {
		var funcDef backup.QueryFunctionDefinition
		funcDefs := make([]backup.QueryFunctionDefinition, 1)
		funcDefault := backup.QueryFunctionDefinition{"public", "func_name", false, "add_two_ints", "", "integer, integer", "integer, integer", "integer",
			"v", false, false, "", float32(1), float32(0), "", "internal", "", ""}
		BeforeEach(func() {
			funcDef = funcDefault
			funcDefs[0] = funcDef
		})

		Describe("PrintCreateFunctionStatements", func() {
			It("prints a function definition for an internal function without a binary path", func() {
				backup.PrintCreateFunctionStatements(buffer, funcDefs)
				testutils.ExpectRegexp(buffer, `CREATE FUNCTION public.func_name(integer, integer) RETURNS integer AS
$$add_two_ints$$
LANGUAGE internal;
`)
			})
			It("prints a function definition for a function with an owner", func() {
				funcDefs[0].Owner = "testrole"
				backup.PrintCreateFunctionStatements(buffer, funcDefs)
				testutils.ExpectRegexp(buffer, `CREATE FUNCTION public.func_name(integer, integer) RETURNS integer AS
$$add_two_ints$$
LANGUAGE internal;

ALTER FUNCTION public.func_name(integer, integer) OWNER TO testrole;
`)
			})
			It("prints a function definition for a function that returns a set", func() {
				funcDefs[0].ReturnsSet = true
				funcDefs[0].ResultType = "SETOF integer"
				backup.PrintCreateFunctionStatements(buffer, funcDefs)
				testutils.ExpectRegexp(buffer, `CREATE FUNCTION public.func_name(integer, integer) RETURNS SETOF integer AS
$$add_two_ints$$
LANGUAGE internal;
`)
			})
			It("prints a function definition for a function with a comment", func() {
				funcDefs[0].Comment = "This is a function comment."
				backup.PrintCreateFunctionStatements(buffer, funcDefs)
				testutils.ExpectRegexp(buffer, `CREATE FUNCTION public.func_name(integer, integer) RETURNS integer AS
$$add_two_ints$$
LANGUAGE internal;

COMMENT ON FUNCTION public.func_name(integer, integer) IS 'This is a function comment.';
`)
			})
			It("prints a function definition for a function with an owner and a comment", func() {
				funcDefs[0].Owner = "testrole"
				funcDefs[0].Comment = "This is a function comment."
				backup.PrintCreateFunctionStatements(buffer, funcDefs)
				testutils.ExpectRegexp(buffer, `CREATE FUNCTION public.func_name(integer, integer) RETURNS integer AS
$$add_two_ints$$
LANGUAGE internal;

ALTER FUNCTION public.func_name(integer, integer) OWNER TO testrole;

COMMENT ON FUNCTION public.func_name(integer, integer) IS 'This is a function comment.';
`)
			})
		})
		Describe("PrintFunctionBodyOrPath", func() {
			It("prints a function definition for an internal function with 'NULL' binary path using '-'", func() {
				funcDef.BinaryPath = "-"
				backup.PrintFunctionBodyOrPath(buffer, funcDef)
				testutils.ExpectRegexp(buffer, `
$$add_two_ints$$
`)
			})
			It("prints a function definition for an internal function with a binary path", func() {
				funcDef.BinaryPath = "$libdir/binary"
				backup.PrintFunctionBodyOrPath(buffer, funcDef)
				testutils.ExpectRegexp(buffer, `
'$libdir/binary', 'add_two_ints'
`)
			})
			It("prints a function definition for a function with a one-line function definition", func() {
				funcDef.FunctionBody = "SELECT $1+$2"
				funcDef.Language = "sql"
				backup.PrintFunctionBodyOrPath(buffer, funcDef)
				testutils.ExpectRegexp(buffer, `$_$SELECT $1+$2$_$`)
			})
			It("prints a function definition for a function with a multi-line function definition", func() {
				funcDef.FunctionBody = `
BEGIN
	SELECT $1 + $2
END
`
				funcDef.Language = "sql"
				backup.PrintFunctionBodyOrPath(buffer, funcDef)
				testutils.ExpectRegexp(buffer, `$_$
BEGIN
	SELECT $1 + $2
END
$_$`)
			})
		})
		Describe("PrintFunctionModifiers", func() {
			Context("SqlUsage cases", func() {
				It("prints 'c' as CONTAINS SQL", func() {
					funcDef.SqlUsage = "c"
					backup.PrintFunctionModifiers(buffer, funcDef)
					testutils.ExpectRegexp(buffer, "CONTAINS SQL")
				})
				It("prints 'm' as MODIFIES SQL DATA", func() {
					funcDef.SqlUsage = "m"
					backup.PrintFunctionModifiers(buffer, funcDef)
					testutils.ExpectRegexp(buffer, "MODIFIES SQL DATA")
				})
				It("prints 'n' as NO SQL", func() {
					funcDef.SqlUsage = "n"
					backup.PrintFunctionModifiers(buffer, funcDef)
					testutils.ExpectRegexp(buffer, "NO SQL")
				})
				It("prints 'r' as READS SQL DATA", func() {
					funcDef.SqlUsage = "r"
					backup.PrintFunctionModifiers(buffer, funcDef)
					testutils.ExpectRegexp(buffer, "READS SQL DATA")
				})
			})
			Context("Volatility cases", func() {
				It("does not print anything for 'v'", func() {
					funcDef.Volatility = "v"
					backup.PrintFunctionModifiers(buffer, funcDef)
					Expect(buffer.Contents()).To(Equal([]byte{}))
				})
				It("prints 's' as STABLE", func() {
					funcDef.Volatility = "s"
					backup.PrintFunctionModifiers(buffer, funcDef)
					testutils.ExpectRegexp(buffer, "STABLE")
				})
				It("prints 'i' as IMMUTABLE", func() {
					funcDef.Volatility = "i"
					backup.PrintFunctionModifiers(buffer, funcDef)
					testutils.ExpectRegexp(buffer, "IMMUTABLE")
				})
			})
			It("prints 'STRICT' if IsStrict is set", func() {
				funcDef.IsStrict = true
				backup.PrintFunctionModifiers(buffer, funcDef)
				testutils.ExpectRegexp(buffer, "STRICT")
			})
			It("prints 'SECURITY DEFINER' if IsSecurityDefiner is set", func() {
				funcDef.IsSecurityDefiner = true
				backup.PrintFunctionModifiers(buffer, funcDef)
				testutils.ExpectRegexp(buffer, "SECURITY DEFINER")
			})
			Context("Cost cases", func() {
				/*
				 * The default COST values are 1 for C and internal functions and
				 * 100 for any other language, so it should not print COST clauses
				 * for those values but print any other COST.
				 */
				It("prints 'COST 5' if Cost is set to 5", func() {
					funcDef.Cost = 5
					backup.PrintFunctionModifiers(buffer, funcDef)
					testutils.ExpectRegexp(buffer, "COST 5")
				})
				It("prints 'COST 1' if Cost is set to 1 and language is not c or internal", func() {
					funcDef.Cost = 1
					funcDef.Language = "sql"
					backup.PrintFunctionModifiers(buffer, funcDef)
					testutils.ExpectRegexp(buffer, "COST 1")
				})
				It("does not print 'COST 1' if Cost is set to 1 and language is c", func() {
					funcDef.Cost = 1
					funcDef.Language = "c"
					backup.PrintFunctionModifiers(buffer, funcDef)
					Expect(buffer.Contents()).To(Equal([]byte{}))
				})
				It("does not print 'COST 1' if Cost is set to 1 and language is internal", func() {
					funcDef.Cost = 1
					funcDef.Language = "internal"
					backup.PrintFunctionModifiers(buffer, funcDef)
					Expect(buffer.Contents()).To(Equal([]byte{}))
				})
				It("prints 'COST 100' if Cost is set to 100 and language is c", func() {
					funcDef.Cost = 100
					funcDef.Language = "c"
					backup.PrintFunctionModifiers(buffer, funcDef)
					testutils.ExpectRegexp(buffer, "COST 100")
				})
				It("prints 'COST 100' if Cost is set to 100 and language is internal", func() {
					funcDef.Cost = 100
					funcDef.Language = "internal"
					backup.PrintFunctionModifiers(buffer, funcDef)
					testutils.ExpectRegexp(buffer, "COST 100")
				})
				It("does not print 'COST 100' if Cost is set to 100 and language is not c or internal", func() {
					funcDef.Cost = 100
					funcDef.Language = "sql"
					backup.PrintFunctionModifiers(buffer, funcDef)
					Expect(buffer.Contents()).To(Equal([]byte{}))
				})
			})
			Context("NumRows cases", func() {
				/*
				 * A ROWS value of 0 means "no estimate" and 1000 means "too high
				 * to estimate", so those should not be printed but any other ROWS
				 * value should be.
				 */
				It("prints 'ROWS 5' if Rows is set to 5", func() {
					funcDef.NumRows = 5
					funcDef.ReturnsSet = true
					backup.PrintFunctionModifiers(buffer, funcDef)
					testutils.ExpectRegexp(buffer, "ROWS 5")
				})
				It("does not print 'ROWS' if Rows is set but ReturnsSet is false", func() {
					funcDef.NumRows = 100
					funcDef.ReturnsSet = false
					backup.PrintFunctionModifiers(buffer, funcDef)
					Expect(buffer.Contents()).To(Equal([]byte{}))
				})
				It("does not print 'ROWS' if Rows is set to 0", func() {
					funcDef.NumRows = 0
					funcDef.ReturnsSet = true
					backup.PrintFunctionModifiers(buffer, funcDef)
					Expect(buffer.Contents()).To(Equal([]byte{}))
				})
				It("does not print 'ROWS' if Rows is set to 1000", func() {
					funcDef.NumRows = 1000
					funcDef.ReturnsSet = true
					backup.PrintFunctionModifiers(buffer, funcDef)
					Expect(buffer.Contents()).To(Equal([]byte{}))
				})
			})
			It("prints config statements if any are set", func() {
				funcDef.Config = "SET client_min_messages TO error"
				backup.PrintFunctionModifiers(buffer, funcDef)
				testutils.ExpectRegexp(buffer, "SET client_min_messages TO error")
			})
		})
	})
	Describe("PrintCreateAggregateStatements", func() {
		aggDefs := make([]backup.QueryAggregateDefinition, 1)
		aggDefault := backup.QueryAggregateDefinition{"public", "agg_name", "integer, integer", "integer, integer", 1, 0, 0, 0, "integer", "", false, "", ""}
		funcInfoMap := map[uint32]backup.FunctionInfo{
			1: backup.FunctionInfo{QualifiedName: "public.mysfunc", Arguments: "integer"},
			2: backup.FunctionInfo{QualifiedName: "public.mypfunc", Arguments: "numeric, numeric"},
			3: backup.FunctionInfo{QualifiedName: "public.myffunc", Arguments: "text"},
			4: backup.FunctionInfo{QualifiedName: "public.mysortop", Arguments: "bigint"},
		}
		BeforeEach(func() {
			aggDefs[0] = aggDefault
		})

		It("prints an aggregate definition for an unordered aggregate with no optional specifications", func() {
			backup.PrintCreateAggregateStatements(buffer, aggDefs, funcInfoMap)
			testutils.ExpectRegexp(buffer, `CREATE AGGREGATE public.agg_name(integer, integer) (
	SFUNC = public.mysfunc,
	STYPE = integer
);`)
		})
		It("prints an aggregate definition for an ordered aggregate with no optional specifications", func() {
			aggDefs[0].IsOrdered = true
			backup.PrintCreateAggregateStatements(buffer, aggDefs, funcInfoMap)
			testutils.ExpectRegexp(buffer, `CREATE ORDERED AGGREGATE public.agg_name(integer, integer) (
	SFUNC = public.mysfunc,
	STYPE = integer
);`)
		})
		It("prints an aggregate with a preliminary function", func() {
			aggDefs[0].PreliminaryFunction = 2
			backup.PrintCreateAggregateStatements(buffer, aggDefs, funcInfoMap)
			testutils.ExpectRegexp(buffer, `CREATE AGGREGATE public.agg_name(integer, integer) (
	SFUNC = public.mysfunc,
	STYPE = integer,
	PREFUNC = public.mypfunc
);`)
		})
		It("prints an aggregate with a final function", func() {
			aggDefs[0].FinalFunction = 3
			backup.PrintCreateAggregateStatements(buffer, aggDefs, funcInfoMap)
			testutils.ExpectRegexp(buffer, `CREATE AGGREGATE public.agg_name(integer, integer) (
	SFUNC = public.mysfunc,
	STYPE = integer,
	FINALFUNC = public.myffunc
);`)
		})
		It("prints an aggregate with an initial condition", func() {
			aggDefs[0].InitialValue = "0"
			backup.PrintCreateAggregateStatements(buffer, aggDefs, funcInfoMap)
			testutils.ExpectRegexp(buffer, `CREATE AGGREGATE public.agg_name(integer, integer) (
	SFUNC = public.mysfunc,
	STYPE = integer,
	INITCOND = '0'
);`)
		})
		It("prints an aggregate with a sort operator", func() {
			aggDefs[0].SortOperator = 4
			backup.PrintCreateAggregateStatements(buffer, aggDefs, funcInfoMap)
			testutils.ExpectRegexp(buffer, `CREATE AGGREGATE public.agg_name(integer, integer) (
	SFUNC = public.mysfunc,
	STYPE = integer,
	SORTOP = public.mysortop
);`)
		})
		It("prints an aggregate with multiple specifications", func() {
			aggDefs[0].FinalFunction = 3
			aggDefs[0].SortOperator = 4
			backup.PrintCreateAggregateStatements(buffer, aggDefs, funcInfoMap)
			testutils.ExpectRegexp(buffer, `CREATE AGGREGATE public.agg_name(integer, integer) (
	SFUNC = public.mysfunc,
	STYPE = integer,
	FINALFUNC = public.myffunc,
	SORTOP = public.mysortop
);`)
		})
	})
	Describe("PrintCreateCastStatements", func() {
		It("prints an explicit cast with a function", func() {
			castDef := backup.QueryCastDefinition{"src", "dst", "public", "cast_func", "integer, integer", "e", ""}
			backup.PrintCreateCastStatements(buffer, []backup.QueryCastDefinition{castDef})
			testutils.ExpectRegexp(buffer, `CREATE CAST (src AS dst)
	WITH FUNCTION public.cast_func(integer, integer);`)
		})
		It("prints an implicit cast with a function", func() {
			castDef := backup.QueryCastDefinition{"src", "dst", "public", "cast_func", "integer, integer", "i", ""}
			backup.PrintCreateCastStatements(buffer, []backup.QueryCastDefinition{castDef})
			testutils.ExpectRegexp(buffer, `CREATE CAST (src AS dst)
	WITH FUNCTION public.cast_func(integer, integer)
AS IMPLICIT;`)
		})
		It("prints an assignment cast with a function", func() {
			castDef := backup.QueryCastDefinition{"src", "dst", "public", "cast_func", "integer, integer", "a", ""}
			backup.PrintCreateCastStatements(buffer, []backup.QueryCastDefinition{castDef})
			testutils.ExpectRegexp(buffer, `CREATE CAST (src AS dst)
	WITH FUNCTION public.cast_func(integer, integer)
AS ASSIGNMENT;`)
		})
		It("prints an explicit cast without a function", func() {
			castDef := backup.QueryCastDefinition{"src", "dst", "", "", "", "e", ""}
			backup.PrintCreateCastStatements(buffer, []backup.QueryCastDefinition{castDef})
			testutils.ExpectRegexp(buffer, `CREATE CAST (src AS dst)
	WITHOUT FUNCTION;`)
		})
		It("prints an implicit cast without a function", func() {
			castDef := backup.QueryCastDefinition{"src", "dst", "", "", "", "i", ""}
			backup.PrintCreateCastStatements(buffer, []backup.QueryCastDefinition{castDef})
			testutils.ExpectRegexp(buffer, `CREATE CAST (src AS dst)
	WITHOUT FUNCTION
AS IMPLICIT;`)
		})
		It("prints an assignment cast without a function", func() {
			castDef := backup.QueryCastDefinition{"src", "dst", "", "", "", "a", ""}
			backup.PrintCreateCastStatements(buffer, []backup.QueryCastDefinition{castDef})
			testutils.ExpectRegexp(buffer, `CREATE CAST (src AS dst)
	WITHOUT FUNCTION
AS ASSIGNMENT;`)
		})
		It("prints a cast with a comment", func() {
			castDef := backup.QueryCastDefinition{"src", "dst", "", "", "", "e", "This is a cast comment."}
			backup.PrintCreateCastStatements(buffer, []backup.QueryCastDefinition{castDef})
			testutils.ExpectRegexp(buffer, `CREATE CAST (src AS dst)
	WITHOUT FUNCTION;

COMMENT ON CAST (src AS dst) IS 'This is a cast comment.';`)
		})
	})
	Describe("PrintCreateCompositeAndEnumTypeStatements", func() {
		compOne := backup.TypeDefinition{TypeSchema: "public", TypeName: "composite_type", Type: "c", AttName: "bar", AttType: "integer"}
		compTwo := backup.TypeDefinition{TypeSchema: "public", TypeName: "composite_type", Type: "c", AttName: "baz", AttType: "text"}
		compThree := backup.TypeDefinition{TypeSchema: "public", TypeName: "composite_type", Type: "c", AttName: "foo", AttType: "float"}
		compCommentOwnerOne := backup.TypeDefinition{TypeSchema: "public", TypeName: "composite_type", Type: "c", AttName: "bar",
			AttType: "integer", Comment: "This is a type comment.", Owner: "test_role"}
		compCommentOwnerTwo := backup.TypeDefinition{TypeSchema: "public", TypeName: "composite_type", Type: "c", AttName: "foo",
			AttType: "float", Comment: "This is a type comment.", Owner: "test_role"}
		enumOne := backup.TypeDefinition{TypeSchema: "public", TypeName: "enum_type", Type: "e", EnumLabels: "'bar',\n\t'baz',\n\t'foo'"}

		It("prints a composite type with one attribute", func() {
			backup.PrintCreateCompositeAndEnumTypeStatements(buffer, []backup.TypeDefinition{compOne})
			testutils.ExpectRegexp(buffer, `CREATE TYPE public.composite_type AS (
	bar integer
);`)
		})
		It("prints a composite type with multiple attributes", func() {
			backup.PrintCreateCompositeAndEnumTypeStatements(buffer, []backup.TypeDefinition{compOne, compTwo, compThree})
			testutils.ExpectRegexp(buffer, `CREATE TYPE public.composite_type AS (
	bar integer,
	baz text,
	foo float
);`)
		})
		It("prints a composite type with comment and owner", func() {
			backup.PrintCreateCompositeAndEnumTypeStatements(buffer, []backup.TypeDefinition{compCommentOwnerOne, compCommentOwnerTwo})
			testutils.ExpectRegexp(buffer, `CREATE TYPE public.composite_type AS (
	bar integer,
	foo float
);

COMMENT ON TYPE public.composite_type IS 'This is a type comment.';

ALTER TYPE public.composite_type OWNER TO test_role;`)
		})
		It("prints an enum type with multiple attributes", func() {
			backup.PrintCreateCompositeAndEnumTypeStatements(buffer, []backup.TypeDefinition{enumOne})
			testutils.ExpectRegexp(buffer, `CREATE TYPE public.enum_type AS ENUM (
	'bar',
	'baz',
	'foo'
);`)
		})
		It("prints both an enum type and a composite type", func() {
			backup.PrintCreateCompositeAndEnumTypeStatements(buffer, []backup.TypeDefinition{compOne, enumOne})
			testutils.ExpectRegexp(buffer, `CREATE TYPE public.composite_type AS (
	bar integer
);


CREATE TYPE public.enum_type AS ENUM (
	'bar',
	'baz',
	'foo'
);`)
		})
	})
	Describe("PrintCreateBaseTypeStatements", func() {
		baseSimple := backup.TypeDefinition{"public", "base_type", "b", "", "", "input_fn", "output_fn",
			"-", "-", "-", "-", -1, false, "c", "p", "", "-", "", "", "", ""}
		basePartial := backup.TypeDefinition{"public", "base_type", "b", "", "", "input_fn", "output_fn",
			"receive_fn", "send_fn", "modin_fn", "modout_fn", -1, false, "c", "p", "42", "int4", ",", "", "", ""}
		baseFull := backup.TypeDefinition{"public", "base_type", "b", "", "", "input_fn", "output_fn",
			"receive_fn", "send_fn", "modin_fn", "modout_fn", 16, true, "s", "e", "42", "int4", ",", "", "", ""}
		basePermOne := backup.TypeDefinition{"public", "base_type", "b", "", "", "input_fn", "output_fn",
			"-", "-", "-", "-", -1, false, "d", "m", "", "-", "", "", "", ""}
		basePermTwo := backup.TypeDefinition{"public", "base_type", "b", "", "", "input_fn", "output_fn",
			"-", "-", "-", "-", -1, false, "i", "x", "", "-", "", "", "", ""}
		baseCommentOwner := backup.TypeDefinition{"public", "base_type", "b", "", "", "input_fn", "output_fn",
			"-", "-", "-", "-", -1, false, "c", "p", "", "-", "", "", "This is a type comment.", "test_role"}

		It("prints a base type with no optional arguments", func() {
			backup.PrintCreateBaseTypeStatements(buffer, []backup.TypeDefinition{baseSimple})
			testutils.ExpectRegexp(buffer, `CREATE TYPE public.base_type (
	INPUT = input_fn,
	OUTPUT = output_fn
);`)
		})
		It("prints a base type where all optional arguments have default values where possible", func() {
			backup.PrintCreateBaseTypeStatements(buffer, []backup.TypeDefinition{basePartial})
			testutils.ExpectRegexp(buffer, `CREATE TYPE public.base_type (
	INPUT = input_fn,
	OUTPUT = output_fn,
	RECEIVE = receive_fn,
	SEND = send_fn,
	TYPMOD_IN = modin_fn,
	TYPMOD_OUT = modout_fn,
	DEFAULT = 42,
	ELEMENT = int4,
	DELIMITER = ','
);`)
		})
		It("prints a base type with all optional arguments provided", func() {
			backup.PrintCreateBaseTypeStatements(buffer, []backup.TypeDefinition{baseFull})
			testutils.ExpectRegexp(buffer, `CREATE TYPE public.base_type (
	INPUT = input_fn,
	OUTPUT = output_fn,
	RECEIVE = receive_fn,
	SEND = send_fn,
	TYPMOD_IN = modin_fn,
	TYPMOD_OUT = modout_fn,
	INTERNALLENGTH = 16,
	PASSEDBYVALUE,
	ALIGNMENT = int2,
	STORAGE = extended,
	DEFAULT = 42,
	ELEMENT = int4,
	DELIMITER = ','
);`)
		})
		It("prints a base type with double alignment and main storage", func() {
			backup.PrintCreateBaseTypeStatements(buffer, []backup.TypeDefinition{basePermOne})
			testutils.ExpectRegexp(buffer, `CREATE TYPE public.base_type (
	INPUT = input_fn,
	OUTPUT = output_fn,
	ALIGNMENT = double,
	STORAGE = main
);`)
		})
		It("prints a base type with int4 alignment and external storage", func() {
			backup.PrintCreateBaseTypeStatements(buffer, []backup.TypeDefinition{basePermTwo})
			testutils.ExpectRegexp(buffer, `CREATE TYPE public.base_type (
	INPUT = input_fn,
	OUTPUT = output_fn,
	ALIGNMENT = int4,
	STORAGE = external
);`)
		})
		It("prints a base type with comment and owner", func() {
			backup.PrintCreateBaseTypeStatements(buffer, []backup.TypeDefinition{baseCommentOwner})
			testutils.ExpectRegexp(buffer, `CREATE TYPE public.base_type (
	INPUT = input_fn,
	OUTPUT = output_fn
);

COMMENT ON TYPE public.base_type IS 'This is a type comment.';

ALTER TYPE public.base_type OWNER TO test_role;`)
		})
	})
	Describe("PrintShellTypeStatements", func() {
		baseOne := backup.TypeDefinition{"public", "base_type1", "b", "", "", "input_fn", "output_fn",
			"-", "-", "-", "-", -1, false, "c", "p", "", "-", "", "", "", ""}
		baseTwo := backup.TypeDefinition{"public", "base_type2", "b", "", "", "input_fn", "output_fn",
			"-", "-", "-", "-", -1, false, "c", "p", "", "-", "", "", "", ""}
		compOne := backup.TypeDefinition{TypeSchema: "public", TypeName: "composite_type1", Type: "c", AttName: "bar", AttType: "integer"}
		compTwo := backup.TypeDefinition{TypeSchema: "public", TypeName: "composite_type2", Type: "c", AttName: "bar", AttType: "integer"}
		enumOne := backup.TypeDefinition{TypeSchema: "public", TypeName: "enum_type", Type: "e", EnumLabels: "'bar',\n\t'baz',\n\t'foo'"}
		It("prints shell type for only a base type", func() {
			backup.PrintShellTypeStatements(buffer, []backup.TypeDefinition{baseOne, baseTwo, compOne, compTwo, enumOne})
			testutils.ExpectRegexp(buffer, `CREATE TYPE public.base_type1;
CREATE TYPE public.base_type2;`)
		})
	})
})
