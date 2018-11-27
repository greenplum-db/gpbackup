package backup_test

import (
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup/dependencies tests", func() {
	var (
		relation1 backup.Relation
		relation2 backup.Relation
		relation3 backup.Relation
		depMap    map[backup.UniqueID]map[backup.UniqueID]bool
	)

	BeforeEach(func() {
		relation1 = backup.Relation{Schema: "public", Name: "relation1", Oid: 1}
		relation2 = backup.Relation{Schema: "public", Name: "relation2", Oid: 2}
		relation3 = backup.Relation{Schema: "public", Name: "relation3", Oid: 3}
		depMap = make(map[backup.UniqueID]map[backup.UniqueID]bool, 0)
		toc, backupfile = testutils.InitializeTestTOC(buffer, "predata")
	})
	Describe("TopologicalSort", func() {
		It("returns the original slice if there are no dependencies among objects", func() {
			relations := []backup.Sortable{relation1, relation2, relation3}

			relations = backup.TopologicalSort(relations, depMap)

			Expect(relations[0].FQN()).To(Equal("public.relation1"))
			Expect(relations[1].FQN()).To(Equal("public.relation2"))
			Expect(relations[2].FQN()).To(Equal("public.relation3"))
		})
		It("sorts the slice correctly if there is an object dependent on one other object", func() {
			depMap[backup.UniqueID{ClassID: backup.PG_CLASS_OID, Oid: 1}] = map[backup.UniqueID]bool{{ClassID: backup.PG_CLASS_OID, Oid: 3}: true}
			relations := []backup.Sortable{relation1, relation2, relation3}

			relations = backup.TopologicalSort(relations, depMap)

			Expect(relations[0].FQN()).To(Equal("public.relation2"))
			Expect(relations[1].FQN()).To(Equal("public.relation3"))
			Expect(relations[2].FQN()).To(Equal("public.relation1"))
		})
		It("sorts the slice correctly if there are two objects dependent on one other object", func() {
			depMap[backup.UniqueID{ClassID: backup.PG_CLASS_OID, Oid: 1}] = map[backup.UniqueID]bool{{ClassID: backup.PG_CLASS_OID, Oid: 2}: true}
			depMap[backup.UniqueID{ClassID: backup.PG_CLASS_OID, Oid: 3}] = map[backup.UniqueID]bool{{ClassID: backup.PG_CLASS_OID, Oid: 2}: true}
			relations := []backup.Sortable{relation1, relation2, relation3}

			relations = backup.TopologicalSort(relations, depMap)

			Expect(relations[0].FQN()).To(Equal("public.relation2"))
			Expect(relations[1].FQN()).To(Equal("public.relation1"))
			Expect(relations[2].FQN()).To(Equal("public.relation3"))
		})
		It("sorts the slice correctly if there is one object dependent on two other objects", func() {
			depMap[backup.UniqueID{ClassID: backup.PG_CLASS_OID, Oid: 2}] = map[backup.UniqueID]bool{{ClassID: backup.PG_CLASS_OID, Oid: 1}: true, {ClassID: backup.PG_CLASS_OID, Oid: 1}: true}
			relations := []backup.Sortable{relation1, relation2, relation3}

			relations = backup.TopologicalSort(relations, depMap)

			Expect(relations[0].FQN()).To(Equal("public.relation1"))
			Expect(relations[1].FQN()).To(Equal("public.relation3"))
			Expect(relations[2].FQN()).To(Equal("public.relation2"))
		})
		It("aborts if dependency loop (this shouldn't be possible)", func() {
			depMap[backup.UniqueID{ClassID: backup.PG_CLASS_OID, Oid: 1}] = map[backup.UniqueID]bool{{ClassID: backup.PG_CLASS_OID, Oid: 3}: true}
			depMap[backup.UniqueID{ClassID: backup.PG_CLASS_OID, Oid: 2}] = map[backup.UniqueID]bool{{ClassID: backup.PG_CLASS_OID, Oid: 1}: true}
			depMap[backup.UniqueID{ClassID: backup.PG_CLASS_OID, Oid: 3}] = map[backup.UniqueID]bool{{ClassID: backup.PG_CLASS_OID, Oid: 2}: true}

			sortable := []backup.Sortable{relation1, relation2, relation3}
			defer func() {
				testhelper.ExpectRegexp(logfile, "Object: public.relation1 {ClassID:1259 Oid:1}")
				testhelper.ExpectRegexp(logfile, "Dependencies:")
				testhelper.ExpectRegexp(logfile, "\tpublic.relation3 {ClassID:1259 Oid:3}")
				testhelper.ExpectRegexp(logfile, "Object: public.relation2 {ClassID:1259 Oid:2}")
				testhelper.ExpectRegexp(logfile, "Dependencies:")
				testhelper.ExpectRegexp(logfile, "\tpublic.relation1 {ClassID:1259 Oid:1}")
				testhelper.ExpectRegexp(logfile, "Object: public.relation3 {ClassID:1259 Oid:3}")
				testhelper.ExpectRegexp(logfile, "Dependencies:")
				testhelper.ExpectRegexp(logfile, "\tpublic.relation2 {ClassID:1259 Oid:2}")
			}()
			defer testhelper.ShouldPanicWithMessage("Dependency resolution failed; see log file gbytes.Buffer for details. This is a bug, please report.")
			sortable = backup.TopologicalSort(sortable, depMap)
		})
		It("aborts if dependencies are not met", func() {
			depMap[backup.UniqueID{ClassID: backup.PG_CLASS_OID, Oid: 1}] = map[backup.UniqueID]bool{{ClassID: backup.PG_CLASS_OID, Oid: 2}: true}
			depMap[backup.UniqueID{ClassID: backup.PG_CLASS_OID, Oid: 1}] = map[backup.UniqueID]bool{{ClassID: backup.PG_CLASS_OID, Oid: 4}: true}
			sortable := []backup.Sortable{relation1, relation2}

			defer testhelper.ShouldPanicWithMessage("Dependency resolution failed; see log file gbytes.Buffer for details. This is a bug, please report.")
			sortable = backup.TopologicalSort(sortable, depMap)
		})
	})
	Describe("ConstructDependentObjectMetadataMap", func() {
		It("composes metadata maps for functions, types, and tables into one map", func() {
			funcMap := backup.MetadataMap{backup.UniqueID{Oid: 1}: backup.ObjectMetadata{Comment: "function"}}
			typeMap := backup.MetadataMap{backup.UniqueID{Oid: 2}: backup.ObjectMetadata{Comment: "type"}}
			tableMap := backup.MetadataMap{backup.UniqueID{Oid: 3}: backup.ObjectMetadata{Comment: "relation"}}
			protoMap := backup.MetadataMap{backup.UniqueID{Oid: 4}: backup.ObjectMetadata{Comment: "protocol"}}
			tsParserMap := backup.MetadataMap{backup.UniqueID{Oid: 5}: backup.ObjectMetadata{Comment: "text search parser"}}
			tsConfigMap := backup.MetadataMap{backup.UniqueID{Oid: 6}: backup.ObjectMetadata{Comment: "text search config"}}
			tsTemplateMap := backup.MetadataMap{backup.UniqueID{Oid: 7}: backup.ObjectMetadata{Comment: "text search template"}}
			tsDictionaryMap := backup.MetadataMap{backup.UniqueID{Oid: 8}: backup.ObjectMetadata{Comment: "text search dictionary"}}
			result := backup.ConstructDependentObjectMetadataMap(funcMap, typeMap, tableMap, protoMap, tsParserMap, tsConfigMap, tsTemplateMap, tsDictionaryMap)
			expected := backup.MetadataMap{
				backup.UniqueID{Oid: 1}: backup.ObjectMetadata{Comment: "function"},
				backup.UniqueID{Oid: 2}: backup.ObjectMetadata{Comment: "type"},
				backup.UniqueID{Oid: 3}: backup.ObjectMetadata{Comment: "relation"},
				backup.UniqueID{Oid: 4}: backup.ObjectMetadata{Comment: "protocol"},
				backup.UniqueID{Oid: 5}: backup.ObjectMetadata{Comment: "text search parser"},
				backup.UniqueID{Oid: 6}: backup.ObjectMetadata{Comment: "text search config"},
				backup.UniqueID{Oid: 7}: backup.ObjectMetadata{Comment: "text search template"},
				backup.UniqueID{Oid: 8}: backup.ObjectMetadata{Comment: "text search dictionary"},
			}
			Expect(result).To(Equal(expected))
		})
	})
	Describe("PrintDependentObjectStatements", func() {
		var (
			objects     []backup.Sortable
			metadataMap backup.MetadataMap
			funcInfoMap map[uint32]backup.FunctionInfo
		)
		BeforeEach(func() {
			funcInfoMap = map[uint32]backup.FunctionInfo{
				1: {QualifiedName: "public.write_to_s3", Arguments: "", IsInternal: false},
				2: {QualifiedName: "public.read_from_s3", Arguments: "", IsInternal: false},
			}
			objects = []backup.Sortable{
				backup.Function{Oid: 1, Schema: "public", Name: "function", FunctionBody: "SELECT $1 + $2",
					Arguments: "integer, integer", IdentArgs: "integer, integer", ResultType: "integer", Language: "sql"},
				backup.BaseType{Oid: 2, Schema: "public", Name: "base", Type: "b", Input: "typin", Output: "typout", Category: "U"},
				backup.CompositeType{Oid: 3, Schema: "public", Name: "composite", Attributes: []backup.Attribute{{Name: "foo", Type: "integer"}}},
				backup.Domain{Oid: 4, Schema: "public", Name: "domain", BaseType: "numeric"},
				backup.Table{
					Relation:        backup.Relation{Oid: 5, Schema: "public", Name: "relation"},
					TableDefinition: backup.TableDefinition{DistPolicy: "DISTRIBUTED RANDOMLY", ColumnDefs: []backup.ColumnDefinition{}},
				},
				backup.ExternalProtocol{Oid: 6, Name: "ext_protocol", Trusted: true, ReadFunction: 2, WriteFunction: 1, Validator: 0},
				backup.RangeType{Oid: 7, Schema: "public", Name: "rangetype1"},
			}
			metadataMap = backup.MetadataMap{
				backup.UniqueID{ClassID: backup.PG_PROC_OID, Oid: 1}:        backup.ObjectMetadata{Comment: "function"},
				backup.UniqueID{ClassID: backup.PG_TYPE_OID, Oid: 2}:        backup.ObjectMetadata{Comment: "base type"},
				backup.UniqueID{ClassID: backup.PG_TYPE_OID, Oid: 3}:        backup.ObjectMetadata{Comment: "composite type"},
				backup.UniqueID{ClassID: backup.PG_TYPE_OID, Oid: 4}:        backup.ObjectMetadata{Comment: "domain"},
				backup.UniqueID{ClassID: backup.PG_CLASS_OID, Oid: 5}:       backup.ObjectMetadata{Comment: "relation"},
				backup.UniqueID{ClassID: backup.PG_EXTPROTOCOL_OID, Oid: 6}: backup.ObjectMetadata{Comment: "protocol"},
				backup.UniqueID{ClassID: backup.PG_TYPE_OID, Oid: 7}:        backup.ObjectMetadata{Comment: "range type"},
			}
		})
		It("prints create statements for dependent types, functions, protocols, and tables (domain has a constraint)", func() {
			constraints := []backup.Constraint{
				{Name: "check_constraint", ConDef: "CHECK (VALUE > 2)", OwningObject: "public.domain"},
			}
			backup.PrintDependentObjectStatements(backupfile, toc, objects, metadataMap, constraints, funcInfoMap)
			testhelper.ExpectRegexp(buffer, `
CREATE FUNCTION public.function(integer, integer) RETURNS integer AS
$_$SELECT $1 + $2$_$
LANGUAGE sql;


COMMENT ON FUNCTION public.function(integer, integer) IS 'function';


CREATE TYPE public.base (
	INPUT = typin,
	OUTPUT = typout
);


COMMENT ON TYPE public.base IS 'base type';


CREATE TYPE public.composite AS (
	foo integer
);

COMMENT ON TYPE public.composite IS 'composite type';

CREATE DOMAIN public.domain AS numeric
	CONSTRAINT check_constraint CHECK (VALUE > 2);


COMMENT ON DOMAIN public.domain IS 'domain';


CREATE TABLE public.relation (
) DISTRIBUTED RANDOMLY;


COMMENT ON TABLE public.relation IS 'relation';


CREATE TRUSTED PROTOCOL ext_protocol (readfunc = public.read_from_s3, writefunc = public.write_to_s3);


COMMENT ON PROTOCOL ext_protocol IS 'protocol';
`)
		})
		It("prints create statements for dependent types, functions, protocols, and tables (no domain constraint)", func() {
			constraints := []backup.Constraint{}
			backup.PrintDependentObjectStatements(backupfile, toc, objects, metadataMap, constraints, funcInfoMap)
			testhelper.ExpectRegexp(buffer, `
CREATE FUNCTION public.function(integer, integer) RETURNS integer AS
$_$SELECT $1 + $2$_$
LANGUAGE sql;


COMMENT ON FUNCTION public.function(integer, integer) IS 'function';


CREATE TYPE public.base (
	INPUT = typin,
	OUTPUT = typout
);


COMMENT ON TYPE public.base IS 'base type';


CREATE TYPE public.composite AS (
	foo integer
);

COMMENT ON TYPE public.composite IS 'composite type';

CREATE DOMAIN public.domain AS numeric;


COMMENT ON DOMAIN public.domain IS 'domain';


CREATE TABLE public.relation (
) DISTRIBUTED RANDOMLY;


COMMENT ON TABLE public.relation IS 'relation';


CREATE TRUSTED PROTOCOL ext_protocol (readfunc = public.read_from_s3, writefunc = public.write_to_s3);


COMMENT ON PROTOCOL ext_protocol IS 'protocol';
`)
		})
	})
})
