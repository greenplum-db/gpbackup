package backup_test

import (
	"database/sql"
	"fmt"

	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"

	. "github.com/onsi/ginkgo/v2"
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
		depMap = make(map[backup.UniqueID]map[backup.UniqueID]bool)
		tocfile, backupfile = testutils.InitializeTestTOC(buffer, "predata")
	})
	Describe("TopologicalSort", func() {
		It("returns the original slice if there are no dependencies among objects", func() {
			relations := []backup.Sortable{relation1, relation2, relation3}

			relations, _ = backup.TopologicalSort(relations, depMap)

			Expect(relations[0].FQN()).To(Equal("public.relation1"))
			Expect(relations[1].FQN()).To(Equal("public.relation2"))
			Expect(relations[2].FQN()).To(Equal("public.relation3"))
		})
		It("sorts the slice correctly if there is an object dependent on one other object", func() {
			depMap[backup.UniqueID{ClassID: backup.PG_CLASS_OID, Oid: 1}] = map[backup.UniqueID]bool{{ClassID: backup.PG_CLASS_OID, Oid: 3}: true}
			relations := []backup.Sortable{relation1, relation2, relation3}

			relations, _ = backup.TopologicalSort(relations, depMap)

			Expect(relations[0].FQN()).To(Equal("public.relation2"))
			Expect(relations[1].FQN()).To(Equal("public.relation3"))
			Expect(relations[2].FQN()).To(Equal("public.relation1"))
		})
		It("sorts the slice correctly if there are two objects dependent on one other object", func() {
			depMap[backup.UniqueID{ClassID: backup.PG_CLASS_OID, Oid: 1}] = map[backup.UniqueID]bool{{ClassID: backup.PG_CLASS_OID, Oid: 2}: true}
			depMap[backup.UniqueID{ClassID: backup.PG_CLASS_OID, Oid: 3}] = map[backup.UniqueID]bool{{ClassID: backup.PG_CLASS_OID, Oid: 2}: true}
			relations := []backup.Sortable{relation1, relation2, relation3}

			relations, _ = backup.TopologicalSort(relations, depMap)

			Expect(relations[0].FQN()).To(Equal("public.relation2"))
			Expect(relations[1].FQN()).To(Equal("public.relation1"))
			Expect(relations[2].FQN()).To(Equal("public.relation3"))
		})
		It("sorts the slice correctly if there is one object dependent on two other objects", func() {
			depMap[backup.UniqueID{ClassID: backup.PG_CLASS_OID, Oid: 2}] = map[backup.UniqueID]bool{{ClassID: backup.PG_CLASS_OID, Oid: 1}: true, {ClassID: backup.PG_CLASS_OID, Oid: 1}: true}
			relations := []backup.Sortable{relation1, relation2, relation3}

			relations, _ = backup.TopologicalSort(relations, depMap)

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
			sortable, _ = backup.TopologicalSort(sortable, depMap)
		})
		It("aborts if dependencies are not met", func() {
			depMap[backup.UniqueID{ClassID: backup.PG_CLASS_OID, Oid: 1}] = map[backup.UniqueID]bool{{ClassID: backup.PG_CLASS_OID, Oid: 2}: true}
			depMap[backup.UniqueID{ClassID: backup.PG_CLASS_OID, Oid: 1}] = map[backup.UniqueID]bool{{ClassID: backup.PG_CLASS_OID, Oid: 4}: true}
			sortable := []backup.Sortable{relation1, relation2}

			defer testhelper.ShouldPanicWithMessage("Dependency resolution failed; see log file gbytes.Buffer for details. This is a bug, please report.")
			sortable, _ = backup.TopologicalSort(sortable, depMap)
		})
	})
	Describe("PrintDependentObjectStatements", func() {
		var (
			objects             []backup.Sortable
			metadataMap         backup.MetadataMap
			funcInfoMap         map[uint32]backup.FunctionInfo
			plannerSupportValue string
			parallelValue       string
			default_parallel    string
		)
		BeforeEach(func() {
			plannerSupportValue = ""
			parallelValue = ""
			default_parallel = ""
			if connectionPool.Version.AtLeast("7") {
				plannerSupportValue = "-"
				parallelValue = "u"
				default_parallel = " PARALLEL UNSAFE"
			}
			funcInfoMap = map[uint32]backup.FunctionInfo{
				1: {QualifiedName: "public.write_to_s3", Arguments: sql.NullString{String: "", Valid: true}, IsInternal: false},
				2: {QualifiedName: "public.read_from_s3", Arguments: sql.NullString{String: "", Valid: true}, IsInternal: false},
			}
			objects = []backup.Sortable{
				backup.Function{Oid: 1, Schema: "public", Name: "function", FunctionBody: "SELECT $1 + $2",
					Arguments:  sql.NullString{String: "integer, integer", Valid: true},
					IdentArgs:  sql.NullString{String: "integer, integer", Valid: true},
					ResultType: sql.NullString{String: "integer", Valid: true},
					Language:   "sql", PlannerSupport: plannerSupportValue, Parallel: parallelValue},
				backup.BaseType{Oid: 2, Schema: "public", Name: "base", Input: "typin", Output: "typout", Category: "U"},
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
				{Name: "check_constraint", Def: sql.NullString{String: "CHECK (VALUE > 2)", Valid: true}, OwningObject: "public.domain"},
			}
			backup.PrintDependentObjectStatements(backupfile, tocfile, objects, metadataMap, constraints, funcInfoMap)
			testhelper.ExpectRegexp(buffer, fmt.Sprintf(`
CREATE FUNCTION public.function(integer, integer) RETURNS integer AS
$_$SELECT $1 + $2$_$
LANGUAGE sql%s;


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
`, default_parallel))
		})
		It("prints create statements for dependent types, functions, protocols, and tables (no domain constraint)", func() {
			constraints := make([]backup.Constraint, 0)
			backup.PrintDependentObjectStatements(backupfile, tocfile, objects, metadataMap, constraints, funcInfoMap)
			testhelper.ExpectRegexp(buffer, fmt.Sprintf(`
CREATE FUNCTION public.function(integer, integer) RETURNS integer AS
$_$SELECT $1 + $2$_$
LANGUAGE sql%s;


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
`, default_parallel))
		})
	})
	Describe("MarkViewsDependingOnConstraints", func() {
		It("marks views that depend on constraints", func() {
			view1 := backup.View{Schema: "public", Name: "view1", Oid: 1}
			view2 := backup.View{Schema: "public", Name: "view2", Oid: 2}
			view3 := backup.View{Schema: "public", Name: "view3", Oid: 3}
			sortableObjs := []backup.Sortable{view1, view2, view3}

			depMap[backup.UniqueID{ClassID: backup.PG_CLASS_OID, Oid: 1}] = map[backup.UniqueID]bool{{ClassID: backup.PG_CONSTRAINT_OID, Oid: 4}: true}

			resultViews := backup.MarkViewsDependingOnConstraints(sortableObjs, depMap)
			Expect(resultViews).To(HaveLen(1))
			Expect(resultViews[0].FQN()).To(Equal("public.view1"))
		})
		It("marks no views if none depend on constraints", func() {
			view1 := backup.View{Schema: "public", Name: "view1", Oid: 1}
			view2 := backup.View{Schema: "public", Name: "view2", Oid: 2}
			view3 := backup.View{Schema: "public", Name: "view3", Oid: 3}
			sortableObjs := []backup.Sortable{view1, view2, view3}

			resultViews := backup.MarkViewsDependingOnConstraints(sortableObjs, depMap)
			Expect(resultViews).To(HaveLen(0))

		})
		It("does not marks any object that is not a view", func() {
			view1 := backup.View{Schema: "public", Name: "view1", Oid: 1}
			view2 := backup.View{Schema: "public", Name: "view2", Oid: 2}
			view3 := backup.View{Schema: "public", Name: "view3", Oid: 3}
			relation1 := backup.Relation{Schema: "public", Name: "relation1", Oid: 4}
			sortableObjs := []backup.Sortable{view1, view2, view3, relation1}

			depMap[backup.UniqueID{ClassID: backup.PG_CLASS_OID, Oid: 1}] = map[backup.UniqueID]bool{{ClassID: backup.PG_CONSTRAINT_OID, Oid: 4}: true}

			resultViews := backup.MarkViewsDependingOnConstraints(sortableObjs, depMap)
			Expect(resultViews).To(HaveLen(1))
			Expect(resultViews[0].FQN()).To(Equal("public.view1"))
		})
	})
})
