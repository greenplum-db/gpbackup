package backup_test

import (
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup/dependencies tests", func() {
	var (
		relation1 backup.Relation
		relation2 backup.Relation
		relation3 backup.Relation
		depMap    map[backup.DepEntry][]backup.DepEntry
	)

	BeforeEach(func() {
		relation1 = backup.Relation{Schema: "public", Name: "relation1", Oid: 1}
		relation2 = backup.Relation{Schema: "public", Name: "relation2", Oid: 2}
		relation3 = backup.Relation{Schema: "public", Name: "relation3", Oid: 3}
		depMap = make(map[backup.DepEntry][]backup.DepEntry, 0)
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
			depMap[backup.DepEntry{Classid: backup.PG_CLASS_OID, Objid: 1}] = []backup.DepEntry{{Classid: backup.PG_CLASS_OID, Objid: 3}}
			relations := []backup.Sortable{relation1, relation2, relation3}

			relations = backup.TopologicalSort(relations, depMap)

			Expect(relations[0].FQN()).To(Equal("public.relation2"))
			Expect(relations[1].FQN()).To(Equal("public.relation3"))
			Expect(relations[2].FQN()).To(Equal("public.relation1"))
		})
		It("sorts the slice correctly if there are two objects dependent on one other object", func() {
			depMap[backup.DepEntry{Classid: backup.PG_CLASS_OID, Objid: 1}] = []backup.DepEntry{{Classid: backup.PG_CLASS_OID, Objid: 2}}
			depMap[backup.DepEntry{Classid: backup.PG_CLASS_OID, Objid: 3}] = []backup.DepEntry{{Classid: backup.PG_CLASS_OID, Objid: 2}}
			relations := []backup.Sortable{relation1, relation2, relation3}

			relations = backup.TopologicalSort(relations, depMap)

			Expect(relations[0].FQN()).To(Equal("public.relation2"))
			Expect(relations[1].FQN()).To(Equal("public.relation1"))
			Expect(relations[2].FQN()).To(Equal("public.relation3"))
		})
		It("sorts the slice correctly if there is one object dependent on two other objects", func() {
			depMap[backup.DepEntry{Classid: backup.PG_CLASS_OID, Objid: 2}] = []backup.DepEntry{{Classid: backup.PG_CLASS_OID, Objid: 1}, {Classid: backup.PG_CLASS_OID, Objid: 1}}
			relations := []backup.Sortable{relation1, relation2, relation3}

			relations = backup.TopologicalSort(relations, depMap)

			Expect(relations[0].FQN()).To(Equal("public.relation1"))
			Expect(relations[1].FQN()).To(Equal("public.relation3"))
			Expect(relations[2].FQN()).To(Equal("public.relation2"))
		})
		It("aborts if dependency loop (this shouldn't be possible)", func() {
			depMap[backup.DepEntry{Classid: backup.PG_CLASS_OID, Objid: 1}] = []backup.DepEntry{{Classid: backup.PG_CLASS_OID, Objid: 3}}
			depMap[backup.DepEntry{Classid: backup.PG_CLASS_OID, Objid: 2}] = []backup.DepEntry{{Classid: backup.PG_CLASS_OID, Objid: 1}}
			depMap[backup.DepEntry{Classid: backup.PG_CLASS_OID, Objid: 3}] = []backup.DepEntry{{Classid: backup.PG_CLASS_OID, Objid: 2}}

			sortable := []backup.Sortable{relation1, relation2, relation3}

			defer testhelper.ShouldPanicWithMessage("Dependency resolution failed; see log file gbytes.Buffer for details. This is a bug, please report.")
			sortable = backup.TopologicalSort(sortable, depMap)
		})
		It("aborts if dependencies are not met", func() {
			depMap[backup.DepEntry{Classid: backup.PG_CLASS_OID, Objid: 1}] = []backup.DepEntry{{Classid: backup.PG_CLASS_OID, Objid: 2}}
			depMap[backup.DepEntry{Classid: backup.PG_CLASS_OID, Objid: 1}] = []backup.DepEntry{{Classid: backup.PG_CLASS_OID, Objid: 4}}
			sortable := []backup.Sortable{relation1, relation2}

			defer testhelper.ShouldPanicWithMessage("Dependency resolution failed; see log file gbytes.Buffer for details. This is a bug, please report.")
			sortable = backup.TopologicalSort(sortable, depMap)
		})
	})
	Describe("ConstructDependentObjectMetadataMap", func() {
		It("composes metadata maps for functions, types, and tables into one map", func() {
			funcMap := backup.MetadataMap{1: backup.ObjectMetadata{Comment: "function"}}
			typeMap := backup.MetadataMap{2: backup.ObjectMetadata{Comment: "type"}}
			tableMap := backup.MetadataMap{3: backup.ObjectMetadata{Comment: "relation"}}
			protoMap := backup.MetadataMap{4: backup.ObjectMetadata{Comment: "protocol"}}
			result := backup.ConstructDependentObjectMetadataMap(funcMap, typeMap, tableMap, protoMap)
			expected := backup.MetadataMap{
				1: backup.ObjectMetadata{Comment: "function"},
				2: backup.ObjectMetadata{Comment: "type"},
				3: backup.ObjectMetadata{Comment: "relation"},
				4: backup.ObjectMetadata{Comment: "protocol"},
			}
			Expect(result).To(Equal(expected))
		})
	})
})
