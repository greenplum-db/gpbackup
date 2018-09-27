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
		depMap    map[backup.UniqueID]map[backup.UniqueID]bool
	)

	BeforeEach(func() {
		relation1 = backup.Relation{Schema: "public", Name: "relation1", Oid: 1}
		relation2 = backup.Relation{Schema: "public", Name: "relation2", Oid: 2}
		relation3 = backup.Relation{Schema: "public", Name: "relation3", Oid: 3}
		depMap = make(map[backup.UniqueID]map[backup.UniqueID]bool, 0)
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
			result := backup.ConstructDependentObjectMetadataMap(funcMap, typeMap, tableMap, protoMap)
			expected := backup.MetadataMap{
				backup.UniqueID{Oid: 1}: backup.ObjectMetadata{Comment: "function"},
				backup.UniqueID{Oid: 2}: backup.ObjectMetadata{Comment: "type"},
				backup.UniqueID{Oid: 3}: backup.ObjectMetadata{Comment: "relation"},
				backup.UniqueID{Oid: 4}: backup.ObjectMetadata{Comment: "protocol"},
			}
			Expect(result).To(Equal(expected))
		})
	})
})
