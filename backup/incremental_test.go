package backup_test

import (
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/utils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup/incremental tests", func() {
	BeforeEach(func() {

	})
	Describe("FilterTablesForIncremental", func() {
		defaultEntry := utils.AOEntry{
			Modcount:         0,
			LastDDLTimestamp: "00000",
		}
		prevTOC := utils.TOC{
			IncrementalMetadata: utils.IncrementalEntries{
				AO: map[string]utils.AOEntry{
					"public.ao_changed_modcount":  defaultEntry,
					"public.ao_changed_timestamp": defaultEntry,
					"public.ao_unchanged":         defaultEntry,
				},
			},
		}

		currTOC := utils.TOC{
			IncrementalMetadata: utils.IncrementalEntries{
				AO: map[string]utils.AOEntry{
					"public.ao_changed_modcount": {
						Modcount:         2,
						LastDDLTimestamp: "00000",
					},
					"public.ao_changed_timestamp": {
						Modcount:         0,
						LastDDLTimestamp: "00001",
					},
					"public.ao_unchanged": defaultEntry,
				},
			},
		}

		tblHeap := backup.Relation{Schema: "public", Name: "heap"}
		tblAOChangedModcount := backup.Relation{Schema: "public", Name: "ao_changed_modcount"}
		tblAOChangedTS := backup.Relation{Schema: "public", Name: "ao_changed_timestamp"}
		tblAOUnchanged := backup.Relation{Schema: "public", Name: "ao_unchanged"}
		tables := []backup.Relation{
			tblHeap,
			tblAOChangedModcount,
			tblAOChangedTS,
			tblAOUnchanged,
		}

		filteredTables := backup.FilterTablesForIncremental(&prevTOC, &currTOC, tables)

		It("Should include the heap table in the filtered list", func() {
			Expect(filteredTables).To(ContainElement(tblHeap))
		})

		It("Should include the AO table having a modified modcount", func() {
			Expect(filteredTables).To(ContainElement(tblAOChangedModcount))
		})

		It("Should include the AO table having a modified last DDL timestamp", func() {
			Expect(filteredTables).To(ContainElement(tblAOChangedTS))
		})

		It("Should NOT include the unmodified AO table", func() {
			Expect(filteredTables).To(Not(ContainElement(tblAOUnchanged)))
		})
	})
})
