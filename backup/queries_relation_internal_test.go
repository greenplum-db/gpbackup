package backup

import (
	"fmt"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup internal tests", func() {
	Describe("generateLockQueries", func() {
		It("batches tables together and generates lock queries", func() {
			tables := make([]Relation, 0)
			for i := 0; i < 200; i++ {
				tables = append(tables, Relation{0, 0, "public", fmt.Sprintf("foo%d", i)})
			}

			batchSize := 100
			lockQueries := generateTableBatches(tables, batchSize)
			Expect(len(lockQueries)).To(Equal(2))
		})
		It("batches up remaining leftover tables together in a single lock query", func() {
			tables := make([]Relation, 0)
			for i := 0; i < 101; i++ {
				tables = append(tables, Relation{0, 0, "public", fmt.Sprintf("foo%d", i)})
			}

			batchSize := 50
			lockQueries := generateTableBatches(tables, batchSize)
			Expect(len(lockQueries)).To(Equal(3))
		})
	})
})
