package restore_test

import (
	"github.com/greenplum-db/gpbackup/restore"
	"github.com/greenplum-db/gpbackup/toc"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("restore/parallel tests", func() {
	Describe("BatchPostdataStatements", func() {
		index1 := toc.StatementWithType{ObjectType: "INDEX", ReferenceObject: "public.table1", Statement: `CREATE INDEX testindex ON public.testtable USING btree(i);`}
		index2 := toc.StatementWithType{ObjectType: "INDEX", ReferenceObject: "public.table2", Statement: `CREATE INDEX testindex ON public.testtable USING btree(i);`}
		index3 := toc.StatementWithType{ObjectType: "INDEX", ReferenceObject: "public.table3", Statement: `CREATE INDEX testindex ON public.testtable USING btree(i);`}
		trigger := toc.StatementWithType{ObjectType: "TRIGGER", ReferenceObject: "public.table3", Statement: `CREATE INDEX testindex ON public.testtable USING btree(i);`}
		It("places all indexes in first batch when all are on different tables", func() {
			statements := []toc.StatementWithType{index1, index2, index3}
			firstBatch, secondBatch := restore.BatchPostdataStatements(statements)
			Expect(firstBatch).To(Equal([]toc.StatementWithType{index1, index2, index3}))
			Expect(secondBatch).To(Equal([]toc.StatementWithType{}))
		})
		It("places first index for a table in first batch, and other indexes for that table in second", func() {
			statements := []toc.StatementWithType{index1, index2, index2, index2, index3, index3}
			firstBatch, secondBatch := restore.BatchPostdataStatements(statements)
			Expect(firstBatch).To(Equal([]toc.StatementWithType{index1, index2, index3}))
			Expect(secondBatch).To(Equal([]toc.StatementWithType{index2, index2, index3}))
		})
		It("places non-index objects in second batch", func() {
			statements := []toc.StatementWithType{index1, index1, trigger}
			firstBatch, secondBatch := restore.BatchPostdataStatements(statements)
			Expect(firstBatch).To(Equal([]toc.StatementWithType{index1}))
			Expect(secondBatch).To(Equal([]toc.StatementWithType{index1, trigger}))
		})

	})
})
