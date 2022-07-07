package backup

import (
	"github.com/greenplum-db/gp-common-go-libs/testhelper"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gbytes"
)

var _ = Describe("backup internal tests", func() {
	var log *Buffer
	BeforeEach(func() {
		_, _, log = testhelper.SetupTestLogger()
	})

	Describe("backupData", func() {
		It("returns successfully immediately if there is no table data to backup", func() {
			emptyTableSlice := make([]Table, 0)

			backupData(emptyTableSlice)
			Expect(string(log.Contents())).To(ContainSubstring("Data backup complete"))
		})
	})
})
