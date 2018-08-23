package restore_test

import (
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/restore"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("wrapper tests", func() {
	Describe("SetMaxCsvLineLengthQuery", func() {
		It("returns nothing with a connection version of at least 6.0.0", func() {
			testhelper.SetDBVersion(connection, "6.0.0")
			result := restore.SetMaxCsvLineLengthQuery(connection)
			Expect(result).To(Equal(""))
		})
		It("sets gp_max_csv_line_length to 1GB when connection version is 4.X and at least 4.3.30.0", func() {
			testhelper.SetDBVersion(connection, "4.3.30")
			result := restore.SetMaxCsvLineLengthQuery(connection)
			Expect(result).To(Equal("SET gp_max_csv_line_length = 1073741824;\n"))
		})
		It("sets gp_max_csv_line_length to 1GB when connection version is 5.X and at least 5.11.0", func() {
			testhelper.SetDBVersion(connection, "5.11.0")
			result := restore.SetMaxCsvLineLengthQuery(connection)
			Expect(result).To(Equal("SET gp_max_csv_line_length = 1073741824;\n"))
		})
		It("sets gp_max_csv_line_length to 4MB when connection version is 4.X and before 4.3.30.0", func() {
			testhelper.SetDBVersion(connection, "4.3.29")
			result := restore.SetMaxCsvLineLengthQuery(connection)
			Expect(result).To(Equal("SET gp_max_csv_line_length = 4194304;\n"))
		})
		It("sets gp_max_csv_line_length to 4MB when connection version is 5.X and before 5.11.0", func() {
			testhelper.SetDBVersion(connection, "5.10.999")
			result := restore.SetMaxCsvLineLengthQuery(connection)
			Expect(result).To(Equal("SET gp_max_csv_line_length = 4194304;\n"))
		})
	})
})
