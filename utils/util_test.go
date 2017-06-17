package utils_test

import (
	"time"

	"github.com/greenplum-db/gpbackup/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("utils/util tests", func() {
	Context("CurrentTimestamp", func() {
		It("returns the current timestamp", func() {
			utils.System.Now = func() time.Time { return time.Date(2017, time.January, 1, 1, 1, 1, 1, time.Local) }
			expected := "20170101010101"
			actual := utils.CurrentTimestamp()
			Expect(actual).To(Equal(expected))
		})
	})
	Context("DollarQuoteString", func() {
		It("uses $$ if the string contains no dollar signs", func() {
			testStr := "message"
			expected := "$$message$$"
			actual := utils.DollarQuoteString(testStr)
			Expect(actual).To(Equal(expected))
		})
		It("uses $_$ if the string contains $", func() {
			testStr := "message$text"
			expected := "$_$message$text$_$"
			actual := utils.DollarQuoteString(testStr)
			Expect(actual).To(Equal(expected))
		})
		It("uses $_X$ if the string contains $_", func() {
			testStr := "message$_text"
			expected := "$_X$message$_text$_X$"
			actual := utils.DollarQuoteString(testStr)
			Expect(actual).To(Equal(expected))
		})
		It("uses $_$ if the string contains non-adjacent $ and _", func() {
			testStr := "message$text_"
			expected := "$_$message$text_$_$"
			actual := utils.DollarQuoteString(testStr)
			Expect(actual).To(Equal(expected))
		})
	})
})
