package utils_test

import (
	"time"

	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("utils/util tests", func() {
	Context("CurrentTimestamp", func() {
		It("returns the current timestamp", func() {
			operating.System.Now = func() time.Time { return time.Date(2017, time.January, 1, 1, 1, 1, 1, time.Local) }
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
	Describe("ValidateFQNs", func() {
		It("validates an unquoted string", func() {
			testStrings := []string{`schemaname.tablename`}
			utils.ValidateFQNs(testStrings)
		})
		It("validates a string with a quoted schema", func() {
			testStrings := []string{`"schema,name".tablename`}
			utils.ValidateFQNs(testStrings)
		})
		It("validates a string with a quoted table", func() {
			testStrings := []string{`schemaname."table,name"`}
			utils.ValidateFQNs(testStrings)
		})
		It("validates a string with both schema and table quoted", func() {
			testStrings := []string{`"schema,name"."table,name"`}
			utils.ValidateFQNs(testStrings)
		})
		It("panics if given a string without a schema", func() {
			testStrings := []string{`tablename`}
			defer testhelper.ShouldPanicWithMessage(`tablename is not correctly fully-qualified.`)
			utils.ValidateFQNs(testStrings)
		})
		It("panics if given an invalid string", func() {
			testStrings := []string{`schema"name.table.name`}
			defer testhelper.ShouldPanicWithMessage(`schema"name.table.name is not correctly fully-qualified.`)
			utils.ValidateFQNs(testStrings)
		})
		It("panics if given a string with preceding whitespace", func() {
			testStrings := []string{`  schemaname.tablename`}
			defer testhelper.ShouldPanicWithMessage(`  schemaname.tablename is not correctly fully-qualified.`)
			utils.ValidateFQNs(testStrings)
		})
		It("panics if given a string with trailing whitespace", func() {
			testStrings := []string{`schemaname.tablename  `}
			defer testhelper.ShouldPanicWithMessage(`schemaname.tablename   is not correctly fully-qualified.`)
			utils.ValidateFQNs(testStrings)
		})
	})
	Context("ValidateFullPath", func() {
		It("does not panic when the flag is not set", func() {
			path := ""
			utils.ValidateFullPath(path)
		})
		It("does not panic when given an absolute path", func() {
			path := "/this/is/an/absolute/path"
			utils.ValidateFullPath(path)
		})
		It("panics when given a relative path", func() {
			path := "this/is/a/relative/path"
			defer testhelper.ShouldPanicWithMessage("this/is/a/relative/path is not an absolute path.")
			utils.ValidateFullPath(path)
		})
	})
	Describe("ValidateGPDBVersionCompatibility", func() {
		It("panics if GPDB version is less than 4.3.17", func() {
			testhelper.SetDBVersion(connection, "4.3.14")
			defer testhelper.ShouldPanicWithMessage("GPDB version 4.3.14 is not supported. Please upgrade to GPDB 4.3.17.0 or later.")
			utils.ValidateGPDBVersionCompatibility(connection)
		})
		It("panics if GPDB 5 version is less than 5.1.0", func() {
			testhelper.SetDBVersion(connection, "5.0.0")
			defer testhelper.ShouldPanicWithMessage("GPDB version 5.0.0 is not supported. Please upgrade to GPDB 5.1.0 or later.")
			utils.ValidateGPDBVersionCompatibility(connection)
		})
		It("does not panic if GPDB version is at least 4.3.17", func() {
			testhelper.SetDBVersion(connection, "4.3.17")
			utils.ValidateGPDBVersionCompatibility(connection)
		})
		It("does not panic if GPDB version is at least 5.1.0", func() {
			testhelper.SetDBVersion(connection, "5.1.0")
			utils.ValidateGPDBVersionCompatibility(connection)
		})
		It("does not panic if GPDB version is at least 6.0.0", func() {
			testhelper.SetDBVersion(connection, "6.0.0")
			utils.ValidateGPDBVersionCompatibility(connection)
		})
	})
})
