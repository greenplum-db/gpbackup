package utils_test

import (
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("utils/util tests", func() {
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
		It("does not return error when the flag is not set", func() {
			path := ""
			Expect(utils.ValidateFullPath(path)).To(Succeed())
		})
		It("does not return error when given an absolute path", func() {
			path := "/this/is/an/absolute/path"
			Expect(utils.ValidateFullPath(path)).To(Succeed())
		})
		It("panics when given a relative path", func() {
			path := "this/is/a/relative/path"
			err := utils.ValidateFullPath(path)
			Expect(err).To(MatchError("this/is/a/relative/path is not an absolute path."))

		})
	})
	Describe("ValidateGPDBVersionCompatibility", func() {
		It("panics if GPDB version is less than 4.3.17", func() {
			testhelper.SetDBVersion(connectionPool, "4.3.14")
			defer testhelper.ShouldPanicWithMessage("GPDB version 4.3.14 is not supported. Please upgrade to GPDB 4.3.17.0 or later.")
			utils.ValidateGPDBVersionCompatibility(connectionPool)
		})
		It("panics if GPDB 5 version is less than 5.1.0", func() {
			testhelper.SetDBVersion(connectionPool, "5.0.0")
			defer testhelper.ShouldPanicWithMessage("GPDB version 5.0.0 is not supported. Please upgrade to GPDB 5.1.0 or later.")
			utils.ValidateGPDBVersionCompatibility(connectionPool)
		})
		It("does not panic if GPDB version is at least 4.3.17", func() {
			testhelper.SetDBVersion(connectionPool, "4.3.17")
			utils.ValidateGPDBVersionCompatibility(connectionPool)
		})
		It("does not panic if GPDB version is at least 5.1.0", func() {
			testhelper.SetDBVersion(connectionPool, "5.1.0")
			utils.ValidateGPDBVersionCompatibility(connectionPool)
		})
		It("does not panic if GPDB version is at least 6.0.0", func() {
			testhelper.SetDBVersion(connectionPool, "6.0.0")
			utils.ValidateGPDBVersionCompatibility(connectionPool)
		})
	})
	Describe("ValidateCompressionLevel", func() {
		It("validates a compression level between 1 and 9", func() {
			compressLevel := 5
			err := utils.ValidateCompressionLevel(compressLevel)
			Expect(err).To(Not(HaveOccurred()))
		})
		It("panics if given a compression level < 1", func() {
			compressLevel := 0
			err := utils.ValidateCompressionLevel(compressLevel)
			Expect(err).To(MatchError("Compression level must be between 1 and 9"))
		})
		It("panics if given a compression level > 9", func() {
			compressLevel := 11
			err := utils.ValidateCompressionLevel(compressLevel)
			Expect(err).To(MatchError("Compression level must be between 1 and 9"))
		})
	})

})
