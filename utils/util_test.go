package utils_test

import (
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/utils"

	. "github.com/onsi/ginkgo/v2"
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
		It("validates the following cases correctly", func() {
			testStrings := []string{
				`schemaname.tablename`,                              // unquoted
				`"schema,name".tablename`,                           // quoted schema
				`schemaname."table,name"`,                           // quoted table
				`schema name.tablename"`,                            // spaces
				`schema name	.tablename"`,                           //tabs
				`schemaname.TABLENAME!@#$%^&*()_+={}|[]\';":/,?><"`, // special characters
			}
			utils.ValidateFQNs(testStrings)
		})
		It("fails if given a string without a schema", func() {
			testStrings := []string{`.tablename`}
			err := utils.ValidateFQNs(testStrings)
			Expect(err).To(HaveOccurred())
		})
		It("fails if given a string without a table", func() {
			testStrings := []string{`schemaname.`}
			err := utils.ValidateFQNs(testStrings)
			Expect(err).To(HaveOccurred())
		})
		It("fails if the schema and table can't be determined", func() {
			testStrings := []string{`schemanametablename`}
			err := utils.ValidateFQNs(testStrings)
			Expect(err).To(HaveOccurred())
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
	Describe("ValidateCompressionTypeAndLevel", func() {
		It("validates a compression type 'gzip' and a level between 1 and 9", func() {
			compressType := "gzip"
			compressLevel := 5
			err := utils.ValidateCompressionTypeAndLevel(compressType, compressLevel)
			Expect(err).To(Not(HaveOccurred()))
		})
		It("panics if given a compression type 'gzip' and a compression level < 1", func() {
			compressType := "gzip"
			compressLevel := 0
			err := utils.ValidateCompressionTypeAndLevel(compressType, compressLevel)
			Expect(err).To(MatchError("compression type 'gzip' only allows compression levels between 1 and 9, but the provided level is 0"))
		})
		It("panics if given a compression type 'gzip' and a compression level > 9", func() {
			compressType := "gzip"
			compressLevel := 11
			err := utils.ValidateCompressionTypeAndLevel(compressType, compressLevel)
			Expect(err).To(MatchError("compression type 'gzip' only allows compression levels between 1 and 9, but the provided level is 11"))
		})
		It("panics if given a compression type 'invalid' and a compression level > 0", func() {
			compressType := "invalid"
			compressLevel := 1
			err := utils.ValidateCompressionTypeAndLevel(compressType, compressLevel)
			Expect(err).To(MatchError("unknown compression type 'invalid'"))
		})
		It("panics if given a compression type 'invalid' and a compression level < 0", func() {
			compressType := "invalid"
			compressLevel := -1
			err := utils.ValidateCompressionTypeAndLevel(compressType, compressLevel)
			Expect(err).To(MatchError("unknown compression type 'invalid'"))
		})
		It("panics if given a compression type '' and a compression level > 0", func() {
			compressType := ""
			compressLevel := 1
			err := utils.ValidateCompressionTypeAndLevel(compressType, compressLevel)
			Expect(err).To(MatchError("unknown compression type ''"))
		})
		It("validates a compression type 'zstd' and a level between 1 and 19", func() {
			compressType := "zstd"
			compressLevel := 11
			err := utils.ValidateCompressionTypeAndLevel(compressType, compressLevel)
			Expect(err).To(Not(HaveOccurred()))
		})
		It("panics if given a compression type 'zstd' and a compression level < 1", func() {
			compressType := "zstd"
			compressLevel := 0
			err := utils.ValidateCompressionTypeAndLevel(compressType, compressLevel)
			Expect(err).To(MatchError("compression type 'zstd' only allows compression levels between 1 and 19, but the provided level is 0"))
		})
		It("panics if given a compression type 'gzip' and a compression level > 19", func() {
			compressType := "zstd"
			compressLevel := 20
			err := utils.ValidateCompressionTypeAndLevel(compressType, compressLevel)
			Expect(err).To(MatchError("compression type 'zstd' only allows compression levels between 1 and 19, but the provided level is 20"))
		})
	})
	Describe("UnquoteIdent", func() {
		It("returns unchanged ident when passed a single char", func() {
			dbname := `a`
			resultString := utils.UnquoteIdent(dbname)

			Expect(resultString).To(Equal(`a`))
		})
		It("returns unchanged ident when passed an unquoted ident", func() {
			dbname := `test`
			resultString := utils.UnquoteIdent(dbname)

			Expect(resultString).To(Equal(`test`))
		})
		It("returns one double quote when passed a double quote", func() {
			dbname := `"`
			resultString := utils.UnquoteIdent(dbname)

			Expect(resultString).To(Equal(`"`))
		})
		It("returns empty string when passed an empty string", func() {
			dbname := ""
			resultString := utils.UnquoteIdent(dbname)

			Expect(resultString).To(Equal(``))
		})
		It("properly unquotes an identfier string and unescapes double quotes", func() {
			dbname := `"""test"`
			resultString := utils.UnquoteIdent(dbname)

			Expect(resultString).To(Equal(`"test`))
		})
	})
	Describe("SliceToQuotedString", func() {
		It("quotes and joins a slice of strings into a single string", func() {
			inputStrings := []string{"string1", "string2", "string3"}
			expectedString := "'string1','string2','string3'"
			resultString := utils.SliceToQuotedString(inputStrings)
			Expect(resultString).To(Equal(expectedString))
		})
		It("returns an empty string when given an empty slice", func() {
			inputStrings := make([]string, 0)
			resultString := utils.SliceToQuotedString(inputStrings)
			Expect(resultString).To(Equal(""))
		})
	})
})
