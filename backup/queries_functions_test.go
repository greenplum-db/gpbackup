package backup_test

import (
	"github.com/greenplum-db/gpbackup/backup"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup/queries_acl tests", func() {
	Describe("PostProcessFunctionConfigs", func() {
		It("returns correct value for search_path", func() {
			allFunctions := []backup.Function{
				{Config: "SET SEARCH_PATH TO bar"},
			}
			err := backup.PostProcessFunctionConfigs(allFunctions)
			Expect(err).ToNot(HaveOccurred())
			Expect(allFunctions[0].Config).To(Equal(`SET search_path TO 'bar'`))
		})
		It("returns error when function config does not parse", func() {
			allFunctions := []backup.Function{
				{Config: "SET foo blah blah blah"},
			}
			err := backup.PostProcessFunctionConfigs(allFunctions)
			Expect(err).To(HaveOccurred())
		})
		// known bug https://www.pivotaltracker.com/story/show/164575992
		PIt("returns correct value for multiple GUCs in one function", func() {
			allFunctions := []backup.Function{
				// not clear how the native pg_proc.proconfig field will translate into our Config attribute: assuming we get 2 separate strings
				{Config: `SET search_path TO bar, blah
SET BAZ TO abc`},
			}
			err := backup.PostProcessFunctionConfigs(allFunctions)
			Expect(err).ToNot(HaveOccurred())
			// expecting separate lines stored in the Config attribute;
			// this may not be the perfect solution, TBD: may want to have it become a slice of strings
			Expect(allFunctions[0].Config).To(Equal(`SET search_path TO 'bar', blah
SET baz to abc`))
		})
	})
	Describe("QuoteGUCValue", func() {
		It("returns correct value for a name/value pair", func() {
			result := backup.QuoteGUCValue("foo", `bar`)
			Expect(result).To(Equal(`'bar'`))
		})
		It("returns correct value for SEARCH_PATH", func() {
			result := backup.QuoteGUCValue("search_path", `"$user",public`)
			Expect(result).To(Equal(`'$user', 'public'`))
		})
		It("returns correct value for temp_tablespaces", func() {
			result := backup.QuoteGUCValue("temp_tablespaces", `"tables""pace1%",     tablespace2`)
			Expect(result).To(Equal(`'tables"pace1%', 'tablespace2'`))
		})
	})
	Describe("UnescapeDoubleQuote", func() {
		It("removes outside quotes", func() {
			result := backup.UnescapeDoubleQuote(`"foo"`)
			Expect(result).To(Equal(`foo`))
		})
		It("does nothing if string has no quotes surrounding it", func() {
			result := backup.UnescapeDoubleQuote(`foo`)
			Expect(result).To(Equal(`foo`))
		})
		It("removes outside quotes and unescapes embedded quote", func() {
			result := backup.UnescapeDoubleQuote(`"foo"""`)
			Expect(result).To(Equal(`foo"`))
		})
		It("removes outside quotes and unescapes multiple embedded quotes", func() {
			result := backup.UnescapeDoubleQuote(`"""foo"""`)
			Expect(result).To(Equal(`"foo"`))
		})
	})
})
