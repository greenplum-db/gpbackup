package utils_test

import (
	"flag"

	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("utils/flag tests", func() {
	Context("IsValidTimestamp", func() {
		It("allows a valid timestamp", func() {
			timestamp := "20170101010101"
			isValid := utils.IsValidTimestamp(timestamp)
			Expect(isValid).To(BeTrue())
		})
		It("invalidates a non-numeric timestamp", func() {
			timestamp := "2017ababababab"
			isValid := utils.IsValidTimestamp(timestamp)
			Expect(isValid).To(BeFalse())
		})
		It("invalidates a timestamp that is too short", func() {
			timestamp := "201701010101"
			isValid := utils.IsValidTimestamp(timestamp)
			Expect(isValid).To(BeFalse())
		})
		It("invalidates a timestamp that is too long", func() {
			timestamp := "2017010101010101"
			isValid := utils.IsValidTimestamp(timestamp)
			Expect(isValid).To(BeFalse())
		})
	})
	Context("ValidateBackupDir", func() {
		It("does not panic when the flag is not set", func() {
			path := ""
			utils.ValidateBackupDir(path)
		})
		It("does not panic when given an absolute path", func() {
			path := "/this/is/an/absolute/path"
			utils.ValidateBackupDir(path)
		})
		It("panics when given a relative path", func() {
			path := "this/is/a/relative/path"
			defer testhelper.ShouldPanicWithMessage("Absolute path required for backupdir")
			utils.ValidateBackupDir(path)
		})
	})
	Context("Flag parsing functions ", func() {
		BeforeEach(func() {
			flag.CommandLine = flag.NewFlagSet("", flag.ContinueOnError)
			_ = flag.String("stringFlag", "", "This is a sample string flag.")
			_ = flag.Bool("boolFlag", false, "This is a sample bool flag.")
			_ = flag.Int("intFlag", 0, "This is a sample int flag.")
		})
		Context("CheckMandatoryFlags", func() {
			It("does not panic if a mandatory flag is set", func() {
				flag.CommandLine.Parse([]string{"-stringFlag", "foo"})
				utils.CheckMandatoryFlags("stringFlag")
			})
			It("panics if a mandatory flag is not set", func() {
				flag.CommandLine.Parse([]string{})
				defer testhelper.ShouldPanicWithMessage("Flag stringFlag must be set")
				utils.CheckMandatoryFlags("stringFlag")
			})
		})
		Context("CheckExclusiveFlags", func() {
			It("does not panic if no flags in the argument list are set", func() {
				flag.CommandLine.Parse([]string{})
				utils.CheckExclusiveFlags("stringFlag", "boolFlag")
			})
			It("does not panic if one flags in the argument list is set", func() {
				flag.CommandLine.Parse([]string{"-stringFlag", "foo"})
				utils.CheckExclusiveFlags("stringFlag", "boolFlag")
			})
			It("does not panic if one flags in the argument list is set with flags not in the set", func() {
				flag.CommandLine.Parse([]string{"-stringFlag", "foo", "-intFlag", "42"})
				utils.CheckExclusiveFlags("stringFlag", "boolFlag")
			})
			It("panics if two or more flags in the argument list are set", func() {
				flag.CommandLine.Parse([]string{"-stringFlag", "foo", "-boolFlag"})
				defer testhelper.ShouldPanicWithMessage("The following flags may not be specified together: stringFlag, boolFlag")
				utils.CheckExclusiveFlags("stringFlag", "boolFlag")
			})
		})
	})
})
