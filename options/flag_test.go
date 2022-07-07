package options_test

import (
	"flag"

	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/options"
	"github.com/spf13/pflag"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("utils/flag tests", func() {
	Context("Flag parsing functions ", func() {
		var flagSet *pflag.FlagSet
		BeforeEach(func() {
			flagSet = pflag.NewFlagSet("testFlags", pflag.ContinueOnError)
			_ = flagSet.String("stringFlag", "", "This is a sample string flag.")
			_ = flagSet.Bool("boolFlag", false, "This is a sample bool flag.")
			_ = flagSet.Int("intFlag", 0, "This is a sample int flag.")
		})
		Context("CheckExclusiveFlags", func() {
			It("does not panic if no flags in the argument list are set", func() {
				Expect(flag.CommandLine.Parse([]string{})).To(Succeed())
				options.CheckExclusiveFlags(flagSet, "boolFlag")
			})
			It("does not panic if one flags in the argument list is set", func() {
				Expect(flagSet.Parse([]string{"--stringFlag", "foo"})).To(Succeed())
				options.CheckExclusiveFlags(flagSet, "boolFlag")
			})
			It("does not panic if one flags in the argument list is set with flags not in the set", func() {
				Expect(flagSet.Parse([]string{"--stringFlag", "foo", "--intFlag", "42"})).To(Succeed())
				options.CheckExclusiveFlags(flagSet, "boolFlag")
			})
			It("panics if two or more flags in the argument list are set", func() {
				Expect(flagSet.Parse([]string{"--stringFlag", "foo", "--boolFlag"})).To(Succeed())
				defer testhelper.ShouldPanicWithMessage("The following flags may not be specified together: stringFlag, boolFlag")
				options.CheckExclusiveFlags(flagSet, "stringFlag", "boolFlag")
			})
		})
		Context("HandleSingleDashes", func() {
			It("replaces single dash at beginning of command", func() {
				result := options.HandleSingleDashes([]string{"-some_flag", "some_argument"})
				Expect(result).To(Equal([]string{"--some_flag", "some_argument"}))
			})
			It("does not replace single dashes inside command", func() {
				result := options.HandleSingleDashes([]string{"-some-flag", "some_argument"})
				Expect(result).To(Equal([]string{"--some-flag", "some_argument"}))
			})
			It("does not replace double dashes", func() {
				result := options.HandleSingleDashes([]string{"--some_flag", "some_argument"})
				Expect(result).To(Equal([]string{"--some_flag", "some_argument"}))
			})
			It("does not replace dash for single character flag", func() {
				result := options.HandleSingleDashes([]string{"-s", "some_argument"})
				Expect(result).To(Equal([]string{"-s", "some_argument"}))
			})
		})
	})
})
