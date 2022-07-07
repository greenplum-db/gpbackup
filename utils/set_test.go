package utils_test

import (
	"github.com/greenplum-db/gpbackup/utils"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("utils/set tests", func() {
	Describe("NewSet", func() {
		It("can create a set from an empty list", func() {
			emptySet := utils.NewSet([]string{})

			expectedMap := map[string]bool{}
			Expect(emptySet.Set).To(Equal(expectedMap))
			Expect(emptySet.IsExclude).To(BeFalse())
			Expect(emptySet.AlwaysMatchesFilter).To(BeFalse())
		})
		It("can create a set from a list of strings", func() {
			setWithEntries := utils.NewSet([]string{"foo", "bar"})

			expectedMap := map[string]bool{"foo": true, "bar": true}
			Expect(setWithEntries.Set).To(Equal(expectedMap))
			Expect(setWithEntries.IsExclude).To(BeFalse())
			Expect(setWithEntries.AlwaysMatchesFilter).To(BeFalse())
		})
	})
	Describe("NewIncludeSet", func() {
		It("can create a set from an empty list", func() {
			emptyInclude := utils.NewIncludeSet([]string{})

			expectedMap := map[string]bool{}
			Expect(emptyInclude.Set).To(Equal(expectedMap))
			Expect(emptyInclude.IsExclude).To(BeFalse())
			Expect(emptyInclude.AlwaysMatchesFilter).To(BeTrue())
		})
		It("can create a set from a list of strings", func() {
			includeWithEntries := utils.NewIncludeSet([]string{"foo", "bar"})

			expectedMap := map[string]bool{"foo": true, "bar": true}
			Expect(includeWithEntries.Set).To(Equal(expectedMap))
			Expect(includeWithEntries.IsExclude).To(BeFalse())
			Expect(includeWithEntries.AlwaysMatchesFilter).To(BeFalse())
		})
	})
	Describe("NewExcludeSet", func() {
		It("can create a set from an empty list", func() {
			emptyExclude := utils.NewExcludeSet([]string{})

			expectedMap := map[string]bool{}
			Expect(emptyExclude.Set).To(Equal(expectedMap))
			Expect(emptyExclude.IsExclude).To(BeTrue())
			Expect(emptyExclude.AlwaysMatchesFilter).To(BeTrue())
		})
		It("can create a set from a list of strings", func() {
			excludeWithEntries := utils.NewExcludeSet([]string{"foo", "bar"})

			expectedMap := map[string]bool{"foo": true, "bar": true}
			Expect(excludeWithEntries.Set).To(Equal(expectedMap))
			Expect(excludeWithEntries.IsExclude).To(BeTrue())
			Expect(excludeWithEntries.AlwaysMatchesFilter).To(BeFalse())
		})
	})
	Describe("MatchesFilter", func() {
		Context("Include Set", func() {
			It("returns true if an item is in a non-empty include set", func() {
				includeWithEntries := utils.NewIncludeSet([]string{"foo", "bar"})
				Expect(includeWithEntries.MatchesFilter("foo")).To(BeTrue())
			})
			It("returns false if an item is not in a non-empty include set", func() {
				includeWithEntries := utils.NewIncludeSet([]string{"foo", "bar"})
				Expect(includeWithEntries.MatchesFilter("nonexistent")).To(BeFalse())
			})
			It("returns true if an include set is empty", func() {
				emptyInclude := utils.NewIncludeSet([]string{})
				Expect(emptyInclude.MatchesFilter("foo")).To(BeTrue())
			})
		})
		Context("Exclude Set", func() {
			It("returns true if an item is not in a non-empty exclude set", func() {
				excludeWithEntries := utils.NewExcludeSet([]string{"foo", "bar"})
				Expect(excludeWithEntries.MatchesFilter("nonexistent")).To(BeTrue())
			})
			It("returns false if an item is in a non-empty exclude set", func() {
				excludeWithEntries := utils.NewExcludeSet([]string{"foo", "bar"})
				Expect(excludeWithEntries.MatchesFilter("foo")).To(BeFalse())
			})
			It("returns true if an exclude set is empty", func() {
				emptyExclude := utils.NewExcludeSet([]string{})
				Expect(emptyExclude.MatchesFilter("foo")).To(BeTrue())
			})
		})
	})
	Describe("Length", func() {
		It("returns the length of the underlying map", func() {
			setWithEntries := utils.NewSet([]string{"foo", "bar"})

			Expect(setWithEntries.Length()).To(Equal(2))
		})
	})
	Describe("Equals", func() {
		set1 := utils.NewIncludeSet([]string{"foo", "bar"})
		set1Copy := utils.NewIncludeSet([]string{"foo", "bar"})
		set2 := utils.NewIncludeSet([]string{"foo"})
		set3 := utils.NewIncludeSet([]string{"foo", "baz"})

		Context("sets have different elements", func() {
			It("returns false if sets have different number of elements", func() {
				Expect(set1.Equals(set2)).To(BeFalse())
			})
			It("returns false if sets have different elements", func() {
				Expect(set1.Equals(set3)).To(BeFalse())
			})
		})
		It("returns true if sets have the same elements", func() {
			Expect(set1.Equals(set1Copy)).To(BeTrue())
		})
		It("returns true if both sets are empty", func() {
			Expect(utils.NewIncludeSet([]string{}).Equals(utils.NewIncludeSet([]string{}))).To(BeTrue())
		})
	})
})
