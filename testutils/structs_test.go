package testutils

import (
	"github.com/greenplum-db/gpbackup/utils"

	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestTestUtils(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "testutils tests")
}

var _ = Describe("StructMatchers", func() {
	Describe("StructMatcher", func() {
		It("returns no failures for the same structs", func() {
			schema1 := utils.Schema{0, "test_schema", "", "testrole"}
			schema2 := utils.Schema{0, "test_schema", "", "testrole"}
			mismatches := StructMatcher(&schema1, &schema2)
			Expect(mismatches).To(BeEmpty())
		})
		It("returns mismatches with different structs", func() {
			schema1 := utils.Schema{0, "test_schema", "", "testrole"}
			schema2 := utils.Schema{0, "", "", "testrole"}
			mismatches := StructMatcher(&schema1, &schema2)
			Expect(mismatches).ToNot(BeEmpty())
		})
		It("formats a nice error message for mismatches", func() {
			schema1 := utils.Schema{0, "test_schema", "", "testrole"}
			schema2 := utils.Schema{0, "another_schema", "", "testrole"}
			mismatches := StructMatcher(&schema1, &schema2)
			Expect(mismatches).To(Equal([]string{"Mismatch on field SchemaName \nExpected\n    <string>: test_schema\nto equal\n    <string>: another_schema"}))
		})
	})
	Describe("StructMatcherExcluding", func() {
		It("returns no failures for the same structs", func() {
			schema1 := utils.Schema{0, "test_schema", "", "testrole"}
			schema2 := utils.Schema{0, "test_schema", "", "testrole"}
			mismatches := StructMatcherExcluding(&schema1, &schema2, []string{})
			Expect(mismatches).To(BeEmpty())
		})
		It("returns no mismatches when skipped value is different", func() {
			schema1 := utils.Schema{0, "test_schema", "", "testrole"}
			schema2 := utils.Schema{1, "test_schema", "", "testrole"}
			mismatches := StructMatcherExcluding(&schema1, &schema2, []string{"SchemaOid"})
			Expect(mismatches).To(BeEmpty())
		})
		It("returns mismatches with different structs", func() {
			schema1 := utils.Schema{0, "test_schema", "", "testrole"}
			schema2 := utils.Schema{0, "", "", "testrole"}
			mismatches := StructMatcherExcluding(&schema1, &schema2, []string{"SchemaOid"})
			Expect(mismatches).ToNot(BeEmpty())
		})
		It("formats a nice error message for mismatches", func() {
			schema1 := utils.Schema{0, "test_schema", "", "testrole"}
			schema2 := utils.Schema{0, "another_schema", "", "testrole"}
			mismatches := StructMatcherExcluding(&schema1, &schema2, []string{"SchemaOid"})
			Expect(mismatches).To(Equal([]string{"Mismatch on field SchemaName \nExpected\n    <string>: test_schema\nto equal\n    <string>: another_schema"}))
		})
	})
	Describe("StructMatcherIncluding", func() {
		It("returns no failures for the same structs", func() {
			schema1 := utils.Schema{0, "test_schema", "", "testrole"}
			schema2 := utils.Schema{0, "test_schema", "", "testrole"}
			mismatches := StructMatcherIncluding(&schema1, &schema2, []string{"SchemaOid", "SchemaName", "Comment", "Owner"})
			Expect(mismatches).To(BeEmpty())
		})
		It("returns no mismatches when included fields are the same", func() {
			schema1 := utils.Schema{0, "test_schema", "", "testrole"}
			schema2 := utils.Schema{1, "test_schema", "", "testrole"}
			mismatches := StructMatcherIncluding(&schema1, &schema2, []string{"SchemaName", "Comment", "Owner"})
			Expect(mismatches).To(BeEmpty())
		})
		It("returns mismatches  when included fields are different", func() {
			schema1 := utils.Schema{0, "test_schema", "", "testrole"}
			schema2 := utils.Schema{0, "another_schema", "", "testrole"}
			mismatches := StructMatcherIncluding(&schema1, &schema2, []string{"SchemaOid", "SchemaName", "Comment"})
			Expect(mismatches).ToNot(BeEmpty())
		})
		It("formats a nice error message for mismatches", func() {
			schema1 := utils.Schema{0, "test_schema", "", "testrole"}
			schema2 := utils.Schema{0, "another_schema", "", "testrole"}
			mismatches := StructMatcherIncluding(&schema1, &schema2, []string{"SchemaOid", "SchemaName", "Comment"})
			Expect(mismatches).To(Equal([]string{"Mismatch on field SchemaName \nExpected\n    <string>: test_schema\nto equal\n    <string>: another_schema"}))
		})

	})
})
