package utils_test

import (
	"fmt"

	"github.com/greenplum-db/gpbackup/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("utils/table tests", func() {
	Describe("QuoteIdent", func() {
		It("does not quote a string that contains no special characters", func() {
			name := `_tablename1`
			output := utils.QuoteIdent(name)
			Expect(output).To(Equal(`_tablename1`))
		})
		It("replaces double quotes with pairs of double quotes", func() {
			name := `table"name`
			output := utils.QuoteIdent(name)
			Expect(output).To(Equal(`"table""name"`))
		})
		It("properly escapes capital letters", func() {
			names := []string{"Relationname", "TABLENAME", "TaBlEnAmE"}
			expected := []string{`"Relationname"`, `"TABLENAME"`, `"TaBlEnAmE"`}
			for i := range names {
				output := utils.QuoteIdent(names[i])
				Expect(output).To(Equal(expected[i]))
			}
		})
		It("properly escapes shell-significant special characters", func() {
			special := `.,!$/\` + "`"
			for _, spec := range special {
				name := fmt.Sprintf(`table%cname`, spec)
				expected := fmt.Sprintf(`"table%cname"`, spec)
				output := utils.QuoteIdent(name)
				Expect(output).To(Equal(expected))
			}
		})
		It("properly escapes whitespace", func() {
			names := []string{"table name", "table\tname", "table\nname"}
			expected := []string{`"table name"`, `"table\tname"`, `"table\nname"`}
			for i := range names {
				output := utils.QuoteIdent(names[i])
				Expect(output).To(Equal(expected[i]))
			}
		})
		It("properly escapes all other punctuation", func() {
			special := `'~@#$%^&*()-+[]{}><|;:?`
			for _, spec := range special {
				name := fmt.Sprintf(`table%cname`, spec)
				expected := fmt.Sprintf(`"table%cname"`, spec)
				output := utils.QuoteIdent(name)
				Expect(output).To(Equal(expected))
			}
		})
		It("properly escapes Unicode characters", func() {
			special := `Ää表`
			for _, spec := range special {
				name := fmt.Sprintf(`table%cname`, spec)
				expected := fmt.Sprintf(`"table%cname"`, spec)
				output := utils.QuoteIdent(name)
				Expect(output).To(Equal(expected))
			}
		})
	})
	Describe("Schema.ToString", func() {
		It("remains unquoted if it contains no special characters", func() {
			testSchema := utils.BasicSchema(`schemaname`)
			expected := `schemaname`
			Expect(testSchema.ToString()).To(Equal(expected))
		})
		It("is quoted if it contains special characters", func() {
			testSchema := utils.BasicSchema(`schema,name`)
			expected := `"schema,name"`
			Expect(testSchema.ToString()).To(Equal(expected))
		})
	})
	Describe("SchemaFromString", func() {
		It("can parse an unquoted string", func() {
			testString := `schemaname`
			newSchema := utils.SchemaFromString(testString)
			Expect(newSchema.SchemaOid).To(Equal(uint32(0)))
			Expect(newSchema.SchemaName).To(Equal(`schemaname`))
		})
		It("can parse a quoted string", func() {
			testString := `"schema,name"`
			newSchema := utils.SchemaFromString(testString)
			Expect(newSchema.SchemaOid).To(Equal(uint32(0)))
			Expect(newSchema.SchemaName).To(Equal(`schema,name`))
		})
	})
	Describe("Relation.ToString", func() {
		It("remains unquoted if neither the schema nor the table name contains special characters", func() {
			testTable := utils.BasicRelation(`schemaname`, `tablename`)
			expected := `schemaname.tablename`
			Expect(testTable.ToString()).To(Equal(expected))
		})
		It("is quoted if the schema name contains special characters", func() {
			testTable := utils.BasicRelation(`schema,name`, `tablename`)
			expected := `"schema,name".tablename`
			Expect(testTable.ToString()).To(Equal(expected))
		})
		It("is quoted if the table name contains special characters", func() {
			testTable := utils.BasicRelation(`schemaname`, `table,name`)
			expected := `schemaname."table,name"`
			Expect(testTable.ToString()).To(Equal(expected))
		})
		It("is quoted if both the schema and the table name contain special characters", func() {
			testTable := utils.BasicRelation(`schema,name`, `table,name`)
			expected := `"schema,name"."table,name"`
			Expect(testTable.ToString()).To(Equal(expected))
		})
	})
	Describe("RelationFromString", func() {
		It("can parse an unquoted string", func() {
			testString := `schemaname.tablename`
			newTable := utils.RelationFromString(testString)
			Expect(newTable.SchemaOid).To(Equal(uint32(0)))
			Expect(newTable.RelationOid).To(Equal(uint32(0)))
			Expect(newTable.SchemaName).To(Equal(`schemaname`))
			Expect(newTable.RelationName).To(Equal(`tablename`))
		})
		It("can parse a string with a quoted schema", func() {
			testString := `"schema,name".tablename`
			newTable := utils.RelationFromString(testString)
			Expect(newTable.SchemaOid).To(Equal(uint32(0)))
			Expect(newTable.RelationOid).To(Equal(uint32(0)))
			Expect(newTable.SchemaName).To(Equal(`schema,name`))
			Expect(newTable.RelationName).To(Equal(`tablename`))
		})
		It("can parse a string with a quoted table", func() {
			testString := `schemaname."table,name"`
			newTable := utils.RelationFromString(testString)
			Expect(newTable.SchemaOid).To(Equal(uint32(0)))
			Expect(newTable.RelationOid).To(Equal(uint32(0)))
			Expect(newTable.SchemaName).To(Equal(`schemaname`))
			Expect(newTable.RelationName).To(Equal(`table,name`))
		})
		It("can parse a string with both schema and table quoted", func() {
			testString := `"schema,name"."table,name"`
			newTable := utils.RelationFromString(testString)
			Expect(newTable.SchemaOid).To(Equal(uint32(0)))
			Expect(newTable.RelationOid).To(Equal(uint32(0)))
			Expect(newTable.SchemaName).To(Equal(`schema,name`))
			Expect(newTable.RelationName).To(Equal(`table,name`))
		})
	})
	Describe("GetUniqueSchemas", func() {
		alphabeticalAFoo := utils.Relation{1, 0, "otherschema", "foo", "", ""}
		alphabeticalABar := utils.Relation{1, 0, "otherschema", "bar", "", ""}
		schemaOther := utils.Schema{2, "otherschema", "", ""}
		alphabeticalBFoo := utils.Relation{2, 0, "public", "foo", "", ""}
		alphabeticalBBar := utils.Relation{2, 0, "public", "bar", "", ""}
		schemaPublic := utils.Schema{1, "public", "Standard public schema", ""}
		schemas := []utils.Schema{schemaOther, schemaPublic}

		It("has multiple tables in a single schema", func() {
			tables := []utils.Relation{alphabeticalAFoo, alphabeticalABar}
			uniqueSchemas := utils.GetUniqueSchemas(schemas, tables)
			Expect(uniqueSchemas).To(Equal([]utils.Schema{schemaPublic}))
		})
		It("has multiple schemas, each with multiple tables", func() {
			tables := []utils.Relation{alphabeticalBFoo, alphabeticalBBar, alphabeticalAFoo, alphabeticalABar}
			uniqueSchemas := utils.GetUniqueSchemas(schemas, tables)
			Expect(uniqueSchemas).To(Equal([]utils.Schema{schemaOther, schemaPublic}))
		})
		It("has no tables", func() {
			tables := []utils.Relation{}
			uniqueSchemas := utils.GetUniqueSchemas(schemas, tables)
			Expect(uniqueSchemas).To(Equal([]utils.Schema{}))
		})
	})
})
