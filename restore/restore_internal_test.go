package restore

import (
	"github.com/greenplum-db/gpbackup/toc"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("restore internal tests", func() {
	Describe("editStatementsRedirectStatements", func() {
		It("does not alter schemas if no redirect was specified", func() {
			statements := []toc.StatementWithType{
				{ // simple table
					Schema: "foo", Name: "bar", ObjectType: "TABLE",
					Statement: "\n\nCREATE TABLE foo.bar (\n\ti integer\n) DISTRIBUTED BY (i);\n",
				},
				{ // view with mulitple schema replacements
					Schema: "foo", Name: "myview", ObjectType: "VIEW",
					Statement: "\n\nCREATE VIEW foo.myview AS  SELECT bar.i\n   FROM foo.bar;\n",
				},
				{ // schema and table are the same name
					Schema: "foo", Name: "foo", ObjectType: "TABLE",
					Statement: "\n\nCREATE TABLE foo.foo (\n\ti integer\n) DISTRIBUTED BY (i);\n",
				},
			}

			editStatementsRedirectSchema(statements, "")
			Expect(statements).To(Equal(statements))
		})
		It("changes schema in the sql statement", func() {
			statements := []toc.StatementWithType{
				{ // simple table
					Schema: "foo", Name: "bar", ObjectType: "TABLE",
					Statement: "\n\nCREATE TABLE foo.bar (\n\ti integer\n) DISTRIBUTED BY (i);\n",
				},
				{ // schema and table are the same name
					Schema: "foo", Name: "foo", ObjectType: "TABLE",
					Statement: "\n\nCREATE TABLE foo.foo (\n\ti integer\n) DISTRIBUTED BY (i);\n",
				},
			}

			editStatementsRedirectSchema(statements, "foo2")

			expectedStatements := []toc.StatementWithType{
				{
					Schema: "foo2", Name: "bar", ObjectType: "TABLE",
					Statement: "\n\nCREATE TABLE foo2.bar (\n\ti integer\n) DISTRIBUTED BY (i);\n",
				},
				{
					Schema: "foo2", Name: "foo", ObjectType: "TABLE",
					Statement: "\n\nCREATE TABLE foo2.foo (\n\ti integer\n) DISTRIBUTED BY (i);\n",
				},
			}
			Expect(statements).To(Equal(expectedStatements))
		})
	})
})
