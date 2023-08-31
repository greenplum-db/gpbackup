package restore

import (
	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gpbackup/toc"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("restore internal tests", func() {
	statements := []toc.StatementWithType{
		{ // simple table
			Schema: "foo", Name: "bar", ObjectType: "TABLE",
			Statement: "\n\nCREATE TABLE foo.bar (\n\ti integer\n) DISTRIBUTED BY (i);\n",
		},
		{ // simple schema
			Schema: "foo", Name: "foo", ObjectType: "SCHEMA",
			Statement: "\n\nCREATE SCHEMA foo;\n",
		},
		{ // table with a schema containing dots
			Schema: "\"foo.bar\"", Name: "baz", ObjectType: "TABLE",
			Statement: "\n\nCREATE TABLE \"foo.bar\".baz (\n\ti integer\n) DISTRIBUTED BY (i);\n",
		},
		{ // table with a schema containing quotes
			Schema: "\"foo\"bar\"", Name: "baz", ObjectType: "TABLE",
			Statement: "\n\nCREATE TABLE \"foo\"bar\".baz (\n\ti integer\n) DISTRIBUTED BY (i);\n",
		},
		{ // view with multiple schema replacements
			Schema: "foo", Name: "myview", ObjectType: "VIEW",
			Statement: "\n\nCREATE VIEW foo.myview AS  SELECT bar.i\n   FROM foo.bar;\n",
		},
		{ // schema and table are the same name
			Schema: "foo", Name: "foo", ObjectType: "TABLE",
			Statement: "\n\nCREATE TABLE foo.foo (\n\ti integer\n) DISTRIBUTED BY (i);\n",
		},
		{ // multi-line permissions block for a schema
			Schema: "foo", Name: "foo", ObjectType: "SCHEMA",
			Statement: "\n\nREVOKE ALL ON SCHEMA foo FROM PUBLIC;\nGRANT ALL ON SCHEMA foo TO testuser;\n",
		},
		{ // multi-line permissions block for a non-schema object
			Schema: "foo", Name: "myfunc", ObjectType: "FUNCTION",
			Statement: "\n\nREVOKE ALL ON FUNCTION foo.myfunc(integer) FROM PUBLIC;\nGRANT ALL ON FUNCTION foo.myfunc(integer) TO testuser;\n",
		},
		{ // multi-line permissions block with a schema containing dots
			Schema: "\"foo.bar\"", Name: "myfunc", ObjectType: "FUNCTION",
			Statement: "\n\nREVOKE ALL ON FUNCTION \"foo.bar\".myfunc(integer) FROM PUBLIC;\nGRANT ALL ON FUNCTION \"foo.bar\".myfunc(integer) TO testuser;\n",
		},
		{ // ALTER TABLE ... ATTACH PARTITION statement
			Schema: "public", Name: "foopart_p1", ObjectType: "TABLE", ReferenceObject: "public.foopart",
			Statement: "\n\nALTER TABLE public.foopart ATTACH PARTITION public.foopart_p1 FOR VALUES FROM (0) TO (1);\n",
		},
		{ // ALTER TABLE ONLY ... ATTACH PARTITION statement
			Schema: "public", Name: "foopart_p1", ObjectType: "TABLE", ReferenceObject: "public.foopart",
			Statement: "\n\nALTER TABLE ONLY public.foopart ATTACH PARTITION public.foopart_p1 FOR VALUES FROM (0) TO (1);\n",
		},
	}
	Describe("editStatementsRedirectStatements", func() {
		It("does not alter schemas if no redirect was specified", func() {
			originalStatements := make([]toc.StatementWithType, len(statements))
			copy(originalStatements, statements)

			editStatementsRedirectSchema(statements, "")

			// Loop through statements individually instead of comparing the whole arrays directly,
			// to make it easier to find the statements with issues
			for i := range statements {
				Expect(statements[i]).To(Equal(originalStatements[i]))
			}
		})
		It("changes schema in the sql statement", func() {
			// We need to temporarily set the version to 7 or later to test the ATTACH PARTITION replacement
			oldVersion := connectionPool.Version
			connectionPool.Version = dbconn.NewVersion("7.0.0")
			defer func() { connectionPool.Version = oldVersion }()

			editStatementsRedirectSchema(statements, "foo2")

			expectedStatements := []toc.StatementWithType{
				{
					Schema: "foo2", Name: "bar", ObjectType: "TABLE",
					Statement: "\n\nCREATE TABLE foo2.bar (\n\ti integer\n) DISTRIBUTED BY (i);\n",
				},
				{
					Schema: "foo2", Name: "foo2", ObjectType: "SCHEMA",
					Statement: "\n\nCREATE SCHEMA foo2;\n",
				},
				{
					Schema: "foo2", Name: "baz", ObjectType: "TABLE",
					Statement: "\n\nCREATE TABLE foo2.baz (\n\ti integer\n) DISTRIBUTED BY (i);\n",
				},
				{
					Schema: "foo2", Name: "baz", ObjectType: "TABLE",
					Statement: "\n\nCREATE TABLE foo2.baz (\n\ti integer\n) DISTRIBUTED BY (i);\n",
				},
				{
					Schema: "foo2", Name: "myview", ObjectType: "VIEW",
					Statement: "\n\nCREATE VIEW foo2.myview AS  SELECT bar.i\n   FROM foo.bar;\n",
				},
				{
					Schema: "foo2", Name: "foo", ObjectType: "TABLE",
					Statement: "\n\nCREATE TABLE foo2.foo (\n\ti integer\n) DISTRIBUTED BY (i);\n",
				},
				{
					Schema: "foo2", Name: "foo2", ObjectType: "SCHEMA",
					Statement: "\n\nREVOKE ALL ON SCHEMA foo2 FROM PUBLIC;\nGRANT ALL ON SCHEMA foo2 TO testuser;\n",
				},
				{
					Schema: "foo2", Name: "myfunc", ObjectType: "FUNCTION",
					Statement: "\n\nREVOKE ALL ON FUNCTION foo2.myfunc(integer) FROM PUBLIC;\nGRANT ALL ON FUNCTION foo2.myfunc(integer) TO testuser;\n",
				},
				{
					Schema: "foo2", Name: "myfunc", ObjectType: "FUNCTION",
					Statement: "\n\nREVOKE ALL ON FUNCTION foo2.myfunc(integer) FROM PUBLIC;\nGRANT ALL ON FUNCTION foo2.myfunc(integer) TO testuser;\n",
				},
				{ // ALTER TABLE ... ATTACH PARTITION statement
					Schema: "foo2", Name: "foopart_p1", ObjectType: "TABLE", ReferenceObject: "foo2.foopart",
					Statement: "\n\nALTER TABLE foo2.foopart ATTACH PARTITION foo2.foopart_p1 FOR VALUES FROM (0) TO (1);\n",
				},
				{ // ALTER TABLE ONLY ... ATTACH PARTITION statement
					Schema: "foo2", Name: "foopart_p1", ObjectType: "TABLE", ReferenceObject: "foo2.foopart",
					Statement: "\n\nALTER TABLE ONLY foo2.foopart ATTACH PARTITION foo2.foopart_p1 FOR VALUES FROM (0) TO (1);\n",
				},
			}

			for i := range statements {
				//fmt.Println("\n\nACTUAL\n", statements[i], "\nEXPECTED\n", expectedStatements[i])
				Expect(statements[i]).To(Equal(expectedStatements[i]))
			}
		})
	})
})
