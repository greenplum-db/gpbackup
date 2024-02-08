package backup_test

import (
	"database/sql"
	"database/sql/driver"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/greenplum-db/gp-common-go-libs/structmatcher"
	"github.com/greenplum-db/gpbackup/backup"

	. "github.com/onsi/ginkgo/v2"
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
	Describe("GetFunctions", func() {
		It("GetFunctions properly handles NULL function arguments, NULL function identity arguments, or NULL function result types", func() {
			var (
				header                                                  []string
				rowGood, rowNullArg, rowNullIdentArg, rowNullResultType []driver.Value
			)
			//The column prodataaccess was removed in GP7 so we declare two sets of variables with and without that column
			if connectionPool.Version.AtLeast("7") {
				header = []string{"oid", "schema", "name", "proretset", "functionbody", "binarypath", "arguments", "identargs", "resulttype",
					"provolatile", "proisstrict", "prosecdef", "proconfig", "procost", "prorows", "language"}
				rowGood = []driver.Value{"1", "mock_schema", "mock_table", false, "mock_funcbody", "mock_path",
					sql.NullString{String: "mock_args", Valid: true}, sql.NullString{String: "mock_identargs", Valid: true},
					sql.NullString{String: "mock_resulttype", Valid: true}, "mock_volatility", false, false, "", 0, 0,
					"mock_language"}
				rowNullArg = []driver.Value{"2", "mock_schema2", "mock_table2", false, "mock_funcbody2", "mock_path2", nil,
					sql.NullString{String: "mock_identargs2", Valid: true}, sql.NullString{String: "mock_resulttype2", Valid: true}, "mock_volatility2",
					false, false, "", 0, 0, "mock_language2"}
				rowNullIdentArg = []driver.Value{"3", "mock_schema3", "mock_table3", false, "mock_funcbody3", "mock_path3",
					sql.NullString{String: "mock_args3", Valid: true}, nil, sql.NullString{String: "mock_resulttype3", Valid: true}, "mock_volatility3",
					false, false, "", 0, 0, "mock_language3"}
				rowNullResultType = []driver.Value{"4", "mock_schema4", "mock_table4", false, "mock_funcbody4", "mock_path4",
					sql.NullString{String: "mock_args4", Valid: true}, sql.NullString{String: "mock_identargs4", Valid: true}, nil, "mock_volatility4",
					false, false, "", 0, 0, "mock_language4"}
			} else {
				header = []string{"oid", "schema", "name", "proretset", "functionbody", "binarypath", "arguments", "identargs", "resulttype",
					"provolatile", "proisstrict", "prosecdef", "proconfig", "procost", "prorows", "prodataaccess", "language"}
				rowGood = []driver.Value{"1", "mock_schema", "mock_table", false, "mock_funcbody", "mock_path",
					sql.NullString{String: "mock_args", Valid: true}, sql.NullString{String: "mock_identargs", Valid: true},
					sql.NullString{String: "mock_resulttype", Valid: true}, "mock_volatility", false, false, "", 0, 0,
					"mock_dataaccess", "mock_language"}
				rowNullArg = []driver.Value{"2", "mock_schema2", "mock_table2", false, "mock_funcbody2", "mock_path2", nil,
					sql.NullString{String: "mock_identargs2", Valid: true}, sql.NullString{String: "mock_resulttype2", Valid: true}, "mock_volatility2",
					false, false, "", 0, 0, "mock_dataaccess2", "mock_language2"}
				rowNullIdentArg = []driver.Value{"3", "mock_schema3", "mock_table3", false, "mock_funcbody3", "mock_path3",
					sql.NullString{String: "mock_args3", Valid: true}, nil, sql.NullString{String: "mock_resulttype3", Valid: true}, "mock_volatility3",
					false, false, "", 0, 0, "mock_dataaccess3", "mock_language3"}
				rowNullResultType = []driver.Value{"4", "mock_schema4", "mock_table4", false, "mock_funcbody4", "mock_path4",
					sql.NullString{String: "mock_args4", Valid: true}, sql.NullString{String: "mock_identargs4", Valid: true}, nil, "mock_volatility4",
					false, false, "", 0, 0, "mock_dataaccess4", "mock_language4"}
			}

			fakeRows := sqlmock.NewRows(header).AddRow(rowGood...).AddRow(rowNullArg...).AddRow(rowNullIdentArg...).AddRow(rowNullResultType...)
			mock.ExpectQuery(`SELECT (.*)`).WillReturnRows(fakeRows)
			result := backup.GetFunctions(connectionPool)

			// Expect the GetFunctions function to return only the 1st row since all other rows have invalid NULL strings
			expectedResult := []backup.Function{{Oid: 1, Schema: "mock_schema", Name: "mock_table", ReturnsSet: false, FunctionBody: "mock_funcbody",
				BinaryPath: "mock_path", Arguments: sql.NullString{String: "mock_args", Valid: true},
				IdentArgs: sql.NullString{String: "mock_identargs", Valid: true}, ResultType: sql.NullString{String: "mock_resulttype", Valid: true},
				Volatility: "mock_volatility", IsStrict: false, IsSecurityDefiner: false, Config: "", Cost: 0,
				NumRows: 0, DataAccess: "mock_dataaccess", Language: "mock_language"}}
			if connectionPool.Version.AtLeast("7") {
				expectedResult[0].DataAccess = ""
			}

			Expect(result).To(HaveLen(1))
			structmatcher.ExpectStructsToMatch(&expectedResult[0], &result[0])
		})
	})
	Describe("GetAggregates", func() {
		It("GetAggregates properly handles NULL aggregate arguments or NULL aggregate identity arguments", func() {
			header := []string{"oid", "schema", "name", "arguments", "identargs", "aggtransfn", "aggprelimfn", "aggfinalfn", "sortoperator",
				"sortoperatorschema", "transitiondatatype", "initialvalue", "initvalisnull", "minitvalisnull", "aggordered"}
			rowGood := []driver.Value{"1", "mock_schema", "mock_table", sql.NullString{String: "mock_args", Valid: true},
				sql.NullString{String: "mock_identargs", Valid: true}, 0, 0, 0, "mock_operator", "mock_operatorschema",
				"mock_transdatatype", "mock_initvalue", false, false, false}
			rowNullArg := []driver.Value{"2", "mock_schema2", "mock_table2", nil, sql.NullString{String: "mock_identargs2", Valid: true}, 0, 0, 0,
				"mock_operator2", "mock_operatorschema2", "mock_transdatatype2", "mock_initvalue2", false, false, false}
			rowNullIdentArg := []driver.Value{"3", "mock_schema3", "mock_table3", sql.NullString{String: "mock_args3", Valid: true}, nil, 0, 0, 0,
				"mock_operator3", "mock_operatorschema3", "mock_transdatatype3", "mock_initvalue3", false, false, false}
			fakeRows := sqlmock.NewRows(header).AddRow(rowGood...).AddRow(rowNullArg...).AddRow(rowNullIdentArg...)
			mock.ExpectQuery(`SELECT (.*)`).WillReturnRows(fakeRows)
			result := backup.GetAggregates(connectionPool)

			// Expect the GetAggregates function to return only the 1st row since all other rows have invalid NULL strings
			expectedResult := []backup.Aggregate{{Oid: 1, Schema: "mock_schema", Name: "mock_table",
				Arguments: sql.NullString{String: "mock_args", Valid: true}, IdentArgs: sql.NullString{String: "mock_identargs", Valid: true},
				TransitionFunction: 0, PreliminaryFunction: 0, FinalFunction: 0, SortOperator: "mock_operator",
				SortOperatorSchema: "mock_operatorschema", TransitionDataType: "mock_transdatatype", InitialValue: "mock_initvalue",
				InitValIsNull: false, MInitValIsNull: false, IsOrdered: false}}
			Expect(result).To(HaveLen(1))
			structmatcher.ExpectStructsToMatch(&expectedResult[0], &result[0])
		})
	})
})
