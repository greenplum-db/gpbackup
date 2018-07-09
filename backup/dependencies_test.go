package backup_test

import (
	"database/sql/driver"

	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"
	sqlmock "gopkg.in/DATA-DOG/go-sqlmock.v1"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup/dependencies tests", func() {
	var (
		function1 backup.Function
		function2 backup.Function
		function3 backup.Function
		relation1 backup.Relation
		relation2 backup.Relation
		relation3 backup.Relation
		type1     backup.Type
		type2     backup.Type
		type3     backup.Type
		view1     backup.View
		view2     backup.View
		view3     backup.View
		protocol1 backup.ExternalProtocol
		protocol2 backup.ExternalProtocol
		protocol3 backup.ExternalProtocol
	)

	BeforeEach(func() {
		function1 = backup.Function{Schema: "public", Name: "function1", Arguments: "integer, integer", DependsUpon: []string{}}
		function2 = backup.Function{Schema: "public", Name: "function2", Arguments: "numeric, text", DependsUpon: []string{}}
		function3 = backup.Function{Schema: "public", Name: "function3", Arguments: "integer, integer", DependsUpon: []string{}}
		relation1 = backup.Relation{Schema: "public", Name: "relation1", DependsUpon: []string{}}
		relation2 = backup.Relation{Schema: "public", Name: "relation2", DependsUpon: []string{}}
		relation3 = backup.Relation{Schema: "public", Name: "relation3", DependsUpon: []string{}}
		type1 = backup.Type{Schema: "public", Name: "type1", DependsUpon: []string{}}
		type2 = backup.Type{Schema: "public", Name: "type2", DependsUpon: []string{}}
		type3 = backup.Type{Schema: "public", Name: "type3", DependsUpon: []string{}}
		view1 = backup.View{Schema: "public", Name: "view1", DependsUpon: []string{}}
		view2 = backup.View{Schema: "public", Name: "view2", DependsUpon: []string{}}
		view3 = backup.View{Schema: "public", Name: "view3", DependsUpon: []string{}}
		protocol1 = backup.ExternalProtocol{Name: "protocol1", DependsUpon: []string{}}
		protocol2 = backup.ExternalProtocol{Name: "protocol2", DependsUpon: []string{}}
		protocol3 = backup.ExternalProtocol{Name: "protocol3", DependsUpon: []string{}}
	})
	Describe("TopologicalSort", func() {
		It("returns the original slice if there are no dependencies among objects", func() {
			relations := []backup.Sortable{relation1, relation2, relation3}

			relations = backup.TopologicalSort(relations)

			Expect(relations[0].FQN()).To(Equal("public.relation1"))
			Expect(relations[1].FQN()).To(Equal("public.relation2"))
			Expect(relations[2].FQN()).To(Equal("public.relation3"))
		})
		It("sorts the slice correctly if there is an object dependent on one other object", func() {
			relation1.DependsUpon = []string{"public.relation3"}
			relations := []backup.Sortable{relation1, relation2, relation3}

			relations = backup.TopologicalSort(relations)

			Expect(relations[0].FQN()).To(Equal("public.relation2"))
			Expect(relations[1].FQN()).To(Equal("public.relation3"))
			Expect(relations[2].FQN()).To(Equal("public.relation1"))
		})
		It("sorts the slice correctly if there are two objects dependent on one other object", func() {
			view1.DependsUpon = []string{"public.view2"}
			view3.DependsUpon = []string{"public.view2"}
			views := []backup.Sortable{view1, view2, view3}

			views = backup.TopologicalSort(views)

			Expect(views[0].FQN()).To(Equal("public.view2"))
			Expect(views[1].FQN()).To(Equal("public.view1"))
			Expect(views[2].FQN()).To(Equal("public.view3"))
		})
		It("sorts the slice correctly if there is one object dependent on two other objects", func() {
			type2.DependsUpon = []string{"public.type1", "public.type3"}
			types := []backup.Sortable{type1, type2, type3}

			types = backup.TopologicalSort(types)

			Expect(types[0].FQN()).To(Equal("public.type1"))
			Expect(types[1].FQN()).To(Equal("public.type3"))
			Expect(types[2].FQN()).To(Equal("public.type2"))
		})
		It("sorts the slice correctly if there are explicit dependencies", func() {
			type2.DependsUpon = []string{"public.type1", "public.function3(integer, integer)"}
			function3.DependsUpon = []string{"public.type1"}
			sortable := []backup.Sortable{type1, type2, function3}

			sortable = backup.TopologicalSort(sortable)

			Expect(sortable[0].FQN()).To(Equal("public.type1"))
			Expect(sortable[1].FQN()).To(Equal("public.function3(integer, integer)"))
			Expect(sortable[2].FQN()).To(Equal("public.type2"))
		})
		It("aborts if dependency loop (this shouldn't be possible)", func() {
			type1.DependsUpon = []string{"public.type3"}
			type2.DependsUpon = []string{"public.type1"}
			type3.DependsUpon = []string{"public.type2"}
			sortable := []backup.Sortable{type1, type2, type3}

			defer testhelper.ShouldPanicWithMessage("Dependency resolution failed; see log file gbytes.Buffer for details. This is a bug, please report.")
			sortable = backup.TopologicalSort(sortable)
		})
		It("aborts if dependencies are not met", func() {
			type1.DependsUpon = []string{"missing_thing", "public.type2"}
			sortable := []backup.Sortable{type1, type2}

			defer testhelper.ShouldPanicWithMessage("Dependency resolution failed; see log file gbytes.Buffer for details. This is a bug, please report.")
			sortable = backup.TopologicalSort(sortable)
		})
	})
	Describe("SortObjectsInDependencyOrder", func() {
		It("returns a slice of unsorted functions followed by types followed by tables followed by protocols if there are no dependencies among objects", func() {
			functions := []backup.Function{function1, function2, function3}
			types := []backup.Type{type1, type2, type3}
			relations := []backup.Relation{relation1, relation2, relation3}
			protocols := []backup.ExternalProtocol{protocol1, protocol2, protocol3}
			results := backup.SortObjectsInDependencyOrder(functions, types, relations, protocols)
			expected := []backup.Sortable{function1, function2, function3, type1, type2, type3, relation1, relation2, relation3, protocol1, protocol2, protocol3}
			Expect(results).To(Equal(expected))
		})
		It("returns a slice of sorted functions, types, and relations if there are dependencies among objects of the same type", func() {
			function2.DependsUpon = []string{"public.function3(integer, integer)"}
			type2.DependsUpon = []string{"public.type3"}
			relation2.DependsUpon = []string{"public.relation3"}
			protocol2.DependsUpon = []string{"protocol3"}
			functions := []backup.Function{function1, function2, function3}
			types := []backup.Type{type1, type2, type3}
			relations := []backup.Relation{relation1, relation2, relation3}
			protocols := []backup.ExternalProtocol{protocol1, protocol2, protocol3}
			results := backup.SortObjectsInDependencyOrder(functions, types, relations, protocols)
			expected := []backup.Sortable{function1, function3, type1, type3, relation1, relation3, protocol1, protocol3, function2, type2, relation2, protocol2}
			Expect(results).To(Equal(expected))
		})
		It("returns a slice of sorted functions, types, and relations if there are dependencies among objects of different types", func() {
			function2.DependsUpon = []string{"public.type3"}
			type2.DependsUpon = []string{"public.relation3"}
			relation2.DependsUpon = []string{"public.type1"}
			protocol2.DependsUpon = []string{"public.function1(integer, integer)", "public.relation2"}
			functions := []backup.Function{function1, function2, function3}
			types := []backup.Type{type1, type2, type3}
			relations := []backup.Relation{relation1, relation2, relation3}
			protocols := []backup.ExternalProtocol{protocol1, protocol2, protocol3}
			results := backup.SortObjectsInDependencyOrder(functions, types, relations, protocols)
			expected := []backup.Sortable{function1, function3, type1, type3, relation1, relation3, protocol1, protocol3, relation2, function2, type2, protocol2}
			Expect(results).To(Equal(expected))
		})
	})
	Describe("ConstructFunctionDependencies", func() {
		It("queries function dependencies in GPDB 5", func() {
			testutils.SetDBVersion(connectionPool, "5.0.0")
			header := []string{"oid", "referencedobject"}
			functionRows := sqlmock.NewRows(header).AddRow([]driver.Value{"1", "public.type"}...)

			function1.Oid = 1
			functions := []backup.Function{function1}

			mock.ExpectQuery(`SELECT (.*)`).WillReturnRows(functionRows)
			functions = backup.ConstructFunctionDependencies(connectionPool, functions)

			Expect(functions[0].DependsUpon).To(Equal([]string{"public.type"}))
		})
		It("queries function dependencies in GPDB 4.3", func() {
			testutils.SetDBVersion(connectionPool, "4.3.0")
			header := []string{"oid", "referencedobject"}
			functionRows := sqlmock.NewRows(header).AddRow([]driver.Value{"1", "public.type"}...)

			function1.Oid = 1
			functions := []backup.Function{function1}

			mock.ExpectQuery(`SELECT (.*)`).WillReturnRows(functionRows)
			functions = backup.ConstructFunctionDependencies(connectionPool, functions)

			Expect(functions[0].DependsUpon).To(Equal([]string{"public.type"}))
		})
	})
	Describe("ConstructBaseTypeDependencies", func() {
		It("queries base type dependencies in GPDB 5", func() {
			testutils.SetDBVersion(connectionPool, "5.0.0")
			header := []string{"oid", "referencedobject"}
			baseTypeRows := sqlmock.NewRows(header).AddRow([]driver.Value{"2", "public.func(integer, integer)"}...)

			type1.Oid = 2
			type1.Type = "b"
			types := []backup.Type{type1}

			mock.ExpectQuery(`SELECT (.*)`).WillReturnRows(baseTypeRows)
			types = backup.ConstructBaseTypeDependencies5(connectionPool, types)

			Expect(types[0].DependsUpon).To(Equal([]string{"public.func(integer, integer)"}))
		})
		It("queries base type dependencies in GPDB 4.3", func() {
			testutils.SetDBVersion(connectionPool, "4.3.0")
			funcInfoMap := map[uint32]backup.FunctionInfo{
				5: {QualifiedName: "public.func", Arguments: "integer, integer"},
			}
			header := []string{"oid", "referencedoid"}
			baseTypeRows := sqlmock.NewRows(header).AddRow([]driver.Value{"2", "5"}...)

			type1.Oid = 2
			type1.Type = "b"
			types := []backup.Type{type1}

			mock.ExpectQuery(`SELECT (.*)`).WillReturnRows(baseTypeRows)
			types = backup.ConstructBaseTypeDependencies4(connectionPool, types, funcInfoMap)

			Expect(types[0].DependsUpon).To(Equal([]string{"public.func(integer, integer)"}))
		})
	})
	Describe("ConstructCompositeTypeDependencies", func() {
		It("queries composite type dependencies in GPDB 5", func() {
			testutils.SetDBVersion(connectionPool, "5.0.0")
			header := []string{"oid", "referencedobject"}
			compTypeRows := sqlmock.NewRows(header).AddRow([]driver.Value{"3", "public.othertype"}...)

			type2.Oid = 3
			type2.Type = "c"
			types := []backup.Type{type2}

			mock.ExpectQuery(`SELECT (.*)`).WillReturnRows(compTypeRows)
			types = backup.ConstructCompositeTypeDependencies(connectionPool, types)

			Expect(types[0].DependsUpon).To(Equal([]string{"public.othertype"}))
		})
		It("queries composite type dependencies in GPDB 4.3", func() {
			testutils.SetDBVersion(connectionPool, "4.3.0")
			header := []string{"oid", "referencedobject"}
			compTypeRows := sqlmock.NewRows(header).AddRow([]driver.Value{"3", "public.othertype"}...)

			type2.Oid = 3
			type2.Type = "c"
			types := []backup.Type{type2}

			mock.ExpectQuery(`SELECT (.*)`).WillReturnRows(compTypeRows)

			types = backup.ConstructCompositeTypeDependencies(connectionPool, types)

			Expect(types[0].DependsUpon).To(Equal([]string{"public.othertype"}))
		})
	})
	Describe("ConstructDomainDependencies", func() {
		It("queries domain dependencies in GPDB 5", func() {
			testutils.SetDBVersion(connectionPool, "5.0.0")
			header := []string{"oid", "referencedobject"}
			domainRows := sqlmock.NewRows(header).AddRow([]driver.Value{"4", "public.builtin"}...)

			type3.Oid = 4
			type3.Type = "d"

			types := []backup.Type{type3}

			mock.ExpectQuery(`SELECT (.*)`).WillReturnRows(domainRows)
			types = backup.ConstructDomainDependencies(connectionPool, types)

			Expect(types[0].DependsUpon).To(Equal([]string{"public.builtin"}))
		})
		It("queries domain dependencies in GPDB 4.3", func() {
			testutils.SetDBVersion(connectionPool, "4.3.0")
			header := []string{"oid", "referencedobject"}
			domainRows := sqlmock.NewRows(header).AddRow([]driver.Value{"4", "public.builtin"}...)

			type3.Oid = 4
			type3.Type = "d"
			types := []backup.Type{type3}

			mock.ExpectQuery(`SELECT (.*)`).WillReturnRows(domainRows)
			types = backup.ConstructDomainDependencies(connectionPool, types)

			Expect(types[0].DependsUpon).To(Equal([]string{"public.builtin"}))
		})
	})
	Describe("ConstructDependentObjectMetadataMap", func() {
		It("composes metadata maps for functions, types, and tables into one map", func() {
			funcMap := backup.MetadataMap{1: backup.ObjectMetadata{Comment: "function"}}
			typeMap := backup.MetadataMap{2: backup.ObjectMetadata{Comment: "type"}}
			tableMap := backup.MetadataMap{3: backup.ObjectMetadata{Comment: "relation"}}
			protoMap := backup.MetadataMap{4: backup.ObjectMetadata{Comment: "protocol"}}
			result := backup.ConstructDependentObjectMetadataMap(funcMap, typeMap, tableMap, protoMap)
			expected := backup.MetadataMap{
				1: backup.ObjectMetadata{Comment: "function"},
				2: backup.ObjectMetadata{Comment: "type"},
				3: backup.ObjectMetadata{Comment: "relation"},
				4: backup.ObjectMetadata{Comment: "protocol"},
			}
			Expect(result).To(Equal(expected))
		})
	})
	Describe("SortViews", func() {
		It("sorts the slice correctly if there are two objects dependent on one other object", func() {
			view1.DependsUpon = []string{"public.view2"}
			view3.DependsUpon = []string{"public.view2"}
			views := []backup.View{view1, view2, view3}

			views = backup.SortViews(views)

			Expect(views[0].FQN()).To(Equal("public.view2"))
			Expect(views[1].FQN()).To(Equal("public.view1"))
			Expect(views[2].FQN()).To(Equal("public.view3"))
		})
	})
})
