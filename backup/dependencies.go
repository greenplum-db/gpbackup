package backup

import (
	"fmt"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpbackup/utils"
	"github.com/pkg/errors"
)

/* This file contains functions to sort objects that have dependencies among themselves.
 *  For example, functions and types can be dependent on one another, we cannot simply
 *  dump all functions and then all types.
 *  The following objects are included the dependency sorting logic:
 *   - Functions
 *   - Types
 *   - Tables
 *   - Protocols
 */
func AddProtocolDependenciesForGPDB4(tables []Relation, tableDefs map[uint32]TableDefinition, protocols []ExternalProtocol) []Relation {
	protocolMap := make(map[string]bool, len(protocols))
	for _, p := range protocols {
		protocolMap[p.Name] = true
	}
	for i, table := range tables {
		extTableDef := tableDefs[table.Oid].ExtTableDef
		if extTableDef.Location != "" {
			protocolName := extTableDef.Location[0:strings.Index(extTableDef.Location, "://")]
			if protocolMap[protocolName] {
				tables[i].DependsUpon = append(tables[i].DependsUpon, protocolName)
			}
		}
	}
	return tables
}

/*
 * We need to include arguments to differentiate functions with the same name;
 * we don't use IdentArgs because we already have Arguments in the funcInfoMap.
 */
func SortObjectsInDependencyOrder(functions []Function, types []Type, tables []Relation, protocols []ExternalProtocol) []Sortable {
	objects := make([]Sortable, 0)
	for _, function := range functions {
		objects = append(objects, function)
	}
	for _, typ := range types {
		if typ.Type != "e" && typ.Type != "p" {
			objects = append(objects, typ)
		}
	}
	for _, table := range tables {
		objects = append(objects, table)
	}
	for _, protocol := range protocols {
		objects = append(objects, protocol)
	}
	sorted := TopologicalSort(objects)
	return sorted
}

func ConstructDependentObjectMetadataMap(functions MetadataMap, types MetadataMap, tables MetadataMap, protocols MetadataMap) MetadataMap {
	metadataMap := make(MetadataMap, 0)
	for k, v := range functions {
		metadataMap[k] = v
	}
	for k, v := range types {
		metadataMap[k] = v
	}
	for k, v := range tables {
		metadataMap[k] = v
	}
	for k, v := range protocols {
		metadataMap[k] = v
	}
	return metadataMap
}

/*
 * Structs and functions for topological sort
 */

type Sortable interface {
	FQN() string
	Dependencies() []string
}

func (r Relation) FQN() string {
	return r.ToString()
}

func (v View) FQN() string {
	return utils.MakeFQN(v.Schema, v.Name)
}

func (f Function) FQN() string {
	/*
	 * We need to include arguments to differentiate functions with the same name;
	 * we don't use IdentArgs because we already have Arguments in the funcInfoMap.
	 */
	return fmt.Sprintf("%s(%s)", utils.MakeFQN(f.Schema, f.Name), f.Arguments)
}

func (t Type) FQN() string {
	return utils.MakeFQN(t.Schema, t.Name)
}

func (p ExternalProtocol) FQN() string {
	return p.Name
}

func (r Relation) Dependencies() []string {
	return r.DependsUpon
}

func (v View) Dependencies() []string {
	return v.DependsUpon
}

func (f Function) Dependencies() []string {
	return f.DependsUpon
}

func (t Type) Dependencies() []string {
	return t.DependsUpon
}

func (p ExternalProtocol) Dependencies() []string {
	return p.DependsUpon
}

func SortViews(views []View) []View {
	sortable := make([]Sortable, len(views))
	for i := range views {
		sortable[i] = views[i]
	}
	sortable = TopologicalSort(sortable)
	for i := range views {
		views[i] = sortable[i].(View)
	}
	return views
}

func TopologicalSort(slice []Sortable) []Sortable {
	inDegrees := make(map[string]int, 0)
	dependencyIndexes := make(map[string]int, 0)
	isDependentOn := make(map[string][]string, 0)
	queue := make([]Sortable, 0)
	sorted := make([]Sortable, 0)
	notVisited := utils.NewEmptyIncludeSet()
	for i, item := range slice {
		name := item.FQN()
		deps := item.Dependencies()
		notVisited.Add(name)
		inDegrees[name] = len(deps)
		for _, dep := range deps {
			isDependentOn[dep] = append(isDependentOn[dep], name)
		}
		dependencyIndexes[name] = i
		if len(deps) == 0 {
			queue = append(queue, item)
		}
	}
	for len(queue) > 0 {
		item := queue[0]
		queue = queue[1:]
		sorted = append(sorted, item)
		notVisited.Delete(item.FQN())
		for _, dep := range isDependentOn[item.FQN()] {
			inDegrees[dep]--
			if inDegrees[dep] == 0 {
				queue = append(queue, slice[dependencyIndexes[dep]])
			}
		}
	}
	if len(slice) != len(sorted) {
		gplog.Verbose("Failed to sort dependencies.")
		gplog.Verbose("Not yet visited:")
		for _, item := range slice {
			if notVisited.MatchesFilter(item.FQN()) {
				gplog.Verbose("Object: %s; Dependencies: %s", item.FQN(), item.Dependencies())
			}
		}
		gplog.Fatal(errors.Errorf("Dependency resolution failed; see log file %s for details. This is a bug, please report.", gplog.GetLogFilePath()), "")
	}
	return sorted
}
