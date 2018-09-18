package backup

import (
	"fmt"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
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
// func AddProtocolDependenciesForGPDB4(tables []Relation, tableDefs map[uint32]TableDefinition, protocols []ExternalProtocol) []Relation {
// 	protocolMap := make(map[string]bool, len(protocols))
// 	for _, p := range protocols {
// 		protocolMap[p.Name] = true
// 	}
// 	for i, table := range tables {
// 		extTableDef := tableDefs[table.Oid].ExtTableDef
// 		if extTableDef.Location != "" {
// 			protocolName := extTableDef.Location[0:strings.Index(extTableDef.Location, "://")]
// 			if protocolMap[protocolName] {
// 				tables[i].DependsUpon = append(tables[i].DependsUpon, protocolName)
// 			}
// 		}
// 	}
// 	return tables
// }

var (
	PG_PROC_OID        uint32 = 1255
	PG_CLASS_OID       uint32 = 1259
	PG_TYPE_OID        uint32 = 1247
	PG_EXTPROTOCOL_OID uint32 = 7175
)

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
	GetDepEntry() DepEntry
}

func SortViews(views []View) []View {
	sortable := make([]Sortable, len(views))
	for i := range views {
		sortable[i] = views[i]
	}
	tmp := make(DependencyMap, 0)
	sortable = TopologicalSort(sortable, tmp)
	for i := range views {
		views[i] = sortable[i].(View)
	}
	return views
}

func TopologicalSort(slice []Sortable, dependencies DependencyMap) []Sortable {
	inDegrees := make(map[DepEntry]int, 0)
	dependencyIndexes := make(map[DepEntry]int, 0)
	isDependentOn := make(map[DepEntry][]DepEntry, 0)
	queue := make([]Sortable, 0)
	sorted := make([]Sortable, 0)
	notVisited := make(map[DepEntry]bool, 0)
	for i, item := range slice {
		depEntry := item.GetDepEntry()
		deps := dependencies[item.GetDepEntry()]
		notVisited[item.GetDepEntry()] = true
		inDegrees[depEntry] = len(deps)
		for _, dep := range deps {
			isDependentOn[dep] = append(isDependentOn[dep], depEntry)
		}
		dependencyIndexes[depEntry] = i
		if len(deps) == 0 {
			queue = append(queue, item)
		}
	}
	for len(queue) > 0 {
		item := queue[0]
		queue = queue[1:]
		sorted = append(sorted, item)
		notVisited[item.GetDepEntry()] = false
		for _, dep := range isDependentOn[item.GetDepEntry()] {
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
			if notVisited[item.GetDepEntry()] {
				gplog.Verbose("Object: %s; TODO:ADD DEPENDENCIES", item.FQN())
			}
		}
		gplog.Fatal(errors.Errorf("Dependency resolution failed; see log file %s for details. This is a bug, please report.", gplog.GetLogFilePath()), "")
	}
	return sorted
}

type DependencyMap map[DepEntry][]DepEntry

type DepEntry struct {
	Classid uint32
	Objid   uint32
}

// This function only returns dependencies that are referenced in the backup set
func GetDependencies(connectionPool *dbconn.DBConn, backupSet map[DepEntry]bool) DependencyMap {
	query := fmt.Sprintf(`SELECT
    CASE
        WHEN id1.refclassid IS NOT NULL
        THEN id1.refclassid
        ELSE d.classid
    END AS classid,
    CASE
        WHEN id1.refobjid IS NOT NULL
        THEN id1.refobjid
        ELSE d.objid
    END AS objid,
    CASE
        WHEN id2.refclassid IS NOT NULL
        THEN id2.refclassid
        ELSE d.refclassid
    END AS refclassid,
    CASE
        WHEN id2.refobjid IS NOT NULL
        THEN id2.refobjid
        ELSE d.refobjid
    END AS refobjid
FROM pg_depend d
LEFT JOIN pg_depend id1 ON (d.objid = id1.objid and d.classid = id1.classid and id1.deptype='i')
LEFT JOIN pg_depend id2 ON (d.refobjid = id2.objid and d.refclassid = id2.classid and id2.deptype='i')
WHERE d.classid != 0
AND d.deptype != 'i'`)

	pgDependDeps := make([]struct {
		ClassID    uint32
		ObjID      uint32
		RefClassID uint32
		RefObjID   uint32
	}, 0)

	err := connectionPool.Select(&pgDependDeps, query)
	gplog.FatalOnError(err)

	dependencyMap := make(DependencyMap, 0)
	for _, dep := range pgDependDeps {
		object := DepEntry{
			Classid: dep.ClassID,
			Objid:   dep.ObjID,
		}
		referenceObject := DepEntry{
			Classid: dep.RefClassID,
			Objid:   dep.RefObjID,
		}

		_, objInBackup := backupSet[object]
		_, referenceInBackup := backupSet[referenceObject]

		if !objInBackup || !referenceInBackup {
			continue
		}

		if _, ok := dependencyMap[object]; !ok {
			dependencyMap[object] = make([]DepEntry, 0)
		}

		dependencyMap[object] = append(dependencyMap[object], referenceObject)
	}

	breakCircularDependencies(dependencyMap)

	return dependencyMap
}

func breakCircularDependencies(depMap DependencyMap) {
	for key, dep := range depMap {
		for _, entry := range dep {
			if _, ok := depMap[entry]; ok {
				for entry2Index, entry2 := range depMap[entry] {
					if key == entry2 {
						// Break circular dep where function depends on something.
						if entry.Classid == PG_PROC_OID {
							last := len(depMap[entry]) - 1
							if last == 0 {
								delete(depMap, entry)
							} else {
								depMap[entry][entry2Index] = depMap[entry][last]
								depMap[entry] = depMap[entry][:last]
							}
						}
					}
				}
			}
		}
	}
}
