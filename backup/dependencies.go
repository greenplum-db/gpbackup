package backup

import (
	"fmt"
	"strings"

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
func AddProtocolDependenciesForGPDB4(depMap DependencyMap, tables []Relation, tableDefs map[uint32]TableDefinition, protocols []ExternalProtocol) {
	protocolMap := make(map[string]DepEntry, len(protocols))
	for _, p := range protocols {
		protocolMap[p.Name] = p.GetDepEntry()
	}
	for _, table := range tables {
		extTableDef := tableDefs[table.Oid].ExtTableDef
		if extTableDef.Location != "" {
			protocolName := extTableDef.Location[0:strings.Index(extTableDef.Location, "://")]
			if protocolEntry, ok := protocolMap[protocolName]; ok {
				tableEntry := table.GetDepEntry()
				if _, ok := depMap[tableEntry]; !ok {
					depMap[tableEntry] = make(map[DepEntry]bool, 0)
				}
				depMap[tableEntry][protocolEntry] = true
			}
		}
	}
}

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

func TopologicalSort(slice []Sortable, dependencies DependencyMap) []Sortable {
	inDegrees := make(map[DepEntry]int, 0)
	dependencyIndexes := make(map[DepEntry]int, 0)
	isDependentOn := make(map[DepEntry][]DepEntry, 0)
	queue := make([]Sortable, 0)
	sorted := make([]Sortable, 0)
	notVisited := make(map[DepEntry]bool, 0)
	nameForDepEntries := make(map[DepEntry]string, 0)
	for i, item := range slice {
		depEntry := item.GetDepEntry()
		nameForDepEntries[depEntry] = item.FQN()
		deps := dependencies[depEntry]
		notVisited[depEntry] = true
		inDegrees[depEntry] = len(deps)
		for dep := range deps {
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
				gplog.Verbose("Object: %s %+v ", item.FQN(), item.GetDepEntry())
				gplog.Verbose("Dependencies: ")
				for depEntry := range dependencies[item.GetDepEntry()] {
					gplog.Verbose("\t%s %+v", nameForDepEntries[depEntry], depEntry)
				}
			}
		}
		gplog.Fatal(errors.Errorf("Dependency resolution failed; see log file %s for details. This is a bug, please report.", gplog.GetLogFilePath()), "")
	}
	return sorted
}

type DependencyMap map[DepEntry]map[DepEntry]bool

type DepEntry struct {
	Classid uint32
	Objid   uint32
}

// This function only returns dependencies that are referenced in the backup set
func GetDependencies(connectionPool *dbconn.DBConn, backupSet map[DepEntry]bool) DependencyMap {
	query := fmt.Sprintf(`SELECT
	coalesce(id1.refclassid, d.classid) AS classid,
	coalesce(id1.refobjid, d.objid) AS objid,
	coalesce(id2.refclassid, d.refclassid) AS refclassid,
	coalesce(id2.refobjid, d.refobjid) AS refobjid
FROM pg_depend d
-- link implicit objects, using objid and refobjid, to the objects that created them
LEFT JOIN pg_depend id1 ON (d.objid = id1.objid and d.classid = id1.classid and id1.deptype='i')
LEFT JOIN pg_depend id2 ON (d.refobjid = id2.objid and d.refclassid = id2.classid and id2.deptype='i')
WHERE d.classid != 0
AND d.deptype != 'i'
UNION
-- this converts function dependencies on array types to the underlying type
-- this is needed because pg_depend in 4.3.x doesn't have the info we need
SELECT
	d.classid,
	d.objid,
	d.refclassid,
	t.typelem AS refobjid
FROM pg_depend d
JOIN pg_type t ON d.refobjid = t.oid
WHERE d.classid = 'pg_proc'::regclass::oid
AND typelem != 0`)

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

		if (object == referenceObject) || (!objInBackup || !referenceInBackup) {
			continue
		}

		if _, ok := dependencyMap[object]; !ok {
			dependencyMap[object] = make(map[DepEntry]bool, 0)
		}

		dependencyMap[object][referenceObject] = true
	}

	breakCircularDependencies(dependencyMap)

	return dependencyMap
}

func breakCircularDependencies(depMap DependencyMap) {
	for entry, deps := range depMap {
		for dep := range deps {
			if _, ok := depMap[dep]; ok && entry.Classid == PG_TYPE_OID && dep.Classid == PG_PROC_OID {
				if _, ok := depMap[dep][entry]; ok {
					if len(depMap[dep]) == 1 {
						delete(depMap, dep)
					} else {
						delete(depMap[dep], entry)
					}
				}
			}
		}
	}
}
