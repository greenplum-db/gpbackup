package backup

import (
	"fmt"
	"sort"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpbackup/toc"
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
func AddProtocolDependenciesForGPDB4(depMap DependencyMap, tables []Table, protocols []ExternalProtocol) {
	protocolMap := make(map[string]UniqueID, len(protocols))
	for _, p := range protocols {
		protocolMap[p.Name] = p.GetUniqueID()
	}
	for _, table := range tables {
		extTableDef := table.ExtTableDef
		if extTableDef.Location.Valid && extTableDef.Location.String != "" {
			protocolName := extTableDef.Location.String[0:strings.Index(extTableDef.Location.String, "://")]
			if protocolEntry, ok := protocolMap[protocolName]; ok {
				tableEntry := table.GetUniqueID()
				if _, ok := depMap[tableEntry]; !ok {
					depMap[tableEntry] = make(map[UniqueID]bool)
				}
				depMap[tableEntry][protocolEntry] = true
			}
		}
	}
}

var (
	PG_AGGREGATE_OID            uint32 = 1255
	PG_AUTHID_OID               uint32 = 1260
	PG_CAST_OID                 uint32 = 2605
	PG_CLASS_OID                uint32 = 1259
	PG_COLLATION_OID            uint32 = 3456
	PG_CONSTRAINT_OID           uint32 = 2606
	PG_CONVERSION_OID           uint32 = 2607
	PG_DATABASE_OID             uint32 = 1262
	PG_EVENT_TRIGGER            uint32 = 3466
	PG_EXTENSION_OID            uint32 = 3079
	PG_EXTPROTOCOL_OID          uint32 = 7175
	PG_FOREIGN_DATA_WRAPPER_OID uint32 = 2328
	PG_FOREIGN_SERVER_OID       uint32 = 1417
	PG_INDEX_OID                uint32 = 2610
	PG_LANGUAGE_OID             uint32 = 2612
	PG_TRANSFORM_OID            uint32 = 3576
	PG_NAMESPACE_OID            uint32 = 2615
	PG_OPCLASS_OID              uint32 = 2616
	PG_OPERATOR_OID             uint32 = 2617
	PG_OPFAMILY_OID             uint32 = 2753
	PG_PROC_OID                 uint32 = 1255
	PG_RESGROUP_OID             uint32 = 6436
	PG_RESQUEUE_OID             uint32 = 6026
	PG_REWRITE_OID              uint32 = 2618
	PG_STATISTIC_EXT_OID        uint32 = 3381
	PG_TABLESPACE_OID           uint32 = 1213
	PG_TRIGGER_OID              uint32 = 2620
	PG_TS_CONFIG_OID            uint32 = 3602
	PG_TS_DICT_OID              uint32 = 3600
	PG_TS_PARSER_OID            uint32 = 3601
	PG_TS_TEMPLATE_OID          uint32 = 3764
	PG_TYPE_OID                 uint32 = 1247
	PG_USER_MAPPING_OID         uint32 = 1418

	FIRST_NORMAL_OBJECT_ID uint32 = 16384
)

/*
 * Structs and functions for topological sort
 */

type Sortable interface {
	FQN() string
	GetUniqueID() UniqueID
}

// This function sorts all dependencies to ensure that metadata restore happens in a correct order.
// It also assigns "tiers" to each metadata object, to ensure that in the case of parallel restores
// the parallel transactions do not contend with each other for locks on reference objects. Tiers
// are represented as slices of length=2 like so: [0,0]. The first value indicates the depth of
// dependency of the object, so that each "batch" of restores is of generally parallelizable
// objects. The second value indicates which connection should restore that object, so that for
// example views and constraints which reference the same table are assigned to the same
// transaction and do not deadlock each other. A tier[0]==0 value is used to indicate that this
// object must be restored serially and should not be parallelized. Given this, both values are
// 1-indexed to keep them consistent.
func TopologicalSort(slice []Sortable, dependencies DependencyMap) ([]Sortable, map[UniqueID][]uint32) {
	inDegrees := make(map[UniqueID]int)
	dependencyIndexes := make(map[UniqueID]int)
	isDependentOn := make(map[UniqueID][]UniqueID)
	queue := make([]Sortable, 0)
	sorted := make([]Sortable, 0)
	notVisited := make(map[UniqueID]bool)
	nameForUniqueID := make(map[UniqueID]string)
	tierMap := make(map[UniqueID][]uint32)
	nextTier := make([]Sortable, 0)
	tierCount := 1
	for i, item := range slice {
		uniqueID := item.GetUniqueID()
		tierMap[uniqueID] = []uint32{0, 0}
		nameForUniqueID[uniqueID] = item.FQN()
		deps := dependencies[uniqueID]
		notVisited[uniqueID] = true
		inDegrees[uniqueID] = len(deps)
		for dep := range deps {
			isDependentOn[dep] = append(isDependentOn[dep], uniqueID)
		}
		dependencyIndexes[uniqueID] = i
		if len(deps) == 0 {
			queue = append(queue, item)
		}
	}
STAGE2:
	for len(queue) > 0 {
		item := queue[0]
		uniqueID := item.GetUniqueID()
		queue = queue[1:]
		tierMap[uniqueID][0] = uint32(tierCount)
		sorted = append(sorted, item)
		notVisited[item.GetUniqueID()] = false
		for _, dep := range isDependentOn[item.GetUniqueID()] {
			inDegrees[dep]--
			if inDegrees[dep] == 0 {
				nextTier = append(nextTier, slice[dependencyIndexes[dep]])
			}
		}
	}
	if len(nextTier) > 0 {
		queue = nextTier
		nextTier = make([]Sortable, 0)
		tierCount++
		goto STAGE2
	}
	if len(slice) != len(sorted) {
		gplog.Verbose("Failed to sort dependencies.")
		gplog.Verbose("Not yet visited:")
		for _, item := range slice {
			if notVisited[item.GetUniqueID()] {
				gplog.Verbose("Object: %s %+v ", item.FQN(), item.GetUniqueID())
				gplog.Verbose("Dependencies: ")
				for uniqueID := range dependencies[item.GetUniqueID()] {
					gplog.Verbose("\t%s %+v", nameForUniqueID[uniqueID], uniqueID)
				}
			}
		}
		gplog.Fatal(errors.Errorf("Dependency resolution failed; see log file %s for details. This is a bug, please report.", gplog.GetLogFilePath()), "")
	}
	assignCohorts(slice, dependencies, isDependentOn, tierMap)
	return sorted, tierMap
}

func mergeCohorts(cohortNumberMap map[uint32]map[UniqueID]bool, objectToCohortMap map[UniqueID]uint32, firstCohort uint32, secondCohort uint32) uint32 {
	var lowerCohort uint32
	var higherCohort uint32
	if firstCohort < secondCohort {
		lowerCohort = firstCohort
		higherCohort = secondCohort
	} else {
		lowerCohort = secondCohort
		higherCohort = firstCohort
	}
	_, lowerCohortExists := cohortNumberMap[lowerCohort]
	if !lowerCohortExists {
		cohortNumberMap[lowerCohort] = make(map[UniqueID]bool)
	}

	cohortToMerge, cohortExists := cohortNumberMap[higherCohort]
	if cohortExists {
		for object := range cohortToMerge {
			objectToCohortMap[object] = lowerCohort
			cohortNumberMap[lowerCohort][object] = true
		}
		delete(cohortNumberMap, higherCohort)
	}
	return lowerCohort
}

// When doing parallel restore, each tier must not contend for locks with the others, as this can
// result in deadlocks between parallel transactions. This routine pre-assigns cohorts of objects
// that can all be safely restored together. The maximum cohort number for a given tier represents
// the theoretically maximum number of jobs it's worth running from a metadata perspective. This
// function alters the input tierMap to populate the second value in the []uint32 slice for each
// UniqueID.
func assignCohorts(slice []Sortable, dependencies DependencyMap, isDependentOn map[UniqueID][]UniqueID, tierMap map[UniqueID][]uint32) {
	objectToCohortMap := make(map[UniqueID]uint32)
	cohortNumberMap := make(map[uint32]map[UniqueID]bool)

	// We assign each object to its own cohort initially, then any time a dependency relationship
	// is found, we merge the "trees" of dependencies of the found object. At the end, we have
	// accumulated the theoretically minimal number of cohorts needed to capture the unique
	// dependency trees within the item sets.
	i := uint32(0)
	for _, item := range slice {
		id := item.GetUniqueID()
		objectToCohortMap[id] = i + uint32(1)
		cohortNumberMap[i+uint32(1)] = make(map[UniqueID]bool)
		cohortNumberMap[i+uint32(1)][id] = true
		i++
	}

	for _, item := range slice {
		id := item.GetUniqueID()
		currCohort := objectToCohortMap[id]

		// Check upstream dependencies
		parents := dependencies[id]
		for parent := range parents {
			parentCohort := objectToCohortMap[parent]
			if parentCohort != currCohort {
				currCohort = mergeCohorts(cohortNumberMap, objectToCohortMap, currCohort, parentCohort)
			}
		}

		// Check downstream dependencies
		children := isDependentOn[id]
		for _, child := range children {
			childCohort := objectToCohortMap[child]
			if childCohort != currCohort {
				currCohort = mergeCohorts(cohortNumberMap, objectToCohortMap, currCohort, childCohort)
			}
		}
	}
	// Remap the cohort numbers to get a continuous range starting at 1
	cohorts := make([]uint32, len(cohortNumberMap))
	i = uint32(0)
	for k := range cohortNumberMap {
		cohorts[i] = k
		i++
	}
	sort.SliceStable(cohorts, func(i, j int) bool { return cohorts[i] < cohorts[j] })
	expectedCohort := uint32(1)
	for _, actualCohort := range cohorts {
		if expectedCohort != actualCohort {
			_ = mergeCohorts(cohortNumberMap, objectToCohortMap, expectedCohort, actualCohort)
		}
		expectedCohort++
	}

	// Finally, we mutate the tierMap, updating the minimal cohorts we've selected for writing out
	// to the toc.
	for id := range tierMap {
		cohort := objectToCohortMap[id]
		tierMap[id][1] = uint32(cohort)
	}
	return
}

type DependencyMap map[UniqueID]map[UniqueID]bool

type UniqueID struct {
	ClassID uint32
	Oid     uint32
}

type SortableDependency struct {
	ClassID    uint32
	ObjID      uint32
	RefClassID uint32
	RefObjID   uint32
}

// This function only returns dependencies that are referenced in the backup set
func GetDependencies(connectionPool *dbconn.DBConn, backupSet map[UniqueID]bool, tables []Table) DependencyMap {
	query := `SELECT
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
AND typelem != 0`

	pgDependDeps := make([]SortableDependency, 0)
	err := connectionPool.Select(&pgDependDeps, query)
	gplog.FatalOnError(err)

	// In GP7 restoring a child table to a parent when the parent already has a
	// constraint applied will error.  Our solution is to add additional
	// "synthetic" dependencies to the backup, requiring all child tables to be
	// attached to the parent before the constraints are applied.
	if connectionPool.Version.AtLeast("7") && len(tables) > 0 {
		tableOids := make([]string, len(tables))
		for idx, table := range tables {
			tableOids[idx] = fmt.Sprintf("%d", table.Oid)
		}
		syntheticConstraintDeps := make([]SortableDependency, 0)
		synthConstrDepQuery := fmt.Sprintf(`
			WITH constr_cte AS (
                SELECT
                    dep.refobjid,
                    con.conname,
                    con.connamespace
                FROM
                    pg_depend dep
                    INNER JOIN pg_constraint con ON dep.objid = con.oid
					INNER JOIN pg_class cls ON dep.refobjid = cls.oid
                WHERE
                    dep.refobjid IN (%s)
					AND cls.relkind in ('r','p', 'f')
					AND dep.deptype = 'n'
                )
              SELECT
                  'pg_constraint'::regclass::oid AS ClassID,
                  con.oid AS ObjID,
                  'pg_class'::regclass::oid AS RefClassID,
                  constr_cte.refobjid AS RefObjID
              FROM
                  pg_constraint con
                  INNER JOIN constr_cte
					ON con.conname = constr_cte.conname
					AND con.connamespace = constr_cte.connamespace
              WHERE
                  con.conislocal = true;`, strings.Join(tableOids, ", "))
		err := connectionPool.Select(&syntheticConstraintDeps, synthConstrDepQuery)
		gplog.FatalOnError(err)

		if len(syntheticConstraintDeps) > 0 {
			pgDependDeps = append(pgDependDeps, syntheticConstraintDeps...)
		}
	}

	dependencyMap := make(DependencyMap)
	for _, dep := range pgDependDeps {
		object := UniqueID{
			ClassID: dep.ClassID,
			Oid:     dep.ObjID,
		}
		referenceObject := UniqueID{
			ClassID: dep.RefClassID,
			Oid:     dep.RefObjID,
		}

		_, objInBackup := backupSet[object]
		_, referenceInBackup := backupSet[referenceObject]

		if object == referenceObject || !objInBackup || !referenceInBackup {
			continue
		}

		if _, ok := dependencyMap[object]; !ok {
			dependencyMap[object] = make(map[UniqueID]bool)
		}

		dependencyMap[object][referenceObject] = true
	}

	breakCircularDependencies(dependencyMap)

	return dependencyMap
}

func breakCircularDependencies(depMap DependencyMap) {
	for entry, deps := range depMap {
		for dep := range deps {
			if _, ok := depMap[dep]; ok && entry.ClassID == PG_TYPE_OID && dep.ClassID == PG_PROC_OID {
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

func PrintDependentObjectStatements(metadataFile *utils.FileWithByteCount, objToc *toc.TOC, objects []Sortable, metadataMap MetadataMap, domainConstraints []Constraint, funcInfoMap map[uint32]FunctionInfo) {
	domainConMap := make(map[string][]Constraint)
	for _, constraint := range domainConstraints {
		domainConMap[constraint.OwningObject] = append(domainConMap[constraint.OwningObject], constraint)
	}
	for _, object := range objects {
		objMetadata := metadataMap[object.GetUniqueID()]
		switch obj := object.(type) {
		case BaseType:
			PrintCreateBaseTypeStatement(metadataFile, objToc, obj, objMetadata)
		case CompositeType:
			PrintCreateCompositeTypeStatement(metadataFile, objToc, obj, objMetadata)
		case Domain:
			PrintCreateDomainStatement(metadataFile, objToc, obj, objMetadata, domainConMap[obj.FQN()])
		case RangeType:
			PrintCreateRangeTypeStatement(metadataFile, objToc, obj, objMetadata)
		case Function:
			PrintCreateFunctionStatement(metadataFile, objToc, obj, objMetadata)
		case Table:
			PrintCreateTableStatement(metadataFile, objToc, obj, objMetadata)
		case ExternalProtocol:
			PrintCreateExternalProtocolStatement(metadataFile, objToc, obj, funcInfoMap, objMetadata)
		case View:
			PrintCreateViewStatement(metadataFile, objToc, obj, objMetadata)
		case TextSearchParser:
			PrintCreateTextSearchParserStatement(metadataFile, objToc, obj, objMetadata)
		case TextSearchConfiguration:
			PrintCreateTextSearchConfigurationStatement(metadataFile, objToc, obj, objMetadata)
		case TextSearchTemplate:
			PrintCreateTextSearchTemplateStatement(metadataFile, objToc, obj, objMetadata)
		case TextSearchDictionary:
			PrintCreateTextSearchDictionaryStatement(metadataFile, objToc, obj, objMetadata)
		case Operator:
			PrintCreateOperatorStatement(metadataFile, objToc, obj, objMetadata)
		case OperatorClass:
			PrintCreateOperatorClassStatement(metadataFile, objToc, obj, objMetadata)
		case Aggregate:
			PrintCreateAggregateStatement(metadataFile, objToc, obj, funcInfoMap, objMetadata)
		case Cast:
			PrintCreateCastStatement(metadataFile, objToc, obj, objMetadata)
		case ForeignDataWrapper:
			PrintCreateForeignDataWrapperStatement(metadataFile, objToc, obj, funcInfoMap, objMetadata)
		case ForeignServer:
			PrintCreateServerStatement(metadataFile, objToc, obj, objMetadata)
		case UserMapping:
			PrintCreateUserMappingStatement(metadataFile, objToc, obj)
		case Constraint:
			PrintConstraintStatement(metadataFile, objToc, obj, objMetadata)
		case Transform:
			PrintCreateTransformStatement(metadataFile, objToc, obj, funcInfoMap, objMetadata)
		}
		// Remove ACLs from metadataMap for the current object since they have been processed
		delete(metadataMap, object.GetUniqueID())
	}
	//  Process ACLs for left over objects in the metadata map
	printExtensionFunctionACLs(metadataFile, objToc, metadataMap, funcInfoMap)
}
