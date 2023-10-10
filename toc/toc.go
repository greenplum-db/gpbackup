package toc

import (
	"fmt"
	"io"
	"io/ioutil"
	"regexp"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpbackup/utils"
	"gopkg.in/yaml.v2"
)

type TOC struct {
	metadataEntryMap    map[string]*[]MetadataEntry
	GlobalEntries       []MetadataEntry
	PredataEntries      []MetadataEntry
	PostdataEntries     []MetadataEntry
	StatisticsEntries   []MetadataEntry
	DataEntries         []CoordinatorDataEntry
	IncrementalMetadata IncrementalEntries
}

type SegmentTOC struct {
	DataEntries map[uint]SegmentDataEntry
}

type MetadataEntry struct {
	Schema          string
	Name            string
	ObjectType      string
	ReferenceObject string
	StartByte       uint64
	EndByte         uint64
	Tier            []uint32
}

type CoordinatorDataEntry struct {
	Schema          string
	Name            string
	Oid             uint32
	AttributeString string
	RowsCopied      int64
	PartitionRoot   string
	IsReplicated    bool
}

type SegmentDataEntry struct {
	StartByte uint64
	EndByte   uint64
}

type IncrementalEntries struct {
	AO map[string]AOEntry
}

type AOEntry struct {
	Modcount         int64
	LastDDLTimestamp string
}

type UniqueID struct {
	ClassID uint32
	Oid     uint32
}

// These are to be used as the values for setting object type on any of the structs that require
// that field. Note that we do still dynamically add " METADATA" to some postdata object types to
// assist in batching for parallel restore.
const (
	OBJ_ACCESS_METHOD             = "ACCESS METHOD"
	OBJ_AGGREGATE                 = "AGGREGATE"
	OBJ_CAST                      = "CAST"
	OBJ_COLLATION                 = "COLLATION"
	OBJ_COLUMN                    = "COLUMN"
	OBJ_CONSTRAINT                = "CONSTRAINT"
	OBJ_CONVERSION                = "CONVERSION"
	OBJ_DATABASE                  = "DATABASE"
	OBJ_DATABASE_GUC              = "DATABASE GUC"
	OBJ_DATABASE_METADATA         = "DATABASE METADATA"
	OBJ_DEFAULT_PRIVILEGES        = "DEFAULT PRIVILEGES"
	OBJ_DOMAIN                    = "DOMAIN"
	OBJ_EVENT_TRIGGER             = "EVENT TRIGGER"
	OBJ_EXCHANGE_PARTITION        = "EXCHANGE PARTITION"
	OBJ_EXTENSION                 = "EXTENSION"
	OBJ_FOREIGN_DATA_WRAPPER      = "FOREIGN DATA WRAPPER"
	OBJ_FOREIGN_SERVER            = "FOREIGN SERVER"
	OBJ_FOREIGN_TABLE             = "FOREIGN TABLE"
	OBJ_FUNCTION                  = "FUNCTION"
	OBJ_INDEX                     = "INDEX"
	OBJ_LANGUAGE                  = "LANGUAGE"
	OBJ_MATERIALIZED_VIEW         = "MATERIALIZED VIEW"
	OBJ_OPERATOR                  = "OPERATOR"
	OBJ_OPERATOR_CLASS            = "OPERATOR CLASS"
	OBJ_OPERATOR_FAMILY           = "OPERATOR FAMILY"
	OBJ_POLICY                    = "POLICY"
	OBJ_PROCEDURE                 = "PROCEDURE"
	OBJ_PROTOCOL                  = "PROTOCOL"
	OBJ_RELATION                  = "RELATION"
	OBJ_RESOURCE_GROUP            = "RESOURCE GROUP"
	OBJ_RESOURCE_QUEUE            = "RESOURCE QUEUE"
	OBJ_ROLE                      = "ROLE"
	OBJ_ROLE_GRANT                = "ROLE GRANT"
	OBJ_ROLE_GUC                  = "ROLE GUCS"
	OBJ_RULE                      = "RULE"
	OBJ_SCHEMA                    = "SCHEMA"
	OBJ_SEQUENCE                  = "SEQUENCE"
	OBJ_SEQUENCE_OWNER            = "SEQUENCE OWNER"
	OBJ_SERVER                    = "SERVER"
	OBJ_SESSION_GUC               = "SESSION GUCS"
	OBJ_STATISTICS                = "STATISTICS"
	OBJ_STATISTICS_EXT            = "STATISTICS_EXT"
	OBJ_TABLE                     = "TABLE"
	OBJ_TABLESPACE                = "TABLESPACE"
	OBJ_TEXT_SEARCH_CONFIGURATION = "TEXT SEARCH CONFIGURATION"
	OBJ_TEXT_SEARCH_DICTIONARY    = "TEXT SEARCH DICTIONARY"
	OBJ_TEXT_SEARCH_PARSER        = "TEXT SEARCH PARSER"
	OBJ_TEXT_SEARCH_TEMPLATE      = "TEXT SEARCH TEMPLATE"
	OBJ_TRANSFORM                 = "TRANSFORM"
	OBJ_TRIGGER                   = "TRIGGER"
	OBJ_TYPE                      = "TYPE"
	OBJ_USER_MAPPING              = "USER MAPPING"
	OBJ_VIEW                      = "VIEW"
)

func NewTOC(filename string) *TOC {
	toc := &TOC{}
	contents, err := ioutil.ReadFile(filename)
	gplog.FatalOnError(err)
	err = yaml.Unmarshal(contents, toc)
	gplog.FatalOnError(err)
	return toc
}

func NewSegmentTOC(filename string) *SegmentTOC {
	toc := &SegmentTOC{}
	contents, err := ioutil.ReadFile(filename)
	gplog.FatalOnError(err)
	err = yaml.Unmarshal(contents, toc)
	gplog.FatalOnError(err)
	return toc
}

func (toc *TOC) WriteToFileAndMakeReadOnly(filename string) {
	contents, err := yaml.Marshal(toc)
	gplog.FatalOnError(err)
	err = utils.WriteToFileAndMakeReadOnly(filename, contents)
	gplog.FatalOnError(err)
}

func (toc *SegmentTOC) WriteToFileAndMakeReadOnly(filename string) error {
	contents, err := yaml.Marshal(toc)
	if err != nil {
		return err
	}
	return utils.WriteToFileAndMakeReadOnly(filename, contents)
}

type StatementWithType struct {
	Schema          string
	Name            string
	ObjectType      string
	ReferenceObject string
	Statement       string
	Tier            []uint32
}

func GetIncludedPartitionRoots(tocDataEntries []CoordinatorDataEntry, includeRelations []string) []string {
	if len(includeRelations) == 0 {
		return []string{}
	}
	rootPartitions := make([]string, 0)

	FQNToPartitionRoot := make(map[string]string)
	for _, entry := range tocDataEntries {
		if entry.PartitionRoot != "" {
			FQNToPartitionRoot[utils.MakeFQN(entry.Schema, entry.Name)] = utils.MakeFQN(entry.Schema, entry.PartitionRoot)
		}
	}

	for _, relation := range includeRelations {
		if rootPartition, ok := FQNToPartitionRoot[relation]; ok {
			rootPartitions = append(rootPartitions, rootPartition)
		}
	}

	return rootPartitions
}

func (toc *TOC) GetSQLStatementForObjectTypes(section string, metadataFile io.ReaderAt, includeObjectTypes []string, excludeObjectTypes []string, includeSchemas []string, excludeSchemas []string, includeRelations []string, excludeRelations []string) []StatementWithType {
	entries := *toc.metadataEntryMap[section]

	objectSet, schemaSet, relationSet := constructFilterSets(includeObjectTypes, excludeObjectTypes, includeSchemas, excludeSchemas, includeRelations, excludeRelations)
	statements := make([]StatementWithType, 0)
	for _, entry := range entries {
		if shouldIncludeStatement(entry, objectSet, schemaSet, relationSet) {
			contents := make([]byte, entry.EndByte-entry.StartByte)
			_, err := metadataFile.ReadAt(contents, int64(entry.StartByte))
			gplog.FatalOnError(err)
			statements = append(statements, StatementWithType{Schema: entry.Schema, Name: entry.Name, ObjectType: entry.ObjectType, ReferenceObject: entry.ReferenceObject, Statement: string(contents), Tier: entry.Tier})
		}
	}
	return statements
}

func constructFilterSets(includeObjectTypes []string, excludeObjectTypes []string, includeSchemas []string, excludeSchemas []string, includeRelations []string, excludeRelations []string) (*utils.FilterSet, *utils.FilterSet, *utils.FilterSet) {
	var objectSet, schemaSet, relationSet *utils.FilterSet
	if len(includeObjectTypes) > 0 {
		objectSet = utils.NewIncludeSet(includeObjectTypes)
	} else {
		objectSet = utils.NewExcludeSet(excludeObjectTypes)
	}
	if len(includeSchemas) > 0 {
		schemaSet = utils.NewIncludeSet(includeSchemas)
	} else {
		schemaSet = utils.NewExcludeSet(excludeSchemas)
	}
	if len(includeRelations) > 0 {
		relationSet = utils.NewIncludeSet(includeRelations)
	} else {
		relationSet = utils.NewExcludeSet(excludeRelations)
	}
	return objectSet, schemaSet, relationSet
}

func shouldIncludeStatement(entry MetadataEntry, objectSet *utils.FilterSet, schemaSet *utils.FilterSet, relationSet *utils.FilterSet) bool {
	shouldIncludeObject := objectSet.MatchesFilter(entry.ObjectType)
	shouldIncludeSchema := schemaSet.MatchesFilter(entry.Schema)
	relationFQN := utils.MakeFQN(entry.Schema, entry.Name)

	// In GPDB 7+, leaf partitions have the reference object set to their
	// upper-most root. If that root is in the exclude set, then we need
	// to prevent the leaf partition from being included.
	includeLeafPartition := true
	if relationSet.IsExclude && entry.ReferenceObject != "" && !relationSet.MatchesFilter(entry.ReferenceObject) {
		includeLeafPartition = false
	}

	shouldIncludeRelation := (relationSet.IsExclude && entry.ObjectType != OBJ_TABLE && entry.ObjectType != OBJ_VIEW && entry.ObjectType != OBJ_MATERIALIZED_VIEW && entry.ObjectType != OBJ_SEQUENCE && entry.ObjectType != OBJ_STATISTICS && entry.ReferenceObject == "") ||
		((entry.ObjectType == OBJ_TABLE || entry.ObjectType == OBJ_VIEW || entry.ObjectType == OBJ_MATERIALIZED_VIEW || entry.ObjectType == OBJ_SEQUENCE || entry.ObjectType == OBJ_STATISTICS) && relationSet.MatchesFilter(relationFQN) && includeLeafPartition) || // Relations should match the filter
		(entry.ObjectType != OBJ_SEQUENCE_OWNER && entry.ReferenceObject != "" && relationSet.MatchesFilter(entry.ReferenceObject)) || // Include relations that filtered tables depend on
		(entry.ObjectType == OBJ_SEQUENCE_OWNER && relationSet.MatchesFilter(relationFQN) && relationSet.MatchesFilter(entry.ReferenceObject)) //Include sequence owners if both table and sequence are being restored

	return shouldIncludeObject && shouldIncludeSchema && shouldIncludeRelation
}

func getLeafPartitions(tableFQNs []string, tocDataEntries []CoordinatorDataEntry) (leafPartitions []string) {
	tableSet := utils.NewSet(tableFQNs)

	for _, entry := range tocDataEntries {
		if entry.PartitionRoot == "" {
			continue
		}

		parentFQN := utils.MakeFQN(entry.Schema, entry.PartitionRoot)
		if tableSet.MatchesFilter(parentFQN) {
			leafPartitions = append(leafPartitions, utils.MakeFQN(entry.Schema, entry.Name))
		}
	}

	return leafPartitions
}

func (toc *TOC) GetDataEntriesMatching(includeSchemas []string, excludeSchemas []string,
	includeTableFQNs []string, excludeTableFQNs []string, restorePlanTableFQNs []string) []CoordinatorDataEntry {

	schemaSet := utils.NewIncludeSet([]string{})
	if len(includeSchemas) > 0 {
		schemaSet = utils.NewIncludeSet(includeSchemas)
	} else if len(excludeSchemas) > 0 {
		schemaSet = utils.NewExcludeSet(excludeSchemas)
	}

	tableSet := utils.NewIncludeSet([]string{})
	if len(includeTableFQNs) > 0 {
		includeTableFQNs = append(includeTableFQNs, getLeafPartitions(includeTableFQNs, toc.DataEntries)...)
		tableSet = utils.NewIncludeSet(includeTableFQNs)
	} else if len(excludeTableFQNs) > 0 {
		excludeTableFQNs = append(excludeTableFQNs, getLeafPartitions(excludeTableFQNs, toc.DataEntries)...)
		tableSet = utils.NewExcludeSet(excludeTableFQNs)
	}

	restorePlanTableSet := utils.NewSet(restorePlanTableFQNs)

	matchingEntries := make([]CoordinatorDataEntry, 0)
	for _, entry := range toc.DataEntries {
		tableFQN := utils.MakeFQN(entry.Schema, entry.Name)

		validSchema := schemaSet.MatchesFilter(entry.Schema)
		validRestorePlan := restorePlanTableSet.MatchesFilter(tableFQN)
		validTable := tableSet.MatchesFilter(tableFQN)
		if validRestorePlan && validSchema && validTable {
			matchingEntries = append(matchingEntries, entry)
		}
	}
	return matchingEntries
}

func SubstituteRedirectDatabaseInStatements(statements []StatementWithType, oldQuotedName string, newQuotedName string) []StatementWithType {
	shouldReplace := map[string]bool{OBJ_DATABASE_GUC: true, OBJ_DATABASE: true, OBJ_DATABASE_METADATA: true}
	pattern := regexp.MustCompile(fmt.Sprintf("DATABASE %s(;| OWNER| SET| TO| FROM| IS| TEMPLATE)", regexp.QuoteMeta(oldQuotedName)))
	for i := range statements {
		if shouldReplace[statements[i].ObjectType] {
			statements[i].Statement = pattern.ReplaceAllString(statements[i].Statement, fmt.Sprintf("DATABASE %s$1", newQuotedName))
		}
	}
	return statements
}

func RemoveActiveRole(activeUser string, statements []StatementWithType) []StatementWithType {
	newStatements := make([]StatementWithType, 0)
	for _, statement := range statements {
		if statement.ObjectType == OBJ_ROLE && statement.Name == activeUser {
			continue
		}
		newStatements = append(newStatements, statement)
	}
	return newStatements
}

func (toc *TOC) InitializeMetadataEntryMap() {
	toc.metadataEntryMap = make(map[string]*[]MetadataEntry, 4)
	toc.metadataEntryMap["global"] = &toc.GlobalEntries
	toc.metadataEntryMap["predata"] = &toc.PredataEntries
	toc.metadataEntryMap["postdata"] = &toc.PostdataEntries
	toc.metadataEntryMap["statistics"] = &toc.StatisticsEntries
}

type TOCObject interface {
	GetMetadataEntry() (string, MetadataEntry)
}

type TOCObjectWithMetadata interface {
	GetMetadataEntry() (string, MetadataEntry)
	FQN() string
}

func (toc *TOC) AddMetadataEntry(section string, entry MetadataEntry, start, end uint64, tier []uint32) {
	entry.StartByte = start
	entry.EndByte = end
	entry.Tier = tier
	*toc.metadataEntryMap[section] = append(*toc.metadataEntryMap[section], entry)
}

func (toc *TOC) AddCoordinatorDataEntry(schema string, name string, oid uint32, attributeString string, rowsCopied int64, PartitionRoot string, distPolicy string) {
	isReplicated := strings.Contains(distPolicy, "REPLICATED")
	toc.DataEntries = append(toc.DataEntries, CoordinatorDataEntry{schema, name, oid, attributeString, rowsCopied, PartitionRoot, isReplicated})
}

func (toc *SegmentTOC) AddSegmentDataEntry(oid uint, startByte uint64, endByte uint64) {
	// We use uint for oid since the flags package does not have a uint32 flag
	toc.DataEntries[oid] = SegmentDataEntry{startByte, endByte}
}
