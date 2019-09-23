package utils

import (
	"fmt"
	"io"
	"os"
	"regexp"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"gopkg.in/yaml.v2"
)

type TOC struct {
	metadataEntryMap    map[string]*[]MetadataEntry
	GlobalEntries       []MetadataEntry
	PredataEntries      []MetadataEntry
	PostdataEntries     []MetadataEntry
	StatisticsEntries   []MetadataEntry
	DataEntries         []MasterDataEntry
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
}

type MasterDataEntry struct {
	Schema          string
	Name            string
	Oid             uint32
	AttributeString string
	RowsCopied      int64
	PartitionRoot   string
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

func NewTOC(filename string) *TOC {
	toc := &TOC{}
	contents, err := operating.System.ReadFile(filename)
	gplog.FatalOnError(err)
	err = yaml.Unmarshal(contents, toc)
	gplog.FatalOnError(err)
	return toc
}

func NewSegmentTOC(filename string) *SegmentTOC {
	toc := &SegmentTOC{}
	contents, err := operating.System.ReadFile(filename)
	gplog.FatalOnError(err)
	err = yaml.Unmarshal(contents, toc)
	gplog.FatalOnError(err)
	return toc
}

func (toc *TOC) WriteToFileAndMakeReadOnly(filename string) {
	tocContents, err := yaml.Marshal(toc)
	gplog.FatalOnError(err)

	tocFile, err := os.OpenFile(filename, os.O_CREATE | os.O_EXCL | os.O_WRONLY, 0644)
	gplog.FatalOnError(err)

	_, err = tocFile.Write(tocContents)
	gplog.FatalOnError(err)

	err = tocFile.Sync()
	gplog.FatalOnError(err)

	err = tocFile.Close()
	gplog.FatalOnError(err)

	err = os.Chmod(filename, 0444)
	gplog.FatalOnError(err)
}

//This function return an error rather than Fataling because it is called by the helper
func (toc *SegmentTOC) WriteToFileAndMakeReadOnly(filename string) error {
	tocContents, err := yaml.Marshal(toc)
	if err != nil {
		return err
	}

	tocFile, err := os.OpenFile(filename, os.O_CREATE | os.O_EXCL | os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	_, err = tocFile.Write(tocContents)
	if err != nil {
		return err
	}

	err = tocFile.Sync()
	if err != nil {
		return err
	}

	err = tocFile.Close()
	if err != nil {
		return err
	}

	err = os.Chmod(filename, 0444)
	return err
}

type StatementWithType struct {
	Schema          string
	Name            string
	ObjectType      string
	ReferenceObject string
	Statement       string
}

func GetIncludedPartitionRoots(tocDataEntries []MasterDataEntry, includeRelations []string) []string {
	if len(includeRelations) == 0 {
		return []string{}
	}
	rootPartitions := make([]string, 0)

	FQNToPartitionRoot := make(map[string]string)
	for _, entry := range tocDataEntries {
		if entry.PartitionRoot != "" {
			FQNToPartitionRoot[MakeFQN(entry.Schema, entry.Name)] = MakeFQN(entry.Schema, entry.PartitionRoot)
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
			statements = append(statements, StatementWithType{Schema: entry.Schema, Name: entry.Name, ObjectType: entry.ObjectType, ReferenceObject: entry.ReferenceObject, Statement: string(contents)})
		}
	}
	return statements
}

func constructFilterSets(includeObjectTypes []string, excludeObjectTypes []string, includeSchemas []string, excludeSchemas []string, includeRelations []string, excludeRelations []string) (*FilterSet, *FilterSet, *FilterSet) {
	var objectSet, schemaSet, relationSet *FilterSet
	if len(includeObjectTypes) > 0 {
		objectSet = NewIncludeSet(includeObjectTypes)
	} else {
		objectSet = NewExcludeSet(excludeObjectTypes)
	}
	if len(includeSchemas) > 0 {
		schemaSet = NewIncludeSet(includeSchemas)
	} else {
		schemaSet = NewExcludeSet(excludeSchemas)
	}
	if len(includeRelations) > 0 {
		relationSet = NewIncludeSet(includeRelations)
	} else {
		relationSet = NewExcludeSet(excludeRelations)
	}
	return objectSet, schemaSet, relationSet
}

func shouldIncludeStatement(entry MetadataEntry, objectSet *FilterSet, schemaSet *FilterSet, relationSet *FilterSet) bool {
	shouldIncludeObject := objectSet.MatchesFilter(entry.ObjectType)
	shouldIncludeSchema := schemaSet.MatchesFilter(entry.Schema)
	relationFQN := MakeFQN(entry.Schema, entry.Name)
	shouldIncludeRelation := (relationSet.IsExclude && entry.ObjectType != "TABLE" && entry.ObjectType != "VIEW" && entry.ObjectType != "SEQUENCE" && entry.ReferenceObject == "") ||
		((entry.ObjectType == "TABLE" || entry.ObjectType == "VIEW" || entry.ObjectType == "SEQUENCE") && relationSet.MatchesFilter(relationFQN) && entry.ReferenceObject == "") || // Relations should match the filter
		(entry.ObjectType != "SEQUENCE OWNER" && entry.ReferenceObject != "" && relationSet.MatchesFilter(entry.ReferenceObject)) || // Include relations that filtered tables depend on
		(entry.ObjectType == "SEQUENCE OWNER" && relationSet.MatchesFilter(relationFQN) && relationSet.MatchesFilter(entry.ReferenceObject)) //Include sequence owners if both table and sequence are being restored

	return shouldIncludeObject && shouldIncludeSchema && shouldIncludeRelation
}

func getLeafPartitions(tableFQNs []string, tocDataEntries []MasterDataEntry) (leafPartitions []string) {
	tableSet := NewSet(tableFQNs)

	for _, entry := range tocDataEntries {
		if entry.PartitionRoot == "" {
			continue
		}

		parentFQN := MakeFQN(entry.Schema, entry.PartitionRoot)
		if tableSet.MatchesFilter(parentFQN) {
			leafPartitions = append(leafPartitions, MakeFQN(entry.Schema, entry.Name))
		}
	}

	return leafPartitions
}

func (toc *TOC) GetDataEntriesMatching(includeSchemas []string, excludeSchemas []string,
	includeTableFQNs []string, excludeTableFQNs []string, restorePlanTableFQNs []string) []MasterDataEntry {

	schemaSet := NewIncludeSet([]string{})
	if len(includeSchemas) > 0 {
		schemaSet = NewIncludeSet(includeSchemas)
	} else if len(excludeSchemas) > 0 {
		schemaSet = NewExcludeSet(excludeSchemas)
	}

	tableSet := NewIncludeSet([]string{})
	if len(includeTableFQNs) > 0 {
		includeTableFQNs = append(includeTableFQNs, getLeafPartitions(includeTableFQNs, toc.DataEntries)...)
		tableSet = NewIncludeSet(includeTableFQNs)
	} else if len(excludeTableFQNs) > 0 {
		excludeTableFQNs = append(excludeTableFQNs, getLeafPartitions(excludeTableFQNs, toc.DataEntries)...)
		tableSet = NewExcludeSet(excludeTableFQNs)
	}

	restorePlanTableSet := NewSet(restorePlanTableFQNs)

	matchingEntries := make([]MasterDataEntry, 0)
	for _, entry := range toc.DataEntries {
		tableFQN := MakeFQN(entry.Schema, entry.Name)

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
	shouldReplace := map[string]bool{"DATABASE GUC": true, "DATABASE": true, "DATABASE METADATA": true}
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
		if statement.ObjectType == "ROLE" && statement.Name == activeUser {
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

func (toc *TOC) AddMetadataEntry(section string, entry MetadataEntry, start, end uint64) {
	entry.StartByte = start
	entry.EndByte = end
	*toc.metadataEntryMap[section] = append(*toc.metadataEntryMap[section], entry)
}

func (toc *TOC) AddMasterDataEntry(schema string, name string, oid uint32, attributeString string, rowsCopied int64, PartitionRoot string) {
	toc.DataEntries = append(toc.DataEntries, MasterDataEntry{schema, name, oid, attributeString, rowsCopied, PartitionRoot})
}

func (toc *SegmentTOC) AddSegmentDataEntry(oid uint, startByte uint64, endByte uint64) {
	// We use uint for oid since the flags package does not have a uint32 flag
	toc.DataEntries[oid] = SegmentDataEntry{startByte, endByte}
}
