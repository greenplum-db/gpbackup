package backup

/*
 * This file contains structs and functions related to backing up metadata on the
 * master for objects that connect to external data (external tables and external
 * protocols).
 */

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/greenplum-db/gpbackup/utils"
)

const (
	// Type of external table
	READABLE = iota
	READABLE_WEB
	WRITABLE
	WRITABLE_WEB
	// Protocol external table is using
	FILE
	GPFDIST
	GPHDFS
	HTTP
	S3
)

type ExternalTableDefinition struct {
	Oid             uint32
	Type            int
	Protocol        int
	Location        string
	ExecLocation    string
	FormatType      string
	FormatOpts      string
	Options         string
	Command         string
	RejectLimit     int
	RejectLimitType string
	ErrTableName    string
	ErrTableSchema  string
	Encoding        string
	Writable        bool
	URIs            []string
}

func PrintExternalTableCreateStatement(metadataFile *utils.FileWithByteCount, toc *utils.TOC, table Relation, tableDef TableDefinition) {
	start := metadataFile.ByteCount
	tableTypeStrMap := map[int]string{
		READABLE:     "READABLE EXTERNAL",
		READABLE_WEB: "READABLE EXTERNAL WEB",
		WRITABLE:     "WRITABLE EXTERNAL",
		WRITABLE_WEB: "WRITABLE EXTERNAL WEB",
	}
	extTableDef := tableDef.ExtTableDef
	extTableDef.Type, extTableDef.Protocol = DetermineExternalTableCharacteristics(extTableDef)
	metadataFile.MustPrintf("\n\nCREATE %s TABLE %s (\n", tableTypeStrMap[extTableDef.Type], table.FQN())
	printColumnDefinitions(metadataFile, tableDef.ColumnDefs, "")
	metadataFile.MustPrintf(") ")
	PrintExternalTableStatements(metadataFile, table, extTableDef)
	if extTableDef.Writable {
		metadataFile.MustPrintf("\n%s", tableDef.DistPolicy)
	}
	metadataFile.MustPrintf(";")
	if toc != nil {
		toc.AddPredataEntry(table.Schema, table.Name, "TABLE", "", start, metadataFile)
	}
}

func DetermineExternalTableCharacteristics(extTableDef ExternalTableDefinition) (int, int) {
	isWritable := extTableDef.Writable
	tableType := -1
	tableProtocol := -1
	if extTableDef.Location == "" { // EXTERNAL WEB tables may have EXECUTE instead of LOCATION
		tableProtocol = HTTP
		if isWritable {
			tableType = WRITABLE_WEB
		} else {
			tableType = READABLE_WEB
		}
	} else {
		/*
		 * All data sources must use the same protocol, so we can use Location to determine
		 * the table's protocol even though it only holds one data source URI.
		 */
		isWeb := strings.HasPrefix(extTableDef.Location, "http")
		if isWeb && isWritable {
			tableType = WRITABLE_WEB
		} else if isWeb && !isWritable {
			tableType = READABLE_WEB
		} else if !isWeb && isWritable {
			tableType = WRITABLE
		} else {
			tableType = READABLE
		}
		prefix := extTableDef.Location[0:strings.Index(extTableDef.Location, "://")]
		switch prefix {
		case "file":
			tableProtocol = FILE
		case "gpfdist":
			tableProtocol = GPFDIST
		case "gpfdists":
			tableProtocol = GPFDIST
		case "gphdfs":
			tableProtocol = GPHDFS
		case "http":
			tableProtocol = HTTP
		case "https":
			tableProtocol = HTTP
		case "s3":
			tableProtocol = S3
		}
	}
	return tableType, tableProtocol
}

func PrintExternalTableStatements(metadataFile *utils.FileWithByteCount, table Relation, extTableDef ExternalTableDefinition) {
	if extTableDef.Type != WRITABLE_WEB {
		if len(extTableDef.URIs) > 0 {
			metadataFile.MustPrintf("LOCATION (\n\t'%s'\n)", strings.Join(extTableDef.URIs, "',\n\t'"))
		}
	}
	if extTableDef.Type == READABLE || (extTableDef.Type == WRITABLE_WEB && extTableDef.Protocol == S3) {
		if extTableDef.ExecLocation == "MASTER_ONLY" {
			metadataFile.MustPrintf(" ON MASTER")
		}
	}
	if extTableDef.Type == READABLE_WEB || extTableDef.Type == WRITABLE_WEB {
		if extTableDef.Command != "" {
			extTableDef.Command = strings.Replace(extTableDef.Command, `'`, `''`, -1)
			metadataFile.MustPrintf("EXECUTE '%s'", extTableDef.Command)
			execType := strings.Split(extTableDef.ExecLocation, ":")
			switch execType[0] {
			case "ALL_SEGMENTS": // Default case, don't print anything else
			case "HOST":
				metadataFile.MustPrintf(" ON HOST '%s'", execType[1])
			case "MASTER_ONLY":
				metadataFile.MustPrintf(" ON MASTER")
			case "PER_HOST":
				metadataFile.MustPrintf(" ON HOST")
			case "SEGMENT_ID":
				metadataFile.MustPrintf(" ON SEGMENT %s", execType[1])
			case "TOTAL_SEGS":
				metadataFile.MustPrintf(" ON %s", execType[1])
			}
		}
	}
	metadataFile.MustPrintln()
	formatType := ""
	switch extTableDef.FormatType {
	case "a":
		formatType = "avro"
	case "b":
		formatType = "custom"
	case "c":
		formatType = "csv"
	case "p":
		formatType = "parquet"
	case "t":
		formatType = "text"
	}
	metadataFile.MustPrintf("FORMAT '%s'", formatType)
	if extTableDef.FormatOpts != "" {
		formatStr := extTableDef.FormatOpts
		if formatType == "custom" {
			/*
			 * The options for the custom format are stored in an invalid format, so we
			 * need to reformat them before printing.
			 *
			 * The below regular expression performs a single-line non-greedy match on tokens
			 * in the format "key 'value'", so we don't need to manually escape single quotes.
			 */
			reformat := regexp.MustCompile(`(\w+) ((?sU:'.*')|(?s:[^ ]+)) ?`)
			formatStr = reformat.ReplaceAllString(formatStr, `$1 = $2, `)
			fLen := len(formatStr)
			if formatStr[fLen-2:fLen] == ", " {
				formatStr = formatStr[:fLen-2]
			}
		}
		metadataFile.MustPrintf(" (%s)", formatStr)
	}
	metadataFile.MustPrintln()
	if extTableDef.Options != "" {
		metadataFile.MustPrintf("OPTIONS (\n\t%s\n)\n", extTableDef.Options)
	}
	metadataFile.MustPrintf("ENCODING '%s'", extTableDef.Encoding)
	if extTableDef.Type == READABLE || extTableDef.Type == READABLE_WEB {
		/*
		 * If an external table is created using LOG ERRORS instead of LOG ERRORS INTO [tablename],
		 * the value of pg_exttable.fmterrtbl will match the table's own name.
		 */
		errTableFQN := utils.MakeFQN(extTableDef.ErrTableSchema, extTableDef.ErrTableName)
		if errTableFQN == table.FQN() {
			metadataFile.MustPrintf("\nLOG ERRORS")
		} else if extTableDef.ErrTableName != "" {
			metadataFile.MustPrintf("\nLOG ERRORS INTO %s", errTableFQN)
		}
		if extTableDef.RejectLimit != 0 {
			metadataFile.MustPrintf("\nSEGMENT REJECT LIMIT %d ", extTableDef.RejectLimit)
			switch extTableDef.RejectLimitType {
			case "r":
				metadataFile.MustPrintf("ROWS")
			case "p":
				metadataFile.MustPrintf("PERCENT")
			}
		}
	}
}

func ProcessProtocols(protocols []ExternalProtocol, funcInfoMap map[uint32]FunctionInfo) []ExternalProtocol {
	protocolsToBackup := make([]ExternalProtocol, 0, len(protocols))
	for _, p := range protocols {
		p.FuncMap = make(map[uint32]string)
		funcOidList := []uint32{p.ReadFunction, p.WriteFunction, p.Validator}
		hasUserDefinedFunc := false
		for _, funcOid := range funcOidList {
			if funcInfo, hasFunction := funcInfoMap[funcOid]; hasFunction {
				if !funcInfo.IsInternal {
					hasUserDefinedFunc = true
				}
				dependencyStr := fmt.Sprintf("%s(%s)", funcInfo.QualifiedName, funcInfo.Arguments)
				p.DependsUpon = append(p.DependsUpon, dependencyStr)
				p.FuncMap[funcOid] = funcInfo.QualifiedName
			}
		}
		if hasUserDefinedFunc {
			protocolsToBackup = append(protocolsToBackup, p)
		}
	}
	return protocolsToBackup
}

func PrintCreateExternalProtocolStatement(metadataFile *utils.FileWithByteCount, toc *utils.TOC, protocol ExternalProtocol, protoMetadata ObjectMetadata) {
	start := metadataFile.ByteCount
	protocolFunctions := []string{}
	if protocol.ReadFunction != 0 {
		protocolFunctions = append(protocolFunctions, fmt.Sprintf("readfunc = %s", protocol.FuncMap[protocol.ReadFunction]))
	}
	if protocol.WriteFunction != 0 {
		protocolFunctions = append(protocolFunctions, fmt.Sprintf("writefunc = %s", protocol.FuncMap[protocol.WriteFunction]))
	}
	if protocol.Validator != 0 {
		protocolFunctions = append(protocolFunctions, fmt.Sprintf("validatorfunc = %s", protocol.FuncMap[protocol.Validator]))
	}

	metadataFile.MustPrintf("\n\nCREATE ")
	if protocol.Trusted {
		metadataFile.MustPrintf("TRUSTED ")
	}
	metadataFile.MustPrintf("PROTOCOL %s (%s);\n", protocol.Name, strings.Join(protocolFunctions, ", "))
	PrintObjectMetadata(metadataFile, protoMetadata, protocol.Name, "PROTOCOL")
	toc.AddPredataEntry("", protocol.Name, "PROTOCOL", "", start, metadataFile)
}

func PrintExchangeExternalPartitionStatements(metadataFile *utils.FileWithByteCount, toc *utils.TOC, extPartitions []PartitionInfo, partInfoMap map[uint32]PartitionInfo, tables []Relation) {
	tableNameMap := make(map[uint32]string, len(tables))
	for _, table := range tables {
		tableNameMap[table.Oid] = table.FQN()
	}
	for _, externalPartition := range extPartitions {
		extPartRelationName := tableNameMap[externalPartition.RelationOid]
		if extPartRelationName == "" {
			continue //Not included in the list of tables to back up
		}
		parentRelationName := utils.MakeFQN(externalPartition.ParentSchema, externalPartition.ParentRelationName)
		start := metadataFile.ByteCount
		alterPartitionStr := ""
		currentPartition := externalPartition
		for currentPartition.PartitionParentRuleOid != 0 {
			parent := partInfoMap[currentPartition.PartitionParentRuleOid]
			if parent.PartitionName == "" {
				alterPartitionStr = fmt.Sprintf("ALTER PARTITION FOR (RANK(%d)) ", parent.PartitionRank) + alterPartitionStr
			} else {
				alterPartitionStr = fmt.Sprintf("ALTER PARTITION %s ", parent.PartitionName) + alterPartitionStr
			}
			currentPartition = parent
		}
		metadataFile.MustPrintf("\n\nALTER TABLE %s %s", parentRelationName, alterPartitionStr)
		if externalPartition.PartitionName == "" {
			metadataFile.MustPrintf("EXCHANGE PARTITION FOR (RANK(%d)) ", externalPartition.PartitionRank)
		} else {
			metadataFile.MustPrintf("EXCHANGE PARTITION %s ", externalPartition.PartitionName)
		}
		metadataFile.MustPrintf("WITH TABLE %s WITHOUT VALIDATION;", extPartRelationName)
		metadataFile.MustPrintf("\n\nDROP TABLE %s;", extPartRelationName)
		toc.AddPredataEntry(externalPartition.ParentSchema, externalPartition.ParentRelationName, "EXCHANGE PARTITION", "", start, metadataFile)
	}
}
