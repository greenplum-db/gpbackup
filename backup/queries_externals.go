package backup

/*
 * This file contains structs and functions related to executing specific
 * queries to gather metadata for the objects handled in predata_externals.go.
 */

import (
	"fmt"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpbackup/utils"
)

func GetExternalTableDefinitions(connectionPool *dbconn.DBConn) map[uint32]ExternalTableDefinition {
	gplog.Verbose("Retrieving external table information")
	execOptions := "'ALL_SEGMENTS', 'HOST', 'MASTER_ONLY', 'PER_HOST', 'SEGMENT_ID', 'TOTAL_SEGS'"
	version4query := fmt.Sprintf(`
	SELECT reloid AS oid,
		CASE WHEN SPLIT_PART(location[1], ':', 1) NOT IN (%s) THEN UNNEST(location) ELSE '' END AS location,
		CASE WHEN SPLIT_PART(location[1], ':', 1) IN (%s) THEN UNNEST(location) ELSE 'ALL_SEGMENTS' END AS execlocation,
		fmttype AS formattype,
		fmtopts AS formatopts,
		'' AS options,
		COALESCE(command, '') AS command,
		COALESCE(rejectlimit, 0) AS rejectlimit,
		COALESCE(rejectlimittype, '') AS rejectlimittype,
		COALESCE(QUOTE_IDENT(c.relname),'') AS errtablename,
		COALESCE((SELECT QUOTE_IDENT(nspname) FROM pg_namespace n WHERE n.oid = c.relnamespace), '') AS errtableschema,
		PG_ENCODING_TO_CHAR(encoding) AS encoding,
		writable
	FROM pg_exttable e
		LEFT JOIN pg_class c ON e.fmterrtbl = c.oid`, execOptions, execOptions)

	version5query := `
	SELECT reloid AS oid,
		CASE WHEN urilocation IS NOT NULL THEN UNNEST(urilocation) ELSE '' END AS location,
		ARRAY_TO_STRING(execlocation, ',') AS execlocation,
		fmttype AS formattype,
		fmtopts AS formatopts,
		ARRAY_TO_STRING(ARRAY(SELECT pg_catalog.QUOTE_IDENT(option_name) || ' ' || pg_catalog.QUOTE_LITERAL(option_value)
			FROM pg_options_to_table(options) ORDER BY option_name), E',\n\t') AS options,
		COALESCE(command, '') AS command,
		COALESCE(rejectlimit, 0) AS rejectlimit,
		COALESCE(rejectlimittype, '') AS rejectlimittype,
		COALESCE(QUOTE_IDENT(c.relname),'') AS errtablename,
		COALESCE((SELECT QUOTE_IDENT(nspname) FROM pg_namespace n WHERE n.oid = c.relnamespace), '') AS errtableschema,
		PG_ENCODING_TO_CHAR(encoding) AS encoding,
		writable
	FROM pg_exttable e
		LEFT JOIN pg_class c ON e.fmterrtbl = c.oid`

	query := `
	SELECT reloid AS oid,
		CASE WHEN urilocation IS NOT NULL THEN UNNEST(urilocation) ELSE '' END AS location,
		ARRAY_TO_STRING(execlocation, ',') AS execlocation,
		fmttype AS formattype,
		fmtopts AS formatopts,
		ARRAY_TO_STRING(ARRAY(SELECT pg_catalog.QUOTE_IDENT(option_name) || ' ' || pg_catalog.QUOTE_LITERAL(option_value)
			FROM pg_options_to_table(options) ORDER BY option_name), E',\n\t') AS options,
		COALESCE(command, '') AS command,
		COALESCE(rejectlimit, 0) AS rejectlimit,
		COALESCE(rejectlimittype, '') AS rejectlimittype,
		CASE WHEN logerrors = 'false' THEN '' ELSE QUOTE_IDENT(c.relname) END AS errtablename,
		CASE WHEN logerrors = 'false' THEN '' ELSE COALESCE((SELECT QUOTE_IDENT(nspname) FROM pg_namespace n WHERE n.oid = c.relnamespace), '') END AS errtableschema,
		PG_ENCODING_TO_CHAR(encoding) AS encoding,
		writable
	FROM pg_exttable e
		LEFT JOIN pg_class c ON e.reloid = c.oid`

	results := make([]ExternalTableDefinition, 0)
	var err error
	if connectionPool.Version.Before("5") {
		err = connectionPool.Select(&results, version4query)
	} else if connectionPool.Version.Before("6") {
		err = connectionPool.Select(&results, version5query)
	} else {
		err = connectionPool.Select(&results, query)
	}
	gplog.FatalOnError(err)
	resultMap := make(map[uint32]ExternalTableDefinition)
	var extTableDef ExternalTableDefinition
	for _, result := range results {
		if resultMap[result.Oid].Oid != 0 {
			extTableDef = resultMap[result.Oid]
		} else {
			extTableDef = result
		}
		if result.Location != "" {
			extTableDef.URIs = append(extTableDef.URIs, result.Location)
		}
		resultMap[result.Oid] = extTableDef
	}
	return resultMap
}

type ExternalProtocol struct {
	Oid           uint32
	Name          string
	Owner         string
	Trusted       bool   `db:"ptctrusted"`
	ReadFunction  uint32 `db:"ptcreadfn"`
	WriteFunction uint32 `db:"ptcwritefn"`
	Validator     uint32 `db:"ptcvalidatorfn"`
}

func (p ExternalProtocol) GetMetadataEntry() (string, utils.MetadataEntry) {
	return "predata",
		utils.MetadataEntry{
			Schema:          "",
			Name:            p.Name,
			ObjectType:      "PROTOCOL",
			ReferenceObject: "",
			StartByte:       0,
			EndByte:         0,
		}
}

func (p ExternalProtocol) GetUniqueID() UniqueID {
	return UniqueID{ClassID: PG_EXTPROTOCOL_OID, Oid: p.Oid}
}

func (p ExternalProtocol) FQN() string {
	return p.Name
}

func GetExternalProtocols(connectionPool *dbconn.DBConn) []ExternalProtocol {
	results := make([]ExternalProtocol, 0)
	query := `
	SELECT p.oid,
		QUOTE_IDENT(p.ptcname) AS name,
		PG_GET_USERBYID(p.ptcowner) AS owner,
		p.ptctrusted,
		p.ptcreadfn,
		p.ptcwritefn,
		p.ptcvalidatorfn
	FROM pg_extprotocol p`
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)
	return results
}

type PartitionInfo struct {
	PartitionRuleOid       uint32
	PartitionParentRuleOid uint32
	ParentRelationOid      uint32
	ParentSchema           string
	ParentRelationName     string
	RelationOid            uint32
	PartitionName          string
	PartitionRank          int
	IsExternal             bool
}

func (pi PartitionInfo) GetMetadataEntry() (string, utils.MetadataEntry) {
	return "predata",
		utils.MetadataEntry{
			Schema:          pi.ParentSchema,
			Name:            pi.ParentRelationName,
			ObjectType:      "EXCHANGE PARTITION",
			ReferenceObject: "",
			StartByte:       0,
			EndByte:         0,
		}
}

func GetExternalPartitionInfo(connectionPool *dbconn.DBConn) ([]PartitionInfo, map[uint32]PartitionInfo) {
	results := make([]PartitionInfo, 0)
	query := `
	SELECT pr1.oid AS partitionruleoid,
		pr1.parparentrule AS partitionparentruleoid,
		cl.oid AS parentrelationoid,
		QUOTE_IDENT(n.nspname) AS parentschema,
		QUOTE_IDENT(cl.relname) AS parentrelationname,
		pr1.parchildrelid AS relationoid,
		CASE WHEN pr1.parname = '' THEN '' ELSE QUOTE_IDENT(pr1.parname) END AS partitionname,
		CASE WHEN pp.parkind <> 'r'::"char" OR pr1.parisdefault THEN 0
			ELSE pg_catalog.RANK() OVER (PARTITION BY pp.oid, cl.relname, pp.parlevel, cl3.relname
				ORDER BY pr1.parisdefault, pr1.parruleord) END AS partitionrank,
		CASE WHEN e.reloid IS NOT NULL then 't' ELSE 'f' END AS isexternal
	FROM pg_namespace n, pg_namespace n2, pg_class cl
		LEFT JOIN pg_tablespace sp ON cl.reltablespace = sp.oid, pg_class cl2
		LEFT JOIN pg_tablespace sp3 ON cl2.reltablespace = sp3.oid, pg_partition pp, pg_partition_rule pr1
		LEFT JOIN pg_partition_rule pr2 ON pr1.parparentrule = pr2.oid
		LEFT JOIN pg_class cl3 ON pr2.parchildrelid = cl3.oid
		LEFT JOIN pg_exttable e ON e.reloid = pr1.parchildrelid
	WHERE pp.paristemplate = false
		AND pp.parrelid = cl.oid
		AND pr1.paroid = pp.oid
		AND cl2.oid = pr1.parchildrelid
		AND cl.relnamespace = n.oid
		AND cl2.relnamespace = n2.oid`
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)

	extPartitions := make([]PartitionInfo, 0)
	partInfoMap := make(map[uint32]PartitionInfo, len(results))
	for _, partInfo := range results {
		if partInfo.IsExternal {
			extPartitions = append(extPartitions, partInfo)
		}
		partInfoMap[partInfo.PartitionRuleOid] = partInfo
	}

	return extPartitions, partInfoMap
}
