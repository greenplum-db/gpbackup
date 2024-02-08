package backup

/*
 * This file contains structs and functions related to executing specific
 * queries to gather metadata for the objects handled in predata_externals.go.
 */

import (
	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpbackup/toc"
)

func GetExternalTableDefinitions(connectionPool *dbconn.DBConn) map[uint32]ExternalTableDefinition {
	gplog.Verbose("Retrieving external table information")

	// In GPDB 5+, error tables were removed but internal error logging
	// is still available using `LOG ERRORS`with an optional `PERSISTENTLY`
	// syntax to persist the error logs. The `PERSISTENTLY` part is stored
	// in the pg_exttable.options array.
	version5Query := `
	SELECT e.reloid AS oid,
		CASE WHEN e.urilocation IS NOT NULL THEN unnest(e.urilocation) ELSE '' END AS location,
		array_to_string(e.execlocation, ',') AS execlocation,
		e.fmttype AS formattype,
		e.fmtopts AS formatopts,
		coalesce(e.command, '') AS command,
		coalesce(e.rejectlimit, 0) AS rejectlimit,
		coalesce(e.rejectlimittype, '') AS rejectlimittype,
		e.fmterrtbl IS NOT NULL AND e.reloid = e.fmterrtbl AS logerrors,
		'error_log_persistent=true' = any(e.options) AS logerrpersist,
		pg_encoding_to_char(e.encoding) AS encoding,
		e.writable
	FROM pg_exttable e`

	// In GPDB 6, the logerrors field was added directly onto pg_exttable,
	// so it is no longer necessary to derive it using the fmterrtable field
	version6Query := `
	SELECT e.reloid AS oid,
		CASE WHEN e.urilocation IS NOT NULL THEN unnest(urilocation) ELSE '' END AS location,
		array_to_string(e.execlocation, ',') AS execlocation,
		e.fmttype AS formattype,
		e.fmtopts AS formatopts,
		coalesce(e.command, '') AS command,
		coalesce(e.rejectlimit, 0) AS rejectlimit,
		coalesce(e.rejectlimittype, '') AS rejectlimittype,
		e.logerrors,
		'error_log_persistent=true' = any(e.options) AS logerrpersist,
		pg_encoding_to_char(e.encoding) AS encoding,
		e.writable
	FROM pg_exttable e`

	// Cannot use unnest() in CASE statements anymore in GPDB 7+ so convert
	// it to a LEFT JOIN LATERAL. We do not use LEFT JOIN LATERAL for GPDB 6
	// because the CASE unnest() logic is more performant.
	atLeast7Query := `
	SELECT e.reloid AS oid,
		ljl_unnest AS location,
		array_to_string(e.execlocation, ',') AS execlocation,
		e.fmttype AS formattype,
		e.fmtopts AS formatopts,
		coalesce(e.command, '') AS command,
		coalesce(e.rejectlimit, 0) AS rejectlimit,
		coalesce(e.rejectlimittype, '') AS rejectlimittype,
		e.logerrors,
		coalesce('log_errors=persistently' = any(ft.ftoptions), false) AS logerrpersist,
		pg_encoding_to_char(e.encoding) AS encoding,
		e.writable
	FROM pg_exttable e
		LEFT JOIN pg_foreign_table ft ON e.reloid = ft.ftrelid
		LEFT JOIN LATERAL unnest(urilocation) ljl_unnest ON urilocation IS NOT NULL`

	var query string
	if connectionPool.Version.Is("5") {
		query = version5Query
	} else if connectionPool.Version.Is("6") {
		query = version6Query
	} else if connectionPool.Version.AtLeast("7") {
		query = atLeast7Query
	}

	results := make([]ExternalTableDefinition, 0)
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)
	resultMap := make(map[uint32]ExternalTableDefinition)
	var extTableDef ExternalTableDefinition
	for _, result := range results {
		if resultMap[result.Oid].Oid != 0 {
			extTableDef = resultMap[result.Oid]
		} else {
			extTableDef = result
		}
		if result.Location.Valid && result.Location.String != "" {
			extTableDef.URIs = append(extTableDef.URIs, result.Location.String)
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

func (p ExternalProtocol) GetMetadataEntry() (string, toc.MetadataEntry) {
	return "predata",
		toc.MetadataEntry{
			Schema:          "",
			Name:            p.Name,
			ObjectType:      toc.OBJ_PROTOCOL,
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
		quote_ident(p.ptcname) AS name,
		pg_get_userbyid(p.ptcowner) AS owner,
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

func (pi PartitionInfo) GetMetadataEntry() (string, toc.MetadataEntry) {
	return "predata",
		toc.MetadataEntry{
			Schema:          pi.ParentSchema,
			Name:            pi.ParentRelationName,
			ObjectType:      "EXCHANGE PARTITION",
			ReferenceObject: "",
			StartByte:       0,
			EndByte:         0,
		}
}

func GetExternalPartitionInfo(connectionPool *dbconn.DBConn) ([]PartitionInfo, map[uint32]PartitionInfo) {
	// For GPDB 7+, external partitions will have their own ATTACH PARTITION DDL command
	// instead of a complicated EXCHANGE PARTITION command.
	if connectionPool.Version.AtLeast("7") {
		return []PartitionInfo{}, make(map[uint32]PartitionInfo, 0)
	}

	results := make([]PartitionInfo, 0)
	query := `
	SELECT pr1.oid AS partitionruleoid,
		pr1.parparentrule AS partitionparentruleoid,
		cl.oid AS parentrelationoid,
		quote_ident(n.nspname) AS parentschema,
		quote_ident(cl.relname) AS parentrelationname,
		pr1.parchildrelid AS relationoid,
		CASE WHEN pr1.parname = '' THEN '' ELSE quote_ident(pr1.parname) END AS partitionname,
		CASE WHEN pp.parkind <> 'r'::"char" OR pr1.parisdefault THEN 0
			ELSE pg_catalog.rank() OVER (PARTITION BY pp.oid, cl.relname, pp.parlevel, cl3.relname
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
