package backup

/*
 * This file contains structs and functions related to executing specific
 * queries to gather metadata for the objects handled in predata_relations.go.
 */

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpbackup/options"
	"github.com/greenplum-db/gpbackup/toc"
)

type Table struct {
	Relation
	TableDefinition
}

func (t Table) SkipDataBackup() bool {
	def := t.TableDefinition
	return def.IsExternal || (def.ForeignDef != ForeignTableDefinition{})
}

func (t Table) GetMetadataEntry() (string, toc.MetadataEntry) {
	objectType := "TABLE"
	if (t.ForeignDef != ForeignTableDefinition{}) {
		objectType = "FOREIGN TABLE"
	}
	referenceObject := ""
	if t.AttachPartitionInfo != (AttachPartitionInfo{}) {
		referenceObject = t.AttachPartitionInfo.Parent
	}

	return "predata",
		toc.MetadataEntry{
			Schema:          t.Schema,
			Name:            t.Name,
			ObjectType:      objectType,
			ReferenceObject: referenceObject,
			StartByte:       0,
			EndByte:         0,
		}
}

/*
 * Extract all the unique schemas out from a Table array.
 */
func createAlteredPartitionSchemaSet(tables []Table) map[string]bool {
	partitionAlteredSchemas := make(map[string]bool)
	for _, table := range tables {
		for _, alteredPartitionRelation := range table.PartitionAlteredSchemas {
			partitionAlteredSchemas[alteredPartitionRelation.NewSchema] = true
		}
	}

	return partitionAlteredSchemas
}

type TableDefinition struct {
	DistPolicy              string
	PartDef                 string
	PartTemplateDef         string
	StorageOpts             string
	TablespaceName          string
	ColumnDefs              []ColumnDefinition
	IsExternal              bool
	ExtTableDef             ExternalTableDefinition
	PartitionLevelInfo      PartitionLevelInfo
	TableType               string
	IsUnlogged              bool
	ForeignDef              ForeignTableDefinition
	Inherits                []string
	ReplicaIdentity         string
	PartitionAlteredSchemas []AlteredPartitionRelation
	AccessMethodName        string
	PartitionKeyDef         string
	AttachPartitionInfo     AttachPartitionInfo
	ForceRowSecurity        bool
}

/*
 * This function calls all the functions needed to gather the metadata for a
 * single table and assembles the metadata into ColumnDef and TableDef structs
 * for more convenient handling in the PrintCreateTableStatement() function.
 */
func ConstructDefinitionsForTables(connectionPool *dbconn.DBConn, tableRelations []Relation) []Table {
	tables := make([]Table, 0)

	gplog.Info("Gathering additional table metadata")
	columnDefs := GetColumnDefinitions(connectionPool)
	distributionPolicies := GetDistributionPolicies(connectionPool)
	partitionDefs, partTemplateDefs := GetPartitionDetails(connectionPool)
	tablespaceNames, storageOptions := GetTableStorage(connectionPool)
	extTableDefs := GetExternalTableDefinitions(connectionPool)
	partTableMap := GetPartitionTableMap(connectionPool)
	tableTypeMap := GetTableType(connectionPool)
	accessMethodMap := GetTableAccessMethod(connectionPool)
	unloggedTableMap := GetUnloggedTables(connectionPool)
	foreignTableDefs := GetForeignTableDefinitions(connectionPool)
	inheritanceMap := GetTableInheritance(connectionPool, tableRelations)
	replicaIdentityMap := GetTableReplicaIdentity(connectionPool)
	partitionAlteredSchemaMap := GetPartitionAlteredSchema(connectionPool)
	partitionKeyDefs := GetPartitionKeyDefs(connectionPool)
	attachPartitionInfo := GetAttachPartitionInfo(connectionPool)
	forceRowSecurity := GetForceRowSecurity(connectionPool)

	gplog.Verbose("Constructing table definition map")
	for _, tableRel := range tableRelations {
		oid := tableRel.Oid
		tableDef := TableDefinition{
			DistPolicy:              distributionPolicies[oid],
			PartDef:                 partitionDefs[oid],
			PartTemplateDef:         partTemplateDefs[oid],
			StorageOpts:             storageOptions[oid],
			TablespaceName:          tablespaceNames[oid],
			ColumnDefs:              columnDefs[oid],
			IsExternal:              extTableDefs[oid].Oid != 0,
			ExtTableDef:             extTableDefs[oid],
			PartitionLevelInfo:      partTableMap[oid],
			TableType:               tableTypeMap[oid],
			IsUnlogged:              unloggedTableMap[oid],
			ForeignDef:              foreignTableDefs[oid],
			Inherits:                inheritanceMap[oid],
			ReplicaIdentity:         replicaIdentityMap[oid],
			PartitionAlteredSchemas: partitionAlteredSchemaMap[oid],
			AccessMethodName:        accessMethodMap[oid],
			PartitionKeyDef:         partitionKeyDefs[oid],
			AttachPartitionInfo:     attachPartitionInfo[oid],
			ForceRowSecurity:        forceRowSecurity[oid],
		}
		if tableDef.Inherits == nil {
			tableDef.Inherits = []string{}
		}
		tables = append(tables, Table{tableRel, tableDef})
	}
	return tables
}

/*
 * This returns a map of all parent partition tables and leaf partition tables;
 * "p" indicates a parent table, "l" indicates a leaf table, and "i" indicates
 * an intermediate table.
 */

type PartitionLevelInfo struct {
	Oid      uint32
	Level    string
	RootName string
}

func GetPartitionTableMap(connectionPool *dbconn.DBConn) map[uint32]PartitionLevelInfo {
	before7Query := `
		SELECT pc.oid AS oid,
			'p' AS level,
			'' AS rootname
		FROM pg_partition p
			JOIN pg_class pc ON p.parrelid = pc.oid
		UNION ALL
		SELECT r.parchildrelid AS oid,
			CASE WHEN p.parlevel = levels.pl THEN 'l' ELSE 'i' END AS level,
			quote_ident(cparent.relname) AS rootname
		FROM pg_partition p
			JOIN pg_partition_rule r ON p.oid = r.paroid
			JOIN pg_class cparent ON cparent.oid = p.parrelid
			JOIN (SELECT parrelid AS relid, max(parlevel) AS pl
				FROM pg_partition GROUP BY parrelid) AS levels ON p.parrelid = levels.relid
		WHERE r.parchildrelid != 0`

	atLeast7Query := `
		SELECT c.oid,
			CASE WHEN p.partrelid IS NOT NULL AND c.relispartition = false THEN ''
				ELSE rc.relname
			END AS rootname,
			CASE WHEN p.partrelid IS NOT NULL AND c.relispartition = false THEN 'p'
				WHEN p.partrelid IS NOT NULL AND c.relispartition = true THEN 'i'
				ELSE 'l'
			END AS level
		FROM pg_class c
			LEFT JOIN pg_partitioned_table p ON c.oid = p.partrelid
			LEFT JOIN pg_class rc ON pg_partition_root(c.oid) = rc.oid
		WHERE c.relispartition = true OR c.relkind = 'p'`

	query := ""
	if connectionPool.Version.Before("7") {
		query = before7Query
	} else {
		query = atLeast7Query
	}

	results := make([]PartitionLevelInfo, 0)
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)

	resultMap := make(map[uint32]PartitionLevelInfo)
	for _, result := range results {
		resultMap[result.Oid] = result
	}

	return resultMap
}

type ColumnDefinition struct {
	Oid                   uint32 `db:"attrelid"`
	Num                   int    `db:"attnum"`
	Name                  string
	NotNull               bool `db:"attnotnull"`
	HasDefault            bool `db:"atthasdef"`
	Type                  string
	Encoding              string
	StatTarget            int `db:"attstattarget"`
	StorageType           string
	DefaultVal            string
	Comment               string
	Privileges            sql.NullString
	Kind                  string
	Options               string
	FdwOptions            string
	Collation             string
	SecurityLabelProvider string
	SecurityLabel         string
	AttGenerated          string
	IsInherited           bool
}

var storageTypeCodes = map[string]string{
	"e": "EXTERNAL",
	"m": "MAIN",
	"p": "PLAIN",
	"x": "EXTENDED",
}

var attGeneratedCodes = map[string]string{
	"s": "STORED",
}

func GetColumnDefinitions(connectionPool *dbconn.DBConn) map[uint32][]ColumnDefinition {
	// This query is adapted from the getTableAttrs() function in pg_dump.c.
	// Optimize Get column definitions to avoid child partitions
	// Include child partitions that are also external tables
	gplog.Verbose("Getting column definitions")
	results := make([]ColumnDefinition, 0)

	before6Query := fmt.Sprintf(`
	SELECT a.attrelid,
		a.attnum,
		quote_ident(a.attname) AS name,
		a.attnotnull,
		a.atthasdef,
		pg_catalog.format_type(t.oid,a.atttypmod) AS type,
		coalesce(pg_catalog.array_to_string(e.attoptions, ','), '') AS encoding,
		a.attstattarget,
		CASE WHEN a.attstorage != t.typstorage THEN a.attstorage ELSE '' END AS storagetype,
		coalesce('('||pg_catalog.pg_get_expr(ad.adbin, ad.adrelid)||')', '') AS defaultval,
		coalesce(d.description, '') AS comment
	FROM pg_catalog.pg_attribute a
		JOIN pg_class c ON a.attrelid = c.oid
		JOIN pg_namespace n ON c.relnamespace = n.oid
		LEFT JOIN pg_catalog.pg_attrdef ad ON a.attrelid = ad.adrelid AND a.attnum = ad.adnum
		LEFT JOIN pg_catalog.pg_type t ON a.atttypid = t.oid
		LEFT JOIN pg_catalog.pg_attribute_encoding e ON e.attrelid = a.attrelid AND e.attnum = a.attnum
		LEFT JOIN pg_description d ON d.objoid = a.attrelid AND d.classoid = 'pg_class'::regclass AND d.objsubid = a.attnum
	WHERE %s
		AND NOT EXISTS (
			SELECT 1 FROM 
				(SELECT parchildrelid FROM pg_partition_rule EXCEPT SELECT reloid FROM pg_exttable) par 
			WHERE par.parchildrelid = c.oid)
		AND c.reltype <> 0
		AND a.attnum > 0::pg_catalog.int2
		AND a.attisdropped = 'f'
	ORDER BY a.attrelid, a.attnum`, relationAndSchemaFilterClause())

	// GPDB 6 adds multiple additional column attributes and options including fdwoptions and security labels.
	// Add capture logic for all of these.
	version6Query := fmt.Sprintf(`
	SELECT a.attrelid,
		a.attnum,
		quote_ident(a.attname) AS name,
		a.attnotnull,
		a.atthasdef,
		pg_catalog.format_type(t.oid,a.atttypmod) AS type,
		coalesce(pg_catalog.array_to_string(e.attoptions, ','), '') AS encoding,
		a.attstattarget,
		CASE WHEN a.attstorage != t.typstorage THEN a.attstorage ELSE '' END AS storagetype,
		coalesce('('||pg_catalog.pg_get_expr(ad.adbin, ad.adrelid)||')', '') AS defaultval,
		coalesce(d.description, '') AS comment,
		CASE
			WHEN a.attacl IS NULL THEN NULL
			WHEN array_upper(a.attacl, 1) = 0 THEN a.attacl[0]
			ELSE unnest(a.attacl) END AS privileges,
		CASE
			WHEN a.attacl IS NULL THEN ''
			WHEN array_upper(a.attacl, 1) = 0 THEN 'Empty'
			ELSE ''
		END AS kind,
		coalesce(pg_catalog.array_to_string(a.attoptions, ','), '') AS options,
		coalesce(array_to_string(ARRAY(SELECT option_name || ' ' || quote_literal(option_value) FROM pg_options_to_table(attfdwoptions) ORDER BY option_name), ', '), '') AS fdwoptions,
		CASE WHEN a.attcollation <> t.typcollation THEN quote_ident(cn.nspname) || '.' || quote_ident(coll.collname) ELSE '' END AS collation,
		coalesce(sec.provider,'') AS securitylabelprovider,
		coalesce(sec.label,'') AS securitylabel
	FROM pg_catalog.pg_attribute a
		JOIN pg_class c ON a.attrelid = c.oid
		JOIN pg_namespace n ON c.relnamespace = n.oid
		LEFT JOIN pg_catalog.pg_attrdef ad ON a.attrelid = ad.adrelid AND a.attnum = ad.adnum
		LEFT JOIN pg_catalog.pg_type t ON a.atttypid = t.oid
		LEFT JOIN pg_catalog.pg_attribute_encoding e ON e.attrelid = a.attrelid AND e.attnum = a.attnum
		LEFT JOIN pg_description d ON d.objoid = a.attrelid AND d.classoid = 'pg_class'::regclass AND d.objsubid = a.attnum
		LEFT JOIN pg_collation coll ON a.attcollation = coll.oid
		LEFT JOIN pg_namespace cn ON coll.collnamespace = cn.oid
		LEFT JOIN pg_seclabel sec ON sec.objoid = a.attrelid AND sec.classoid = 'pg_class'::regclass AND sec.objsubid = a.attnum
	WHERE %s
		AND NOT EXISTS (
			SELECT 1 FROM 
				(SELECT parchildrelid FROM pg_partition_rule EXCEPT SELECT reloid FROM pg_exttable) par 
			WHERE par.parchildrelid = c.oid)
		AND c.reltype <> 0
		AND a.attnum > 0::pg_catalog.int2
		AND a.attisdropped = 'f'
	ORDER BY a.attrelid, a.attnum`, relationAndSchemaFilterClause())

	// In GPDB7+ we do not want to exclude child partitions, they function as separate tables.
	// Cannot use unnest() in CASE statements anymore in GPDB 7+ so convert
	// it to a LEFT JOIN LATERAL. We do not use LEFT JOIN LATERAL for GPDB 6
	// because the CASE unnest() logic is more performant.
	atLeast7Query := fmt.Sprintf(`
	SELECT a.attrelid,
		a.attnum,
		quote_ident(a.attname) AS name,
		a.attnotnull,
		a.atthasdef,
		pg_catalog.format_type(t.oid,a.atttypmod) AS type,
		coalesce(pg_catalog.array_to_string(e.attoptions, ','), '') AS encoding,
		a.attstattarget,
		CASE WHEN a.attstorage != t.typstorage THEN a.attstorage ELSE '' END AS storagetype,
		coalesce('('||pg_catalog.pg_get_expr(ad.adbin, ad.adrelid)||')', '') AS defaultval,
		coalesce(d.description, '') AS comment,
		a.attgenerated,
		ljl_unnest AS privileges,
		CASE
			WHEN a.attacl IS NULL THEN ''
			WHEN array_upper(a.attacl, 1) = 0 THEN 'Empty'
			ELSE ''
		END AS kind,
		coalesce(pg_catalog.array_to_string(a.attoptions, ','), '') AS options,
		coalesce(array_to_string(ARRAY(SELECT option_name || ' ' || quote_literal(option_value) FROM pg_options_to_table(attfdwoptions) ORDER BY option_name), ', '), '') AS fdwoptions,
		CASE WHEN a.attcollation <> t.typcollation THEN quote_ident(cn.nspname) || '.' || quote_ident(coll.collname) ELSE '' END AS collation,
		coalesce(sec.provider,'') AS securitylabelprovider,
		coalesce(sec.label,'') AS securitylabel,
		(a.attinhcount > 0) AS isinherited
	FROM pg_catalog.pg_attribute a
		JOIN pg_class c ON a.attrelid = c.oid
		JOIN pg_namespace n ON c.relnamespace = n.oid
		LEFT JOIN pg_catalog.pg_attrdef ad ON a.attrelid = ad.adrelid AND a.attnum = ad.adnum
		LEFT JOIN pg_catalog.pg_type t ON a.atttypid = t.oid
		LEFT JOIN pg_catalog.pg_attribute_encoding e ON e.attrelid = a.attrelid AND e.attnum = a.attnum
		LEFT JOIN pg_description d ON d.objoid = a.attrelid AND d.classoid = 'pg_class'::regclass AND d.objsubid = a.attnum
		LEFT JOIN pg_collation coll ON a.attcollation = coll.oid
		LEFT JOIN pg_namespace cn ON coll.collnamespace = cn.oid
		LEFT JOIN pg_seclabel sec ON sec.objoid = a.attrelid AND sec.classoid = 'pg_class'::regclass AND sec.objsubid = a.attnum
		LEFT JOIN LATERAL unnest(a.attacl) ljl_unnest ON a.attacl IS NOT NULL AND array_length(a.attacl, 1) != 0
	WHERE %s
		AND c.reltype <> 0
		AND a.attnum > 0::pg_catalog.int2
		AND a.attisdropped = 'f'
	ORDER BY a.attrelid, a.attnum`, relationAndSchemaFilterClause())

	query := ``
	if connectionPool.Version.Before("6") {
		query = before6Query
	} else if connectionPool.Version.Is("6") {
		query = version6Query
	} else {
		query = atLeast7Query
	}

	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)
	resultMap := make(map[uint32][]ColumnDefinition)
	for _, result := range results {
		result.StorageType = storageTypeCodes[result.StorageType]
		if connectionPool.Version.AtLeast("7") {
			result.AttGenerated = attGeneratedCodes[result.AttGenerated]
		}
		resultMap[result.Oid] = append(resultMap[result.Oid], result)
	}
	return resultMap
}

func GetDistributionPolicies(connectionPool *dbconn.DBConn) map[uint32]string {
	gplog.Verbose("Getting distribution policies")

	// This query is adapted from the addDistributedBy() function in pg_dump.c.
	before6Query := `
		SELECT p.localoid AS oid,
			'DISTRIBUTED BY (' || string_agg(quote_ident(a.attname) , ', ' ORDER BY index) || ')' AS value	
		FROM (SELECT localoid, unnest(attrnums) AS attnum,
				generate_series(1, array_upper(attrnums, 1)) AS index
				FROM gp_distribution_policy WHERE attrnums IS NOT NULL) p
			JOIN pg_attribute a ON (p.localoid, p.attnum) = (a.attrelid, a.attnum)
		GROUP BY localoid
		UNION ALL
		SELECT p.localoid AS oid, 'DISTRIBUTED RANDOMLY' AS value
		FROM gp_distribution_policy p WHERE attrnums IS NULL`

	atLeast6Query := `
		SELECT localoid AS oid, pg_catalog.pg_get_table_distributedby(localoid) AS value
		FROM gp_distribution_policy`

	query := ""
	if connectionPool.Version.Before("6") {
		query = before6Query
	} else {
		query = atLeast6Query
	}

	return selectAsOidToStringMap(connectionPool, query)
}

func GetTableType(connectionPool *dbconn.DBConn) map[uint32]string {
	if connectionPool.Version.Before("6") {
		return map[uint32]string{}
	}
	query := `SELECT oid, reloftype::pg_catalog.regtype AS value FROM pg_class WHERE reloftype != 0`
	return selectAsOidToStringMap(connectionPool, query)
}

func GetTableAccessMethod(connectionPool *dbconn.DBConn) map[uint32]string {
	if connectionPool.Version.Before("7") {
		return map[uint32]string{}
	}
	query := `
	SELECT c.oid, a.amname AS value
	FROM pg_class c
        JOIN pg_am a ON c.relam = a.oid
	WHERE a.amtype = 't'
        AND a.amname != 'heap';`
	return selectAsOidToStringMap(connectionPool, query)
}

func GetTableReplicaIdentity(connectionPool *dbconn.DBConn) map[uint32]string {
	if connectionPool.Version.Before("6") {
		return make(map[uint32]string)
	}

	relkindFilter := "'r', 'm'"
	if connectionPool.Version.AtLeast("7") {
		relkindFilter = "'r', 'm', 'p'"
	}

	query := fmt.Sprintf(`
	SELECT oid,
		relreplident AS value
	FROM pg_class
	WHERE relkind IN (%s)
		AND oid >= %d`, relkindFilter, FIRST_NORMAL_OBJECT_ID)
	return selectAsOidToStringMap(connectionPool, query)
}

func GetPartitionDetails(connectionPool *dbconn.DBConn) (map[uint32]string, map[uint32]string) {
	if connectionPool.Version.AtLeast("7") {
		// GPDB7 reworked partition tables significantly, to match
		// how upstream handles them.  These separate details are no
		// longer needed, and instead partition tables are handled as
		// true tables and their information is retrieved during "normal"
		// table queries.
		return make(map[uint32]string), make(map[uint32]string)
	}

	gplog.Info("Getting partition definitions")
	query := fmt.Sprintf(`
	SELECT p.parrelid AS oid,
		pg_get_partition_def(p.parrelid, true, true) AS definition,
		pg_get_partition_template_def(p.parrelid, true, true) AS template
	FROM pg_partition p
		JOIN pg_class c ON p.parrelid = c.oid
		JOIN pg_namespace n ON c.relnamespace = n.oid
	WHERE %s`, relationAndSchemaFilterClause())
	var results []struct {
		Oid        uint32
		Definition string
		Template   sql.NullString
	}
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)
	partitionDef := make(map[uint32]string)
	partitionTemp := make(map[uint32]string)
	for _, result := range results {
		partitionDef[result.Oid] = result.Definition
		if result.Template.Valid {
			partitionTemp[result.Oid] = result.Template.String
		}
	}
	return partitionDef, partitionTemp
}

type AlteredPartitionRelation struct {
	OldSchema string
	NewSchema string
	Name      string
}

/*
 * Partition tables could have child partitions in schemas different
 * than the root partition. We need to keep track of these child
 * partitions and later create ALTER TABLE SET SCHEMA statements for
 * them.
 */
func GetPartitionAlteredSchema(connectionPool *dbconn.DBConn) map[uint32][]AlteredPartitionRelation {
	if connectionPool.Version.AtLeast("7") {
		return make(map[uint32][]AlteredPartitionRelation)
	}

	gplog.Info("Getting child partitions with altered schema")
	query := `
	SELECT pgp.parrelid AS oid,
		quote_ident(pgn2.nspname) AS oldschema,
		quote_ident(pgn.nspname) AS newschema,
		quote_ident(pgc.relname) AS name
	FROM pg_catalog.pg_partition_rule pgpr
		JOIN pg_catalog.pg_partition pgp ON pgp.oid = pgpr.paroid
		JOIN pg_catalog.pg_class pgc ON pgpr.parchildrelid = pgc.oid
		JOIN pg_catalog.pg_class pgc2 ON pgp.parrelid = pgc2.oid
		JOIN pg_catalog.pg_namespace pgn ON pgc.relnamespace = pgn.oid
		JOIN pg_catalog.pg_namespace pgn2 ON pgc2.relnamespace = pgn2.oid
	WHERE pgc.relnamespace != pgc2.relnamespace`
	var results []struct {
		Oid uint32
		AlteredPartitionRelation
	}
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)
	partitionAlteredSchemaMap := make(map[uint32][]AlteredPartitionRelation)
	for _, result := range results {
		alteredPartitionRelation := AlteredPartitionRelation{result.OldSchema, result.NewSchema, result.Name}
		partitionAlteredSchemaMap[result.Oid] = append(partitionAlteredSchemaMap[result.Oid], alteredPartitionRelation)
	}
	return partitionAlteredSchemaMap
}

func GetTableStorage(connectionPool *dbconn.DBConn) (map[uint32]string, map[uint32]string) {
	gplog.Info("Getting storage information")
	query := fmt.Sprintf(`
	SELECT c.oid,
		quote_ident(t.spcname) AS tablespace,
		array_to_string(reloptions, ', ') AS reloptions
	FROM pg_class c
		JOIN pg_namespace n ON c.relnamespace = n.oid
		LEFT JOIN pg_tablespace t ON t.oid = c.reltablespace
	WHERE %s
		AND (t.spcname IS NOT NULL OR reloptions IS NOT NULL)`,
		relationAndSchemaFilterClause())
	var results []struct {
		Oid        uint32
		Tablespace sql.NullString
		RelOptions sql.NullString
	}
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)
	tableSpaces := make(map[uint32]string)
	relOptions := make(map[uint32]string)
	for _, result := range results {
		if result.Tablespace.Valid {
			tableSpaces[result.Oid] = result.Tablespace.String
		}
		if result.RelOptions.Valid {
			relOptions[result.Oid] = result.RelOptions.String
		}
	}
	return tableSpaces, relOptions
}

func GetUnloggedTables(connectionPool *dbconn.DBConn) map[uint32]bool {
	if connectionPool.Version.Before("6") {
		return map[uint32]bool{}
	}
	query := `SELECT oid FROM pg_class WHERE relpersistence = 'u'`
	var results []struct {
		Oid uint32
	}
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)
	resultMap := make(map[uint32]bool)
	for _, result := range results {
		resultMap[result.Oid] = true
	}
	return resultMap
}

type ForeignTableDefinition struct {
	Oid     uint32 `db:"ftrelid"`
	Options string `db:"ftoptions"`
	Server  string `db:"ftserver"`
}

func GetForeignTableDefinitions(connectionPool *dbconn.DBConn) map[uint32]ForeignTableDefinition {
	if connectionPool.Version.Before("6") {
		return map[uint32]ForeignTableDefinition{}
	}

	query := fmt.Sprintf(`
	SELECT ftrelid, quote_ident(fs.srvname) AS ftserver,
		array_to_string(array(
			SELECT quote_ident(option_name) || ' ' || quote_literal(option_value)
			FROM pg_options_to_table(ftoptions) ORDER BY option_name
		), e',    ') AS ftoptions
	FROM pg_foreign_table ft
		JOIN pg_foreign_server fs ON ft.ftserver = fs.oid
	WHERE ft.ftrelid >= %d AND fs.oid >= %d`, FIRST_NORMAL_OBJECT_ID, FIRST_NORMAL_OBJECT_ID)
	results := make([]ForeignTableDefinition, 0)
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)
	resultMap := make(map[uint32]ForeignTableDefinition, len(results))
	for _, result := range results {
		resultMap[result.Oid] = result
	}
	return resultMap
}

type Dependency struct {
	Oid              uint32
	ReferencedObject string
}

func GetTableInheritance(connectionPool *dbconn.DBConn, tables []Relation) map[uint32][]string {
	tableFilterStr := ""
	if len(MustGetFlagStringArray(options.INCLUDE_RELATION)) > 0 {
		tableOidList := make([]string, len(tables))
		for i, table := range tables {
			tableOidList[i] = fmt.Sprintf("%d", table.Oid)
		}
		// If we are filtering on tables, we only want to record dependencies on other tables in the list
		if len(tableOidList) > 0 {
			tableFilterStr = fmt.Sprintf("\nAND i.inhrelid IN (%s)", strings.Join(tableOidList, ","))
		}
	}

	query := fmt.Sprintf(`
	SELECT i.inhrelid AS oid,
		quote_ident(n.nspname) || '.' || quote_ident(p.relname) AS referencedobject
	FROM pg_inherits i
		JOIN pg_class p ON i.inhparent = p.oid
		JOIN pg_namespace n ON p.relnamespace = n.oid
	WHERE %s%s
	ORDER BY i.inhrelid, i.inhseqno`,
		ExtensionFilterClause("p"), tableFilterStr)

	results := make([]Dependency, 0)
	resultMap := make(map[uint32][]string)
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)
	for _, result := range results {
		resultMap[result.Oid] = append(resultMap[result.Oid], result.ReferencedObject)
	}
	return resultMap
}

// Used to contruct root tables for GPDB 7+, because the root partition must be
// constructed by itself first.
func GetPartitionKeyDefs(connectionPool *dbconn.DBConn) map[uint32]string {
	if connectionPool.Version.Before("7") {
		return make(map[uint32]string, 0)
	}
	query := `
	SELECT
		partrelid AS oid,
		pg_get_partkeydef(partrelid) AS keydef
	FROM
		pg_partitioned_table;`

	var results []struct {
		Oid    uint32
		Keydef string
	}
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)
	resultMap := make(map[uint32]string)
	for _, result := range results {
		resultMap[result.Oid] = result.Keydef
	}
	return resultMap
}

type AttachPartitionInfo struct {
	Oid     uint32
	Relname string
	Parent  string
	Expr    string
}

func GetAttachPartitionInfo(connectionPool *dbconn.DBConn) map[uint32]AttachPartitionInfo {
	if connectionPool.Version.Before("7") {
		return make(map[uint32]AttachPartitionInfo, 0)
	}

	query := `
	SELECT
		c.oid,
		quote_ident(n.nspname) || '.' || quote_ident(c.relname) AS relname,
		quote_ident(rn.nspname) || '.' || quote_ident(rc.relname) AS parent,
		coalesce(pg_get_expr(c.relpartbound, c.oid), '') AS expr
	FROM pg_class c
		JOIN pg_namespace n ON c.relnamespace = n.oid
		JOIN pg_class rc ON pg_partition_root(c.oid) = rc.oid
		JOIN pg_namespace rn ON rc.relnamespace = rn.oid
	WHERE c.relispartition = 't'`

	results := make([]AttachPartitionInfo, 0)
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)
	resultMap := make(map[uint32]AttachPartitionInfo)
	for _, result := range results {
		resultMap[result.Oid] = result
	}
	return resultMap
}

func GetForceRowSecurity(connectionPool *dbconn.DBConn) map[uint32]bool {
	resultMap := make(map[uint32]bool)
	if connectionPool.Version.Before("7") {
		return resultMap
	}

	query := fmt.Sprintf(`
	SELECT oid
	FROM pg_class
	WHERE relforcerowsecurity = 't'
		AND oid >= %d`, FIRST_NORMAL_OBJECT_ID)

	results := make([]uint32, 0)
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)

	for _, oid := range results {
		resultMap[oid] = true
	}
	return resultMap
}

func selectAsOidToStringMap(connectionPool *dbconn.DBConn, query string) map[uint32]string {
	var results []struct {
		Oid   uint32
		Value string
	}
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)
	resultMap := make(map[uint32]string)
	for _, result := range results {
		resultMap[result.Oid] = result.Value
	}
	return resultMap
}
