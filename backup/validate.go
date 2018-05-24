package backup

import (
	"fmt"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpbackup/utils"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
)

/*
 * This file contains functions related to validating user input.
 */

func validateFilterLists() {
	ValidateFilterSchemas(connection, *excludeSchemas)
	ValidateFilterSchemas(connection, *includeSchemas)
	ValidateFilterTables(connection, *excludeTables)
	ValidateFilterTables(connection, *includeTables)
}

func ValidateFilterSchemas(connection *dbconn.DBConn, schemaList []string) {
	if len(schemaList) > 0 {
		quotedSchemasStr := utils.SliceToQuotedString(schemaList)
		query := fmt.Sprintf("SELECT nspname AS string FROM pg_namespace WHERE nspname IN (%s)", quotedSchemasStr)
		resultSchemas := dbconn.MustSelectStringSlice(connection, query)
		if len(resultSchemas) < len(schemaList) {
			schemaSet := utils.NewIncludeSet(resultSchemas)
			for _, schema := range schemaList {
				if !schemaSet.MatchesFilter(schema) {
					gplog.Fatal(nil, "Schema %s does not exist", schema)
				}
			}
		}
	}
}

func ValidateFilterTables(connection *dbconn.DBConn, tableList []string) {
	if len(tableList) > 0 {
		utils.ValidateFQNs(tableList)
		quotedTablesStr := utils.SliceToQuotedString(tableList)
		query := fmt.Sprintf(`
SELECT
	c.oid,
	n.nspname || '.' || c.relname AS name
FROM pg_namespace n
JOIN pg_class c ON n.oid = c.relnamespace
WHERE quote_ident(n.nspname) || '.' || quote_ident(c.relname) IN (%s)`, quotedTablesStr)
		resultTables := make([]struct {
			Oid  uint32
			Name string
		}, 0)
		err := connection.Select(&resultTables, query)
		gplog.FatalOnError(err)
		tableMap := make(map[string]uint32)
		for _, table := range resultTables {
			tableMap[table.Name] = table.Oid
		}

		partTableMap := GetPartitionTableMap(connection)
		for _, table := range tableList {
			tableOid := tableMap[table]
			if tableOid == 0 {
				gplog.Fatal(nil, "Table %s does not exist", table)
			}
			if partTableMap[tableOid] == "i" {
				gplog.Fatal(nil, "Cannot filter on %s, as it is an intermediate partition table.  Only parent partition tables and leaf partition tables may be specified.", table)
			}
		}
	}
}

func ValidateFlagCombinations(flags *pflag.FlagSet) {
	utils.CheckExclusiveFlags(flags, "debug", "quiet", "verbose")
	utils.CheckExclusiveFlags(flags, "data-only", "metadata-only")
	utils.CheckExclusiveFlags(flags, "include-schema", "include-table", "include-table-file")
	utils.CheckExclusiveFlags(flags, "exclude-schema", "include-schema")
	utils.CheckExclusiveFlags(flags, "exclude-schema", "exclude-table", "include-table", "exclude-table-file", "include-table-file")
	utils.CheckExclusiveFlags(flags, "exclude-table", "exclude-table-file", "leaf-partition-data")
	utils.CheckExclusiveFlags(flags, "metadata-only", "leaf-partition-data")
	utils.CheckExclusiveFlags(flags, "metadata-only", "single-data-file")
	utils.CheckExclusiveFlags(flags, "no-compression", "compression-level")
	if *pluginConfigFile != "" && !(*singleDataFile || *metadataOnly) {
		gplog.Fatal(errors.Errorf("--plugin-config must be specified with either --single-data-file or --metadata-only"), "")
	}
}

func ValidateCompressionLevel(compressionLevel int) {
	//We treat 0 as a default value and so assume the flag is not set if it is 0
	if compressionLevel < 0 || compressionLevel > 9 {
		gplog.Fatal(errors.Errorf("Compression level must be between 1 and 9"), "")
	}
}

func ValidateFlagValues() {
	utils.ValidateFullPath(*backupDir)
	utils.ValidateFullPath(*pluginConfigFile)
	ValidateCompressionLevel(*compressionLevel)
}
