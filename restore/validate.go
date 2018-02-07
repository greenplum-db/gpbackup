package restore

import (
	"fmt"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpbackup/utils"
	"github.com/pkg/errors"
)

/*
 * This file contains functions related to validating user input.
 */

func validateFilterListsInRestoreDatabase() {
	ValidateFilterSchemasInRestoreDatabase(connection, includeSchemas)
	ValidateFilterTablesInRestoreDatabase(connection, includeTables)
}

func validateFilterListsInBackupSet() {
	ValidateFilterSchemasInBackupSet(includeSchemas)
	ValidateFilterTablesInBackupSet(includeTables)
}

func ValidateFilterSchemasInRestoreDatabase(connection *dbconn.DBConn, schemaList utils.ArrayFlags) {
	if len(schemaList) > 0 {
		quotedSchemasStr := utils.SliceToQuotedString(schemaList)
		query := fmt.Sprintf("SELECT nspname AS string FROM pg_namespace WHERE nspname IN (%s)", quotedSchemasStr)
		resultSchemas := dbconn.MustSelectStringSlice(connection, query)
		if len(resultSchemas) > 0 {
			gplog.Fatal(nil, "Schema %s already exists", resultSchemas[0])
		}
	}
}

func ValidateFilterSchemasInBackupSet(schemaList utils.ArrayFlags) {
	schemaMap := make(map[string]bool, len(schemaList))
	for _, schema := range schemaList {
		schemaMap[schema] = true
	}
	if len(schemaList) > 0 {
		if !backupConfig.DataOnly {
			for _, entry := range globalTOC.PredataEntries {
				if _, ok := schemaMap[entry.Schema]; ok {
					delete(schemaMap, entry.Schema)
				}
				if len(schemaMap) == 0 {
					return
				}
			}
		} else {
			for _, entry := range globalTOC.DataEntries {
				if _, ok := schemaMap[entry.Schema]; ok {
					delete(schemaMap, entry.Schema)
				}
				if len(schemaMap) == 0 {
					return
				}
			}
		}
	} else {
		return
	}

	keys := make([]string, len(schemaMap))
	i := 0
	for k := range schemaMap {
		keys[i] = k
		i++
	}
	gplog.Fatal(errors.Errorf("Could not find the following schema(s) in the backup set: %s", strings.Join(keys, ", ")), "")
}

func ValidateFilterTablesInRestoreDatabase(connection *dbconn.DBConn, tableList utils.ArrayFlags) {
	if len(tableList) > 0 {
		utils.ValidateFQNs(tableList)
		quotedTablesStr := utils.SliceToQuotedString(tableList)
		query := fmt.Sprintf(`
SELECT
	n.nspname || '.' || c.relname AS string
FROM pg_namespace n
JOIN pg_class c ON n.oid = c.relnamespace
WHERE quote_ident(n.nspname) || '.' || quote_ident(c.relname) IN (%s)`, quotedTablesStr)
		resultTables := dbconn.MustSelectStringSlice(connection, query)
		if len(resultTables) > 0 {
			gplog.Fatal(nil, "Table %s already exists", resultTables[0])
		}
	}
}

func ValidateFilterTablesInBackupSet(tableList utils.ArrayFlags) {
	tableMap := make(map[string]bool, len(tableList))
	for _, table := range tableList {
		tableMap[table] = true
	}
	if len(tableList) > 0 {
		if !backupConfig.DataOnly {
			for _, entry := range globalTOC.PredataEntries {
				if entry.ObjectType != "TABLE" {
					continue
				}
				fqn := utils.MakeFQN(entry.Schema, entry.Name)
				if _, ok := tableMap[fqn]; ok {
					delete(tableMap, fqn)
				}
				if len(tableMap) == 0 {
					return
				}
			}
		} else {
			for _, entry := range globalTOC.DataEntries {
				fqn := utils.MakeFQN(entry.Schema, entry.Name)
				if _, ok := tableMap[fqn]; ok {
					delete(tableMap, fqn)
				}
				if len(tableMap) == 0 {
					return
				}
			}
		}
	} else {
		return
	}

	keys := make([]string, len(tableMap))
	i := 0
	for k := range tableMap {
		keys[i] = k
		i++
	}
	gplog.Fatal(errors.Errorf("Could not find the following table(s) in the backup set: %s", strings.Join(keys, ", ")), "")
}

func ValidateBackupFlagCombinations() {
	if backupConfig.SingleDataFile {
		if *numJobs != 1 {
			gplog.Fatal(errors.Errorf("Cannot use jobs flag when restoring backups with a single data file per segment."), "")
		}
	}
}

func ValidateFlagCombinations() {
	utils.CheckMandatoryFlags("timestamp")
	utils.CheckExclusiveFlags("debug", "quiet", "verbose")
	utils.CheckExclusiveFlags("include-table-file", "include-schema")
}
