package restore

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpbackup/utils"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
)

/*
 * This file contains functions related to validating user input.
 */

func validateFilterListsInBackupSet() {
	ValidateFilterSchemasInBackupSet(MustGetFlagStringSlice(utils.INCLUDE_SCHEMA))
	ValidateFilterRelationsInBackupSet(MustGetFlagStringSlice(utils.INCLUDE_RELATION))
}

func ValidateFilterSchemasInBackupSet(schemaList []string) {
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

func ValidateRelationsInRestoreDatabase(connection *dbconn.DBConn, relationList []string) {
	if len(relationList) == 0 {
		if len(globalTOC.DataEntries) == 0 {
			return
		}
		for _, entry := range globalTOC.DataEntries {
			fqn := utils.MakeFQN(entry.Schema, entry.Name)
			relationList = append(relationList, fqn)
		}
	}
	utils.ValidateFQNs(relationList)
	quotedTablesStr := utils.SliceToQuotedString(relationList)
	query := fmt.Sprintf(`
SELECT
	n.nspname || '.' || c.relname AS string
FROM pg_namespace n
JOIN pg_class c ON n.oid = c.relnamespace
WHERE quote_ident(n.nspname) || '.' || quote_ident(c.relname) IN (%s)`, quotedTablesStr)
	relationsInDB := dbconn.MustSelectStringSlice(connection, query)

	/*
	 * For data-only we check that the relations we are planning to restore
	 * are already defined in the database so we have somewhere to put the data.
	 *
	 * For non-data-only we check that the relations we are planning to restore
	 * are not already in the database so we don't get duplicate data.
	 */
	errMsg := ""
	if backupConfig.DataOnly || MustGetFlagBool(utils.DATA_ONLY) {
		if len(relationsInDB) < len(relationList) {
			dbRelationsSet := utils.NewIncludeSet(relationsInDB)
			//The default behavior is to match when the set is empty, but we don't want this
			dbRelationsSet.AlwaysMatchesFilter = false
			for _, restoreRelation := range relationList {
				matches := dbRelationsSet.MatchesFilter(restoreRelation)
				if !matches {
					errMsg = fmt.Sprintf("Relation %s must exist for data-only restore", restoreRelation)
				}
			}
		}
	} else if len(relationsInDB) > 0 {
		errMsg = fmt.Sprintf("Relation %s already exists", relationsInDB[0])
	}
	if errMsg != "" {
		gplog.Fatal(nil, errMsg)
	}
}

func ValidateFilterRelationsInBackupSet(relationList []string) {
	if len(relationList) == 0 {
		return
	}
	relationMap := make(map[string]bool, len(relationList))
	for _, relation := range relationList {
		relationMap[relation] = true
	}
	for _, entry := range globalTOC.PredataEntries {
		if entry.ObjectType != "TABLE" && entry.ObjectType != "SEQUENCE" && entry.ObjectType != "VIEW" {
			continue
		}
		fqn := utils.MakeFQN(entry.Schema, entry.Name)
		if _, ok := relationMap[fqn]; ok {
			delete(relationMap, fqn)
		}
		if len(relationMap) == 0 {
			return
		}
	}

	dataEntries := make([]string, 0)
	for _, restorePlanEntry := range backupConfig.RestorePlan {
		dataEntries = append(dataEntries, restorePlanEntry.TableFQNs...)
	}
	for _, fqn := range dataEntries {
		if _, ok := relationMap[fqn]; ok {
			delete(relationMap, fqn)
		}
		if len(relationMap) == 0 {
			return
		}
	}

	keys := make([]string, len(relationMap))
	i := 0
	for k := range relationMap {
		keys[i] = k
		i++
	}
	gplog.Fatal(errors.Errorf("Could not find the following relation(s) in the backup set: %s", strings.Join(keys, ", ")), "")
}

func ValidateDatabaseExistence(dbname string, createDatabase bool, isFiltered bool) {
	databaseExists, err := strconv.ParseBool(dbconn.MustSelectString(connectionPool, fmt.Sprintf(`
SELECT CASE
	WHEN EXISTS (SELECT datname FROM pg_database WHERE datname='%s') THEN 'true'
	ELSE 'false'
END AS string;`, dbname)))
	gplog.FatalOnError(err)
	if !databaseExists {
		if isFiltered {
			gplog.Fatal(errors.Errorf(`Database "%s" must be created manually to restore table-filtered or data-only backups.`, dbname), "")
		} else if !createDatabase {
			gplog.Fatal(errors.Errorf(`Database "%s" does not exist. Use the --create-db flag to create "%s" as part of the restore process.`, dbname, dbname), "")
		}
	} else if createDatabase {
		gplog.Fatal(errors.Errorf(`Database "%s" already exists. Run gprestore again without --create-db flag.`, dbname), "")
	}
}

func ValidateBackupFlagCombinations() {
	if backupConfig.SingleDataFile && MustGetFlagInt(utils.JOBS) != 1 {
		gplog.Fatal(errors.Errorf("Cannot use jobs flag when restoring backups with a single data file per segment."), "")
	}
	if (backupConfig.IncludeTableFiltered || backupConfig.DataOnly) && MustGetFlagBool(utils.WITH_GLOBALS) {
		gplog.Fatal(errors.Errorf("Global metadata is not backed up in table-filtered or data-only backups."), "")
	}
	if backupConfig.MetadataOnly && MustGetFlagBool(utils.DATA_ONLY) {
		gplog.Fatal(errors.Errorf("Cannot use data-only flag when restoring metadata-only backup"), "")
	}
	if backupConfig.DataOnly && MustGetFlagBool(utils.METADATA_ONLY) {
		gplog.Fatal(errors.Errorf("Cannot use metadata-only flag when restoring data-only backup"), "")
	}
	validateBackupFlagPluginCombinations()
}

func validateBackupFlagPluginCombinations() {
	if backupConfig.Plugin != "" && MustGetFlagString(utils.PLUGIN_CONFIG) == "" {
		gplog.Fatal(errors.Errorf("Backup was taken with plugin %s. The --plugin-config flag must be used to restore.", backupConfig.Plugin), "")
	} else if backupConfig.Plugin == "" && MustGetFlagString(utils.PLUGIN_CONFIG) != "" {
		gplog.Fatal(errors.Errorf("The --plugin-config flag cannot be used to restore a backup taken without a plugin."), "")
	}
}

func ValidateFlagCombinations(flags *pflag.FlagSet) {
	utils.CheckExclusiveFlags(flags, utils.DATA_ONLY, utils.WITH_GLOBALS)
	utils.CheckExclusiveFlags(flags, utils.DATA_ONLY, utils.CREATE_DB)
	utils.CheckExclusiveFlags(flags, utils.DEBUG, utils.QUIET, utils.VERBOSE)
	utils.CheckExclusiveFlags(flags, utils.INCLUDE_SCHEMA, utils.INCLUDE_RELATION, utils.INCLUDE_RELATION_FILE)
	utils.CheckExclusiveFlags(flags, utils.EXCLUDE_SCHEMA, utils.INCLUDE_SCHEMA)
	utils.CheckExclusiveFlags(flags, utils.EXCLUDE_SCHEMA, utils.EXCLUDE_RELATION, utils.INCLUDE_RELATION, utils.EXCLUDE_RELATION_FILE, utils.INCLUDE_RELATION_FILE)
	utils.CheckExclusiveFlags(flags, utils.METADATA_ONLY, utils.DATA_ONLY)
	utils.CheckExclusiveFlags(flags, utils.PLUGIN_CONFIG, utils.BACKUP_DIR)
}
