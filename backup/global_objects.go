package backup

import (
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/greenplum-db/gpbackup/utils"
)

/*
 * Functions to print to the global or postdata file instead of, or in addition
 * to, the predata file.
 */

func PrintConnectionString(metadataFile io.Writer, dbname string) {
	utils.MustPrintf(metadataFile, "\\c %s\n", dbname)
}

/*
 * Session GUCs are printed to global, predata, and postdata files so we
 * will use the correct settings when the files are run during restore
 */
func PrintSessionGUCs(metadataFile io.Writer, gucs QuerySessionGUCs) {
	utils.MustPrintf(metadataFile, `SET statement_timeout = 0;
SET check_function_bodies = false;
SET client_min_messages = error;
SET client_encoding = '%s';
SET standard_conforming_strings = %s;
SET default_with_oids = %s;
`, gucs.ClientEncoding, gucs.StdConformingStrings, gucs.DefaultWithOids)
}

func PrintCreateDatabaseStatement(globalFile io.Writer, connection *utils.DBConn) {
	dbname := utils.QuoteIdent(connection.DBName)
	owner := utils.QuoteIdent(GetDatabaseOwner(connection))
	utils.MustPrintf(globalFile, "\n\nCREATE DATABASE %s;", dbname)
	utils.MustPrintf(globalFile, "\nALTER DATABASE %s OWNER TO %s;", dbname, owner)
}

func PrintDatabaseGUCs(globalFile io.Writer, gucs []string, dbname string) {
	for _, guc := range gucs {
		utils.MustPrintf(globalFile, "\nALTER DATABASE %s %s;", dbname, guc)
	}
}

func PrintCreateResourceQueueStatements(globalFile io.Writer, resQueues []QueryResourceQueue) {
	for _, resQueue := range resQueues {
		attributes := []string{}
		if resQueue.ActiveStatements != -1 {
			attributes = append(attributes, fmt.Sprintf("ACTIVE_STATEMENTS=%d", resQueue.ActiveStatements))
		}
		maxCostFloat, maxCostErr := strconv.ParseFloat(resQueue.MaxCost, 64)
		utils.CheckError(maxCostErr)
		if maxCostFloat > -1 {
			attributes = append(attributes, fmt.Sprintf("MAX_COST=%s", resQueue.MaxCost))
		}
		if resQueue.CostOvercommit {
			attributes = append(attributes, "COST_OVERCOMMIT=TRUE")
		}
		minCostFloat, minCostErr := strconv.ParseFloat(resQueue.MinCost, 64)
		utils.CheckError(minCostErr)
		if minCostFloat > 0 {
			attributes = append(attributes, fmt.Sprintf("MIN_COST=%s", resQueue.MinCost))
		}
		if resQueue.Priority != "medium" {
			attributes = append(attributes, fmt.Sprintf("PRIORITY=%s", strings.ToUpper(resQueue.Priority)))
		}
		if resQueue.MemoryLimit != "-1" {
			attributes = append(attributes, fmt.Sprintf("MEMORY_LIMIT='%s'", resQueue.MemoryLimit))
		}
		action := "CREATE"
		if resQueue.Name == "pg_default" {
			action = "ALTER"
		}
		utils.MustPrintf(globalFile, "\n\n%s RESOURCE QUEUE %s WITH (%s);", action, utils.QuoteIdent(resQueue.Name), strings.Join(attributes, ", "))

		if resQueue.Comment != "" {
			utils.MustPrintf(globalFile, "\n\nCOMMENT ON RESOURCE QUEUE %s IS '%s';", utils.QuoteIdent(resQueue.Name), resQueue.Comment)
		}
	}
}
