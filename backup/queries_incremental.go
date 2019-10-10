package backup

import (
	"fmt"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpbackup/utils"
)

func GetAOIncrementalMetadata(connectionPool *dbconn.DBConn) map[string]utils.AOEntry {
	gplog.Verbose("Querying table row mod counts")
	var modCounts = getAllModCounts(connectionPool)
	gplog.Verbose("Querying last DDL modification timestamp for tables")
	var lastDDLTimestamps = getLastDDLTimestamps(connectionPool)
	aoTableEntries := make(map[string]utils.AOEntry)
	for aoTableFQN := range modCounts {
		aoTableEntries[aoTableFQN] = utils.AOEntry{
			Modcount:         modCounts[aoTableFQN],
			LastDDLTimestamp: lastDDLTimestamps[aoTableFQN],
		}
	}

	return aoTableEntries
}

func getAllModCounts(connectionPool *dbconn.DBConn) map[string]int64 {
	var segTableFQNs = getAOSegTableFQNs(connectionPool)
	modCounts := make(map[string]int64)
	for aoTableFQN, segTableFQN := range segTableFQNs {
		modCounts[aoTableFQN] = getModCount(connectionPool, segTableFQN)
	}
	return modCounts
}

func getAOSegTableFQNs(connectionPool *dbconn.DBConn) map[string]string {
	query := fmt.Sprintf(`
	SELECT
		seg.aotablefqn,
		'pg_aoseg.' || quote_ident(aoseg_c.relname) AS aosegtablefqn
	FROM
		pg_class aoseg_c
	JOIN
		(
			SELECT
				pg_ao.relid AS aooid,
				pg_ao.segrelid,
				aotables.aotablefqn
			FROM
				pg_appendonly pg_ao
				JOIN
				(
					SELECT
						c.oid,
						quote_ident(n.nspname)|| '.' || quote_ident(c.relname) AS aotablefqn
					FROM
						pg_class c
						JOIN
						pg_namespace n
						ON c.relnamespace = n.oid
					WHERE
						relstorage IN ( 'ao', 'co' )
					AND
						%s
				) aotables
				ON
					pg_ao.relid = aotables.oid
		) seg
	ON
		aoseg_c.oid = seg.segrelid
`, relationAndSchemaFilterClause())
	results := make([]struct {
		AOTableFQN    string
		AOSegTableFQN string
	}, 0)
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)
	resultMap := make(map[string]string)
	for _, result := range results {
		resultMap[result.AOTableFQN] = result.AOSegTableFQN
	}
	return resultMap
}

func getModCount(connectionPool *dbconn.DBConn, aosegtablefqn string) int64 {
	query := fmt.Sprintf(`
	SELECT COALESCE(pg_catalog.sum(modcount), 0) AS modcount FROM %s
`, aosegtablefqn)

	var results []struct {
		Modcount int64
	}
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)

	return results[0].Modcount
}

func getLastDDLTimestamps(connectionPool *dbconn.DBConn) map[string]string {
	query := fmt.Sprintf(`
	SELECT
		quote_ident(aoschema) || '.' || quote_ident(aorelname) as aotablefqn,
		lastddltimestamp
	FROM
		(
			SELECT
				c.oid AS aooid,
				n.nspname AS aoschema,
				c.relname AS aorelname
			FROM
				pg_class c
			JOIN
				pg_namespace n
			ON
				c.relnamespace = n.oid
			WHERE
				c.relstorage IN ('ao', 'co')
			AND
				%s
		) aotables
	JOIN
		(
			SELECT
				lo.objid,
				MAX(lo.statime) AS lastddltimestamp
			FROM
				pg_stat_last_operation lo
			WHERE
				lo.staactionname IN ('CREATE', 'ALTER', 'TRUNCATE')
			GROUP BY
				lo.objid
		) lastop
	ON
		aotables.aooid = lastop.objid
`, relationAndSchemaFilterClause())

	var results []struct {
		AOTableFQN       string
		LastDDLTimestamp string
	}
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)
	resultMap := make(map[string]string)
	for _, result := range results {
		resultMap[result.AOTableFQN] = result.LastDDLTimestamp
	}
	return resultMap
}
