package backup

/*
 * This file contains structs and functions related to backing up query planner
 * statistics on the coordinator.
 */

import (
	"fmt"
	"strings"

	"github.com/greenplum-db/gpbackup/toc"
	"github.com/greenplum-db/gpbackup/utils"
	"github.com/lib/pq"
)

func PrintStatisticsStatements(statisticsFile *utils.FileWithByteCount, tocfile *toc.TOC, tables []Table, attStats map[uint32][]AttributeStatistic, tupleStats map[uint32]TupleStatistic) {
	for _, table := range tables {
		tupleQuery := GenerateTupleStatisticsQuery(table, tupleStats[table.Oid])
		printStatisticsStatementForTable(statisticsFile, tocfile, table, tupleQuery)
		for _, attStat := range attStats[table.Oid] {
			attributeQueries := GenerateAttributeStatisticsQueries(table, attStat)
			for _, attrQuery := range attributeQueries {
				printStatisticsStatementForTable(statisticsFile, tocfile, table, attrQuery)
			}
		}
	}
}

func printStatisticsStatementForTable(statisticsFile *utils.FileWithByteCount, tocfile *toc.TOC, table Table, query string) {
	start := statisticsFile.ByteCount
	statisticsFile.MustPrintf("\n\n%s\n", query)
	entry := toc.MetadataEntry{Schema: table.Schema, Name: table.Name, ObjectType: toc.OBJ_STATISTICS}
	tocfile.AddMetadataEntry("statistics", entry, start, statisticsFile.ByteCount, []uint32{0, 0})
}

func GenerateTupleStatisticsQuery(table Table, tupleStat TupleStatistic) string {
	tupleQuery := `UPDATE pg_class
SET
	relpages = %d::int,
	reltuples = %f::real
WHERE oid = '%s'::regclass::oid;`
	return fmt.Sprintf(
		tupleQuery,
		tupleStat.RelPages,
		tupleStat.RelTuples,
		utils.EscapeSingleQuotes(table.FQN()))
}

func GenerateAttributeStatisticsQueries(table Table, attStat AttributeStatistic) []string {
	/*
	 * When restoring statistics to a new database, we cannot determine what the
	 * new OID for a given object will be, so we need to perform an explicit cast
	 * from the name to an OID in the below statements, rather than backing up the
	 * OID in the source database.
	 */
	starelidStr := fmt.Sprintf("'%s'::regclass::oid", utils.EscapeSingleQuotes(table.FQN()))
	// The entry may or may not already exist, so we can't either just UPDATE or just INSERT without a DELETE.
	inheritStr := ""
	attributeSlotsQueryStr := ""
	var attributeQueries []string
	if connectionPool.Version.AtLeast("6") {
		inheritStr = fmt.Sprintf("\n\t%t::boolean,", attStat.Inherit)
		if connectionPool.Version.AtLeast("7") {
			attributeSlotsQueryStr = generateAttributeSlotsQuery7(attStat)
		} else {
			attributeSlotsQueryStr = generateAttributeSlotsQuery6(attStat)
		}
	} else {
		attributeSlotsQueryStr = generateAttributeSlotsQuery4(attStat)
	}

	attributeQueries = append(attributeQueries, fmt.Sprintf(`DELETE FROM pg_statistic WHERE starelid = %s AND staattnum = %d;`, starelidStr, attStat.AttNumber))
	attributeQueries = append(attributeQueries, fmt.Sprintf(`INSERT INTO pg_statistic VALUES (
	%s,
	%d::smallint,%s
	%f::real,
	%d::integer,
	%f::real,
	%s);`, starelidStr, attStat.AttNumber, inheritStr, attStat.NullFraction, attStat.Width, attStat.Distinct, attributeSlotsQueryStr))

	/*
	 * If a type name starts with exactly one underscore, it describes an array
	 * type.  We can't restore statistics of array columns, so we'll zero and
	 * NULL everything out.
	 */

	return attributeQueries
}

// GPDB7 introduced statistic collations
func generateAttributeSlotsQuery7(attStat AttributeStatistic) string {
	attributeQuery := ""
	if len(attStat.Type) > 1 && attStat.Type[0] == '_' && attStat.Type[1] != '_' {
		attributeQuery = `0::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	NULL::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	NULL,
	NULL,
	NULL,
	NULL,
	NULL`
	} else {
		attributeQuery = fmt.Sprintf(`%d::smallint,
	%d::smallint,
	%d::smallint,
	%d::smallint,
	%d::smallint,
	%d::oid,
	%d::oid,
	%d::oid,
	%d::oid,
	%d::oid,
	%d::oid,
	%d::oid,
	%d::oid,
	%d::oid,
	%d::oid,
	%s::real[],
	%s::real[],
	%s::real[],
	%s::real[],
	%s::real[],
	%s,
	%s,
	%s,
	%s,
	%s`, attStat.Kind1,
			attStat.Kind2,
			attStat.Kind3,
			attStat.Kind4,
			attStat.Kind5,
			attStat.Operator1,
			attStat.Operator2,
			attStat.Operator3,
			attStat.Operator4,
			attStat.Operator5,
			attStat.Collation1,
			attStat.Collation2,
			attStat.Collation3,
			attStat.Collation4,
			attStat.Collation5,
			realValues(attStat.Numbers1),
			realValues(attStat.Numbers2),
			realValues(attStat.Numbers3),
			realValues(attStat.Numbers4),
			realValues(attStat.Numbers5),
			AnyValues(attStat.Values1, attStat.Type),
			AnyValues(attStat.Values2, attStat.Type),
			AnyValues(attStat.Values3, attStat.Type),
			AnyValues(attStat.Values4, attStat.Type),
			AnyValues(attStat.Values5, attStat.Type))
	}
	return attributeQuery
}

// GPDB6 introduced an additional statistic slot that we account for in this function
func generateAttributeSlotsQuery6(attStat AttributeStatistic) string {
	attributeQuery := ""
	if len(attStat.Type) > 1 && attStat.Type[0] == '_' && attStat.Type[1] != '_' {
		attributeQuery = `0::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	NULL::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	NULL,
	NULL,
	NULL,
	NULL,
	NULL`
	} else {
		attributeQuery = fmt.Sprintf(`%d::smallint,
	%d::smallint,
	%d::smallint,
	%d::smallint,
	%d::smallint,
	%d::oid,
	%d::oid,
	%d::oid,
	%d::oid,
	%d::oid,
	%s::real[],
	%s::real[],
	%s::real[],
	%s::real[],
	%s::real[],
	%s,
	%s,
	%s,
	%s,
	%s`, attStat.Kind1,
			attStat.Kind2,
			attStat.Kind3,
			attStat.Kind4,
			attStat.Kind5,
			attStat.Operator1,
			attStat.Operator2,
			attStat.Operator3,
			attStat.Operator4,
			attStat.Operator5,
			realValues(attStat.Numbers1),
			realValues(attStat.Numbers2),
			realValues(attStat.Numbers3),
			realValues(attStat.Numbers4),
			realValues(attStat.Numbers5),
			AnyValues(attStat.Values1, attStat.Type),
			AnyValues(attStat.Values2, attStat.Type),
			AnyValues(attStat.Values3, attStat.Type),
			AnyValues(attStat.Values4, attStat.Type),
			AnyValues(attStat.Values5, attStat.Type))
	}
	return attributeQuery
}

func generateAttributeSlotsQuery4(attStat AttributeStatistic) string {
	attributeQuery := ""
	if len(attStat.Type) > 1 && attStat.Type[0] == '_' && attStat.Type[1] != '_' {
		attributeQuery = `0::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	NULL::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	NULL,
	NULL,
	NULL,
	NULL`
	} else {
		attributeQuery = fmt.Sprintf(`%d::smallint,
	%d::smallint,
	%d::smallint,
	%d::smallint,
	%d::oid,
	%d::oid,
	%d::oid,
	%d::oid,
	%s::real[],
	%s::real[],
	%s::real[],
	%s::real[],
	%s,
	%s,
	%s,
	%s`, attStat.Kind1,
			attStat.Kind2,
			attStat.Kind3,
			attStat.Kind4,
			attStat.Operator1,
			attStat.Operator2,
			attStat.Operator3,
			attStat.Operator4,
			realValues(attStat.Numbers1),
			realValues(attStat.Numbers2),
			realValues(attStat.Numbers3),
			realValues(attStat.Numbers4),
			AnyValues(attStat.Values1, attStat.Type),
			AnyValues(attStat.Values2, attStat.Type),
			AnyValues(attStat.Values3, attStat.Type),
			AnyValues(attStat.Values4, attStat.Type))
	}
	return attributeQuery
}

// It is assumed that the elements in the input slice are already escaped
func SliceToPostgresArray(slice []string) string {
	quotedStrings := make([]string, len(slice))
	for i, str := range slice {
		// Escape single quotes because we are using array_in
		escapedStr := utils.EscapeSingleQuotes(str)
		// Store a Go-syntax representation of the value because writing to the
		// file will evaluate the string
		quotedStrings[i] = fmt.Sprintf(`%#v`, escapedStr)
	}
	return fmt.Sprintf(`'{%s}'`, strings.Join(quotedStrings, ","))
}

func realValues(reals pq.StringArray) string {
	if len(reals) > 0 {
		return SliceToPostgresArray(reals)
	}
	return "NULL"
}

/*
 * A given type is not guaranteed to have a corresponding array type, so we need
 * to use array_in() instead of casting to an array.
 */
func AnyValues(any pq.StringArray, typ string) string {
	if len(any) > 0 {
		return fmt.Sprintf(`array_in(%s, '%s'::regtype::oid, -1)`, SliceToPostgresArray(any), typ)
	}
	return fmt.Sprintf("NULL")
}
