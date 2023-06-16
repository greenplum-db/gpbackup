package backup

/*
 * This file contains structs and functions related to executing specific
 * queries to gather database query plan statistics.
 */

import (
	"fmt"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpbackup/utils"
	"github.com/lib/pq"
)

type AttributeStatistic struct {
	Oid          uint32
	Schema       string
	Table        string
	AttName      string
	Type         string
	Relid        uint32         `db:"starelid"`
	AttNumber    int            `db:"staattnum"`
	Inherit      bool           `db:"stainherit"`
	NullFraction float64        `db:"stanullfrac"`
	Width        int            `db:"stawidth"`
	Distinct     float64        `db:"stadistinct"`
	Kind1        int            `db:"stakind1"`
	Kind2        int            `db:"stakind2"`
	Kind3        int            `db:"stakind3"`
	Kind4        int            `db:"stakind4"`
	Kind5        int            `db:"stakind5"`
	Operator1    uint32         `db:"staop1"`
	Operator2    uint32         `db:"staop2"`
	Operator3    uint32         `db:"staop3"`
	Operator4    uint32         `db:"staop4"`
	Operator5    uint32         `db:"staop5"`
	Collation1   uint32         `db:"stacoll1"`
	Collation2   uint32         `db:"stacoll2"`
	Collation3   uint32         `db:"stacoll3"`
	Collation4   uint32         `db:"stacoll4"`
	Collation5   uint32         `db:"stacoll5"`
	Numbers1     pq.StringArray `db:"stanumbers1"`
	Numbers2     pq.StringArray `db:"stanumbers2"`
	Numbers3     pq.StringArray `db:"stanumbers3"`
	Numbers4     pq.StringArray `db:"stanumbers4"`
	Numbers5     pq.StringArray `db:"stanumbers5"`
	Values1      pq.StringArray `db:"stavalues1"`
	Values2      pq.StringArray `db:"stavalues2"`
	Values3      pq.StringArray `db:"stavalues3"`
	Values4      pq.StringArray `db:"stavalues4"`
	Values5      pq.StringArray `db:"stavalues5"`
}

func GetAttributeStatistics(connectionPool *dbconn.DBConn, tables []Table) map[uint32][]AttributeStatistic {
	inheritClause := ""
	statSlotClause := ""
	if connectionPool.Version.AtLeast("6") {
		inheritClause = "s.stainherit,"
		statSlotClause = `s.stakind5,
	s.staop5,
	s.stanumbers5,
	s.stavalues5,`
	}

	statCollationClause := ""
	if connectionPool.Version.AtLeast("7") {
		statCollationClause = `s.stacoll1,
					s.stacoll2,
					s.stacoll3,
					s.stacoll4,
					s.stacoll5,`
	}

	tablenames := make([]string, 0)
	for _, table := range tables {
		tablenames = append(tablenames, table.FQN())
	}
	query := fmt.Sprintf(`
	SELECT c.oid,
		quote_ident(n.nspname) AS schema,
		quote_ident(c.relname) AS table,
		quote_ident(a.attname) AS attname,
		quote_ident(t.typname) AS type,
		s.starelid,
		s.staattnum,
		%s
		s.stanullfrac,
		s.stawidth,
		s.stadistinct,
		%s
		s.stakind1,
		s.stakind2,
		s.stakind3,
		s.stakind4,
		s.staop1,
		s.staop2,
		s.staop3,
		s.staop4,
		%s
		s.stanumbers1,
		s.stanumbers2,
		s.stanumbers3,
		s.stanumbers4,
		s.stavalues1,
		s.stavalues2,
		s.stavalues3,
		s.stavalues4
	FROM pg_class c
		JOIN pg_namespace n ON c.relnamespace = n.oid

	JOIN pg_attribute a ON a.attrelid = c.oid
		JOIN pg_statistic s ON (c.oid = s.starelid AND a.attnum = s.staattnum)
		JOIN pg_type t ON a.atttypid = t.oid
	WHERE %s
		AND quote_ident(n.nspname) || '.' || quote_ident(c.relname) IN (%s)
	ORDER BY n.nspname, c.relname, a.attnum`,
		inheritClause, statSlotClause, statCollationClause,
		SchemaFilterClause("n"), utils.SliceToQuotedString(tablenames))

	results := make([]AttributeStatistic, 0)
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)
	stats := make(map[uint32][]AttributeStatistic)
	for _, stat := range results {
		stats[stat.Oid] = append(stats[stat.Oid], stat)
	}
	return stats
}

type TupleStatistic struct {
	Oid       uint32
	Schema    string
	Table     string
	RelPages  int
	RelTuples float64
}

func GetTupleStatistics(connectionPool *dbconn.DBConn, tables []Table) map[uint32]TupleStatistic {
	tablenames := make([]string, 0)
	for _, table := range tables {
		tablenames = append(tablenames, table.FQN())
	}
	query := fmt.Sprintf(`
	SELECT c.oid,
		quote_ident(n.nspname) AS schema,
		quote_ident(c.relname) AS table,
		c.relpages,
		c.reltuples
	FROM pg_class c
		JOIN pg_namespace n ON c.relnamespace = n.oid
	WHERE %s
		AND quote_ident(n.nspname) || '.' || quote_ident(c.relname) IN (%s)
	ORDER BY n.nspname, c.relname`,
		SchemaFilterClause("n"), utils.SliceToQuotedString(tablenames))

	results := make([]TupleStatistic, 0)
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)
	stats := make(map[uint32]TupleStatistic)
	for _, stat := range results {
		stats[stat.Oid] = stat
	}
	return stats
}
