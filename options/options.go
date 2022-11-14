package options

import (
	"fmt"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/iohelper"
	"github.com/greenplum-db/gpbackup/utils"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
)

// This is meant to be a read only package. Values inside should only be
// modified by setters, it's method functions, or initialization function.
// This package is meant to make mocking flags easier.
type Options struct {
	IncludedRelations         []string
	ExcludedRelations         []string
	isLeafPartitionData       bool
	ExcludedSchemas           []string
	IncludedSchemas           []string
	originalIncludedRelations []string
	RedirectSchema            string
}

func NewOptions(initialFlags *pflag.FlagSet) (*Options, error) {
	includedRelations, err := setFiltersFromFile(initialFlags, INCLUDE_RELATION, INCLUDE_RELATION_FILE)
	if err != nil {
		return nil, err
	}
	err = utils.ValidateFQNs(includedRelations)
	if err != nil {
		return nil, err
	}

	excludedRelations, err := setFiltersFromFile(initialFlags, EXCLUDE_RELATION, EXCLUDE_RELATION_FILE)
	if err != nil {
		return nil, err
	}
	err = utils.ValidateFQNs(excludedRelations)
	if err != nil {
		return nil, err
	}

	includedSchemas, err := setFiltersFromFile(initialFlags, INCLUDE_SCHEMA, INCLUDE_SCHEMA_FILE)
	if err != nil {
		return nil, err
	}

	excludedSchemas, err := setFiltersFromFile(initialFlags, EXCLUDE_SCHEMA, EXCLUDE_SCHEMA_FILE)
	if err != nil {
		return nil, err
	}

	leafPartitionData, err := initialFlags.GetBool(LEAF_PARTITION_DATA)
	if err != nil {
		return nil, err
	}

	redirectSchema := ""
	if initialFlags.Lookup(REDIRECT_SCHEMA) != nil {
		redirectSchema, err = initialFlags.GetString(REDIRECT_SCHEMA)
		if err != nil {
			return nil, err
		}
	}

	return &Options{
		IncludedRelations:         includedRelations,
		ExcludedRelations:         excludedRelations,
		IncludedSchemas:           includedSchemas,
		ExcludedSchemas:           excludedSchemas,
		isLeafPartitionData:       leafPartitionData,
		originalIncludedRelations: includedRelations,
		RedirectSchema:            redirectSchema,
	}, nil
}

func setFiltersFromFile(initialFlags *pflag.FlagSet, filterFlag string, filterFileFlag string) ([]string, error) {
	filters, err := initialFlags.GetStringArray(filterFlag)
	if err != nil {
		return nil, err
	}
	// values obtained from file filterFileFlag are copied to values in filterFlag
	// values are mutually exclusive so this is not an overwrite, it is a "fresh" setting
	filename, err := initialFlags.GetString(filterFileFlag)
	if err != nil {
		return nil, err
	}
	if filename != "" {
		filterLines, err := iohelper.ReadLinesFromFile(filename)
		if err != nil {
			return nil, err
		}
		// copy any values for flag filterFileFlag into global flag for filterFlag
		for _, fqn := range filterLines {
			if fqn != "" {
				filters = append(filters, fqn)          //This appends filter to options
				err = initialFlags.Set(filterFlag, fqn) //This appends to the slice underlying the flag.
				if err != nil {
					return nil, err
				}
			}
		}
		if err != nil {
			return nil, err
		}
	}
	return filters, nil
}

func (o Options) GetIncludedTables() []string {
	return o.IncludedRelations
}

func (o Options) GetOriginalIncludedTables() []string {
	return o.originalIncludedRelations
}

func (o Options) GetExcludedTables() []string {
	return o.ExcludedRelations
}

func (o Options) IsLeafPartitionData() bool {
	return o.isLeafPartitionData
}

func (o Options) GetIncludedSchemas() []string {
	return o.IncludedSchemas
}

func (o Options) GetExcludedSchemas() []string {
	return o.ExcludedSchemas
}

func (o *Options) AddIncludedRelation(relation string) {
	o.IncludedRelations = append(o.IncludedRelations, relation)
}

type FqnStruct struct {
	SchemaName string
	TableName  string
}

func QuoteTableNames(conn *dbconn.DBConn, tableNames []string) ([]string, error) {
	if len(tableNames) == 0 {
		return []string{}, nil
	}

	// Properly escape single quote before running quote ident. Postgres
	// quote_ident escapes single quotes by doubling them
	escapedTables := make([]string, 0)
	for _, v := range tableNames {
		escapedTables = append(escapedTables, utils.EscapeSingleQuotes(v))
	}

	fqnSlice, err := SeparateSchemaAndTable(escapedTables)
	if err != nil {
		return nil, err
	}
	result := make([]string, 0)

	quoteIdentTableFQNQuery := `SELECT quote_ident('%s') AS schemaname, quote_ident('%s') AS tablename`
	for _, fqn := range fqnSlice {
		queryResultTable := make([]FqnStruct, 0)
		query := fmt.Sprintf(quoteIdentTableFQNQuery, fqn.SchemaName, fqn.TableName)
		err := conn.Select(&queryResultTable, query)
		if err != nil {
			return nil, err
		}
		quoted := queryResultTable[0].SchemaName + "." + queryResultTable[0].TableName
		result = append(result, quoted)
	}

	return result, nil
}

func SeparateSchemaAndTable(tableNames []string) ([]FqnStruct, error) {
	fqnSlice := make([]FqnStruct, 0)
	for _, fqn := range tableNames {
		parts := strings.Split(fqn, ".")
		if len(parts) > 2 {
			return nil, errors.Errorf("cannot process an Fully Qualified Name with embedded dots yet: %s", fqn)
		}
		if len(parts) < 2 {
			return nil, errors.Errorf("Fully Qualified Names require a minimum of one dot, specifying the schema and table. Cannot process: %s", fqn)
		}
		schema := parts[0]
		table := parts[1]
		if schema == "" || table == "" {
			return nil, errors.Errorf("Fully Qualified Names must specify the schema and table. Cannot process: %s", fqn)
		}

		currFqn := FqnStruct{
			SchemaName: schema,
			TableName:  table,
		}

		fqnSlice = append(fqnSlice, currFqn)
	}

	return fqnSlice, nil
}

func (o *Options) ExpandIncludesForPartitions(conn *dbconn.DBConn, flags *pflag.FlagSet) error {
	if len(o.GetIncludedTables()) == 0 {
		return nil
	}

	quotedIncludeRelations, err := QuoteTableNames(conn, o.GetIncludedTables())
	if err != nil {
		return err
	}

	allFqnStructs, err := o.getUserTableRelationsWithIncludeFiltering(conn, quotedIncludeRelations)
	if err != nil {
		return err
	}

	includeSet := map[string]bool{}
	for _, include := range o.GetIncludedTables() {
		includeSet[include] = true
	}

	allFqnSet := map[string]bool{}
	for _, fqnStruct := range allFqnStructs {
		fqn := fmt.Sprintf("%s.%s", fqnStruct.SchemaName, fqnStruct.TableName)
		allFqnSet[fqn] = true
	}

	// set arithmetic: find difference
	diff := make([]string, 0)
	for key := range allFqnSet {
		_, keyExists := includeSet[key]
		if !keyExists {
			diff = append(diff, key)
		}
	}

	for _, fqn := range diff {
		err = flags.Set(INCLUDE_RELATION, fqn)
		if err != nil {
			return err
		}
		o.AddIncludedRelation(fqn)
	}

	return nil
}

func (o *Options) QuoteIncludeRelations(conn *dbconn.DBConn) error {
	var err error
	o.IncludedRelations, err = QuoteTableNames(conn, o.GetIncludedTables())
	if err != nil {
		return err
	}

	return nil
}

func (o *Options) QuoteExcludeRelations(conn *dbconn.DBConn) error {
	var err error
	o.ExcludedRelations, err = QuoteTableNames(conn, o.GetExcludedTables())
	if err != nil {
		return err
	}

	return nil
}

func (o Options) getUserTableRelationsWithIncludeFiltering(connectionPool *dbconn.DBConn, includedRelationsQuoted []string) ([]FqnStruct, error) {
	includeOids, err := getOidsFromRelationList(connectionPool, includedRelationsQuoted)
	if err != nil {
		return nil, err
	}

	oidStr := strings.Join(includeOids, ", ")
	childPartitionFilter := ""
	if o.isLeafPartitionData {
		//Get all leaf partition tables whose parents are in the include list
		childPartitionFilter = fmt.Sprintf(`
	OR c.oid IN (
		SELECT
			r.parchildrelid
		FROM pg_partition p
		JOIN pg_partition_rule r ON p.oid = r.paroid
		WHERE p.paristemplate = false
		AND p.parrelid IN (%s))`, oidStr)
	} else {
		//Get only external partition tables whose parents are in the include list
		childPartitionFilter = fmt.Sprintf(`
	OR c.oid IN (
		SELECT
			r.parchildrelid
		FROM pg_partition p
		JOIN pg_partition_rule r ON p.oid = r.paroid
		JOIN pg_exttable e ON r.parchildrelid = e.reloid
		WHERE p.paristemplate = false
		AND e.reloid IS NOT NULL
		AND p.parrelid IN (%s))`, oidStr)
	}

	query := fmt.Sprintf(`
SELECT schemaname, tablename FROM (
	SELECT c.oid AS oid,
		n.nspname AS schemaname,
		c.relname AS tablename,
		coalesce(prt.pages, c.relpages) AS pages
	FROM pg_class c
	JOIN pg_namespace n ON c.relnamespace = n.oid
	LEFT JOIN (
		SELECT
			p.parrelid,
			sum(pc.relpages) AS pages
		FROM pg_partition_rule AS pr
		JOIN pg_partition AS p ON pr.paroid = p.oid
		JOIN pg_class AS pc ON pr.parchildrelid = pc.oid
		GROUP BY p.parrelid
	) AS prt ON prt.parrelid = c.oid
	WHERE %s
	AND (
		-- Get tables in the include list
		c.oid IN (%s)
		-- Get parent partition tables whose children are in the include list
		OR c.oid IN (
			SELECT
				p.parrelid
			FROM pg_partition p
			JOIN pg_partition_rule r ON p.oid = r.paroid
			WHERE p.paristemplate = false
			AND r.parchildrelid IN (%s)
		)
		-- Get external partition tables whose siblings are in the include list
		OR c.oid IN (
			SELECT
				r.parchildrelid
			FROM pg_partition_rule r
			JOIN pg_exttable e ON r.parchildrelid = e.reloid
			WHERE r.paroid IN (
				SELECT
					pr.paroid
				FROM pg_partition_rule pr
				WHERE pr.parchildrelid IN (%s)
			)
		)
		%s
	)
	AND (relkind = 'r' OR relkind = 'f')
	AND %s
) res
ORDER BY pages DESC, oid;`, o.schemaFilterClause("n"), oidStr, oidStr, oidStr, childPartitionFilter, ExtensionFilterClause("c"))

	results := make([]FqnStruct, 0)
	err = connectionPool.Select(&results, query)

	return results, err
}

func getOidsFromRelationList(connectionPool *dbconn.DBConn, quotedRelationNames []string) ([]string, error) {
	relList := utils.SliceToQuotedString(quotedRelationNames)
	query := fmt.Sprintf(`
SELECT
	c.oid AS string
FROM pg_class c
JOIN pg_namespace n ON c.relnamespace = n.oid
WHERE quote_ident(n.nspname) || '.' || quote_ident(c.relname) IN (%s)`, relList)

	return dbconn.SelectStringSlice(connectionPool, query)
}

// A list of schemas we don't want to back up, formatted for use in a WHERE clause
func (o Options) schemaFilterClause(namespace string) string {
	schemaFilterClauseStr := ""
	if len(o.GetIncludedSchemas()) > 0 {
		schemaFilterClauseStr = fmt.Sprintf("\nAND %s.nspname IN (%s)", namespace, utils.SliceToQuotedString(o.GetIncludedSchemas()))
	}
	if len(o.GetExcludedSchemas()) > 0 {
		schemaFilterClauseStr = fmt.Sprintf("\nAND %s.nspname NOT IN (%s)", namespace, utils.SliceToQuotedString(o.GetExcludedSchemas()))
	}
	return fmt.Sprintf(`%s.nspname NOT LIKE 'pg_temp_%%' AND %s.nspname NOT LIKE 'pg_toast%%' AND %s.nspname NOT IN ('gp_toolkit', 'information_schema', 'pg_aoseg', 'pg_bitmapindex', 'pg_catalog') %s`, namespace, namespace, namespace, schemaFilterClauseStr)
}

func ExtensionFilterClause(namespace string) string {
	oidStr := "oid"
	if namespace != "" {
		oidStr = fmt.Sprintf("%s.oid", namespace)
	}

	return fmt.Sprintf("%s NOT IN (select objid from pg_depend where deptype = 'e')", oidStr)
}
