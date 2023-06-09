package options

import (
	"fmt"
	"sort"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
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

	allFqnStructs, err := o.getUserTableRelationsWithIncludeFiltering(conn, quotedIncludeRelations, MustGetFlagBool(flags, NO_INHERITS))
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

	if len(diff) > 0 {
		gplog.Info("The filtered table set has been expanded to include additional dependent tables; see the log file for a full list of tables that have been added")
		sort.Strings(diff)
	}
	for _, fqn := range diff {
		err = flags.Set(INCLUDE_RELATION, fqn)
		if err != nil {
			return err
		}
		o.AddIncludedRelation(fqn)
		gplog.Verbose("Added %s to the backup set", fqn)
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

// given a set of table oids, return a deduplicated set of other tables that EITHER depend
// on them, OR that they depend on. The behavior for which is set with recurseDirection.
func (o *Options) recurseTableDepend(conn *dbconn.DBConn, includeOids []string, tablesToRetrieve string, getLeafPartitions bool) ([]string, error) {
	var err error
	var dependQuery string

	expandedIncludeOids := make(map[string]bool)
	for _, oid := range includeOids {
		expandedIncludeOids[oid] = true
	}

	parentsQuery := `
			SELECT dep.refobjid
			FROM
				pg_depend dep
				INNER JOIN pg_class cls ON dep.refobjid = cls.oid
			WHERE
				dep.objid IN (%[1]s)
				AND cls.relkind IN ('r', 'p', 'f')`
	childrenQuery := `
			SELECT dep.objid
			FROM
				pg_depend dep
				INNER JOIN pg_class cls ON dep.objid = cls.oid
			WHERE
				dep.refobjid IN (%[1]s)
				AND cls.relkind IN ('r', 'p', 'f')`

	if conn.Version.Before("7") {
		if getLeafPartitions {
			// Get external partitions and leaves in the include list
			// Also get leaves of root partitions that currently have no leaves in the include list,
			// i.e. those that have no filtering on their leaves and so should be fully expanded.
			childrenQuery += `
				AND (
					-- External partitions
					dep.objid NOT IN (SELECT parchildrelid FROM pg_partition_rule WHERE parchildrelid NOT IN (SELECT reloid FROM pg_exttable))
					-- Leaf partitions in the include list
					OR dep.objid IN (%[1]s)
					-- Leaves of root partitions that currently have no leaves in the include list
					OR dep.refobjid NOT IN (SELECT parrelid FROM pg_partition p JOIN pg_partition_rule r ON p.oid = r.paroid WHERE r.parchildrelid in (%[1]s))
				)`
		} else {
			// Only get external partitions; specifically, exclude non-external leaf partitions, since we can't just
			// grab external partitions because we need to deal with non-partition tables as well.
			childrenQuery += `
				AND dep.objid NOT IN (SELECT parchildrelid FROM pg_partition_rule WHERE parchildrelid NOT IN (SELECT reloid FROM pg_exttable))`
		}
	}

	if tablesToRetrieve == "parents" {
		dependQuery = parentsQuery
	} else if tablesToRetrieve == "children" {
		dependQuery = childrenQuery
	} else {
		gplog.Error(`Please fix calling of this function recurseTableDepend. Argument tablesToRetrieve only accepts "parents" or "children".`)
	}

	// here we loop until no further table dependencies are found.  implemented iteratively, but functions like a recursion
	foundDeps := true
	loopOids := includeOids
	for foundDeps {
		foundDeps = false
		depOids := make([]string, 0)
		loopDepQuery := fmt.Sprintf(dependQuery, strings.Join(loopOids, ", "))
		err = conn.Select(&depOids, loopDepQuery)
		if err != nil {
			gplog.Warn("Table dependency query failed: %s", loopDepQuery)
			return nil, err
		}

		// confirm that any table dependencies are found
		// save the table dependencies for both output and for next recursion
		loopOids = loopOids[:]
		for _, depOid := range depOids {
			// must exclude oids already captured to avoid circular dependencies
			// causing an infinite loop
			if !expandedIncludeOids[depOid] {
				foundDeps = true
				loopOids = append(loopOids, depOid)
				expandedIncludeOids[depOid] = true
			}
		}
	}

	// capture deduplicated oids from map keys, return as array
	// done as a direct array assignment loop because it's faster and we know the length
	expandedIncludeOidsArr := make([]string, len(expandedIncludeOids))
	arrayIdx := 0
	for idx := range expandedIncludeOids {
		expandedIncludeOidsArr[arrayIdx] = idx
		arrayIdx++
	}
	return expandedIncludeOidsArr, err
}

func (o Options) getUserTableRelationsWithIncludeFiltering(connectionPool *dbconn.DBConn, includedRelationsQuoted []string, no_inherits bool) ([]FqnStruct, error) {
	includeOids, err := getOidsFromRelationList(connectionPool, includedRelationsQuoted)
	if err != nil {
		return nil, err
	}

	oidStr := strings.Join(includeOids, ", ")
	var childPartitionFilter, parentAndExternalPartitionFilter string
	// GPDB7+ reworks the nature of partition tables.  It is no longer sufficient
	// to pull parents and children in one step.  Instead we must recursively climb/descend
	// the pg_depend ladder, filtering to only members of pg_class at each step, until the
	// full hierarchy has been retrieved.
	//
	// While we could query pg_partition and pg_partition_rule for this information in 6 and
	// earlier, this inheritance-based approach still works in those earlier versions and it's
	// good to keep the logic as similar as possible between the earlier and later versions.
	//
	// If --no-inherits is passed, we retrieve parent tables (so that filtering on partition
	// leaves works) but skip retrieving child tables.

	// Step 1: Get all children
	if !no_inherits {
		childOids, err := o.recurseTableDepend(connectionPool, includeOids, "children", o.isLeafPartitionData)
		if err != nil {
			return nil, err
		}
		if len(childOids) > 0 {
			childPartitionFilter = fmt.Sprintf(`OR c.oid IN (%s)`, strings.Join(childOids, ", "))
		}
		includeOids = childOids
	}

	// Step 2: Get all parents, both of the original included tables and of the children retrieved in step 1.
	// This ensures that if e.g. a child table inherits from another table not in the include list then it, too,
	// will be backed up correctly.
	parentOids, err := o.recurseTableDepend(connectionPool, includeOids, "parents", o.isLeafPartitionData)
	if err != nil {
		return nil, err
	}
	if len(parentOids) > 0 {
		parentAndExternalPartitionFilter = fmt.Sprintf(`OR c.oid IN (%s)`, strings.Join(parentOids, ", "))
	}
	includeOids = parentOids

	// Step 3: In GPDB 6 and earlier, retrieve children a second time, as if any parents retrieved in step 2 are
	// partition roots we need to retrieve their included and external partitions.
	// This does not apply to GPDB 7 and later, because partition tables are created individually and so we don't
	// care about the metadata of sibling tables of any included leaf partitions.
	if connectionPool.Version.Before("7") && !no_inherits {
		childOids, err := o.recurseTableDepend(connectionPool, includeOids, "children", o.isLeafPartitionData)
		if err != nil {
			return nil, err
		}
		if len(childOids) > 0 {
			childPartitionFilter = fmt.Sprintf(`OR c.oid IN (%s)`, strings.Join(childOids, ", "))
		}
		includeOids = childOids
	}

	query := fmt.Sprintf(`
SELECT
	n.nspname AS schemaname,
	c.relname AS tablename
FROM pg_class c
JOIN pg_namespace n
	ON c.relnamespace = n.oid
WHERE %s
AND (
	-- Get tables in the include list
	c.oid IN (%s)
	%s
	%s
)
AND relkind IN ('r', 'f', 'p')
AND %s
ORDER BY c.oid;`, o.schemaFilterClause("n"), oidStr, parentAndExternalPartitionFilter, childPartitionFilter, ExtensionFilterClause("c"))

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
