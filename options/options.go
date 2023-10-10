package options

import (
	"fmt"
	"regexp"
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

type Sections struct {
	Globals    bool
	Predata    bool
	Data       bool
	Postdata   bool
	Statistics bool
}

func (s *Sections) AsString() string {
	sections := []string{}
	if s.Globals {
		sections = append(sections, "Globals")
	}
	if s.Predata {
		sections = append(sections, "Predata")
	}
	if s.Data {
		sections = append(sections, "Data")
	}
	if s.Postdata {
		sections = append(sections, "Postdata")
	}
	if s.Statistics {
		sections = append(sections, "Statistics")
	}
	return strings.Join(sections, ", ")
}

// We need to keep these config values around for backwards compatibility, but
// don't want to print a bunch of useless variables in the config file going
// forward, so we split these off into their own struct.
type DeprecatedMetadata struct {
	DataOnly       bool
	MetadataOnly   bool
	WithoutGlobals bool
	WithStatistics bool
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

type Relation struct {
	SchemaOid uint32
	Oid       uint32
	Schema    string
	Name      string
}

func QuoteTableNames(conn *dbconn.DBConn, tableNames []string) ([]string, error) {
	if len(tableNames) == 0 {
		return []string{}, nil
	}

	fqnSlice, err := SeparateSchemaAndTable(tableNames)
	if err != nil {
		return nil, err
	}
	result := make([]string, 0)

	quoteIdentTableFQNQuery := `SELECT quote_ident('%s') AS schemaname, quote_ident('%s') AS tablename`
	for _, fqn := range fqnSlice {
		queryResultTable := make([]struct {
			SchemaName string
			TableName  string
		}, 0)
		query := fmt.Sprintf(quoteIdentTableFQNQuery, fqn.Schema, fqn.Name)
		err := conn.Select(&queryResultTable, query)
		if err != nil {
			return nil, err
		}
		quoted := queryResultTable[0].SchemaName + "." + queryResultTable[0].TableName
		result = append(result, quoted)
	}

	return result, nil
}

func SeparateSchemaAndTable(tableNames []string) ([]Relation, error) {
	fqnSlice := make([]Relation, 0)

	for _, fqn := range tableNames {

		var quotesPattern *regexp.Regexp
		schemaNameIsQuoted := strings.HasPrefix(fqn, "\"")
		tableNameIsQuoted := strings.HasSuffix(fqn, "\"")

		if schemaNameIsQuoted && tableNameIsQuoted {
			quotesPattern = regexp.MustCompile(`^"|"$|"\."`)
		} else if schemaNameIsQuoted {
			quotesPattern = regexp.MustCompile(`^"|"\.`)
		} else if tableNameIsQuoted {
			quotesPattern = regexp.MustCompile(`"$|\."`)
		} else {
			quotesPattern = regexp.MustCompile(`\.`)
		}
		parts := quotesPattern.Split(fqn, -1)

		fqnParts := make([]string, 0)
		for _, part := range parts {
			if len(part) > 0 {
				fqnParts = append(fqnParts, part)
			}
		}

		if len(fqnParts) != 2 {
			return nil, errors.Errorf("cannot process this Fully Qualified Name: %s", fqn)
		}

		// Properly escape quotes before running quote ident. Postgres
		// quote_ident escapes quotes by doubling them
		schema := utils.EscapeSingleQuotes(fqnParts[0])
		table := utils.EscapeSingleQuotes(fqnParts[1])

		currFqn := Relation{
			SchemaOid: 0,
			Oid:       0,
			Schema:    schema,
			Name:      table,
		}

		fqnSlice = append(fqnSlice, currFqn)
	}

	return fqnSlice, nil
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

func (o Options) GetUserTableRelationsWithIncludeFiltering(connectionPool *dbconn.DBConn, includeOids []string, no_inherits bool) ([]Relation, error) {

	oidStr := strings.Join(includeOids, ", ")
	var childPartitionFilter, parentAndExternalPartitionFilter string

	// If --no-inherits is passed, do not expand to parents or children, and just pass through the
	// initial list of filtered tables to populate the Relation structs.
	if !no_inherits {
		// GPDB7+ reworks the nature of partition tables.  It is no longer sufficient
		// to pull parents and children in one step.  Instead we must recursively climb/descend
		// the pg_depend ladder, filtering to only members of pg_class at each step, until the
		// full hierarchy has been retrieved.
		//
		// While we could query pg_partition and pg_partition_rule for this information in 6 and
		// earlier, this inheritance-based approach still works in those earlier versions and it's
		// good to keep the logic as similar as possible between the earlier and later versions.

		// Step 1: Get all children
		childOids, err := o.recurseTableDepend(connectionPool, includeOids, "children", o.isLeafPartitionData)
		if err != nil {
			return nil, err
		}
		if len(childOids) > 0 {
			childPartitionFilter = fmt.Sprintf(`OR c.oid IN (%s)`, strings.Join(childOids, ", "))
		}
		includeOids = childOids

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
		if connectionPool.Version.Before("7") {
			childOids, err := o.recurseTableDepend(connectionPool, includeOids, "children", o.isLeafPartitionData)
			if err != nil {
				return nil, err
			}
			if len(childOids) > 0 {
				childPartitionFilter = fmt.Sprintf(`OR c.oid IN (%s)`, strings.Join(childOids, ", "))
			}
			includeOids = childOids
		}
	}

	query := fmt.Sprintf(`
SELECT
    n.oid as schemaoid,
    c.oid as oid,
	n.nspname AS schema,
	c.relname AS name
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

	results := make([]Relation, 0)
	err := connectionPool.Select(&results, query)

	return results, err
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

func GetSections(flags *pflag.FlagSet) Sections {
	var sectionSlice []string
	var sections Sections

	// Always parse this even if --sections wasn't set, as default values differ for backup and restore
	sectionSlice = MustGetFlagStringSlice(flags, SECTIONS)
	for _, section := range sectionSlice {
		switch section {
		case "globals":
			sections.Globals = true
		case "predata":
			sections.Predata = true
		case "data":
			sections.Data = true
		case "postdata":
			sections.Postdata = true
		case "statistics":
			sections.Statistics = true
		default:
			gplog.Fatal(errors.Errorf("Unrecognized section flag input: %s", section), "")
		}
	}

	// Allow overrides by legacy flags for backwards compatibility, but log a warning
	deprecationMsg := "The %s flag is deprecated and may be removed in a future release, please use --sections instead"
	if MustGetFlagBool(flags, METADATA_ONLY) {
		gplog.Warn(fmt.Sprintf(deprecationMsg, METADATA_ONLY))
		sections.Data = false
	}
	if MustGetFlagBool(flags, DATA_ONLY) {
		gplog.Warn(fmt.Sprintf(deprecationMsg, DATA_ONLY))
		sections.Globals = false
		sections.Predata = false
		sections.Postdata = false
		sections.Statistics = false
	}
	if MustGetFlagBool(flags, WITH_STATS) {
		gplog.Warn(fmt.Sprintf(deprecationMsg, WITH_STATS))
		sections.Statistics = true
	}
	// This function is called in both backup and restore, so we need to check that certain flags exist
	// before we try to reference them
	if flags.Lookup(WITHOUT_GLOBALS) != nil && MustGetFlagBool(flags, WITHOUT_GLOBALS) {
		gplog.Warn(fmt.Sprintf(deprecationMsg, WITHOUT_GLOBALS))
		sections.Globals = false
	}
	if flags.Lookup(WITH_GLOBALS) != nil && MustGetFlagBool(flags, WITH_GLOBALS) {
		gplog.Warn(fmt.Sprintf(deprecationMsg, WITH_GLOBALS))
		sections.Globals = true
	}

	return sections
}
