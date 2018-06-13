package backup

/*
 * This file contains structs and functions related to executing specific
 * queries to gather metadata for the objects handled in metadata_globals.go.
 */

import (
	"fmt"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/lib/pq"
)

type SessionGUCs struct {
	ClientEncoding string `db:"client_encoding"`
}

func GetSessionGUCs(connection *dbconn.DBConn) SessionGUCs {
	result := SessionGUCs{}
	query := "SHOW client_encoding;"
	err := connection.Get(&result, query)
	gplog.FatalOnError(err)
	return result
}

type Database struct {
	Oid        uint32
	Name       string
	Tablespace string
	Collate    string
	CType      string
	Encoding   string
}

func GetDatabaseInfo(connection *dbconn.DBConn) Database {
	lcQuery := ""
	if connection.Version.AtLeast("6") {
		lcQuery = "datcollate AS collate, datctype AS ctype,"
	}
	query := fmt.Sprintf(`
SELECT
	d.oid,
	quote_ident(d.datname) AS name,
	quote_ident(t.spcname) AS tablespace,
	%s
	pg_encoding_to_char(d.encoding) AS encoding
FROM pg_database d
JOIN pg_tablespace t
ON d.dattablespace = t.oid
WHERE d.datname = '%s';`, lcQuery, connection.DBName)

	result := Database{}
	err := connection.Get(&result, query)
	gplog.FatalOnError(err)
	return result
}

func GetDatabaseGUCs(connection *dbconn.DBConn) []string {
	//We do not want to quote list type config settings such as search_path and DateStyle
	query := `
SELECT CASE
	WHEN option_name='search_path' OR option_name = 'DateStyle'
	THEN ('SET ' || option_name || ' TO ' || option_value)
	ELSE ('SET ' || option_name || ' TO ''' || option_value || '''')
END AS string
FROM pg_options_to_table(
	(%s)
);`
	if connection.Version.Before("6") {
		subquery := fmt.Sprintf("SELECT datconfig FROM pg_database WHERE datname = '%s'", connection.DBName)
		query = fmt.Sprintf(query, subquery)
	} else {
		subquery := fmt.Sprintf("SELECT setconfig FROM pg_db_role_setting WHERE setdatabase = (SELECT oid FROM pg_database WHERE datname = '%s')", connection.DBName)
		query = fmt.Sprintf(query, subquery)
	}
	return dbconn.MustSelectStringSlice(connection, query)
}

type ResourceQueue struct {
	Oid              uint32
	Name             string
	ActiveStatements int
	MaxCost          string
	CostOvercommit   bool
	MinCost          string
	Priority         string
	MemoryLimit      string
}

func GetResourceQueues(connection *dbconn.DBConn) []ResourceQueue {
	/*
	 * maxcost and mincost are represented as real types in the database, but we round to two decimals
	 * and cast them as text for more consistent formatting. pg_dumpall does this as well.
	 */
	query := `
SELECT
	r.oid,
	quote_ident(rsqname) AS name,
	rsqcountlimit AS activestatements,
	ROUND(rsqcostlimit::numeric, 2)::text AS maxcost,
	rsqovercommit AS costovercommit,
	ROUND(rsqignorecostlimit::numeric, 2)::text AS mincost,
	priority_capability.ressetting::text AS priority,
	memory_capability.ressetting::text AS memorylimit
FROM
	pg_resqueue r
		JOIN
		(SELECT resqueueid, ressetting FROM pg_resqueuecapability WHERE restypid = 5) priority_capability
		ON r.oid = priority_capability.resqueueid
	JOIN
		(SELECT resqueueid, ressetting FROM pg_resqueuecapability WHERE restypid = 6) memory_capability
		ON r.oid = memory_capability.resqueueid;
`
	results := make([]ResourceQueue, 0)
	err := connection.Select(&results, query)
	gplog.FatalOnError(err)
	return results
}

type ResourceGroup struct {
	Oid               uint32
	Name              string
	Concurrency       int
	CPURateLimit      int
	MemoryLimit       int
	MemorySharedQuota int
	MemorySpillRatio  int
	MemoryAuditor     int
	Cpuset            string
}

func GetResourceGroups(connection *dbconn.DBConn) []ResourceGroup {
	query := `
SELECT g.oid,
	quote_ident(g.rsgname) AS name,
	t1.proposed AS concurrency,
	t2.value    AS cpuratelimit,
	t3.proposed AS memorylimit,
	t4.proposed AS memorysharedquota,
	t5.proposed AS memoryspillratio,
	t6.value    AS memoryauditor,
	t7.value    AS cpuset
FROM pg_resgroup g
	JOIN pg_resgroupcapability t1 ON t1.resgroupid = g.oid
	JOIN pg_resgroupcapability t2 ON t2.resgroupid = g.oid
	JOIN pg_resgroupcapability t3 ON t3.resgroupid = g.oid
	JOIN pg_resgroupcapability t4 ON t4.resgroupid = g.oid
	JOIN pg_resgroupcapability t5 ON t5.resgroupid = g.oid
	LEFT JOIN pg_resgroupcapability t6 ON t6.resgroupid = g.oid
	LEFT JOIN pg_resgroupcapability t7 ON t7.resgroupid = g.oid
WHERE t1.reslimittype = 1 AND
	t2.reslimittype = 2 AND
	t3.reslimittype = 3 AND
	t4.reslimittype = 4 AND
	t5.reslimittype = 5 AND
	t6.reslimittype = 6 AND
	t7.reslimittype = 7;`

	results := make([]ResourceGroup, 0)
	err := connection.Select(&results, query)
	gplog.FatalOnError(err)
	return results
}

type TimeConstraint struct {
	Oid       uint32
	StartDay  int
	StartTime string
	EndDay    int
	EndTime   string
}

type Role struct {
	Oid             uint32
	Name            string
	Super           bool `db:"rolsuper"`
	Inherit         bool `db:"rolinherit"`
	CreateRole      bool `db:"rolcreaterole"`
	CreateDB        bool `db:"rolcreatedb"`
	CanLogin        bool `db:"rolcanlogin"`
	ConnectionLimit int  `db:"rolconnlimit"`
	Password        string
	ValidUntil      string
	ResQueue        string
	ResGroup        string
	Createrextgpfd  bool `db:"rolcreaterexthttp"`
	Createrexthttp  bool `db:"rolcreaterextgpfd"`
	Createwextgpfd  bool `db:"rolcreatewextgpfd"`
	Createrexthdfs  bool `db:"rolcreaterexthdfs"`
	Createwexthdfs  bool `db:"rolcreatewexthdfs"`
	TimeConstraints []TimeConstraint
}

/*
 * We convert rolvaliduntil to UTC and then append '-00' so that
 * we standardize times to UTC but do not lose time zone information
 * in the timestamp.
 */
func GetRoles(connection *dbconn.DBConn) []Role {
	resgroupQuery := ""
	if connection.Version.AtLeast("5") {
		resgroupQuery = "(SELECT quote_ident(rsgname) FROM pg_resgroup WHERE pg_resgroup.oid = rolresgroup) AS resgroup,"
	}
	query := fmt.Sprintf(`
SELECT
	oid,
	quote_ident(rolname) AS name,
	rolsuper,
	rolinherit,
	rolcreaterole,
	rolcreatedb,
	rolcanlogin,
	rolconnlimit,
	coalesce(rolpassword, '') AS password,
	coalesce(timezone('UTC', rolvaliduntil) || '-00', '') AS validuntil,
	(SELECT quote_ident(rsqname) FROM pg_resqueue WHERE pg_resqueue.oid = rolresqueue) AS resqueue,
	%s
	rolcreaterexthttp,
	rolcreaterextgpfd,
	rolcreatewextgpfd,
	rolcreaterexthdfs,
	rolcreatewexthdfs
FROM
	pg_authid`, resgroupQuery)

	roles := make([]Role, 0)
	err := connection.Select(&roles, query)
	gplog.FatalOnError(err)

	constraintsByRole := getTimeConstraintsByRole(connection)

	for idx, role := range roles {
		roles[idx].TimeConstraints = constraintsByRole[role.Oid]
	}

	return roles
}

func GetRoleGUCs(connection *dbconn.DBConn) map[string][]string {
	query := `
SELECT
	quote_ident(rolname) AS name,
	array_agg(guc) AS config
FROM (
	SELECT
		rolname,
		CASE
			WHEN option_name='search_path' OR option_name = 'DateStyle'
			THEN ('SET ' || option_name || ' TO ' || option_value)
			ELSE ('SET ' || option_name || ' TO ''' || option_value || '''')
		END AS guc
	FROM (
		SELECT rolname, (pg_options_to_table(rolconfig)).option_name, (pg_options_to_table(rolconfig)).option_value FROM pg_roles
	) AS options
) AS gucs GROUP BY rolname;`

	results := make([]struct {
		Name   string
		Config pq.StringArray
	}, 0)
	err := connection.Select(&results, query)
	gplog.FatalOnError(err)
	resultMap := make(map[string][]string, len(results))
	for _, result := range results {
		resultMap[result.Name] = []string(result.Config)
	}
	return resultMap
}

func getTimeConstraintsByRole(connection *dbconn.DBConn) map[uint32][]TimeConstraint {
	timeConstraints := make([]TimeConstraint, 0)
	query := `
SELECT
	authid AS oid,
	start_day AS startday,
	start_time::text AS starttime,
	end_day AS endday,
	end_time::text AS endtime
FROM
	pg_auth_time_constraint
	`

	err := connection.Select(&timeConstraints, query)
	gplog.FatalOnError(err)

	constraintsByRole := make(map[uint32][]TimeConstraint, 0)
	for _, constraint := range timeConstraints {
		roleConstraints, ok := constraintsByRole[constraint.Oid]
		if !ok {
			roleConstraints = make([]TimeConstraint, 0)
		}
		constraintsByRole[constraint.Oid] = append(roleConstraints, constraint)
	}

	return constraintsByRole
}

type RoleMember struct {
	Role    string
	Member  string
	Grantor string
	IsAdmin bool
}

func GetRoleMembers(connection *dbconn.DBConn) []RoleMember {
	query := `
SELECT
	quote_ident(pg_get_userbyid(roleid)) AS role,
	quote_ident(pg_get_userbyid(member)) AS member,
	quote_ident(pg_get_userbyid(grantor)) AS grantor,
	admin_option as isadmin
FROM pg_auth_members
ORDER BY roleid, member;`

	results := make([]RoleMember, 0)
	err := connection.Select(&results, query)
	gplog.FatalOnError(err)
	return results
}

type Tablespace struct {
	Oid          uint32
	Tablespace   string
	FileLocation string // FILESPACE in 4.3 and 5, LOCATION in 6 and later
}

func GetTablespaces(connection *dbconn.DBConn) []Tablespace {
	before6query := `
SELECT
	t.oid,
	quote_ident(t.spcname) AS tablespace,
	quote_ident(f.fsname) AS filelocation
FROM pg_tablespace t
JOIN pg_filespace f
ON t.spcfsoid = f.oid
WHERE spcname != 'pg_default'
AND spcname != 'pg_global';`
	query := `
SELECT
	oid,
	quote_ident(spcname) AS tablespace,
	'''' || pg_catalog.pg_tablespace_location(oid) || '''' AS filelocation
FROM pg_tablespace
WHERE spcname != 'pg_default'
AND spcname != 'pg_global';`

	results := make([]Tablespace, 0)
	var err error
	if connection.Version.Before("6") {
		err = connection.Select(&results, before6query)
	} else {
		err = connection.Select(&results, query)
	}
	gplog.FatalOnError(err)
	return results
}

func GetDBSize(connection *dbconn.DBConn) string {
	size := struct{ DBSize string }{}
	sizeQuery := fmt.Sprintf("SELECT pg_size_pretty(pg_database_size(E'%s')) as dbsize", dbconn.EscapeConnectionParam(connection.DBName))
	err := connection.Get(&size, sizeQuery)
	gplog.FatalOnError(err)
	return size.DBSize
}
