package utils

/*
 * This file contains structs and functions related to connecting to a database
 * and executing queries.
 */

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq" // Need driver for postgres
	"github.com/pkg/errors"
)

const MINIMUM_GPDB4_VERSION = "4.3.17"
const MINIMUM_GPDB5_VERSION = "5.1.0"

/*
 * While the sqlx.DB struct (and indirectly the sql.DB struct) maintains its own
 * connection pool, there is no guarantee of session-level consistency between
 * queries and we require that level of control for e.g. setting certain GUCs on
 * each connection before restoring data.  Also, while sql.Conn is a struct that
 * represents a single session, there is no sqlx.Conn equivalent we could use.
 *
 * Thus, DBConn maintains its own connection pool of sqlx.DBs (all set to have
 * exactly one database connection each) in an array, such that callers can
 * create NumConns goroutines and assign each an index from 0 to NumConns to
 * guarantee that each goroutine gets its own connection that exhibits single-
 * session behavior.  The Exec, Select, and Get functions are set up to default
 * to the first connection (index 0), so the DBConn will still exhibit session-
 * like behavior if no connection is specified, and other functions that want to
 * execute in serial should pass in a 0 wherever a connection number is needed.
 */
type DBConn struct {
	ConnPool []*sqlx.DB
	NumConns int
	Driver   DBDriver
	User     string
	DBName   string
	Host     string
	Port     int
	Tx       *sqlx.Tx
	Version  GPDBVersion
}

func NewDBConn(dbname string) *DBConn {
	username := ""
	host := ""
	port := 0

	currentUser, _, currentHost := GetUserAndHostInfo()
	username = TryEnv("PGUSER", currentUser)
	if dbname == "" {
		logger.Fatal(errors.New("No database provided"), "")
	}
	host = TryEnv("PGHOST", currentHost)
	port, _ = strconv.Atoi(TryEnv("PGPORT", "5432"))

	return &DBConn{
		ConnPool: nil,
		NumConns: 0,
		Driver:   GPDBDriver{},
		User:     username,
		DBName:   dbname,
		Host:     host,
		Port:     port,
		Tx:       nil,
		Version:  GPDBVersion{},
	}
}

/*
 * This function doesn't allow Begin()ing if there are multiple connections,
 * since for example Connect()ing with a pool of 2 connections and trying to
 * Select() on connection 1 doesn't really make sense given the "automatic
 * transaction" behavior of Get/Exec/Select, as explained in the relevant
 * comment below.
 *
 * TODO: Refactor transaction logic to allow starting multiple transactions
 * on different connections while leaving other connections transaction-less,
 * in such a way that one cannot easily accidentally execute a query intended
 * to be part of a transaction on a transaction-less connection or vice versa.
 */
func (dbconn *DBConn) Begin() {
	if dbconn.NumConns > 1 {
		logger.Fatal(errors.New("Cannot begin transaction; the connection was initialized with a connection pool size larger than 1"), "")
	}
	if dbconn.Tx != nil {
		logger.Fatal(errors.New("Cannot begin transaction; there is already a transaction in progress"), "")
	}
	var err error
	dbconn.Tx, err = dbconn.ConnPool[0].Beginx()
	CheckError(err)
	/*
	 * This uses a SERIALIZABLE transaction so the backup can effectively take a
	 * "snapshot" of the database via MVCC, to keep backups consistent without
	 * requiring a pg_class lock.
	 */
	dbconn.MustExec("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
}

func (dbconn *DBConn) Close() {
	if dbconn.ConnPool != nil {
		for _, conn := range dbconn.ConnPool {
			if conn != nil {
				conn.Close()
			}
		}
		dbconn.ConnPool = nil
		dbconn.NumConns = 0
	}
}

func (dbconn *DBConn) Commit() {
	if dbconn.Tx == nil {
		logger.Fatal(errors.New("Cannot commit transaction; there is no transaction in progress"), "")
	}
	err := dbconn.Tx.Commit()
	CheckError(err)
	dbconn.Tx = nil
}

func (dbconn *DBConn) Connect(numConns int) {
	if numConns < 1 {
		logger.Fatal(errors.Errorf("Must specify a connection pool size that is a positive integer"), "")
	}
	if dbconn.ConnPool != nil {
		logger.Fatal(errors.Errorf("The database connection must be closed before reusing the connection"), "")
	}
	dbname := escapeConnectionParam(dbconn.DBName)
	user := escapeConnectionParam(dbconn.User)
	connStr := fmt.Sprintf(`user='%s' dbname='%s' host=%s port=%d sslmode=disable`, user, dbname, dbconn.Host, dbconn.Port)
	dbconn.ConnPool = make([]*sqlx.DB, numConns)
	for i := 0; i < numConns; i++ {
		conn, err := dbconn.Driver.Connect("postgres", connStr)
		dbconn.handleConnectionError(err)
		conn.SetMaxOpenConns(1)
		conn.SetMaxIdleConns(1)
		dbconn.ConnPool[i] = conn
	}
	dbconn.NumConns = numConns
}

func (dbconn *DBConn) handleConnectionError(err error) {
	if err != nil {
		if strings.Contains(err.Error(), "does not exist") {
			if strings.Contains(err.Error(), "pq: role") {
				logger.Fatal(errors.Errorf(`Role "%s" does not exist, exiting`, dbconn.User), "")
			} else if strings.Contains(err.Error(), "pq: database") {
				logger.Fatal(errors.Errorf(`Database "%s" does not exist, exiting`, dbconn.DBName), "")
			}
		} else if strings.Contains(err.Error(), "connection refused") {
			logger.Fatal(errors.Errorf(`could not connect to server: Connection refused
	Is the server running on host "%s" and accepting
	TCP/IP connections on port %d?`, dbconn.Host, dbconn.Port), "")
		}
	}
	CheckError(err)
}

/*
 * Wrapper functions for built-in sqlx and database/sql functionality; they will
 * automatically execute the query as part of an existing transaction if one is
 * in progress, to ensure that the whole backup process occurs in one transaction
 * without requiring that to be ensured at the call site.
 */

func (dbconn *DBConn) Exec(query string, whichConn ...int) (sql.Result, error) {
	if dbconn.Tx != nil {
		return dbconn.Tx.Exec(query)
	}
	connNum := dbconn.ValidateConnNum(whichConn...)
	return dbconn.ConnPool[connNum].Exec(query)
}

func (dbconn *DBConn) MustExec(query string, whichConn ...int) {
	_, err := dbconn.Exec(query, whichConn...)
	CheckError(err)
}

func (dbconn *DBConn) Get(destination interface{}, query string, whichConn ...int) error {
	if dbconn.Tx != nil {
		return dbconn.Tx.Get(destination, query)
	}
	connNum := dbconn.ValidateConnNum(whichConn...)
	return dbconn.ConnPool[connNum].Get(destination, query)
}

func (dbconn *DBConn) Select(destination interface{}, query string, whichConn ...int) error {
	if dbconn.Tx != nil {
		return dbconn.Tx.Select(destination, query)
	}
	connNum := dbconn.ValidateConnNum(whichConn...)
	return dbconn.ConnPool[connNum].Select(destination, query)
}

/*
 * Ensure there isn't a mismatch between the connection pool size and number of
 * jobs, and default to using the first connection if no number is given.
 */
func (dbconn *DBConn) ValidateConnNum(whichConn ...int) int {
	if len(whichConn) == 0 {
		return 0
	}
	if len(whichConn) != 1 {
		logger.Fatal(errors.Errorf("At most one connection number may be specified for a given connection"), "")
	}
	if whichConn[0] < 0 || whichConn[0] >= dbconn.NumConns {
		logger.Fatal(errors.Errorf("Invalid connection number: %d", whichConn[0]), "")
	}
	return whichConn[0]
}

/*
 * Other useful/helper functions involving DBConn
 */

func escapeConnectionParam(param string) string {
	param = strings.Replace(param, `\`, `\\`, -1)
	param = strings.Replace(param, `'`, `\'`, -1)
	return param
}

func (dbconn *DBConn) GetDBSize() string {
	size := struct{ DBSize string }{}
	sizeQuery := fmt.Sprintf("SELECT pg_size_pretty(sodddatsize) as dbsize FROM gp_toolkit.gp_size_of_database WHERE sodddatname=E'%s'", escapeConnectionParam(dbconn.DBName))
	err := dbconn.Get(&size, sizeQuery)
	CheckError(err)
	return size.DBSize
}

func (dbconn *DBConn) SetDatabaseVersion() {
	dbconn.Version.Initialize(dbconn)
	dbconn.validateGPDBVersionCompatibility()
}

func (dbconn *DBConn) validateGPDBVersionCompatibility() {
	if dbconn.Version.Before(MINIMUM_GPDB4_VERSION) {
		logger.Fatal(errors.Errorf(`GPDB version %s is not supported. Please upgrade to GPDB %s.0 or later.`, dbconn.Version.VersionString, MINIMUM_GPDB4_VERSION), "")
	} else if dbconn.Version.Is("5") && dbconn.Version.Before(MINIMUM_GPDB5_VERSION) {
		logger.Fatal(errors.Errorf(`GPDB version %s is not supported. Please upgrade to GPDB %s or later.`, dbconn.Version.VersionString, MINIMUM_GPDB5_VERSION), "")
	}
}

/*
 * This is a convenience function for Select() when we're selecting single string
 * that may be NULL or not exist.  We can't use Get() because that expects exactly
 * one string and will panic if no rows are returned, even if using a sql.NullString.
 */
func SelectString(connection *DBConn, query string, whichConn ...int) string {
	results := make([]struct{ String string }, 0)
	connNum := connection.ValidateConnNum(whichConn...)
	err := connection.Select(&results, query, connNum)
	CheckError(err)
	if len(results) == 1 {
		return results[0].String
	} else if len(results) > 1 {
		logger.Fatal(errors.Errorf("Too many rows returned from query: got %d rows, expected 1 row", len(results)), "")
	}
	return ""
}

// This is a convenience function for Select() when we're selecting single strings.
func SelectStringSlice(connection *DBConn, query string, whichConn ...int) []string {
	results := make([]struct{ String string }, 0)
	connNum := connection.ValidateConnNum(whichConn...)
	err := connection.Select(&results, query, connNum)
	CheckError(err)
	retval := make([]string, 0)
	for _, str := range results {
		if str.String != "" {
			retval = append(retval, str.String)
		}
	}
	return retval
}

// TODO: Uniquely identify COPY commands in the multiple data file case to allow terminating sessions
func TerminateHangingCopySessions(connection *DBConn, cluster Cluster, appName string) {
	copyFileName := fmt.Sprintf("%s_%d", cluster.GetSegmentPipePathForCopyCommand(), cluster.PID)
	query := fmt.Sprintf(`SELECT
	pg_terminate_backend(procpid)
FROM pg_stat_activity
WHERE application_name = '%s'
AND current_query LIKE '%%%s%%'
AND procpid <> pg_backend_pid()`, appName, copyFileName)
	connection.MustExec(query)
}
