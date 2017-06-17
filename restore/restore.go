package restore

import (
	"flag"
	"fmt"

	"github.com/greenplum-db/gpbackup/utils"

	"github.com/pkg/errors"
)

var (
	connection *utils.DBConn
	logger     *utils.Logger
)

var ( // Command-line flags
	debug     = flag.Bool("debug", false, "Print verbose and debug log messages")
	dumpDir   = flag.String("dumpdir", "", "The directory in which the dump files to be restored are located")
	quiet     = flag.Bool("quiet", false, "Suppress non-warning, non-error log messages")
	timestamp = flag.String("timestamp", "", "The timestamp to be restored, in the format YYYYMMDDHHMMSS")
	verbose   = flag.Bool("verbose", false, "Print verbose log messages")
)

// This function handles setup that can be done before parsing flags.
func DoInit() {
	SetLogger(utils.InitializeLogging("gprestore", ""))
}

func SetLogger(log *utils.Logger) {
	logger = log
}

/*
* This function handles argument parsing and validation, e.g. checking that a passed filename exists.
* It should only validate; initialization with any sort of side effects should go in DoInit or DoSetup.
 */
func DoValidation() {
	flag.Parse()
	utils.CheckExclusiveFlags("debug", "quiet", "verbose")
	utils.CheckMandatoryFlags("timestamp")
	if !utils.IsValidTimestamp(*timestamp) {
		logger.Fatal(errors.Errorf("Timestamp %s is invalid.  Timestamps must be in the format YYYYMMDDHHMMSS.", *timestamp), "")
	}
}

// This function handles setup that must be done after parsing flags.
func DoSetup() {
	if *quiet {
		logger.SetVerbosity(utils.LOGERROR)
	} else if *debug {
		logger.SetVerbosity(utils.LOGDEBUG)
	} else if *verbose {
		logger.SetVerbosity(utils.LOGVERBOSE)
	}
	connection = utils.NewDBConn("postgres")
	connection.Connect()
	connection.Exec("SET application_name TO 'gprestore'")

	utils.SetDumpTimestamp(*timestamp)

	if *dumpDir != "" {
		utils.BaseDumpDir = *dumpDir
	}
	logger.Verbose("Gathering information on dump directories")
	segConfig := utils.GetSegmentConfiguration(connection)
	utils.SetupSegmentConfiguration(segConfig)
}

func DoRestore() {
	logger.Info("Restore Key = %s", utils.DumpTimestamp)

	utils.AssertDumpDirsExist()

	masterDumpDir := utils.GetDirForContent(-1)
	globalFilename := fmt.Sprintf("%s/global.sql", masterDumpDir)
	predataFilename := fmt.Sprintf("%s/predata.sql", masterDumpDir)
	postdataFilename := fmt.Sprintf("%s/postdata.sql", masterDumpDir)

	logger.Info("Restoring global database metadata from %s", globalFilename)
	restoreGlobal(globalFilename)
	logger.Info("Global database metadata restore complete")

	logger.Info("Restoring pre-data metadata from %s", predataFilename)
	restorePredata(predataFilename)
	logger.Info("Pre-data metadata restore complete")

	//TODO: Restore data
	//logger.Info("Restoring data")
	//restoreData(tables)
	//logger.Info("Data restore complete")

	logger.Info("Restoring post-data metadata from %s", postdataFilename)
	restorePostdata(postdataFilename)
	logger.Info("Post-data metadata restore complete")
}

func restoreGlobal(filename string) {
	utils.ExecuteSQLFile(connection, filename)
}

func restorePredata(filename string) {
	utils.ExecuteSQLFile(connection, filename)
}

func restoreData() {
}

func restorePostdata(filename string) {
}

func DoTeardown() {
	if r := recover(); r != nil {
		fmt.Println(r)
	}
	if connection != nil {
		connection.Close()
	}
	// TODO: Add logic for error codes based on whether we Abort()ed or not
}
