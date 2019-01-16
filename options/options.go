package options

import (
	"regexp"

	"github.com/spf13/pflag"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"

	"github.com/greenplum-db/gp-common-go-libs/iohelper"
	"github.com/greenplum-db/gpbackup/utils"
	"github.com/pkg/errors"
)

type Options struct {
	includedRelations []string
}

func (o Options) GetIncludedTables() []string {
	return o.includedRelations
}

func NewOptions(initialFlags *pflag.FlagSet, connectionPool *dbconn.DBConn, validator DbValidator) (*Options, error) {
	tableNames, err := initialFlags.GetStringSlice(utils.INCLUDE_RELATION)
	if err != nil {
		return nil, err
	}

	// flag for INCLUDE_RELATION_FILE is mutually exclusive with INCLUDE_RELATION flag
	// so this is not an overwrite, we are just skipping testing if either was set
	filename, err := initialFlags.GetString(utils.INCLUDE_RELATION_FILE)
	if err != nil {
		return nil, err
	}
	if filename != "" {
		tableNames = iohelper.MustReadLinesFromFile(filename)
	}

	err = validateCharacters(tableNames)
	if err != nil {
		return nil, err
	}

	validator.ValidateInDatabase(tableNames, connectionPool)

	return &Options{
		includedRelations: tableNames,
	}, nil
}

func validateCharacters(tableList []string) error {
	if len(tableList) == 0 {
		return nil
	}

	validFormat := regexp.MustCompile(`^.*\..*$`)
	for _, fqn := range tableList {
		if !validFormat.Match([]byte(fqn)) {
			return errors.Errorf(`Table %s is not correctly fully-qualified.  Please ensure that it is in the format schema.table, it is quoted appropriately, and it has no preceding or trailing whitespace.`, fqn)
		}
	}

	return nil
}
