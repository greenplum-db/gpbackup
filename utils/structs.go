package utils

/*
 * This file contains structs and functions used as entry points for
 * unit testing via dependency injection.
 */

import (
	"github.com/jmoiron/sqlx"
)

/*
 * Structs and functions for testing database functions
 */

type DBDriver interface {
	Connect(driverName string, dataSourceName string) (*sqlx.DB, error)
}

type GPDBDriver struct {
}

func (driver GPDBDriver) Connect(driverName string, dataSourceName string) (*sqlx.DB, error) {
	return sqlx.Connect(driverName, dataSourceName)
}
