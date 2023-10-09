//go:build cgo

package sqlite

import (
	"database/sql"

	_ "github.com/mattn/go-sqlite3"
)

// sqlOpen will use the github.com/mattn/go-sqlite3 package when compiled with CGO
// this driver is more performant but, as said, it requires CGO
var sqlOpen = func(dataSourceName string) (*sql.DB, error) {
	return sql.Open("sqlite3", dataSourceName)
}
