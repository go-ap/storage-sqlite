//go:build !cgo

package sqlite

import (
	"database/sql"

	_ "modernc.org/sqlite"
)

// sqlOpen will use the learnc.org/sqlite when compiled without CGO
// this driver is less performant.
var sqlOpen = func(dataSourceName string) (*sql.DB, error) {
	return sql.Open("sqlite", dataSourceName)
}
