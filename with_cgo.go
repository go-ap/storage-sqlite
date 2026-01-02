//go:build cgo

package sqlite

import (
	"database/sql"
	"net/url"
	"strconv"

	_ "github.com/mattn/go-sqlite3"
)

type sqlError struct {
	Code         int
	ExtendedCode int
	SystemErrno  int
	Err          string
}

func (e sqlError) Error() string {
	return e.Err
}

var errCantOpen = sqlError{
	Code:         14,
	ExtendedCode: 14,
	SystemErrno:  0x0d,
	Err:          "unable to open database file: permission denied",
}

var errNoSuchTable = &sqlError{
	Code:         1,
	ExtendedCode: 1,
	Err:          "no such table: activities",
}

var defaultQueryParam = url.Values{
	"_txlock": []string{"immediate"},
	// from BJohnson's recommendations to use with litestream
	//"_journal_mode": []string{"WAL"},
	"_busy_timeout": []string{strconv.Itoa(int(2 * defaultTimeout.Seconds()))},
}

// sqlOpen will use the github.com/mattn/go-sqlite3 package when compiled with CGO
// this driver is more performant but, as said, it requires CGO
var sqlOpen = func(dataSourceName string) (*sql.DB, error) {
	return sql.Open("sqlite3", dataSourceName+"?"+defaultQueryParam.Encode())
}
