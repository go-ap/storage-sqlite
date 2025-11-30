//go:build cgo

package sqlite

import (
	"database/sql"
	"net/url"

	"github.com/mattn/go-sqlite3"
	_ "github.com/mattn/go-sqlite3"
)

var (
	ErrPerm     = sqlite3.ErrPerm
	ErrInternal = sqlite3.ErrInternal
)

type Error = sqlite3.Error

var defaultQueryParam = url.Values{
	"_txlock": []string{"immediate"},
	// from BJohnson's recommendations to use with litestream
	//"_journal_mode": []string{"WAL"},
	"_busy_timeout": []string{"5000"},
}

// sqlOpen will use the github.com/mattn/go-sqlite3 package when compiled with CGO
// this driver is more performant but, as said, it requires CGO
var sqlOpen = func(dataSourceName string) (*sql.DB, error) {
	return sql.Open("sqlite3", dataSourceName+"?"+defaultQueryParam.Encode())
}
