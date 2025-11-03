//go:build !cgo

package sqlite

import (
	"database/sql"
	"net/url"

	_ "modernc.org/sqlite"
)

var defaultQueryParam = url.Values{
	"_txlock": []string{"immediate"},
	"_pragma": []string{
		// Faster synchronization that still keeps the data safe:
		//"temp_store=MEMORY",
		//"synchronous=NORMAL",
		// Increase cache size (in this case to 64MB), the default is 2MB
		//"cache_size=-64000",
		// from BJohnson's recommendations to use with litestream
		//"journal_mode=WAL",
		"busy_timeout=5000",
		//"wal_autocheckpoint=0",
	},
}

// sqlOpen will use the learnc.org/sqlite when compiled without CGO
// this driver is less performant.
var sqlOpen = func(dataSourceName string) (*sql.DB, error) {
	return sql.Open("sqlite", dataSourceName+"?"+defaultQueryParam.Encode())
}
