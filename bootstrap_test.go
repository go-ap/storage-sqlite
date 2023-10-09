package sqlite

import (
	"database/sql"
	"testing"

	"github.com/carlmjohnson/be"
	vocab "github.com/go-ap/activitypub"
)

func checkInsertedValue(t *testing.T, db *sql.DB, actor vocab.Item) {
	sel := "SELECT id, raw FROM objects WHERE id = ?;"
	res, err := db.Query(sel, actor.GetLink())
	be.NilErr(t, err)
	be.Nonzero(t, res)
	defer res.Close()

	for res.Next() {
		var id string
		var raw []byte

		err := res.Scan(&id, &raw)
		be.NilErr(t, err)
		be.Equal(t, actor.GetLink().String(), id)

		incraw, err := vocab.MarshalJSON(actor)
		be.NilErr(t, err)

		be.Equal(t, string(incraw), string(raw))
	}
}

func TestBootstrap(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{
			name: "noop",
			err:  nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conf := Config{
				Path:        t.TempDir(),
				CacheEnable: false,
				LogFn:       t.Logf,
				ErrFn:       t.Errorf,
			}

			err := Bootstrap(conf)
			checkErrorsEqual(t, err, tt.err)
		})
	}
}
