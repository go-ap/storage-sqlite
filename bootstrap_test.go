package sqlite

import (
	"database/sql"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/carlmjohnson/be"
	vocab "github.com/go-ap/activitypub"
	"github.com/go-ap/errors"
	"github.com/google/go-cmp/cmp"
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

func TestBootstrap1(t *testing.T) {
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
			if !cmp.Equal(tt.err, err, EquateWeakErrors) {
				t.Errorf("Bootstrap() error %s", cmp.Diff(tt.err, err, EquateWeakErrors))
			}
		})
	}
}

func createForbiddenDir(t *testing.T) string {
	forbiddenPath := filepath.Join(t.TempDir(), "forbidden")
	err := os.MkdirAll(forbiddenPath, 0o000)
	if err != nil {
		t.Fatalf("unable to create forbidden test path %s: %s", forbiddenPath, err)
	}
	return forbiddenPath
}

func TestBootstrap(t *testing.T) {
	forbiddenPath := createForbiddenDir(t)
	tests := []struct {
		name    string
		arg     Config
		wantErr error
	}{
		{
			name:    "empty",
			arg:     Config{},
			wantErr: os.ErrNotExist,
		},
		{
			name: "temp",
			arg:  Config{Path: filepath.Join(t.TempDir())},
		},
		{
			name:    "deeper than forbidden",
			arg:     Config{Path: filepath.Join(forbiddenPath, "should-fail")},
			wantErr: &fs.PathError{Op: "stat", Path: filepath.Join(forbiddenPath, "should-fail"), Err: syscall.EACCES},
		},
		{
			name: "forbidden",
			arg:  Config{Path: forbiddenPath},
			wantErr: errors.Annotatef(
				errCantOpen,
				`unable to execute: "CREATE TABLE IF NOT EXISTS objects (  "raw" TEXT,  "iri" TEXT NOT NULL constraint objects_key unique,  "id" TEXT GENERATED ALWAYS AS (json_extract(raw, '$.id')) VIRTUAL ,  "type" TEXT GENERATED ALWAYS AS (json_extract(raw, '$.type')) VIRTUAL,  "to" TEXT GENERATED ALWAYS AS (json_extract(raw, '$.to')) VIRTUAL,  "bto" TEXT GENERATED ALWAYS AS (json_extract(raw, '$.bto')) VIRTUAL,  "cc" TEXT GENERATED ALWAYS AS (json_extract(raw, '$.cc')) VIRTUAL,  "bcc" TEXT GENERATED ALWAYS AS (json_extract(raw, '$.bcc')) VIRTUAL,  "published" TEXT GENERATED ALWAYS AS (json_extract(raw, '$.published')) VIRTUAL,  "updated" TEXT GENERATED ALWAYS AS (coalesce(json_extract(raw, '$.updated'), json_extract(raw, '$.deleted'), json_extract(raw, '$.published'))) VIRTUAL,  "url" TEXT GENERATED ALWAYS AS (json_extract(raw, '$.url')) VIRTUAL,  "name" TEXT GENERATED ALWAYS AS (json_extract(raw, '$.name')) VIRTUAL,  "summary" TEXT GENERATED ALWAYS AS (json_extract(raw, '$.summary')) VIRTUAL,  "content" TEXT GENERATED ALWAYS AS (json_extract(raw, '$.content')) VIRTUAL) STRICT;CREATE INDEX objects_type ON objects(type);CREATE INDEX objects_name ON objects(name);CREATE INDEX objects_content ON objects(content);CREATE INDEX objects_published ON objects(published);CREATE INDEX objects_updated ON objects(updated);"`,
			),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := Bootstrap(tt.arg); !cmp.Equal(err, tt.wantErr, EquateWeakErrors) {
				t.Errorf("Bootstrap() error = %s", cmp.Diff(tt.wantErr, err, EquateWeakErrors))
				return
			}
			if tt.wantErr != nil {
				return
			}

			ff := fields{
				path: tt.arg.Path,
			}
			r := mockRepo(t, ff, withOpenRoot)
			defer r.Close()

		})
	}
}

func TestClean(t *testing.T) {
	forbiddenPath := createForbiddenDir(t)
	tests := []struct {
		name    string
		arg     Config
		wantErr error
	}{
		{
			name:    "empty",
			arg:     Config{},
			wantErr: nil,
		},
		{
			name:    "temp - exists, but empty",
			arg:     Config{Path: t.TempDir()},
			wantErr: nil,
		},
		{
			name:    "temp - does not exists",
			arg:     Config{Path: filepath.Join(t.TempDir(), "test")},
			wantErr: nil,
		},
		{
			name:    "invalid path " + os.DevNull,
			arg:     Config{Path: os.DevNull},
			wantErr: errors.Errorf("path exists, and is not a folder %s", os.DevNull),
		},
		{
			name:    "deeper than forbidden",
			arg:     Config{Path: filepath.Join(forbiddenPath, "should-fail")},
			wantErr: &fs.PathError{Op: "stat", Path: filepath.Join(forbiddenPath, "should-fail"), Err: syscall.EACCES},
		},
		{
			name:    "forbidden",
			arg:     Config{Path: forbiddenPath},
			wantErr: &fs.PathError{Op: "open", Path: forbiddenPath, Err: syscall.EACCES},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := Clean(tt.arg); !cmp.Equal(err, tt.wantErr, EquateWeakErrors) {
				t.Errorf("Clean() error = %s", cmp.Diff(tt.wantErr, err, EquateWeakErrors))
			}
		})
	}
}

func Test_repo_Reset(t *testing.T) {
	tests := []struct {
		name     string
		fields   fields
		setupFns []initFn
	}{
		{
			name: "not empty",
			fields: fields{
				path: t.TempDir(),
			},
			setupFns: []initFn{withOpenRoot, withBootstrap, withMockItems, withClient, withAccess, withAuthorization},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := mockRepo(t, tt.fields, tt.setupFns...)
			t.Cleanup(r.Close)

			// Reset closes the db
			r.Reset()

			for _, table := range tables {
				if r.conn != nil {
					t.Errorf("Reset() left db connection open")
				}
				_ = r.Open()

				var count sql.NullInt32
				err := r.conn.QueryRow(fmt.Sprintf("select count(*) FROM %s WHERE true", table)).Scan(&count)
				if err != nil {
					t.Errorf("Reset() left table in invalid state: %s", err)
					return
				}
				if !count.Valid {
					t.Errorf("Reset() left table in invalid state: %v", count)
					return
				}
				if count.Int32 > 0 {
					t.Errorf("Reset() left table with existing rows: %d", count.Int32)
				}
				r.Close()
			}
		})
	}
}
