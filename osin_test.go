package sqlite

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/carlmjohnson/be"
	vocab "github.com/go-ap/activitypub"
	"github.com/go-ap/errors"
	"github.com/google/go-cmp/cmp"
	"github.com/openshift/osin"
)

type initFn func(db *sql.DB) error

func initializeOsinDb(t *testing.T, fns ...initFn) repo {
	dbPath := filepath.Join(t.TempDir(), "storage.sqlite")
	r := repo{path: dbPath, logFn: t.Logf, errFn: t.Errorf}
	_ = r.Open()

	be.NilErr(t, bootstrapOsin(r))

	for _, fn := range fns {
		be.NilErr(t, fn(r.conn))
	}
	return r
}

func Test_repo_Clone(t *testing.T) {
	s := initializeOsinDb(t)
	got := s.Clone()

	cloned, ok := got.(*repo)
	be.True(t, ok)
	s.logFn = nil
	s.errFn = nil
	cloned.logFn = nil
	cloned.errFn = nil
	be.DeepEqual[*repo](t, &s, cloned)
}

func Test_repo_CreateClient(t *testing.T) {
	tests := []struct {
		name string
		init []initFn
		arg  osin.Client
		err  error
	}{
		{
			name: "empty",
			arg:  nil,
			err:  nilClientErr,
		},
		{
			name: "default client",
			arg:  &osin.DefaultClient{Id: "test", Secret: "test", RedirectUri: "/"},
		},
		{
			name: "default client with user data",
			arg:  &osin.DefaultClient{Id: "test", Secret: "test", RedirectUri: "/", UserData: "https://example.com"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := initializeOsinDb(t, tt.init...)
			err := s.CreateClient(tt.arg)
			checkErrorsEqual(t, tt.err, err)

			if tt.arg == nil {
				return
			}

			err = s.Open()
			be.NilErr(t, err)

			sel := "SELECT code, secret, redirect_uri, extra from clients where code=?;"
			res, err := s.conn.Query(sel, tt.arg.GetId())
			be.NilErr(t, err)

			for res.Next() {
				var code string
				var secret string
				var redir string
				var extra []byte

				err := res.Scan(&code, &secret, &redir, &extra)
				be.NilErr(t, err)

				be.Equal(t, tt.arg.GetId(), code)
				be.Equal(t, tt.arg.GetSecret(), secret)
				be.Equal(t, tt.arg.GetRedirectUri(), redir)

				if tt.arg.GetUserData() != nil {
					ud, err := assertToBytes(tt.arg.GetUserData())
					be.NilErr(t, err)
					be.DeepEqual(t, ud, extra)
				}
			}
		})
	}
}

func Test_repo_GetClient(t *testing.T) {
	tests := []struct {
		name string
		init []initFn
		arg  string
		want osin.Client
		err  error
	}{
		{
			name: "missing",
			arg:  "missing",
			err:  errClientNotFound(nil),
		},
		{
			name: "found",
			init: []initFn{
				func(db *sql.DB) error {
					_, err := db.Exec(createClient, "found", "secret", "redirURI", any("extra123"))
					return err
				},
			},
			arg: "found",
			want: &cl{
				Id:          "found",
				Secret:      "secret",
				RedirectUri: "redirURI",
				UserData:    "extra123",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := initializeOsinDb(t, tt.init...)
			got, err := s.GetClient(tt.arg)
			checkErrorsEqual(t, tt.err, err)
			if !cmp.Equal(tt.want, got) {
				t.Errorf("Different client received, from expected: %s", cmp.Diff(tt.want, got))
			}
		})
	}
}

func Test_repo_ListClients(t *testing.T) {
	tests := []struct {
		name string
		init []initFn
		want []osin.Client
		err  error
	}{
		{
			name: "missing",
			want: []osin.Client{},
		},
		{
			name: "found",
			init: []initFn{
				func(db *sql.DB) error {
					_, err := db.Exec(createClient, "found", "secret", "redirURI", any("extra123"))
					return err
				},
			},
			want: []osin.Client{
				&cl{
					Id:          "found",
					Secret:      "secret",
					RedirectUri: "redirURI",
					UserData:    "extra123",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := initializeOsinDb(t, tt.init...)
			defer s.Close()

			got, err := s.ListClients()
			checkErrorsEqual(t, tt.err, err)
			be.DeepEqual(t, tt.want, got)
		})
	}
}

var hellTimeStr = "2666-06-06 06:06:06"
var hellTime, _ = time.Parse("2006-01-02 15:04:05", hellTimeStr)

func Test_repo_LoadAccess(t *testing.T) {
	tests := []struct {
		name string
		init []initFn
		arg  string
		want *osin.AccessData
		err  error
	}{
		{
			name: "empty",
			arg:  "",
			want: nil,
			err:  errors.Newf("Empty access code"),
		},
		{
			name: "find one",
			arg:  "one",
			init: []initFn{
				func(db *sql.DB) error {
					_, _ = db.Exec(createClient, "client", "secret", "redir", "extra123")
					_, _ = db.Exec(saveAuthorize, "client", "auth", "666", "scop", "redir", "state", hellTimeStr, "extra123")
					_, err := db.Exec(saveAccess, "client", "auth", nil, "one", "ref", "666", "scope", "redir", hellTimeStr, "extra")
					return err
				},
			},
			want: &osin.AccessData{
				Client: &cl{
					Id:          "client",
					Secret:      "secret",
					RedirectUri: "redir",
					UserData:    "extra123",
				},
				AuthorizeData: &osin.AuthorizeData{
					Client: &cl{
						Id:          "client",
						Secret:      "secret",
						RedirectUri: "redir",
						UserData:    "extra123",
					},
					Code:        "auth",
					ExpiresIn:   666,
					Scope:       "scop",
					RedirectUri: "redir",
					State:       "state",
					CreatedAt:   hellTime,
					UserData:    vocab.IRI("extra123"),
				},
				AccessToken:  "one",
				RefreshToken: "ref",
				ExpiresIn:    666,
				Scope:        "scope",
				RedirectUri:  "redir",
				CreatedAt:    hellTime,
				UserData:     vocab.IRI("extra"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := initializeOsinDb(t, tt.init...)
			got, err := s.LoadAccess(tt.arg)
			checkErrorsEqual(t, tt.err, err)
			if !cmp.Equal(tt.want, got) {
				t.Errorf("Different access data received, from expected: %s", cmp.Diff(tt.want, got))
			}
		})
	}
}

func Test_repo_LoadAuthorize(t *testing.T) {
	tests := []struct {
		name string
		init []initFn
		arg  string
		want *osin.AuthorizeData
		err  error
	}{
		{
			name: "empty",
			arg:  "",
			want: nil,
			err:  errors.Newf("Empty authorize code"),
		},
		{
			name: "find one",
			arg:  "one",
			init: []initFn{
				func(db *sql.DB) error {
					_, _ = db.Exec(createClient, "client", "secret", "redir", "extra123")
					_, err := db.Exec(saveAuthorize, "client", "one", "666", "scop", "redir", "state", hellTimeStr, "extra123")
					return err
				},
			},
			want: &osin.AuthorizeData{
				Client: &cl{
					Id:          "client",
					Secret:      "secret",
					RedirectUri: "redir",
					UserData:    "extra123",
				},
				Code:        "one",
				ExpiresIn:   666,
				Scope:       "scop",
				RedirectUri: "redir",
				State:       "state",
				CreatedAt:   hellTime,
				UserData:    vocab.IRI("extra123"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := initializeOsinDb(t, tt.init...)
			got, err := s.LoadAuthorize(tt.arg)
			checkErrorsEqual(t, tt.err, err)
			if !cmp.Equal(tt.want, got) {
				t.Errorf("Different authorize data received, from expected: %s", cmp.Diff(tt.want, got))
			}
		})
	}
}

func Test_repo_LoadRefresh(t *testing.T) {
	tests := []struct {
		name string
		init []initFn
		arg  string
		want *osin.AccessData
		err  error
	}{
		{
			name: "empty",
			arg:  "",
			want: nil,
			err:  errors.Newf("Empty refresh code"),
		},
		{
			name: "find refresh",
			arg:  "ref1",
			init: []initFn{
				func(db *sql.DB) error {
					_, _ = db.Exec(createClient, "client", "secret", "redir", "extra123")
					_, _ = db.Exec(saveAuthorize, "client", "auth", "666", "scop", "redir", "state", hellTimeStr, "extra123")
					_, _ = db.Exec(saveAccess, "client", "auth", nil, "one", "ref", "666", "scope", "redir", hellTimeStr, "extra")
					_, err := db.Exec(saveRefresh, "ref1", "one")
					return err
				},
			},
			want: &osin.AccessData{
				Client: &cl{
					Id:          "client",
					Secret:      "secret",
					RedirectUri: "redir",
					UserData:    "extra123",
				},
				AuthorizeData: &osin.AuthorizeData{
					Client: &cl{
						Id:          "client",
						Secret:      "secret",
						RedirectUri: "redir",
						UserData:    "extra123",
					},
					Code:        "auth",
					ExpiresIn:   666,
					Scope:       "scop",
					RedirectUri: "redir",
					State:       "state",
					CreatedAt:   hellTime,
					UserData:    vocab.IRI("extra123"),
				},
				AccessToken:  "one",
				RefreshToken: "ref",
				ExpiresIn:    666,
				Scope:        "scope",
				RedirectUri:  "redir",
				CreatedAt:    hellTime,
				UserData:     vocab.IRI("extra"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := initializeOsinDb(t, tt.init...)
			got, err := s.LoadRefresh(tt.arg)
			checkErrorsEqual(t, tt.err, err)
			if !cmp.Equal(tt.want, got) {
				t.Errorf("Different refresh data received, from expected: %s", cmp.Diff(tt.want, got))
			}
		})
	}
}

func Test_repo_openOsin(t *testing.T) {
	tests := []struct {
		name string
		init []initFn
		err  error
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := initializeOsinDb(t, tt.init...)
			err := s.Open()
			checkErrorsEqual(t, tt.err, err)
		})
	}
}

func Test_repo_RemoveAccess(t *testing.T) {
	tests := []struct {
		name string
		init []initFn
		arg  string
		err  error
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := initializeOsinDb(t, tt.init...)
			err := s.RemoveAccess(tt.arg)
			checkErrorsEqual(t, tt.err, err)
		})
	}
}

func Test_repo_RemoveAuthorize(t *testing.T) {
	tests := []struct {
		name string
		init []initFn
		arg  string
		err  error
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := initializeOsinDb(t, tt.init...)
			err := s.RemoveAuthorize(tt.arg)
			checkErrorsEqual(t, tt.err, err)
		})
	}
}

func Test_repo_RemoveClient(t *testing.T) {
	tests := []struct {
		name string
		init []initFn
		arg  string
		err  error
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := initializeOsinDb(t, tt.init...)
			err := s.RemoveClient(tt.arg)
			checkErrorsEqual(t, tt.err, err)
		})
	}
}

func Test_repo_RemoveRefresh(t *testing.T) {
	tests := []struct {
		name string
		init []initFn
		arg  string
		err  error
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := initializeOsinDb(t, tt.init...)
			err := s.RemoveRefresh(tt.arg)
			checkErrorsEqual(t, tt.err, err)
		})
	}
}

func Test_repo_SaveAccess(t *testing.T) {
	tests := []struct {
		name string
		init []initFn
		arg  *osin.AccessData
		err  error
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := initializeOsinDb(t, tt.init...)
			err := s.SaveAccess(tt.arg)
			checkErrorsEqual(t, tt.err, err)
		})
	}
}

func Test_repo_SaveAuthorize(t *testing.T) {
	tests := []struct {
		name string
		init []initFn
		arg  *osin.AuthorizeData
		err  error
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := initializeOsinDb(t, tt.init...)
			err := s.SaveAuthorize(tt.arg)
			checkErrorsEqual(t, tt.err, err)
		})
	}
}

func Test_repo_UpdateClient(t *testing.T) {
	tests := []struct {
		name string
		init []initFn
		arg  osin.Client
		err  error
	}{
		{
			name: "empty",
			err:  nilClientErr,
		},
		{
			name: "basic",
			init: []initFn{
				func(db *sql.DB) error {
					_, err := db.Exec(createClient, "found", "secret", "redirURI", any("extra123"))
					return err
				},
			},
			arg: &osin.DefaultClient{
				Id:          "found",
				Secret:      "secret",
				RedirectUri: "redirURI",
				UserData:    any("extra123"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := initializeOsinDb(t, tt.init...)
			err := s.UpdateClient(tt.arg)
			checkErrorsEqual(t, tt.err, err)
		})
	}
}
