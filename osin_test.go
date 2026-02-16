package sqlite

import (
	"reflect"
	"testing"
	"time"

	"github.com/carlmjohnson/be"
	vocab "github.com/go-ap/activitypub"
	"github.com/go-ap/errors"
	"github.com/google/go-cmp/cmp"
	"github.com/openshift/osin"
)

func Test_repo_CreateClient(t *testing.T) {
	tests := []struct {
		name     string
		setupFns []initFn
		arg      osin.Client
		err      error
	}{
		{
			name: "empty",
			arg:  nil,
			err:  errNotOpen,
		},
		{
			name:     "default client",
			setupFns: []initFn{withOpenRoot, withBootstrap},
			arg:      &osin.DefaultClient{Id: "test", Secret: "test", RedirectUri: "/"},
		},
		{
			name:     "default client with user data",
			setupFns: []initFn{withOpenRoot, withBootstrap},
			arg:      &osin.DefaultClient{Id: "test", Secret: "test", RedirectUri: "/", UserData: "https://example.com"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := mockRepo(t, fields{path: t.TempDir()}, tt.setupFns...)
			t.Cleanup(s.Close)
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
		name     string
		setupFns []initFn
		arg      string
		want     osin.Client
		err      error
	}{
		{
			name: "not open",
			arg:  "missing",
			err:  errNotOpen,
		},
		{
			name:     "missing",
			arg:      "missing",
			setupFns: []initFn{withOpenRoot, withBootstrap},
			err:      errClientNotFound(nil),
		},
		{
			name: "found",
			setupFns: []initFn{
				withOpenRoot,
				withBootstrap,
				func(t *testing.T, r *repo) *repo {
					if _, err := r.conn.Exec(createClient, "found", "secret", "redirURI", any("extra123")); err != nil {
						r.errFn("unable to save client: %s", err)
					}
					return r
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
			s := mockRepo(t, fields{path: t.TempDir()}, tt.setupFns...)
			t.Cleanup(s.Close)
			got, err := s.GetClient(tt.arg)
			if !cmp.Equal(tt.err, err, EquateWeakErrors) {
				t.Errorf("invalid error type received %s", cmp.Diff(tt.err, got, EquateWeakErrors))
			}
			if !cmp.Equal(tt.want, got) {
				t.Errorf("Different client received, from expected: %s", cmp.Diff(tt.want, got))
			}
		})
	}
}

var hellTimeStr = "2666-06-06 06:06:06"
var hellTime, _ = time.Parse("2006-01-02 15:04:05", hellTimeStr)

func Test_repo_LoadAuthorize(t *testing.T) {
	tests := []struct {
		name     string
		fields   fields
		setupFns []initFn
		code     string
		want     *osin.AuthorizeData
		err      error
	}{
		{
			name: "not open",
			err:  errNotOpen,
		},
		{
			name:     "empty",
			setupFns: []initFn{withOpenRoot, withBootstrap},
			err:      errors.Newf("Empty authorize code"),
		},
		{
			name:   "find one",
			code:   "one",
			fields: fields{path: t.TempDir()},
			setupFns: []initFn{
				withOpenRoot,
				withBootstrap,
				func(t *testing.T, r *repo) *repo {
					_, _ = r.conn.Exec(createClient, "client", "secret", "redir", "extra123")
					_, err := r.conn.Exec(saveAuthorize, "client", "one", "666", "scop", "redir", "state", hellTimeStr, "extra123", "0000000000000000000000000000000000000000123", "PLAIN")
					if err != nil {
						r.errFn("unable to save authorization data: %s", err)
					}
					return r
				},
			},
			want: &osin.AuthorizeData{
				Client: &osin.DefaultClient{
					Id:          "client",
					Secret:      "secret",
					RedirectUri: "redir",
					UserData:    "extra123",
				},
				Code:                "one",
				ExpiresIn:           666,
				Scope:               "scop",
				RedirectUri:         "redir",
				State:               "state",
				CreatedAt:           hellTime,
				UserData:            vocab.IRI("extra123"),
				CodeChallengeMethod: "PLAIN",
				CodeChallenge:       "0000000000000000000000000000000000000000123",
			},
		},
		{
			name:     "authorized",
			fields:   fields{path: t.TempDir()},
			code:     "test-code",
			setupFns: []initFn{withOpenRoot, withBootstrap, withClient, withAuthorization},
			want:     mockAuth("test-code", defaultClient),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := mockRepo(t, tt.fields, tt.setupFns...)
			got, err := s.LoadAuthorize(tt.code)
			checkErrorsEqual(t, tt.err, err)
			if !cmp.Equal(tt.want, got) {
				t.Errorf("Different authorize data received, from expected: %s", cmp.Diff(tt.want, got))
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
			s := mockRepo(t, fields{path: t.TempDir()}, tt.init...)
			err := s.Open()
			checkErrorsEqual(t, tt.err, err)
		})
	}
}

func Test_repo_RemoveAccess1(t *testing.T) {
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
			s := mockRepo(t, fields{path: t.TempDir()}, tt.init...)
			err := s.RemoveAccess(tt.arg)
			checkErrorsEqual(t, tt.err, err)
		})
	}
}

func Test_repo_UpdateClient(t *testing.T) {
	tests := []struct {
		name     string
		setupFns []initFn
		arg      osin.Client
		err      error
	}{
		{
			name: "empty",
			err:  errNotOpen,
		},
		{
			name: "basic",
			setupFns: []initFn{
				withOpenRoot,
				withBootstrap,
				func(t *testing.T, r *repo) *repo {
					if _, err := r.conn.Exec(createClient, "found", "secret", "redirURI", any("extra123")); err != nil {
						r.errFn("unable to create client: %s", err)
					}
					return r
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
			s := mockRepo(t, fields{path: t.TempDir()}, tt.setupFns...)
			t.Cleanup(s.Close)
			err := s.UpdateClient(tt.arg)
			checkErrorsEqual(t, tt.err, err)
		})
	}
}

func Test_repo_Clone(t *testing.T) {
	s := new(repo)
	ss := s.Clone()
	s1, ok := ss.(*repo)
	if !ok {
		t.Errorf("Error when cloning repoage, unable to convert interface back to %T: %T", s, ss)
	}
	if !reflect.DeepEqual(s, s1) {
		t.Errorf("Error when cloning repoage, invalid pointer returned %p: %p", s, s1)
	}
}

func Test_repo_LoadAccess(t *testing.T) {
	tests := []struct {
		name     string
		fields   fields
		setupFns []initFn
		code     string
		want     *osin.AccessData
		wantErr  error
	}{
		{
			name:    "not open",
			wantErr: errNotOpen,
		},
		{
			name:     "empty no bootstrap",
			fields:   fields{},
			setupFns: []initFn{withOpenRoot},
			wantErr:  errors.NotFoundf("Empty access code"),
		},
		{
			name:     "empty",
			fields:   fields{path: t.TempDir()},
			setupFns: []initFn{withOpenRoot, withBootstrap},
			code:     "",
			wantErr:  errors.Newf("Empty access code"),
		},
		{
			name:     "load access",
			fields:   fields{path: t.TempDir()},
			setupFns: []initFn{withOpenRoot, withBootstrap, withClient, withAuthorization, withAccess},
			code:     "access-666",
			want:     mockAccess("access-666", defaultClient),
			wantErr:  nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := mockRepo(t, tt.fields, tt.setupFns...)
			t.Cleanup(r.Close)

			got, err := r.LoadAccess(tt.code)
			if !cmp.Equal(err, tt.wantErr, EquateWeakErrors) {
				t.Errorf("LoadAccess() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr != nil {
				return
			}
			if !cmp.Equal(got, tt.want) {
				t.Errorf("LoadAccess() got %s", cmp.Diff(tt.want, got))
			}
		})
	}
}

func Test_repo_LoadRefresh(t *testing.T) {
	tests := []struct {
		name     string
		fields   fields
		setupFns []initFn
		code     string
		want     *osin.AccessData
		wantErr  error
	}{
		{
			name:    "not open",
			wantErr: errNotOpen,
		},
		{
			name:     "empty no bootstrap",
			fields:   fields{},
			setupFns: []initFn{withOpenRoot},
			wantErr:  errors.Newf("Empty refresh code"),
		},
		{
			name:     "empty",
			fields:   fields{path: t.TempDir()},
			setupFns: []initFn{withOpenRoot, withBootstrap},
			wantErr:  errors.Newf("Empty refresh code"),
		},
		{
			name:     "with refresh",
			fields:   fields{path: t.TempDir()},
			code:     "refresh-666",
			setupFns: []initFn{withOpenRoot, withBootstrap, withClient, withAuthorization, withAccess},
			want:     mockAccess("access-666", defaultClient),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := mockRepo(t, tt.fields, tt.setupFns...)
			t.Cleanup(r.Close)

			got, err := r.LoadRefresh(tt.code)
			if !cmp.Equal(err, tt.wantErr, EquateWeakErrors) {
				t.Errorf("LoadRefresh() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr != nil {
				return
			}
			if !cmp.Equal(got, tt.want) {
				t.Errorf("LoadRefresh() got %s", cmp.Diff(tt.want, got))
			}
		})
	}
}

func Test_repo_RemoveAccess(t *testing.T) {
	tests := []struct {
		name     string
		fields   fields
		setupFns []initFn
		code     string
		wantErr  error
	}{
		{
			name:    "empty",
			fields:  fields{},
			wantErr: errNotOpen,
		},
		{
			name:    "empty",
			fields:  fields{},
			code:    "test",
			wantErr: errNotOpen,
		},
		{
			name:     "remove access",
			fields:   fields{path: t.TempDir()},
			setupFns: []initFn{withOpenRoot, withBootstrap, withClient, withAuthorization, withAccess},
			code:     "access-666",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := mockRepo(t, tt.fields, tt.setupFns...)
			t.Cleanup(r.Close)

			if err := r.RemoveAccess(tt.code); !errors.Is(err, tt.wantErr) {
				t.Errorf("RemoveAccess() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_repo_RemoveAuthorize(t *testing.T) {
	tests := []struct {
		name     string
		fields   fields
		setupFns []initFn
		code     string
		wantErr  error
	}{
		{
			name:    "empty",
			fields:  fields{},
			wantErr: errNotOpen,
		},
		{
			name:     "remove auth",
			fields:   fields{path: t.TempDir()},
			setupFns: []initFn{withOpenRoot, withBootstrap, withClient, withAuthorization},
			code:     "test-auth",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := mockRepo(t, tt.fields, tt.setupFns...)
			if err := r.RemoveAuthorize(tt.code); !errors.Is(err, tt.wantErr) {
				t.Errorf("RemoveAuthorize() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_repo_RemoveClient(t *testing.T) {
	tests := []struct {
		name     string
		fields   fields
		setupFns []initFn
		code     string
		wantErr  error
	}{
		{
			name:    "empty",
			fields:  fields{},
			wantErr: errNotOpen,
		},
		{
			name:    "empty",
			fields:  fields{},
			code:    "test",
			wantErr: errNotOpen,
		},
		{
			name:     "remove client",
			fields:   fields{path: t.TempDir()},
			setupFns: []initFn{withOpenRoot, withBootstrap, withClient},
			code:     "test-client",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := mockRepo(t, tt.fields, tt.setupFns...)
			if err := r.RemoveClient(tt.code); !errors.Is(err, tt.wantErr) {
				t.Errorf("RemoveClient() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_repo_RemoveRefresh(t *testing.T) {
	tests := []struct {
		name     string
		fields   fields
		setupFns []initFn
		code     string
		wantErr  error
	}{
		{
			name:    "not open",
			fields:  fields{path: t.TempDir()},
			wantErr: errNotOpen,
		},
		{
			name:    "empty not open",
			fields:  fields{},
			code:    "test",
			wantErr: errNotOpen,
		},
		{
			name:     "empty",
			fields:   fields{path: t.TempDir()},
			setupFns: []initFn{withOpenRoot, withBootstrap},
			code:     "test",
		},
		{
			name:     "mock access",
			fields:   fields{path: t.TempDir()},
			setupFns: []initFn{withOpenRoot, withBootstrap, withAccess},
			code:     "access-666",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := mockRepo(t, tt.fields, tt.setupFns...)
			t.Cleanup(r.Close)

			if err := r.RemoveRefresh(tt.code); !errors.Is(err, tt.wantErr) {
				t.Errorf("RemoveRefresh() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_repo_SaveAuthorize(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		setupFns []initFn
		auth     *osin.AuthorizeData
		wantErr  error
	}{
		{
			name:    "not open",
			path:    t.TempDir(),
			wantErr: errNotOpen,
		},
		{
			name:     "empty",
			path:     t.TempDir(),
			setupFns: []initFn{withOpenRoot},
			wantErr:  errors.Newf("unable to save nil authorization data"),
		},
		{
			name:     "save mock auth",
			path:     t.TempDir(),
			setupFns: []initFn{withOpenRoot, withBootstrap, withClient},
			auth:     mockAuth("test-code123", defaultClient),
			wantErr:  nil,
		},
		{
			name:     "save mock auth with PKCE",
			path:     t.TempDir(),
			setupFns: []initFn{withOpenRoot, withBootstrap, withClient},
			auth:     mockAuthWithCodeChallenge("test-code123", defaultClient),
			wantErr:  nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := mockRepo(t, fields{path: tt.path}, tt.setupFns...)
			t.Cleanup(r.Close)

			err := r.SaveAuthorize(tt.auth)
			if tt.wantErr != nil {
				if err != nil {
					if tt.wantErr.Error() != err.Error() {
						t.Errorf("SaveAuthorize() error = %v, wantErr %v", err, tt.wantErr)
					}
				} else {
					t.Errorf("SaveAuthorize() error = %v, wantErr %v", err, tt.wantErr)
				}
				return
			}
			got, err := r.LoadAuthorize(tt.auth.Code)
			if tt.wantErr != nil {
				if err != nil {
					if tt.wantErr.Error() != err.Error() {
						t.Errorf("LoadAuthorize() after SaveAuthorize() error = %v, wantErr %v", err, tt.wantErr)
					}
				} else {
					t.Errorf("LoadAuthorize() after SaveAuthorize() error = %v, wantErr %v", err, tt.wantErr)
				}
				return
			}
			if !cmp.Equal(got, tt.auth) {
				t.Errorf("SaveAuthorize() diff %s", cmp.Diff(got, tt.auth))
			}
		})
	}
}

func Test_repo_SaveAccess(t *testing.T) {
	tests := []struct {
		name     string
		fields   fields
		setupFns []initFn
		data     *osin.AccessData
		wantErr  error
	}{
		{
			name:    "empty",
			fields:  fields{},
			wantErr: errNotOpen,
		},
		{
			name:     "save access",
			fields:   fields{path: t.TempDir()},
			setupFns: []initFn{withOpenRoot, withBootstrap, withClient, withAuthorization},
			data:     mockAccess("access-666", defaultClient),
			wantErr:  nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := mockRepo(t, tt.fields, tt.setupFns...)
			if err := r.SaveAccess(tt.data); !errors.Is(err, tt.wantErr) {
				t.Errorf("SaveAccess() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_repo_ListClients(t *testing.T) {
	tests := []struct {
		name     string
		fields   fields
		setupFns []initFn
		want     []osin.Client
		wantErr  error
	}{
		{
			name:    "empty",
			wantErr: errNotOpen,
		},
		{
			name:    "not open",
			wantErr: errNotOpen,
		},
		{
			name:     "missing",
			fields:   fields{path: t.TempDir()},
			setupFns: []initFn{withOpenRoot, withBootstrap},
		},
		{
			name:   "found",
			fields: fields{path: t.TempDir()},
			setupFns: []initFn{
				withOpenRoot,
				withBootstrap,
				func(t *testing.T, r *repo) *repo {
					if _, err := r.conn.Exec(createClient, "found", "secret", "redirURI", any("extra123")); err != nil {
						r.errFn("unable to save client: %s", err)
					}
					return r
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
			r := mockRepo(t, tt.fields, tt.setupFns...)
			t.Cleanup(r.Close)

			got, err := r.ListClients()
			if !cmp.Equal(err, tt.wantErr, EquateWeakErrors) {
				t.Errorf("ListClients() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !cmp.Equal(got, tt.want) {
				t.Errorf("ListClients() got = %s", cmp.Diff(got, tt.want))
			}
		})
	}
}

func Test_repo_SaveXXX_with_brokenEncode(t *testing.T) {
	wantErr := errors.Newf("broken")

	rr := mockRepo(t, fields{path: t.TempDir()}, withOpenRoot, withBootstrap, withMockItems)
	oldEncode := encodeFn
	encodeFn = func(v any) ([]byte, error) {
		return nil, wantErr
	}
	t.Cleanup(func() {
		rr.Close()
		encodeFn = oldEncode
	})

	t.Run("CreateClient", func(t *testing.T) {
		t.Skip("we don't need to unmarshal osin data")
		err := rr.CreateClient(defaultClient)
		if !cmp.Equal(err, wantErr, EquateWeakErrors) {
			t.Errorf("CreateClient() error = %v, wantErr %v", err, wantErr)
		}
	})

	t.Run("SaveAuthorize", func(t *testing.T) {
		t.Skip("we don't need to unmarshal osin data")
		err := rr.SaveAuthorize(mockAuth("test", defaultClient))
		if !cmp.Equal(err, wantErr, EquateWeakErrors) {
			t.Errorf("SaveAuthorize() error = %v, wantErr %v", err, wantErr)
		}
	})

	t.Run("SaveAccess", func(t *testing.T) {
		t.Skip("we don't need to unmarshal osin data")
		err := rr.SaveAccess(mockAccess("test-access", defaultClient))
		if !cmp.Equal(err, wantErr, EquateWeakErrors) {
			t.Errorf("SaveAccess() error = %v, wantErr %v", err, wantErr)
		}
	})

	t.Run("SaveMetadata", func(t *testing.T) {
		t.Skip("we don't need to unmarshal osin data")
		err := rr.SaveMetadata("https://example.com/~jdoe", Metadata{Pw: []byte("asd"), PrivateKey: pkcs8Pk})
		if !cmp.Equal(err, wantErr, EquateWeakErrors) {
			t.Errorf("SaveMetadata() error = %v, wantErr %v", err, wantErr)
		}
	})

	t.Run("PasswordSet", func(t *testing.T) {
		err := rr.PasswordSet("https://example.com/~jdoe", []byte("dsa"))
		if !cmp.Equal(err, wantErr, EquateWeakErrors) {
			t.Errorf("PasswordSet() error = %v, wantErr %v", err, wantErr)
		}
	})

	t.Run("SaveKey", func(t *testing.T) {
		_, err := rr.SaveKey("https://example.com/~jdoe", pk)
		if !cmp.Equal(err, wantErr, EquateWeakErrors) {
			t.Errorf("SaveKey() error = %v, wantErr %v", err, wantErr)
		}
	})
}

func Test_repo_LoadXXX_with_brokenDecode(t *testing.T) {
	wantErr := errors.Newf("broken")

	rr := mockRepo(t, fields{path: t.TempDir()}, withOpenRoot, withBootstrap, withMockItems, withMetadataJDoe, withClient, withAuthorization, withAccess)
	oldDecode := decodeFn
	decodeFn = func(_ []byte, m any) error {
		return wantErr
	}
	t.Cleanup(func() {
		rr.Close()
		decodeFn = oldDecode
	})

	t.Run("GetClient", func(t *testing.T) {
		t.Skip("we don't need to unmarshal osin data")
		_, err := rr.GetClient("test-client")
		if !cmp.Equal(err, wantErr, EquateWeakErrors) {
			t.Errorf("GetClient() error = %v, wantErr %v", err, wantErr)
		}
	})

	t.Run("LoadAuthorize", func(t *testing.T) {
		t.Skip("we don't need to unmarshal osin data")
		_, err := rr.LoadAuthorize("test-code")
		if !cmp.Equal(err, wantErr, EquateWeakErrors) {
			t.Errorf("LoadAuthorize() error = %v, wantErr %v", err, wantErr)
		}
	})

	t.Run("LoadAccess", func(t *testing.T) {
		t.Skip("we don't need to unmarshal osin data")
		_, err := rr.LoadAccess("access-666")
		if !cmp.Equal(err, wantErr, EquateWeakErrors) {
			t.Errorf("LoadAccess() error = %v, wantErr %v", err, wantErr)
		}
	})

	t.Run("LoadRefresh", func(t *testing.T) {
		t.Skip("we don't need to unmarshal osin data")
		_, err := rr.LoadRefresh("refresh-666")
		if !cmp.Equal(err, wantErr, EquateWeakErrors) {
			t.Errorf("LoadRefresh() error = %v, wantErr %v", err, wantErr)
		}
	})

	t.Run("LoadMetadata", func(t *testing.T) {
		err := rr.LoadMetadata("https://example.com/~jdoe", Metadata{Pw: []byte("asd"), PrivateKey: pkcs8Pk})
		if !errors.Is(err, wantErr) {
			t.Errorf("LoadMetadata() error = %v, wantErr %v", err, wantErr)
		}
	})

	t.Run("LoadKey", func(t *testing.T) {
		_, err := rr.LoadKey("https://example.com/~jdoe")
		if !errors.Is(err, wantErr) {
			t.Errorf("LoadKey() error = %v, wantErr %v", err, wantErr)
		}
	})

	t.Run("PasswordCheck", func(t *testing.T) {
		err := rr.PasswordCheck("https://example.com/~jdoe", []byte("asd"))
		if !errors.Is(err, wantErr) {
			t.Errorf("PasswordCheck() error = %v, wantErr %v", err, wantErr)
		}
	})
}
