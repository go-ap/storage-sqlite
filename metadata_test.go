package sqlite

import (
	"crypto"
	"fmt"
	"reflect"
	"testing"

	vocab "github.com/go-ap/activitypub"
	"github.com/go-ap/errors"
	"github.com/google/go-cmp/cmp"
	"golang.org/x/crypto/bcrypt"
)

func Test_repo_LoadKey(t *testing.T) {
	tests := []struct {
		name     string
		fields   fields
		setupFns []initFn
		iri      vocab.IRI
		want     crypto.PrivateKey
		wantErr  error
	}{
		{
			name:    "empty",
			fields:  fields{},
			wantErr: errNotOpen,
		},
		{
			name: "empty IRI is not found",
			fields: fields{
				path: t.TempDir(),
			},
			setupFns: []initFn{withOpenRoot, withBootstrap, withMockItems},
			wantErr:  errors.NotFoundf("not found"),
		},
		{
			name: "~jdoe without metadata",
			fields: fields{
				path: t.TempDir(),
			},
			setupFns: []initFn{withOpenRoot, withBootstrap, withMockItems},
			iri:      "https://example.com/~jdoe",
			wantErr:  errors.NotFoundf("not found"),
		},
		{
			name: "~jdoe with metadata",
			fields: fields{
				path: t.TempDir(),
			},
			setupFns: []initFn{withOpenRoot, withBootstrap, withMockItems, withMetadataJDoe},
			iri:      "https://example.com/~jdoe",
			want:     pk,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := mockRepo(t, tt.fields, tt.setupFns...)
			t.Cleanup(r.Close)

			got, err := r.LoadKey(tt.iri)
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("LoadKey() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !cmp.Equal(got, tt.want) {
				t.Errorf("LoadKey() diff = %s", cmp.Diff(got, tt.want))
			}
		})
	}
}

func Test_repo_LoadMetadata(t *testing.T) {
	type args struct {
		iri vocab.IRI
		m   any
	}
	tests := []struct {
		name     string
		fields   fields
		setupFns []initFn
		args     args
		want     any
		wantErr  error
	}{
		{
			name:    "empty",
			fields:  fields{},
			args:    args{},
			wantErr: errNotOpen,
		},
		{
			name: "empty args is not found",
			fields: fields{
				path: t.TempDir(),
			},
			setupFns: []initFn{withOpenRoot, withBootstrap, withMockItems},
			wantErr:  errors.NotFoundf("not found"),
		},
		{
			name: "~jdoe without metadata",
			fields: fields{
				path: t.TempDir(),
			},
			setupFns: []initFn{withOpenRoot, withBootstrap, withMockItems},
			args: args{
				iri: "https://example.com/~jdoe",
				m:   Metadata{},
			},
			wantErr: errors.NotFoundf("not found"),
		},
		{
			name: "~jdoe with metadata",
			fields: fields{
				path: t.TempDir(),
			},
			setupFns: []initFn{withOpenRoot, withBootstrap, withMockItems, withMetadataJDoe},
			args: args{
				iri: "https://example.com/~jdoe",
				m:   &Metadata{},
			},
			want: &Metadata{
				Pw:         encPw,
				PrivateKey: key,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := mockRepo(t, tt.fields, tt.setupFns...)
			t.Cleanup(r.Close)

			if err := r.LoadMetadata(tt.args.iri, tt.args.m); !errors.Is(err, tt.wantErr) {
				t.Errorf("LoadMetadata() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr != nil {
				return
			}

			if !cmp.Equal(tt.want, tt.args.m) {
				t.Errorf("LoadMetadata() diff = %s", cmp.Diff(tt.want, tt.args.m))
			}
		})
	}
}

func Test_repo_PasswordCheck(t *testing.T) {
	type args struct {
		iri vocab.IRI
		pw  []byte
	}
	tests := []struct {
		name     string
		fields   fields
		setupFns []initFn
		args     args
		wantErr  error
	}{
		{
			name:    "empty",
			fields:  fields{},
			args:    args{},
			wantErr: errNotOpen,
		},
		{
			name: "empty args is not found",
			fields: fields{
				path: t.TempDir(),
			},
			setupFns: []initFn{withOpenRoot, withBootstrap, withMockItems},
			wantErr:  errors.NotFoundf("not found"),
		},
		{
			name: "~jdoe without metadata",
			fields: fields{
				path: t.TempDir(),
			},
			setupFns: []initFn{withOpenRoot, withBootstrap, withMockItems},
			args: args{
				iri: "https://example.com/~jdoe",
			},
			wantErr: errors.NotFoundf("not found"),
		},
		{
			name: "~jdoe with correct pw",
			fields: fields{
				path: t.TempDir(),
			},
			setupFns: []initFn{withOpenRoot, withBootstrap, withMockItems, withMetadataJDoe},
			args: args{
				iri: "https://example.com/~jdoe",
				pw:  defaultPw,
			},
		},
		{
			name: "~jdoe with incorrect pw",
			fields: fields{
				path: t.TempDir(),
			},
			setupFns: []initFn{withOpenRoot, withBootstrap, withMockItems, withMetadataJDoe},
			args: args{
				iri: "https://example.com/~jdoe",
				pw:  []byte("asd"),
			},
			wantErr: errors.Unauthorizedf("Invalid pw"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := mockRepo(t, tt.fields, tt.setupFns...)
			t.Cleanup(r.Close)

			if err := r.PasswordCheck(tt.args.iri, tt.args.pw); !errors.Is(err, tt.wantErr) {
				t.Errorf("PasswordCheck() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_repo_PasswordSet(t *testing.T) {
	type args struct {
		iri vocab.IRI
		pw  []byte
	}
	tests := []struct {
		name     string
		fields   fields
		setupFns []initFn
		args     args
		wantErr  error
	}{
		{
			name:    "empty",
			fields:  fields{},
			args:    args{},
			wantErr: errNotOpen,
		},
		{
			name:     "empty args",
			fields:   fields{path: t.TempDir()},
			setupFns: []initFn{withOpenRoot, withBootstrap, withMockItems},
			wantErr:  errors.Newf("could not generate hash for nil pw"),
		},
		{
			name: "~jdoe without metadata",
			fields: fields{
				path: t.TempDir(),
			},
			setupFns: []initFn{withOpenRoot, withBootstrap, withMockItems},
			args: args{
				iri: "https://example.com/~jdoe",
			},
			wantErr: errors.Newf("could not generate hash for nil pw"),
		},
		{
			name: "empty iri",
			fields: fields{
				path: t.TempDir(),
			},
			setupFns: []initFn{withOpenRoot, withBootstrap, withMockItems, withMetadataJDoe},
			args: args{
				iri: "",
				pw:  []byte("asd"),
			},
			wantErr: errors.NotFoundf("not found"),
		},
		{
			name: "~jdoe with pw",
			fields: fields{
				path: t.TempDir(),
			},
			setupFns: []initFn{withOpenRoot, withBootstrap, withMockItems, withMetadataJDoe},
			args: args{
				iri: "https://example.com/~jdoe",
				pw:  []byte("asd"),
			},
		},
		{
			name: "password too long",
			fields: fields{
				path: t.TempDir(),
			},
			setupFns: []initFn{withOpenRoot, withBootstrap, withMockItems, withMetadataJDoe},
			args: args{
				iri: "https://example.com/~jdoe",
				pw:  []byte("################################################################################"), // 80 chars
			},
			wantErr: bcrypt.ErrPasswordTooLong,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := mockRepo(t, tt.fields, tt.setupFns...)
			t.Cleanup(r.Close)

			if err := r.PasswordSet(tt.args.iri, tt.args.pw); !cmp.Equal(err, tt.wantErr, EquateWeakErrors) {
				t.Errorf("PasswordSet() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_repo_SaveKey(t *testing.T) {
	type args struct {
		iri vocab.IRI
		key crypto.PrivateKey
	}
	tests := []struct {
		name     string
		fields   fields
		setupFns []initFn
		args     args
		want     *vocab.PublicKey
		wantErr  error
	}{
		{
			name:    "empty",
			fields:  fields{},
			args:    args{},
			want:    nil,
			wantErr: errNotOpen,
		},
		{
			name:     "empty args",
			fields:   fields{path: t.TempDir()},
			setupFns: []initFn{withOpenRoot, withBootstrap, withMockItems},
			wantErr:  fmt.Errorf("x509: unknown key type while marshaling PKCS#8: %T", nil),
		},
		{
			name: "~jdoe with invalid key",
			fields: fields{
				path: t.TempDir(),
			},
			setupFns: []initFn{withOpenRoot, withBootstrap, withMockItems, withMetadataJDoe},
			args: args{
				iri: "https://example.com/~jdoe",
				key: []byte{0x1, 0x2, 0x3},
			},
			wantErr: fmt.Errorf("x509: unknown key type while marshaling PKCS#8: %T", []byte{}),
		},
		{
			name: "~jdoe with private key",
			fields: fields{
				path: t.TempDir(),
			},
			setupFns: []initFn{withOpenRoot, withBootstrap, withMockItems, withMetadataJDoe},
			args: args{
				iri: "https://example.com/~jdoe",
				key: pk,
			},
			want: apPublic,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := mockRepo(t, tt.fields, tt.setupFns...)
			t.Cleanup(r.Close)

			got, err := r.SaveKey(tt.args.iri, tt.args.key)
			if !cmp.Equal(err, tt.wantErr, EquateWeakErrors) {
				t.Errorf("SaveKey() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("SaveKey() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_repo_SaveMetadata(t *testing.T) {
	type args struct {
		iri vocab.IRI
		m   any
	}
	tests := []struct {
		name     string
		fields   fields
		setupFns []initFn
		args     args
		wantErr  error
	}{
		{
			name:    "empty",
			fields:  fields{},
			args:    args{},
			wantErr: errNotOpen,
		},
		{
			name:     "empty args",
			fields:   fields{path: t.TempDir()},
			setupFns: []initFn{withOpenRoot, withBootstrap, withMockItems},
			wantErr:  errors.Newf("Could not save nil metadata"),
		},
		{
			name: "~jdoe with simple pw",
			fields: fields{
				path: t.TempDir(),
			},
			setupFns: []initFn{withOpenRoot, withBootstrap, withMockItems},
			args: args{
				iri: "https://example.com/~jdoe",
				m:   []byte("asd"),
			},
		},
		{
			name: "~jdoe with key/pw metadata",
			fields: fields{
				path: t.TempDir(),
			},
			setupFns: []initFn{withOpenRoot, withBootstrap, withMockItems},
			args: args{
				iri: "https://example.com/~jdoe",
				m: Metadata{
					Pw:         []byte("asd"),
					PrivateKey: pkcs8Pk,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := mockRepo(t, tt.fields, tt.setupFns...)
			t.Cleanup(r.Close)

			if err := r.SaveMetadata(tt.args.iri, tt.args.m); !cmp.Equal(err, tt.wantErr, EquateWeakErrors) {
				t.Errorf("SaveMetadata() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
