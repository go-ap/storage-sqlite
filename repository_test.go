package sqlite

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/carlmjohnson/be"
	vocab "github.com/go-ap/activitypub"
	"github.com/go-ap/errors"
)

func bootstrap(t *testing.T, base string) {
	err := Bootstrap(Config{Path: base, LogFn: t.Logf, ErrFn: t.Errorf})
	be.NilErr(t, err)
}

func saveMocks(t *testing.T, base string, root vocab.Item, mocks ...string) {
	bootstrap(t, base)

	r := repo{path: base, logFn: defaultLogFn, errFn: defaultLogFn}
	err := r.Open()
	db := r.conn
	be.NilErr(t, err)

	rootVal, _ := vocab.MarshalJSON(root)
	mocks = append(mocks, string(rootVal))
	query := fmt.Sprintf(upsertQ, "objects", strings.Join([]string{"raw"}, ", "), strings.Join([]string{"?"}, ", "))
	for _, mock := range mocks {
		res, err := db.Exec(query, []byte(mock))
		be.NilErr(t, err)

		rows, err := res.RowsAffected()
		be.NilErr(t, err)
		be.Equal(t, 1, rows)
	}
}

func checkErrorsEqual(t *testing.T, wanted, got error) {
	t.Helper()
	if wanted == nil {
		be.NilErr(t, got)
	} else {
		if reflect.TypeOf(got) != reflect.TypeOf(wanted) {
			t.Fatalf("invalid error type received %T, expected %T", got, wanted)
		}
		if wanted.Error() != got.Error() {
			t.Fatalf("invalid error message received %v, expected %v", got, wanted)
		}
	}
}

var rootActor = vocab.Actor{ID: "https://example.com", Type: vocab.ActorType}

func Test_repo_Load(t *testing.T) {
	tests := []struct {
		name  string
		root  vocab.Item
		arg   vocab.IRI
		mocks []string
		want  vocab.Item
		err   error
	}{
		{
			name: "empty",
			root: rootActor,
			arg:  "",
			want: nil,
			err:  errors.NotFoundf("unable to find storage for path "),
		},
		{
			name: "load object with just an ID",
			root: vocab.Object{ID: "https://example.com"},
			arg:  "https://example.com",
			want: &vocab.Object{ID: "https://example.com"},
		},
		{
			name: "load actor",
			root: rootActor,
			arg:  "https://example.com",
			want: &rootActor,
			err:  nil,
		},
		{
			name: "load activity",
			root: rootActor,
			arg:  "https://example.com/123",
			mocks: []string{
				`{"id":"https://example.com/123", "type":"Follow"}`,
			},
			want: &vocab.Follow{ID: "https://example.com/123", Type: vocab.FollowType},
			err:  nil,
		},
		{
			name: "load note from deeper actor",
			root: vocab.Actor{ID: "https://example.com/actors/jdoe", Type: vocab.ActorType},
			mocks: []string{
				`{"id":"https://example.com/actors/jdoe/123", "type":"Note"}`,
				`{"id":"https://example.com/actors/jdoe/124", "type":"Article"}`,
			},
			arg:  "https://example.com/actors/jdoe/123",
			want: &vocab.Note{ID: "https://example.com/actors/jdoe/123", Type: vocab.NoteType},
			err:  nil,
		},
		{
			name: "load note from deeper actor",
			root: vocab.Actor{ID: "https://example.com/actors/jdoe", Type: vocab.ActorType},
			mocks: []string{
				`{"id":"https://example.com/actors/jdoe/123", "type":"Note"}`,
				`{"id":"https://example.com/actors/jdoe/124", "type":"Article"}`,
			},
			arg:  "https://example.com/actors/jdoe/124",
			want: &vocab.Note{ID: "https://example.com/actors/jdoe/124", Type: vocab.ArticleType},
			err:  nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := t.TempDir()
			saveMocks(t, path, tt.root, tt.mocks...)

			r := repo{
				path:  path,
				logFn: t.Logf,
				errFn: t.Errorf,
			}
			got, err := r.Load(tt.arg)
			checkErrorsEqual(t, tt.err, err)

			be.DeepEqual(t, tt.want, got)
		})
	}
}

func Test_repo_Save(t *testing.T) {
	tests := []struct {
		name string
		root vocab.Item
		arg  vocab.Item
		want vocab.Item
		err  error
	}{
		{
			name: "empty",
			root: rootActor,
			arg:  nil,
			want: nil,
			err:  nilItemErr,
		},
		{
			name: "save object",
			root: rootActor,
			arg:  vocab.Object{ID: "https://example.com/1"},
			want: vocab.Object{ID: "https://example.com/1"},
			err:  nil,
		},
		{
			name: "save activity",
			root: rootActor,
			arg:  vocab.Activity{ID: "https://example.com/1", Type: vocab.LikeType},
			want: vocab.Activity{ID: "https://example.com/1", Type: vocab.LikeType},
			err:  nil,
		},
		{
			name: "save item collection",
			root: rootActor,
			arg: vocab.ItemCollection{
				vocab.Activity{ID: "https://example.com/1", Type: vocab.LikeType},
				vocab.Activity{ID: "https://example.com/2", Type: vocab.FollowType},
			},
			want: vocab.ItemCollection{
				vocab.Activity{ID: "https://example.com/1", Type: vocab.LikeType},
				vocab.Activity{ID: "https://example.com/2", Type: vocab.FollowType},
			},
			err: nil,
		},
		{
			name: "save another actor",
			root: rootActor,
			arg:  vocab.Actor{ID: "https://example.com/1", Type: vocab.GroupType},
			want: vocab.Actor{ID: "https://example.com/1", Type: vocab.GroupType},
			err:  nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := t.TempDir()
			saveMocks(t, path, tt.root)

			r := repo{path: path, logFn: t.Logf, errFn: t.Errorf}

			got, err := r.Save(tt.arg)
			checkErrorsEqual(t, tt.err, err)

			be.DeepEqual[vocab.Item](t, got, tt.want)
		})
	}
}

func Test_repo_Create(t *testing.T) {
	tests := []struct {
		name string
		root vocab.Item
		arg  vocab.CollectionInterface
		want vocab.CollectionInterface
		err  error
	}{
		{
			name: "nil",
			root: rootActor,
			arg:  nil,
			want: nil,
			err:  errors.Newf("unable to create nil collection"),
		},
		{
			name: "empty",
			root: rootActor,
			arg:  &vocab.ItemCollection{},
			want: &vocab.ItemCollection{},
			err:  nil,
		},
		{
			name: "an item in an item collection",
			root: rootActor,
			arg:  &vocab.ItemCollection{vocab.Object{ID: "https://example.com/1", Type: vocab.NoteType}},
			want: &vocab.ItemCollection{vocab.Object{ID: "https://example.com/1", Type: vocab.NoteType}},
			err:  nil,
		},
		{
			name: "an ordered collection",
			root: rootActor,
			arg:  &vocab.OrderedCollection{ID: "https://example.com/1", Type: vocab.OrderedCollectionType},
			want: &vocab.OrderedCollection{ID: "https://example.com/1", Type: vocab.OrderedCollectionType},
			err:  nil,
		},
		{
			name: "an ordered collection page",
			root: rootActor,
			arg: &vocab.OrderedCollectionPage{
				ID:    "https://example.com/1",
				Type:  vocab.OrderedCollectionPageType,
				First: vocab.IRI("https://example.com/1?first"),
				Next:  vocab.IRI("https://example.com/1?next"),
			},
			want: &vocab.OrderedCollectionPage{
				ID:    "https://example.com/1",
				Type:  vocab.OrderedCollectionPageType,
				First: vocab.IRI("https://example.com/1?first"),
				Next:  vocab.IRI("https://example.com/1?next"),
			},
			err: nil,
		},
		{
			name: "a collection",
			root: rootActor,
			arg: &vocab.Collection{
				ID:    "https://example.com/1",
				Type:  vocab.CollectionType,
				First: vocab.IRI("https://example.com/1?first"),
				Items: vocab.ItemCollection{
					vocab.Object{ID: "https://example.com/1/1", Type: vocab.NoteType},
					vocab.Object{ID: "https://example.com/1/3", Type: vocab.ImageType},
				},
			},
			want: &vocab.Collection{
				ID:    "https://example.com/1",
				Type:  vocab.CollectionType,
				First: vocab.IRI("https://example.com/1?first"),
				Items: vocab.ItemCollection{
					vocab.Object{ID: "https://example.com/1/1", Type: vocab.NoteType},
					vocab.Object{ID: "https://example.com/1/3", Type: vocab.ImageType},
				},
			},
			err: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := t.TempDir()
			saveMocks(t, path, tt.root)

			r := repo{path: path, logFn: t.Logf, errFn: t.Errorf}
			got, err := r.Create(tt.arg)
			checkErrorsEqual(t, tt.err, err)
			be.DeepEqual(t, tt.want, got)
		})
	}
}

func Test_repo_AddTo(t *testing.T) {
	type args struct {
		col vocab.IRI
		it  vocab.Item
	}
	tests := []struct {
		name string
		root vocab.Item
		args args
		err  error
	}{
		{
			name: "empty collection iri",
			root: rootActor,
			args: args{
				col: "",
				it:  nil,
			},
			err: errors.Newf("invalid collection IRI"),
		},
		{
			name: "empty",
			root: rootActor,
			args: args{
				col: "https://example.com/1",
				it:  nil,
			},
			err: nilItemErr,
		},
		{
			name: "test",
			root: rootActor,
			args: args{
				col: "https://example.com/inbox",
				it:  &vocab.Object{ID: "https://example.com/1", Type: vocab.NoteType},
			},
			err: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := t.TempDir()
			saveMocks(t, path, tt.root)

			r := repo{path: path, logFn: t.Logf, errFn: t.Errorf}

			err := r.AddTo(tt.args.col, tt.args.it)
			checkErrorsEqual(t, tt.err, err)
			if tt.err != nil {
				return
			}

			err = r.Open()
			checkErrorsEqual(t, tt.err, err)
			conn := r.conn
			defer conn.Close()

			sel := "SELECT * from collections where id=?;"
			res, err := conn.Query(sel, tt.args.col)
			be.NilErr(t, err)

			for res.Next() {
				var pub string
				var iri string
				var ob []byte

				err := res.Scan(&pub, &iri, &ob)
				be.NilErr(t, err)

				p, err := time.Parse("2006-01-02 15:04:05", pub)
				be.NilErr(t, err)
				be.True(t, time.Now().Sub(p) > time.Millisecond)

				be.Equal(t, tt.args.col, vocab.IRI(iri))

				it, err := vocab.UnmarshalJSON(ob)
				be.NilErr(t, err)
				be.DeepEqual(t, tt.args.it, it)
			}
		})
	}
}

func Test_repo_Delete(t *testing.T) {
	tests := []struct {
		name  string
		root  vocab.Item
		mocks []string
		arg   vocab.Item
		err   error
	}{
		{
			name: "empty",
			root: rootActor,
			arg:  nil,
			err:  nilItemErr,
		},
		{
			name: "delete invalid item - this should probably 404",
			root: rootActor,
			arg:  vocab.IRI("https://example.com/1"),
			err:  nil,
		},
		{
			name: "delete root - maybe we should remove the db also",
			root: rootActor,
			arg:  vocab.IRI("https://example.com"),
			err:  nil,
		},
		{
			name: "delete item",
			root: rootActor,
			mocks: []string{
				`{"id":"https://example.com/1","type":"Accept"}`,
				`{"id":"https://example.com/2","type":"Reject"}`,
			},
			arg: vocab.IRI("https://example.com/1"),
			err: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := t.TempDir()
			saveMocks(t, path, tt.root)

			r := repo{path: path, logFn: t.Logf, errFn: t.Errorf}
			err := r.Delete(tt.arg)
			checkErrorsEqual(t, tt.err, err)
			if tt.arg == nil {
				return
			}

			err = r.Open()
			checkErrorsEqual(t, tt.err, err)
			conn := r.conn
			defer conn.Close()

			{
				sel := "SELECT * from objects where id=?;"
				res, err := conn.Query(sel, tt.arg.GetLink())
				be.NilErr(t, err)
				be.False(t, res.Next())
			}

			if len(tt.mocks) == 0 {
				return
			}
			mocks := make(vocab.ItemCollection, 0)
			for _, mock := range tt.mocks {
				it, err := vocab.UnmarshalJSON([]byte(mock))
				be.NilErr(t, err)
				be.Nonzero(t, it)
				if it.GetLink().Equals(tt.arg.GetLink(), true) {
					continue
				}
				mocks.Append(it)
			}

			sel := "SELECT id, raw from objects where id != ?;"
			res, err := conn.Query(sel, tt.root.GetLink())
			be.NilErr(t, err)
			for res.Next() {
				var iri string
				var ob []byte

				err := res.Scan(&iri, &ob)
				be.NilErr(t, err)

				it, _ := vocab.UnmarshalJSON(ob)
				be.False(t, it.GetLink().Equals(tt.arg.GetLink(), true))
				be.True(t, mocks.Contains(it))
			}
		})
	}
}