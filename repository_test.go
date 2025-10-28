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
	"github.com/google/go-cmp/cmp"
)

func bootstrap(t *testing.T, base string) {
	t.Helper()
	err := Bootstrap(Config{Path: base, LogFn: t.Logf, ErrFn: t.Errorf})
	be.NilErr(t, err)
}

func saveMocks(t *testing.T, base string, root vocab.Item, mocks ...string) string {
	t.Helper()
	bootstrap(t, base)

	p, _ := getFullPath(Config{Path: base})
	r := repo{path: p, logFn: defaultLogFn, errFn: defaultLogFn}
	err := r.Open()
	be.NilErr(t, err)

	db := r.conn

	rootVal, _ := vocab.MarshalJSON(root)
	mocks = append(mocks, string(rootVal))
	for _, mock := range mocks {
		it, _ := vocab.UnmarshalJSON([]byte(mock))
		values := make([]any, 0)
		fields := make([]string, 0)
		params := make([]string, 0)

		table := getCollectionTypeFromItem(it)

		values = append(values, []byte(mock))
		fields = append(fields, "raw")
		params = append(params, "?")
		if table == "collections" {
			vocab.OnCollectionIntf(it, func(col vocab.CollectionInterface) error {
				rawItems, err := vocab.MarshalJSON(col.Collection())
				if err != nil {
					rawItems = emptyCol
				}
				fields = append(fields, "items")
				values = append(values, rawItems)
				params = append(params, "?")
				return nil
			})
		}
		query := fmt.Sprintf(upsertQ, table, strings.Join(fields, ", "), strings.Join(params, ", "))
		res, err := db.Exec(query, values...)
		be.NilErr(t, err)

		rows, err := res.RowsAffected()
		be.NilErr(t, err)
		be.Equal(t, 1, rows)
	}
	return p
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
var jDoeActor = vocab.Actor{ID: "https://example.com/actors/jdoe", Type: vocab.PersonType}

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
			err:  errors.NotFoundf("Not found"),
		},
		{
			name: "load object with just an ID",
			root: vocab.Object{ID: "https://example.com/objects/1"},
			arg:  "https://example.com/objects/1",
			want: &vocab.Object{ID: "https://example.com/objects/1"},
		},
		{
			name: "load actor",
			root: rootActor,
			arg:  "https://example.com",
			want: &rootActor,
		},
		{
			name: "load activity",
			root: rootActor,
			mocks: []string{
				`{"id":"https://example.com/activities/123", "type":"Follow", "actor": "https://example.com"}`,
			},
			arg:  "https://example.com/activities/123",
			want: &vocab.Follow{ID: "https://example.com/activities/123", Type: vocab.FollowType, Actor: &rootActor},
		},
		{
			name: "load activities",
			root: rootActor,
			mocks: []string{
				`{"id":"https://example.com/activities/122", "type":"Like", "actor": "https://example.com"}`,
				`{"id":"https://example.com/activities/123", "type":"Follow", "actor": "https://example.com"}`,
				`{"id":"https://example.com/activities/124", "type":"Create", "actor": "https://example.com"}`,
				`{"id":"https://example.com/activities", "type":"OrderedCollection", "totalItems":3, "orderedItems":["https://example.com/activities/122", "https://example.com/activities/123", "https://example.com/activities/124"]}`,
			},
			arg: "https://example.com/activities",
			want: &vocab.OrderedCollection{
				ID:         "https://example.com/activities",
				Type:       vocab.OrderedCollectionType,
				TotalItems: 3,
				First:      vocab.IRI("https://example.com/activities?maxItems=100"),
				OrderedItems: vocab.ItemCollection{
					&vocab.Like{ID: "https://example.com/activities/122", Type: vocab.LikeType, Actor: &rootActor},
					&vocab.Follow{ID: "https://example.com/activities/123", Type: vocab.FollowType, Actor: &rootActor},
					&vocab.Create{ID: "https://example.com/activities/124", Type: vocab.CreateType, Actor: &rootActor},
				},
			},
		},
		{
			name: "load activities with filter",
			root: rootActor,
			mocks: []string{
				`{"id":"https://example.com/activities", "type":"OrderedCollection", "totalItems":3}`,
				`{"id":"https://example.com/activities/122", "type":"Like", "actor": "https://example.com"}`,
				`{"id":"https://example.com/activities/123", "type":"Follow", "actor": "https://example.com"}`,
				`{"id":"https://example.com/activities/124", "type":"Create", "actor": "https://example.com"}`,
			},
			arg: "https://example.com/activities?type=Follow",
			want: &vocab.OrderedCollection{
				ID:         "https://example.com/activities?type=Follow",
				Type:       vocab.OrderedCollectionType,
				TotalItems: 3,
				First:      vocab.IRI("https://example.com/activities?maxItems=100&type=Follow"),
				OrderedItems: vocab.ItemCollection{
					&vocab.Follow{ID: "https://example.com/activities/123", Type: vocab.FollowType, Actor: &rootActor},
				},
			},
		},
		{
			name: "load note from deeper actor",
			root: jDoeActor,
			mocks: []string{
				`{"id":"https://example.com/objects/123", "type":"Note"}`,
				`{"id":"https://example.com/objects/124", "type":"Article"}`,
			},
			arg:  "https://example.com/objects/123",
			want: &vocab.Note{ID: "https://example.com/objects/123", Type: vocab.NoteType},
		},
		{
			name: "load article from deeper actor",
			root: jDoeActor,
			mocks: []string{
				`{"id":"https://example.com/objects/123", "type":"Note"}`,
				`{"id":"https://example.com/objects/124", "type":"Article"}`,
			},
			arg:  "https://example.com/objects/124",
			want: &vocab.Note{ID: "https://example.com/objects/124", Type: vocab.ArticleType},
		},
		{
			name: "load outbox of deeper actor",
			root: jDoeActor,
			mocks: []string{
				`{"id":"https://example.com/activities/1", "type":"Like", "actor": "https://example.com/actors/jdoe"}`,
				`{"id":"https://example.com/activities/2", "type":"Create", "actor": "https://example.com/actors/jdoe"}`,
				`{"id":"https://example.com/actors/jdoe/outbox", "type":"OrderedCollection", "totalItems":2, "orderedItems":["https://example.com/activities/1","https://example.com/activities/2"]}`,
			},
			arg: "https://example.com/actors/jdoe/outbox",
			want: &vocab.OrderedCollection{
				ID:         "https://example.com/actors/jdoe/outbox",
				Type:       vocab.OrderedCollectionType,
				First:      vocab.IRI("https://example.com/actors/jdoe/outbox?maxItems=100"),
				TotalItems: 2,
				OrderedItems: vocab.ItemCollection{
					&vocab.Like{ID: "https://example.com/activities/1", Type: vocab.LikeType, Actor: &jDoeActor},
					&vocab.Create{ID: "https://example.com/activities/2", Type: vocab.CreateType, Actor: &jDoeActor},
				},
			},
		},
		{
			name: "load filtered outbox of deeper actor",
			root: jDoeActor,
			mocks: []string{
				`{"id":"https://example.com/activities/1", "type":"Like", "actor": "https://example.com/actors/jdoe"}`,
				`{"id":"https://example.com/activities/2", "type":"Create", "actor": "https://example.com/actors/jdoe"}`,
				`{"id":"https://example.com/actors/jdoe/outbox", "type":"OrderedCollection", "totalItems":2, "orderedItems":["https://example.com/activities/1","https://example.com/activities/2"]}`,
			},
			arg: "https://example.com/actors/jdoe/outbox?type=Create",
			want: &vocab.OrderedCollection{
				ID:         "https://example.com/actors/jdoe/outbox?type=Create",
				Type:       vocab.OrderedCollectionType,
				TotalItems: 2,
				First:      vocab.IRI("https://example.com/actors/jdoe/outbox?type=Create&maxItems=100"),
				OrderedItems: vocab.ItemCollection{
					&vocab.Create{ID: "https://example.com/activities/2", Type: vocab.CreateType, Actor: &jDoeActor},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base := t.TempDir()
			path := saveMocks(t, base, tt.root, tt.mocks...)

			r := repo{
				path:  path,
				logFn: t.Logf,
				errFn: t.Errorf,
			}
			_ = r.Open()
			defer r.Close()

			got, err := r.Load(tt.arg)
			checkErrorsEqual(t, tt.err, err)

			if !vocab.ItemsEqual(tt.want, got) {
				t.Errorf("%s", cmp.Diff(tt.want, got))
			}
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
			err:  nil,
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
			arg:  vocab.Activity{ID: "https://example.com/activities/1", Type: vocab.LikeType, Actor: vocab.IRI("https://example.com")},
			want: vocab.Activity{ID: "https://example.com/activities/1", Type: vocab.LikeType, Actor: vocab.IRI("https://example.com")},
			err:  nil,
		},
		{
			name: "save another actor",
			root: rootActor,
			arg:  vocab.Actor{ID: "https://example.com/actors/1", Type: vocab.GroupType},
			want: vocab.Actor{ID: "https://example.com/actors/1", Type: vocab.GroupType},
			err:  nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base := t.TempDir()
			path := saveMocks(t, base, tt.root)

			r := repo{path: path, logFn: t.Logf, errFn: t.Errorf}
			_ = r.Open()
			defer r.Close()

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
			err:  nilItemErr,
		},
		{
			name: "empty",
			root: rootActor,
			arg:  &vocab.ItemCollection{},
			want: &vocab.ItemCollection{},
			err:  nilItemIRIErr,
		},
		{
			name: "an item in an item collection",
			root: rootActor,
			arg:  &vocab.ItemCollection{vocab.Object{ID: "https://example.com/1", Type: vocab.NoteType}},
			want: &vocab.ItemCollection{vocab.Object{ID: "https://example.com/1", Type: vocab.NoteType}},
			err:  nilItemIRIErr,
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
			base := t.TempDir()
			path := saveMocks(t, base, tt.root)

			r := repo{path: path, logFn: t.Logf, errFn: t.Errorf}
			_ = r.Open()
			defer r.Close()

			got, err := r.Create(tt.arg)
			checkErrorsEqual(t, tt.err, err)
			be.True(t, vocab.ItemsEqual(tt.want, got))
		})
	}
}

func orderedCollection(iri vocab.IRI) *vocab.OrderedCollection {
	col := vocab.OrderedCollectionNew(iri)
	col.Published = time.Now().UTC().Truncate(time.Second)
	return col
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
			name: "empty",
			root: rootActor,
			args: args{
				col: "https://example.com/1",
				it:  nil,
			},
		},
		{
			name: "test",
			root: rootActor,
			args: args{
				col: "https://example.com/inbox",
				it:  &vocab.Object{ID: "https://example.com/1", Type: vocab.NoteType},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base := t.TempDir()
			path := saveMocks(t, base, tt.root)

			mockCol := orderedCollection(tt.args.col)

			r := repo{path: path, logFn: t.Logf, errFn: t.Errorf}
			_ = r.Open()
			defer r.Close()

			col, err := r.Create(mockCol)
			be.NilErr(t, err)
			be.Equal(t, tt.args.col, col.GetLink())

			err = r.AddTo(tt.args.col, tt.args.it)
			checkErrorsEqual(t, tt.err, err)
			if tt.err != nil {
				return
			}

			err = r.Open()
			checkErrorsEqual(t, tt.err, err)
			conn := r.conn
			defer conn.Close()

			sel := "SELECT published, iri, raw, items from collections where iri=?;"
			res, err := conn.Query(sel, tt.args.col)
			be.NilErr(t, err)

			for res.Next() {
				var pub string
				var iri string
				var raw []byte
				var itemsRaw []byte

				err := res.Scan(&pub, &iri, &raw, &itemsRaw)
				be.NilErr(t, err)

				be.Equal(t, tt.args.col, vocab.IRI(iri))

				it, err := vocab.UnmarshalJSON(raw)
				be.NilErr(t, err)

				_ = vocab.OnOrderedCollection(it, func(col *vocab.OrderedCollection) error {
					be.True(t, mockCol.ID == col.ID)
					be.True(t, mockCol.Type == col.Type)
					return nil
				})

				it, err = vocab.UnmarshalJSON(itemsRaw)
				be.NilErr(t, err)
				items, err := vocab.ToItemCollection(it)
				be.NilErr(t, err)

				expectedCount := 0
				if tt.args.it != nil {
					expectedCount = 1
				}
				be.Equal(t, expectedCount, len(*items))
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
			err:  nil,
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
			base := t.TempDir()
			path := saveMocks(t, base, tt.root)

			r := repo{path: path, logFn: t.Logf, errFn: t.Errorf}
			_ = r.Open()
			defer r.Close()

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
				sel := "SELECT * from objects where iri=?;"
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
				if it.GetLink().Equal(tt.arg.GetLink()) {
					continue
				}
				_ = mocks.Append(it)
			}

			sel := "SELECT iri, raw from objects where iri != ?;"
			res, err := conn.Query(sel, tt.root.GetLink())
			be.NilErr(t, err)
			for res.Next() {
				var iri string
				var ob []byte

				err := res.Scan(&iri, &ob)
				be.NilErr(t, err)

				it, _ := vocab.UnmarshalJSON(ob)
				be.False(t, it.GetLink().Equal(tt.arg.GetLink()))
				be.True(t, mocks.Contains(it))
			}
		})
	}
}
