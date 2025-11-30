package sqlite

import (
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/carlmjohnson/be"
	vocab "github.com/go-ap/activitypub"
	"github.com/go-ap/errors"
	"github.com/go-ap/filters"
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

		values = append(values, []byte(mock), it.GetLink())
		fields = append(fields, "raw", "iri")
		params = append(params, "?", "?")
		if table == "collections" {
			_ = vocab.OnCollectionIntf(it, func(col vocab.CollectionInterface) error {
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
			err:  errors.NotFoundf("not found"),
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

			ff, _ := filters.FromIRI(tt.arg)
			got, err := r.Load(tt.arg, ff...)
			checkErrorsEqual(t, tt.err, err)

			if !vocab.ItemsEqual(tt.want, got) {
				t.Errorf("%s", cmp.Diff(tt.want, got))
			}
		})
	}
}

func Test_repo_Save(t *testing.T) {
	tests := []struct {
		name    string
		root    vocab.Item
		arg     vocab.Item
		want    vocab.Item
		wantErr error
	}{
		{
			name:    "empty",
			root:    rootActor,
			wantErr: errors.Newf("Unable to save nil element"),
		},
		{
			name:    "save object",
			root:    rootActor,
			arg:     vocab.Object{ID: "https://example.com/1"},
			want:    vocab.Object{ID: "https://example.com/1"},
			wantErr: nil,
		},
		{
			name:    "save activity",
			root:    rootActor,
			arg:     vocab.Activity{ID: "https://example.com/activities/1", Type: vocab.LikeType, Actor: vocab.IRI("https://example.com")},
			want:    vocab.Activity{ID: "https://example.com/activities/1", Type: vocab.LikeType, Actor: vocab.IRI("https://example.com")},
			wantErr: nil,
		},
		{
			name:    "save another actor",
			root:    rootActor,
			arg:     vocab.Actor{ID: "https://example.com/actors/1", Type: vocab.GroupType},
			want:    vocab.Actor{ID: "https://example.com/actors/1", Type: vocab.GroupType},
			wantErr: nil,
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
			if !cmp.Equal(tt.wantErr, err, EquateWeakErrors) {
				t.Errorf("Save() error got %v expected %v", err, tt.wantErr)
			}

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

func Test_New(t *testing.T) {
	dir := os.TempDir()

	conf := Config{
		Path:  dir,
		LogFn: func(s string, p ...interface{}) { t.Logf(s, p...) },
		ErrFn: func(s string, p ...interface{}) { t.Errorf(s, p...) },
	}
	repo, _ := New(conf)
	if repo == nil {
		t.Errorf("Nil result from opening sqlite %s", conf.Path)
		return
	}
	if repo.conn != nil {
		t.Errorf("Non nil %T from New", repo.conn)
	}
	if repo.errFn == nil {
		t.Errorf("Nil error log function, expected %T[%p]", t.Errorf, t.Errorf)
	}
	if repo.logFn == nil {
		t.Errorf("Nil log function, expected %T[%p]", t.Logf, t.Logf)
	}
}

func withCreatePath(t *testing.T, r *repo) *repo {
	validPath, err := getFullPath(Config{Path: r.path})
	if err != nil && r.errFn != nil {
		r.errFn("Unable to build path from %s: %s", r.path, err)
	} else {
		r.path = validPath
	}
	return r
}

func Test_repo_Open(t *testing.T) {
	tests := []struct {
		name     string
		fields   fields
		setupFns []initFn
		wantErr  error
	}{
		{
			name:   "empty does not fail with sqlite",
			fields: fields{},
		},
		{
			name:     "with valid path",
			fields:   fields{path: t.TempDir()},
			setupFns: []initFn{withCreatePath},
		},
	}

	t.Run("Error on nil repo", func(t *testing.T) {
		var r *repo
		wantErr := errors.Newf("Unable to open uninitialized db")
		if err := r.Open(); !cmp.Equal(err, wantErr, EquateWeakErrors) {
			t.Errorf("Open() error = %s", cmp.Diff(wantErr, err, EquateWeakErrors))
		}
	})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// NOTE(marius): we don't use the mockRepo function here as we need to check failure cases
			r := &repo{
				path:  tt.fields.path,
				logFn: t.Logf,
				errFn: t.Errorf,
			}
			for _, fn := range tt.setupFns {
				_ = fn(t, r)
			}

			if err := r.Open(); !cmp.Equal(err, tt.wantErr, EquateWeakErrors) {
				t.Errorf("Open() error = %s", cmp.Diff(tt.wantErr, err, EquateWeakErrors))
			}
			if tt.wantErr != nil {
				return
			}
			if tt.fields.path != "" && r.conn == nil {
				t.Errorf("Open() conn is nil for path %s: %T: %v", tt.fields.path, r.conn, r.conn)
			}
		})
	}
}

func TestRepo_Close(t *testing.T) {
	conf := Config{Path: t.TempDir()}
	err := Bootstrap(conf)
	if err != nil {
		t.Errorf("Unable to bootstrap sqlite %s: %s", conf.Path, err)
	}
	defer os.Remove(conf.Path)

	repo, err := New(conf)
	if err != nil {
		t.Errorf("Error initializing db: %s", err)
	}
	err = repo.Open()
	if err != nil {
		t.Errorf("Unable to open sqlite %s: %s", conf.Path, err)
	}
	err = repo.close()
	if err != nil {
		t.Errorf("Unable to close sqlite %s: %s", conf.Path, err)
	}
	os.Remove(conf.Path)
}

func defaultCol(iri vocab.IRI) vocab.CollectionInterface {
	return &vocab.OrderedCollection{
		ID:        iri,
		Type:      vocab.OrderedCollectionType,
		CC:        vocab.ItemCollection{vocab.PublicNS},
		Published: time.Now().Round(time.Second).UTC(),
	}
}

func withOrderedCollection(iri vocab.IRI) initFn {
	return func(t *testing.T, r *repo) *repo {
		if _, err := r.Create(defaultCol(iri)); err != nil {
			r.errFn("unable to save collection %s: %s", iri, err.Error())
		}
		return r
	}
}

func withCollection(iri vocab.IRI) initFn {
	col := &vocab.Collection{
		ID:        iri,
		Type:      vocab.CollectionType,
		CC:        vocab.ItemCollection{vocab.PublicNS},
		Published: time.Now().Round(time.Second).UTC(),
	}
	return func(t *testing.T, r *repo) *repo {
		if _, err := r.Create(col); err != nil {
			r.errFn("unable to save collection %s: %s", iri, err)
		}
		return r
	}
}

func withOrderedCollectionHavingItems(t *testing.T, r *repo) *repo {
	colIRI := vocab.IRI("https://example.com/followers")
	col := vocab.OrderedCollection{
		ID:        colIRI,
		Type:      vocab.OrderedCollectionType,
		CC:        vocab.ItemCollection{vocab.PublicNS},
		Published: time.Now().UTC(),
	}
	if _, err := r.Create(&col); err != nil {
		r.errFn("unable to save collection %s: %s", colIRI, err)
	}
	obIRI := vocab.IRI("https://example.com")
	ob, err := save(r, vocab.Object{ID: obIRI})
	if err != nil {
		r.errFn("unable to save item %s: %s", obIRI, err)
	}
	if err := r.AddTo(col.ID, ob); err != nil {
		r.errFn("unable to add item to collection %s -> %s : %s", obIRI, colIRI, err)
	}
	return r
}

func withCollectionHavingItems(t *testing.T, r *repo) *repo {
	colIRI := vocab.IRI("https://example.com/followers")
	col := vocab.Collection{
		ID:        colIRI,
		Type:      vocab.CollectionType,
		CC:        vocab.ItemCollection{vocab.PublicNS},
		Published: time.Now().UTC(),
	}
	if _, err := r.Create(&col); err != nil {
		r.errFn("unable to save collection %s: %s", colIRI, err)
	}
	obIRI := vocab.IRI("https://example.com")
	ob, err := save(r, vocab.Object{ID: obIRI})
	if err != nil {
		r.errFn("unable to save item %s: %s", obIRI, err)
	}
	if err := r.AddTo(col.ID, ob); err != nil {
		r.errFn("unable to add item to collection %s -> %s : %s", obIRI, colIRI, err)
	}
	return r
}

func withItems(items ...vocab.Item) initFn {
	return func(t *testing.T, r *repo) *repo {
		for _, it := range items {
			if _, err := save(r, it); err != nil {
				r.errFn("unable to save item %s: %s", it.GetLink(), err)
			}
		}
		return r
	}
}

func Test_repo_RemoveFrom(t *testing.T) {
	type args struct {
		colIRI vocab.IRI
		it     vocab.Item
	}

	tests := []struct {
		name     string
		path     string
		setupFns []initFn
		args     args
		wantErr  error
	}{
		{
			name:    "not open",
			path:    t.TempDir(),
			args:    args{},
			wantErr: errNotOpen,
		},
		{
			name:     "empty",
			path:     t.TempDir(),
			setupFns: []initFn{withOpenRoot, withBootstrap},
			args:     args{},
			wantErr:  errors.NotFoundf("Unable to load root bucket"),
		},
		{
			name:     "collection doesn't exist",
			path:     t.TempDir(),
			setupFns: []initFn{withOpenRoot, withBootstrap},
			args: args{
				colIRI: "https://example.com/followers",
				it:     vocab.IRI("https://example.com"),
			},
			wantErr: errors.NotFoundf("Unable to load root bucket"),
		},
		{
			name:     "item doesn't exist in ordered collection",
			path:     t.TempDir(),
			setupFns: []initFn{withOpenRoot, withBootstrap, withOrderedCollection("https://example.com/followers")},
			args: args{
				colIRI: "https://example.com/followers",
				it:     vocab.IRI("https://example.com"),
			},
			wantErr: nil,
		},
		{
			name:     "item exists in ordered collection",
			path:     t.TempDir(),
			setupFns: []initFn{withOpenRoot, withBootstrap, withOrderedCollectionHavingItems},
			args: args{
				colIRI: "https://example.com/followers",
				it:     vocab.IRI("https://example.com"),
			},
			wantErr: nil,
		},
		{
			name:     "item doesn't exist in collection",
			path:     t.TempDir(),
			setupFns: []initFn{withOpenRoot, withBootstrap, withCollection("https://example.com/followers")},
			args: args{
				colIRI: "https://example.com/followers",
				it:     vocab.IRI("https://example.com"),
			},
			wantErr: nil, // if the item doesn't exist, we don't error out, unsure if that makes sense
		},
		{
			name:     "item exists in collection",
			path:     t.TempDir(),
			setupFns: []initFn{withOpenRoot, withBootstrap, withCollectionHavingItems},
			args: args{
				colIRI: "https://example.com/followers",
				it:     vocab.IRI("https://example.com"),
			},
			wantErr: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := mockRepo(t, fields{path: tt.path}, tt.setupFns...)
			t.Cleanup(r.Close)

			err := r.RemoveFrom(tt.args.colIRI, tt.args.it)
			if !cmp.Equal(tt.wantErr, err, EquateWeakErrors) {
				t.Errorf("RemoveFrom() error = %s", cmp.Diff(tt.wantErr, err, EquateWeakErrors))
				return
			}
			if tt.wantErr != nil {
				// NOTE(marius): if we expected an error we don't need to following tests
				return
			}

			it, err := r.Load(tt.args.colIRI)
			if err != nil {
				t.Errorf("Load() after RemoveFrom() error = %v", err)
				return
			}

			col, ok := it.(vocab.CollectionInterface)
			if !ok {
				t.Errorf("Load() after RemoveFrom(), didn't return a CollectionInterface type")
				return
			}

			if col.Contains(tt.args.it) {
				t.Errorf("Load() after RemoveFrom(), the item is still in collection %#v", col.Collection())
			}

			// NOTE(marius): this is a bit of a hackish way to skip testing of the object when we didn't
			// save it to the disk
			if vocab.IsObject(tt.args.it) {
				ob, err := r.Load(tt.args.it.GetLink())
				if err != nil {
					t.Errorf("Load() of the object after RemoveFrom() error = %v", err)
					return
				}
				if !vocab.ItemsEqual(ob, tt.args.it) {
					t.Errorf("Loaded item after RemoveFrom(), is not equal %#v with the one provided %#v", ob, tt.args.it)
				}
			}
		})
	}
}

func Test_repo_AddTo1(t *testing.T) {
	type args struct {
		colIRI vocab.IRI
		it     vocab.Item
	}

	tests := []struct {
		name     string
		path     string
		setupFns []initFn
		setup    func(*repo) error
		args     args
		wantErr  error
	}{
		{
			name:    "not open",
			path:    t.TempDir(),
			args:    args{},
			wantErr: errNotOpen,
		},
		{
			name:     "empty",
			path:     t.TempDir(),
			setupFns: []initFn{withOpenRoot, withBootstrap},
			args:     args{},
			wantErr:  errors.NotFoundf("not found"),
		},
		{
			name:     "collection doesn't exist",
			path:     t.TempDir(),
			setupFns: []initFn{withOpenRoot, withBootstrap},
			args: args{
				colIRI: "https://example.com/followers",
				it:     vocab.IRI("https://example.com"),
			},
			wantErr: errors.NotFoundf("not found"),
		},
		{
			name:     "item doesn't exist in collection",
			path:     t.TempDir(),
			setupFns: []initFn{withOpenRoot, withBootstrap, withCollection("https://example.com/followers")},
			args: args{
				colIRI: "https://example.com/followers",
				it:     vocab.IRI("https://example.com"),
			},
			wantErr: errors.NotFoundf("invalid item to add to collection"),
		},
		{
			name:     "item doesn't exist in ordered collection",
			path:     t.TempDir(),
			setupFns: []initFn{withOpenRoot, withBootstrap, withOrderedCollection("https://example.com/followers")},
			args: args{
				colIRI: "https://example.com/followers",
				it:     vocab.IRI("https://example.com"),
			},
			wantErr: errors.NotFoundf("invalid item to add to collection"),
		},
		{
			name:     "item exists in ordered collection",
			path:     t.TempDir(),
			setupFns: []initFn{withOpenRoot, withBootstrap, withOrderedCollectionHavingItems},
			args: args{
				colIRI: "https://example.com/followers",
				it:     vocab.IRI("https://example.com"),
			},
			wantErr: nil,
		},
		{
			name:     "item exists in collection",
			path:     t.TempDir(),
			setupFns: []initFn{withOpenRoot, withBootstrap, withCollectionHavingItems},
			args: args{
				colIRI: "https://example.com/followers",
				it:     vocab.IRI("https://example.com"),
			},
			wantErr: nil,
		},
		{
			name:     "item to non-existent hidden collection",
			path:     t.TempDir(),
			setupFns: []initFn{withOpenRoot, withBootstrap, withItems(&vocab.Object{ID: "https://example.com/example", Type: vocab.NoteType})},
			args: args{
				colIRI: "https://example.com/~jdoe/blocked",
				it:     vocab.IRI("https://example.com/example"),
			},
			wantErr: nil,
		},
		{
			name:     "item to hidden collection",
			path:     t.TempDir(),
			setupFns: []initFn{withOpenRoot, withBootstrap, withCollection("https://example.com/~jdoe/blocked"), withItems(&vocab.Object{ID: "https://example.com/example", Type: vocab.NoteType})},
			args: args{
				colIRI: "https://example.com/~jdoe/blocked",
				it:     vocab.IRI("https://example.com/example"),
			},
			wantErr: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := mockRepo(t, fields{path: tt.path}, tt.setupFns...)
			t.Cleanup(r.Close)

			err := r.AddTo(tt.args.colIRI, tt.args.it)
			if !cmp.Equal(err, tt.wantErr, EquateWeakErrors) {
				t.Errorf("AddTo() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr != nil {
				return
			}

			it, err := r.Load(tt.args.colIRI)
			if err != nil {
				t.Errorf("Load() after AddTo() error = %v", err)
				return
			}

			col, ok := it.(vocab.CollectionInterface)
			if !ok {
				t.Errorf("Load() after AddTo(), didn't return a CollectionInterface type")
				return
			}

			if !col.Contains(tt.args.it) {
				t.Errorf("Load() after AddTo(), the item is not in collection %#v", col.Collection())
			}

			ob, err := r.Load(tt.args.it.GetLink())
			if err != nil {
				t.Errorf("Load() of the object after AddTo() error = %v", err)
				return
			}
			if !vocab.ItemsEqual(ob, tt.args.it) {
				t.Errorf("Loaded item after AddTo(), is not equal %#v with the one provided %#v", ob, tt.args.it)
			}
		})
	}
}

func Test_repo_Load_UnhappyPath(t *testing.T) {
	type args struct {
		iri vocab.IRI
		fil filters.Checks
	}
	tests := []struct {
		name     string
		args     args
		setupFns []initFn
		want     vocab.Item
		wantErr  error
	}{
		{
			name:    "not opened",
			wantErr: errNotOpen,
		},
		{
			name:     "empty",
			setupFns: []initFn{withOpenRoot, withBootstrap},
			wantErr:  errors.NotFoundf("file not found"),
		},
		{
			name:     "not bootstrapped",
			args:     args{iri: "https://example.com"},
			setupFns: []initFn{withOpenRoot},
			wantErr:  errors.Annotatef(&Error{}, "unable to run select"),
		},
		{
			name:     "empty iri gives us not found",
			setupFns: []initFn{withOpenRoot, withBootstrap},
			wantErr:  errors.NotFoundf("file not found"),
		},
		{
			name:     "invalid iri gives 404",
			args:     args{iri: "https://example.com/dsad"},
			setupFns: []initFn{withOpenRoot, withBootstrap},
			wantErr:  errors.NotFoundf("example.com not found"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := mockRepo(t, fields{path: t.TempDir()}, tt.setupFns...)
			t.Cleanup(r.Close)

			got, err := r.Load(tt.args.iri, tt.args.fil...)
			if !cmp.Equal(err, tt.wantErr, EquateWeakErrors) {
				t.Errorf("Load() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !vocab.ItemsEqual(got, tt.want) {
				t.Errorf("Load() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_repo_Load1(t *testing.T) {
	// NOTE(marius): happy path tests for a fully mocked repo
	r := mockRepo(t, fields{path: t.TempDir()}, withOpenRoot, withBootstrap, withGeneratedMocks)
	t.Cleanup(r.Close)

	type args struct {
		iri vocab.IRI
		fil filters.Checks
	}
	tests := []struct {
		name    string
		args    args
		want    vocab.Item
		wantErr error
	}{
		{
			name:    "empty",
			args:    args{iri: ""},
			wantErr: errors.NotFoundf("file not found"),
		},
		{
			name:    "empty iri gives us not found",
			args:    args{iri: ""},
			want:    nil,
			wantErr: errors.NotFoundf("file not found"),
		},
		{
			name: "root iri gives us the root",
			args: args{iri: "https://example.com"},
			want: root,
		},
		{
			name:    "invalid iri gives 404",
			args:    args{iri: "https://example.com/dsad"},
			want:    nil,
			wantErr: errors.NotFoundf("dsad not found"),
		},
		{
			name: "first Person",
			args: args{iri: "https://example.com/person/1"},
			want: filter(*allActors.Load(), filters.HasType("Person")).First(),
		},
		{
			name: "first Follow",
			args: args{iri: "https://example.com/follow/1"},
			want: filter(*allActivities.Load(), filters.HasType("Follow")).First(),
		},
		{
			name: "first Image",
			args: args{iri: "https://example.com/image/1"},
			want: filter(*allObjects.Load(), filters.SameID("https://example.com/image/1")).First(),
		},
		{
			name: "full outbox",
			args: args{iri: rootOutboxIRI},
			want: wantsRootOutbox(),
		},
		{
			name: "limit to max 2 things",
			args: args{
				iri: rootOutboxIRI,
				fil: filters.Checks{filters.WithMaxCount(2)},
			},
			want: wantsRootOutboxPage(2, filters.WithMaxCount(2)),
		},
		{
			name: "inbox?type=Create",
			args: args{
				iri: rootOutboxIRI,
				fil: filters.Checks{
					filters.HasType(vocab.CreateType),
				},
			},
			want: wantsRootOutbox(filters.HasType(vocab.CreateType)),
		},
		{
			name: "inbox?type=Create&actor.name=Hank",
			args: args{
				iri: rootOutboxIRI,
				fil: filters.Checks{
					filters.HasType(vocab.CreateType),
					filters.Actor(filters.NameIs("Hank")),
				},
			},
			want: wantsRootOutbox(
				filters.HasType(vocab.CreateType),
				filters.Actor(filters.NameIs("Hank")),
			),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := r.Load(tt.args.iri, tt.args.fil...)
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("Load() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !cmp.Equal(tt.want, got, EquateItemCollections) {
				t.Errorf("Load() got = %s", cmp.Diff(tt.want, got, EquateItemCollections))
			}
		})
	}
}

func Test_repo_Save1(t *testing.T) {
	type test struct {
		name     string
		fields   fields
		setupFns []initFn
		it       vocab.Item
		want     vocab.Item
		wantErr  error
	}
	tests := []test{
		{
			name:    "empty",
			fields:  fields{},
			wantErr: errNotOpen,
		},
		{
			name:     "empty item can't be saved",
			fields:   fields{path: t.TempDir()},
			setupFns: []initFn{withOpenRoot, withBootstrap},
			wantErr:  errors.Newf("Unable to save nil element"),
		},
		{
			name:     "save item collection",
			setupFns: []initFn{withOpenRoot, withBootstrap},
			fields:   fields{path: t.TempDir()},
			it:       mockItems,
			want:     mockItems,
		},
	}
	for i, mockIt := range mockItems {
		tests = append(tests, test{
			name:     fmt.Sprintf("save %d %T to repo", i, mockIt),
			setupFns: []initFn{withOpenRoot, withBootstrap},
			fields:   fields{path: t.TempDir()},
			it:       mockIt,
			want:     mockIt,
		})
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := mockRepo(t, tt.fields, tt.setupFns...)
			t.Cleanup(r.Close)

			got, err := r.Save(tt.it)
			if !cmp.Equal(err, tt.wantErr, EquateWeakErrors) {
				t.Errorf("Save() error = %s", cmp.Diff(tt.wantErr, err))
				return
			}
			if !cmp.Equal(got, tt.want) {
				t.Errorf("Save() got = %s", cmp.Diff(tt.want, got))
			}
		})
	}
}

func Test_repo_Delete1(t *testing.T) {
	type test struct {
		name     string
		fields   fields
		setupFns []initFn
		it       vocab.Item
		wantErr  error
	}
	tests := []test{
		{
			name:    "empty",
			fields:  fields{},
			wantErr: errNotOpen,
		},
		{
			name:     "empty item won't return an error",
			setupFns: []initFn{withOpenRoot, withBootstrap},
			fields:   fields{path: t.TempDir()},
		},
		{
			name:     "delete item collection",
			fields:   fields{path: t.TempDir()},
			setupFns: []initFn{withOpenRoot, withBootstrap, withItems(mockItems)},
			it:       mockItems,
		},
	}
	for i, mockIt := range mockItems {
		tests = append(tests, test{
			name:     fmt.Sprintf("delete %d %T from repo", i, mockIt),
			fields:   fields{path: t.TempDir()},
			setupFns: []initFn{withOpenRoot, withBootstrap, withMockItems},
			it:       mockIt,
		})
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := mockRepo(t, tt.fields, tt.setupFns...)
			t.Cleanup(r.Close)

			if err := r.Delete(tt.it); !cmp.Equal(err, tt.wantErr, EquateWeakErrors) {
				t.Errorf("Delete() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
