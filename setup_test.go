package sqlite

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"database/sql"
	"encoding/pem"
	mrand "math/rand"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	vocab "github.com/go-ap/activitypub"
	"github.com/go-ap/cache"
	"github.com/go-ap/errors"
	"github.com/go-ap/filters"
	conformance "github.com/go-ap/storage-conformance-suite"
	"github.com/google/go-cmp/cmp"
	"github.com/openshift/osin"
	"golang.org/x/crypto/bcrypt"
)

func areErrors(a, b any) bool {
	_, ok1 := a.(error)
	_, ok2 := b.(error)
	return ok1 && ok2
}

func compareErrors(x, y interface{}) bool {
	xe := x.(error)
	ye := y.(error)
	if errors.Is(xe, ye) || errors.Is(ye, xe) {
		return true
	}
	return xe.Error() == ye.Error()
}

var EquateWeakErrors = cmp.FilterValues(areErrors, cmp.Comparer(compareErrors))

func areItemCollections(a, b any) bool {
	_, ok1 := a.(vocab.ItemCollection)
	_, ok3 := a.(*vocab.ItemCollection)
	_, ok2 := b.(vocab.ItemCollection)
	_, ok4 := b.(*vocab.ItemCollection)
	return (ok1 || ok3) && (ok2 || ok4)
}

func compareItemCollections(x, y interface{}) bool {
	var i1 vocab.Item
	var i2 vocab.Item
	if ic1, ok := x.(vocab.ItemCollection); ok {
		i1 = ic1
	}
	if ic1, ok := x.(*vocab.ItemCollection); ok {
		i1 = *ic1
	}
	if ic2, ok := y.(vocab.ItemCollection); ok {
		i2 = ic2
	}
	if ic2, ok := y.(*vocab.ItemCollection); ok {
		i2 = *ic2
	}
	return vocab.ItemsEqual(i1, i2)
}

var EquateItemCollections = cmp.FilterValues(areItemCollections, cmp.Comparer(compareItemCollections))

type fields struct {
	path  string
	cache cache.CanStore
	d     *sql.DB
}

func mockRepo(t *testing.T, f fields, initFns ...initFn) *repo {
	var err error
	f.path, err = getFullPath(Config{Path: f.path})
	if err != nil {
		t.Errorf("unable to get full path for db: %s", err)
		return nil
	}

	r := &repo{
		path:  f.path,
		cache: f.cache,
		logFn: t.Logf,
		errFn: t.Errorf,
	}

	for _, fn := range initFns {
		_ = fn(t, r)
	}
	return r
}

type initFn func(*testing.T, *repo) *repo

func withBootstrap(t *testing.T, r *repo) *repo {
	path := r.path
	if path != "" {
		path = filepath.Dir(path)
	}
	if err := Bootstrap(Config{Path: path, LogFn: t.Logf, ErrFn: t.Logf}); err != nil {
		t.Errorf("unable to bootstrap %s: %+v", r.path, err)
	}
	return r
}

func withOpenRoot(t *testing.T, r *repo) *repo {
	var err error
	r.conn, err = sqlOpen(r.path)
	if err != nil {
		t.Logf("Could not open db %s: %s", r.path, err)
	}
	return r
}

var (
	mockItems = vocab.ItemCollection{
		vocab.IRI("https://example.com/plain-iri"),
		&vocab.Object{ID: "https://example.com/1", Type: vocab.NoteType},
		&vocab.Place{ID: "https://example.com/arctic", Type: vocab.PlaceType},
		//&vocab.Profile{ID: "https://example.com/~jdoe/profile", Type: vocab.ProfileType},
		&vocab.Link{ID: "https://example.com/1", Href: "https://example.com/1", Type: vocab.LinkType},
		&vocab.Actor{ID: "https://example.com/~jdoe", Type: vocab.PersonType},
		&vocab.Activity{ID: "https://example.com/~jdoe/1", Type: vocab.UpdateType},
		&vocab.Object{ID: "https://example.com/~jdoe/tag-none", Type: vocab.UpdateType},
		&vocab.Question{ID: "https://example.com/~jdoe/2", Type: vocab.QuestionType},
		&vocab.IntransitiveActivity{ID: "https://example.com/~jdoe/3", Type: vocab.ArriveType},
		&vocab.Tombstone{ID: "https://example.com/objects/1", Type: vocab.TombstoneType},
		&vocab.Tombstone{ID: "https://example.com/actors/f00", Type: vocab.TombstoneType},
	}

	pk, _      = rsa.GenerateKey(rand.Reader, 4096)
	pkcs8Pk, _ = x509.MarshalPKCS8PrivateKey(pk)
	key        = pem.EncodeToMemory(&pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: pkcs8Pk,
	})

	pubEnc, _  = x509.MarshalPKIXPublicKey(pk.Public())
	pubEncoded = pem.EncodeToMemory(&pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: pubEnc,
	})

	apPublic = &vocab.PublicKey{
		ID:           "https://example.com/~jdoe#main",
		Owner:        "https://example.com/~jdoe",
		PublicKeyPem: string(pubEncoded),
	}

	defaultPw = []byte("dsa")

	encPw, _ = bcrypt.GenerateFromPassword(defaultPw, bcrypt.DefaultCost)
)

func withMockItems(t *testing.T, r *repo) *repo {
	for _, it := range mockItems {
		if _, err := save(r, it); err != nil {
			t.Errorf("unable to save item: %s: %s", it.GetLink(), err)
		}
	}
	return r
}

func withMetadataJDoe(t *testing.T, r *repo) *repo {
	m := Metadata{
		Pw:         encPw,
		PrivateKey: key,
	}

	if err := r.SaveMetadata("https://example.com/~jdoe", m); err != nil {
		t.Errorf("unable to save metadata for jdoe: %s", err)
	}
	return r
}

var (
	defaultClient = &osin.DefaultClient{
		Id:          "test-client",
		Secret:      "asd",
		RedirectUri: "https://example.com",
		UserData:    nil,
	}
)

func mockAuth(code string, cl osin.Client) *osin.AuthorizeData {
	return &osin.AuthorizeData{
		Client:    cl,
		Code:      code,
		ExpiresIn: 10,
		CreatedAt: time.Now().Add(10 * time.Minute).Round(10 * time.Minute),
		UserData:  vocab.IRI("https://example.com/jdoe"),
	}
}

func mockAccess(code string, cl osin.Client) *osin.AccessData {
	ad := &osin.AccessData{
		Client:        cl,
		AuthorizeData: mockAuth("test-code", cl),
		AccessToken:   code,
		ExpiresIn:     10,
		Scope:         "none",
		RedirectUri:   "http://localhost",
		CreatedAt:     time.Now().Add(10 * time.Minute).Round(10 * time.Minute),
		UserData:      vocab.IRI("https://example.com/jdoe"),
	}
	if code != "refresh-666" {
		ad.RefreshToken = "refresh-666"
		ad.AccessData = &osin.AccessData{
			Client:      cl,
			AccessToken: "refresh-666",
			ExpiresIn:   10,
			Scope:       "none",
			RedirectUri: "http://localhost",
			CreatedAt:   time.Now().Add(10 * time.Minute).Round(10 * time.Minute),
			UserData:    vocab.IRI("https://example.com/jdoe"),
		}
	}
	return ad
}

func withClient(t *testing.T, r *repo) *repo {
	if err := r.CreateClient(defaultClient); err != nil {
		t.Errorf("failed to create client: %s", err)
	}
	return r
}

func withAuthorization(t *testing.T, r *repo) *repo {
	if err := r.SaveAuthorize(mockAuth("test-code", defaultClient)); err != nil {
		t.Errorf("failed to create authorization data: %s", err)
	}
	return r
}

func withAccess(t *testing.T, r *repo) *repo {
	if err := r.SaveAccess(mockAccess("refresh-666", defaultClient)); err != nil {
		t.Errorf("failed to create access data: %s", err)
	}
	if err := r.SaveAccess(mockAccess("access-666", defaultClient)); err != nil {
		t.Errorf("failed to create access data: %s", err)
	}
	return r
}

var (
	rootIRI       = vocab.IRI("https://example.com")
	rootInboxIRI  = rootIRI.AddPath(string(vocab.Inbox))
	rootOutboxIRI = rootIRI.AddPath(string(vocab.Outbox))
	root          = &vocab.Actor{
		ID:        rootIRI,
		Type:      vocab.ServiceType,
		Published: publishedTime,
		Name:      vocab.DefaultNaturalLanguage("example.com"),
		Inbox:     rootInboxIRI,
		Outbox:    rootOutboxIRI,
	}

	publishedTime = time.Date(2001, time.January, 1, 0, 0, 0, 0, time.UTC)

	createCnt     = atomic.Int32{}
	allActors     = atomic.Pointer[vocab.ItemCollection]{}
	allObjects    = atomic.Pointer[vocab.ItemCollection]{}
	allActivities = atomic.Pointer[vocab.ItemCollection]{}
)

func withGeneratedRoot(root vocab.Item) initFn {
	return func(t *testing.T, r *repo) *repo {
		if _, err := r.Save(root); err != nil {
			t.Errorf("unable to save root service: %s", err)
		}
		return r
	}
}

func withGeneratedItems(items vocab.ItemCollection) initFn {
	return func(t *testing.T, r *repo) *repo {
		for _, it := range items {
			if _, err := save(r, it); err != nil {
				t.Errorf("unable to save %T[%s]: %s", it, it.GetLink(), err)
			}
		}
		return r
	}
}

func withActivitiesToCollections(activities vocab.ItemCollection) initFn {
	return func(t *testing.T, r *repo) *repo {
		collectionIRI := vocab.Outbox.IRI(root)
		_ = r.AddTo(collectionIRI, activities...)
		return r
	}
}

func createActivity(ob vocab.Item, attrTo vocab.Item) *vocab.Activity {
	act := new(vocab.Activity)
	act.Type = vocab.CreateType
	if ob != nil {
		act.Object = ob
	}
	act.AttributedTo = attrTo.GetLink()
	act.Actor = attrTo.GetLink()
	act.To = vocab.ItemCollection{rootIRI, vocab.PublicNS}
	createCnt.Add(1)

	return act
}

func withGeneratedMocks(t *testing.T, r *repo) *repo {
	idSetter := setId(rootIRI)
	r = withGeneratedRoot(root)(t, r)

	actors := make(vocab.ItemCollection, 0, 20)
	for range cap(actors) - 1 {
		actor := conformance.RandomActor(root)
		_ = vocab.OnObject(actor, func(object *vocab.Object) error {
			object.Published = publishedTime
			return idSetter(object)
		})
		_ = actors.Append(actor)
	}
	r = withGeneratedItems(actors)(t, r)
	allActors.Store(&actors)

	objects := make(vocab.ItemCollection, 0, 50)
	creates := make(vocab.ItemCollection, 0, 50)
	for range cap(objects) {
		//parent := actors[mrand.Intn(len(actors))]
		parent := root
		ob := conformance.RandomObject(parent)
		_ = vocab.OnObject(ob, func(object *vocab.Object) error {
			object.Published = publishedTime
			object.Tag = vocab.ItemCollection{conformance.RandomTag(parent)}
			return idSetter(object)
		})
		_ = objects.Append(ob)
		create := createActivity(ob, root)
		_ = vocab.OnObject(create, func(object *vocab.Object) error {
			object.Published = publishedTime
			return idSetter(object)
		})
		_ = creates.Append(create)
	}
	r = withGeneratedItems(objects)(t, r)
	allObjects.Store(&objects)

	activities := make(vocab.ItemCollection, 0, cap(actors)*10)
	for range cap(activities) {
		object := objects[mrand.Intn(len(objects))]
		//author := actors[mrand.Intn(len(actors))]
		author := root

		activity := conformance.RandomActivity(object, author)
		_ = vocab.OnObject(activity, func(object *vocab.Object) error {
			object.Published = publishedTime
			return idSetter(object)
		})
		_ = activities.Append(activity)
	}
	activities = append(creates, activities...)
	r = withGeneratedItems(activities)(t, r)
	r = withActivitiesToCollections(activities)(t, r)

	allActivities.Store(&activities)
	return r
}

func setId(base vocab.IRI) func(ob *vocab.Object) error {
	idMap := sync.Map{}
	return func(ob *vocab.Object) error {
		typ := ob.Type
		id := 1
		if latestId, ok := idMap.Load(typ); ok {
			id = latestId.(int) + 1
		}
		ob.ID = base.AddPath(strings.ToLower(string(typ))).AddPath(strconv.Itoa(id))
		idMap.Store(typ, id)
		return nil
	}
}

func filter(items vocab.ItemCollection, fil ...filters.Check) vocab.ItemCollection {
	result, _ := vocab.ToItemCollection(filters.Checks(fil).Run(items))
	return *result
}

func wantsRootOutboxPage(maxItems int, ff ...filters.Check) vocab.Item {
	return &vocab.OrderedCollectionPage{
		ID:           rootOutboxIRI,
		Type:         vocab.OrderedCollectionPageType,
		AttributedTo: rootIRI,
		Published:    publishedTime,
		CC:           vocab.ItemCollection{vocab.IRI("https://www.w3.org/ns/activitystreams#Public")},
		PartOf:       rootOutboxIRI,
		First:        vocab.IRI(string(rootOutboxIRI) + "?" + filters.ToValues(filters.WithMaxCount(maxItems)).Encode()),
		Next:         vocab.IRI(string(rootOutboxIRI) + "?" + filters.ToValues(filters.After(filters.SameID(rootIRI.AddPath("create/2"))), filters.WithMaxCount(maxItems)).Encode()),
		OrderedItems: filter(*allActivities.Load(), ff...),
		TotalItems:   allActivities.Load().Count(),
	}
}

func wantsRootOutbox(ff ...filters.Check) vocab.Item {
	col := &vocab.OrderedCollection{
		ID:           rootOutboxIRI,
		Type:         vocab.OrderedCollectionType,
		AttributedTo: rootIRI,
		Published:    publishedTime,
		CC:           vocab.ItemCollection{vocab.PublicNS},
		OrderedItems: filter(*allActivities.Load(), ff...),
		TotalItems:   allActivities.Load().Count(),
	}
	if len(ff) > 0 {
		col.First = vocab.IRI(string(rootOutboxIRI) + "?" + filters.ToValues(filters.WithMaxCount(filters.MaxItems)).Encode())
	}

	return col
}
