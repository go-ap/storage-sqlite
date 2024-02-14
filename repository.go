package sqlite

import (
	"crypto"
	"crypto/dsa"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rsa"
	"crypto/x509"
	"database/sql"
	"encoding/pem"
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	vocab "github.com/go-ap/activitypub"
	"github.com/go-ap/cache"
	"github.com/go-ap/errors"
	"github.com/go-ap/filters"
	"github.com/go-ap/processing"
	"golang.org/x/crypto/bcrypt"
)

var encodeItemFn = vocab.MarshalJSON
var decodeItemFn = vocab.UnmarshalJSON

type loggerFn func(string, ...interface{})

var defaultLogFn = func(string, ...interface{}) {}

type Config struct {
	Path        string
	CacheEnable bool
	LogFn       loggerFn
	ErrFn       loggerFn
}

// New returns a new repo repository
func New(c Config) (*repo, error) {
	p, err := getFullPath(c)
	if err != nil {
		return nil, err
	}
	return &repo{
		path:  p,
		logFn: c.LogFn,
		errFn: c.ErrFn,
		cache: cache.New(c.CacheEnable),
	}, nil
}

type repo struct {
	conn  *sql.DB
	path  string
	cache cache.CanStore
	logFn loggerFn
	errFn loggerFn
}

// Open opens the sqlite database
func (r *repo) Open() (err error) {
	if r.conn == nil {
		if r.conn, err = sqlOpen(r.path); err != nil {
			return err
		}
	}
	return err
}

// Close closes the sqlite database
func (r *repo) close() (err error) {
	if r.conn == nil {
		return
	}
	err = r.conn.Close()
	if err == nil {
		r.conn = nil
	}
	return
}

func getCollectionTypeFromIRI(i vocab.IRI) vocab.CollectionPath {
	col := vocab.CollectionPath(path.Base(i.String()))
	if !(filters.FedBOXCollections.Contains(col) || vocab.ActivityPubCollections.Contains(col)) {
		b, _ := path.Split(i.String())
		col = vocab.CollectionPath(path.Base(b))
	}

	if table := getCollectionTable(col); table != "" {
		return table
	}
	return "actors"
}

func getCollectionTypeFromItem(it vocab.Item) vocab.CollectionPath {
	switch {
	case vocab.ActorTypes.Contains(it.GetType()):
		return "actors"
	case vocab.ActivityTypes.Contains(it.GetType()):
		return "activities"
	case vocab.IntransitiveActivityTypes.Contains(it.GetType()):
		return "activities"
	default:
		if _, isActor := it.(*vocab.Person); isActor {
			return "actors"
		}
		if _, isActivity := it.(*vocab.Activity); isActivity {
			return "activities"
		}
		if _, isActivity := it.(*vocab.IntransitiveActivity); isActivity {
			return "activities"
		}
		return "objects"
	}
}

func getCollectionTable(typ vocab.CollectionPath) vocab.CollectionPath {
	switch typ {
	case vocab.Followers:
		fallthrough
	case vocab.Following:
		fallthrough
	case filters.ActorsType:
		fallthrough
	case vocab.Unknown:
		return "actors"
	case vocab.Inbox:
		fallthrough
	case vocab.Outbox:
		fallthrough
	case vocab.Shares:
		fallthrough
	case vocab.Likes:
		fallthrough
	case filters.ActivitiesType:
		return "activities"
	case filters.ObjectsType:
		fallthrough
	case vocab.Liked:
		fallthrough
	case vocab.Replies:
		return "objects"
	default:
		return ""
	}
}

func getCollectionTableFromFilter(f *filters.Filters) vocab.CollectionPath {
	return getCollectionTable(f.Collection)
}

// Load
func (r *repo) Load(i vocab.IRI, ff ...filters.Check) (vocab.Item, error) {
	if err := r.Open(); err != nil {
		return nil, err
	}
	defer r.Close()

	f, err := filters.FiltersFromIRI(i)
	if err != nil {
		return nil, err
	}

	ret, err := loadFromDb(r, i, f)
	if ret != nil && ret.Count() == 1 && f.IsItemIRI() {
		return ret.Collection().First(), err
	}
	return ret, err
}

var (
	nilItemErr    = errors.Errorf("nil item")
	nilItemIRIErr = errors.Errorf("nil IRI for item")
)

// Save
func (r *repo) Save(it vocab.Item) (vocab.Item, error) {
	if err := r.Open(); err != nil {
		return nil, err
	}
	defer r.Close()
	return save(r, it)
}

var emptyCol = []byte{'[', ']'}

// Create
func (r *repo) Create(col vocab.CollectionInterface) (vocab.CollectionInterface, error) {
	if vocab.IsNil(col) {
		return nil, nilItemErr
	}
	if col.GetLink() == "" {
		return col, nilItemIRIErr
	}
	if err := r.Open(); err != nil {
		return col, err
	}
	defer r.Close()
	return r.createCollection(col)
}

func (r *repo) createCollection(col vocab.CollectionInterface) (vocab.CollectionInterface, error) {
	published := time.Now().UTC()
	_ = vocab.OnCollection(col, func(c *vocab.Collection) error {
		if !c.Published.IsZero() {
			published = c.Published
		} else {
			c.Published = published
		}
		return nil
	})
	_ = vocab.OnOrderedCollection(col, func(c *vocab.OrderedCollection) error {
		if !c.Published.IsZero() {
			published = c.Published
		} else {
			c.Published = published
		}
		return nil
	})

	raw, err := vocab.MarshalJSON(col)
	ins := "INSERT OR REPLACE INTO collections (raw, items) VALUES (?, ?);"
	_, err = r.conn.Exec(ins, raw, emptyCol)
	if err != nil {
		r.errFn("query error: %s\n%s\n%#v", err, ins)
		return col, errors.Annotatef(err, "unable to save Collection")
	}

	return col, nil
}

func (r *repo) removeFrom(col vocab.IRI, it vocab.Item) error {
	if vocab.IsNil(it) {
		return nil
	}
	if r.conn == nil {
		return errors.Newf("nil sql connection")
	}
	colSel := "SELECT iri, raw, items from collections WHERE iri = ?;"
	rows, err := r.conn.Query(colSel, col.GetLink())
	if err != nil {
		r.errFn("query error: %s\n%s\n%#v", err, colSel)
		return errors.NotFoundf("Unable to load %s", col.GetLink())
	}
	var c vocab.Item
	for rows.Next() {
		var iri string
		var raw []byte
		var itemsRaw []byte

		err = rows.Scan(&iri, &raw, &itemsRaw)
		if err != nil {
			return errors.Annotatef(err, "scan values error")
		}
		c, err = vocab.UnmarshalJSON(raw)
		if err != nil {
			return errors.Annotatef(err, "unable to unmarshal Collection")
		}
	}

	if c == nil {
		return errors.NotFoundf("not found Collection %s", col.GetLink())
	}
	allItems := make(vocab.IRIs, 0)
	err = vocab.OnOrderedCollection(c, func(col *vocab.OrderedCollection) error {
		col.Updated = time.Now().UTC()
		col.OrderedItems.Remove(it)
		return nil
	})
	if err != nil {
		return errors.Annotatef(err, "unable to update Collection")
	}
	allItems = append(allItems, it.GetLink())

	raw, err := vocab.MarshalJSON(it)
	if err != nil {
		return errors.Annotatef(err, "unable to marshal Collection")
	}
	items, err := vocab.MarshalJSON(allItems)
	if err != nil {
		return errors.Annotatef(err, "unable to marshal Collection items")
	}

	query := "UPDATE collections SET raw = ?, items = ? WHERE iri = ?;"
	_, err = r.conn.Exec(query, raw, items, c.GetLink())
	if err != nil {
		r.errFn("query error: %s\n%s\n%#v", err, query)
		return errors.Annotatef(err, "query error")
	}

	return nil
}

// RemoveFrom
func (r *repo) RemoveFrom(col vocab.IRI, it vocab.Item) error {
	if err := r.Open(); err != nil {
		return err
	}
	defer r.Close()
	return r.removeFrom(col, it)
}

func (r *repo) addTo(col vocab.IRI, it vocab.Item) error {
	if vocab.IsNil(it) {
		return nil
	}
	if r.conn == nil {
		return errors.Newf("nil sql connection")
	}
	var c vocab.Item

	colSel := "SELECT iri, raw, items from collections WHERE iri = ?;"
	rows, err := r.conn.Query(colSel, col.GetLink())
	if err != nil {
		r.logFn("unable to load collection object for %s: %s", col.GetLink(), err.Error())
	} else {
		for rows.Next() {
			var iri string
			var raw []byte
			var itemsRaw []byte

			err = rows.Scan(&iri, &raw, &itemsRaw)
			if err != nil {
				return errors.Annotatef(err, "scan values error")
			}
			c, err = vocab.UnmarshalJSON(raw)
			if err != nil {
				return errors.Annotatef(err, "unable to unmarshal Collection")
			}
		}
	}

	if c == nil && isHiddenCollectionIRI(col.GetLink()) {
		// NOTE(marius): this creates blocked/ignored collections if they don't exist as dumb folders
		if c, err = save(r, newOrderedCollection(col.GetLink())); err != nil {
			r.errFn("query error: %s\n%s %#v", err, colSel, vocab.IRIs{col.GetLink()})
		}
	}

	if c == nil {
		return errors.NotFoundf("not found Collection %s", col.GetLink())
	}
	allItems := make(vocab.IRIs, 0)
	err = vocab.OnOrderedCollection(c, func(col *vocab.OrderedCollection) error {
		col.Updated = time.Now().UTC()
		// NOTE(marius): this should ideally be empty
		for _, cit := range col.OrderedItems {
			allItems = append(allItems, cit.GetLink())
		}
		return nil
	})
	if err != nil {
		return errors.Annotatef(err, "unable to update Collection")
	}
	allItems = append(allItems, it.GetLink())

	rawItems, err := vocab.MarshalJSON(allItems)
	if err != nil {
		return errors.Annotatef(err, "unable to marshal Collection items")
	}

	if orderedCollectionTypes.Contains(col.GetType()) {
		err = vocab.OnOrderedCollection(col, func(c *vocab.OrderedCollection) error {
			c.TotalItems += 1
			c.OrderedItems = nil
			return nil
		})
	} else if collectionTypes.Contains(col.GetType()) {
		err = vocab.OnCollection(col, func(c *vocab.Collection) error {
			c.TotalItems += 1
			c.Items = nil
			return nil
		})
	}

	raw, err := vocab.MarshalJSON(c)
	if err != nil {
		return errors.Annotatef(err, "unable to marshal Collection")
	}
	query := `INSERT INTO collections (raw, items) VALUES (?, ?) 
		ON CONFLICT(iri) DO UPDATE SET raw = ?, items = ?;`
	_, err = r.conn.Exec(query, raw, rawItems, raw, rawItems)
	if err != nil {
		r.errFn("query error: %s\n%s %#v", err, query, vocab.IRIs{c.GetLink()})
		return errors.Annotatef(err, "query error")
	}

	return nil
}

// AddTo
func (r *repo) AddTo(col vocab.IRI, it vocab.Item) error {
	if err := r.Open(); err != nil {
		return err
	}
	defer r.Close()

	return r.addTo(col, it)
}

// Delete
func (r *repo) Delete(it vocab.Item) error {
	if vocab.IsNil(it) {
		return nil
	}
	err := r.Open()
	defer r.Close()
	if err != nil {
		return err
	}

	if it.IsCollection() {
		err := vocab.OnCollectionIntf(it, func(c vocab.CollectionInterface) error {
			var err error
			for _, it := range c.Collection() {
				if err = r.Delete(it); err != nil {
					return err
				}
			}
			return nil
		})
		return err
	}

	return delete(*r, it)
}

// PasswordSet
func (r *repo) PasswordSet(it vocab.Item, pw []byte) error {
	pw, err := bcrypt.GenerateFromPassword(pw, -1)
	if err != nil {
		return errors.Annotatef(err, "could not generate pw hash")
	}
	m := processing.Metadata{
		Pw: pw,
	}
	return r.SaveMetadata(m, it.GetLink())
}

// PasswordCheck
func (r *repo) PasswordCheck(it vocab.Item, pw []byte) error {
	m, err := r.LoadMetadata(it.GetLink())
	if err != nil {
		return errors.Annotatef(err, "Could not find load metadata for %s", it)
	}
	if err := bcrypt.CompareHashAndPassword(m.Pw, pw); err != nil {
		return errors.NewUnauthorized(err, "Invalid pw")
	}
	return err
}

// LoadMetadata
func (r *repo) LoadMetadata(iri vocab.IRI) (*processing.Metadata, error) {
	err := r.Open()
	if err != nil {
		return nil, err
	}
	defer r.Close()

	m := new(processing.Metadata)
	raw, err := loadMetadataFromTable(r.conn, iri)
	if err != nil {
		return nil, err
	}
	err = decodeFn(raw, m)
	if err != nil {
		return nil, errors.Annotatef(err, "Could not unmarshal metadata")
	}
	return m, nil
}

// SaveMetadata
func (r *repo) SaveMetadata(m processing.Metadata, iri vocab.IRI) error {
	err := r.Open()
	if err != nil {
		return err
	}
	defer r.Close()

	entryBytes, err := encodeFn(m)
	if err != nil {
		return errors.Annotatef(err, "Could not marshal metadata")
	}
	return saveMetadataToTable(r.conn, iri, entryBytes)
}

// LoadKey loads a private key for an actor found by its IRI
func (r *repo) LoadKey(iri vocab.IRI) (crypto.PrivateKey, error) {
	m, err := r.LoadMetadata(iri)
	if err != nil {
		return nil, err
	}
	b, _ := pem.Decode(m.PrivateKey)
	if b == nil {
		return nil, errors.Errorf("failed decoding pem")
	}
	prvKey, err := x509.ParsePKCS8PrivateKey(b.Bytes)
	if err != nil {
		return nil, err
	}
	return prvKey, nil
}

func getFullPath(c Config) (string, error) {
	p, err := getAbsStoragePath(c.Path)
	if err != nil {
		return "memory", err
	}
	if err := mkDirIfNotExists(p); err != nil {
		return "memory", err
	}
	return path.Join(p, "storage.sqlite"), nil
}

func getAbsStoragePath(p string) (string, error) {
	if !filepath.IsAbs(p) {
		var err error
		p, err = filepath.Abs(p)
		if err != nil {
			return "", err
		}
	}
	if fi, err := os.Stat(p); err != nil {
		return "", err
	} else if !fi.IsDir() {
		return "", errors.NotValidf("path %s is invalid for storage", p)
	}
	return p, nil
}

func mkDirIfNotExists(p string) error {
	fi, err := os.Stat(p)
	if err != nil && os.IsNotExist(err) {
		err = os.MkdirAll(p, os.ModeDir|os.ModePerm|0700)
	}
	if err != nil {
		return err
	}
	fi, err = os.Stat(p)
	if err != nil {
		return err
	} else if !fi.IsDir() {
		return errors.Errorf("path exists, and is not a folder %s", p)
	}
	return nil
}

func saveMetadataToTable(conn *sql.DB, iri vocab.IRI, m []byte) error {
	query := "INSERT OR REPLACE INTO meta (iri, raw) VALUES(?, ?);"
	_, err := conn.Exec(query, iri, m)
	return err
}

func loadMetadataFromTable(conn *sql.DB, iri vocab.IRI) ([]byte, error) {
	var meta []byte
	sel := "SELECT raw FROM meta WHERE iri = ?;"
	err := conn.QueryRow(sel, iri).Scan(&meta)
	return meta, err
}

type Filterable = vocab.LinkOrIRI

func isSingleItem(f Filterable) bool {
	if _, isIRI := f.(vocab.IRI); isIRI {
		return true
	}
	if _, isItem := f.(vocab.Item); isItem {
		return true
	}
	return false
}

func loadFromThreeTables(r *repo, f *filters.Filters) (vocab.CollectionInterface, error) {
	conn := r.conn
	// NOTE(marius): this doesn't seem to be working, our filter is never an IRI or Item
	if isSingleItem(f) && r.cache != nil {
		if cachedIt := r.cache.Load(f.GetLink()); cachedIt != nil {
			return &vocab.ItemCollection{cachedIt}, nil
		}
	}
	clauses, values := getWhereClauses(f)
	var total uint = 0

	selects := make([]string, 0, 3)
	counts := make([]string, 0, 3)
	for _, table := range []string{"actors", "objects", "activities"} {
		counts = append(counts, fmt.Sprintf("SELECT COUNT(iri) FROM %s WHERE %s", table, strings.Join(clauses, " AND ")))
		selects = append(selects, fmt.Sprintf("SELECT iri, raw FROM %s WHERE %s ORDER BY published %s", table, strings.Join(clauses, " AND "), getLimit(f)))
	}
	selCnt := strings.Join(counts, " UNION ")
	if err := conn.QueryRow(selCnt, values...).Scan(&total); err != nil {
		return nil, errors.Annotatef(err, "unable to count all rows")
	}
	ret := make(vocab.ItemCollection, 0)
	if total == 0 {
		return &ret, nil
	}

	sel := strings.Join(counts, " UNION ")
	rows, err := conn.Query(sel, values...)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, errors.Annotatef(err, "unable to run select")
	}

	// Iterate through the result set
	for rows.Next() {
		var iri string
		var raw []byte
		err = rows.Scan(&iri, &raw)
		if err != nil {
			return &ret, errors.Annotatef(err, "scan values error")
		}

		it, err := decodeItemFn(raw)
		if err != nil {
			return &ret, errors.Annotatef(err, "unable to unmarshal raw item")
		}
		if vocab.IsObject(it) && r.cache != nil {
			r.cache.Store(it.GetLink(), it)
		}
		ret = append(ret, it)
	}
	/*
		if table == "actors" {
			ret = loadActorFirstLevelIRIProperties(r, ret, f)
		}
		if table == "objects" {
			ret = loadObjectFirstLevelIRIProperties(r, ret, f)
		}
		if table == "activities" {
			ret = runActivityFilters(r, ret, f)
		}
		ret = runObjectFilters(r, ret, f)
	*/
	return &ret, err
}

var storageCollectionPaths = append(filters.FedBOXCollections, append(vocab.OfActor, vocab.OfObject...)...)

func colIRI(iri vocab.IRI) vocab.IRI {
	u, err := iri.URL()
	if err != nil {
		return ""
	}
	u.RawFragment = ""
	u.RawQuery = ""

	return vocab.IRI(u.String())
}

func iriPath(iri vocab.IRI) string {
	u, err := iri.URL()
	if err != nil {
		return ""
	}

	pieces := make([]string, 0)
	if h := u.Host; h != "" {
		pieces = append(pieces, h)
	}
	if p := u.Path; p != "" && p != "/" {
		pieces = append(pieces, p)
	}
	//if u.ForceQuery || u.RawQuery != "" {
	//	pieces = append(pieces, url.PathEscape(u.RawQuery))
	//}
	if u.Fragment != "" {
		pieces = append(pieces, url.PathEscape(u.Fragment))
	}
	return filepath.Join(pieces...)
}

func isStorageCollectionIRI(iri vocab.IRI) bool {
	lst := vocab.CollectionPath(filepath.Base(iriPath(iri)))
	return storageCollectionPaths.Contains(lst)
}

func loadFromOneTable(r *repo, table vocab.CollectionPath, f *filters.Filters) (vocab.CollectionInterface, error) {
	conn := r.conn
	// NOTE(marius): this doesn't seem to be working, our filter is never an IRI or Item
	if isSingleItem(f) && r.cache != nil {
		if cachedIt := r.cache.Load(f.GetLink()); cachedIt != nil {
			return &vocab.ItemCollection{cachedIt}, nil
		}
	}
	if table == "" {
		table = getCollectionTableFromFilter(f)
	}
	clauses, values := getWhereClauses(f)
	var total uint = 0

	selCnt := fmt.Sprintf("SELECT COUNT(iri) FROM %s WHERE %s", table, strings.Join(clauses, " AND "))
	if err := conn.QueryRow(selCnt, values...).Scan(&total); err != nil {
		return nil, errors.Annotatef(err, "unable to count all rows")
	}
	ret := make(vocab.ItemCollection, 0)
	if total == 0 {
		return &ret, nil
	}

	sel := fmt.Sprintf("SELECT iri, raw FROM %s WHERE %s ORDER BY published %s", table, strings.Join(clauses, " AND "), getLimit(f))
	rows, err := conn.Query(sel, values...)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, errors.Annotatef(err, "unable to run select")
	}

	// Iterate through the result set
	for rows.Next() {
		var iri string
		var raw []byte
		err = rows.Scan(&iri, &raw)
		if err != nil {
			return &ret, errors.Annotatef(err, "scan values error")
		}

		it, err := decodeItemFn(raw)
		if err != nil {
			return &ret, errors.Annotatef(err, "unable to unmarshal raw item")
		}
		if vocab.IsObject(it) && r.cache != nil {
			r.cache.Store(it.GetLink(), it)
		}
		ret = append(ret, it)
	}
	if table == "actors" {
		ret = loadActorFirstLevelIRIProperties(r, ret, f)
	}
	if table == "objects" {
		ret = loadObjectFirstLevelIRIProperties(r, ret, f)
	}
	if table == "activities" {
		ret = runActivityFilters(r, ret, f)
	}
	ret = runObjectFilters(r, ret, f)
	return &ret, err
}

func runObjectFilters(r *repo, ret vocab.ItemCollection, f *filters.Filters) vocab.ItemCollection {
	result := make(vocab.ItemCollection, 0)

	for i, it := range ret {
		if it, _ = filters.FilterIt(it, f); it != nil {
			result = append(result, ret[i])
		}
	}

	return result
}

func objectFilter(ret vocab.ItemCollection) *filters.Filters {
	if len(ret) == 0 {
		return nil
	}
	f := new(filters.Filters)
	f.ItemKey = make(filters.CompStrs, len(ret))
	for i, it := range ret {
		vocab.OnActivity(it, func(a *vocab.Activity) error {
			f.ItemKey[i] = filters.StringEquals(a.Object.GetLink().String())
			return nil
		})
	}
	return f
}

func actorFilter(ret vocab.ItemCollection) *filters.Filters {
	if len(ret) == 0 {
		return nil
	}
	f := new(filters.Filters)
	f.ItemKey = make(filters.CompStrs, len(ret))
	for i, it := range ret {
		vocab.OnActivity(it, func(a *vocab.Activity) error {
			f.ItemKey[i] = filters.StringEquals(a.Actor.GetLink().String())
			return nil
		})
	}
	return f
}

func targetFilter(ret vocab.ItemCollection) *filters.Filters {
	if len(ret) == 0 {
		return nil
	}
	f := new(filters.Filters)
	f.ItemKey = make(filters.CompStrs, len(ret))
	for i, it := range ret {
		vocab.OnActivity(it, func(a *vocab.Activity) error {
			if a.Target != nil {
				f.ItemKey[i] = filters.StringEquals(a.Target.GetLink().String())
			}
			return nil
		})
	}
	return f
}

func keepObject(f *filters.Filters) func(act *vocab.Activity, ob vocab.Item) bool {
	return func(act *vocab.Activity, ob vocab.Item) bool {
		keep := false
		if act.Object.GetLink().Equals(ob.GetLink(), false) {
			act.Object = ob
			keep = f.ItemsMatch(act.Object)
		}
		return keep
	}
}

func keepActor(f *filters.Filters) func(act *vocab.Activity, ob vocab.Item) bool {
	return func(act *vocab.Activity, ob vocab.Item) bool {
		var keep bool
		if act.Actor.GetLink().Equals(ob.GetLink(), false) {
			act.Actor = ob
			keep = f.Actor.ItemsMatch(act.Actor)
		}
		return keep
	}
}

func keepTarget(f *filters.Filters) func(act *vocab.Activity, ob vocab.Item) bool {
	return func(act *vocab.Activity, ob vocab.Item) bool {
		var keep bool
		if act.Target.GetLink().Equals(ob.GetLink(), false) {
			act.Target = ob
			keep = f.Target.ItemsMatch(act.Target)
		}
		return keep
	}
}

func loadTagsForObject(r *repo) func(o *vocab.Object) error {
	return func(o *vocab.Object) error {
		if len(o.Tag) == 0 {
			return nil
		}
		return vocab.OnItemCollection(o.Tag, func(col *vocab.ItemCollection) error {
			for i, t := range *col {
				if vocab.IsNil(t) || !vocab.IsIRI(t) {
					return nil
				}
				f, _ := filters.FiltersFromIRI(t.GetLink())
				if ob, err := loadFromOneTable(r, "objects", f); err == nil {
					(*col)[i] = ob.Collection().First()
				}
			}
			return nil
		})
	}
}

func loadActorFirstLevelIRIProperties(r *repo, ret vocab.ItemCollection, f *filters.Filters) vocab.ItemCollection {
	return loadObjectFirstLevelIRIProperties(r, ret, f)
}

func loadObjectFirstLevelIRIProperties(r *repo, ret vocab.ItemCollection, f *filters.Filters) vocab.ItemCollection {
	for _, it := range ret {
		vocab.OnObject(it, loadTagsForObject(r))
	}
	return ret
}

func runActivityFilters(r *repo, ret vocab.ItemCollection, f *filters.Filters) vocab.ItemCollection {
	// If our filter contains values for filtering the activity's object or actor, we do that here:
	//  for the case where the corresponding values are not set, this doesn't do anything
	toRemove := make(vocab.IRIs, 0)
	if f.Object != nil {
		toRemove = append(toRemove, childFilter(r, &ret, objectFilter, keepObject(f.Object))...)
	}
	if f.Actor != nil {
		toRemove = append(toRemove, childFilter(r, &ret, actorFilter, keepActor(f.Actor))...)
	}
	if f.Target != nil {
		toRemove = append(toRemove, childFilter(r, &ret, targetFilter, keepTarget(f.Target))...)
	}

	result := make(vocab.ItemCollection, 0)
	for _, it := range ret {
		keep := true
		for _, iri := range toRemove {
			if it.GetLink().Equals(iri, false) {
				keep = false
			}
		}
		if keep {
			result = append(result, it)
		}
	}
	return result
}

type (
	iriFilterFn func(ret vocab.ItemCollection) *filters.Filters
	keepFn      func(act *vocab.Activity, ob vocab.Item) bool
)

func childFilter(r *repo, ret *vocab.ItemCollection, filterFn iriFilterFn, keepFn keepFn) vocab.IRIs {
	f := filterFn(*ret)
	if f == nil {
		return nil
	}
	toRemove := make(vocab.IRIs, 0)
	children, err := loadFromThreeTables(r, f)
	if err != nil {
		return toRemove
	}
	for _, rr := range *ret {
		if !vocab.ActivityTypes.Contains(rr.GetType()) {
			toRemove = append(toRemove, rr.GetID())
			continue
		}
		keep := false
		_ = vocab.OnActivity(rr, func(a *vocab.Activity) error {
			for _, ob := range children.Collection() {
				keep = keepFn(a, ob)
				if keep {
					break
				}
			}
			return nil
		})
		if !keep {
			toRemove = append(toRemove, rr.GetID())
		}
	}
	return toRemove
}

var orderedCollectionTypes = vocab.ActivityVocabularyTypes{vocab.OrderedCollectionPageType, vocab.OrderedCollectionType}
var collectionTypes = vocab.ActivityVocabularyTypes{vocab.CollectionPageType, vocab.CollectionType}

func loadFromDb(r *repo, iri vocab.IRI, f *filters.Filters) (vocab.CollectionInterface, error) {
	conn := r.conn
	table := getCollectionTableFromFilter(f)
	clauses, values := getWhereClauses(f)
	var total uint = 0

	if !isStorageCollectionIRI(f.GetLink()) {
		col, err := loadFromOneTable(r, table, f)
		if err != nil {
			return col, err
		}
		if col.Count() == 0 {
			return nil, errors.NotFoundf("Not found")
		}
		return col, nil
	}
	// todo(marius): this needs to be split into three cases:
	//  1. IRI corresponds to a collection that is not one of the storage tables (ie, not activities, actors, objects):
	//    Then we look for correspondences in the collections table.
	//  2. The IRI corresponds to the activities, actors, objects tables:
	//    Then we load from the corresponding table using `iri LIKE IRI%` criteria
	//  3. IRI corresponds to an object: we load directly from the corresponding table.
	selCnt := fmt.Sprintf("SELECT COUNT(iri) FROM %s WHERE %s", table, strings.Join(clauses, " AND "))
	err := conn.QueryRow(selCnt, values...).Scan(&total)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, errors.Annotatef(err, "unable to count all rows")
	}
	items, err := loadFromOneTable(r, table, f)
	if err != nil {
		return nil, err
	}
	par, err := loadFromCollectionTable(r, colIRI(iri), f)
	if err != nil {
		return nil, err
	}
	if vocab.IsNil(par) {
		return nil, errors.NotFoundf("Unable to find collection %s", iri)
	}
	_ = vocab.OnObject(par, func(ob *vocab.Object) error {
		ob.ID = iri
		return nil
	})
	if orderedCollectionTypes.Contains(par.GetType()) {
		_ = vocab.OnOrderedCollection(par, postProcessOrderedItems(items.Collection()))
	} else if collectionTypes.Contains(par.GetType()) {
		_ = vocab.OnCollection(par, postProcessItems(items.Collection()))
	}
	return par, err
}

func postProcessItems(items vocab.ItemCollection) vocab.WithCollectionFn {
	return func(col *vocab.Collection) error {
		col.Items = items
		col.TotalItems = uint(len(items))
		return nil
	}
}

func postProcessOrderedItems(items vocab.ItemCollection) vocab.WithOrderedCollectionFn {
	return func(col *vocab.OrderedCollection) error {
		col.OrderedItems = items
		sort.Slice(col.OrderedItems, func(i, j int) bool {
			return vocab.ItemOrderTimestamp(col.OrderedItems[i], col.OrderedItems[j])
		})
		col.TotalItems = uint(len(items))
		return nil
	}
}

func isHiddenCollectionIRI(i vocab.IRI) bool {
	lst := vocab.CollectionPath(filepath.Base(i.String()))
	return filters.HiddenCollections.Contains(lst)
}

func loadFromCollectionTable(r *repo, iri vocab.IRI, f *filters.Filters) (vocab.CollectionInterface, error) {
	conn := r.conn

	var cIri vocab.IRI
	var itemsRaw []byte
	var raw []byte

	sel := "SELECT iri, raw, items FROM collections WHERE iri = ?"
	if err := conn.QueryRow(sel, iri.String()).Scan(&cIri, &raw, &itemsRaw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errors.NotFoundf("Unable to find collection %s", f.Collection)
		}
		return nil, errors.Annotatef(err, "unable to run select")
	}
	ff := *f

	var res vocab.CollectionInterface
	col, err := vocab.UnmarshalJSON(raw)
	if err != nil {
		return nil, errors.Annotatef(err, "Collection unmarshal error")
	}
	var ok bool
	if res, ok = col.(vocab.CollectionInterface); !ok {
		return nil, errors.Newf("loaded item is not a valid Collection")
	}

	items, err := vocab.UnmarshalJSON(itemsRaw)
	if err != nil {
		return nil, errors.Annotatef(err, "Collection items unmarshal error")
	}
	_ = vocab.OnIRIs(items, func(col *vocab.IRIs) error {
		for _, iri := range *col {
			iriF := filters.StringEquals(iri.String())
			ff.ItemKey = append(ff.ItemKey, iriF)
		}
		return nil
	})

	if len(ff.ItemKey) > 0 {
		err = vocab.OnCollectionIntf(res, func(col vocab.CollectionInterface) error {
			retAct, err := loadFromThreeTables(r, &ff)
			if err != nil {
				return err
			}
			col.Append(retAct.Collection()...)
			return nil
		})
	}
	return res, nil
}

func delete(l repo, it vocab.Item) error {
	iri := it.GetLink()

	table := string(filters.ObjectsType)
	if vocab.ActivityTypes.Contains(it.GetType()) {
		table = string(filters.ActivitiesType)
	} else if vocab.ActorTypes.Contains(it.GetType()) {
		table = string(filters.ActorsType)
	} else if it.GetType() == vocab.TombstoneType {
		if strings.Contains(iri.String(), string(filters.ActorsType)) {
			table = string(filters.ActorsType)
		}
		if strings.Contains(iri.String(), string(filters.ActivitiesType)) {
			table = string(filters.ActivitiesType)
		}
	}

	query := fmt.Sprintf("DELETE FROM %s where iri = $1;", table)
	if _, err := l.conn.Exec(query, it.GetLink()); err != nil {
		l.errFn("query error: %s\n%s", err, query)
		return errors.Annotatef(err, "query error")
	}

	if l.cache != nil {
		l.cache.Delete(it.GetLink())
	}
	return nil
}

const upsertQ = "INSERT OR REPLACE INTO %s (%s) VALUES (%s);"

func save(r *repo, it vocab.Item) (vocab.Item, error) {
	if vocab.IsNil(it) {
		return nil, nil
	}

	iri := it.GetLink()

	if err := createCollections(r, it); err != nil {
		return it, errors.Annotatef(err, "could not create object's collections")
	}
	raw, err := encodeItemFn(it)
	if err != nil {
		r.errFn("query error: %s", err)
		return it, errors.Annotatef(err, "query error")
	}

	columns := []string{"raw"}
	tokens := []string{"?"}
	params := []interface{}{interface{}(raw)}

	table := string(filters.ObjectsType)
	typ := it.GetType()
	if vocab.ActivityTypes.Contains(typ) || vocab.IntransitiveActivityTypes.Contains(typ) {
		table = string(filters.ActivitiesType)
	} else if vocab.ActorTypes.Contains(typ) {
		table = string(filters.ActorsType)
	} else if typ == vocab.TombstoneType {
		if strings.Contains(iri.String(), string(filters.ActorsType)) {
			table = string(filters.ActorsType)
		}
		if strings.Contains(iri.String(), string(filters.ActivitiesType)) {
			table = string(filters.ActivitiesType)
		}
	}

	query := fmt.Sprintf("INSERT OR REPLACE INTO %s (%s) VALUES (%s);", table, strings.Join(columns, ", "), strings.Join(tokens, ", "))

	if _, err = r.conn.Exec(query, params...); err != nil {
		r.errFn("query error: %s\n%s", err, query)
		return it, errors.Annotatef(err, "query error")
	}
	col, key := path.Split(iri.String())
	if len(key) > 0 && vocab.ValidCollection(vocab.CollectionPath(path.Base(col))) {
		// Add private items to the collections table
		if colIRI, k := vocab.Split(vocab.IRI(col)); k == "" {
			if err := r.addTo(colIRI, it); err != nil {
				return it, err
			}
		}
	}

	if r.cache != nil {
		r.cache.Store(it.GetLink(), it)
	}
	return it, nil
}

func newOrderedCollection(colIRI vocab.IRI) vocab.CollectionInterface {
	col := vocab.OrderedCollection{
		ID:        colIRI,
		Type:      vocab.OrderedCollectionType,
		Published: time.Now().UTC(),
	}
	return &col
}

func createCollectionInTable(r *repo, it vocab.Item) (vocab.Item, error) {
	if vocab.IsNil(it) {
		return nil, nil
	}

	ff, _ := filters.FiltersFromIRI(it.GetLink())
	colObject, err := loadFromCollectionTable(r, colIRI(it.GetLink()), ff)
	if colObject == nil {
		c, ok := it.(vocab.CollectionInterface)
		if !ok {
			c = newOrderedCollection(it.GetLink())
		}
		it, err = r.createCollection(c)
	}
	if err != nil {
		return nil, errors.Annotatef(err, "saving collection object is not done")
	}

	return it.GetLink(), nil
}

// createCollections
func createCollections(r *repo, it vocab.Item) error {
	if vocab.IsNil(it) || !it.IsObject() {
		return nil
	}
	if vocab.ActorTypes.Contains(it.GetType()) {
		vocab.OnActor(it, func(p *vocab.Actor) error {
			p.Inbox, _ = createCollectionInTable(r, p.Inbox)
			p.Outbox, _ = createCollectionInTable(r, p.Outbox)
			p.Followers, _ = createCollectionInTable(r, p.Followers)
			p.Following, _ = createCollectionInTable(r, p.Following)
			p.Liked, _ = createCollectionInTable(r, p.Liked)
			return nil
		})
	}
	return vocab.OnObject(it, func(o *vocab.Object) error {
		o.Replies, _ = createCollectionInTable(r, o.Replies)
		o.Likes, _ = createCollectionInTable(r, o.Likes)
		o.Shares, _ = createCollectionInTable(r, o.Shares)
		return nil
	})
}

// SaveKey saves a private key for an actor found by its IRI
func (r *repo) SaveKey(iri vocab.IRI, key crypto.PrivateKey) (vocab.Item, error) {
	f, _ := filters.FiltersFromIRI(iri)
	result, err := loadFromThreeTables(r, f)
	if err != nil {
		return nil, err
	}
	if result.Count() == 0 {
		return nil, errors.NotFoundf("not found %s", iri)
	}

	ob := result.Collection().First()
	typ := ob.GetType()
	if !vocab.ActorTypes.Contains(typ) {
		return ob, errors.Newf("trying to generate keys for invalid ActivityPub object type: %s", typ)
	}
	actor, err := vocab.ToActor(ob)
	if err != nil {
		return ob, errors.Newf("trying to generate keys for invalid ActivityPub object type: %s", typ)
	}

	m, err := r.LoadMetadata(iri)
	if err != nil && !errors.IsNotFound(err) {
		return ob, err
	}
	if m.PrivateKey != nil {
		r.logFn("actor %s already has a private key", iri)
	}
	prvEnc, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		r.errFn("unable to x509.MarshalPKCS8PrivateKey() the private key %T for %s", key, iri)
		return ob, err
	}

	m.PrivateKey = pem.EncodeToMemory(&pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: prvEnc,
	})
	if err = r.SaveMetadata(*m, iri); err != nil {
		r.errFn("unable to save the private key %T for %s", key, iri)
		return ob, err
	}

	var pub crypto.PublicKey
	switch prv := key.(type) {
	case *ecdsa.PrivateKey:
		pub = prv.Public()
	case *rsa.PrivateKey:
		pub = prv.Public()
	case *dsa.PrivateKey:
		pub = &prv.PublicKey
	case *ed25519.PrivateKey:
		pub = prv.Public()
	default:
		r.errFn("received key %T does not match any of the known private key types", key)
		return ob, nil
	}
	pubEnc, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		r.errFn("unable to x509.MarshalPKIXPublicKey() the private key %T for %s", pub, iri)
		return ob, err
	}
	pubEncoded := pem.EncodeToMemory(&pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: pubEnc,
	})

	actor.PublicKey = vocab.PublicKey{
		ID:           vocab.IRI(fmt.Sprintf("%s#main", iri)),
		Owner:        iri,
		PublicKeyPem: string(pubEncoded),
	}
	return r.Save(actor)
}
