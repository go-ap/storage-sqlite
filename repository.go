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
	"os"
	"path"
	"path/filepath"
	"strings"

	vocab "github.com/go-ap/activitypub"
	"github.com/go-ap/errors"
	"github.com/go-ap/filters"
	"github.com/go-ap/processing"
	"github.com/go-ap/storage-sqlite/internal/cache"
	"golang.org/x/crypto/bcrypt"
)

var encodeItemFn = vocab.MarshalJSON
var decodeItemFn = vocab.UnmarshalJSON

var errNotImplemented = errors.NotImplementedf("not implemented")

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

func (r *repo) CreateService(service *vocab.Service) (err error) {
	err = r.Open()
	if err != nil {
		return err
	}
	defer func() {
		if err = r.close(); err != nil {
			r.errFn("error closing the db: %+s", err)
		}
	}()
	it, err := save(*r, service)
	if err != nil {
		r.errFn("%s %s: %s", err, it.GetType(), it.GetLink())
	}
	return err
}

func getCollectionTypeFromIRI(i string) vocab.CollectionPath {
	col := vocab.CollectionPath(path.Base(i))
	if !(filters.FedBOXCollections.Contains(col) || vocab.ActivityPubCollections.Contains(col)) {
		b, _ := path.Split(i)
		col = vocab.CollectionPath(path.Base(b))
	}
	return getCollectionTable(col)
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
		fallthrough
	default:
		return "objects"
	}
}

func getCollectionTableFromFilter(f *filters.Filters) vocab.CollectionPath {
	return getCollectionTable(f.Collection)
}

// Load
func (r *repo) Load(i vocab.IRI) (vocab.Item, error) {
	if err := r.Open(); err != nil {
		return nil, err
	}
	defer r.Close()

	f, err := filters.FiltersFromIRI(i)
	if err != nil {
		return nil, err
	}

	ret, err := loadFromDb(r, f)
	if len(ret) == 1 && f.IsItemIRI() {
		return ret.First(), err
	}
	return ret, err
}

// Save
func (r *repo) Save(it vocab.Item) (vocab.Item, error) {
	if err := r.Open(); err != nil {
		return nil, err
	}
	defer r.Close()
	return save(*r, it)
}

// Create
func (r *repo) Create(col vocab.CollectionInterface) (vocab.CollectionInterface, error) {
	if col.IsObject() {
		_, err := r.Save(col)
		if err != nil {
			return col, err
		}
	}
	return col, nil
}

// RemoveFrom
func (r *repo) RemoveFrom(col vocab.IRI, it vocab.Item) error {
	if err := r.Open(); err != nil {
		return err
	}
	defer r.Close()
	query := "DELETE FROM collections where iri = ? AND object = ?;"

	if _, err := r.conn.Exec(query, col, it.GetLink()); err != nil {
		r.errFn("query error: %s\n%s\n%#v", err, query)
		return errors.Annotatef(err, "query error")
	}

	return nil
}

func (r *repo) addTo(col vocab.IRI, it vocab.Item) error {
	if r.conn == nil {
		return errors.Newf("nil sql connection")
	}
	query := "INSERT INTO collections (iri, object) VALUES (?, ?);"

	if _, err := r.conn.Exec(query, col, it.GetLink()); err != nil {
		r.errFn("query error: %s\n%s\n%#v", err, query)
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
	table := getCollectionTypeFromIRI(iri.String())

	query := fmt.Sprintf("UPDATE %s SET meta = ? WHERE iri = ?;", table)
	_, err := conn.Exec(query, m, iri)
	return err
}

func loadMetadataFromTable(conn *sql.DB, iri vocab.IRI) ([]byte, error) {
	table := getCollectionTypeFromIRI(iri.String())

	var meta []byte
	sel := fmt.Sprintf("SELECT meta FROM %s WHERE iri = ?;", table)
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

func loadFromObjects(r *repo, f *filters.Filters) (vocab.ItemCollection, error) {
	return loadFromOneTable(r, "objects", f)
}
func loadFromActors(r *repo, f *filters.Filters) (vocab.ItemCollection, error) {
	return loadFromOneTable(r, "actors", f)
}
func loadFromActivities(r *repo, f *filters.Filters) (vocab.ItemCollection, error) {
	return loadFromOneTable(r, "activities", f)
}

func loadFromThreeTables(r *repo, f *filters.Filters) (vocab.ItemCollection, error) {
	result := make(vocab.ItemCollection, 0)
	if obj, err := loadFromObjects(r, f); err == nil {
		result = append(result, obj...)
	}
	if actors, err := loadFromActors(r, f); err == nil {
		result = append(result, actors...)
	}
	if activities, err := loadFromActivities(r, f); err == nil {
		result = append(result, activities...)
	}
	return result, nil
}

func loadFromOneTable(r *repo, table vocab.CollectionPath, f *filters.Filters) (vocab.ItemCollection, error) {
	conn := r.conn
	// NOTE(marius): this doesn't seem to be working, our filter is never an IRI or Item
	if isSingleItem(f) && r.cache != nil {
		if cachedIt := r.cache.Get(f.GetLink()); cachedIt != nil {
			return vocab.ItemCollection{cachedIt}, nil
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
		return ret, nil
	}

	sel := fmt.Sprintf("SELECT iri, raw FROM %s WHERE %s ORDER BY published %s", table, strings.Join(clauses, " AND "), getLimit(f))
	rows, err := conn.Query(sel, values...)
	if err != nil {
		if err == sql.ErrNoRows {
			return vocab.ItemCollection{}, nil
		}
		return nil, errors.Annotatef(err, "unable to run select")
	}

	// Iterate through the result set
	for rows.Next() {
		var iri string
		var raw []byte
		err = rows.Scan(&iri, &raw)
		if err != nil {
			return ret, errors.Annotatef(err, "scan values error")
		}

		it, err := decodeItemFn(raw)
		if err != nil {
			return ret, errors.Annotatef(err, "unable to unmarshal raw item")
		}
		if vocab.IsObject(it) && r.cache != nil {
			r.cache.Set(it.GetLink(), it)
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
	return ret, err
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
					(*col)[i] = ob.First()
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
	children, _ := loadFromThreeTables(r, f)
	for _, rr := range *ret {
		if !vocab.ActivityTypes.Contains(rr.GetType()) {
			toRemove = append(toRemove, rr.GetID())
			continue
		}
		keep := false
		vocab.OnActivity(rr, func(a *vocab.Activity) error {
			for _, ob := range children {
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

func loadFromDb(r *repo, f *filters.Filters) (vocab.ItemCollection, error) {
	conn := r.conn
	table := getCollectionTableFromFilter(f)
	clauses, values := getWhereClauses(f)
	var total uint = 0

	// todo(marius): this needs to be split into three cases:
	//  1. IRI corresponds to a collection that is not one of the storage tables (ie, not activities, actors, objects):
	//    Then we look for correspondences in the collections table.
	//  2. The IRI corresponds to the activities, actors, objects tables:
	//    Then we load from the corresponding table using `iri LIKE IRI%` criteria
	//  3. IRI corresponds to an object: we load directly from the corresponding table.
	selCnt := fmt.Sprintf("SELECT COUNT(iri) FROM %s WHERE %s", table, strings.Join(clauses, " AND "))
	if err := conn.QueryRow(selCnt, values...).Scan(&total); err != nil && err != sql.ErrNoRows {
		return nil, errors.Annotatef(err, "unable to count all rows")
	}
	if total > 0 {
		return loadFromOneTable(r, table, f)
	}
	var (
		iriClause string
		iriValue  interface{}
		hasIRI    = false
	)
	valIdx := -1
	for _, c := range clauses {
		valIdx += strings.Count(c, "?")
		if strings.Contains(c, "iri") {
			iriClause = c
			iriValue = values[valIdx]
			hasIRI = true
		}
	}
	if !hasIRI {
		return nil, errors.NotFoundf("Not found")
	}
	colCntQ := fmt.Sprintf("SELECT COUNT(iri) FROM %s WHERE %s", "collections", iriClause)
	if err := conn.QueryRow(colCntQ, iriValue).Scan(&total); err != nil && err != sql.ErrNoRows {
		return nil, errors.Annotatef(err, "unable to count all rows")
	}
	if total == 0 && vocab.ActivityPubCollections.Contains(f.Collection) && !MandatoryCollections.Contains(f.Collection) {
		return nil, errors.NotFoundf("Unable to find collection %s", f.Collection)
	}
	sel := fmt.Sprintf("SELECT iri, object FROM %s WHERE %s %s", "collections", iriClause, getLimit(f))
	rows, err := conn.Query(sel, iriValue)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, errors.NotFoundf("Unable to load %s", f.Collection)
		}
		return nil, errors.Annotatef(err, "unable to run select")
	}
	fOb := *f
	fActors := *f
	fActivities := *f

	fOb.IRI = ""
	fOb.Collection = "objects"
	fOb.ItemKey = make(filters.CompStrs, 0)
	fActors.IRI = ""
	fActors.Collection = "actors"
	fActors.ItemKey = make(filters.CompStrs, 0)
	fActivities.IRI = ""
	fActivities.Collection = "activities"
	fActivities.ItemKey = make(filters.CompStrs, 0)
	// Iterate through the result set
	for rows.Next() {
		var object string
		var iri string

		err = rows.Scan(&iri, &object)
		if err != nil {
			return vocab.ItemCollection{}, errors.Annotatef(err, "scan values error")
		}
		col := getCollectionTypeFromIRI(iri)
		if col == "objects" {
			fOb.ItemKey = append(fOb.ItemKey, filters.StringEquals(object))
		} else if col == "actors" {
			fActors.ItemKey = append(fActors.ItemKey, filters.StringEquals(object))
		} else if col == "activities" {
			fActivities.ItemKey = append(fActivities.ItemKey, filters.StringEquals(object))
		} else {
			switch table {
			case "activities":
				fActivities.ItemKey = append(fActivities.ItemKey, filters.StringEquals(object))
			case "actors":
				fActors.ItemKey = append(fActors.ItemKey, filters.StringEquals(object))
			case "objects":
				fallthrough
			default:
				fOb.ItemKey = append(fOb.ItemKey, filters.StringEquals(object))
			}
		}
	}
	ret := make(vocab.ItemCollection, 0)
	if len(fActivities.ItemKey) > 0 {
		retAct, err := loadFromActivities(r, &fActivities)
		if err != nil {
			return ret, err
		}
		ret = append(ret, retAct...)
	}
	if len(fActors.ItemKey) > 0 {
		retAct, err := loadFromActors(r, &fActors)
		if err != nil {
			return ret, err
		}
		ret = append(ret, retAct...)
	}
	if len(fOb.ItemKey) > 0 {
		retOb, err := loadFromObjects(r, &fOb)
		if err != nil {
			return ret, err
		}
		ret = append(ret, retOb...)
	}
	return ret, nil
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
		l.cache.Remove(it.GetLink())
	}
	return nil
}

func save(l repo, it vocab.Item) (vocab.Item, error) {
	iri := it.GetLink()

	if err := flattenCollections(it); err != nil {
		return it, errors.Annotatef(err, "could not create object's collections")
	}
	raw, err := encodeItemFn(it)
	if err != nil {
		l.errFn("query error: %s", err)
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

	if _, err = l.conn.Exec(query, params...); err != nil {
		l.errFn("query error: %s\n%s", err, query)
		return it, errors.Annotatef(err, "query error")
	}
	col, key := path.Split(iri.String())
	if len(key) > 0 && vocab.ValidCollection(vocab.CollectionPath(path.Base(col))) {
		// Add private items to the collections table
		if colIRI, k := vocab.Split(vocab.IRI(col)); k == "" {
			if err := l.addTo(colIRI, it); err != nil {
				return it, err
			}
		}
	}

	if l.cache != nil {
		l.cache.Set(it.GetLink(), it)
	}
	return it, nil
}

// flattenCollections
func flattenCollections(it vocab.Item) error {
	if vocab.IsNil(it) || !it.IsObject() {
		return nil
	}
	if vocab.ActorTypes.Contains(it.GetType()) {
		vocab.OnActor(it, func(p *vocab.Actor) error {
			p.Inbox = vocab.FlattenToIRI(p.Inbox)
			p.Outbox = vocab.FlattenToIRI(p.Outbox)
			p.Followers = vocab.FlattenToIRI(p.Followers)
			p.Following = vocab.FlattenToIRI(p.Following)
			p.Liked = vocab.FlattenToIRI(p.Liked)
			return nil
		})
	}
	return vocab.OnObject(it, func(o *vocab.Object) error {
		o.Replies = vocab.FlattenToIRI(o.Replies)
		o.Likes = vocab.FlattenToIRI(o.Likes)
		o.Shares = vocab.FlattenToIRI(o.Shares)
		return nil
	})
}

// SaveKey saves a private key for an actor found by its IRI
func (r *repo) SaveKey(iri vocab.IRI, key crypto.PrivateKey) (vocab.Item, error) {
	f, _ := filters.FiltersFromIRI(iri)
	result, err := loadFromActors(r, f)
	if err != nil {
		return nil, err
	}
	if result.Count() == 0 {
		return nil, errors.NotFoundf("not found %s", iri)
	}

	ob := result.First()
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
