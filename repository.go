package sqlite

import (
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	vocab "github.com/go-ap/activitypub"
	"github.com/go-ap/cache"
	"github.com/go-ap/errors"
	"github.com/go-ap/filters"
	"github.com/go-ap/jsonld"
)

var encodeItemFn = vocab.MarshalJSON
var decodeItemFn = vocab.UnmarshalJSON

type loggerFn func(string, ...any)

var defaultLogFn = func(string, ...any) {}

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
	rr := repo{
		path:  p,
		logFn: defaultLogFn,
		errFn: defaultLogFn,
		cache: cache.New(c.CacheEnable),
	}

	if c.LogFn != nil {
		rr.logFn = c.LogFn
	}
	if c.ErrFn != nil {
		rr.errFn = c.ErrFn
	}
	return &rr, nil
}

type repo struct {
	conn  *sql.DB
	path  string
	cache cache.CanStore
	logFn loggerFn
	errFn loggerFn
}

var errNotOpen = errors.Newf("sqlite db is not open")

// Open opens the sqlite database
func (r *repo) Open() (err error) {
	if r == nil {
		return errors.Newf("Unable to open uninitialized db")
	}
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

var allCollections = append(filters.FedBOXCollections, vocab.ActivityPubCollections...)

func getCollectionTypeFromIRI(i vocab.IRI) vocab.CollectionPath {
	_, col := allCollections.Split(i)
	if !(allCollections.Contains(col)) {
		b, _ := path.Split(i.String())
		col = vocab.CollectionPath(path.Base(b))
	}

	if table := getCollectionTable(col); table != "" {
		return table
	}
	return ""
}

func getCollectionTypeFromItem(it vocab.Item) vocab.CollectionPath {
	switch {
	case vocab.ActorTypes.Contains(it.GetType()):
		return "actors"
	case vocab.ActivityTypes.Contains(it.GetType()):
		return "activities"
	case vocab.IntransitiveActivityTypes.Contains(it.GetType()):
		return "activities"
	case append(collectionTypes, orderedCollectionTypes...).Contains(it.GetType()):
		return "collections"
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

// Load
func (r *repo) Load(i vocab.IRI, ff ...filters.Check) (vocab.Item, error) {
	if r == nil || r.conn == nil {
		return nil, errNotOpen
	}

	if !isCollectionIRI(i) {
		ff = append(filters.Checks{filters.SameID(i)}, ff...)
	}
	it, err := load(r, i, ff...)
	if err != nil {
		return nil, err
	}
	maybeIt := filters.Checks(ff).Run(it)
	if col, ok := maybeIt.(vocab.ItemCollection); ok && col.Count() == 1 {
		return col.First(), nil
	}
	return maybeIt, nil
}

var (
	nilItemErr    = errors.Errorf("nil item")
	nilItemIRIErr = errors.Errorf("nil IRI for item")
)

// Save
func (r *repo) Save(it vocab.Item) (vocab.Item, error) {
	if r == nil || r.conn == nil {
		return nil, errNotOpen
	}
	if vocab.IsNil(it) {
		return nil, errors.Newf("Unable to save nil element")
	}

	return save(r, it)
}

var emptyCol = []byte{'[', ']'}

// Create
func (r *repo) Create(col vocab.CollectionInterface) (vocab.CollectionInterface, error) {
	if r == nil || r.conn == nil {
		return nil, errNotOpen
	}

	if vocab.IsNil(col) {
		return nil, nilItemErr
	}
	if col.GetLink() == "" {
		return col, nilItemIRIErr
	}
	return r.createCollection(col)
}

func (r *repo) createCollection(col vocab.CollectionInterface) (vocab.CollectionInterface, error) {
	raw, err := vocab.MarshalJSON(col)
	if err != nil {
		r.errFn("unable to marshal collection %s: %s", col.GetLink(), err)
		return col, errors.Annotatef(err, "unable to save Collection")
	}
	ins := "INSERT OR REPLACE INTO collections (raw, iri, items) VALUES (?, ?, ?);"
	if _, err = r.conn.Exec(ins, raw, col.GetLink(), emptyCol); err != nil {
		r.errFn("query error when creating %s: %s\n%s", col.GetLink(), err, ins)
		return col, errors.Annotatef(err, "unable to save Collection")
	}

	return col, nil
}

func (r *repo) removeFrom(col vocab.IRI, items ...vocab.Item) error {
	if r.conn == nil {
		return errors.Newf("nil sql connection")
	}
	colSel := "SELECT iri, raw, items from collections WHERE iri = ?;"
	rows, err := r.conn.Query(colSel, col.GetLink())
	if err != nil {
		r.errFn("query error: %s\n%s\n%#v", err, colSel)
		return errors.NotFoundf("Unable to load %s", col.GetLink())
	}
	defer rows.Close()

	var c vocab.Item
	var iris vocab.ItemCollection
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
		var itemCol vocab.Item
		if itemCol, err = vocab.UnmarshalJSON(itemsRaw); err != nil {
			return errors.Annotatef(err, "unable to unmarshal Collection items")
		}
		var ok bool
		if iris, ok = itemCol.(vocab.ItemCollection); !ok {
			return errors.Annotatef(err, "unable to load Collection items")
		}
		iris.Remove(items...)
	}

	if vocab.IsNil(c) {
		return errors.NotFoundf("not found Collection %s", col.GetLink())
	}

	raw, err := vocab.MarshalJSON(c)
	if err != nil {
		return errors.Annotatef(err, "unable to marshal Collection")
	}
	rawItems, err := vocab.MarshalJSON(iris.IRIs())
	if err != nil {
		return errors.Annotatef(err, "unable to marshal Collection rawItems")
	}

	query := "UPDATE collections SET raw = ?, items = ? WHERE iri = ?;"
	_, err = r.conn.Exec(query, raw, rawItems, c.GetLink())
	if err != nil {
		r.errFn("query error: %s\n%s\n%s", err, query, c.GetLink())
		return errors.Annotatef(err, "query error")
	}

	return nil
}

func (r *repo) exec(query string, args ...any) (sql.Result, error) {
	if r.conn == nil {
		return nil, errors.Newf("nil sql connection")
	}
	return r.conn.Exec(query, args...)
}

func (r *repo) queryRow(query string, args ...any) *sql.Row {
	if r.conn == nil {
		return &sql.Row{}
	}
	return r.conn.QueryRow(query, args...)
}

// RemoveFrom
func (r *repo) RemoveFrom(col vocab.IRI, items ...vocab.Item) error {
	if r == nil || r.conn == nil {
		return errNotOpen
	}

	return r.removeFrom(col, items...)
}

func (r *repo) addTo(col vocab.IRI, items ...vocab.Item) error {
	var c vocab.Item

	var iri string
	var raw []byte
	var irisRaw []byte

	iris := make(vocab.IRIs, 0)
	colSel := "SELECT iri, raw, items from collections WHERE iri = ?;"
	err := r.queryRow(colSel, col.GetLink()).Scan(&iri, &raw, &irisRaw)
	if err != nil {
		r.logFn("unable to load collection object for %s: %s", col, err)
		if errors.Is(err, sql.ErrNoRows) && (isHiddenCollectionIRI(col)) {
			// NOTE(marius): this creates blocked/ignored collections if they don't exist
			if c, err = r.createCollection(createCollection(col.GetLink(), nil)); err != nil {
				r.errFn("query error: %s\n%s %#v", err, colSel, vocab.IRIs{col})
			}
		}
	} else {
		c, err = vocab.UnmarshalJSON(raw)
		if err != nil {
			return errors.Annotatef(err, "unable to unmarshal Collection")
		}
		if err = jsonld.Unmarshal(irisRaw, &iris); err != nil {
			return errors.Annotatef(err, "unable to unmarshal Collection items")
		}
	}
	if c == nil {
		return errors.NotFoundf("not found Collection %s", col.GetLink())
	}

	// NOTE(marius): load previous items' IRIs
	err = vocab.OnOrderedCollection(c, func(col *vocab.OrderedCollection) error {
		return iris.Append(col.Collection()...)
	})
	if err != nil {
		return errors.Annotatef(err, "unable to update Collection")
	}

	for _, it := range items {
		if vocab.IsIRI(it) {
			// NOTE(marius): append received items to the list
			if _, err = loadFromThreeTables(r, it.GetLink()); err != nil {
				return errors.NewNotFound(err, "invalid item to add to collection")
			}
		}
		_ = iris.Append(it)
	}

	rawItems, err := iris.MarshalJSON()
	if err != nil {
		return errors.Annotatef(err, "unable to marshal Collection items")
	}

	if orderedCollectionTypes.Contains(c.GetType()) {
		err = vocab.OnOrderedCollection(c, func(col *vocab.OrderedCollection) error {
			col.TotalItems = iris.Count()
			col.OrderedItems = nil
			return nil
		})
	} else if collectionTypes.Contains(c.GetType()) {
		err = vocab.OnCollection(c, func(col *vocab.Collection) error {
			col.TotalItems = iris.Count()
			col.Items = nil
			return nil
		})
	}

	raw, err = vocab.MarshalJSON(c)
	if err != nil {
		return errors.Annotatef(err, "unable to marshal Collection")
	}
	query := `INSERT INTO collections (raw, iri, items) VALUES (?, ?, ?) ON CONFLICT(iri) DO UPDATE SET raw = ?, items = ?;`
	_, err = r.exec(query, raw, col.GetLink(), rawItems, raw, rawItems)
	if err != nil {
		r.errFn("query error: %s\n%s %#v", err, query, vocab.IRIs{c.GetLink()})
		return errors.Annotatef(err, "query error")
	}

	return nil
}

// AddTo
func (r *repo) AddTo(col vocab.IRI, items ...vocab.Item) error {
	if r == nil || r.conn == nil {
		return errNotOpen
	}

	return r.addTo(col, items...)
}

// Delete
func (r *repo) Delete(it vocab.Item) error {
	if r == nil || r.conn == nil {
		return errNotOpen
	}

	if vocab.IsNil(it) {
		return nil
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

const dbFile = "storage.sqlite"

func getFullPath(c Config) (string, error) {
	if !filepath.IsAbs(c.Path) {
		c.Path, _ = filepath.Abs(c.Path)
	}
	if err := mkDirIfNotExists(c.Path); err != nil {
		return "memory", err
	}
	return path.Join(c.Path, dbFile), nil
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

func isSingleItem(f ...filters.Check) bool {
	return len(f) == 0 || len(filters.IDChecks(f...)) == 1
}

func stripFiltersFromIRI(iri vocab.IRI) vocab.IRI {
	u, _ := iri.URL()
	u.RawQuery = ""
	return vocab.IRI(u.String())
}

func loadFromThreeTables(r *repo, iri vocab.IRI, f ...filters.Check) (vocab.CollectionInterface, error) {
	conn := r.conn
	// NOTE(marius): this doesn't seem to be working, our filter is never an IRI or Item
	if isSingleItem(f...) {
		f = filters.Checks{filters.SameID(iri)}
		if r.cache != nil {
			if cachedIt := r.cache.Load(iri); cachedIt != nil {
				return &vocab.ItemCollection{cachedIt}, nil
			}
		}
	}

	selects := make([]string, 0)
	params := make([]any, 0)
	for _, table := range []string{"actors", "objects", "activities"} {
		clauses, values := filters.GetWhereClauses(f...)
		if isStorageCollectionIRI(iri) {
			clauses = append(clauses, `iri like ?`)
			values = append(values, stripFiltersFromIRI(iri)+"%")
		}
		if len(clauses) == 0 {
			clauses = []string{"true"}
		}
		selects = append(selects, fmt.Sprintf("SELECT iri, raw, published FROM %s WHERE %s", table, strings.Join(clauses, " AND ")))
		params = append(params, values...)
	}

	ret := make(vocab.ItemCollection, 0)

	limit := filters.GetLimit(f...)
	if limit < 0 {
		limit = filters.MaxItems
	}
	sel := fmt.Sprintf(`SELECT iri, raw FROM (%s) ORDER BY published DESC LIMIT %d;`, strings.Join(selects, " UNION "), limit)
	rows, err := conn.Query(sel, params...)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, errors.Annotatef(err, "unable to run select")
	}
	defer rows.Close()

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
	if len(ret) == 0 {
		return nil, errors.NotFoundf("not found")
	}

	ret = loadActorFirstLevelIRIProperties(r, ret, f...)
	ret = loadObjectFirstLevelIRIProperties(r, ret, f...)
	ret = runActivityFilters(r, ret, f...)
	ret = runObjectFilters(ret, f...)
	return &ret, err
}

var collectionPaths = append(filters.FedBOXCollections, append(vocab.OfActor, vocab.OfObject...)...)

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

var collectionTables = vocab.CollectionPaths{filters.ActivitiesType, filters.ActorsType, filters.ObjectsType}

func isStorageCollectionIRI(iri vocab.IRI) bool {
	lst := vocab.CollectionPath(filepath.Base(iriPath(iri)))
	return collectionTables.Contains(lst)
}

func isCollectionIRI(iri vocab.IRI) bool {
	lst := vocab.CollectionPath(filepath.Base(iriPath(iri)))
	return collectionPaths.Contains(lst)
}

func loadFromOneTable(r *repo, iri vocab.IRI, table vocab.CollectionPath, f ...filters.Check) (vocab.CollectionInterface, error) {
	conn := r.conn
	if isSingleItem(f...) && r.cache != nil {
		if cachedIt := r.cache.Load(iri); cachedIt != nil {
			return &vocab.ItemCollection{cachedIt}, nil
		}
	}
	if table == "" {
		return nil, errors.Newf("invalid table")
	}

	ret := make(vocab.ItemCollection, 0)
	clauses, values := filters.GetWhereClauses(f...)
	if isStorageCollectionIRI(iri) {
		clauses = append(clauses, `iri like ?`)
		values = append(values, stripFiltersFromIRI(iri)+"%")
	}
	if len(clauses) == 0 {
		clauses = []string{"true"}
	}

	limit := filters.GetLimit(f...)
	if limit < 0 {
		limit = filters.MaxItems
	}

	sel := fmt.Sprintf("SELECT iri, raw FROM %s WHERE %s ORDER BY updated DESC LIMIT %d;", table, strings.Join(clauses, " AND "), limit)
	rows, err := conn.Query(sel, values...)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, errors.Annotatef(err, "unable to run select")
	}
	defer rows.Close()

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
		if it = filters.Checks(f).Filter(it); vocab.IsNil(it) {
			continue
		}
		if vocab.IsObject(it) && r.cache != nil {
			r.cache.Store(it.GetLink(), it)
		}
		ret = append(ret, it)
	}

	if table == "actors" {
		ret = loadActorFirstLevelIRIProperties(r, ret, f...)
	}
	if table == "objects" {
		ret = loadObjectFirstLevelIRIProperties(r, ret, f...)
	}
	if table == "activities" {
		ret = loadActivityFirstLevelIRIProperties(r, ret, f...)
		ret = runActivityFilters(r, ret, f...)
	}
	ret = runObjectFilters(ret, f...)
	return &ret, nil
}

func runObjectFilters(ret vocab.ItemCollection, f ...filters.Check) vocab.ItemCollection {
	maybeCol := filters.Checks(f).Run(vocab.Item(ret))
	ret, _ = maybeCol.(vocab.ItemCollection)
	return ret
}

func keepObject(f ...filters.Check) func(act *vocab.Activity, ob vocab.Item) bool {
	return func(act *vocab.Activity, ob vocab.Item) bool {
		keep := false
		if act.Object.GetLink().Equals(ob.GetLink(), false) {
			act.Object = ob
			keep = !vocab.IsNil(filters.Checks(f).Run(act.Object))
		}
		return keep
	}
}

func keepActor(f ...filters.Check) func(act *vocab.Activity, ob vocab.Item) bool {
	return func(act *vocab.Activity, ob vocab.Item) bool {
		var keep bool
		if act.Actor.GetLink().Equals(ob.GetLink(), false) {
			act.Actor = ob
			keep = !vocab.IsNil(filters.Checks(f).Run(act.Actor))
		}
		return keep
	}
}

func keepTarget(f ...filters.Check) func(act *vocab.Activity, ob vocab.Item) bool {
	return func(act *vocab.Activity, ob vocab.Item) bool {
		var keep bool
		if act.Target.GetLink().Equals(ob.GetLink(), false) {
			act.Target = ob
			keep = !vocab.IsNil(filters.Checks(f).Run(act.Target))
		}
		return keep
	}
}

func loadTagsForObject(r *repo, _ ...filters.Check) func(o *vocab.Object) error {
	return func(o *vocab.Object) error {
		if len(o.Tag) == 0 {
			return nil
		}
		return vocab.OnItemCollection(o.Tag, func(col *vocab.ItemCollection) error {
			for i, t := range *col {
				if vocab.IsNil(t) || !vocab.IsIRI(t) {
					return nil
				}
				if ob, err := loadFromOneTable(r, t.GetLink(), "objects"); err == nil {
					(*col)[i] = ob.Collection().First()
				}
			}
			return nil
		})
	}
}

func loadTargetForActivity(r *repo, a *vocab.Activity) error {
	if vocab.IsNil(a.Target) {
		return nil
	}

	if ob, err := loadFromOneTable(r, a.Target.GetLink(), "objects"); err == nil {
		if c := ob.Collection(); c.Count() > 1 {
			a.Target = ob.Collection()
		} else if c.Count() == 1 {
			a.Target = ob.Collection().First()
		}
	}
	return nil
}

func loadObjectForActivity(r *repo, a *vocab.Activity) error {
	if vocab.IsNil(a.Object) {
		return nil
	}

	if ob, err := loadFromOneTable(r, a.Object.GetLink(), "objects"); err == nil {
		if c := ob.Collection(); c.Count() > 1 {
			a.Object = ob.Collection()
		} else if c.Count() == 1 {
			a.Object = ob.Collection().First()
		}
	}
	return nil
}

func loadActorForActivity(r *repo, a *vocab.Activity) error {
	if vocab.IsNil(a.Actor) {
		return nil
	}

	if ob, err := loadFromOneTable(r, a.Actor.GetLink(), "actors"); err == nil {
		if c := ob.Collection(); c.Count() > 1 {
			a.Actor = ob.Collection()
		} else if c.Count() == 1 {
			a.Actor = ob.Collection().First()
		}
	}
	return nil
}

func loadPropertiesForActivity(r *repo, ff ...filters.Check) func(o *vocab.Activity) error {
	return func(a *vocab.Activity) error {
		if af := filters.ActorChecks(ff...); len(af) > 0 {
			if err := loadActorForActivity(r, a); err != nil {
				return err
			}
		}
		if of := filters.ObjectChecks(ff...); len(of) > 0 {
			if err := loadObjectForActivity(r, a); err != nil {
				return err
			}
		}
		if tf := filters.TargetChecks(ff...); len(tf) > 0 {
			if err := loadTargetForActivity(r, a); err != nil {
				return err
			}
		}
		return nil
	}
}

func loadActivityFirstLevelIRIProperties(r *repo, ret vocab.ItemCollection, f ...filters.Check) vocab.ItemCollection {
	for _, it := range ret {
		_ = vocab.OnActivity(it, loadPropertiesForActivity(r, f...))
	}
	return loadObjectFirstLevelIRIProperties(r, ret, f...)
}

func loadActorFirstLevelIRIProperties(r *repo, ret vocab.ItemCollection, f ...filters.Check) vocab.ItemCollection {
	return loadObjectFirstLevelIRIProperties(r, ret, f...)
}

func loadObjectFirstLevelIRIProperties(r *repo, ret vocab.ItemCollection, f ...filters.Check) vocab.ItemCollection {
	for _, it := range ret {
		_ = vocab.OnObject(it, loadTagsForObject(r, f...))
	}
	return ret
}

func runActivityFilters(r *repo, ret vocab.ItemCollection, f ...filters.Check) vocab.ItemCollection {
	// If our filter contains values for filtering the activity's object or actor, we do that here:
	//  for the case where the corresponding values are not set, this doesn't do anything
	toRemove := make(vocab.IRIs, 0)

	if of := filters.ObjectChecks(f...); len(of) > 0 {
		toRemove = append(toRemove, childFilter(r, &ret, keepObject(of...), of...)...)
	}
	if af := filters.ActorChecks(f...); len(af) > 0 {
		toRemove = append(toRemove, childFilter(r, &ret, keepActor(af...), af...)...)
	}
	if tf := filters.TargetChecks(f...); len(tf) > 0 {
		toRemove = append(toRemove, childFilter(r, &ret, keepTarget(tf...), tf...)...)
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

type keepFn func(act *vocab.Activity, ob vocab.Item) bool

func childFilter(r *repo, ret *vocab.ItemCollection, keepFn keepFn, ff ...filters.Check) vocab.IRIs {
	f := filters.Checks(ff).Run(vocab.Item(*ret))
	if f == nil {
		return nil
	}
	toRemove := make(vocab.IRIs, 0)
	children, err := loadFromThreeTables(r, "", ff...)
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

func load(r *repo, iri vocab.IRI, f ...filters.Check) (vocab.CollectionInterface, error) {
	var items vocab.CollectionInterface
	var err error

	if !isCollectionIRI(iri) {
		items, err = loadFromThreeTables(r, iri, f...)
		if err != nil {
			return items, err
		}

		if items.Count() == 0 {
			return nil, errors.NotFoundf("Not found")
		}
		return items, nil
	}
	par, err := loadFromCollectionTable(r, colIRI(iri), f...)
	if err != nil {
		return nil, err
	}
	if vocab.IsNil(par) {
		return nil, errors.NotFoundf("Unable to find collection %s", iri)
	}
	err = vocab.OnObject(par, func(ob *vocab.Object) error {
		ob.ID = iri
		return nil
	})
	if orderedCollectionTypes.Contains(par.GetType()) {
		_ = vocab.OnOrderedCollection(par, postProcessOrderedCollection(par.Collection()))
	} else if collectionTypes.Contains(par.GetType()) {
		_ = vocab.OnCollection(par, postProcessCollection(par.Collection()))
	}
	return par, err
}

func postProcessCollection(items vocab.ItemCollection) vocab.WithCollectionFn {
	return func(col *vocab.Collection) error {
		if len(items) > 0 {
			col.Items = items
		}
		return nil
	}
}

func postProcessOrderedCollection(items vocab.ItemCollection) vocab.WithOrderedCollectionFn {
	return func(col *vocab.OrderedCollection) error {
		if len(items) > 0 {
			col.OrderedItems = items
		}
		return nil
	}
}

func isHiddenCollectionIRI(i vocab.IRI) bool {
	lst := vocab.CollectionPath(filepath.Base(i.String()))
	return filters.HiddenCollections.Contains(lst)
}

func loadFromCollectionTable(r *repo, iri vocab.IRI, f ...filters.Check) (vocab.CollectionInterface, error) {
	conn := r.conn

	selects := []string{"c.iri = ? "}
	params := []any{iri}

	table := getCollectionTypeFromIRI(iri)
	var selWithItems string
	if isStorageCollectionIRI(iri) {
		limit := filters.GetLimit(f...)
		if limit < 0 {
			limit = filters.MaxItems
		}
		sel := `
select c.iri,
	json_patch(
		json(c.raw),
		json_object('orderedItems', json_group_array(json(x.raw)))
	) raw 
from collections c 
	left join %s x where %s group by c.iri order by x.published desc LIMIT %d;`
		selWithItems = fmt.Sprintf(sel, table, strings.Join(selects, " AND "), limit)
	} else {
		limit := filters.GetLimit(f...)
		if limit < 0 {
			limit = filters.MaxItems
		}
		where := strings.Join(selects, " AND ")
		selWithItems = fmt.Sprintf(`
	select c.iri iri,
		json_patch(
			json(c.raw),
			json_object('orderedItems', json_group_array(json(coalesce(x.raw, y.raw, o.raw))))
		) raw
	from collections c, json_each(json_insert(c.items, '$[0]', null))
		left join activities x on value = x.iri 
		left join actors y on value = y.iri 
		left join objects o on value = o.iri 
where %s order by coalesce(x.published, y.published, o.published) desc LIMIT %d`, where, limit)
	}

	var cIri sql.NullString
	var raw []byte

	if err := conn.QueryRow(selWithItems, params...).Scan(&cIri, &raw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errors.NotFoundf("failed to find items in collection %s", iri)
		}
		return nil, errors.Annotatef(err, "failed to run select for %s", iri)
	}

	if len(raw) == 0 {
		return nil, errors.NotFoundf("Unable to find items in collection %s", iri)
	}

	var err error
	res := vocab.OrderedCollection{}
	if err = decodeFn(raw, &res); err != nil {
		return nil, errors.Annotatef(err, "Collection unmarshal error")
	}

	items := res.Collection()
	if table == "actors" {
		items = loadActorFirstLevelIRIProperties(r, items, f...)
	}
	if table == "objects" {
		items = loadObjectFirstLevelIRIProperties(r, items, f...)
	}
	if table == "activities" {
		items = loadActivityFirstLevelIRIProperties(r, items, f...)
		items = runActivityFilters(r, items, f...)
	}
	items = runObjectFilters(items, f...)

	if isStorageCollectionIRI(iri) {
		if orderedCollectionTypes.Contains(res.GetType()) {
			err = vocab.OnOrderedCollection(res, postProcessOrderedCollection(items))
		} else if collectionTypes.Contains(res.GetType()) {
			err = vocab.OnCollection(res, postProcessCollection(items))
		}
	}
	return &res, err
}

func delete(l repo, it vocab.Item) error {
	iri := it.GetLink()
	cleanupTables := []string{"meta"}

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
	cleanupTables = append(cleanupTables, table)

	removeFn := func(table string, iri vocab.IRI) error {
		query := fmt.Sprintf("DELETE FROM %s where iri = $1;", table)
		if _, err := l.conn.Exec(query, iri); err != nil {
			l.errFn("query error: %s\n%s", err, query)
			return errors.Annotatef(err, "query error")
		}
		return nil
	}

	for _, tbl := range cleanupTables {
		if err := removeFn(tbl, it.GetLink()); err != nil {
			return err
		}
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

	columns := []string{"raw, iri"}
	tokens := []string{"?, ?"}
	params := []any{raw, iri}

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

	query := fmt.Sprintf(`INSERT OR REPLACE INTO %s (%s) VALUES (%s);`, table, strings.Join(columns, ", "), strings.Join(tokens, ", "))

	if _, err = r.conn.Exec(query, params...); err != nil {
		return it, errors.Annotatef(err, "query error")
	}
	col, _ := path.Split(iri.String())
	if isCollectionIRI(vocab.IRI(col)) {
		// Add private items to the collections table
		if colIRI, k := vocab.Split(vocab.IRI(col)); k == "" {
			if err := r.addTo(colIRI, it); err != nil {
				r.logFn("warning adding item: %s", colIRI, err)
			}
		}
	}

	if r.cache != nil {
		r.cache.Store(it.GetLink(), it)
	}
	return it, nil
}

func createCollection(colIRI vocab.IRI, owner vocab.Item) vocab.CollectionInterface {
	col := vocab.OrderedCollection{
		ID:        colIRI,
		Type:      vocab.OrderedCollectionType,
		Published: time.Now().UTC(),
		CC:        vocab.ItemCollection{vocab.PublicNS},
	}
	if !vocab.IsNil(owner) {
		col.AttributedTo = owner.GetLink()
		_ = vocab.OnObject(owner, func(ob *vocab.Object) error {
			if !ob.Published.IsZero() {
				col.Published = ob.Published
			}
			return nil
		})
	}
	return &col
}

func createCollectionInTable(r *repo, it vocab.Item, owner vocab.Item) (vocab.Item, error) {
	if vocab.IsNil(it) {
		return nil, nil
	}

	colObject, _ := loadFromCollectionTable(r, colIRI(it.GetLink()))
	if colObject == nil {
		var err error
		c, ok := it.(vocab.CollectionInterface)
		if !ok {
			c = createCollection(it.GetLink(), owner)
		}
		it, err = r.createCollection(c)
		if err != nil {
			return nil, errors.Annotatef(err, "saving collection object is not done")
		}
	}

	return it.GetLink(), nil
}

// createCollections
func createCollections(r *repo, it vocab.Item) error {
	if vocab.IsNil(it) || !it.IsObject() {
		return nil
	}
	if vocab.ActorTypes.Contains(it.GetType()) {
		_ = vocab.OnActor(it, func(p *vocab.Actor) error {
			p.Inbox, _ = createCollectionInTable(r, p.Inbox, p)
			p.Outbox, _ = createCollectionInTable(r, p.Outbox, p)
			p.Followers, _ = createCollectionInTable(r, p.Followers, p)
			p.Following, _ = createCollectionInTable(r, p.Following, p)
			p.Liked, _ = createCollectionInTable(r, p.Liked, p)
			return nil
		})
	}
	return vocab.OnObject(it, func(o *vocab.Object) error {
		o.Replies, _ = createCollectionInTable(r, o.Replies, o)
		o.Likes, _ = createCollectionInTable(r, o.Likes, o)
		o.Shares, _ = createCollectionInTable(r, o.Shares, o)
		return nil
	})
}
