package sqlite

import (
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	vocab "github.com/go-ap/activitypub"
	"github.com/go-ap/cache"
	"github.com/go-ap/errors"
	"github.com/go-ap/filters"
	"github.com/go-ap/jsonld"
	"github.com/leporo/sqlf"
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
	ro    *sql.DB
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
	// NOTE(marius): we split the connection into:
	if r.conn == nil {
		// a "write only" connection - allowing only one concurrent query execution
		if r.conn, err = sqlOpen(r.path); err != nil {
			return err
		}
		r.conn.SetMaxOpenConns(1)

		// and a "read only" connection - which allows multiple connections concurrently
		if r.ro, err = sqlOpen(r.path); err != nil {
			return err
		}
		r.ro.SetMaxOpenConns(max(2, runtime.NumCPU()))
	}
	return err
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
	typ := it.GetType()
	if typ == nil {
		return "objects"
	}
	switch {
	case vocab.ActorTypes.Match(typ):
		return "actors"
	case vocab.ActivityTypes.Match(typ):
		return "activities"
	case vocab.IntransitiveActivityTypes.Match(typ):
		return "activities"
	case append(collectionTypes, orderedCollectionTypes...).Match(typ):
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

func getCollectionTable(colName vocab.CollectionPath) vocab.CollectionPath {
	switch colName {
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
	if r == nil || r.ro == nil {
		return nil, errNotOpen
	}

	if !isCollectionIRI(i) {
		ff = append(filters.Checks{filters.SameID(i)}, ff...)
		if u, _ := i.URL(); u != nil && u.Path == "" {
			// NOTE(marius): I
			u.Path = "/"
			ff = append(ff, filters.SameID(vocab.IRI(u.String())))
		}
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

var errNilItem = errors.Errorf("nil item")

// Save
func (r *repo) Save(it vocab.Item) (vocab.Item, error) {
	if r == nil || r.conn == nil {
		return nil, errNotOpen
	}
	if vocab.IsNil(it) {
		return nil, errNilItem
	}

	tx, err := r.conn.Begin()
	if err != nil {
		r.errFn("%s", errors.Annotatef(err, "transaction start error"))
	}
	defer func() {
		if err := tx.Commit(); err != nil {
			r.errFn("%s", errors.Annotatef(err, "transaction commit error"))
		}
	}()
	return r.save(tx, it)
}

var emptyCol = []byte{'[', ']'}

// Create
// Deprecated
func (r *repo) Create(col vocab.CollectionInterface) (vocab.CollectionInterface, error) {
	it, err := r.Save(col)
	col, _ = it.(vocab.CollectionInterface)
	return col, err
}

func (r *repo) removeFrom(col vocab.IRI, items ...vocab.Item) error {
	if r.ro == nil || r.conn == nil {
		return errNotOpen
	}
	colSel := "SELECT iri, raw, items from collections WHERE iri = ?;"
	rows, err := r.ro.Query(colSel, col.GetLink())
	if err != nil {
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
	_, err = r.conn.Exec(query, string(raw), string(rawItems), c.GetLink())
	if err != nil {
		r.errFn("query error: %s\n%s\n%s", err, stringClean(query), c.GetLink())
		return errors.Annotatef(err, "query error")
	}

	return nil
}

// RemoveFrom
func (r *repo) RemoveFrom(col vocab.IRI, items ...vocab.Item) error {
	if r == nil || r.conn == nil {
		return errNotOpen
	}

	return r.removeFrom(col, items...)
}

func (r *repo) addTo(tx *sql.Tx, col vocab.IRI, items ...vocab.Item) error {
	if r == nil {
		return errNotOpen
	}
	var c vocab.Item

	var iri string
	var raw []byte
	var irisRaw []byte
	iris := make(vocab.IRIs, 0)
	colSel := "SELECT iri, raw, items from collections WHERE iri = ?;"
	row := tx.QueryRow(colSel, col)
	if row != nil {
		if err := row.Scan(&iri, &raw, &irisRaw); err != nil {
			r.logFn("unable to load collection object for %s: %s", col, err)
			if errors.Is(err, sql.ErrNoRows) && (isHiddenCollectionIRI(col)) {
				// NOTE(marius): this creates blocked/ignored collections if they don't exist
				if c, err = r.save(tx, createCollection(col.GetLink(), nil)); err != nil {
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
		err := vocab.OnOrderedCollection(c, func(col *vocab.OrderedCollection) error {
			return iris.Append(col.Collection()...)
		})
		if err != nil {
			return errors.Annotatef(err, "unable to update Collection")
		}
	}

	for _, it := range items {
		if vocab.IsIRI(it) {
			// NOTE(marius): append received items to the list
			if _, err := loadFromThreeTables(r, it.GetLink()); err != nil {
				return errors.NewNotFound(err, "invalid item to add to collection")
			}
		}
		_ = iris.Append(it)
	}

	rawItems, err := iris.MarshalJSON()
	if err != nil {
		return errors.Annotatef(err, "unable to marshal Collection items")
	}

	typ := c.GetType()
	if orderedCollectionTypes.Match(typ) {
		err = vocab.OnOrderedCollection(c, func(col *vocab.OrderedCollection) error {
			col.TotalItems = iris.Count()
			col.OrderedItems = nil
			return nil
		})
	} else if collectionTypes.Match(typ) {
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
	query := `INSERT OR REPLACE INTO collections (iri, raw, items) VALUES (?, ?, ?);`
	_, err = tx.Exec(query, col.GetLink(), string(raw), string(rawItems))
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

	tx, err := r.conn.Begin()
	if err != nil {
		r.errFn("%s", errors.Annotatef(err, "transaction start error"))
	}
	defer func() {
		if err := tx.Commit(); err != nil {
			r.errFn("%s", errors.Annotatef(err, "transaction commit error"))
		}
	}()

	return r.addTo(tx, col, items...)
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
	_, err := conn.Exec(query, iri, string(m))
	return err
}

func loadMetadataFromTable(conn *sql.DB, iri vocab.IRI) ([]byte, error) {
	var meta []byte
	sel := "SELECT raw FROM meta WHERE iri = ?;"
	err := conn.QueryRow(sel, iri).Scan(&meta)
	return meta, err
}

func isSingleItem(f ...filters.Check) bool {
	return len(f) == 0 || len(filters.SameIDChecks(f...)) == 1
}

func stripFiltersFromIRI(iri vocab.IRI) vocab.IRI {
	u, err := iri.URL()
	if err != nil {
		return iri
	}
	u.RawQuery = ""
	return vocab.IRI(u.String())
}

func loadFromThreeTables(r *repo, iri vocab.IRI, f ...filters.Check) (vocab.CollectionInterface, error) {
	if isSingleItem(f...) {
		if len(f) == 0 {
			f = filters.Checks{filters.SameID(iri)}
		}
	}
	if r.cache != nil {
		if cachedIt := r.cache.Load(iri); cachedIt != nil {
			return &vocab.ItemCollection{cachedIt}, nil
		}
	}

	conn := r.ro
	var unions *sqlf.Stmt
	for _, table := range []string{"actors", "objects", "activities"} {
		st := sqlf.From(table)
		st.Select("iri").Select("raw").Select("published")
		_ = filters.SQLWhere(st, f...)
		if unions == nil {
			unions = st
		} else {
			unions.Union(true, st)
		}
	}

	ret := make(vocab.ItemCollection, 0)
	topSt := sqlf.From("("+unions.String()+") as x", unions.Args()...)
	topSt.Select("iri").Select("raw")
	filters.SQLLimit(topSt, f...)

	sq := topSt.String()
	ag := topSt.Args()

	st, err := conn.Prepare(sq)
	if err != nil {
		return nil, errors.Annotatef(err, "unable to prepare statement")
	}
	defer st.Close()

	rows, err := st.Query(ag...)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errors.NotFoundf("no rows found")
		}
		return nil, errors.Annotatef(err, "unable to run select")
	}
	defer rows.Close()

	// Iterate through the result set
	for rows.Next() {
		var iri string
		var raw []byte
		if err = rows.Scan(&iri, &raw); err != nil {
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

func isActorsCollectionIRI(iri vocab.IRI) bool {
	lst := vocab.CollectionPath(filepath.Base(iriPath(iri)))
	return lst == filters.ActorsType
}

func isObjectsCollectionIRI(iri vocab.IRI) bool {
	lst := vocab.CollectionPath(filepath.Base(iriPath(iri)))
	return lst == filters.ObjectsType
}

func isCollectionIRI(iri vocab.IRI) bool {
	lst := vocab.CollectionPath(filepath.Base(iriPath(iri)))
	return collectionPaths.Contains(lst)
}

func loadFromOneTable(r *repo, iri vocab.IRI, table vocab.CollectionPath, f ...filters.Check) (vocab.CollectionInterface, error) {
	conn := r.ro
	if isSingleItem(f...) && r.cache != nil {
		if cachedIt := r.cache.Load(iri); cachedIt != nil {
			return &vocab.ItemCollection{cachedIt}, nil
		}
	}
	if table == "" {
		return nil, errors.Newf("invalid table")
	}

	selS := sqlf.From(string(table))
	selS.Select("iri").Select("raw")
	ret := make(vocab.ItemCollection, 0)
	_ = filters.SQLWhere(selS, f...)

	filters.SQLLimit(selS, f...)
	selS.OrderBy("updated DESC")

	sel := selS.String()
	args := selS.Args()
	st, err := conn.Prepare(sel)
	if err != nil {
		return nil, errors.Annotatef(err, "unable to prepare statement")
	}
	defer st.Close()

	rows, err := st.Query(args...)
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
		target := act.Target
		if !vocab.IsNil(target) && target.GetLink().Equals(ob.GetLink(), false) {
			act.Target = ob
			keep = !vocab.IsNil(filters.Checks(f).Run(act.Target))
		}
		return keep
	}
}

func loadTagsForObject(r *repo, ff ...filters.Check) func(o *vocab.Object) error {
	tf := filters.TagChecks(ff...)
	return func(o *vocab.Object) error {
		if len(o.Tag) == 0 {
			return nil
		}
		return vocab.OnItemCollection(o.Tag, func(col *vocab.ItemCollection) error {
			for i, t := range *col {
				if vocab.IsNil(t) || !vocab.IsIRI(t) {
					return nil
				}
				if len(tf) == 0 {
					tf = append(tf, filters.SameID(t.GetLink()))
				}
				if ob, err := loadFromOneTable(r, t.GetLink(), "objects", tf...); err == nil {
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
		if !vocab.ActivityTypes.Match(rr.GetType()) {
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
	typ := par.GetType()
	if orderedCollectionTypes.Match(typ) {
		_ = vocab.OnOrderedCollection(par, postProcessOrderedCollection(par.Collection()))
	} else if collectionTypes.Match(typ) {
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
	conn := r.ro

	selects := []string{"c.iri = ? "}
	params := []any{iri}

	table := getCollectionTypeFromIRI(iri)
	s := sqlf.From("collections c")
	s.Select("c.iri")
	s.Where("c.iri = ?", iri)

	if isActorsCollectionIRI(iri) {
		// NOTE(marius): if loading the /actors storage collection we should keep only
		// items that are "namespaced" in that collection.
		// This fixes an issue that we would return also the root service for FedBOX, or collections
		selects = append(selects, "x.iri LIKE ?")
		params = append(params, iri.String()+"%")
		s.Where("x.iri like ?", iri.String()+"%")
	}

	filters.SQLLimit(s, f...)
	if isStorageCollectionIRI(iri) {
		s.Select(`json_patch(json(c.raw), json_object('orderedItems', json_group_array(json(x.raw)))) raw`)
		s.LeftJoin(string(table)+" x", "true")
		s.OrderBy("x.published DESC")
	} else {
		s.Select(`json_patch(json(c.raw), json_object('orderedItems', json_group_array(json(coalesce(x.raw, y.raw, o.raw))))) raw`)
		s.From(`json_each(json_insert(c.items, '$[0]', null))`)
		s.LeftJoin("activities x", "value = x.iri")
		s.LeftJoin("actors y", "value = y.iri")
		s.LeftJoin("objects o", "value = o.iri")
		s.OrderBy("COALESCE(x.published, y.published, o.published) DESC")
	}

	sq := s.String()
	args := s.Args()

	st, err := conn.Prepare(sq)
	if err != nil {
		return nil, errors.Annotatef(err, "unable to prepare statement")
	}
	defer st.Close()

	var cIri sql.NullString
	var raw []byte
	if err = st.QueryRow(args...).Scan(&cIri, &raw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errors.NotFoundf("failed to find items in collection %s", iri)
		}
		return nil, errors.Annotatef(err, "failed to run select for %s", iri)
	}

	if len(raw) == 0 {
		return nil, errors.NotFoundf("Unable to find items in collection %s", iri)
	}

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
		typ := res.GetType()
		if orderedCollectionTypes.Match(typ) {
			err = vocab.OnOrderedCollection(res, postProcessOrderedCollection(items))
		} else if collectionTypes.Match(typ) {
			err = vocab.OnCollection(res, postProcessCollection(items))
		}
	}
	return &res, err
}

func delete(r repo, it vocab.Item) error {
	iri := it.GetLink()
	cleanupTables := []string{"meta", "actors", "objects", "activities"}

	tx, err := r.conn.Begin()
	if err != nil {
		r.errFn("%s", errors.Annotatef(err, "transaction start error"))
	}
	defer func() {
		if err := tx.Commit(); err != nil {
			r.errFn("%s", errors.Annotatef(err, "transaction commit error"))
		}
	}()

	removeFn := func(table string, iri vocab.IRI) error {
		query := fmt.Sprintf("DELETE FROM %s where iri = $1;", table)
		if _, err := tx.Exec(query, iri); err != nil {
			r.errFn("query error: %s\n%s", err, query)
			return errors.Annotatef(err, "query error")
		}
		return nil
	}

	for _, tbl := range cleanupTables {
		if err := removeFn(tbl, iri); err != nil {
			return err
		}
	}

	if r.cache != nil {
		r.cache.Delete(iri)
	}
	return nil
}

const upsertQ = "INSERT OR REPLACE INTO %s (%s) VALUES (%s);"

func (r *repo) save(tx *sql.Tx, it vocab.Item) (vocab.Item, error) {
	if vocab.IsNil(it) {
		return nil, nil
	}

	iri := it.GetLink()

	var raw []byte
	var err error
	raw, err = encodeItemFn(it)

	if err != nil {
		r.errFn("query error: %s", err)
		return it, errors.Annotatef(err, "query error")
	}

	columns := []string{"raw, iri"}
	tokens := []string{"?, ?"}
	params := []any{string(raw), iri}

	table := string(filters.ObjectsType)
	typ := it.GetType()
	if append(collectionTypes, orderedCollectionTypes...).Match(typ) {
		table = "collections"
	} else if append(vocab.ActivityTypes, vocab.IntransitiveActivityTypes...).Match(typ) {
		table = string(filters.ActivitiesType)
	} else if vocab.ActorTypes.Match(typ) {
		table = string(filters.ActorsType)
	} else if vocab.TombstoneType.Match(typ) {
		if strings.Contains(iri.String(), string(filters.ActorsType)) {
			table = string(filters.ActorsType)
		}
		if strings.Contains(iri.String(), string(filters.ActivitiesType)) {
			table = string(filters.ActivitiesType)
		}
	}

	query := fmt.Sprintf(`INSERT OR REPLACE INTO %s (%s) VALUES (%s);`, table, strings.Join(columns, ", "), strings.Join(tokens, ", "))

	if _, err = tx.Exec(query, params...); err != nil {
		return it, errors.Annotatef(err, "query error")
	}
	col, _ := path.Split(iri.String())
	if isCollectionIRI(vocab.IRI(col)) {
		// Add private items to the collections table
		if colIRI, k := vocab.Split(vocab.IRI(col)); k == "" {
			if err = r.addTo(tx, colIRI, it); err != nil {
				r.logFn("warning adding item: %s: %s", colIRI, err)
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
		if vocab.ActorTypes.Match(owner.GetType()) {
			col.AttributedTo = owner.GetLink()
		}
		_ = vocab.OnObject(owner, func(ob *vocab.Object) error {
			if !ob.Published.IsZero() {
				col.Published = ob.Published
			}
			return nil
		})
	}
	return &col
}
