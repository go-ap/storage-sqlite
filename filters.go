package sqlite

import (
	"fmt"
	"net/url"
	"path"
	"strconv"
	"strings"

	vocab "github.com/go-ap/activitypub"
	"github.com/go-ap/filters"
)

func isCollection(col string) bool {
	return col == string(filters.ActorsType) || col == string(filters.ActivitiesType) || col == string(filters.ObjectsType)
}

func getStringFieldInJSONWheres(strs filters.CompStrs, props ...string) (string, []interface{}) {
	if len(strs) == 0 {
		return "", nil
	}
	var values = make([]interface{}, 0)
	keyWhere := make([]string, 0)
	for _, n := range strs {
		switch n.Operator {
		case "!":
			for _, prop := range props {
				if len(n.Str) == 0 || n.Str == vocab.NilLangRef.String() {
					keyWhere = append(keyWhere, fmt.Sprintf(`json_extract("raw", '$.%s') IS NOT NULL`, prop))
				} else {
					keyWhere = append(keyWhere, fmt.Sprintf(`json_extract("raw", '$.%s') NOT LIKE ?`, prop))
					values = append(values, interface{}("%"+n.Str+"%"))
				}
			}
		case "~":
			for _, prop := range props {
				keyWhere = append(keyWhere, fmt.Sprintf(`json_extract("raw", '$.%s') LIKE ?`, prop))
				values = append(values, interface{}("%"+n.Str+"%"))
			}
		case "", "=":
			fallthrough
		default:
			for _, prop := range props {
				if len(n.Str) == 0 || n.Str == vocab.NilLangRef.String() {
					keyWhere = append(keyWhere, fmt.Sprintf(`json_extract("raw", '$.%s') IS NULL`, prop))
				} else {
					keyWhere = append(keyWhere, fmt.Sprintf(`json_extract("raw", '$.%s') = ?`, prop))
					values = append(values, interface{}(n.Str))
				}
			}
		}
	}
	return fmt.Sprintf("(%s)", strings.Join(keyWhere, " OR ")), values
}

func getStringFieldWheres(strs filters.CompStrs, fields ...string) (string, []interface{}) {
	if len(strs) == 0 {
		return "", nil
	}
	var values = make([]interface{}, 0)
	keyWhere := make([]string, 0)
	for _, t := range strs {
		switch t.Operator {
		case "!":
			for _, field := range fields {
				if len(t.Str) == 0 || t.Str == vocab.NilLangRef.String() {
					keyWhere = append(keyWhere, fmt.Sprintf(`"%s" IS NOT NULL`, field))
				} else {
					keyWhere = append(keyWhere, fmt.Sprintf(`"%s" NOT LIKE ?`, field))
					values = append(values, interface{}("%"+t.Str+"%"))
				}
			}
		case "~":
			for _, field := range fields {
				keyWhere = append(keyWhere, fmt.Sprintf(`"%s" LIKE ?`, field))
				values = append(values, interface{}("%"+t.Str+"%"))
			}
		case "", "=":
			for _, field := range fields {
				if len(t.Str) == 0 || t.Str == vocab.NilLangRef.String() {
					keyWhere = append(keyWhere, fmt.Sprintf(`"%s" IS NULL`, field))
				} else {
					keyWhere = append(keyWhere, fmt.Sprintf(`"%s" = ?`, field))
					values = append(values, interface{}(t.Str))
				}
			}
		}
	}

	return fmt.Sprintf("(%s)", strings.Join(keyWhere, " OR ")), values
}

func getTypeWheres(strs filters.CompStrs) (string, []interface{}) {
	return getStringFieldWheres(strs, "type")
}

func getContextWheres(strs filters.CompStrs) (string, []interface{}) {
	return getStringFieldInJSONWheres(strs, "context")
}

func irisFilter(iris vocab.IRIs) filters.CompStrs {
	f := make(filters.CompStrs, 0, len(iris))
	for _, iri := range iris {
		f = append(f, filters.StringEquals(iri.String()))
	}
	return f
}

func replaceFilter(f *filters.Filters, with string) ([]string, []any) {
	clauses, wheres := getWhereClauses("activities", f)
	for i, cl := range clauses {
		clauses[i] = strings.Replace(cl, "iri", with, 1)
	}
	return clauses, wheres
}
func getActorWheres(f *filters.Filters) (string, []any) {
	clauses, wheres := replaceFilter(f, "actor")
	return strings.Join(clauses, " AND "), wheres
}

func getObjectWheres(f *filters.Filters) (string, []interface{}) {
	clauses, wheres := replaceFilter(f, "object")
	return strings.Join(clauses, " AND "), wheres
}

func getTargetWheres(f *filters.Filters) (string, []interface{}) {
	clauses, wheres := replaceFilter(f, "target")
	return strings.Join(clauses, " AND "), wheres
}

func getURLWheres(strs filters.CompStrs) (string, []interface{}) {
	clause, values := getStringFieldWheres(strs, "url")
	jClause, jValues := getStringFieldInJSONWheres(strs, "url")
	if len(jClause) > 0 {
		if len(clause) > 0 {
			clause += " OR "
		}
		clause += jClause
	}
	values = append(values, jValues...)
	return clause, values
}

var MandatoryCollections = vocab.CollectionPaths{
	vocab.Inbox,
	vocab.Outbox,
	vocab.Replies,
}

func getIRIWheres(strs filters.CompStrs, id vocab.IRI) (string, []interface{}) {
	iriClause, iriValues := getStringFieldWheres(strs, "iri")

	skipId := strings.Contains(iriClause, `"iri"`)
	if skipId {
		return iriClause, iriValues
	}

	if u, _ := id.URL(); u != nil {
		u.RawQuery = ""
		u.User = nil
		id = vocab.IRI(u.String())
	}
	// FIXME(marius): this is a hack that avoids trying to use clause on IRI, when iri == "/"
	if len(id) > 1 {
		if len(iriClause) > 0 {
			iriClause += " OR "
		}
		if base := path.Base(id.String()); isCollection(base) {
			iriClause += `"iri" LIKE ?`
			iriValues = append(iriValues, interface{}("%"+id+"%"))
		} else {
			iriClause += `"iri" = ?`
			iriValues = append(iriValues, interface{}(id))
		}
	}
	return iriClause, iriValues
}

func getNamesWheres(strs filters.CompStrs) (string, []interface{}) {
	return getStringFieldInJSONWheres(strs, "name", "preferredUsername")
}

func getInReplyToWheres(strs filters.CompStrs) (string, []interface{}) {
	return getStringFieldInJSONWheres(strs, "inReplyTo")
}

func getAttributedToWheres(strs filters.CompStrs) (string, []interface{}) {
	return getStringFieldInJSONWheres(strs, "attributedTo")
}

func getWhereClauses(table string, f *filters.Filters) ([]string, []interface{}) {
	var clauses = make([]string, 0)
	var values = make([]interface{}, 0)
	if f == nil {
		return clauses, values
	}

	if typClause, typValues := getTypeWheres(f.Types()); len(typClause) > 0 {
		values = append(values, typValues...)
		clauses = append(clauses, typClause)
	}

	if iriClause, iriValues := getIRIWheres(f.IRIs(), f.GetLink()); len(iriClause) > 0 {
		values = append(values, iriValues...)
		clauses = append(clauses, iriClause)
	}

	if nameClause, nameValues := getNamesWheres(f.Names()); len(nameClause) > 0 {
		values = append(values, nameValues...)
		clauses = append(clauses, nameClause)
	}

	if replClause, replValues := getInReplyToWheres(f.InReplyTo()); len(replClause) > 0 {
		values = append(values, replValues...)
		clauses = append(clauses, replClause)
	}

	if authorClause, authorValues := getAttributedToWheres(f.AttributedTo()); len(authorClause) > 0 {
		values = append(values, authorValues...)
		clauses = append(clauses, authorClause)
	}

	if urlClause, urlValues := getURLWheres(f.URLs()); len(urlClause) > 0 {
		values = append(values, urlValues...)
		clauses = append(clauses, urlClause)
	}

	if ctxtClause, ctxtValues := getContextWheres(f.Context()); len(ctxtClause) > 0 {
		values = append(values, ctxtValues...)
		clauses = append(clauses, ctxtClause)
	}

	if len(clauses) == 0 {
		if filters.FedBOXCollections.Contains(f.Collection) {
			clauses = append(clauses, " true")
		} else {
			clauses = append(clauses, " false")
		}
	}

	return clauses, values
}

const DefaultMaxItems = 100

func getPagination(f Filterable, alias string, clauses *[]string, values *[]any) string {
	ff, ok := f.(*filters.Filters)
	if !ok {
		return ""
	}
	if ff.MaxItems == 0 {
		ff.MaxItems = DefaultMaxItems
	}
	limit := fmt.Sprintf(" LIMIT %d", ff.MaxItems)
	if ff.CurPage > 0 {
		return fmt.Sprintf("%s OFFSET %d", limit, ff.MaxItems*(int(ff.CurPage)-1))
	}
	table := getCollectionTableFromFilter(ff)
	if len(ff.Next) > 0 {
		*values = append(*values, ff.Next)
		*clauses = append(*clauses, fmt.Sprintf("%s.published < (select published from %s where iri = ?)", alias, table))
	}
	if len(ff.Prev) > 0 {
		*values = append(*values, ff.Prev)
		*clauses = append(*clauses, fmt.Sprintf("%s.published > (select published from %s where iri = ?)", alias, table))
	}
	return limit
}

// PaginateCollection is a function that populates the received collection
func PaginateCollection(it vocab.Item) vocab.Item {
	if vocab.IsNil(it) {
		return it
	}

	col, prevIRI, nextIRI := collectionPageFromItem(it)
	if vocab.IsNil(col) {
		return it
	}
	if vocab.IsItemCollection(col) {
		return col
	}

	partOfIRI := it.GetID()
	firstIRI := partOfIRI
	if u, err := it.GetLink().URL(); err == nil {
		q := u.Query()
		for k := range q {
			if k == keyMaxItems || k == keyAfter || k == keyBefore {
				q.Del(k)
			}
		}
		partOfIRI = vocab.IRI(u.String())
		if !q.Has(keyMaxItems) {
			q.Set(keyMaxItems, strconv.Itoa(DefaultMaxItems))
		}
		u.RawQuery = q.Encode()
		firstIRI = vocab.IRI(u.String())
	}

	switch col.GetType() {
	case vocab.OrderedCollectionType:
		_ = vocab.OnOrderedCollection(col, func(c *vocab.OrderedCollection) error {
			c.First = firstIRI
			return nil
		})
	case vocab.OrderedCollectionPageType:
		_ = vocab.OnOrderedCollectionPage(col, func(c *vocab.OrderedCollectionPage) error {
			c.PartOf = partOfIRI
			c.First = firstIRI
			if !nextIRI.GetLink().Equal(firstIRI, true) {
				c.Next = nextIRI
			}
			if !prevIRI.GetLink().Equal(firstIRI, true) {
				c.Prev = prevIRI
			}
			return nil
		})
	case vocab.CollectionType:
		_ = vocab.OnCollection(col, func(c *vocab.Collection) error {
			c.First = firstIRI
			return nil
		})
	case vocab.CollectionPageType:
		_ = vocab.OnCollectionPage(col, func(c *vocab.CollectionPage) error {
			c.PartOf = partOfIRI
			c.First = firstIRI
			if !nextIRI.GetLink().Equal(firstIRI, true) {
				c.Next = nextIRI
			}
			if !prevIRI.GetLink().Equal(firstIRI, true) {
				c.Prev = prevIRI
			}
			return nil
		})
	}

	return col
}

func collectionPageFromItem(it vocab.Item) (vocab.Item, vocab.Item, vocab.Item) {
	typ := it.GetType()

	if !vocab.CollectionTypes.Contains(typ) {
		return it, nil, nil
	}

	var prev url.Values
	var next url.Values

	var prevIRI vocab.IRI
	var nextIRI vocab.IRI

	shouldBePage := strings.Contains(it.GetLink().String(), keyMaxItems)

	switch typ {
	case vocab.OrderedCollectionPageType:
		_ = vocab.OnOrderedCollectionPage(it, func(new *vocab.OrderedCollectionPage) error {
			new.OrderedItems, prev, next = filterCollection(new.Collection())
			if len(prev) > 0 {
				prevIRI = getURL(it.GetLink(), prev)
			}
			if len(next) > 0 {
				nextIRI = getURL(it.GetLink(), next)
			}
			return nil
		})
	case vocab.CollectionPageType:
		_ = vocab.OnCollectionPage(it, func(new *vocab.CollectionPage) error {
			new.Items, prev, next = filterCollection(new.Collection())
			if len(prev) > 0 {
				prevIRI = getURL(it.GetLink(), prev)
			}
			if len(next) > 0 {
				nextIRI = getURL(it.GetLink(), next)
			}
			return nil
		})
	case vocab.OrderedCollectionType:
		if shouldBePage {
			result := new(vocab.OrderedCollectionPage)
			old, _ := it.(*vocab.OrderedCollection)
			err := vocab.OnOrderedCollection(result, func(new *vocab.OrderedCollection) error {
				_, err := vocab.CopyOrderedCollectionProperties(new, old)
				new.Type = vocab.OrderedCollectionPageType
				new.OrderedItems, prev, next = filterCollection(new.Collection())
				if len(prev) > 0 {
					prevIRI = getURL(it.GetLink(), prev)
				}
				if len(next) > 0 {
					nextIRI = getURL(it.GetLink(), next)
				}
				return err
			})
			if err == nil {
				it = result
			}
		} else {
			_ = vocab.OnOrderedCollection(it, func(new *vocab.OrderedCollection) error {
				new.OrderedItems, prev, next = filterCollection(new.Collection())
				if len(next) > 0 {
					new.First = getURL(it.GetLink(), next)
				}
				return nil
			})
		}
	case vocab.CollectionType:
		if shouldBePage {
			result := new(vocab.CollectionPage)
			old, _ := it.(*vocab.Collection)
			err := vocab.OnCollection(result, func(new *vocab.Collection) error {
				_, err := vocab.CopyCollectionProperties(new, old)
				new.Type = vocab.CollectionPageType
				new.Items, prev, next = filterCollection(new.Collection())
				if len(prev) > 0 {
					prevIRI = getURL(it.GetLink(), prev)
				}
				if len(next) > 0 {
					nextIRI = getURL(it.GetLink(), next)
				}
				return err
			})
			if err == nil {
				it = result
			}
		} else {
			_ = vocab.OnCollection(it, func(new *vocab.Collection) error {
				new.Items, prev, next = filterCollection(new.Collection())
				if len(next) > 0 {
					new.First = getURL(it.GetLink(), next)
				}
				return nil
			})
		}
	case vocab.CollectionOfItems:
		_ = vocab.OnItemCollection(it, func(col *vocab.ItemCollection) error {
			it, _, _ = filterCollection(*col)
			return nil
		})
		return it, nil, nil
	}

	return it, prevIRI, nextIRI
}

const (
	keyAfter  = "after"
	keyBefore = "before"

	keyMaxItems = "maxItems"
)

func filterCollection(col vocab.ItemCollection) (vocab.ItemCollection, url.Values, url.Values) {
	if len(col) == 0 {
		return col, nil, nil
	}

	pp := url.Values{}
	np := url.Values{}

	fpEnd := len(col) - 1
	if fpEnd > DefaultMaxItems {
		fpEnd = DefaultMaxItems
	}
	bpEnd := 0
	if len(col) > DefaultMaxItems {
		bpEnd = (len(col) / DefaultMaxItems) * DefaultMaxItems
	}

	firstPage := col[0:fpEnd]
	lastPage := col[bpEnd:]

	if len(col) == 0 {
		return col, pp, np
	}
	first := col.First()
	if len(col) > DefaultMaxItems {
		pp.Add(keyMaxItems, strconv.Itoa(DefaultMaxItems))
		np.Add(keyMaxItems, strconv.Itoa(DefaultMaxItems))

		onFirstPage := false
		for _, top := range firstPage {
			if onFirstPage = first.GetLink().Equal(top.GetLink(), true); onFirstPage {
				break
			}
		}
		if !onFirstPage {
			pp.Add(keyBefore, first.GetLink().String())
		} else {
			pp = nil
		}
		if len(col) > 1 && len(col) > DefaultMaxItems+1 {
			last := col[len(col)-1]
			onLastPage := false
			for _, bottom := range lastPage {
				if onLastPage = last.GetLink().Equal(bottom.GetLink(), true); onLastPage {
					break
				}
			}
			if !onLastPage {
				np.Add(keyAfter, last.GetLink().String())
			} else {
				np = nil
			}
		}
	}
	return col, pp, np
}
func getURL(i vocab.IRI, f url.Values) vocab.IRI {
	if f == nil {
		return i
	}
	if u, err := i.URL(); err == nil {
		q := u.Query()
		for k, v := range f {
			q.Del(k)
			for _, vv := range v {
				q.Add(k, vv)
			}
		}
		u.RawQuery = q.Encode()
		i = vocab.IRI(u.String())
	}
	return i
}
