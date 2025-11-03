package sqlite

import (
	"fmt"
	"path"
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
