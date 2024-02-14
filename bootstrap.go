package sqlite

import (
	"fmt"
	"os"
	"strings"

	"github.com/go-ap/errors"
)

func stringClean(qSql string) string {
	return strings.ReplaceAll(qSql, "\n", "")
}

func Clean(conf Config) error {
	p, err := getFullPath(conf)
	if err != nil {
		return err
	}
	return os.RemoveAll(p)
}

func Bootstrap(conf Config) error {
	p, err := getFullPath(conf)
	if err != nil {
		return err
	}

	r := repo{
		path:  p,
		logFn: defaultLogFn,
		errFn: defaultLogFn,
	}
	if err = r.Open(); err != nil {
		return err
	}
	defer func() {
		if err = r.close(); err != nil {
			r.errFn("error closing the db: %+s", err)
		}
	}()

	exec := func(qRaw string, par ...interface{}) error {
		qSql := fmt.Sprintf(qRaw, par...)
		if _, err = r.conn.Exec(qSql); err != nil {
			return errors.Annotatef(err, "unable to execute: %q", qSql)
		}
		return nil
	}

	if err = exec(createObjectsQuery); err != nil {
		return err
	}
	if err = exec(createActivitiesQuery); err != nil {
		return err
	}
	if err = exec(createActorsQuery); err != nil {
		return err
	}
	if err = exec(createCollectionsQuery); err != nil {
		return err
	}
	if err = exec(createMetaQuery); err != nil {
		return err
	}
	if err = exec(createClientTable); err != nil {
		return err
	}
	if err = exec(createAuthorizeTable); err != nil {
		return err
	}
	if err = exec(createAccessTable); err != nil {
		return err
	}
	if err = exec(createRefreshTable); err != nil {
		return err
	}
	if err = exec(tuneQuery); err != nil {
		return err
	}

	return nil
}

func (r *repo) Reset() {
	tables := []string{
		"objects",
		"actors",
		"activities",
		"collections",
		"meta",
		"clients",
		"authorize",
		"access",
		"refresh",
	}

	r.Open()
	defer r.close()

	for _, table := range tables {
		r.conn.Exec(fmt.Sprintf("DELETE FROM %s;", table))
	}
}
