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
	if conf.Path == "" {
		return os.ErrNotExist
	}
	p, err := getFullPath(conf)
	if err != nil {
		return err
	}

	r := repo{
		path:  p,
		logFn: defaultLogFn,
		errFn: defaultLogFn,
	}
	if conf.LogFn != nil {
		r.logFn = conf.LogFn
	}
	if conf.ErrFn != nil {
		r.errFn = conf.ErrFn
	}
	if err = r.Open(); err != nil {
		return err
	}
	defer r.Close()

	exec := func(qRaw string, par ...interface{}) error {
		qSql := fmt.Sprintf(qRaw, par...)
		if _, err = r.conn.Exec(qSql); err != nil {
			return errors.Annotatef(err, `unable to execute: "%s"`, stringClean(qSql))
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

var tables = []string{
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

func (r *repo) Reset() {
	err := r.Open()
	if err != nil {
		return
	}
	defer r.Close()

	for _, table := range tables {
		q := `DELETE FROM "` + table + `";`
		_, _ = r.conn.Exec(q)
	}
}
