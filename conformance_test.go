//go:build conformance

package sqlite

import (
	"testing"

	conformance "github.com/go-ap/storage-conformance-suite"
)

func initStorage(t *testing.T) conformance.ActivityPubStorage {
	conf := Config{Path: t.TempDir()}
	if err := Bootstrap(conf); err != nil {
		t.Fatalf("unable to bootstrap storage: %s", err)
	}
	storage, err := New(conf)
	if err != nil {
		t.Fatalf("unable to initialize storage: %s", err)
	}
	storage.errFn = t.Logf
	storage.logFn = t.Logf
	return storage
}

func Test_Conformance(t *testing.T) {
	conformance.Suite(
		conformance.TestActivityPub, conformance.TestMetadata,
		conformance.TestKey, conformance.TestOAuth, conformance.TestPassword,
	).Run(t, initStorage(t))
}
