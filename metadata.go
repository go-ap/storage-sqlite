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

	vocab "github.com/go-ap/activitypub"
	"github.com/go-ap/errors"
	"golang.org/x/crypto/bcrypt"
)

type Metadata struct {
	Pw         []byte `jsonld:"pw,omitempty"`
	PrivateKey []byte `jsonld:"key,omitempty"`
}

// PasswordSet
func (r *repo) PasswordSet(iri vocab.IRI, pw []byte) error {
	if r == nil || r.conn == nil {
		return errNotOpen
	}
	if pw == nil {
		return errors.Newf("could not generate hash for nil pw")
	}
	if len(iri) == 0 {
		return errors.NotFoundf("not found")
	}
	pw, err := bcrypt.GenerateFromPassword(pw, -1)
	if err != nil {
		return errors.Annotatef(err, "could not generate pw hash")
	}
	m := new(Metadata)
	_ = r.LoadMetadata(iri, m)
	m.Pw = pw
	return r.SaveMetadata(iri, m)
}

// PasswordCheck
func (r *repo) PasswordCheck(iri vocab.IRI, pw []byte) error {
	if r == nil || r.conn == nil {
		return errNotOpen
	}
	m := new(Metadata)
	if err := r.LoadMetadata(iri, m); err != nil {
		return errors.Annotatef(err, "Could not find load metadata for %s", iri)
	}
	if err := bcrypt.CompareHashAndPassword(m.Pw, pw); err != nil {
		return errors.NewUnauthorized(err, "Invalid pw")
	}
	return nil
}

// LoadMetadata
func (r *repo) LoadMetadata(iri vocab.IRI, m any) error {
	if r == nil || r.conn == nil {
		return errNotOpen
	}
	raw, err := loadMetadataFromTable(r.conn, iri)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return errors.NewNotFound(err, "not found")
		}
		return err
	}
	if len(raw) == 0 {
		return nil
	}

	if err = decodeFn(raw, m); err != nil {
		return errors.Annotatef(err, "Could not unmarshal metadata")
	}
	return nil
}

// SaveMetadata
func (r *repo) SaveMetadata(iri vocab.IRI, m any) error {
	if r == nil || r.conn == nil {
		return errNotOpen
	}
	if m == nil {
		return errors.Newf("Could not save nil metadata")
	}
	entryBytes, err := encodeFn(m)
	if err != nil {
		return errors.Annotatef(err, "Could not marshal metadata")
	}
	return saveMetadataToTable(r.conn, iri, entryBytes)
}

// LoadKey loads a private key for an actor found by its IRI
func (r *repo) LoadKey(iri vocab.IRI) (crypto.PrivateKey, error) {
	if r == nil || r.conn == nil {
		return nil, errNotOpen
	}
	m := new(Metadata)
	if err := r.LoadMetadata(iri, m); err != nil {
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

// SaveKey saves a private key for an actor found by its IRI
func (r *repo) SaveKey(iri vocab.IRI, key crypto.PrivateKey) (*vocab.PublicKey, error) {
	if r == nil || r.conn == nil {
		return nil, errNotOpen
	}
	m := new(Metadata)
	if err := r.LoadMetadata(iri, m); err != nil && !errors.IsNotFound(err) {
		return nil, err
	}
	if m.PrivateKey != nil {
		r.logFn("actor %s already has a private key", iri)
	}
	prvEnc, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, err
	}

	m.PrivateKey = pem.EncodeToMemory(&pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: prvEnc,
	})
	if err = r.SaveMetadata(iri, m); err != nil {
		return nil, err
	}

	var pub crypto.PublicKey
	switch prv := key.(type) {
	case *ecdsa.PrivateKey:
		pub = prv.Public()
	case *rsa.PrivateKey:
		pub = prv.Public()
	case *dsa.PrivateKey:
		pub = &prv.PublicKey
	case ed25519.PrivateKey:
		pub = prv.Public()
	default:
		r.errFn("received key %T does not match any of the known private key types", key)
		return nil, nil
	}
	pubEnc, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		r.errFn("unable to x509.MarshalPKIXPublicKey() the private key %T for %s", pub, iri)
		return nil, err
	}
	pubEncoded := pem.EncodeToMemory(&pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: pubEnc,
	})

	return &vocab.PublicKey{
		ID:           vocab.IRI(fmt.Sprintf("%s#main", iri)),
		Owner:        iri,
		PublicKeyPem: string(pubEncoded),
	}, nil
}
