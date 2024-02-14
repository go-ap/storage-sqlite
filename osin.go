package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"time"

	"github.com/go-ap/errors"
	"github.com/openshift/osin"
)

const defaultTimeout = 1000 * time.Millisecond

const (
	createClientTable = `CREATE TABLE IF NOT EXISTS "clients"(
	"code" varchar constraint client_code_pkey PRIMARY KEY,
	"secret" varchar NOT NULL,
	"redirect_uri" varchar NOT NULL,
	"extra" BLOB DEFAULT '{}'
);
`

	createAuthorizeTable = `CREATE TABLE IF NOT EXISTS "authorize" (
	"client" varchar REFERENCES clients(code),
	"code" varchar constraint authorize_code_pkey PRIMARY KEY,
	"expires_in" INTEGER,
	"scope" BLOB,
	"redirect_uri" varchar NOT NULL,
	"state" BLOB,
	"created_at" timestamp DEFAULT CURRENT_TIMESTAMP,
	"extra" BLOB DEFAULT '{}'
);
`

	createAccessTable = `CREATE TABLE IF NOT EXISTS "access" (
	"client" varchar REFERENCES clients(code),
	"authorize" varchar REFERENCES authorize(code),
	"previous" varchar,
	"token" varchar NOT NULL,
	"refresh_token" varchar NOT NULL,
	"expires_in" INTEGER,
	"scope" BLOB DEFAULT NULL,
	"redirect_uri" varchar NOT NULL,
	"created_at" timestamp DEFAULT CURRENT_TIMESTAMP,
	"extra" BLOB DEFAULT '{}'
);
`

	createRefreshTable = `CREATE TABLE IF NOT EXISTS "refresh" (
	"access_token" TEXT NOT NULL REFERENCES access(token),
	"token" TEXT PRIMARY KEY NOT NULL
);
`
)

func bootstrapOsin(r repo) error {
	if err := r.Open(); err != nil {
		return err
	}
	defer r.Close()

	exec := func(conn *sql.DB, qRaw string, par ...any) error {
		qSql := fmt.Sprintf(qRaw, par...)
		r.logFn("Executing %s", stringClean(qSql))
		if _, err := conn.Exec(qSql); err != nil {
			r.errFn("Failed: %s", err)
			return errors.Annotatef(err, "unable to execute: %s", stringClean(qSql))
		}
		r.logFn("Success!")
		return nil
	}

	if err := exec(r.conn, createClientTable); err != nil {
		return err
	}
	if err := exec(r.conn, createAuthorizeTable); err != nil {
		return err
	}
	if err := exec(r.conn, createAccessTable); err != nil {
		return err
	}
	if err := exec(r.conn, createRefreshTable); err != nil {
		return err
	}
	return nil
}

var encodeFn = func(v any) ([]byte, error) {
	buf := bytes.Buffer{}
	err := json.NewEncoder(&buf).Encode(v)
	return buf.Bytes(), err
}

var decodeFn = func(data []byte, m any) error {
	return json.NewDecoder(bytes.NewReader(data)).Decode(m)
}

// Clone
func (r *repo) Clone() osin.Storage {
	// NOTICE(marius): osin, uses this before saving the Authorization data, and it fails if the database
	// is not closed. This is why the tuneQuery journal_mode = WAL is needed.
	r.Close()
	db, err := New(Config{Path: filepath.Dir(r.path), LogFn: r.logFn, ErrFn: r.errFn})
	if err != nil {
		r.errFn("unable to clone sqlite repository: %+s", err)
	}
	db.cache = r.cache
	return db
}

// Close
func (r *repo) Close() {
	if r.conn == nil {
		return
	}
	if err := r.conn.Close(); err != nil {
		r.errFn("connection close err: %+s", err)
	}
	r.conn = nil
}

const getClients = "SELECT code, secret, redirect_uri, extra FROM clients;"

// ListClients
func (r *repo) ListClients() ([]osin.Client, error) {
	if err := r.Open(); err != nil {
		return nil, err
	}
	defer r.Close()

	result := make([]osin.Client, 0)

	ctx, _ := context.WithTimeout(context.Background(), defaultTimeout)
	rows, err := r.conn.QueryContext(ctx, getClients)
	if errors.Is(err, sql.ErrNoRows) || errors.Is(rows.Err(), sql.ErrNoRows) {
		return nil, errors.NewNotFound(err, "No clients found")
	} else if err != nil {
		r.errFn("Error listing clients: %+s", err)
		return result, errors.Annotatef(err, "Unable to load clients")
	}
	defer rows.Close()

	for rows.Next() {
		c := new(osin.DefaultClient)
		err = rows.Scan(&c.Id, &c.Secret, &c.RedirectUri, &c.UserData)
		if err != nil {
			continue
		}
		result = append(result, c)
	}

	return result, err
}

const getClientSQL = "SELECT code, secret, redirect_uri, extra FROM clients WHERE code=?;"

func errClientNotFound(err error) error {
	if err == nil {
		return errors.NotFoundf("Client could not be found")
	}
	return errors.NewNotFound(err, "Client could not be found")
}
func getClient(conn *sql.DB, ctx context.Context, id string) (osin.Client, error) {
	rows, err := conn.QueryContext(ctx, getClientSQL, id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errClientNotFound(err)
		}
		//s.errFn(log.Ctx{"code": id, "table": "client", "operation": "select"}, "%s", err)
		return nil, errors.Annotatef(err, "Unable to load client")
	}
	defer rows.Close()

	for rows.Next() {
		c := new(osin.DefaultClient)
		err = rows.Scan(&c.Id, &c.Secret, &c.RedirectUri, &c.UserData)
		if err != nil {
			return nil, errors.Annotatef(err, "Unable to load client information")
		}
		return c, nil
	}
	return nil, errClientNotFound(nil)
}

// GetClient
func (r *repo) GetClient(id string) (osin.Client, error) {
	if id == "" {
		return nil, errors.NotFoundf("Empty client id")
	}
	if err := r.Open(); err != nil {
		return nil, err
	}
	defer r.Close()

	ctx, _ := context.WithTimeout(context.Background(), defaultTimeout)

	return getClient(r.conn, ctx, id)
}

const updateClient = "UPDATE clients SET (secret, redirect_uri, extra) = (?, ?, ?) WHERE code=?"
const updateClientNoExtra = "UPDATE clients SET (secret, redirect_uri) = (?, ?) WHERE code=?"

var nilClientErr = errors.Newf("nil client")

// UpdateClient
func (r *repo) UpdateClient(c osin.Client) error {
	if c == nil {
		return nilClientErr
	}
	if err := r.Open(); err != nil {
		return err
	}
	defer r.Close()

	data, err := assertToBytes(c.GetUserData())
	if err != nil {
		r.errFn("Client id %s: %+s", c.GetId(), err)
		return err
	}

	params := []interface{}{
		c.GetSecret(),
		c.GetRedirectUri(),
	}
	q := updateClientNoExtra
	if data != nil {
		q = updateClient
		params = append(params, interface{}(data))
	}
	params = append(params, c.GetId())

	ctx, _ := context.WithTimeout(context.Background(), defaultTimeout)
	if _, err := r.conn.ExecContext(ctx, q, params...); err != nil {
		r.errFn("Failed to update client id %s: %+s", c.GetId(), err)
		return errors.Annotatef(err, "Unable to update client")
	}
	return nil
}

const createClientNoExtra = "INSERT INTO clients (code, secret, redirect_uri) VALUES (?, ?, ?)"
const createClient = "INSERT INTO clients (code, secret, redirect_uri, extra) VALUES (?, ?, ?, ?)"

// CreateClient
func (r *repo) CreateClient(c osin.Client) error {
	if c == nil {
		return nilClientErr
	}
	if err := r.Open(); err != nil {
		return err
	}
	defer r.Close()

	data, err := assertToBytes(c.GetUserData())
	if err != nil {
		r.errFn("Client id %s: %+s", c.GetId(), err)
		return err
	}
	params := []interface{}{
		c.GetId(),
		c.GetSecret(),
		c.GetRedirectUri(),
	}
	q := createClientNoExtra
	if data != nil {
		q = createClient
		params = append(params, interface{}(data))
	}

	ctx, _ := context.WithTimeout(context.Background(), defaultTimeout)
	if _, err = r.conn.ExecContext(ctx, q, params...); err != nil {
		r.errFn("Error inserting client id %s: %+s", c.GetId(), err)
		return errors.Annotatef(err, "Unable to save new client")
	}
	return nil
}

const removeClient = "DELETE FROM clients WHERE code=?"

// RemoveClient
func (r *repo) RemoveClient(id string) error {
	if err := r.Open(); err != nil {
		return err
	}
	defer r.Close()

	ctx, _ := context.WithTimeout(context.Background(), defaultTimeout)
	if _, err := r.conn.ExecContext(ctx, removeClient, id); err != nil {
		r.errFn("Failed deleting client id %s: %+s", id, err)
		return errors.Annotatef(err, "Unable to remove client")
	}
	r.logFn("Successfully removed client %s", id)
	return nil
}

const saveAuthorizeNoExtra = `INSERT INTO authorize (client, code, expires_in, scope, redirect_uri, state, created_at)
	VALUES (?, ?, ?, ?, ?, ?, ?);
`
const saveAuthorize = `INSERT INTO authorize (client, code, expires_in, scope, redirect_uri, state, created_at, extra)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?);`

// SaveAuthorize saves authorize data.
func (r *repo) SaveAuthorize(data *osin.AuthorizeData) error {
	if data == nil {
		return errors.Newf("invalid nil authorize to save")
	}
	if err := r.Open(); err != nil {
		return err
	}
	defer r.Close()
	extra, err := assertToBytes(data.UserData)
	if err != nil {
		r.errFn("Authorize id %s: %+s", data.Client.GetId(), err)
		return err
	}

	q := saveAuthorizeNoExtra
	var params = []interface{}{
		data.Client.GetId(),
		data.Code,
		data.ExpiresIn,
		data.Scope,
		data.RedirectUri,
		data.State,
		data.CreatedAt.UTC().Format(time.RFC3339),
	}
	if extra != nil {
		q = saveAuthorize
		params = append(params, extra)
	}

	ctx, _ := context.WithTimeout(context.Background(), defaultTimeout)
	if _, err = r.conn.ExecContext(ctx, q, params...); err != nil {
		r.errFn("Failed to insert authorize data for client id %s, code %s: %+s", data.Client.GetId(), data.Code, err)
		return errors.Annotatef(err, "Unable to save authorize token")
	}
	return nil
}

const loadAuthorizeSQL = "SELECT client, code, expires_in, scope, redirect_uri, state, created_at, extra FROM authorize WHERE code=? LIMIT 1"

func loadAuthorize(conn *sql.DB, ctx context.Context, code string) (*osin.AuthorizeData, error) {
	var a *osin.AuthorizeData

	rows, err := conn.QueryContext(ctx, loadAuthorizeSQL, code)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, errors.NotFoundf("Unable to load authorize token")
	} else if err != nil {
		//s.errFn(log.Ctx{"code": code, "table": "authorize", "operation": "select"}, err.Error())
		return nil, errors.Annotatef(err, "Unable to load authorize token")
	}
	defer rows.Close()

	var client string
	for rows.Next() {
		a = new(osin.AuthorizeData)
		var createdAt string
		err = rows.Scan(&client, &a.Code, &a.ExpiresIn, &a.Scope, &a.RedirectUri, &a.State, &createdAt, &a.UserData)
		if err != nil {
			return nil, errors.Annotatef(err, "unable to load authorize data")
		}

		a.CreatedAt, _ = time.Parse(time.RFC3339Nano, createdAt)
		if !a.CreatedAt.IsZero() && a.ExpireAt().Before(time.Now().UTC()) {
			//s.errFn(log.Ctx{"code": code}, err.Error())
			return nil, errors.Errorf("Token expired at %s.", a.ExpireAt().String())
		}
		break
	}

	if len(client) > 0 {
		a.Client, _ = getClient(conn, ctx, client)
	}

	return a, nil
}

// LoadAuthorize looks up AuthorizeData by a code.
func (r *repo) LoadAuthorize(code string) (*osin.AuthorizeData, error) {
	if code == "" {
		return nil, errors.Newf("Empty authorize code")
	}
	if err := r.Open(); err != nil {
		return nil, err
	}
	defer r.Close()

	ctx, _ := context.WithTimeout(context.Background(), defaultTimeout)
	return loadAuthorize(r.conn, ctx, code)
}

const removeAuthorize = "DELETE FROM authorize WHERE code=?"

// RemoveAuthorize revokes or deletes the authorization code.
func (r *repo) RemoveAuthorize(code string) error {
	if err := r.Open(); err != nil {
		return err
	}
	defer r.Close()

	ctx, _ := context.WithTimeout(context.Background(), defaultTimeout)
	if _, err := r.conn.ExecContext(ctx, removeAuthorize, code); err != nil {
		r.errFn("Failed deleting authorize data code %s: %+s", code, err)
		return errors.Annotatef(err, "Unable to delete authorize token")
	}
	r.logFn("Successfully removed authorization token %s", code)
	return nil
}

const saveAccess = `INSERT INTO access (client, authorize, previous, token, refresh_token, expires_in, scope, redirect_uri, created_at, extra) 
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

var WriteTxn = sql.TxOptions{Isolation: sql.LevelWriteCommitted, ReadOnly: false}

// SaveAccess writes AccessData.
func (r *repo) SaveAccess(data *osin.AccessData) error {
	prev := ""
	authorizeData := &osin.AuthorizeData{}

	if data.AccessData != nil {
		prev = data.AccessData.AccessToken
	}

	if data.AuthorizeData != nil {
		authorizeData = data.AuthorizeData
	}

	extra, err := assertToBytes(data.UserData)
	if err != nil {
		r.errFn("Authorize id %s: %+s", data.Client.GetId(), err)
		return err
	}
	if err = r.Open(); err != nil {
		return err
	}
	defer r.Close()

	ctx, _ := context.WithTimeout(context.TODO(), defaultTimeout)
	params := []interface{}{
		data.Client.GetId(),
		authorizeData.Code,
		prev,
		data.AccessToken,
		data.RefreshToken,
		data.ExpiresIn,
		data.Scope,
		data.RedirectUri,
		data.CreatedAt.UTC().Format(time.RFC3339Nano),
		extra,
	}

	if data.Client == nil {
		return errors.Newf("data.Client must not be nil")
	}

	_, err = r.conn.ExecContext(ctx, saveAccess, params...)
	if err != nil {
		r.errFn("Failed saving access data for client id %s: %+s", data.Client.GetId(), err)
		return errors.Annotatef(err, "Unable to create access token")
	}
	if len(data.RefreshToken) > 0 {
		if err = r.saveRefresh(ctx, data.RefreshToken, data.AccessToken); err != nil {
			r.errFn("Failed saving refresh data for client id %s: %+s", data.Client.GetId(), err)
			return err
		}
	}

	return nil
}

const loadAccessSQL = `SELECT client, authorize, previous, token, refresh_token, expires_in, scope, redirect_uri, created_at, extra 
	FROM access WHERE token=? LIMIT 1`

func loadAccess(conn *sql.DB, ctx context.Context, code string) (*osin.AccessData, error) {
	var a *osin.AccessData
	rows, err := conn.QueryContext(ctx, loadAccessSQL, code)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, errors.NewNotFound(err, "Unable to load access token")
	} else if err != nil {
		return nil, errors.Annotatef(err, "Unable to load access token")
	}
	defer rows.Close()

	var createdAt string
	var client, authorize, prev sql.NullString
	for rows.Next() {
		a = new(osin.AccessData)
		err = rows.Scan(&client, &authorize, &prev, &a.AccessToken, &a.RefreshToken, &a.ExpiresIn, &a.RedirectUri,
			&a.Scope, &createdAt, &a.UserData)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil, errors.NewNotFound(err, "Unable to load authorize data")
			}
			return nil, errors.Annotatef(err, "unable to load authorize data")
		}

		a.CreatedAt, _ = time.Parse(time.RFC3339Nano, createdAt)
		if !a.CreatedAt.IsZero() && a.ExpireAt().Before(time.Now().UTC()) {
			//s.errFn(log.Ctx{"code": code}, err.Error())
			return nil, errors.Errorf("Token expired at %s.", a.ExpireAt().String())
		}
		break
	}

	if client.Valid {
		a.Client, _ = getClient(conn, ctx, client.String)
	}
	if authorize.Valid {
		a.AuthorizeData, _ = loadAuthorize(conn, ctx, authorize.String)
	}
	if prev.Valid {
		a.AccessData, _ = loadAccess(conn, ctx, prev.String)
	}

	return a, nil
}

var ReadOnlyTxn = sql.TxOptions{
	Isolation: sql.LevelReadUncommitted,
	ReadOnly:  true,
}

// LoadAccess retrieves access data by token. Client information MUST be loaded together.
func (r *repo) LoadAccess(code string) (*osin.AccessData, error) {
	if code == "" {
		return nil, errors.Newf("Empty access code")
	}
	if err := r.Open(); err != nil {
		return nil, err
	}
	defer r.Close()

	ctx, _ := context.WithTimeout(context.Background(), defaultTimeout)
	return loadAccess(r.conn, ctx, code)
}

const removeAccess = "DELETE FROM access WHERE token=?"

// RemoveAccess revokes or deletes an AccessData.
func (r *repo) RemoveAccess(code string) error {
	if err := r.Open(); err != nil {
		return err
	}
	defer r.Close()

	ctx, _ := context.WithTimeout(context.Background(), defaultTimeout)
	_, err := r.conn.ExecContext(ctx, removeAccess, code)
	if err != nil {
		r.errFn("Failed removing access code %s: %+s", code, err)
		return errors.Annotatef(err, "Unable to remove access token")
	}
	r.logFn("Successfully removed access token %s", code)
	return nil
}

const loadRefresh = "SELECT access_token FROM refresh WHERE token=? LIMIT 1"

// LoadRefresh retrieves refresh AccessData. Client information MUST be loaded together.
func (r *repo) LoadRefresh(code string) (*osin.AccessData, error) {
	if code == "" {
		return nil, errors.Newf("Empty refresh code")
	}
	if err := r.Open(); err != nil {
		return nil, err
	}
	defer r.Close()

	ctx, _ := context.WithTimeout(context.Background(), defaultTimeout)
	var access sql.NullString
	err := r.conn.QueryRowContext(ctx, loadRefresh, code).Scan(&access)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errors.NewNotFound(err, "Unable to load refresh token")
		}
		return nil, errors.Annotatef(err, "Unable to load refresh token")
	}

	return loadAccess(r.conn, ctx, access.String)
}

const removeRefresh = "DELETE FROM refresh WHERE token=?"

// RemoveRefresh revokes or deletes refresh AccessData.
func (r *repo) RemoveRefresh(code string) error {
	if err := r.Open(); err != nil {
		return err
	}
	defer r.Close()

	ctx, _ := context.WithTimeout(context.Background(), defaultTimeout)

	_, err := r.conn.ExecContext(ctx, removeRefresh, code)
	if err != nil {
		r.errFn("Failed removing refresh code %s: %+s", code, err)
		return errors.Annotatef(err, "Unable to remove refresh token")
	}
	r.logFn("Successfully removed refresh token %s", code)
	return nil
}

const saveRefresh = "INSERT INTO refresh (token, access_token) VALUES (?, ?)"

func (r *repo) saveRefresh(ctx context.Context, refresh, access string) (err error) {
	if _, err = r.conn.ExecContext(ctx, saveRefresh, refresh, access); err != nil {
		return errors.Annotatef(err, "Unable to save refresh token")
	}
	return nil
}

func assertToBytes(in interface{}) ([]byte, error) {
	var ok bool
	var data string
	if in == nil {
		return nil, nil
	} else if data, ok = in.(string); ok {
		return []byte(data), nil
	} else if byt, ok := in.([]byte); ok {
		return byt, nil
	} else if byt, ok := in.(json.RawMessage); ok {
		return byt, nil
	} else if str, ok := in.(fmt.Stringer); ok {
		return []byte(str.String()), nil
	}
	return nil, errors.Errorf(`Could not assert "%v" to string`, in)
}
