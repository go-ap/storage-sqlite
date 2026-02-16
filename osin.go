package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	vocab "github.com/go-ap/activitypub"
	"github.com/go-ap/errors"
	"github.com/openshift/osin"
)

const defaultTimeout = 1000 * time.Millisecond

const (
	createClientTable = `CREATE TABLE IF NOT EXISTS "clients"(
	"code" varchar constraint client_code_pkey PRIMARY KEY,
	"secret" varchar NOT NULL,
	"redirect_uri" varchar NOT NULL,
	"extra" BLOB DEFAULT NULL
);
`

	createAuthorizeTable = `CREATE TABLE IF NOT EXISTS "authorize" (
	"client" varchar REFERENCES clients(code),
	"code" varchar constraint authorize_code_pkey PRIMARY KEY,
	"expires_in" INTEGER,
	"scope" BLOB,
	"redirect_uri" varchar NOT NULL,
	"state" BLOB,
	"code_challenge" varchar DEFAULT NULL,
	"code_challenge_method" varchar DEFAULT NULL,
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
	//r.Close()
	//db, err := New(Config{Path: filepath.Dir(r.path), LogFn: r.logFn, ErrFn: r.errFn})
	//if err != nil {
	//	r.errFn("unable to clone sqlite repository: %+s", err)
	//}
	//db.cache = r.cache
	//return db
	return r
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
	if r == nil || r.conn == nil {
		return nil, errNotOpen
	}

	result := make([]osin.Client, 0)

	ctx, cancelFn := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancelFn()

	rows, err := r.conn.QueryContext(ctx, getClients)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errors.NewNotFound(err, "No clients found")
		}
		r.errFn("Error listing clients: %+s", err)
		return result, errors.Annotatef(err, "Unable to load clients")
	}

	if errors.Is(rows.Err(), sql.ErrNoRows) {
		return nil, errors.NewNotFound(err, "No clients found")
	}
	defer rows.Close()

	for rows.Next() {
		c := new(cl)
		err = rows.Scan(&c.Id, &c.Secret, &c.RedirectUri, &c.UserData)
		if err != nil {
			return nil, err
		}
		result = append(result, c)
	}

	if len(result) == 0 {
		return nil, nil
	}

	return result, nil
}

const getClientSQL = "SELECT code, secret, redirect_uri, extra FROM clients WHERE code = ?"

func errClientNotFound(err error) error {
	if err == nil {
		return errors.NotFoundf("Client could not be found")
	}
	return errors.NewNotFound(err, "Client could not be found")
}

type cl struct {
	Id          string
	Secret      string
	RedirectUri string
	UserData    string
}

func (c cl) GetId() string {
	return c.Id
}

func (c cl) GetSecret() string {
	return c.Secret
}

func (c cl) GetRedirectUri() string {
	return c.RedirectUri
}

func (c cl) GetUserData() any {
	return c.UserData
}

var _ osin.Client = cl{}

func getClient(conn *sql.DB, ctx context.Context, code string) (osin.Client, error) {
	row := conn.QueryRowContext(ctx, getClientSQL, code)
	if err := row.Err(); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errClientNotFound(err)
		}
		return nil, errors.Annotatef(err, "Unable to load client")
	}

	c := new(cl)
	var userData sql.NullString
	if err := row.Scan(&c.Id, &c.Secret, &c.RedirectUri, &userData); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errClientNotFound(err)
		}
		return nil, errors.Annotatef(err, "Unable to load client information")
	}

	if userData.Valid {
		c.UserData = userData.String
	}
	return c, nil
}

// GetClient
func (r *repo) GetClient(code string) (osin.Client, error) {
	if r == nil || r.conn == nil {
		return nil, errNotOpen
	}
	if code == "" {
		return nil, errors.NotFoundf("Empty client code")
	}

	ctx, cancelFn := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancelFn()

	return getClient(r.conn, ctx, code)
}

const updateClient = "UPDATE clients SET (secret, redirect_uri, extra) = (?, ?, ?) WHERE code=?"

var nilClientErr = errors.Newf("nil client")

// UpdateClient
func (r *repo) UpdateClient(c osin.Client) error {
	if r == nil || r.conn == nil {
		return errNotOpen
	}
	if c == nil {
		return nilClientErr
	}

	data, err := assertToBytes(c.GetUserData())
	if err != nil {
		r.errFn("Client id %s: %+s", c.GetId(), err)
		return err
	}

	params := []interface{}{
		c.GetSecret(),
		c.GetRedirectUri(),
		data,
	}
	params = append(params, c.GetId())

	ctx, _ := context.WithTimeout(context.Background(), defaultTimeout)
	if _, err := r.conn.ExecContext(ctx, updateClient, params...); err != nil {
		r.errFn("Failed to update client id %s: %+s", c.GetId(), err)
		return errors.Annotatef(err, "Unable to update client")
	}
	return nil
}

const createClient = "INSERT INTO clients (code, secret, redirect_uri, extra) VALUES (?, ?, ?, ?)"

// CreateClient
func (r *repo) CreateClient(c osin.Client) error {
	if r == nil || r.conn == nil {
		return errNotOpen
	}
	if c == nil {
		return nilClientErr
	}

	data, err := assertToBytes(c.GetUserData())
	if err != nil {
		r.errFn("Client id %s: %+s", c.GetId(), err)
		return err
	}
	params := []interface{}{
		c.GetId(),
		c.GetSecret(),
		c.GetRedirectUri(),
		data,
	}

	ctx, _ := context.WithTimeout(context.Background(), defaultTimeout)
	if _, err = r.conn.ExecContext(ctx, createClient, params...); err != nil {
		r.errFn("Error inserting client id %s: %+s", c.GetId(), err)
		return errors.Annotatef(err, "Unable to save new client")
	}
	return nil
}

const removeClient = "DELETE FROM clients WHERE code=?"

// RemoveClient
func (r *repo) RemoveClient(id string) error {
	if r == nil || r.conn == nil {
		return errNotOpen
	}
	ctx, _ := context.WithTimeout(context.Background(), defaultTimeout)
	if _, err := r.conn.ExecContext(ctx, removeClient, id); err != nil {
		r.errFn("Failed deleting client id %s: %+s", id, err)
		return errors.Annotatef(err, "Unable to remove client")
	}
	r.logFn("Successfully removed client %s", id)
	return nil
}

const saveAuthorize = `INSERT INTO authorize (client, code, expires_in, scope, redirect_uri, state, created_at, extra,
code_challenge, code_challenge_method)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`

// SaveAuthorize saves authorize data.
func (r *repo) SaveAuthorize(data *osin.AuthorizeData) error {
	if r == nil || r.conn == nil {
		return errNotOpen
	}
	if data == nil {
		return errors.Newf("unable to save nil authorization data")
	}

	extra, err := assertToBytes(data.UserData)
	if err != nil {
		r.errFn("Authorize id %s: %+s", data.Client.GetId(), err)
		return err
	}

	var params = []interface{}{
		data.Client.GetId(),
		data.Code,
		data.ExpiresIn,
		data.Scope,
		data.RedirectUri,
		data.State,
		data.CreatedAt.UTC().Format(time.RFC3339),
		extra,
	}
	if data.CodeChallengeMethod != "" {
		params = append(params, data.CodeChallenge, data.CodeChallengeMethod)
	} else {
		params = append(params, nil, nil)
	}

	ctx, cancelFn := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancelFn()

	if _, err = r.conn.ExecContext(ctx, saveAuthorize, params...); err != nil {
		r.errFn("Failed to insert authorize data for client id %s, code %s: %+s", data.Client.GetId(), data.Code, err)
		return errors.Annotatef(err, "Unable to save authorize token")
	}
	return nil
}

const loadAuthorizeSQL = `SELECT
    a.code a_code, expires_in, scope, a.redirect_uri a_redirect_uri, state, created_at, a.extra a_extra,
    a.code_challenge a_code_challenge, a.code_challenge_method a_code_challenge_method,
    c.code c_code, c.redirect_uri c_redirect_uri, c.secret, c.extra c_extra
FROM authorize a
INNER JOIN clients c ON a.client = c.code
WHERE a.code = ? LIMIT 1;`

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

	for rows.Next() {
		a = new(osin.AuthorizeData)
		c := new(osin.DefaultClient)
		var aUserData sql.NullString
		var aCodeChallenge sql.NullString
		var aCodeChallengeMethod sql.NullString
		var cUserData sql.NullString
		var createdAt string
		/*
			a.code a_code, expires_in, scope, a.redirect_uri a_redirect_uri, state, created_at, a.extra a_extra,
			a.code_challenge a_code_challenge, a.code_challenge_method a_code_challenge_method,
			c.code c_code, c.redirect_uri c_redirect_uri, c.secret, c.extra c_extra
		*/
		err = rows.Scan(&a.Code, &a.ExpiresIn, &a.Scope, &a.RedirectUri, &a.State, &createdAt, &aUserData,
			&aCodeChallenge, &aCodeChallengeMethod,
			&c.Id, &c.RedirectUri, &c.Secret, &cUserData)
		if err != nil {
			return nil, errors.Annotatef(err, "unable to load authorize data")
		}

		a.CreatedAt, _ = time.Parse(time.RFC3339Nano, createdAt)
		if !a.CreatedAt.IsZero() && a.ExpireAt().Before(time.Now().UTC()) {
			return nil, errors.Errorf("Token expired at %s.", a.ExpireAt().String())
		}
		if aUserData.Valid {
			a.UserData = vocab.IRI(aUserData.String)
		}
		if cUserData.Valid {
			c.UserData = cUserData.String
		}
		if aCodeChallenge.Valid {
			a.CodeChallenge = aCodeChallenge.String
		}
		if aCodeChallengeMethod.Valid {
			a.CodeChallengeMethod = aCodeChallengeMethod.String
		}
		if len(c.Id) > 0 {
			a.Client = c
		}
		break
	}
	if a == nil {
		return nil, errors.NotFoundf("unable to load authorize data")
	}

	return a, nil
}

// LoadAuthorize looks up AuthorizeData by a code.
func (r *repo) LoadAuthorize(code string) (*osin.AuthorizeData, error) {
	if r == nil || r.conn == nil {
		return nil, errNotOpen
	}
	if code == "" {
		return nil, errors.Newf("Empty authorize code")
	}

	ctx, cancelFn := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancelFn()

	return loadAuthorize(r.conn, ctx, code)
}

const removeAuthorize = "DELETE FROM authorize WHERE code=?"

// RemoveAuthorize revokes or deletes the authorization code.
func (r *repo) RemoveAuthorize(code string) error {
	if r == nil || r.conn == nil {
		return errNotOpen
	}
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

// SaveAccess writes AccessData.
func (r *repo) SaveAccess(data *osin.AccessData) error {
	if r == nil || r.conn == nil {
		return errNotOpen
	}
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

	ctx, cancelFn := context.WithTimeout(context.TODO(), defaultTimeout)
	defer cancelFn()

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

const loadAccessSQL = `SELECT 
	acc.token acc_token, acc.refresh_token acc_refresh_token, acc.expires_in acc_expires_in, acc.scope acc_scope, acc.redirect_uri acc_redirect_uri, acc.created_at acc_created_at, acc.extra acc_extra, acc.previous as acc_previous,
	auth.code auth_code, auth.expires_in auth_expires_in,  auth.scope auth_scope, auth.redirect_uri auth_redirect_uri, auth.state auth_state, auth.created_at auth_created_at, auth.extra auth_extra,
	auth.code_challenge auth_code_challenge, auth.code_challenge_method auth_code_challenge_method,
	c.code c_code, c.redirect_uri c_redirect_uri, c.secret, c.extra c_extra
	FROM access acc
	INNER JOIN clients c ON acc.client = c.code
	LEFT JOIN authorize auth ON acc.authorize = auth.code
WHERE acc.token = ? LIMIT 1`

func loadAccess(conn *sql.DB, ctx context.Context, code string, loadDeps bool) (*osin.AccessData, error) {
	rows, err := conn.QueryContext(ctx, loadAccessSQL, code)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errors.NewNotFound(err, "Unable to load access token")
		}
		return nil, errors.Annotatef(err, "Unable to load access token")
	}
	defer rows.Close()

	for rows.Next() {
		acc := new(osin.AccessData)
		c := new(osin.DefaultClient)
		auth := new(osin.AuthorizeData)
		var accUserData, cUserData, accPrevious sql.NullString
		var accCreatedAt string
		var authCode, authScope, authUserData, authRedirectUri, authState, authCreatedAt, authCodeChallenge, authCodeChallengeMethod sql.NullString
		var authExpiresIn sql.NullInt32
		/*
			acc.token acc_token, acc.refresh_token acc_refresh_token, acc.expires_in acc_expires_in, acc.scope acc_scope, acc.redirect_uri acc_redirect_uri, acc.created_at acc_created_at, acc.extra acc_extra,
			auth.code auth_code, auth.expires_in auth_expires_in,  auth.scope auth_scope, auth.redirect_uri auth_redirect_uri, auth.state auth_state, auth.created_at auth_created_at, auth.extra auth_extra,
			c.code c_code, c.redirect_uri c_redirect_uri, c.secret, c.extra c_extra,
		*/
		err = rows.Scan(&acc.AccessToken, &acc.RefreshToken, &acc.ExpiresIn, &acc.Scope, &acc.RedirectUri, &accCreatedAt, &accUserData, &accPrevious,
			&authCode, &authExpiresIn, &authScope, &authRedirectUri, &authState, &authCreatedAt, &authUserData, &authCodeChallenge, &authCodeChallengeMethod,
			&c.Id, &c.RedirectUri, &c.Secret, &cUserData,
		)

		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil, errors.NewNotFound(err, "Unable to load access data")
			}
			return nil, errors.Annotatef(err, "unable to load access data")
		}

		acc.CreatedAt, _ = time.Parse(time.RFC3339Nano, accCreatedAt)
		if !acc.CreatedAt.IsZero() && acc.ExpireAt().Before(time.Now().UTC()) {
			return nil, errors.Errorf("Token expired at %s.", acc.ExpireAt().String())
		}
		if authCreatedAt.Valid {
			auth.CreatedAt, _ = time.Parse(time.RFC3339Nano, authCreatedAt.String)
		}
		if authRedirectUri.Valid {
			auth.RedirectUri = authRedirectUri.String
		}

		if accUserData.Valid {
			acc.UserData = vocab.IRI(accUserData.String)
		}
		if authUserData.Valid {
			auth.UserData = vocab.IRI(authUserData.String)
		}
		if authCode.Valid {
			auth.Code = authCode.String
		}
		if authScope.Valid {
			auth.Scope = authScope.String
		}
		if authState.Valid {
			auth.State = authState.String
		}
		if authExpiresIn.Valid {
			auth.ExpiresIn = authExpiresIn.Int32
		}
		if authCodeChallengeMethod.Valid {
			auth.CodeChallengeMethod = authCodeChallengeMethod.String
		}
		if authCodeChallenge.Valid {
			auth.CodeChallenge = authCodeChallenge.String
		}
		if cUserData.Valid {
			c.UserData = cUserData.String
		}
		if loadDeps {
			if accPrevious.Valid {
				prev, _ := loadAccess(conn, ctx, accPrevious.String, false)
				if prev != nil {
					acc.AccessData = prev
				}
			}
			if len(auth.Code) > 0 {
				acc.AuthorizeData = auth
			}
		}
		if len(c.Id) > 0 {
			acc.Client = c
			if acc.AuthorizeData != nil {
				acc.AuthorizeData.Client = c
			}
		}
		return acc, nil
	}
	return nil, errors.NotFoundf("unable to load access data")
}

var ReadOnlyTxn = sql.TxOptions{
	Isolation: sql.LevelReadUncommitted,
	ReadOnly:  true,
}

// LoadAccess retrieves access data by token. Client information MUST be loaded together.
func (r *repo) LoadAccess(code string) (*osin.AccessData, error) {
	if r == nil || r.conn == nil {
		return nil, errNotOpen
	}
	if code == "" {
		return nil, errors.Newf("Empty access code")
	}

	ctx, cancelFn := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancelFn()

	return loadAccess(r.conn, ctx, code, true)
}

const removeAccess = "DELETE FROM access WHERE token=?"

// RemoveAccess revokes or deletes an AccessData.
func (r *repo) RemoveAccess(code string) error {
	if r == nil || r.conn == nil {
		return errNotOpen
	}
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
	if r == nil || r.conn == nil {
		return nil, errNotOpen
	}
	if code == "" {
		return nil, errors.Newf("Empty refresh code")
	}

	ctx, _ := context.WithTimeout(context.Background(), defaultTimeout)
	var access sql.NullString
	err := r.conn.QueryRowContext(ctx, loadRefresh, code).Scan(&access)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errors.NewNotFound(err, "Unable to load refresh token")
		}
		return nil, errors.Annotatef(err, "Unable to load refresh token")
	}

	return loadAccess(r.conn, ctx, access.String, true)
}

const removeRefresh = "DELETE FROM refresh WHERE token=?"

// RemoveRefresh revokes or deletes refresh AccessData.
func (r *repo) RemoveRefresh(code string) error {
	if r == nil || r.conn == nil {
		return errNotOpen
	}
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
