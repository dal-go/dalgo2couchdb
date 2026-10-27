// Package dalgo2couchdb provides a bounded, read-only DALgo adapter for
// native documents in one existing CouchDB database.
//
// It reads application documents as they are stored: the CouchDB _id is the
// DALgo record key and _rev is returned as document metadata. It does not
// require or write a DALgo envelope. Structured queries are translated to a
// bounded subset of CouchDB Mango; raw URLs, management endpoints, writes,
// joins and aggregation are not exposed.
package dalgo2couchdb

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net"
	"net/http"
	"net/textproto"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/record"
)

const (
	adapterName    = "couchdb"
	adapterVersion = "0.1.0"
)

// Config configures one adapter instance. Exactly one existing CouchDB
// database is addressed; the logical collection may alias that database but
// cannot route to another one.
type Config struct {
	// DatabaseURL is the HTTPS URL of one existing database, for example
	// https://couch.example.com:6984/orders. It must not contain credentials,
	// a query or a fragment. Plain HTTP is accepted only for a loopback host
	// when AllowLoopbackHTTP is true.
	DatabaseURL string

	// Collection is the logical DALgo collection name. It defaults to the
	// CouchDB database name and is the only collection queries may target.
	Collection string

	// Headers is called for every request with that request's context. It is
	// where credentials (for example an Authorization header) belong. Names
	// and values must be CR/LF-safe; hop-by-hop and protocol headers are
	// rejected. The provider's error text is not returned to the caller.
	Headers HeaderProvider

	// HTTPClient optionally supplies the client. The adapter copies it, blocks
	// redirects on the copy, and never mutates the caller's client.
	HTTPClient *http.Client

	// AllowLoopbackHTTP permits plain HTTP only for localhost, 127.0.0.1 and
	// ::1. It is a development option; production URLs must use HTTPS.
	AllowLoopbackHTTP bool

	// MaxRows is the largest accepted structured-query limit. Default 1000.
	MaxRows int
	// MaxRowsPerPage bounds one Mango request. Default 100.
	MaxRowsPerPage int
	// MaxPages bounds how many Mango pages one operation may request.
	// Default 20.
	MaxPages int
	// MaxOffset is the largest accepted query offset. Default 10000.
	MaxOffset int
	// MaxGetMany is the largest accepted GetMulti batch. Default 100.
	MaxGetMany int
	// MaxRequestBytes bounds one JSON request body. Default 1 MiB.
	MaxRequestBytes int64
	// MaxResponseBytes bounds one HTTP response body. Default 4 MiB.
	MaxResponseBytes int64
	// MaxTotalBytes bounds the response bytes of one operation. Default 16 MiB.
	MaxTotalBytes int64
	// MaxRowBytes bounds one decoded document's JSON size. Default 1 MiB.
	MaxRowBytes int64
	// Timeout bounds one HTTP request. Default 30s.
	Timeout time.Duration
}

// HeaderProvider returns request headers for one request context.
type HeaderProvider func(ctx context.Context) (map[string]string, error)

const (
	defaultMaxRows          = 1000
	defaultMaxRowsPerPage   = 100
	defaultMaxPages         = 20
	defaultMaxOffset        = 10000
	defaultMaxGetMany       = 100
	defaultMaxRequestBytes  = 1 << 20
	defaultMaxResponseBytes = 4 << 20
	defaultMaxTotalBytes    = 16 << 20
	defaultMaxRowBytes      = 1 << 20
	defaultTimeout          = 30 * time.Second

	absoluteMaxRows          = 10000
	absoluteMaxRowsPerPage   = 1000
	absoluteMaxPages         = 100
	absoluteMaxOffset        = 1_000_000
	absoluteMaxGetMany       = 1000
	absoluteMaxRequestBytes  = 64 << 20
	absoluteMaxResponseBytes = 64 << 20
	absoluteMaxTotalBytes    = 64 << 20
	absoluteMaxRowBytes      = 64 << 20
	absoluteMaxTimeout       = 2 * time.Minute
)

var (
	reDatabaseName   = regexp.MustCompile(`^[a-z][a-z0-9_$()+-]{0,237}$`)
	reFieldName      = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,127}$`)
	reReservedHeader = map[string]bool{
		"Host":              true,
		"Content-Length":    true,
		"Transfer-Encoding": true,
		"Connection":        true,
		"Trailer":           true,
		"Upgrade":           true,
		"Proxy-Connection":  true,
		"Te":                true,
		"Keep-Alive":        true,
		"Accept":            true,
		"Content-Type":      true,
		"User-Agent":        true,
	}
)

// database is the dal.Backend over one CouchDB database. It is returned to
// callers only through dal.NewDB, so dal.As reaches Close, MangoPager and
// dbschema.SchemaReader on it.
type database struct {
	dal.ConcurrencyAvailable

	cfg        Config
	baseURL    string
	collection string
	headers    HeaderProvider
	client     *http.Client
	closeIdle  bool
	closeOnce  sync.Once
}

var _ dal.Backend = (*database)(nil)
var _ io.Closer = (*database)(nil)

// NewDB validates cfg and returns a sealed DALgo database. It performs no
// network request. Use dal.As[io.Closer] on the returned DB for Close.
func NewDB(ctx context.Context, cfg Config) (dal.DB, error) {
	if ctx == nil {
		return nil, errors.New("couchdb: context is required")
	}
	d, err := newDatabase(cfg)
	if err != nil {
		return nil, err
	}
	return dal.NewDB(d), nil
}

func newDatabase(cfg Config) (*database, error) {
	baseURL, dbName, err := validateDatabaseURL(cfg.DatabaseURL, cfg.AllowLoopbackHTTP)
	if err != nil {
		return nil, err
	}
	collection := cfg.Collection
	if collection == "" {
		collection = dbName
	}
	if err = validateCollection(collection); err != nil {
		return nil, err
	}
	if cfg.MaxRows, err = boundedPositive(cfg.MaxRows, defaultMaxRows, absoluteMaxRows, "MaxRows"); err != nil {
		return nil, err
	}
	if cfg.MaxRowsPerPage, err = boundedPositive(cfg.MaxRowsPerPage, defaultMaxRowsPerPage, absoluteMaxRowsPerPage, "MaxRowsPerPage"); err != nil {
		return nil, err
	}
	if cfg.MaxPages, err = boundedPositive(cfg.MaxPages, defaultMaxPages, absoluteMaxPages, "MaxPages"); err != nil {
		return nil, err
	}
	if cfg.MaxOffset, err = boundedNonNegative(cfg.MaxOffset, defaultMaxOffset, absoluteMaxOffset, "MaxOffset"); err != nil {
		return nil, err
	}
	if cfg.MaxGetMany, err = boundedPositive(cfg.MaxGetMany, defaultMaxGetMany, absoluteMaxGetMany, "MaxGetMany"); err != nil {
		return nil, err
	}
	if cfg.MaxRequestBytes, err = boundedBytes(cfg.MaxRequestBytes, defaultMaxRequestBytes, absoluteMaxRequestBytes, "MaxRequestBytes"); err != nil {
		return nil, err
	}
	if cfg.MaxResponseBytes, err = boundedBytes(cfg.MaxResponseBytes, defaultMaxResponseBytes, absoluteMaxResponseBytes, "MaxResponseBytes"); err != nil {
		return nil, err
	}
	if cfg.MaxTotalBytes, err = boundedBytes(cfg.MaxTotalBytes, defaultMaxTotalBytes, absoluteMaxTotalBytes, "MaxTotalBytes"); err != nil {
		return nil, err
	}
	if cfg.MaxRowBytes, err = boundedBytes(cfg.MaxRowBytes, defaultMaxRowBytes, absoluteMaxRowBytes, "MaxRowBytes"); err != nil {
		return nil, err
	}
	if cfg.Timeout < 0 {
		return nil, errors.New("couchdb: Timeout must not be negative")
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = defaultTimeout
	}
	if cfg.Timeout > absoluteMaxTimeout {
		return nil, errors.New("couchdb: Timeout exceeds the supported maximum")
	}

	client, closeIdle := httpClient(cfg.HTTPClient)
	return &database{
		cfg:        cfg,
		baseURL:    baseURL,
		collection: collection,
		headers:    cfg.Headers,
		client:     client,
		closeIdle:  closeIdle,
	}, nil
}

func httpClient(supplied *http.Client) (*http.Client, bool) {
	blockRedirects := func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	if supplied == nil {
		transport := defaultTransport()
		return &http.Client{Transport: transport, CheckRedirect: blockRedirects}, true
	}
	client := *supplied
	client.CheckRedirect = blockRedirects
	if client.Transport == nil {
		client.Transport = defaultTransport()
		return &client, true
	}
	return &client, false
}

func defaultTransport() *http.Transport {
	if base, ok := http.DefaultTransport.(*http.Transport); ok {
		transport := base.Clone()
		// Do not depend on ambient proxy configuration for a fixed endpoint.
		transport.Proxy = nil
		return transport
	}
	return &http.Transport{Proxy: nil}
}

func validateDatabaseURL(raw string, allowLoopbackHTTP bool) (string, string, error) {
	if strings.TrimSpace(raw) == "" {
		return "", "", errors.New("couchdb: DatabaseURL is required")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", "", errors.New("couchdb: DatabaseURL is not a valid URL")
	}
	scheme := strings.ToLower(u.Scheme)
	if u.Opaque != "" || u.Host == "" {
		return "", "", errors.New("couchdb: DatabaseURL must be an absolute URL with a host")
	}
	if u.User != nil {
		return "", "", errors.New("couchdb: DatabaseURL must not contain credentials")
	}
	if u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return "", "", errors.New("couchdb: DatabaseURL must not contain a query or a fragment")
	}
	if scheme != "https" && scheme != "http" {
		return "", "", errors.New("couchdb: DatabaseURL must use HTTPS")
	}
	if scheme == "http" {
		if !allowLoopbackHTTP {
			return "", "", errors.New("couchdb: plain HTTP requires AllowLoopbackHTTP")
		}
		if !isLoopbackHost(u.Hostname()) {
			return "", "", errors.New("couchdb: plain HTTP is allowed only for a loopback host")
		}
	}
	path := strings.TrimSuffix(u.Path, "/")
	if path == "" || path == "/" || strings.Contains(strings.TrimPrefix(path, "/"), "/") {
		return "", "", errors.New("couchdb: DatabaseURL must name exactly one existing database")
	}
	dbName := strings.TrimPrefix(path, "/")
	if !reDatabaseName.MatchString(dbName) {
		return "", "", errors.New("couchdb: DatabaseURL database name is not supported")
	}
	if u.EscapedPath() != "/"+dbName && strings.TrimSuffix(u.EscapedPath(), "/") != "/"+dbName {
		return "", "", errors.New("couchdb: DatabaseURL database name must not be percent-encoded")
	}
	base := (&url.URL{Scheme: scheme, Host: u.Host, Path: "/" + dbName}).String()
	return base, dbName, nil
}

func isLoopbackHost(host string) bool {
	host = strings.ToLower(strings.TrimSpace(host))
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func validateCollection(name string) error {
	if name == "" || len(name) > 238 || strings.HasPrefix(name, "_") {
		return errors.New("couchdb: Collection is not supported")
	}
	for _, r := range name {
		if r < 0x21 || r > 0x7e || strings.ContainsRune("/\\?#'\"%", r) {
			return errors.New("couchdb: Collection is not supported")
		}
	}
	return nil
}

func boundedPositive(value, fallback, maximum int, name string) (int, error) {
	if value < 0 {
		return 0, errors.New("couchdb: " + name + " must not be negative")
	}
	if value == 0 {
		return fallback, nil
	}
	if value > maximum {
		return 0, errors.New("couchdb: " + name + " exceeds the supported maximum")
	}
	return value, nil
}

func boundedNonNegative(value, fallback, maximum int, name string) (int, error) {
	if value < 0 {
		return 0, errors.New("couchdb: " + name + " must not be negative")
	}
	if value == 0 {
		return fallback, nil
	}
	if value > maximum {
		return 0, errors.New("couchdb: " + name + " exceeds the supported maximum")
	}
	return value, nil
}

func boundedBytes(value, fallback, maximum int64, name string) (int64, error) {
	if value < 0 {
		return 0, errors.New("couchdb: " + name + " must not be negative")
	}
	if value == 0 {
		return fallback, nil
	}
	if value > maximum {
		return 0, errors.New("couchdb: " + name + " exceeds the supported maximum")
	}
	return value, nil
}

func (d *database) ID() string { return d.collection }

func (d *database) Adapter() dal.Adapter { return dal.NewAdapter(adapterName, adapterVersion) }

// Schema is nil: CouchDB has no declared schema. Use dbschema.SchemaReader,
// reachable through dal.As, for the configured collection and real Mango
// indexes.
func (*database) Schema() dal.Schema { return nil }

// Close releases idle HTTP connections owned by the adapter. It is idempotent
// and leaves a caller-supplied client's transport open.
func (d *database) Close() error {
	d.closeOnce.Do(func() {
		if !d.closeIdle {
			return
		}
		if closer, ok := d.client.Transport.(interface{ CloseIdleConnections() }); ok {
			closer.CloseIdleConnections()
		}
	})
	return nil
}

func (d *database) RunReadonlyTransaction(ctx context.Context, f dal.ROTxWorker, opts ...dal.TransactionOption) error {
	return f(ctx, readTx{database: d, options: dal.NewTransactionOptions(opts...)})
}

func (*database) RunReadwriteTransaction(context.Context, dal.RWTxWorker, ...dal.TransactionOption) error {
	return notSupported("couchdb adapter is read-only")
}

type readTx struct {
	*database
	options dal.TransactionOptions
}

func (t readTx) Options() dal.TransactionOptions { return t.options }

// Get reads one native document by key. The key's collection must be the
// configured logical collection and its ID must be a safe string CouchDB _id.
func (d *database) Get(ctx context.Context, rec record.Record) error {
	if err := d.validateRecordKey(rec); err != nil {
		return err
	}
	var totalBytes int64
	_, err := d.getDocument(ctx, rec, &totalBytes)
	return err
}

// getDocument reads one document and, when totalBytes is non-nil, charges the
// raw response against the operation's running byte budget before decoding.
// totalBytes belongs to the whole operation, not to one document.
func (d *database) getDocument(ctx context.Context, rec record.Record, totalBytes *int64) (int64, error) {
	key := rec.Key()
	status, payload, err := d.request(ctx, http.MethodGet, "/"+url.PathEscape(key.ID.(string)), nil, http.StatusOK, http.StatusNotFound)
	if err != nil {
		return 0, err
	}
	size := int64(len(payload))
	if totalBytes != nil {
		if *totalBytes > d.cfg.MaxTotalBytes-size {
			return 0, ErrResponseTooLarge
		}
		*totalBytes += size
	}
	if status == http.StatusNotFound {
		err = dal.NewErrNotFoundByKey(key, nil)
		rec.SetError(err)
		return size, err
	}
	data, err := d.documentData(payload, key.ID.(string))
	if err != nil {
		return size, err
	}
	if err = assignData(rec.Data(), data); err != nil {
		return size, err
	}
	rec.SetError(nil)
	return size, nil
}

func assignData(target any, data map[string]any) error {
	switch destination := target.(type) {
	case map[string]any:
		for key, value := range data {
			destination[key] = value
		}
		return nil
	case *map[string]any:
		*destination = data
		return nil
	default:
		return record.MapToData(target, data)
	}
}

// Exists reports whether a native document exists. It uses HEAD and does not
// download the document body.
func (d *database) Exists(ctx context.Context, key *record.Key) (bool, error) {
	if err := d.validateKey(key); err != nil {
		return false, err
	}
	status, _, err := d.request(ctx, http.MethodHead, "/"+url.PathEscape(key.ID.(string)), nil, http.StatusOK, http.StatusNotFound)
	if err != nil {
		return false, err
	}
	return status == http.StatusOK, nil
}

// GetMulti reads a bounded batch. All keys are validated before the first
// request, so a malformed key or an oversized batch performs no I/O. The whole
// batch shares one deadline, one response-byte budget and one cardinality
// bound: completing N documents is one operation and cannot restart those
// limits for every request. Records are read in caller order. On failure the
// returned *GetMultiError names the first unread item; earlier records stay
// populated and later records are left untouched and unrequested.
func (d *database) GetMulti(ctx context.Context, records []record.Record) error {
	if ctx == nil {
		return errors.New("couchdb: context is required")
	}
	if len(records) > d.cfg.MaxGetMany {
		return notSupported("GetMulti exceeds MaxGetMany")
	}
	for _, rec := range records {
		if err := d.validateRecordKey(rec); err != nil {
			return err
		}
	}
	batchCtx, cancel := context.WithTimeout(ctx, d.cfg.Timeout)
	defer cancel()
	var totalBytes int64
	for index, rec := range records {
		if err := batchCtx.Err(); err != nil {
			return &GetMultiError{Index: index, Err: safeContextError(batchCtx, err)}
		}
		if _, err := d.getDocument(batchCtx, rec, &totalBytes); err != nil {
			return &GetMultiError{Index: index, Err: err}
		}
	}
	return nil
}

func (d *database) validateRecordKey(rec record.Record) error {
	if rec == nil {
		return unsafeValue("record is nil")
	}
	return d.validateKey(rec.Key())
}

func (d *database) validateKey(key *record.Key) error {
	if key == nil || key.Parent() != nil || key.ID == nil {
		return notSupported("couchdb keys must be one root document ID")
	}
	if key.Collection() != d.collection {
		return notSupported("couchdb queries target only the configured collection")
	}
	id, ok := key.ID.(string)
	if !ok {
		return notSupported("couchdb document IDs are strings")
	}
	return validateRequestedID(id)
}

func validateRequestedID(id string) error {
	if id == "" || len(id) > 512 || !utf8.ValidString(id) || strings.HasPrefix(id, "_") {
		return unsafeValue("document ID is not supported")
	}
	if id == "." || id == ".." || strings.Contains(id, "..") {
		return unsafeValue("document ID is not supported")
	}
	for _, r := range id {
		if r < 0x20 || r == 0x7f {
			return unsafeValue("document ID is not supported")
		}
	}
	return nil
}

func (d *database) documentData(payload []byte, expectedID string) (map[string]any, error) {
	var doc map[string]any
	if err := decodeJSON(payload, &doc); err != nil {
		return nil, err
	}
	if doc == nil {
		return nil, malformed("document is not a JSON object")
	}
	id, ok := doc["_id"].(string)
	if !ok || id == "" || (expectedID != "" && id != expectedID) {
		return nil, malformed("document identity is missing or mismatched")
	}
	if strings.HasPrefix(id, "_") {
		return nil, unsafeValue("management documents are not exposed")
	}
	rev, ok := doc["_rev"].(string)
	if !ok || rev == "" {
		return nil, malformed("document revision is missing")
	}
	delete(doc, "_id")
	doc = convertJSONNumbers(doc).(map[string]any)
	size, err := encodedSize(doc)
	if err != nil {
		return nil, malformed("document is not JSON-encodable")
	}
	if int64(size) > d.cfg.MaxRowBytes {
		return nil, ErrRowTooLarge
	}
	return doc, nil
}

func decodeJSON(payload []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	if err := decoder.Decode(target); err != nil {
		return malformed("response is not valid JSON")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return malformed("response has trailing data")
	}
	return nil
}

func encodedSize(value any) (int, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return 0, err
	}
	return len(encoded), nil
}

func convertJSONNumbers(value any) any {
	switch v := value.(type) {
	case json.Number:
		text := v.String()
		if !strings.ContainsAny(text, ".eE") {
			if n, err := strconv.ParseInt(text, 10, 64); err == nil {
				return n
			}
			if n, err := strconv.ParseUint(text, 10, 64); err == nil && n <= math.MaxInt64 {
				return int64(n)
			}
			return v // preserve an integer wider than int64 exactly
		}
		if n, err := strconv.ParseFloat(text, 64); err == nil && !math.IsInf(n, 0) && !math.IsNaN(n) {
			return n
		}
		return v
	case map[string]any:
		for key, item := range v {
			v[key] = convertJSONNumbers(item)
		}
		return v
	case []any:
		for i, item := range v {
			v[i] = convertJSONNumbers(item)
		}
		return v
	default:
		return v
	}
}

func (d *database) request(ctx context.Context, method, path string, body any, accepted ...int) (int, []byte, error) {
	if ctx == nil {
		return 0, nil, errors.New("couchdb: context is required")
	}
	if !strings.HasPrefix(path, "/") || strings.Contains(path, "..") {
		return 0, nil, unsafeValue("request path is not a fixed relative route")
	}
	headers, err := d.requestHeaders(ctx)
	if err != nil {
		return 0, nil, err
	}
	var payload []byte
	if body != nil {
		payload, err = json.Marshal(body)
		if err != nil {
			return 0, nil, malformed("request body is not JSON-encodable")
		}
		if int64(len(payload)) > d.cfg.MaxRequestBytes {
			return 0, nil, ErrRequestTooLarge
		}
	}
	requestCtx, cancel := context.WithTimeout(ctx, d.cfg.Timeout)
	defer cancel()
	var reader io.Reader
	if payload != nil {
		reader = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(requestCtx, method, d.baseURL+path, reader)
	if err != nil {
		return 0, nil, ErrRequestFailed
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", adapterName+"/"+adapterVersion)
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for name, value := range headers {
		req.Header.Set(name, value)
	}
	resp, err := d.client.Do(req)
	if err != nil {
		return 0, nil, safeContextError(requestCtx, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.ContentLength > d.cfg.MaxResponseBytes {
		return resp.StatusCode, nil, ErrResponseTooLarge
	}
	limited := io.LimitReader(resp.Body, d.cfg.MaxResponseBytes+1)
	respBody, readErr := io.ReadAll(limited)
	if readErr != nil {
		return resp.StatusCode, nil, safeContextError(requestCtx, readErr)
	}
	if int64(len(respBody)) > d.cfg.MaxResponseBytes {
		return resp.StatusCode, nil, ErrResponseTooLarge
	}
	for _, status := range accepted {
		if resp.StatusCode == status {
			return resp.StatusCode, respBody, nil
		}
	}
	if resp.StatusCode == http.StatusNotFound {
		return resp.StatusCode, respBody, ErrDatabaseNotFound
	}
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return resp.StatusCode, respBody, &HTTPError{StatusCode: resp.StatusCode}
	}
	if errorCode(respBody) == "no_usable_index" {
		return resp.StatusCode, respBody, ErrIndexRequired
	}
	return resp.StatusCode, respBody, &HTTPError{StatusCode: resp.StatusCode}
}

func (d *database) requestHeaders(ctx context.Context) (map[string]string, error) {
	if d.headers == nil {
		return nil, nil
	}
	headers, err := d.headers(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return nil, ErrCanceled
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, ErrDeadlineExceeded
		}
		return nil, ErrHeaderProvider
	}
	clean := make(map[string]string, len(headers))
	for name, value := range headers {
		canonical := textproto.CanonicalMIMEHeaderKey(name)
		if canonical == "" || reReservedHeader[canonical] {
			return nil, unsafeValue("request header name is not allowed")
		}
		if len(name) > 256 || len(value) > 4096 || strings.ContainsAny(name+value, "\r\n\x00") {
			return nil, unsafeValue("request header is not CR/LF-safe")
		}
		clean[canonical] = value
	}
	return clean, nil
}

func errorCode(payload []byte) string {
	var envelope struct {
		Error string `json:"error"`
	}
	if len(payload) == 0 || json.Unmarshal(payload, &envelope) != nil {
		return ""
	}
	switch envelope.Error {
	case "no_usable_index", "not_found", "unauthorized", "forbidden":
		return envelope.Error
	default:
		return ""
	}
}
