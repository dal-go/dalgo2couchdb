package dalgo2couchdb

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/dalgotest"
	"github.com/dal-go/dalgo/dbschema"
	"github.com/dal-go/dalgo/recordset"
	"github.com/dal-go/record"
)

func newTestDB(t *testing.T, handler http.Handler, mutate ...func(*Config)) dal.DB {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	cfg := Config{DatabaseURL: server.URL + "/appdb", AllowLoopbackHTTP: true}
	for _, apply := range mutate {
		apply(&cfg)
	}
	db, err := NewDB(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	t.Cleanup(func() {
		if closer, ok := dal.As[io.Closer](db); ok {
			_ = closer.Close()
		}
	})
	return db
}

func appQuery() *dal.QueryBuilder {
	return dal.From(dal.NewRootCollectionRef("appdb", "")).NewQuery()
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func readRequestBody(t *testing.T, r *http.Request) map[string]any {
	t.Helper()
	decoder := json.NewDecoder(r.Body)
	decoder.UseNumber()
	var body map[string]any
	if err := decoder.Decode(&body); err != nil {
		t.Errorf("decode request body: %v", err)
		return nil
	}
	return body
}

func canonicalJSON(t *testing.T, value any) string {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(encoded)
}

func dataOf(t *testing.T, rec record.Record) map[string]any {
	t.Helper()
	data, ok := rec.Data().(map[string]any)
	if !ok {
		t.Fatalf("record data is %T, want map[string]any", rec.Data())
	}
	return data
}

func TestConfigValidation(t *testing.T) {
	valid := []Config{
		{DatabaseURL: "https://example.com/appdb"},
		{DatabaseURL: "https://example.com/appdb/"},
		{DatabaseURL: "https://example.com:6984/db_1"},
		{DatabaseURL: "http://127.0.0.1:5984/appdb", AllowLoopbackHTTP: true},
		{DatabaseURL: "http://localhost:5984/appdb", AllowLoopbackHTTP: true},
		{DatabaseURL: "http://[::1]:5984/appdb", AllowLoopbackHTTP: true},
		{DatabaseURL: "https://example.com/appdb", Collection: "orders"},
	}
	for _, cfg := range valid {
		db, err := NewDB(context.Background(), cfg)
		if err != nil {
			t.Fatalf("valid config %q rejected: %v", cfg.DatabaseURL, err)
		}
		if closer, ok := dal.As[io.Closer](db); !ok {
			t.Fatal("Close capability is not reachable through dal.As")
		} else if err = closer.Close(); err != nil {
			t.Fatal(err)
		}
		if err = closerClose(db); err != nil {
			t.Fatal(err)
		}
	}
	invalid := []Config{
		{},
		{DatabaseURL: "not a url"},
		{DatabaseURL: "ftp://example.com/appdb"},
		{DatabaseURL: "https:///appdb"},
		{DatabaseURL: "https://user:password@example.com/appdb"},
		{DatabaseURL: "https://example.com/appdb?x=1"},
		{DatabaseURL: "https://example.com/appdb#fragment"},
		{DatabaseURL: "http://example.com/appdb"},
		{DatabaseURL: "http://127.0.0.1:5984/appdb"},
		{DatabaseURL: "https://example.com/appdb/other"},
		{DatabaseURL: "https://example.com/_users"},
		{DatabaseURL: "https://example.com/Appdb"},
		{DatabaseURL: "https://example.com/appdb%2Fx"},
		{DatabaseURL: "https://example.com/appdb", Collection: "_design"},
		{DatabaseURL: "https://example.com/appdb", Collection: "a/b"},
		{DatabaseURL: "https://example.com/appdb", Collection: "a b"},
		{DatabaseURL: "https://example.com/appdb", MaxRows: -1},
		{DatabaseURL: "https://example.com/appdb", MaxRows: absoluteMaxRows + 1},
		{DatabaseURL: "https://example.com/appdb", MaxRowsPerPage: absoluteMaxRowsPerPage + 1},
		{DatabaseURL: "https://example.com/appdb", MaxPages: absoluteMaxPages + 1},
		{DatabaseURL: "https://example.com/appdb", MaxOffset: absoluteMaxOffset + 1},
		{DatabaseURL: "https://example.com/appdb", MaxGetMany: absoluteMaxGetMany + 1},
		{DatabaseURL: "https://example.com/appdb", MaxRequestBytes: absoluteMaxRequestBytes + 1},
		{DatabaseURL: "https://example.com/appdb", MaxResponseBytes: absoluteMaxResponseBytes + 1},
		{DatabaseURL: "https://example.com/appdb", MaxTotalBytes: absoluteMaxTotalBytes + 1},
		{DatabaseURL: "https://example.com/appdb", MaxRowBytes: absoluteMaxRowBytes + 1},
		{DatabaseURL: "https://example.com/appdb", Timeout: -time.Second},
		{DatabaseURL: "https://example.com/appdb", Timeout: absoluteMaxTimeout + time.Second},
	}
	for _, cfg := range invalid {
		if _, err := NewDB(context.Background(), cfg); err == nil {
			t.Fatalf("invalid config accepted: %+v", cfg)
		} else if strings.Contains(err.Error(), "password") {
			t.Fatalf("config error leaked a secret: %v", err)
		}
	}
	if _, err := NewDB(nil, valid[0]); err == nil {
		t.Fatal("nil context accepted")
	}
}

func closerClose(db dal.DB) error {
	closer, ok := dal.As[io.Closer](db)
	if !ok {
		return errors.New("no closer")
	}
	return closer.Close()
}

func TestTranslationAndNativeTypes(t *testing.T) {
	var body map[string]any
	var mu sync.Mutex
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/appdb/_find" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		mu.Lock()
		body = readRequestBody(t, r)
		mu.Unlock()
		writeJSON(w, http.StatusOK, map[string]any{
			"bookmark": "b1",
			"docs": []any{map[string]any{
				"_id":    "doc-1",
				"_rev":   "1-abc",
				"name":   "tea",
				"score":  10,
				"active": true,
				"note":   nil,
				"big":    json.Number("123456789012345678901234567890"),
				"nested": map[string]any{"n": 1},
				"tags":   []any{"a", 2},
			}},
		})
	})
	db := newTestDB(t, handler)
	q := appQuery().
		WhereField("status", dal.Equal, "active").
		WhereField("score", dal.GreaterOrEqual, 10).
		OrderBy(dal.DescendingField("score")).
		Limit(2).
		Offset(1).
		SelectColumns(dal.Column{Expression: dal.Field("name")}, dal.Column{Expression: dal.Field("score")})
	reader, err := db.ExecuteQueryToRecordsReader(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	rec, err := reader.Next()
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	got := canonicalJSON(t, body)
	mu.Unlock()
	want := `{"allow_fallback":false,"fields":["_id","name","score","_rev"],"limit":2,"selector":{"$and":[{"status":{"$eq":"active"}},{"score":{"$gte":10}}]},"skip":1,"sort":[{"score":"desc"}]}`
	if got != want {
		t.Fatalf("request body\n got: %s\nwant: %s", got, want)
	}
	if rec.Key().Collection() != "appdb" || rec.Key().ID != "doc-1" {
		t.Fatalf("identity: %s %v", rec.Key().Collection(), rec.Key().ID)
	}
	data := dataOf(t, rec)
	if _, exists := data["_id"]; exists {
		t.Fatal("_id leaked into data; it is the record key")
	}
	if data["_rev"] != "1-abc" {
		t.Fatalf("_rev metadata = %#v", data["_rev"])
	}
	if data["name"] != "tea" || data["score"] != int64(10) || data["active"] != true || data["note"] != nil {
		t.Fatalf("native values lost: %#v", data)
	}
	if data["big"] != json.Number("123456789012345678901234567890") {
		t.Fatalf("wide integer lost: %#v (%T)", data["big"], data["big"])
	}
	nested, ok := data["nested"].(map[string]any)
	if !ok || nested["n"] != int64(1) {
		t.Fatalf("nested value lost: %#v", data["nested"])
	}
	if _, err = reader.Next(); !errors.Is(err, dal.ErrNoMoreRecords) {
		t.Fatalf("want no more records, got %v", err)
	}
}

func TestBookmarkedPages(t *testing.T) {
	var mu sync.Mutex
	var bookmarks []string
	var calls int
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := readRequestBody(t, r)
		mu.Lock()
		calls++
		bookmark, _ := body["bookmark"].(string)
		bookmarks = append(bookmarks, bookmark)
		page := calls
		mu.Unlock()
		switch page {
		case 1:
			writeJSON(w, http.StatusOK, map[string]any{"bookmark": "b1", "docs": []any{
				map[string]any{"_id": "doc-1", "_rev": "1-a", "n": 1},
				map[string]any{"_id": "doc-2", "_rev": "1-b", "n": 2},
			}})
		case 2:
			writeJSON(w, http.StatusOK, map[string]any{"bookmark": "b2", "docs": []any{
				map[string]any{"_id": "doc-3", "_rev": "1-c", "n": 3},
				map[string]any{"_id": "doc-4", "_rev": "1-d", "n": 4},
			}})
		case 3:
			writeJSON(w, http.StatusOK, map[string]any{"bookmark": "b3", "docs": []any{
				map[string]any{"_id": "doc-5", "_rev": "1-e", "n": 5},
			}})
		default:
			t.Errorf("unexpected extra page request")
		}
	})
	db := newTestDB(t, handler, func(cfg *Config) {
		cfg.MaxRowsPerPage = 2
		cfg.MaxPages = 3
	})
	reader, err := db.ExecuteQueryToRecordsReader(context.Background(), appQuery().Limit(5).SelectIntoRecord(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	var records []record.Record
	for {
		rec, err := reader.Next()
		if errors.Is(err, dal.ErrNoMoreRecords) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		records = append(records, rec)
	}
	if len(records) != 5 {
		t.Fatalf("got %d records", len(records))
	}
	for i, rec := range records {
		if rec.Key().ID != fmt.Sprintf("doc-%d", i+1) {
			t.Fatalf("record %d key = %v", i, rec.Key().ID)
		}
	}
	dalgotest.AssertUniqueKeys(t, records)
	mu.Lock()
	defer mu.Unlock()
	if calls != 3 || !reflect.DeepEqual(bookmarks, []string{"", "b1", "b2"}) {
		t.Fatalf("calls=%d bookmarks=%v", calls, bookmarks)
	}
}

func TestUnsupportedQueriesFailBeforeIO(t *testing.T) {
	var calls atomic.Int64
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		writeJSON(w, http.StatusOK, map[string]any{"docs": []any{}})
	})
	db := newTestDB(t, handler)
	base := func() *dal.QueryBuilder { return appQuery() }
	joined := dal.From(dal.NewRootCollectionRef("appdb", "")).Join(
		dal.NewJoinedSource(dal.NewRootCollectionRef("other", ""), dal.JoinInner,
			dal.WhereField("x", dal.Equal, 1)),
	).NewQuery()
	inner := appQuery().Limit(1).SelectColumns(dal.Column{Expression: dal.Field("n")})
	cases := []struct {
		name  string
		query dal.Query
	}{
		{"nil", nil},
		{"group-by", base().GroupBy(dal.Field("status")).Limit(1).SelectIntoRecord(nil)},
		{"having", base().GroupBy(dal.Field("status")).Having(dal.WhereField("n", dal.GreaterThen, 1)).Limit(1).SelectColumns(dal.Column{Expression: dal.Field("status")})},
		{"aggregate-column", base().Limit(1).SelectColumns(dal.Count())},
		{"join", joined.Limit(1).SelectIntoRecord(nil)},
		{"subquery-column", base().Limit(1).SelectColumns(dal.Column{Expression: dal.NewQueryExpression(inner, "n")})},
		{"exists-subquery", base().Where(dal.NewExistsCondition(inner)).Limit(1).SelectIntoRecord(nil)},
		{"limit-zero", base().SelectIntoRecord(nil)},
		{"limit-over-max", base().Limit(1001).SelectIntoRecord(nil)},
		{"offset-negative", base().Offset(-1).Limit(1).SelectIntoRecord(nil)},
		{"offset-over-max", base().Offset(10001).Limit(1).SelectIntoRecord(nil)},
		{"mixed-sort", base().OrderBy(dal.AscendingField("a"), dal.DescendingField("b")).Limit(1).SelectIntoRecord(nil)},
		{"nested-field", base().WhereField("a.b", dal.Equal, 1).Limit(1).SelectIntoRecord(nil)},
		{"unsafe-field", base().WhereField("$where", dal.Equal, 1).Limit(1).SelectIntoRecord(nil)},
		{"nested-sort", base().OrderBy(dal.AscendingField("a.b")).Limit(1).SelectIntoRecord(nil)},
		{"other-collection", dal.From(dal.NewRootCollectionRef("other", "")).NewQuery().Limit(1).SelectIntoRecord(nil)},
		{"qualified-collection", dal.From(dal.NewQualifiedRootCollectionRef("schema", "appdb", "")).NewQuery().Limit(1).SelectIntoRecord(nil)},
		{"database-collection", dal.From(dal.NewDatabaseCollectionRef("db", "", "appdb", "")).NewQuery().Limit(1).SelectIntoRecord(nil)},
		{"aliased-collection", dal.From(dal.NewRootCollectionRef("appdb", "a")).NewQuery().Limit(1).SelectIntoRecord(nil)},
		{"parent-key", dal.From(dal.NewCollectionRef("appdb", "", record.NewKeyWithID("parent", "p"))).NewQuery().Limit(1).SelectIntoRecord(nil)},
		{"start-from", base().StartFrom("abc").Limit(1).SelectIntoRecord(nil)},
		{"start-after", base().StartAfter("abc").Limit(1).SelectIntoRecord(nil)},
		{"keys-only", base().Limit(1).SelectKeysOnly(reflect.String)},
		{"wildcard", base().Limit(1).SelectColumns(dal.AllColumnsExcept("secret"))},
		{"projection-alias", base().Limit(1).SelectColumns(dal.Column{Alias: "x", Expression: dal.Field("name")})},
		{"duplicate-projection", base().Limit(1).SelectColumns(dal.Column{Expression: dal.Field("name")}, dal.Column{Expression: dal.Field("name")})},
		{"duplicate-sort", base().OrderBy(dal.AscendingField("a"), dal.AscendingField("a")).Limit(1).SelectIntoRecord(nil)},
		{"non-field-left", base().Where(dal.Comparison{Left: dal.Constant{Value: 1}, Operator: dal.Equal, Right: dal.Constant{Value: 1}}).Limit(1).SelectIntoRecord(nil)},
		{"non-scalar-constant", base().Where(dal.Comparison{Left: dal.Field("x"), Operator: dal.Equal, Right: dal.Constant{Value: map[string]any{}}}).Limit(1).SelectIntoRecord(nil)},
		{"array-without-in", base().Where(dal.Comparison{Left: dal.Field("x"), Operator: dal.Equal, Right: dal.Array{Value: []any{1}}}).Limit(1).SelectIntoRecord(nil)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			backend := dal.BackendOf(db)
			executor, ok := backend.(dal.QueryExecutor)
			if !ok {
				t.Fatal("backend is not a QueryExecutor")
			}
			if _, err := executor.ExecuteQueryToRecordsReader(context.Background(), tc.query); !errors.Is(err, dal.ErrNotSupported) {
				t.Fatalf("records reader error = %v", err)
			}
			if _, err := executor.ExecuteQueryToRecordsetReader(context.Background(), tc.query); !errors.Is(err, dal.ErrNotSupported) {
				t.Fatalf("recordset reader error = %v", err)
			}
		})
	}
	if calls.Load() != 0 {
		t.Fatalf("unsupported queries made %d request(s)", calls.Load())
	}
}

func TestPageBudgetRejectedBeforeIO(t *testing.T) {
	var calls atomic.Int64
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		t.Error("unexpected request")
	})
	db := newTestDB(t, handler, func(cfg *Config) {
		cfg.MaxRowsPerPage = 2
		cfg.MaxPages = 1
	})
	if _, err := db.ExecuteQueryToRecordsReader(context.Background(), appQuery().Limit(3).SelectIntoRecord(nil)); !errors.Is(err, dal.ErrNotSupported) {
		t.Fatal(err)
	}
	if calls.Load() != 0 {
		t.Fatal("page budget made a request")
	}
}

func TestDALgoAggregateFallbackLimitZeroRefused(t *testing.T) {
	var calls atomic.Int64
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		t.Error("aggregate fallback reached the network")
	})
	db := newTestDB(t, handler)
	q := appQuery().Limit(1).GroupBy(dal.Field("status")).SelectColumns(dal.Count())
	if _, err := db.ExecuteQueryToRecordsReader(context.Background(), q); !errors.Is(err, dal.ErrNotSupported) {
		t.Fatalf("records reader: %v", err)
	}
	if _, err := db.ExecuteQueryToRecordsetReader(context.Background(), q); !errors.Is(err, dal.ErrNotSupported) {
		t.Fatalf("recordset reader: %v", err)
	}
	if calls.Load() != 0 {
		t.Fatalf("aggregate fallback made %d request(s)", calls.Load())
	}
}

func TestResponseByteBudgetCheckedBeforeDecode(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), 64)
	for _, chunked := range []bool{false, true} {
		t.Run(fmt.Sprintf("chunked=%v", chunked), func(t *testing.T) {
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				if chunked {
					w.(http.Flusher).Flush()
				}
				_, _ = w.Write(payload)
			})
			db := newTestDB(t, handler, func(cfg *Config) { cfg.MaxResponseBytes = 32 })
			rec := record.NewRecordWithData(record.NewKeyWithID("appdb", "doc-1"), &map[string]any{})
			if err := db.Get(context.Background(), rec); !errors.Is(err, ErrResponseTooLarge) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestRowAndCardinalityCapsBeforeReturningRows(t *testing.T) {
	t.Run("row-bytes", func(t *testing.T) {
		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, http.StatusOK, map[string]any{"docs": []any{
				map[string]any{"_id": "doc-1", "_rev": "1-a", "text": strings.Repeat("x", 64)},
			}})
		})
		db := newTestDB(t, handler, func(cfg *Config) { cfg.MaxRowBytes = 32 })
		reader, err := db.ExecuteQueryToRecordsReader(context.Background(), appQuery().Limit(1).SelectIntoRecord(nil))
		if err != nil {
			t.Fatal(err)
		}
		if rec, err := reader.Next(); rec != nil || !errors.Is(err, ErrRowTooLarge) {
			t.Fatalf("rec=%v err=%v", rec, err)
		}
	})
	t.Run("cardinality", func(t *testing.T) {
		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, http.StatusOK, map[string]any{"docs": []any{
				map[string]any{"_id": "doc-1", "_rev": "1-a"},
				map[string]any{"_id": "doc-2", "_rev": "1-b"},
			}})
		})
		db := newTestDB(t, handler, func(cfg *Config) { cfg.MaxRowsPerPage = 1 })
		reader, err := db.ExecuteQueryToRecordsReader(context.Background(), appQuery().Limit(1).SelectIntoRecord(nil))
		if err != nil {
			t.Fatal(err)
		}
		if rec, err := reader.Next(); rec != nil || !errors.Is(err, ErrTooManyRows) {
			t.Fatalf("rec=%v err=%v", rec, err)
		}
	})
}

func TestRequestByteBudgetBeforeIO(t *testing.T) {
	var calls atomic.Int64
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		t.Error("unexpected request")
	})
	db := newTestDB(t, handler, func(cfg *Config) { cfg.MaxRequestBytes = 8 })
	reader, err := db.ExecuteQueryToRecordsReader(context.Background(), appQuery().WhereField("a", dal.Equal, "value").Limit(1).SelectIntoRecord(nil))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = reader.Next(); !errors.Is(err, ErrRequestTooLarge) {
		t.Fatal(err)
	}
	if calls.Load() != 0 {
		t.Fatal("request budget made a request")
	}
}

func TestCancellationThroughInFlightRequestAndReader(t *testing.T) {
	t.Run("get", func(t *testing.T) {
		started := make(chan struct{})
		release := make(chan struct{})
		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			close(started)
			select {
			case <-r.Context().Done():
			case <-release:
			}
		})
		db := newTestDB(t, handler)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() {
			<-started
			cancel()
		}()
		rec := record.NewRecordWithData(record.NewKeyWithID("appdb", "doc-1"), &map[string]any{})
		err := db.Get(ctx, rec)
		close(release)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("reader", func(t *testing.T) {
		started := make(chan struct{})
		release := make(chan struct{})
		var calls int
		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls++
			if calls == 1 {
				writeJSON(w, http.StatusOK, map[string]any{"bookmark": "b1", "docs": []any{
					map[string]any{"_id": "doc-1", "_rev": "1-a"},
				}})
				return
			}
			close(started)
			select {
			case <-r.Context().Done():
			case <-release:
			}
		})
		db := newTestDB(t, handler, func(cfg *Config) { cfg.MaxRowsPerPage = 1 })
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		reader, err := db.ExecuteQueryToRecordsReader(ctx, appQuery().Limit(2).SelectIntoRecord(nil))
		if err != nil {
			t.Fatal(err)
		}
		if _, err = reader.Next(); err != nil {
			t.Fatal(err)
		}
		go func() {
			<-started
			cancel()
		}()
		_, err = reader.Next()
		close(release)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v", err)
		}
	})
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestTransportAndHTTPErrorsDoNotLeakSecrets(t *testing.T) {
	secret := "super-secret-token"
	t.Run("transport", func(t *testing.T) {
		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
		db := newTestDB(t, handler, func(cfg *Config) {
			cfg.HTTPClient = &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
				return nil, errors.New("dial failed for " + secret)
			})}
		})
		rec := record.NewRecordWithData(record.NewKeyWithID("appdb", "doc-1"), &map[string]any{})
		err := db.Get(context.Background(), rec)
		if !errors.Is(err, ErrRequestFailed) || strings.Contains(err.Error(), secret) {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("deadline", func(t *testing.T) {
		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
		db := newTestDB(t, handler, func(cfg *Config) {
			cfg.HTTPClient = &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
				return nil, fmt.Errorf("%s: %w", secret, context.DeadlineExceeded)
			})}
		})
		rec := record.NewRecordWithData(record.NewKeyWithID("appdb", "doc-1"), &map[string]any{})
		err := db.Get(context.Background(), rec)
		if !errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), secret) {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("http-401", func(t *testing.T) {
		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"unauthorized","reason":"` + secret + `"}`))
		})
		db := newTestDB(t, handler)
		rec := record.NewRecordWithData(record.NewKeyWithID("appdb", "doc-1"), &map[string]any{})
		err := db.Get(context.Background(), rec)
		var httpErr *HTTPError
		if !errors.As(err, &httpErr) || httpErr.StatusCode != http.StatusUnauthorized || strings.Contains(err.Error(), secret) {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("header-provider", func(t *testing.T) {
		var calls atomic.Int64
		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1) })
		db := newTestDB(t, handler, func(cfg *Config) {
			cfg.Headers = func(context.Context) (map[string]string, error) {
				return nil, errors.New(secret)
			}
		})
		rec := record.NewRecordWithData(record.NewKeyWithID("appdb", "doc-1"), &map[string]any{})
		err := db.Get(context.Background(), rec)
		if !errors.Is(err, ErrHeaderProvider) || strings.Contains(err.Error(), secret) {
			t.Fatalf("error = %v", err)
		}
		if calls.Load() != 0 {
			t.Fatal("header provider failure reached the network")
		}
	})
}

func TestHeadersArePerRequestAndValidated(t *testing.T) {
	type ctxKey string
	key := ctxKey("request")
	var seen atomic.Bool
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer token" {
			t.Errorf("Authorization header missing")
		}
		seen.Store(true)
		writeJSON(w, http.StatusOK, map[string]any{"_id": "doc-1", "_rev": "1-a"})
	})
	db := newTestDB(t, handler, func(cfg *Config) {
		cfg.Headers = func(ctx context.Context) (map[string]string, error) {
			if ctx.Value(key) != "ok" {
				return nil, errors.New("context value missing")
			}
			return map[string]string{"Authorization": "Bearer token"}, nil
		}
	})
	ctx := context.WithValue(context.Background(), key, "ok")
	rec := record.NewRecordWithData(record.NewKeyWithID("appdb", "doc-1"), &map[string]any{})
	if err := db.Get(ctx, rec); err != nil {
		t.Fatal(err)
	}
	if !seen.Load() {
		t.Fatal("request did not reach the server")
	}

	var calls atomic.Int64
	badHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1) })
	badDB := newTestDB(t, badHandler, func(cfg *Config) {
		cfg.Headers = func(context.Context) (map[string]string, error) {
			return map[string]string{"X-Test": "a\r\nInjected: yes"}, nil
		}
	})
	badRec := record.NewRecordWithData(record.NewKeyWithID("appdb", "doc-1"), &map[string]any{})
	if err := badDB.Get(context.Background(), badRec); !errors.Is(err, ErrUnsafeValue) {
		t.Fatalf("error = %v", err)
	}
	if calls.Load() != 0 {
		t.Fatal("unsafe header reached the network")
	}
}

func TestRedirectsAreBlocked(t *testing.T) {
	var redirected atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirected.Add(1)
		writeJSON(w, http.StatusOK, map[string]any{"_id": "doc-1", "_rev": "1-a"})
	}))
	defer target.Close()
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+"/appdb/doc-1", http.StatusFound)
	}))
	defer source.Close()
	db, err := NewDB(context.Background(), Config{DatabaseURL: source.URL + "/appdb", AllowLoopbackHTTP: true})
	if err != nil {
		t.Fatal(err)
	}
	rec := record.NewRecordWithData(record.NewKeyWithID("appdb", "doc-1"), &map[string]any{})
	err = db.Get(context.Background(), rec)
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) || httpErr.StatusCode != http.StatusFound {
		t.Fatalf("error = %v", err)
	}
	if redirected.Load() != 0 {
		t.Fatal("redirect was followed")
	}
}

func TestGetExistsAndGetMulti(t *testing.T) {
	var mu sync.Mutex
	var requests []string
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests = append(requests, r.Method+" "+r.URL.Path)
		mu.Unlock()
		switch r.Method + " " + r.URL.Path {
		case "GET /appdb/doc-1", "GET /appdb/doc-2":
			id := strings.TrimPrefix(r.URL.Path, "/appdb/")
			writeJSON(w, http.StatusOK, map[string]any{"_id": id, "_rev": "1-a", "name": id, "n": 1})
		case "GET /appdb/missing", "HEAD /appdb/missing":
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"not_found","reason":"missing"}`))
		case "HEAD /appdb/doc-1":
			w.WriteHeader(http.StatusOK)
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	})
	db := newTestDB(t, handler)
	ctx := context.Background()
	loaded := record.NewRecordWithData(record.NewKeyWithID("appdb", "doc-1"), &map[string]any{})
	if err := db.Get(ctx, loaded); err != nil {
		t.Fatal(err)
	}
	data := *loaded.Data().(*map[string]any)
	if data["name"] != "doc-1" || data["n"] != int64(1) || data["_rev"] != "1-a" {
		t.Fatalf("data = %#v", data)
	}
	missing := record.NewRecordWithData(record.NewKeyWithID("appdb", "missing"), &map[string]any{})
	if err := db.Get(ctx, missing); !errors.Is(err, record.ErrRecordNotFound) {
		t.Fatalf("error = %v", err)
	}
	if exists, err := db.Exists(ctx, record.NewKeyWithID("appdb", "doc-1")); err != nil || !exists {
		t.Fatalf("exists=%v err=%v", exists, err)
	}
	if exists, err := db.Exists(ctx, record.NewKeyWithID("appdb", "missing")); err != nil || exists {
		t.Fatalf("exists=%v err=%v", exists, err)
	}
	first := record.NewRecordWithData(record.NewKeyWithID("appdb", "doc-1"), &map[string]any{})
	second := record.NewRecordWithData(record.NewKeyWithID("appdb", "doc-2"), &map[string]any{})
	if err := db.GetMulti(ctx, []record.Record{first, second}); err != nil {
		t.Fatal(err)
	}
	if (*first.Data().(*map[string]any))["name"] != "doc-1" || (*second.Data().(*map[string]any))["name"] != "doc-2" {
		t.Fatal("GetMulti data mismatch")
	}
	mu.Lock()
	before := len(requests)
	requests = nil
	mu.Unlock()
	bad := record.NewRecordWithData(record.NewKeyWithID("other", "doc-3"), &map[string]any{})
	if err := db.GetMulti(ctx, []record.Record{first, bad}); !errors.Is(err, dal.ErrNotSupported) {
		t.Fatalf("error = %v", err)
	}
	mu.Lock()
	after := len(requests)
	mu.Unlock()
	if after != 0 || before == 0 {
		t.Fatalf("GetMulti validation made %d request(s)", after)
	}
	if _, err := db.Exists(ctx, record.NewKeyWithID("appdb", "../escape")); !errors.Is(err, ErrUnsafeValue) {
		t.Fatalf("unsafe id error = %v", err)
	}
	if _, err := db.Exists(ctx, record.NewKeyWithID("appdb", "_design/x")); !errors.Is(err, ErrUnsafeValue) {
		t.Fatalf("management id error = %v", err)
	}
	if _, err := db.Exists(ctx, record.NewKeyWithID("appdb", 42)); !errors.Is(err, dal.ErrNotSupported) {
		t.Fatalf("numeric id error = %v", err)
	}
}

func TestDocumentIDWithSlashIsPathEscaped(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.EscapedPath(); got != "/appdb/native%2Fslash" {
			t.Errorf("escaped path = %q", got)
		}
		writeJSON(w, http.StatusOK, map[string]any{"_id": "native/slash", "_rev": "1-a", "kind": "slash"})
	})
	db := newTestDB(t, handler)
	rec := record.NewRecordWithData(record.NewKeyWithID("appdb", "native/slash"), &map[string]any{})
	if err := db.Get(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
	if data := *rec.Data().(*map[string]any); data["kind"] != "slash" {
		t.Fatalf("data = %#v", data)
	}
}

func TestMalformedResponses(t *testing.T) {
	cases := []struct {
		name    string
		payload string
		want    error
	}{
		{"invalid-json", `not json`, ErrMalformedResponse},
		{"docs-not-array", `{"docs":"nope"}`, ErrMalformedResponse},
		{"missing-id", `{"docs":[{"_rev":"1-a"}]}`, ErrMalformedResponse},
		{"missing-rev", `{"docs":[{"_id":"doc-1"}]}`, ErrMalformedResponse},
		{"management-id", `{"docs":[{"_id":"_design/x","_rev":"1-a"}]}`, ErrUnsafeValue},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(tc.payload))
			})
			db := newTestDB(t, handler)
			reader, err := db.ExecuteQueryToRecordsReader(context.Background(), appQuery().Limit(1).SelectIntoRecord(nil))
			if err != nil {
				t.Fatal(err)
			}
			if rec, err := reader.Next(); rec != nil || !errors.Is(err, tc.want) {
				t.Fatalf("rec=%v err=%v", rec, err)
			}
		})
	}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"_id":"doc-1"}`))
	})
	db := newTestDB(t, handler)
	rec := record.NewRecordWithData(record.NewKeyWithID("appdb", "doc-1"), &map[string]any{})
	if err := db.Get(context.Background(), rec); !errors.Is(err, ErrMalformedResponse) {
		t.Fatalf("Get error = %v", err)
	}
}

func TestGetMultiBudgetBeforeIO(t *testing.T) {
	var calls atomic.Int64
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1) })
	db := newTestDB(t, handler, func(cfg *Config) { cfg.MaxGetMany = 1 })
	first := record.NewRecordWithData(record.NewKeyWithID("appdb", "doc-1"), &map[string]any{})
	second := record.NewRecordWithData(record.NewKeyWithID("appdb", "doc-2"), &map[string]any{})
	if err := db.GetMulti(context.Background(), []record.Record{first, second}); !errors.Is(err, dal.ErrNotSupported) {
		t.Fatalf("error = %v", err)
	}
	if calls.Load() != 0 {
		t.Fatal("GetMulti budget made a request")
	}
}

func TestReadOnlyCapabilities(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"_id": "doc-1", "_rev": "1-a"})
	})
	db := newTestDB(t, handler)
	if _, ok := dal.As[dal.WriteSession](db); ok {
		t.Fatal("adapter advertises writes")
	}
	called := false
	err := db.RunReadwriteTransaction(context.Background(), func(context.Context, dal.ReadwriteTransaction) error {
		called = true
		return nil
	})
	if !errors.Is(err, dal.ErrNotSupported) || called {
		t.Fatalf("err=%v called=%v", err, called)
	}
	if err = db.RunReadonlyTransaction(context.Background(), func(ctx context.Context, tx dal.ReadTransaction) error {
		rec := record.NewRecordWithData(record.NewKeyWithID("appdb", "doc-1"), &map[string]any{})
		return tx.Get(ctx, rec)
	}); err != nil {
		t.Fatal(err)
	}
	if db.ID() != "appdb" || db.Adapter().Name() != "couchdb" {
		t.Fatalf("metadata: %s %s", db.ID(), db.Adapter())
	}
}

func TestSchemaReader(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/appdb/_index" {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"total_rows": 2,
			"indexes": []any{
				map[string]any{"ddoc": nil, "name": "_all_docs", "type": "special", "def": map[string]any{"fields": []any{map[string]any{"_id": "asc"}}}},
				map[string]any{"ddoc": "_design/abc", "name": "status-score", "type": "json", "def": map[string]any{"fields": []any{map[string]any{"status": "asc"}, map[string]any{"score": "desc"}}}},
			},
		})
	})
	db := newTestDB(t, handler)
	sr, ok := dal.As[dbschema.SchemaReader](db)
	if !ok {
		t.Fatal("SchemaReader not reachable through dal.As")
	}
	collections, err := sr.ListCollections(context.Background(), nil)
	if err != nil || len(collections) != 1 || collections[0].Name() != "appdb" {
		t.Fatalf("collections=%v err=%v", collections, err)
	}
	if _, err = sr.ListCollections(context.Background(), record.NewKeyWithID("parent", "p")); !errors.Is(err, dal.ErrNotSupported) {
		t.Fatalf("parent error = %v", err)
	}
	ref := dal.NewRootCollectionRef("appdb", "")
	definition, err := sr.DescribeCollection(context.Background(), &ref)
	if err != nil {
		t.Fatal(err)
	}
	if definition.Name != "appdb" || len(definition.Fields) != 0 || len(definition.PrimaryKey) != 1 || definition.PrimaryKey[0] != "_id" {
		t.Fatalf("definition = %#v", definition)
	}
	indexes, err := sr.ListIndexes(context.Background(), &ref)
	if err != nil {
		t.Fatal(err)
	}
	if len(indexes) != 2 || indexes[1].Name != "status-score" || !reflect.DeepEqual(indexes[1].Fields, []dal.FieldName{"status", "score"}) {
		t.Fatalf("indexes = %#v", indexes)
	}
	if _, err = sr.ListConstraints(context.Background(), &ref); !errors.Is(err, dal.ErrNotSupported) {
		t.Fatalf("constraints error = %v", err)
	}
	if _, err = sr.ListReferrers(context.Background(), &ref); !errors.Is(err, dal.ErrNotSupported) {
		t.Fatalf("referrers error = %v", err)
	}
	other := dal.NewRootCollectionRef("other", "")
	if _, err = sr.DescribeCollection(context.Background(), &other); !errors.Is(err, dal.ErrNotSupported) {
		t.Fatalf("other collection error = %v", err)
	}
}

func TestSchemaMetadataFailureIsNotAnEmptyList(t *testing.T) {
	secret := "index-secret"
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"error":"forbidden","reason":"` + secret + `"}`))
	})
	db := newTestDB(t, handler)
	sr, _ := dal.As[dbschema.SchemaReader](db)
	ref := dal.NewRootCollectionRef("appdb", "")
	indexes, err := sr.ListIndexes(context.Background(), &ref)
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) || httpErr.StatusCode != http.StatusForbidden || indexes != nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("indexes=%v err=%v", indexes, err)
	}
}

func TestMangoPagerContinuationBinding(t *testing.T) {
	var mu sync.Mutex
	var bodies []map[string]any
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := readRequestBody(t, r)
		mu.Lock()
		bodies = append(bodies, body)
		count := len(bodies)
		mu.Unlock()
		if count == 1 {
			writeJSON(w, http.StatusOK, map[string]any{"bookmark": "b1", "docs": []any{
				map[string]any{"_id": "doc-1", "_rev": "1-a"},
				map[string]any{"_id": "doc-2", "_rev": "1-b"},
			}})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"bookmark": "b2", "docs": []any{
			map[string]any{"_id": "doc-3", "_rev": "1-c"},
		}})
	})
	db := newTestDB(t, handler)
	pager, ok := dal.As[MangoPager](db)
	if !ok {
		t.Fatal("MangoPager not reachable through dal.As")
	}
	firstQuery := appQuery().WhereField("status", dal.Equal, "active").Limit(2).SelectIntoRecord(nil)
	first, err := pager.ReadMangoPage(context.Background(), MangoPageRequest{Query: firstQuery})
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Records) != 2 || first.Continuation == "" {
		t.Fatalf("first page = %#v", first)
	}
	second, err := pager.ReadMangoPage(context.Background(), MangoPageRequest{Query: firstQuery, Continuation: first.Continuation})
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Records) != 1 || second.Continuation != "" {
		t.Fatalf("second page = %#v", second)
	}
	mu.Lock()
	if bookmark, _ := bodies[1]["bookmark"].(string); bookmark != "b1" {
		t.Fatalf("second request bookmark = %q", bookmark)
	}
	before := len(bodies)
	mu.Unlock()
	otherQuery := appQuery().WhereField("status", dal.Equal, "other").Limit(2).SelectIntoRecord(nil)
	if _, err = pager.ReadMangoPage(context.Background(), MangoPageRequest{Query: otherQuery, Continuation: first.Continuation}); !errors.Is(err, ErrContinuationMismatch) {
		t.Fatalf("error = %v", err)
	}
	offsetQuery := appQuery().WhereField("status", dal.Equal, "active").Offset(1).Limit(2).SelectIntoRecord(nil)
	if _, err = pager.ReadMangoPage(context.Background(), MangoPageRequest{Query: offsetQuery, Continuation: first.Continuation}); !errors.Is(err, dal.ErrNotSupported) {
		t.Fatalf("offset continuation error = %v", err)
	}
	mu.Lock()
	after := len(bodies)
	mu.Unlock()
	if after != before {
		t.Fatalf("mismatched continuation made %d request(s)", after-before)
	}
}

func TestRecordsetReaders(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"docs": []any{
			map[string]any{"_id": "doc-1", "_rev": "1-a", "name": "tea", "score": 10},
			map[string]any{"_id": "doc-2", "_rev": "1-b", "name": "coffee", "score": 20},
		}})
	})
	db := newTestDB(t, handler)
	q := appQuery().Limit(2).SelectColumns(dal.Column{Expression: dal.Field("name")}, dal.Column{Expression: dal.Field("score")})
	reader, err := db.ExecuteQueryToRecordsetReader(context.Background(), q, recordset.WithName("custom"))
	if err != nil {
		t.Fatal(err)
	}
	rs := reader.Recordset()
	if rs.Name() != "custom" || rs.RowsCount() != 2 || rs.ColumnsCount() != 3 {
		t.Fatalf("recordset name=%q rows=%d columns=%d", rs.Name(), rs.RowsCount(), rs.ColumnsCount())
	}
	if names := []string{rs.GetColumnByIndex(0).Name(), rs.GetColumnByIndex(1).Name(), rs.GetColumnByIndex(2).Name()}; !reflect.DeepEqual(names, []string{"name", "score", "_rev"}) {
		t.Fatalf("columns = %v", names)
	}
	row, _, err := reader.Next()
	if err != nil {
		t.Fatal(err)
	}
	value, err := row.GetValueByName("name", rs)
	if err != nil || value != "tea" {
		t.Fatalf("value=%v err=%v", value, err)
	}
	if _, _, err = reader.Next(); err != nil {
		t.Fatal(err)
	}
	if _, _, err = reader.Next(); !errors.Is(err, dal.ErrNoMoreRecords) {
		t.Fatalf("error = %v", err)
	}

	emptyHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"docs": []any{}})
	})
	emptyDB := newTestDB(t, emptyHandler)
	emptyReader, err := emptyDB.ExecuteQueryToRecordsetReader(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	empty := emptyReader.Recordset()
	if empty.RowsCount() != 0 || empty.ColumnsCount() != 3 {
		t.Fatalf("empty recordset rows=%d columns=%d", empty.RowsCount(), empty.ColumnsCount())
	}
	if names := []string{empty.GetColumnByIndex(0).Name(), empty.GetColumnByIndex(1).Name(), empty.GetColumnByIndex(2).Name()}; !reflect.DeepEqual(names, []string{"name", "score", "_rev"}) {
		t.Fatalf("empty columns = %v", names)
	}

	unprojectedReader, err := db.ExecuteQueryToRecordsetReader(context.Background(), appQuery().Limit(2).SelectIntoRecord(nil))
	if err != nil {
		t.Fatal(err)
	}
	unprojected := unprojectedReader.Recordset()
	names := make([]string, unprojected.ColumnsCount())
	for i := range names {
		names[i] = unprojected.GetColumnByIndex(i).Name()
	}
	if !reflect.DeepEqual(names, []string{"_rev", "name", "score"}) {
		t.Fatalf("unprojected columns = %v", names)
	}
}

func TestLogicalCollectionAlias(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/appdb/_find" {
			t.Errorf("path = %s", r.URL.Path)
		}
		writeJSON(w, http.StatusOK, map[string]any{"docs": []any{map[string]any{"_id": "doc-1", "_rev": "1-a"}}})
	})
	db := newTestDB(t, handler, func(cfg *Config) { cfg.Collection = "orders" })
	if db.ID() != "orders" {
		t.Fatalf("ID = %s", db.ID())
	}
	q := dal.From(dal.NewRootCollectionRef("orders", "")).NewQuery().Limit(1).SelectIntoRecord(nil)
	reader, err := db.ExecuteQueryToRecordsReader(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = reader.Next(); err != nil {
		t.Fatal(err)
	}
	other := dal.From(dal.NewRootCollectionRef("appdb", "")).NewQuery().Limit(1).SelectIntoRecord(nil)
	if _, err = db.ExecuteQueryToRecordsReader(context.Background(), other); !errors.Is(err, dal.ErrNotSupported) {
		t.Fatalf("error = %v", err)
	}
}
