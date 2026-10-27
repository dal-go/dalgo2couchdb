package dalgo2couchdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/dalgotest"
	"github.com/dal-go/record"
)

// primaryRow builds one native _all_docs row whose document carries the given
// native fields.
func primaryRow(id string, fields map[string]any) map[string]any {
	doc := map[string]any{"_id": id, "_rev": "1-" + id, "name": "name-" + id}
	for key, value := range fields {
		doc[key] = value
	}
	return allDocsRow(doc)
}

// primaryDeletedRow is a live-endpoint-compatible deleted row shape. CouchDB
// 3.5 normally omits deleted documents from _all_docs entirely; the adapter
// still has to ignore one if an endpoint reports it.
func primaryDeletedRow(id string) map[string]any {
	return map[string]any{
		"id":    id,
		"key":   id,
		"value": map[string]any{"rev": "2-deleted", "deleted": true},
		"doc":   nil,
	}
}

// fakePrimary is a small native _all_docs fixture. It keeps rows in native
// _id order and honours descending, an inclusive startkey and skip the way
// CouchDB does, so continuation and page accounting are exercised rather than
// assumed. delete stands in for an external administrator changing the
// database between two reads.
type fakePrimary struct {
	mu      sync.Mutex
	rows    []map[string]any
	queries []url.Values
	methods []string
	paths   []string
}

func newFakePrimary(rows ...map[string]any) *fakePrimary {
	return &fakePrimary{rows: rows}
}

// delete removes one document identity, as an external admin would between
// two independent adapter requests.
func (f *fakePrimary) delete(id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	kept := f.rows[:0]
	for _, row := range f.rows {
		if row["id"] != id {
			kept = append(kept, row)
		}
	}
	f.rows = kept
}

func (f *fakePrimary) handler(t *testing.T) http.Handler {
	t.Helper()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.queries = append(f.queries, r.URL.Query())
		f.methods = append(f.methods, r.Method)
		f.paths = append(f.paths, r.URL.Path)
		rows := append([]map[string]any(nil), f.rows...)
		f.mu.Unlock()

		if r.Method != http.MethodGet || r.URL.Path != "/appdb/_all_docs" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		query := r.URL.Query()
		limit, err := strconv.Atoi(query.Get("limit"))
		if err != nil || limit <= 0 {
			t.Errorf("limit = %q", query.Get("limit"))
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if query.Get("include_docs") != "true" {
			t.Errorf("include_docs = %q", query.Get("include_docs"))
		}
		// CouchDB _all_docs is always in native _id order, regardless of the
		// insertion order of a fixture.
		sort.Slice(rows, func(i, j int) bool {
			return rows[i]["id"].(string) < rows[j]["id"].(string)
		})
		descending := query.Get("descending") == "true"
		if descending {
			for i, j := 0, len(rows)-1; i < j; i, j = i+1, j-1 {
				rows[i], rows[j] = rows[j], rows[i]
			}
		}
		start := 0
		if raw := query.Get("startkey"); raw != "" {
			var key string
			if err := json.Unmarshal([]byte(raw), &key); err != nil {
				t.Errorf("startkey is not a JSON string: %q", raw)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			// startkey is inclusive and does not require the key to exist:
			// CouchDB starts at the first row at or after it in native order,
			// so a deleted anchor continues with the next surviving document.
			for start < len(rows) {
				id := rows[start]["id"].(string)
				if (!descending && id >= key) || (descending && id <= key) {
					break
				}
				start++
			}
		}
		if raw := query.Get("skip"); raw != "" {
			skip, err := strconv.Atoi(raw)
			if err != nil || skip < 0 {
				t.Errorf("skip = %q", raw)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			start += skip
		}
		if start > len(rows) {
			start = len(rows)
		}
		end := start + limit
		if end > len(rows) {
			end = len(rows)
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"total_rows": len(rows),
			"offset":     start,
			"rows":       rows[start:end],
		})
	})
}

func (f *fakePrimary) snapshot() (queries []url.Values, methods, paths []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]url.Values(nil), f.queries...), append([]string(nil), f.methods...), append([]string(nil), f.paths...)
}

func readPrimaryIDs(t *testing.T, reader dal.RecordsReader) []string {
	t.Helper()
	var ids []string
	for {
		rec, err := reader.Next()
		if errors.Is(err, dal.ErrNoMoreRecords) {
			return ids
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		ids = append(ids, rec.Key().ID.(string))
	}
}

func nextPrimary(t *testing.T, reader dal.RecordsReader) (string, map[string]any) {
	t.Helper()
	rec, err := reader.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	id, ok := rec.Key().ID.(string)
	if !ok {
		t.Fatalf("key ID is %T, want string", rec.Key().ID)
	}
	return id, dataOf(t, rec)
}

// primaryRequestLimits returns every requested page limit and rejects a
// continuation that used skip instead of the inclusive anchor contract.
func primaryRequestLimits(t *testing.T, fixture *fakePrimary) []int {
	t.Helper()
	queries, _, _ := fixture.snapshot()
	limits := make([]int, len(queries))
	for i, query := range queries {
		limit, err := strconv.Atoi(query.Get("limit"))
		if err != nil || limit <= 0 {
			t.Fatalf("request %d limit = %q", i, query.Get("limit"))
		}
		if query.Get("startkey") != "" && query.Get("skip") != "" {
			t.Fatalf("request %d = %v; inclusive continuation must not use skip", i, query)
		}
		limits[i] = limit
	}
	return limits
}

func TestPrimaryRouteEligibility(t *testing.T) {
	t.Run("no-order-uses-native-primary", func(t *testing.T) {
		fixture := newFakePrimary(primaryRow("doc-1", nil), primaryRow("doc-2", nil))
		db := newTestDB(t, fixture.handler(t))
		reader, err := db.ExecuteQueryToRecordsReader(context.Background(), appQuery().Limit(2).SelectIntoRecord(nil))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = reader.Close() }()
		if ids := readPrimaryIDs(t, reader); !reflect.DeepEqual(ids, []string{"doc-1", "doc-2"}) {
			t.Fatalf("ids = %v", ids)
		}
		queries, methods, paths := fixture.snapshot()
		if len(queries) != 1 || methods[0] != http.MethodGet || paths[0] != "/appdb/_all_docs" {
			t.Fatalf("requests = %v %v %v", methods, paths, queries)
		}
		first := queries[0]
		if first.Get("include_docs") != "true" || first.Get("descending") != "" || first.Get("startkey") != "" || first.Get("skip") != "" {
			t.Fatalf("first query = %v", first)
		}
	})

	t.Run("explicit-ascending-id-uses-native-primary", func(t *testing.T) {
		fixture := newFakePrimary(primaryRow("doc-1", nil))
		db := newTestDB(t, fixture.handler(t))
		reader, err := db.ExecuteQueryToRecordsReader(context.Background(), appQuery().OrderBy(dal.Ascending(dal.DocumentID())).Limit(1).SelectIntoRecord(nil))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = reader.Close() }()
		if ids := readPrimaryIDs(t, reader); !reflect.DeepEqual(ids, []string{"doc-1"}) {
			t.Fatalf("ids = %v", ids)
		}
		queries, _, _ := fixture.snapshot()
		if len(queries) != 1 || queries[0].Get("descending") != "" {
			t.Fatalf("queries = %v", queries)
		}
	})

	t.Run("explicit-descending-id-uses-native-primary", func(t *testing.T) {
		fixture := newFakePrimary(primaryRow("doc-1", nil), primaryRow("doc-2", nil))
		db := newTestDB(t, fixture.handler(t))
		reader, err := db.ExecuteQueryToRecordsReader(context.Background(), appQuery().OrderBy(dal.DescendingField("_id")).Limit(2).SelectIntoRecord(nil))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = reader.Close() }()
		if ids := readPrimaryIDs(t, reader); !reflect.DeepEqual(ids, []string{"doc-2", "doc-1"}) {
			t.Fatalf("ids = %v", ids)
		}
		queries, _, _ := fixture.snapshot()
		if len(queries) != 1 || queries[0].Get("descending") != "true" {
			t.Fatalf("queries = %v", queries)
		}
	})

	t.Run("filtered-query-stays-on-mango", func(t *testing.T) {
		var mu sync.Mutex
		var paths []string
		var bodies []map[string]any
		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			paths = append(paths, r.URL.Path)
			bodies = append(bodies, readRequestBody(t, r))
			mu.Unlock()
			writeJSON(w, http.StatusOK, map[string]any{"docs": []any{
				map[string]any{"_id": "doc-1", "_rev": "1-a", "status": "active"},
			}})
		})
		db := newTestDB(t, handler)
		reader, err := db.ExecuteQueryToRecordsReader(context.Background(),
			appQuery().WhereField("status", dal.Equal, "active").OrderBy(dal.AscendingField("_id")).Limit(1).SelectIntoRecord(nil))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = reader.Close() }()
		if ids := readPrimaryIDs(t, reader); !reflect.DeepEqual(ids, []string{"doc-1"}) {
			t.Fatalf("ids = %v", ids)
		}
		mu.Lock()
		defer mu.Unlock()
		if !reflect.DeepEqual(paths, []string{"/appdb/_find"}) {
			t.Fatalf("paths = %v; a filtered query must never use _all_docs", paths)
		}
		if allow, ok := bodies[0]["allow_fallback"].(bool); !ok || allow {
			t.Fatalf("allow_fallback = %#v", bodies[0]["allow_fallback"])
		}
	})

	t.Run("unfiltered-non-id-order-stays-on-mango", func(t *testing.T) {
		var calls atomic.Int64
		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			if r.URL.Path != "/appdb/_find" {
				t.Errorf("path = %s; an indexed order must not become a primary scan", r.URL.Path)
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":"invalid_index","reason":"secret"}`))
		})
		db := newTestDB(t, handler)
		reader, err := db.ExecuteQueryToRecordsReader(context.Background(),
			appQuery().OrderBy(dal.AscendingField("name")).Limit(1).SelectIntoRecord(nil))
		if err != nil {
			t.Fatal(err)
		}
		rec, err := reader.Next()
		if rec != nil || !errors.Is(err, ErrIndexRequired) {
			t.Fatalf("rec=%v err=%v", rec, err)
		}
		if strings.Contains(err.Error(), "secret") {
			t.Fatal("error leaked the response reason")
		}
		if calls.Load() != 1 {
			t.Fatalf("calls = %d", calls.Load())
		}
	})

	t.Run("mango-pager-refuses-primary-mode", func(t *testing.T) {
		var calls atomic.Int64
		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			t.Error("unexpected request")
		})
		db := newTestDB(t, handler)
		pager, ok := dal.As[MangoPager](db)
		if !ok {
			t.Fatal("MangoPager not reachable")
		}
		for _, query := range []dal.Query{
			appQuery().Limit(1).SelectIntoRecord(nil),
			appQuery().OrderBy(dal.DescendingField("_id")).Limit(1).SelectIntoRecord(nil),
		} {
			if _, err := pager.ReadMangoPage(context.Background(), MangoPageRequest{Query: query}); !errors.Is(err, dal.ErrNotSupported) {
				t.Fatalf("pager error = %v", err)
			}
			if _, err := pager.ReadMangoPage(context.Background(), MangoPageRequest{Query: query, Continuation: "bogus"}); !errors.Is(err, dal.ErrNotSupported) {
				t.Fatalf("pager continuation error = %v; primary cursors must not be Mango bookmarks", err)
			}
		}
		if calls.Load() != 0 {
			t.Fatalf("refused primary pager made %d request(s)", calls.Load())
		}
	})
}

func TestPrimaryEnumerationNativeOrderAndManagementDocs(t *testing.T) {
	fixture := newFakePrimary(
		primaryRow("Zed", nil),
		primaryRow("_design/aaa", map[string]any{"views": map[string]any{}}),
		primaryRow("_design/zzz", map[string]any{"views": map[string]any{}}),
		primaryRow("a..b", nil),
		primaryRow("doc-1", map[string]any{"score": 1}),
		primaryRow("doc-2", map[string]any{"score": 2}),
		primaryRow("doc-3", map[string]any{"score": 3}),
		primaryDeletedRow("doc-deleted"),
		primaryRow("native/slash", nil),
		primaryRow(`quote"id`, nil),
		primaryRow("Ünïcode", nil),
	)
	db := newTestDB(t, fixture.handler(t), func(cfg *Config) { cfg.MaxRowsPerPage = 2 })
	allIDs := []string{"Zed", "a..b", "doc-1", "doc-2", "doc-3", "native/slash", `quote"id`, "Ünïcode"}

	t.Run("management-docs-excluded-before-limit", func(t *testing.T) {
		reader, err := db.ExecuteQueryToRecordsReader(context.Background(), appQuery().Limit(3).SelectIntoRecord(nil))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = reader.Close() }()
		if ids := readPrimaryIDs(t, reader); !reflect.DeepEqual(ids, allIDs[:3]) {
			t.Fatalf("ids = %v; want %v", ids, allIDs[:3])
		}
	})

	t.Run("logical-offset-counts-application-docs-only", func(t *testing.T) {
		reader, err := db.ExecuteQueryToRecordsReader(context.Background(), appQuery().Offset(2).Limit(3).SelectIntoRecord(nil))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = reader.Close() }()
		if ids := readPrimaryIDs(t, reader); !reflect.DeepEqual(ids, allIDs[2:5]) {
			t.Fatalf("ids = %v; want %v", ids, allIDs[2:5])
		}
	})

	t.Run("multiple-physical-pages-no-duplicates", func(t *testing.T) {
		reader, err := db.ExecuteQueryToRecordsReader(context.Background(), appQuery().Limit(20).SelectIntoRecord(nil))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = reader.Close() }()
		records := make([]record.Record, 0, len(allIDs))
		var ids []string
		for {
			rec, err := reader.Next()
			if errors.Is(err, dal.ErrNoMoreRecords) {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			records = append(records, rec)
			ids = append(ids, rec.Key().ID.(string))
		}
		if !reflect.DeepEqual(ids, allIDs) {
			t.Fatalf("ids = %v; want %v", ids, allIDs)
		}
		dalgotest.AssertUniqueKeys(t, records)
		queries, _, _ := fixture.snapshot()
		if len(queries) < 3 {
			t.Fatalf("physical pages = %d; want several", len(queries))
		}
	})

	t.Run("descending-native-order", func(t *testing.T) {
		reader, err := db.ExecuteQueryToRecordsReader(context.Background(), appQuery().OrderBy(dal.DescendingField("_id")).Limit(3).SelectIntoRecord(nil))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = reader.Close() }()
		want := []string{"Ünïcode", `quote"id`, "native/slash"}
		if ids := readPrimaryIDs(t, reader); !reflect.DeepEqual(ids, want) {
			t.Fatalf("ids = %v; want %v", ids, want)
		}
		queries, _, _ := fixture.snapshot()
		last := queries[len(queries)-1]
		if last.Get("descending") != "true" {
			t.Fatalf("descending query = %v", last)
		}
	})

	t.Run("full-descending-order", func(t *testing.T) {
		reader, err := db.ExecuteQueryToRecordsReader(context.Background(), appQuery().OrderBy(dal.DescendingField("_id")).Limit(20).SelectIntoRecord(nil))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = reader.Close() }()
		want := make([]string, len(allIDs))
		for i := range allIDs {
			want[i] = allIDs[len(allIDs)-1-i]
		}
		if ids := readPrimaryIDs(t, reader); !reflect.DeepEqual(ids, want) {
			t.Fatalf("ids = %v; want %v", ids, want)
		}
	})
}

// TestPrimaryInclusiveContinuation deletes exactly the last physical anchor
// between two reads, as an external administrator can. Continuation must stay
// inclusive so the next surviving document is never dropped, while the anchor
// that is still present must be discarded exactly once.
func TestPrimaryInclusiveContinuation(t *testing.T) {
	const maxPage = 2
	cases := []struct {
		name         string
		descending   bool
		deleteAnchor bool
		want         []string
	}{
		{
			name:         "ascending-anchor-deleted",
			deleteAnchor: true,
			want:         []string{"a-1", "a-2", "a-3", "a-4"},
		},
		{
			name:         "descending-anchor-deleted",
			descending:   true,
			deleteAnchor: true,
			want:         []string{"a-4", "a-3", "a-2", "a-1"},
		},
		{
			name: "ascending-present-anchor",
			want: []string{"a-1", "a-2", "a-3", "a-4"},
		},
		{
			name:       "descending-present-anchor",
			descending: true,
			want:       []string{"a-4", "a-3", "a-2", "a-1"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newFakePrimary(
				primaryRow("a-1", nil),
				primaryRow("a-2", nil),
				primaryRow("a-3", nil),
				primaryRow("a-4", nil),
			)
			db := newTestDB(t, fixture.handler(t), func(cfg *Config) { cfg.MaxRowsPerPage = maxPage })
			builder := appQuery().Limit(4)
			if tc.descending {
				builder = builder.OrderBy(dal.DescendingField("_id"))
			}
			query := builder.SelectColumns(dal.Column{Expression: dal.Field("name")})
			reader, err := db.ExecuteQueryToRecordsReader(context.Background(), query)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = reader.Close() }()

			ids := make([]string, 0, len(tc.want))
			for i := 0; i < 2; i++ {
				id, data := nextPrimary(t, reader)
				ids = append(ids, id)
				if !reflect.DeepEqual(data, map[string]any{"name": "name-" + id}) {
					t.Fatalf("record %q projection = %#v", id, data)
				}
			}
			if tc.deleteAnchor {
				// The second record is the last physical row of the first
				// page, so it is the inclusive continuation anchor.
				fixture.delete(ids[1])
			}
			for i := 2; i < len(tc.want); i++ {
				id, data := nextPrimary(t, reader)
				ids = append(ids, id)
				if !reflect.DeepEqual(data, map[string]any{"name": "name-" + id}) {
					t.Fatalf("record %q projection = %#v", id, data)
				}
			}
			if !reflect.DeepEqual(ids, tc.want) {
				t.Fatalf("ids = %v; want %v", ids, tc.want)
			}
			seen := map[string]bool{}
			for _, id := range ids {
				if seen[id] {
					t.Fatalf("document %q returned twice", id)
				}
				seen[id] = true
			}
			for _, limit := range primaryRequestLimits(t, fixture) {
				if limit > maxPage {
					t.Fatalf("request limit = %d; hard page cap is %d", limit, maxPage)
				}
			}
		})
	}
}

func TestPrimaryInclusiveContinuationManagementAndDeletedAnchors(t *testing.T) {
	ctx := context.Background()
	newReader := func(t *testing.T, fixture *fakePrimary) dal.RecordsReader {
		t.Helper()
		db := newTestDB(t, fixture.handler(t), func(cfg *Config) { cfg.MaxRowsPerPage = 2 })
		reader, err := db.ExecuteQueryToRecordsReader(ctx,
			appQuery().Limit(3).SelectColumns(dal.Column{Expression: dal.Field("name")}))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = reader.Close() })
		return reader
	}
	nativeRows := func() []map[string]any {
		return []map[string]any{
			primaryRow("A-1", nil),
			primaryRow("_design/mid", map[string]any{"views": map[string]any{}}),
			primaryRow("b-2", nil),
			primaryDeletedRow("gone"),
			primaryRow("z-3", nil),
		}
	}

	t.Run("present-management-anchor", func(t *testing.T) {
		fixture := newFakePrimary(nativeRows()...)
		reader := newReader(t, fixture)
		first, _ := nextPrimary(t, reader)
		if first != "A-1" {
			t.Fatalf("first = %q", first)
		}
		if ids := readPrimaryIDs(t, reader); !reflect.DeepEqual(ids, []string{"b-2", "z-3"}) {
			t.Fatalf("remaining ids = %v", ids)
		}
	})

	t.Run("management-anchor-deleted", func(t *testing.T) {
		fixture := newFakePrimary(nativeRows()...)
		reader := newReader(t, fixture)
		first, _ := nextPrimary(t, reader)
		if first != "A-1" {
			t.Fatalf("first = %q", first)
		}
		fixture.delete("_design/mid")
		if ids := readPrimaryIDs(t, reader); !reflect.DeepEqual(ids, []string{"b-2", "z-3"}) {
			t.Fatalf("remaining ids = %v", ids)
		}
	})

	t.Run("deleted-row-anchor-disappears", func(t *testing.T) {
		fixture := newFakePrimary(
			primaryRow("A-1", nil),
			primaryDeletedRow("gone"),
			primaryRow("b-2", nil),
		)
		db := newTestDB(t, fixture.handler(t), func(cfg *Config) { cfg.MaxRowsPerPage = 2 })
		reader, err := db.ExecuteQueryToRecordsReader(ctx,
			appQuery().Limit(2).SelectColumns(dal.Column{Expression: dal.Field("name")}))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = reader.Close() }()
		first, _ := nextPrimary(t, reader)
		if first != "A-1" {
			t.Fatalf("first = %q", first)
		}
		fixture.delete("gone")
		second, _ := nextPrimary(t, reader)
		if second != "b-2" {
			t.Fatalf("second = %q; the surviving row after a deleted anchor was lost", second)
		}
	})
}

func TestPrimaryPageCapOne(t *testing.T) {
	ctx := context.Background()

	t.Run("existing-anchor-refuses-explicitly", func(t *testing.T) {
		fixture := newFakePrimary(primaryRow("a-1", nil), primaryRow("a-2", nil))
		db := newTestDB(t, fixture.handler(t), func(cfg *Config) { cfg.MaxRowsPerPage = 1 })
		reader, err := db.ExecuteQueryToRecordsReader(ctx, appQuery().Limit(2).SelectIntoRecord(nil))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = reader.Close() }()
		if id, _ := nextPrimary(t, reader); id != "a-1" {
			t.Fatalf("first = %q", id)
		}
		if _, err = reader.Next(); !errors.Is(err, dal.ErrNotSupported) {
			t.Fatalf("error = %v; a page cap of one cannot advance past a present anchor", err)
		}
		if _, err = reader.Next(); !errors.Is(err, dal.ErrNotSupported) {
			t.Fatalf("terminal error = %v; want a stable explicit refusal", err)
		}
	})

	t.Run("deleted-anchor-still-advances", func(t *testing.T) {
		fixture := newFakePrimary(primaryRow("a-1", nil), primaryRow("a-2", nil))
		db := newTestDB(t, fixture.handler(t), func(cfg *Config) { cfg.MaxRowsPerPage = 1 })
		reader, err := db.ExecuteQueryToRecordsReader(ctx, appQuery().Limit(2).SelectIntoRecord(nil))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = reader.Close() }()
		if id, _ := nextPrimary(t, reader); id != "a-1" {
			t.Fatalf("first = %q", id)
		}
		fixture.delete("a-1")
		if id, _ := nextPrimary(t, reader); id != "a-2" {
			t.Fatalf("second = %q; a cap of one must still keep the next surviving row", id)
		}
	})

	t.Run("single-row-completes", func(t *testing.T) {
		fixture := newFakePrimary(primaryRow("a-1", nil))
		db := newTestDB(t, fixture.handler(t), func(cfg *Config) { cfg.MaxRowsPerPage = 1 })
		reader, err := db.ExecuteQueryToRecordsReader(ctx, appQuery().Limit(1).SelectIntoRecord(nil))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = reader.Close() }()
		if id, _ := nextPrimary(t, reader); id != "a-1" {
			t.Fatalf("first = %q", id)
		}
		if _, err = reader.Next(); !errors.Is(err, dal.ErrNoMoreRecords) {
			t.Fatalf("error = %v; want EOF after the requested row", err)
		}
	})
}

func TestPrimaryContinuationBudgets(t *testing.T) {
	ctx := context.Background()

	t.Run("lookahead-fits-hard-page-cap", func(t *testing.T) {
		fixture := newFakePrimary(
			primaryRow("a-1", nil),
			primaryRow("a-2", nil),
			primaryRow("a-3", nil),
			primaryRow("a-4", nil),
			primaryRow("a-5", nil),
			primaryRow("a-6", nil),
		)
		db := newTestDB(t, fixture.handler(t), func(cfg *Config) { cfg.MaxRowsPerPage = 3 })
		reader, err := db.ExecuteQueryToRecordsReader(ctx, appQuery().Offset(1).Limit(4).SelectIntoRecord(nil))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = reader.Close() }()
		if ids := readPrimaryIDs(t, reader); !reflect.DeepEqual(ids, []string{"a-2", "a-3", "a-4", "a-5"}) {
			t.Fatalf("ids = %v; logical offset must survive inclusive continuation", ids)
		}
		limits := primaryRequestLimits(t, fixture)
		for _, limit := range limits {
			if limit > 3 {
				t.Fatalf("request limit = %d; hard page cap is 3", limit)
			}
		}
		if len(limits) < 2 || limits[1] != 3 {
			t.Fatalf("limits = %v; a one-row lookahead must fit the hard cap of 3", limits)
		}
	})

	t.Run("page-cap-refuses-mid-continuation", func(t *testing.T) {
		fixture := newFakePrimary(
			primaryRow("a-1", nil),
			primaryRow("a-2", nil),
			primaryRow("a-3", nil),
			primaryRow("a-4", nil),
		)
		db := newTestDB(t, fixture.handler(t), func(cfg *Config) {
			cfg.MaxRowsPerPage = 2
			cfg.MaxPages = 2
		})
		reader, err := db.ExecuteQueryToRecordsReader(ctx, appQuery().Limit(4).SelectIntoRecord(nil))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = reader.Close() }()
		nextPrimary(t, reader)
		nextPrimary(t, reader)
		nextPrimary(t, reader)
		if _, err = reader.Next(); !errors.Is(err, ErrTooManyPages) {
			t.Fatalf("error = %v; want an explicit page-budget refusal, never EOF", err)
		}
		queries, _, _ := fixture.snapshot()
		if len(queries) != 2 {
			t.Fatalf("physical pages = %d; want 2", len(queries))
		}
	})

	t.Run("byte-budget-includes-continuation-anchor-and-lookahead", func(t *testing.T) {
		text := strings.Repeat("x", 500)
		fixture := newFakePrimary(
			primaryRow("a-1", map[string]any{"text": text}),
			primaryRow("a-2", map[string]any{"text": text}),
			primaryRow("a-3", map[string]any{"text": text}),
		)
		db := newTestDB(t, fixture.handler(t), func(cfg *Config) {
			cfg.MaxRowsPerPage = 2
			cfg.MaxTotalBytes = 1500
		})
		reader, err := db.ExecuteQueryToRecordsReader(ctx, appQuery().Limit(3).SelectIntoRecord(nil))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = reader.Close() }()
		for i := 0; i < 2; i++ {
			if _, err = reader.Next(); err != nil {
				t.Fatalf("record %d: %v", i, err)
			}
		}
		if _, err = reader.Next(); !errors.Is(err, ErrResponseTooLarge) {
			t.Fatalf("error = %v; want the continuation page charged against the byte budget", err)
		}
		queries, _, _ := fixture.snapshot()
		if len(queries) != 2 {
			t.Fatalf("physical pages = %d; want 2", len(queries))
		}
	})
}

func TestPrimaryProjectionsAndRecordsets(t *testing.T) {
	fixture := newFakePrimary(
		primaryRow("doc-1", map[string]any{"score": 10}),
		primaryRow("doc-2", map[string]any{"score": 20}),
	)
	db := newTestDB(t, fixture.handler(t))
	ctx := context.Background()

	t.Run("id-only-records", func(t *testing.T) {
		reader, err := db.ExecuteQueryToRecordsReader(ctx, appQuery().Limit(1).SelectColumns(dal.Column{Expression: dal.Field("_id")}))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = reader.Close() }()
		rec, err := reader.Next()
		if err != nil {
			t.Fatal(err)
		}
		if rec.Key().ID != "doc-1" {
			t.Fatalf("key = %v", rec.Key().ID)
		}
		if data := dataOf(t, rec); len(data) != 0 {
			t.Fatalf("id-only data = %#v", data)
		}
	})

	t.Run("id-only-recordset", func(t *testing.T) {
		reader, err := db.ExecuteQueryToRecordsetReader(ctx, appQuery().Limit(1).SelectColumns(dal.Column{Expression: dal.Field("_id")}))
		if err != nil {
			t.Fatal(err)
		}
		rs := reader.Recordset()
		if names := columnNames(rs); !reflect.DeepEqual(names, []string{"_id"}) {
			t.Fatalf("columns = %v", names)
		}
		if rs.GetColumnByName("_rev") != nil {
			t.Fatal("unrequested _rev column was fabricated")
		}
		id, err := rs.GetRow(0).GetValueByName("_id", rs)
		if err != nil || id != "doc-1" {
			t.Fatalf("_id = %#v err=%v", id, err)
		}
	})

	t.Run("explicit-rev", func(t *testing.T) {
		reader, err := db.ExecuteQueryToRecordsetReader(ctx, appQuery().Limit(1).
			SelectColumns(dal.Column{Expression: dal.Field("_rev")}, dal.Column{Expression: dal.Field("name")}))
		if err != nil {
			t.Fatal(err)
		}
		rs := reader.Recordset()
		if names := columnNames(rs); !reflect.DeepEqual(names, []string{"_rev", "name"}) {
			t.Fatalf("columns = %v", names)
		}
		row := rs.GetRow(0)
		rev, err := row.GetValueByName("_rev", rs)
		if err != nil || rev != "1-doc-1" {
			t.Fatalf("_rev = %#v err=%v", rev, err)
		}
		if name, _ := row.GetValueByName("name", rs); name != "name-doc-1" {
			t.Fatalf("name = %#v", name)
		}
	})

	t.Run("unprojected-metadata", func(t *testing.T) {
		reader, err := db.ExecuteQueryToRecordsetReader(ctx, appQuery().Limit(1).SelectIntoRecord(nil))
		if err != nil {
			t.Fatal(err)
		}
		rs := reader.Recordset()
		if names := columnNames(rs); !reflect.DeepEqual(names, []string{"_rev", "name", "score"}) {
			t.Fatalf("columns = %v", names)
		}
		if rs.GetColumnByName("_id") != nil {
			t.Fatal("unprojected recordset fabricated an _id column")
		}
		row := rs.GetRow(0)
		if score, _ := row.GetValueByName("score", rs); score != int64(10) {
			t.Fatalf("score = %#v", score)
		}
	})

	t.Run("native-alias-collection", func(t *testing.T) {
		aliasDB := newTestDB(t, fixture.handler(t), func(cfg *Config) { cfg.Collection = "orders" })
		reader, err := aliasDB.ExecuteQueryToRecordsReader(ctx, dal.From(dal.NewRootCollectionRef("orders", "")).NewQuery().Limit(1).SelectIntoRecord(nil))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = reader.Close() }()
		rec, err := reader.Next()
		if err != nil {
			t.Fatal(err)
		}
		if rec.Key().Collection() != "orders" {
			t.Fatalf("collection = %s", rec.Key().Collection())
		}
	})
}

func TestPrimaryHostileResponses(t *testing.T) {
	cases := []struct {
		name    string
		payload string
		limit   int
		want    error
	}{
		{"missing-rows", `{}`, 1, ErrMalformedResponse},
		{"rows-null", `{"rows":null}`, 1, ErrMalformedResponse},
		{"rows-object", `{"rows":{}}`, 1, ErrMalformedResponse},
		{"rows-scalar", `{"rows":"nope"}`, 1, ErrMalformedResponse},
		{"empty-rows", `{"rows":[]}`, 1, dal.ErrNoMoreRecords},
		{"missing-id", `{"rows":[{"key":"doc-1","value":{"rev":"1-a"},"doc":{"_id":"doc-1","_rev":"1-a"}}]}`, 1, ErrMalformedResponse},
		{"key-mismatch", `{"rows":[{"id":"doc-1","key":"doc-2","value":{"rev":"1-a"},"doc":{"_id":"doc-1","_rev":"1-a"}}]}`, 1, ErrMalformedResponse},
		{"missing-doc", `{"rows":[{"id":"doc-1","key":"doc-1","value":{"rev":"1-a"}}]}`, 1, ErrMalformedResponse},
		{"doc-null-without-deleted", `{"rows":[{"id":"doc-1","key":"doc-1","value":{"rev":"1-a"},"doc":null}]}`, 1, ErrMalformedResponse},
		{"doc-not-object", `{"rows":[{"id":"doc-1","key":"doc-1","value":{"rev":"1-a"},"doc":"nope"}]}`, 1, ErrMalformedResponse},
		{"doc-id-mismatch", `{"rows":[{"id":"doc-1","key":"doc-1","value":{"rev":"1-a"},"doc":{"_id":"doc-2","_rev":"1-a"}}]}`, 1, ErrMalformedResponse},
		{"missing-rev", `{"rows":[{"id":"doc-1","key":"doc-1","value":{},"doc":{"_id":"doc-1"}}]}`, 1, ErrMalformedResponse},
		{"too-many-rows", `{"rows":[{"id":"doc-1","key":"doc-1","value":{"rev":"1-a"},"doc":{"_id":"doc-1","_rev":"1-a"}},{"id":"doc-2","key":"doc-2","value":{"rev":"1-a"},"doc":{"_id":"doc-2","_rev":"1-a"}}]}`, 1, ErrTooManyRows},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int64
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(tc.payload))
			})
			db := newTestDB(t, handler)
			reader, err := db.ExecuteQueryToRecordsReader(context.Background(), appQuery().Limit(tc.limit).SelectIntoRecord(nil))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = reader.Close() }()
			rec, err := reader.Next()
			if rec != nil || !errors.Is(err, tc.want) {
				t.Fatalf("rec=%v err=%v; want %v", rec, err, tc.want)
			}
			if calls.Load() != 1 {
				t.Fatalf("calls = %d", calls.Load())
			}
		})
	}

	t.Run("anchor-only-page-at-page-cap-one-refused", func(t *testing.T) {
		var calls atomic.Int64
		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			writeJSON(w, http.StatusOK, allDocsPage(map[string]any{"_id": "doc-1", "_rev": "1-a"}))
		})
		db := newTestDB(t, handler, func(cfg *Config) { cfg.MaxRowsPerPage = 1 })
		reader, err := db.ExecuteQueryToRecordsReader(context.Background(), appQuery().Limit(2).SelectIntoRecord(nil))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = reader.Close() }()
		if _, err = reader.Next(); err != nil {
			t.Fatal(err)
		}
		// The second page only repeats the inclusive anchor. With a hard page
		// cap of one there is no room to advance, so the reader must refuse
		// explicitly instead of reporting a false EOF or looping silently.
		if _, err = reader.Next(); !errors.Is(err, dal.ErrNotSupported) {
			t.Fatalf("error = %v; want explicit not-supported refusal", err)
		}
		if _, err = reader.Next(); !errors.Is(err, dal.ErrNotSupported) {
			t.Fatalf("terminal error = %v; want stable refusal", err)
		}
		if calls.Load() != 2 {
			t.Fatalf("calls = %d", calls.Load())
		}
	})

	t.Run("page-before-continuation-rejected", func(t *testing.T) {
		var calls atomic.Int64
		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			switch calls.Load() {
			case 1:
				writeJSON(w, http.StatusOK, allDocsPage(map[string]any{"_id": "doc-1", "_rev": "1-a"}))
			default:
				writeJSON(w, http.StatusOK, allDocsPage(map[string]any{"_id": "doc-0", "_rev": "1-a"}))
			}
		})
		db := newTestDB(t, handler, func(cfg *Config) { cfg.MaxRowsPerPage = 1 })
		reader, err := db.ExecuteQueryToRecordsReader(context.Background(), appQuery().Limit(2).SelectIntoRecord(nil))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = reader.Close() }()
		if _, err = reader.Next(); err != nil {
			t.Fatal(err)
		}
		if _, err = reader.Next(); !errors.Is(err, ErrMalformedResponse) {
			t.Fatalf("error = %v; want ErrMalformedResponse", err)
		}
	})

	t.Run("non-monotonic-page-rejected", func(t *testing.T) {
		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, http.StatusOK, allDocsPage(
				map[string]any{"_id": "doc-2", "_rev": "1-a"},
				map[string]any{"_id": "doc-1", "_rev": "1-b"},
			))
		})
		db := newTestDB(t, handler)
		reader, err := db.ExecuteQueryToRecordsReader(context.Background(), appQuery().Limit(2).SelectIntoRecord(nil))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = reader.Close() }()
		if rec, err := reader.Next(); rec != nil || !errors.Is(err, ErrMalformedResponse) {
			t.Fatalf("rec=%v err=%v; want ErrMalformedResponse", rec, err)
		}
	})

	t.Run("deleted-row-skipped-before-limit", func(t *testing.T) {
		var calls atomic.Int64
		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			if r.URL.Query().Get("startkey") == "" {
				writeJSON(w, http.StatusOK, map[string]any{"rows": []any{primaryDeletedRow("gone")}})
				return
			}
			writeJSON(w, http.StatusOK, map[string]any{"rows": []any{}})
		})
		db := newTestDB(t, handler)
		reader, err := db.ExecuteQueryToRecordsReader(context.Background(), appQuery().Limit(1).SelectIntoRecord(nil))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = reader.Close() }()
		if _, err = reader.Next(); !errors.Is(err, dal.ErrNoMoreRecords) {
			t.Fatalf("error = %v; want ErrNoMoreRecords", err)
		}
		if calls.Load() != 2 {
			t.Fatalf("calls = %d", calls.Load())
		}
	})
}

func TestPrimaryBounds(t *testing.T) {
	t.Run("short-page-is-sticky-eof", func(t *testing.T) {
		var calls atomic.Int64
		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			if calls.Load() > 1 {
				t.Error("a short page must end the operation")
			}
			writeJSON(w, http.StatusOK, allDocsPage(map[string]any{"_id": "doc-1", "_rev": "1-a"}))
		})
		db := newTestDB(t, handler)
		reader, err := db.ExecuteQueryToRecordsReader(context.Background(), appQuery().Limit(2).SelectIntoRecord(nil))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = reader.Close() }()
		if _, err = reader.Next(); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 2; i++ {
			if _, err = reader.Next(); !errors.Is(err, dal.ErrNoMoreRecords) {
				t.Fatalf("Next after EOF call %d = %v; want stable ErrNoMoreRecords", i+1, err)
			}
		}
		if calls.Load() != 1 {
			t.Fatalf("calls = %d", calls.Load())
		}
	})

	t.Run("page-budget-includes-management-docs", func(t *testing.T) {
		fixture := newFakePrimary(
			primaryRow("_design/one", nil),
			primaryRow("_design/two", nil),
			primaryRow("_design/three", nil),
			primaryRow("doc-1", nil),
		)
		db := newTestDB(t, fixture.handler(t), func(cfg *Config) {
			cfg.MaxRowsPerPage = 2
			cfg.MaxPages = 2
		})
		reader, err := db.ExecuteQueryToRecordsReader(context.Background(), appQuery().Limit(1).SelectIntoRecord(nil))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = reader.Close() }()
		rec, err := reader.Next()
		if rec != nil || !errors.Is(err, ErrTooManyPages) {
			t.Fatalf("rec=%v err=%v; want ErrTooManyPages, never a successful short EOF", rec, err)
		}
		queries, _, _ := fixture.snapshot()
		if len(queries) != 2 {
			t.Fatalf("physical pages = %d", len(queries))
		}
	})

	t.Run("offset-work-exhausts-page-budget", func(t *testing.T) {
		rows := make([]map[string]any, 0, 8)
		for i := 1; i <= 8; i++ {
			rows = append(rows, primaryRow(fmt.Sprintf("doc-%d", i), nil))
		}
		fixture := newFakePrimary(rows...)
		db := newTestDB(t, fixture.handler(t), func(cfg *Config) {
			cfg.MaxRowsPerPage = 2
			cfg.MaxPages = 3
		})
		reader, err := db.ExecuteQueryToRecordsReader(context.Background(), appQuery().Offset(7).Limit(1).SelectIntoRecord(nil))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = reader.Close() }()
		if rec, err := reader.Next(); rec != nil || !errors.Is(err, ErrTooManyPages) {
			t.Fatalf("rec=%v err=%v; skipped work must exhaust the page cap explicitly", rec, err)
		}
		queries, _, _ := fixture.snapshot()
		if len(queries) != 3 {
			t.Fatalf("physical pages = %d", len(queries))
		}
	})

	t.Run("total-byte-budget-across-pages", func(t *testing.T) {
		text := strings.Repeat("x", 500)
		fixture := newFakePrimary(
			primaryRow("doc-1", map[string]any{"text": text}),
			primaryRow("doc-2", map[string]any{"text": text}),
			primaryRow("doc-3", map[string]any{"text": text}),
			primaryRow("doc-4", map[string]any{"text": text}),
		)
		db := newTestDB(t, fixture.handler(t), func(cfg *Config) {
			cfg.MaxRowsPerPage = 2
			cfg.MaxTotalBytes = 1500
		})
		reader, err := db.ExecuteQueryToRecordsReader(context.Background(), appQuery().Limit(4).SelectIntoRecord(nil))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = reader.Close() }()
		for i := 0; i < 2; i++ {
			if _, err = reader.Next(); err != nil {
				t.Fatalf("record %d: %v", i, err)
			}
		}
		_, err = reader.Next()
		if !errors.Is(err, ErrResponseTooLarge) {
			t.Fatalf("error = %v; want ErrResponseTooLarge", err)
		}
		queries, _, _ := fixture.snapshot()
		if len(queries) != 2 {
			t.Fatalf("physical pages = %d", len(queries))
		}
	})

	t.Run("row-byte-cap", func(t *testing.T) {
		fixture := newFakePrimary(primaryRow("doc-1", map[string]any{"text": strings.Repeat("x", 64)}))
		db := newTestDB(t, fixture.handler(t), func(cfg *Config) { cfg.MaxRowBytes = 32 })
		reader, err := db.ExecuteQueryToRecordsReader(context.Background(), appQuery().Limit(1).SelectIntoRecord(nil))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = reader.Close() }()
		if rec, err := reader.Next(); rec != nil || !errors.Is(err, ErrRowTooLarge) {
			t.Fatalf("rec=%v err=%v", rec, err)
		}
	})

	t.Run("operation-deadline-spans-pages", func(t *testing.T) {
		var calls atomic.Int64
		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if calls.Add(1) == 1 {
				writeJSON(w, http.StatusOK, allDocsPage(map[string]any{"_id": "doc-1", "_rev": "1-a"}))
				return
			}
			// The second physical page hangs; it must be cancelled by the one
			// operation deadline that started before the first page.
			<-r.Context().Done()
		})
		db := newTestDB(t, handler, func(cfg *Config) {
			cfg.MaxRowsPerPage = 1
			cfg.MaxPages = 5
			cfg.Timeout = 50 * time.Millisecond
		})
		reader, err := db.ExecuteQueryToRecordsReader(context.Background(), appQuery().Limit(3).SelectIntoRecord(nil))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = reader.Close() }()
		if _, err = reader.Next(); err != nil {
			t.Fatalf("first page: %v", err)
		}
		if _, err = reader.Next(); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("error = %v; want one operation deadline across pages", err)
		}
	})
}

func TestPrimaryCancellationAndClose(t *testing.T) {
	t.Run("cancel-between-buffered-rows", func(t *testing.T) {
		var calls atomic.Int64
		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			writeJSON(w, http.StatusOK, allDocsPage(
				map[string]any{"_id": "doc-1", "_rev": "1-a"},
				map[string]any{"_id": "doc-2", "_rev": "1-b"},
			))
		})
		db := newTestDB(t, handler)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		reader, err := db.ExecuteQueryToRecordsReader(ctx, appQuery().Limit(2).SelectIntoRecord(nil))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = reader.Close() }()
		if _, err = reader.Next(); err != nil {
			t.Fatal(err)
		}
		cancel()
		if _, err = reader.Next(); !errors.Is(err, context.Canceled) {
			t.Fatalf("buffered row outlived cancellation: %v", err)
		}
		if calls.Load() != 1 {
			t.Fatalf("calls = %d; cancellation must not fetch another page", calls.Load())
		}
	})

	t.Run("close-during-request", func(t *testing.T) {
		started := make(chan struct{})
		release := make(chan struct{})
		var calls atomic.Int64
		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			close(started)
			select {
			case <-r.Context().Done():
			case <-release:
			}
		})
		db := newTestDB(t, handler)
		reader, err := db.ExecuteQueryToRecordsReader(context.Background(), appQuery().Limit(2).SelectIntoRecord(nil))
		if err != nil {
			t.Fatal(err)
		}
		result := make(chan error, 1)
		go func() {
			_, err := reader.Next()
			result <- err
		}()
		<-started
		if err = reader.Close(); err != nil {
			t.Fatal(err)
		}
		err = <-result
		close(release)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("in-flight request error = %v; want cancellation", err)
		}
		if _, err = reader.Next(); !errors.Is(err, dal.ErrReaderClosed) {
			t.Fatalf("Next after Close = %v", err)
		}
		if calls.Load() != 1 {
			t.Fatalf("calls = %d", calls.Load())
		}
	})
}
