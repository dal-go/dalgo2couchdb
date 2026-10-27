package dalgo2couchdb

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/dalgotest"
	"github.com/dal-go/dalgo/dbschema"
	"github.com/dal-go/dalgo/recordset"
	"github.com/dal-go/record"
)

// TestRealCouchDB runs against an owned disposable CouchDB service. It is
// skipped unless the environment names a loopback test database and test-only
// administrative/reader credentials.
func TestRealCouchDB(t *testing.T) {
	base, dbName, adminAuth, readerUser, readerAuth, readerPassword := integrationConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	dropDatabase(t, ctx, base, dbName, adminAuth)
	createDatabase(t, ctx, base, dbName, adminAuth)
	ensureReaderUser(t, ctx, base, adminAuth, readerUser, readerPassword)
	grantMemberAccess(t, ctx, base, dbName, adminAuth, readerUser)
	seed := []map[string]any{
		{"_id": "doc-1", "kind": "product", "status": "active", "name": "alpha", "score": 50, "price": 1.5, "active": true, "note": nil, "nullable": nil},
		{"_id": "doc-2", "kind": "product", "status": "active", "name": "bravo", "score": 40, "price": 2.5, "active": false, "note": "n2", "nullable": "n2"},
		{"_id": "doc-3", "kind": "product", "status": "active", "name": "charlie", "score": 30, "price": 3.5, "active": true, "note": nil, "nullable": nil},
		{"_id": "doc-4", "kind": "product", "status": "active", "name": "delta", "score": 20, "price": 4.5, "active": false, "note": "n4", "nullable": "n4"},
		{"_id": "doc-5", "kind": "product", "status": "active", "name": "echo", "score": 10, "price": 5.5, "active": true, "note": nil, "nullable": nil},
		{"_id": "doc-6", "kind": "product", "status": "active", "name": "foxtrot", "score": 5, "price": 6.5, "active": true, "note": nil},
		{"_id": "doc-7", "kind": "product", "status": "archived", "name": "golf", "score": 60, "price": 7.5, "active": true, "note": nil, "nullable": "n7"},
		{"_id": "native/slash", "kind": "other", "status": "other", "name": "slash", "score": 1, "price": 0.5, "active": false, "note": nil},
	}
	bulkDocs(t, ctx, base, dbName, adminAuth, seed)
	createIndex(t, ctx, base, dbName, adminAuth, "score-index", []string{"score"})
	createIndex(t, ctx, base, dbName, adminAuth, "status-score-index", []string{"status", "score"})
	createIndex(t, ctx, base, dbName, adminAuth, "nullable-index", []string{"nullable"})
	createIndex(t, ctx, base, dbName, adminAuth, "status-nullable-index", []string{"status", "nullable"})

	cfg := Config{
		DatabaseURL:       base + "/" + dbName,
		AllowLoopbackHTTP: true,
		MaxRowsPerPage:    2,
		Headers: func(context.Context) (map[string]string, error) {
			return map[string]string{"Authorization": readerAuth}, nil
		},
	}
	db, err := NewDB(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if closer, ok := dal.As[io.Closer](db); ok {
			_ = closer.Close()
		}
	}()

	t.Run("get-exists-getmany", func(t *testing.T) {
		loaded := record.NewRecordWithData(record.NewKeyWithID(dbName, "doc-1"), &map[string]any{})
		if err = db.Get(ctx, loaded); err != nil {
			t.Fatal(err)
		}
		data := *loaded.Data().(*map[string]any)
		if data["name"] != "alpha" || data["score"] != int64(50) || data["price"] != 1.5 || data["active"] != true || data["note"] != nil {
			t.Fatalf("native values = %#v", data)
		}
		if _, ok := data["_rev"]; !ok {
			t.Fatal("_rev metadata missing")
		}
		if _, ok := data["_id"]; ok {
			t.Fatal("_id must be the record key, not duplicated in data")
		}
		if found, err := db.Exists(ctx, record.NewKeyWithID(dbName, "doc-1")); err != nil || !found {
			t.Fatalf("exists=%v err=%v", found, err)
		}
		if found, err := db.Exists(ctx, record.NewKeyWithID(dbName, "missing")); err != nil || found {
			t.Fatalf("exists=%v err=%v", found, err)
		}
		first := record.NewRecordWithData(record.NewKeyWithID(dbName, "doc-1"), &map[string]any{})
		second := record.NewRecordWithData(record.NewKeyWithID(dbName, "doc-2"), &map[string]any{})
		if err = db.GetMulti(ctx, []record.Record{first, second}); err != nil {
			t.Fatal(err)
		}
		if (*first.Data().(*map[string]any))["name"] != "alpha" || (*second.Data().(*map[string]any))["name"] != "bravo" {
			t.Fatal("GetMulti mismatch")
		}
		slash := record.NewRecordWithData(record.NewKeyWithID(dbName, "native/slash"), &map[string]any{})
		if err = db.Get(ctx, slash); err != nil {
			t.Fatalf("native slash ID: %v", err)
		}
		if (*slash.Data().(*map[string]any))["kind"] != "other" {
			t.Fatalf("native slash data = %#v", *slash.Data().(*map[string]any))
		}
	})

	query := func() dal.Query {
		return dal.From(dal.NewRootCollectionRef(dbName, "")).NewQuery().
			WhereField("status", dal.Equal, "active").
			WhereField("score", dal.GreaterOrEqual, 0).
			OrderBy(dal.DescendingField("score")).
			Limit(5).
			SelectColumns(dal.Column{Expression: dal.Field("name")}, dal.Column{Expression: dal.Field("score")})
	}

	t.Run("bounded-mango-pages", func(t *testing.T) {
		reader, err := db.ExecuteQueryToRecordsReader(ctx, query())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = reader.Close() }()
		var records []record.Record
		var scores []int64
		for {
			rec, err := reader.Next()
			if errors.Is(err, dal.ErrNoMoreRecords) {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			records = append(records, rec)
			data := dataOf(t, rec)
			scores = append(scores, data["score"].(int64))
			if data["status"] != nil {
				t.Fatalf("projection leaked status: %#v", data)
			}
			if _, ok := data["name"]; !ok {
				t.Fatalf("projection lost name: %#v", data)
			}
		}
		if len(records) != 5 {
			t.Fatalf("got %d records", len(records))
		}
		if !sort.SliceIsSorted(scores, func(i, j int) bool { return scores[i] > scores[j] }) || scores[0] != 50 || scores[4] != 10 {
			t.Fatalf("scores = %v", scores)
		}
		dalgotest.AssertUniqueKeys(t, records)
	})

	t.Run("projection-keeps-id", func(t *testing.T) {
		idQuery := func() dal.Query {
			return dal.From(dal.NewRootCollectionRef(dbName, "")).NewQuery().
				WhereField("status", dal.Equal, "active").
				WhereField("score", dal.GreaterOrEqual, 0).
				OrderBy(dal.DescendingField("score")).
				Limit(2).
				SelectColumns(dal.Column{Expression: dal.Field("_id")})
		}
		recordsReader, err := db.ExecuteQueryToRecordsReader(ctx, idQuery())
		if err != nil {
			t.Fatal(err)
		}
		rec, err := recordsReader.Next()
		if err != nil {
			t.Fatal(err)
		}
		if rec.Key().ID != "doc-1" {
			t.Fatalf("key ID = %v", rec.Key().ID)
		}
		if data := dataOf(t, rec); len(data) != 0 {
			t.Fatalf("id-only record data = %#v; want no row fields", data)
		}
		_ = recordsReader.Close()

		reader, err := db.ExecuteQueryToRecordsetReader(ctx, idQuery())
		if err != nil {
			t.Fatal(err)
		}
		rs := reader.Recordset()
		names := columnNames(rs)
		if !reflect.DeepEqual(names, []string{"_id"}) {
			t.Fatalf("id-only columns = %v", names)
		}
		row := rs.GetRow(0)
		id, err := row.GetValueByName("_id", rs)
		if err != nil || id != "doc-1" {
			t.Fatalf("_id = %#v err=%v", id, err)
		}

		nameOnly, err := db.ExecuteQueryToRecordsetReader(ctx, dal.From(dal.NewRootCollectionRef(dbName, "")).NewQuery().
			WhereField("status", dal.Equal, "active").
			WhereField("score", dal.GreaterOrEqual, 0).
			OrderBy(dal.DescendingField("score")).
			Limit(2).
			SelectColumns(dal.Column{Expression: dal.Field("name")}))
		if err != nil {
			t.Fatal(err)
		}
		nameRS := nameOnly.Recordset()
		if names = columnNames(nameRS); !reflect.DeepEqual(names, []string{"name"}) {
			t.Fatalf("name-only columns = %v", names)
		}
		if nameRS.GetColumnByName("_rev") != nil {
			t.Fatal("name-only projection fabricated a _rev column")
		}
		nameReader, err := db.ExecuteQueryToRecordsReader(ctx, dal.From(dal.NewRootCollectionRef(dbName, "")).NewQuery().
			WhereField("status", dal.Equal, "active").
			WhereField("score", dal.GreaterOrEqual, 0).
			OrderBy(dal.DescendingField("score")).
			Limit(1).
			SelectColumns(dal.Column{Expression: dal.Field("name")}))
		if err != nil {
			t.Fatal(err)
		}
		nameRecord, err := nameReader.Next()
		if err != nil {
			t.Fatal(err)
		}
		_ = nameReader.Close()
		if data := dataOf(t, nameRecord); !reflect.DeepEqual(data, map[string]any{"name": "alpha"}) {
			t.Fatalf("name-only record data = %#v; want exactly name", data)
		}

		explicitRev, err := db.ExecuteQueryToRecordsetReader(ctx, dal.From(dal.NewRootCollectionRef(dbName, "")).NewQuery().
			WhereField("status", dal.Equal, "active").
			WhereField("score", dal.GreaterOrEqual, 0).
			OrderBy(dal.DescendingField("score")).
			Limit(2).
			SelectColumns(
				dal.Column{Expression: dal.Field("_rev")},
				dal.Column{Expression: dal.Field("name")},
			))
		if err != nil {
			t.Fatal(err)
		}
		revRS := explicitRev.Recordset()
		if names = columnNames(revRS); !reflect.DeepEqual(names, []string{"_rev", "name"}) {
			t.Fatalf("explicit _rev columns = %v", names)
		}
		revRow := revRS.GetRow(0)
		rev, err := revRow.GetValueByName("_rev", revRS)
		if err != nil || rev == nil || rev == "" {
			t.Fatalf("_rev = %#v err=%v", rev, err)
		}
		if name, _ := revRow.GetValueByName("name", revRS); name != "alpha" {
			t.Fatalf("explicit _rev name = %#v", name)
		}

		mixedQuery := dal.From(dal.NewRootCollectionRef(dbName, "")).NewQuery().
			WhereField("status", dal.Equal, "active").
			WhereField("score", dal.GreaterOrEqual, 0).
			OrderBy(dal.DescendingField("score")).
			Limit(2).
			SelectColumns(
				dal.Column{Expression: dal.Field("name")},
				dal.Column{Expression: dal.Field("_id")},
			)
		mixed, err := db.ExecuteQueryToRecordsetReader(ctx, mixedQuery)
		if err != nil {
			t.Fatal(err)
		}
		mixedRS := mixed.Recordset()
		if names = columnNames(mixedRS); !reflect.DeepEqual(names, []string{"name", "_id"}) {
			t.Fatalf("mixed columns = %v", names)
		}
		mixedRow := mixedRS.GetRow(0)
		if id, _ = mixedRow.GetValueByName("_id", mixedRS); id != "doc-1" {
			t.Fatalf("mixed _id = %#v", id)
		}
		if name, _ := mixedRow.GetValueByName("name", mixedRS); name != "alpha" {
			t.Fatalf("mixed name = %#v", name)
		}

		wildcard, err := db.ExecuteQueryToRecordsetReader(ctx, dal.From(dal.NewRootCollectionRef(dbName, "")).NewQuery().
			WhereField("status", dal.Equal, "active").
			WhereField("score", dal.GreaterOrEqual, 0).
			OrderBy(dal.DescendingField("score")).
			Limit(2).
			SelectIntoRecord(nil))
		if err != nil {
			t.Fatal(err)
		}
		wildcardRS := wildcard.Recordset()
		if wildcardRS.GetColumnByName("_id") != nil {
			t.Fatal("wildcard recordset fabricated an _id column instead of using the record key")
		}
		for _, required := range []string{"_rev", "name", "score"} {
			if wildcardRS.GetColumnByName(required) == nil {
				t.Fatalf("wildcard columns %v lost %q", columnNames(wildcardRS), required)
			}
		}
	})

	t.Run("is-null-and-native-null-comparison", func(t *testing.T) {
		activeQuery := func() dal.IQueryBuilder {
			return dal.From(dal.NewRootCollectionRef(dbName, "")).NewQuery().
				WhereField("status", dal.Equal, "active").
				OrderBy(dal.AscendingField("score")).
				Limit(10)
		}
		ids := func(query dal.Query) []string {
			t.Helper()
			reader, err := db.ExecuteQueryToRecordsReader(ctx, query)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = reader.Close() }()
			var ids []string
			for {
				rec, err := reader.Next()
				if errors.Is(err, dal.ErrNoMoreRecords) {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				id, ok := rec.Key().ID.(string)
				if !ok {
					t.Fatalf("key ID is %T, want string", rec.Key().ID)
				}
				ids = append(ids, id)
			}
			return ids
		}

		// Native adapter semantics: an explicit == nil is Mango $eq:null and
		// matches only documents that carry an explicit null, never a document
		// with the field absent (doc-6).
		explicitNull := ids(activeQuery().
			WhereField("nullable", dal.Equal, nil).
			SelectColumns(dal.Column{Expression: dal.Field("_id")}))
		if !reflect.DeepEqual(explicitNull, []string{"doc-5", "doc-3", "doc-1"}) {
			t.Fatalf("explicit null ids = %v", explicitNull)
		}

		// IS NOT NULL is exactly expressible: present and non-null only.
		notNull := ids(activeQuery().
			Where(dal.Field("nullable").IsNotNull()).
			SelectColumns(dal.Column{Expression: dal.Field("_id")}))
		if !reflect.DeepEqual(notNull, []string{"doc-4", "doc-2"}) {
			t.Fatalf("IS NOT NULL ids = %v", notNull)
		}

		// IS NULL must count doc-6 (absent) as NULL as well; Mango cannot
		// serve that through an index, so the adapter refuses before I/O
		// instead of returning explicit nulls only.
		nullQuery := activeQuery().
			Where(dal.Field("nullable").IsNull()).
			SelectColumns(dal.Column{Expression: dal.Field("_id")})
		if _, err := db.ExecuteQueryToRecordsReader(ctx, nullQuery); !errors.Is(err, dal.ErrNotSupported) {
			t.Fatalf("IS NULL error = %v; want ErrNotSupported", err)
		}
		if _, err := db.ExecuteQueryToRecordsetReader(ctx, nullQuery); !errors.Is(err, dal.ErrNotSupported) {
			t.Fatalf("IS NULL recordset error = %v; want ErrNotSupported", err)
		}

		// The exact Mango translation CouchDB would need (explicit null OR
		// absent) has no usable index under allow_fallback:false, which is why
		// the adapter refuses it rather than silently dropping absent fields.
		status, body, err := integrationRequest(ctx, http.MethodPost, base+"/"+dbName+"/_find", readerAuth, "application/json", map[string]any{
			"selector": map[string]any{"$or": []any{
				map[string]any{"nullable": map[string]any{"$eq": nil}},
				map[string]any{"nullable": map[string]any{"$exists": false}},
			}},
			"fields":         []string{"_id"},
			"limit":          10,
			"allow_fallback": false,
		})
		if err != nil {
			t.Fatal(err)
		}
		if status != http.StatusBadRequest || !bytes.Contains(body, []byte(`"invalid_index"`)) {
			t.Fatalf("raw IS NULL translation status=%d body=%s", status, body)
		}

		// IS NOT NULL is served by the real nullable index, not by _all_docs.
		status, body, err = integrationRequest(ctx, http.MethodPost, base+"/"+dbName+"/_explain", readerAuth, "application/json", map[string]any{
			"selector":       map[string]any{"nullable": map[string]any{"$ne": nil}},
			"fields":         []string{"_id"},
			"limit":          10,
			"allow_fallback": false,
		})
		if err != nil {
			t.Fatal(err)
		}
		var explained struct {
			Index struct {
				Name string `json:"name"`
			} `json:"index"`
		}
		if status != http.StatusOK {
			t.Fatalf("explain status=%d body=%s", status, body)
		}
		if err = json.Unmarshal(body, &explained); err != nil {
			t.Fatal(err)
		}
		if explained.Index.Name != "nullable-index" {
			t.Fatalf("IS NOT NULL explain index = %q; want nullable-index", explained.Index.Name)
		}

		// An OR across different fields cannot share one Mango index; the
		// adapter reports the honest ErrIndexRequired rather than scanning.
		orQuery := dal.From(dal.NewRootCollectionRef(dbName, "")).NewQuery().
			Where(dal.NewGroupCondition(dal.Or,
				dal.Field("nullable").IsNotNull(),
				dal.WhereField("score", dal.LessThen, 15))).
			Limit(10).
			SelectIntoRecord(nil)
		orReader, err := db.ExecuteQueryToRecordsReader(ctx, orQuery)
		if err != nil {
			if !errors.Is(err, ErrIndexRequired) {
				t.Fatalf("OR error = %v; want ErrIndexRequired", err)
			}
			return
		}
		defer func() { _ = orReader.Close() }()
		if _, err = orReader.Next(); !errors.Is(err, ErrIndexRequired) {
			t.Fatalf("OR next error = %v; want ErrIndexRequired", err)
		}
	})

	t.Run("recordset", func(t *testing.T) {
		reader, err := db.ExecuteQueryToRecordsetReader(ctx, query(), recordset.WithName("products"))
		if err != nil {
			t.Fatal(err)
		}
		rs := reader.Recordset()
		if rs.Name() != "products" || rs.RowsCount() != 5 || rs.ColumnsCount() != 2 {
			t.Fatalf("name=%q rows=%d columns=%d", rs.Name(), rs.RowsCount(), rs.ColumnsCount())
		}
	})

	t.Run("mango-pager", func(t *testing.T) {
		pager, ok := dal.As[MangoPager](db)
		if !ok {
			t.Fatal("MangoPager not reachable")
		}
		pageQuery := dal.From(dal.NewRootCollectionRef(dbName, "")).NewQuery().
			WhereField("status", dal.Equal, "active").
			WhereField("score", dal.GreaterOrEqual, 10).
			OrderBy(dal.DescendingField("score")).
			Limit(2).
			SelectColumns(dal.Column{Expression: dal.Field("name")}, dal.Column{Expression: dal.Field("score")})
		var all []record.Record
		continuation := ""
		for pageNumber := 0; pageNumber < 5; pageNumber++ {
			page, err := pager.ReadMangoPage(ctx, MangoPageRequest{Query: pageQuery, Continuation: continuation})
			if err != nil {
				t.Fatal(err)
			}
			all = append(all, page.Records...)
			if page.Continuation == "" {
				break
			}
			continuation = page.Continuation
		}
		if len(all) != 5 {
			t.Fatalf("pager returned %d rows", len(all))
		}
		dalgotest.AssertUniqueKeys(t, all)
	})

	t.Run("schema-and-indexes", func(t *testing.T) {
		sr, ok := dal.As[dbschema.SchemaReader](db)
		if !ok {
			t.Fatal("SchemaReader not reachable")
		}
		collections, err := sr.ListCollections(ctx, nil)
		if err != nil || len(collections) != 1 || collections[0].Name() != dbName {
			t.Fatalf("collections=%v err=%v", collections, err)
		}
		ref := dal.NewRootCollectionRef(dbName, "")
		definition, err := sr.DescribeCollection(ctx, &ref)
		if err != nil {
			t.Fatal(err)
		}
		if definition.Name != dbName || len(definition.Fields) != 0 || len(definition.PrimaryKey) != 1 || definition.PrimaryKey[0] != "_id" {
			t.Fatalf("definition = %#v", definition)
		}
		indexes, err := sr.ListIndexes(ctx, &ref)
		if err != nil {
			t.Fatal(err)
		}
		names := make([]string, len(indexes))
		for i, index := range indexes {
			names[i] = index.Name
		}
		if !contains(names, "score-index") || !contains(names, "status-score-index") {
			t.Fatalf("indexes = %v", names)
		}
	})

	t.Run("index-required", func(t *testing.T) {
		q := dal.From(dal.NewRootCollectionRef(dbName, "")).NewQuery().
			WhereField("name", dal.GreaterOrEqual, "").
			OrderBy(dal.AscendingField("name")).
			Limit(2).
			SelectIntoRecord(nil)
		reader, err := db.ExecuteQueryToRecordsReader(ctx, q)
		if err != nil {
			if !errors.Is(err, ErrIndexRequired) {
				t.Fatalf("error = %v", err)
			}
			return
		}
		defer func() { _ = reader.Close() }()
		if _, err = reader.Next(); !errors.Is(err, ErrIndexRequired) {
			t.Fatalf("error = %v", err)
		}
	})

	t.Run("bad-auth", func(t *testing.T) {
		badCfg := cfg
		badCfg.Headers = func(context.Context) (map[string]string, error) {
			return map[string]string{"Authorization": basicAuth("datatug_couchdb_reader", "wrong-password")}, nil
		}
		badDB, err := NewDB(ctx, badCfg)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if closer, ok := dal.As[io.Closer](badDB); ok {
				_ = closer.Close()
			}
		}()
		rec := record.NewRecordWithData(record.NewKeyWithID(dbName, "doc-1"), &map[string]any{})
		err = badDB.Get(ctx, rec)
		var httpErr *HTTPError
		if !errors.As(err, &httpErr) || httpErr.StatusCode != http.StatusUnauthorized {
			t.Fatalf("error = %v", err)
		}
		if strings.Contains(err.Error(), "wrong-password") {
			t.Fatal("error leaked the password")
		}
	})
}

func integrationConfig(t *testing.T) (base, dbName, adminAuth, readerUser, readerAuth, readerPassword string) {
	t.Helper()
	rawURL := os.Getenv("COUCHDB_TEST_URL")
	adminUser := os.Getenv("COUCHDB_TEST_ADMIN_USER")
	adminPassword := os.Getenv("COUCHDB_TEST_ADMIN_PASSWORD")
	readerUser = os.Getenv("COUCHDB_TEST_READER_USER")
	readerPassword = os.Getenv("COUCHDB_TEST_READER_PASSWORD")
	if rawURL == "" || adminUser == "" || adminPassword == "" || readerUser == "" || readerPassword == "" {
		t.Skip("set COUCHDB_TEST_URL and the COUCHDB_TEST_ADMIN_*/COUCHDB_TEST_READER_* credentials for a disposable local CouchDB")
	}
	u, err := url.Parse(rawURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		t.Fatal("COUCHDB_TEST_URL must be a plain database URL without credentials, query or fragment")
	}
	dbName = strings.TrimPrefix(strings.TrimSuffix(u.Path, "/"), "/")
	if !strings.HasPrefix(dbName, "datatug_couchdb_test") {
		t.Fatalf("refusing to use non-disposable database %q", dbName)
	}
	base = u.Scheme + "://" + u.Host
	if status, _, err := integrationRequest(context.Background(), http.MethodGet, base+"/_up", "", "", nil); err != nil || status != http.StatusOK {
		t.Fatalf("CouchDB _up check failed: status=%d err=%v", status, err)
	}
	adminAuth = basicAuth(adminUser, adminPassword)
	readerAuth = basicAuth(readerUser, readerPassword)
	return base, dbName, adminAuth, readerUser, readerAuth, readerPassword
}

func basicAuth(user, password string) string {
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(user+":"+password))
}

func dropDatabase(t *testing.T, ctx context.Context, base, dbName, auth string) {
	t.Helper()
	status, body, err := integrationRequest(ctx, http.MethodDelete, base+"/"+dbName, auth, "", nil)
	if err != nil {
		t.Fatalf("drop database: %v", err)
	}
	if status != http.StatusOK && status != http.StatusNotFound && status != http.StatusAccepted {
		t.Fatalf("drop database: status=%d body=%s", status, body)
	}
}

func createDatabase(t *testing.T, ctx context.Context, base, dbName, auth string) {
	t.Helper()
	status, body, err := integrationRequest(ctx, http.MethodPut, base+"/"+dbName, auth, "", nil)
	if err != nil {
		t.Fatalf("create database: %v", err)
	}
	if status != http.StatusCreated && status != http.StatusAccepted {
		t.Fatalf("create database: status=%d body=%s", status, body)
	}
}

func ensureReaderUser(t *testing.T, ctx context.Context, base, adminAuth, user, password string) {
	t.Helper()
	// System databases are normally present; creating them is idempotent.
	status, _, err := integrationRequest(ctx, http.MethodPut, base+"/_users", adminAuth, "", nil)
	if err != nil {
		t.Fatalf("create _users: %v", err)
	}
	if status != http.StatusCreated && status != http.StatusAccepted && status != http.StatusPreconditionFailed {
		t.Fatalf("create _users: status=%d", status)
	}
	path := base + "/_users/org.couchdb.user:" + url.PathEscape(user)
	status, body, err := integrationRequest(ctx, http.MethodGet, path, adminAuth, "", nil)
	if err != nil {
		t.Fatalf("read user: %v", err)
	}
	var existing map[string]any
	if status == http.StatusOK {
		if err = json.Unmarshal(body, &existing); err != nil {
			t.Fatalf("decode user: %v", err)
		}
	} else if status != http.StatusNotFound {
		t.Fatalf("read user: status=%d", status)
	}
	document := map[string]any{
		"name":     user,
		"password": password,
		"roles":    []any{},
		"type":     "user",
	}
	if rev, ok := existing["_rev"].(string); ok {
		document["_rev"] = rev
	}
	status, body, err = integrationRequest(ctx, http.MethodPut, path, adminAuth, "application/json", document)
	if err != nil {
		t.Fatalf("write user: %v", err)
	}
	if status != http.StatusCreated && status != http.StatusAccepted {
		t.Fatalf("write user: status=%d body=%s", status, body)
	}
}

func grantMemberAccess(t *testing.T, ctx context.Context, base, dbName, adminAuth, readerUser string) {
	t.Helper()
	security := map[string]any{
		"admins":  map[string]any{"names": []any{}, "roles": []any{}},
		"members": map[string]any{"names": []any{readerUser}, "roles": []any{}},
	}
	status, body, err := integrationRequest(ctx, http.MethodPut, base+"/"+dbName+"/_security", adminAuth, "application/json", security)
	if err != nil {
		t.Fatalf("grant database membership: %v", err)
	}
	if status != http.StatusOK {
		t.Fatalf("grant database membership: status=%d body=%s", status, body)
	}
}

func bulkDocs(t *testing.T, ctx context.Context, base, dbName, auth string, docs []map[string]any) {
	t.Helper()
	status, body, err := integrationRequest(ctx, http.MethodPost, base+"/"+dbName+"/_bulk_docs", auth, "application/json", map[string]any{"docs": docs})
	if err != nil {
		t.Fatalf("bulk docs: %v", err)
	}
	if status != http.StatusCreated && status != http.StatusAccepted {
		t.Fatalf("bulk docs: status=%d body=%s", status, body)
	}
	var results []map[string]any
	if err = json.Unmarshal(body, &results); err != nil {
		t.Fatalf("decode bulk docs: %v", err)
	}
	for _, result := range results {
		if errText, ok := result["error"].(string); ok && errText != "" {
			t.Fatalf("bulk docs error: %v", result)
		}
	}
}

func createIndex(t *testing.T, ctx context.Context, base, dbName, auth, name string, fields []string) {
	t.Helper()
	body := map[string]any{"index": map[string]any{"fields": fields}, "name": name, "type": "json"}
	status, response, err := integrationRequest(ctx, http.MethodPost, base+"/"+dbName+"/_index", auth, "application/json", body)
	if err != nil {
		t.Fatalf("create index: %v", err)
	}
	if status != http.StatusOK && status != http.StatusCreated {
		t.Fatalf("create index: status=%d body=%s", status, response)
	}
}

func integrationRequest(ctx context.Context, method, rawURL, auth, contentType string, body any) (int, []byte, error) {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return 0, nil, err
		}
		reader = bytes.NewReader(encoded)
	}
	req, err := http.NewRequestWithContext(ctx, method, rawURL, reader)
	if err != nil {
		return 0, nil, err
	}
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	client := &http.Client{
		Timeout:       15 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	payload, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return resp.StatusCode, nil, err
	}
	return resp.StatusCode, payload, nil
}

func contains(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}
