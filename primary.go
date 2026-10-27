package dalgo2couchdb

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"unicode/utf8"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/record"
)

// primaryReader enumerates application documents through CouchDB's native
// _all_docs primary index. It is used only for an unfiltered collection read
// with no ordering or a single _id ordering; filtered queries keep the Mango
// path. Physical pages are bounded by MaxRowsPerPage, all pages share one
// operation deadline, and every byte of every page counts against one
// operation budget.
type primaryReader struct {
	ctx    context.Context
	cancel context.CancelFunc
	db     *database
	plan   plan

	page []record.Record
	pos  int

	// skip is the remaining logical application-document offset. Management
	// and deleted rows never consume it.
	skip int
	// total is the number of application records already returned.
	total int
	// pages counts physical primary-index requests, including pages that only
	// carried management documents.
	pages int
	// lastID is the native _id that the next page must continue after.
	lastID string

	networkBytes int64
	rowBytes     int64
	exhausted    bool
	closed       atomic.Bool
	terminal     error
}

var _ dal.RecordsReader = (*primaryReader)(nil)

func (r *primaryReader) Next() (record.Record, error) {
	if r.closed.Load() {
		return nil, dal.ErrReaderClosed
	}
	if r.terminal != nil {
		return nil, r.terminal
	}
	// Recheck before every returned row, not only before a page is fetched:
	// buffered rows must not outlive a caller abort, and the check also runs
	// after record conversion.
	if err := r.ctx.Err(); err != nil {
		return r.fail(safeContextError(r.ctx, err))
	}
	if r.pos < len(r.page) {
		rec := r.page[r.pos]
		r.pos++
		return rec, nil
	}
	for {
		if r.total >= r.plan.limit || r.exhausted {
			return r.eof()
		}
		if r.pages >= r.db.cfg.MaxPages {
			return r.fail(ErrTooManyPages)
		}
		remaining := r.plan.limit - r.total
		need := remaining
		if r.skip > 0 {
			need += r.skip
		}
		pageSize := r.plan.pageSize
		if need < pageSize {
			pageSize = need
		}
		if pageSize <= 0 {
			return r.eof()
		}
		docs, physical, lastID, exhausted, err := r.db.fetchPrimaryPage(r.ctx, r.plan, pageSize, r.lastID, &r.networkBytes)
		if err != nil {
			return r.fail(err)
		}
		r.pages++
		if physical > 0 {
			r.lastID = lastID
		}
		r.exhausted = exhausted || physical == 0
		converted := make([]record.Record, 0, len(docs))
		var pageBytes int64
		for _, doc := range docs {
			if r.skip > 0 {
				r.skip--
				continue
			}
			if len(converted) >= remaining {
				break
			}
			rec, err := r.db.recordFromDoc(doc, r.plan)
			if err != nil {
				return r.fail(err)
			}
			size, err := encodedSize(rec.Data())
			if err != nil {
				return r.fail(malformed("document is not JSON-encodable"))
			}
			if pageBytes > r.db.cfg.MaxTotalBytes-int64(size) {
				return r.fail(ErrResponseTooLarge)
			}
			pageBytes += int64(size)
			converted = append(converted, rec)
		}
		if r.rowBytes > r.db.cfg.MaxTotalBytes-pageBytes {
			return r.fail(ErrResponseTooLarge)
		}
		r.rowBytes += pageBytes
		r.total += len(converted)
		r.page = converted
		r.pos = 0
		if len(converted) > 0 {
			rec := converted[0]
			r.pos = 1
			return rec, nil
		}
		// The whole physical page was management documents (or logical offset
		// rows); continue only while the page and operation budgets allow.
	}
}

func (r *primaryReader) Cursor() (string, error) {
	if r.closed.Load() {
		return "", dal.ErrReaderClosed
	}
	return "", nil
}

func (r *primaryReader) Close() error {
	if r.closed.CompareAndSwap(false, true) {
		r.cancel()
	}
	return nil
}

// eof caches the terminal result and releases the operation context. Without
// caching, a later Next would surface context.Canceled instead of the stable
// dal.ErrNoMoreRecords contract.
func (r *primaryReader) eof() (record.Record, error) {
	r.terminal = dal.ErrNoMoreRecords
	r.page = nil
	r.pos = 0
	r.cancel()
	return nil, r.terminal
}

func (r *primaryReader) fail(err error) (record.Record, error) {
	r.terminal = err
	r.page = nil
	r.pos = 0
	r.cancel()
	return nil, err
}

// allDocsValue is the native row metadata. Deleted rows are normally absent
// from _all_docs; when a live endpoint reports one, value.deleted marks it so
// it can be skipped before the application offset and limit.
type allDocsValue struct {
	Rev     string `json:"rev"`
	Deleted bool   `json:"deleted"`
}

type allDocsWireRow struct {
	ID    string          `json:"id"`
	Key   *string         `json:"key"`
	Value *allDocsValue   `json:"value"`
	Doc   json.RawMessage `json:"doc"`
}

type allDocsWireResponse struct {
	Rows json.RawMessage `json:"rows"`
}

// fetchPrimaryPage performs exactly one bounded native _all_docs request.
// startAfter is the stable native _id continuation: CouchDB returns rows from
// that key inclusive, so skip=1 advances past it without inventing a synthetic
// key. totalBytes is the operation's running response-byte counter.
//
// docs contains application documents only: management (_design/_local) and
// deleted rows are filtered here, before the caller applies the logical offset
// and limit. physical reports how many native rows the page actually carried,
// which is what proves a full page; lastID is the continuation identity.
func (d *database) fetchPrimaryPage(ctx context.Context, p plan, pageSize int, startAfter string, totalBytes *int64) (docs []map[string]any, physical int, lastID string, exhausted bool, err error) {
	if pageSize <= 0 || pageSize > d.cfg.MaxRowsPerPage {
		return nil, 0, "", false, notSupported("couchdb page size is outside the configured bound")
	}
	query := url.Values{}
	query.Set("include_docs", "true")
	query.Set("limit", strconv.Itoa(pageSize))
	if p.descending {
		query.Set("descending", "true")
	}
	if startAfter != "" {
		encoded, encodeErr := json.Marshal(startAfter)
		if encodeErr != nil {
			return nil, 0, "", false, unsafeValue("document ID is not supported")
		}
		query.Set("startkey", string(encoded))
		query.Set("skip", "1")
	}
	_, payload, err := d.requestQuery(ctx, http.MethodGet, "/_all_docs", query, nil, http.StatusOK)
	if err != nil {
		return nil, 0, "", false, err
	}
	if int64(len(payload)) > d.cfg.MaxTotalBytes-*totalBytes {
		return nil, 0, "", false, ErrResponseTooLarge
	}
	*totalBytes += int64(len(payload))

	var response allDocsWireResponse
	if err = decodeJSON(payload, &response); err != nil {
		return nil, 0, "", false, err
	}
	rows, err := decodeRequiredArray[allDocsWireRow](response.Rows, "all_docs response rows")
	if err != nil {
		return nil, 0, "", false, err
	}
	if len(rows) > pageSize {
		return nil, 0, "", false, ErrTooManyRows
	}
	physical = len(rows)
	exhausted = len(rows) < pageSize
	if physical == 0 {
		return nil, 0, "", exhausted, nil
	}
	lastID = rows[physical-1].ID
	if startAfter != "" && rows[0].ID == startAfter {
		// skip=1 must advance past the continuation key. A repeated key would
		// loop forever and could duplicate an application document.
		return nil, 0, "", false, malformed("all_docs pagination did not advance past the continuation key")
	}
	seen := make(map[string]bool, len(rows))
	previousID := ""
	for _, row := range rows {
		id := row.ID
		if id == "" || !utf8.ValidString(id) || len(id) > 1024 {
			return nil, 0, "", false, malformed("all_docs row identity is missing or unsafe")
		}
		if row.Key != nil && *row.Key != id {
			return nil, 0, "", false, malformed("all_docs row key does not match its identity")
		}
		if seen[id] {
			return nil, 0, "", false, malformed("all_docs page repeats a document identity")
		}
		seen[id] = true
		// CouchDB's _all_docs order is native _id order. Values must advance
		// past the continuation key and stay strictly monotonic inside a page;
		// this rejects a page that starts earlier, repeats a row or comes back
		// out of order instead of silently duplicating application documents.
		if startAfter != "" {
			if p.descending {
				if id >= startAfter {
					return nil, 0, "", false, malformed("all_docs pagination did not advance past the continuation key")
				}
			} else if id <= startAfter {
				return nil, 0, "", false, malformed("all_docs pagination did not advance past the continuation key")
			}
		}
		if previousID != "" {
			if p.descending {
				if id >= previousID {
					return nil, 0, "", false, malformed("all_docs page is not in descending native order")
				}
			} else if id <= previousID {
				return nil, 0, "", false, malformed("all_docs page is not in ascending native order")
			}
		}
		previousID = id
		if strings.HasPrefix(id, "_") {
			// Design and local management documents are never exposed and do
			// not consume the application offset or limit.
			continue
		}
		doc, deleted, docErr := decodeAllDocsDocument(row, id)
		if docErr != nil {
			return nil, 0, "", false, docErr
		}
		if deleted {
			continue
		}
		docs = append(docs, doc)
	}
	return docs, physical, lastID, exhausted, nil
}

// decodeAllDocsDocument validates one include_docs row. A null document is a
// deleted row only when the native row says so; a missing or mismatched
// document is a malformed response, never an empty success.
func decodeAllDocsDocument(row allDocsWireRow, id string) (map[string]any, bool, error) {
	trimmed := bytes.TrimSpace(row.Doc)
	if len(trimmed) == 0 {
		return nil, false, malformed("all_docs row is missing its document")
	}
	if bytes.Equal(trimmed, []byte("null")) {
		if row.Value == nil || !row.Value.Deleted {
			return nil, false, malformed("all_docs deleted row is missing its deleted marker")
		}
		return nil, true, nil
	}
	if trimmed[0] != '{' {
		return nil, false, malformed("all_docs row document is not a JSON object")
	}
	var doc map[string]any
	if err := decodeJSON(trimmed, &doc); err != nil {
		return nil, false, err
	}
	if doc == nil {
		return nil, false, malformed("all_docs row document is not a JSON object")
	}
	docID, ok := doc["_id"].(string)
	if !ok || docID != id {
		return nil, false, malformed("all_docs row document identity is missing or mismatched")
	}
	return doc, false, nil
}
