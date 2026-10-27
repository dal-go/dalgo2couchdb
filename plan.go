package dalgo2couchdb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"time"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/record"
)

// plan is the bounded, validated Mango request shape. It is constructed before
// any network request so unsupported, malformed and over-budget queries cannot
// reach CouchDB.
type plan struct {
	selector      map[string]any
	columns       []string
	fields        []string
	sort          []map[string]string
	limit         int
	offset        int
	pageSize      int
	hasProjection bool
	identity      string
}

// findRequest is the fixed Mango request body. allow_fallback is always false:
// the adapter never lets CouchDB silently fall back to an implicit primary
// scan. A query that needs an index must be served by one or fail explicitly.
type findRequest struct {
	Selector      map[string]any      `json:"selector"`
	Fields        []string            `json:"fields,omitempty"`
	Sort          []map[string]string `json:"sort,omitempty"`
	Limit         int                 `json:"limit"`
	Skip          int                 `json:"skip,omitempty"`
	Bookmark      string              `json:"bookmark,omitempty"`
	AllowFallback bool                `json:"allow_fallback"`
}

type findResponse struct {
	Docs     []map[string]any
	Bookmark string
	Warning  string
}

// decodeRequiredArray accepts only an explicitly present JSON array. An absent
// or null field, an object, a scalar or a nil slice is a malformed response,
// never an empty success. An explicit [] is a legitimate empty array.
func decodeRequiredArray[T any](raw json.RawMessage, what string) ([]T, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil, malformed(what + " is missing a JSON array")
	}
	if trimmed[0] != '[' {
		return nil, malformed(what + " is not a JSON array")
	}
	var values []T
	if err := decodeJSON(trimmed, &values); err != nil {
		return nil, err
	}
	if values == nil {
		return nil, malformed(what + " is not a JSON array")
	}
	return values, nil
}

// decodeFindResponse decodes a Mango find body. CouchDB must return an explicit
// non-null docs array; {}, null and {"docs":null} are errors rather than empty
// successful results.
func decodeFindResponse(payload []byte) (findResponse, error) {
	var wire struct {
		Docs     json.RawMessage `json:"docs"`
		Bookmark string          `json:"bookmark"`
		Warning  string          `json:"warning"`
	}
	if err := decodeJSON(payload, &wire); err != nil {
		return findResponse{}, err
	}
	docs, err := decodeRequiredArray[map[string]any](wire.Docs, "find response docs")
	if err != nil {
		return findResponse{}, err
	}
	return findResponse{Docs: docs, Bookmark: wire.Bookmark, Warning: wire.Warning}, nil
}

func (d *database) makePlan(query dal.Query) (plan, error) {
	if query == nil {
		return plan{}, notSupported("couchdb requires a structured query")
	}
	q, ok := query.(dal.StructuredQuery)
	if !ok || q.From() == nil || q.From().Base() == nil {
		return plan{}, notSupported("couchdb supports structured single-collection queries only")
	}
	if len(q.From().Joins()) != 0 {
		return plan{}, notSupported("couchdb joins are not supported")
	}
	if len(q.GroupBy()) != 0 || q.Having() != nil {
		return plan{}, notSupported("couchdb grouping and aggregation are not supported")
	}
	if q.StartFrom() != "" || q.StartAfter() != "" {
		return plan{}, notSupported("couchdb DALgo query cursors are not supported; use MangoPager")
	}
	if q.IDKind() != reflect.Invalid {
		return plan{}, notSupported("couchdb keys-only queries are not supported")
	}
	ref, ok := q.From().Base().(dal.CollectionRef)
	if !ok || ref.Parent() != nil || ref.Schema() != "" || ref.Database() != "" || ref.Alias() != "" || ref.ScanLimit() != 0 || len(ref.ScanOrders()) != 0 {
		return plan{}, notSupported("couchdb supports one unqualified root collection")
	}
	if ref.Name() != d.collection {
		return plan{}, notSupported("couchdb queries target only the configured collection")
	}
	if q.Limit() <= 0 {
		return plan{}, notSupported("couchdb requires a positive bounded limit")
	}
	if q.Limit() > d.cfg.MaxRows {
		return plan{}, notSupported("couchdb query limit exceeds MaxRows")
	}
	if q.Offset() < 0 || q.Offset() > d.cfg.MaxOffset {
		return plan{}, notSupported("couchdb query offset is outside the configured bound")
	}
	pageSize := d.cfg.MaxRowsPerPage
	if q.Limit() < pageSize {
		pageSize = q.Limit()
	}
	if requiredPages(q.Limit(), pageSize) > d.cfg.MaxPages {
		return plan{}, notSupported("couchdb query needs more pages than MaxPages")
	}

	p := plan{
		selector: map[string]any{},
		limit:    q.Limit(),
		offset:   q.Offset(),
		pageSize: pageSize,
	}
	var err error
	budget := 0
	if p.selector, err = condition(q.Where(), 0, &budget); err != nil {
		return plan{}, err
	}

	seenColumns := map[string]bool{}
	for _, column := range q.Columns() {
		if column.Wildcard != nil {
			return plan{}, notSupported("couchdb wildcard projections are not supported")
		}
		field, ok := column.Expression.(dal.FieldRef)
		if !ok {
			return plan{}, notSupported("couchdb projections must name simple fields")
		}
		name, err := mapField(field)
		if err != nil {
			return plan{}, err
		}
		if column.Alias != "" && column.Alias != field.Name() {
			return plan{}, notSupported("couchdb projection aliases are not supported")
		}
		if seenColumns[name] {
			return plan{}, notSupported("couchdb duplicate projection fields are not supported")
		}
		seenColumns[name] = true
		p.columns = append(p.columns, name)
	}
	if len(p.columns) > 0 {
		p.hasProjection = true
		// columns is the caller's declared output, in order. fields is the
		// wider Mango projection: it adds the native identity and _rev
		// metadata needed to build records even when they were not declared.
		p.fields = append(p.fields, p.columns...)
		if !seenColumns["_id"] {
			p.fields = append([]string{"_id"}, p.fields...)
		}
		if !seenColumns["_rev"] {
			p.fields = append(p.fields, "_rev")
		}
	}

	direction := ""
	seenSort := map[string]bool{}
	for _, order := range q.OrderBy() {
		if order == nil {
			return plan{}, notSupported("couchdb order expressions must name simple fields")
		}
		field, ok := order.Expression().(dal.FieldRef)
		if !ok {
			return plan{}, notSupported("couchdb order expressions must name simple fields")
		}
		name, err := mapField(field)
		if err != nil {
			return plan{}, err
		}
		if seenSort[name] {
			return plan{}, notSupported("couchdb duplicate order fields are not supported")
		}
		seenSort[name] = true
		current := "asc"
		if order.Descending() {
			current = "desc"
		}
		if direction != "" && direction != current {
			return plan{}, notSupported("couchdb Mango requires one sort direction for all order fields")
		}
		direction = current
		p.sort = append(p.sort, map[string]string{name: current})
	}

	p.identity = d.identity(p)
	return p, nil
}

func requiredPages(limit, pageSize int) int {
	if pageSize <= 0 {
		return 0
	}
	return (limit + pageSize - 1) / pageSize
}

func mapField(field dal.FieldRef) (string, error) {
	if field.Source() != "" {
		return "", notSupported("couchdb qualified fields are not supported")
	}
	name := field.Name()
	if field.IsID() || name == "_id" || name == "__name__" {
		return "_id", nil
	}
	if name == "_rev" {
		return "_rev", nil
	}
	if !reFieldName.MatchString(name) || strings.HasPrefix(name, "_") {
		return "", notSupported("couchdb supports simple top-level field names only")
	}
	return name, nil
}

func condition(value dal.Condition, depth int, budget *int) (map[string]any, error) {
	*budget++
	if value == nil {
		return map[string]any{}, nil
	}
	if depth > 8 || *budget > 64 {
		return nil, notSupported("couchdb condition tree exceeds the configured complexity bound")
	}
	switch c := value.(type) {
	case dal.GroupCondition:
		operator := ""
		switch c.Operator() {
		case dal.And:
			operator = "$and"
		case dal.Or:
			operator = "$or"
		default:
			return nil, notSupported("couchdb supports AND/OR condition groups only")
		}
		if len(c.Conditions()) == 0 {
			return nil, notSupported("couchdb empty condition groups are not supported")
		}
		children := make([]any, 0, len(c.Conditions()))
		for _, child := range c.Conditions() {
			if child == nil {
				return nil, notSupported("couchdb nil conditions are not supported")
			}
			mapped, err := condition(child, depth+1, budget)
			if err != nil {
				return nil, err
			}
			children = append(children, mapped)
		}
		return map[string]any{operator: children}, nil
	case dal.Comparison:
		field, ok := c.Left.(dal.FieldRef)
		if !ok {
			return nil, notSupported("couchdb comparisons must name a field on the left")
		}
		name, err := mapField(field)
		if err != nil {
			return nil, err
		}
		operator, err := mangoOperator(c.Operator)
		if err != nil {
			return nil, err
		}
		switch right := c.Right.(type) {
		case dal.Constant:
			if c.Operator == dal.In || c.Operator == dal.NotIn {
				return nil, notSupported("couchdb IN filters require an array value")
			}
			normalized, err := normalizeConstant(right.Value)
			if err != nil {
				return nil, err
			}
			return map[string]any{name: map[string]any{operator: normalized}}, nil
		case dal.Array:
			if c.Operator != dal.In && c.Operator != dal.NotIn {
				return nil, notSupported("couchdb array values require IN or NOT IN")
			}
			values, err := normalizeArray(right.Value)
			if err != nil {
				return nil, err
			}
			return map[string]any{name: map[string]any{operator: values}}, nil
		default:
			return nil, notSupported("couchdb comparison values must be literals")
		}
	case dal.IsNullCondition:
		field, ok := c.Operand().(dal.FieldRef)
		if !ok {
			return nil, notSupported("couchdb IS NULL requires a field")
		}
		name, err := mapField(field)
		if err != nil {
			return nil, err
		}
		operator := "$eq"
		if c.Negated() {
			operator = "$ne"
		}
		return map[string]any{name: map[string]any{operator: nil}}, nil
	default:
		return nil, notSupported("couchdb condition type is not supported")
	}
}

func mangoOperator(operator dal.Operator) (string, error) {
	switch operator {
	case dal.Equal:
		return "$eq", nil
	case dal.GreaterThen:
		return "$gt", nil
	case dal.GreaterOrEqual:
		return "$gte", nil
	case dal.LessThen:
		return "$lt", nil
	case dal.LessOrEqual:
		return "$lte", nil
	case dal.In:
		return "$in", nil
	case dal.NotIn:
		return "$nin", nil
	default:
		return "", notSupported("couchdb comparison operator is not supported")
	}
}

func normalizeConstant(value any) (any, error) {
	switch v := value.(type) {
	case nil, bool, string:
		return v, nil
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return v, nil
	case float32:
		if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
			return nil, unsafeValue("non-finite numbers are not supported")
		}
		return v, nil
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return nil, unsafeValue("non-finite numbers are not supported")
		}
		return v, nil
	case json.Number:
		if _, err := v.Float64(); err != nil {
			return nil, unsafeValue("invalid JSON number")
		}
		return v, nil
	case time.Time:
		return v.UTC().Format(time.RFC3339Nano), nil
	default:
		return nil, notSupported("couchdb comparison values must be JSON scalars")
	}
}

func normalizeArray(value any) ([]any, error) {
	if value == nil {
		return nil, unsafeValue("array value is nil")
	}
	rv := reflect.ValueOf(value)
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return nil, notSupported("couchdb IN values must be an array")
	}
	if rv.Len() > 100 {
		return nil, notSupported("couchdb IN arrays are limited to 100 values")
	}
	values := make([]any, rv.Len())
	for i := 0; i < rv.Len(); i++ {
		normalized, err := normalizeConstant(rv.Index(i).Interface())
		if err != nil {
			return nil, err
		}
		values[i] = normalized
	}
	return values, nil
}

func (d *database) identity(p plan) string {
	identity := struct {
		Version    int                 `json:"v"`
		Endpoint   string              `json:"endpoint"`
		Collection string              `json:"collection"`
		Selector   map[string]any      `json:"selector"`
		Fields     []string            `json:"fields,omitempty"`
		Sort       []map[string]string `json:"sort,omitempty"`
	}{
		Version:    1,
		Endpoint:   d.baseURL,
		Collection: d.collection,
		Selector:   p.selector,
		Fields:     p.fields,
		Sort:       p.sort,
	}
	encoded, err := json.Marshal(identity)
	if err != nil {
		// Every component was validated as JSON-encodable before this point.
		return ""
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

type continuationPayload struct {
	Version  int    `json:"v"`
	Identity string `json:"i"`
	Bookmark string `json:"b"`
}

func makeContinuation(identity, bookmark string) string {
	payload, _ := json.Marshal(continuationPayload{Version: 1, Identity: identity, Bookmark: bookmark})
	return base64.RawURLEncoding.EncodeToString(payload)
}

func parseContinuation(identity, token string) (string, error) {
	if token == "" || len(token) > 8192 {
		return "", ErrContinuationMismatch
	}
	decoded, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || len(decoded) > 6144 {
		return "", ErrContinuationMismatch
	}
	var payload continuationPayload
	if err = decodeJSON(decoded, &payload); err != nil {
		return "", ErrContinuationMismatch
	}
	if payload.Version != 1 || payload.Identity != identity || payload.Bookmark == "" || len(payload.Bookmark) > 4096 {
		return "", ErrContinuationMismatch
	}
	return payload.Bookmark, nil
}

// fetchMangoPage performs exactly one bounded Mango request. totalBytes must
// be the operation's running response-byte counter.
func (d *database) fetchMangoPage(ctx context.Context, p plan, pageSize, skip int, bookmark string, totalBytes *int64) (docs []map[string]any, nextBookmark string, exhausted bool, err error) {
	if pageSize <= 0 || pageSize > d.cfg.MaxRowsPerPage {
		return nil, "", false, notSupported("couchdb page size is outside the configured bound")
	}
	request := findRequest{
		Selector:      p.selector,
		Fields:        p.fields,
		Sort:          p.sort,
		Limit:         pageSize,
		Skip:          skip,
		Bookmark:      bookmark,
		AllowFallback: false,
	}
	_, payload, err := d.request(ctx, "POST", "/_find", request, 200)
	if err != nil {
		return nil, "", false, err
	}
	if len(payload) > 0 {
		if *totalBytes > d.cfg.MaxTotalBytes-int64(len(payload)) {
			return nil, "", false, ErrResponseTooLarge
		}
		*totalBytes += int64(len(payload))
	}
	var response findResponse
	if response, err = decodeFindResponse(payload); err != nil {
		return nil, "", false, err
	}
	if len(response.Docs) > pageSize {
		return nil, "", false, ErrTooManyRows
	}
	exhausted = len(response.Docs) < pageSize || response.Bookmark == ""
	return response.Docs, response.Bookmark, exhausted, nil
}

// MangoPageRequest asks for one bounded native Mango page of a DALgo
// structured query. Continuation must be the token returned by the previous
// page of the same query; it is rejected if the endpoint, collection,
// selector, order or projection changed.
type MangoPageRequest struct {
	Query        dal.Query
	Continuation string
}

// MangoPage is one page of native records plus the bound continuation token
// for the next page, if any.
type MangoPage struct {
	Records      []record.Record
	Continuation string
}

// MangoPager is the optional capability for cross-call Mango pagination. It is
// reachable with dal.As[MangoPager](db).
type MangoPager interface {
	ReadMangoPage(ctx context.Context, request MangoPageRequest) (MangoPage, error)
}

var _ MangoPager = (*database)(nil)

// ReadMangoPage reads one page. Query.Limit is the page size and is capped by
// MaxRowsPerPage. Offset is accepted only on the first page.
func (d *database) ReadMangoPage(ctx context.Context, request MangoPageRequest) (MangoPage, error) {
	p, err := d.makePlan(request.Query)
	if err != nil {
		return MangoPage{}, err
	}
	if p.limit > d.cfg.MaxRowsPerPage {
		return MangoPage{}, notSupported("couchdb page limit exceeds MaxRowsPerPage")
	}
	if request.Continuation != "" && p.offset != 0 {
		return MangoPage{}, notSupported("couchdb continuation cannot be combined with an offset")
	}
	bookmark := ""
	if request.Continuation != "" {
		if bookmark, err = parseContinuation(p.identity, request.Continuation); err != nil {
			return MangoPage{}, err
		}
	}
	var totalBytes int64
	docs, next, exhausted, err := d.fetchMangoPage(ctx, p, p.limit, p.offset, bookmark, &totalBytes)
	if err != nil {
		return MangoPage{}, err
	}
	records := make([]record.Record, 0, len(docs))
	var rowBytes int64
	for _, doc := range docs {
		rec, err := d.recordFromDoc(doc, p)
		if err != nil {
			return MangoPage{}, err
		}
		size, err := encodedSize(rec.Data())
		if err != nil {
			return MangoPage{}, malformed("document is not JSON-encodable")
		}
		if rowBytes > d.cfg.MaxTotalBytes-int64(size) {
			return MangoPage{}, ErrResponseTooLarge
		}
		rowBytes += int64(size)
		records = append(records, rec)
	}
	page := MangoPage{Records: records}
	if !exhausted && next != "" {
		page.Continuation = makeContinuation(p.identity, next)
	}
	return page, nil
}

// recordFromDoc converts one native document into a DALgo record. _id becomes
// the key; _rev and every other projected field stay in the data map.
func (d *database) recordFromDoc(doc map[string]any, p plan) (record.Record, error) {
	id, ok := doc["_id"].(string)
	if !ok || id == "" || !utf8Safe(id, 1024) {
		return nil, malformed("document identity is missing or unsafe")
	}
	if strings.HasPrefix(id, "_") {
		return nil, unsafeValue("management documents are not exposed")
	}
	if _, ok = doc["_rev"].(string); !ok {
		return nil, malformed("document revision is missing")
	}
	data := make(map[string]any, len(doc))
	for key, value := range doc {
		if key == "_id" {
			continue
		}
		data[key] = convertJSONNumbers(value)
	}
	size, err := encodedSize(data)
	if err != nil {
		return nil, malformed("document is not JSON-encodable")
	}
	if int64(size) > d.cfg.MaxRowBytes {
		return nil, ErrRowTooLarge
	}
	return record.NewRecordWithData(record.NewKeyWithID(d.collection, id), data), nil
}

func utf8Safe(value string, maximum int) bool {
	if value == "" || len(value) > maximum {
		return false
	}
	for _, r := range value {
		if r < 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}
