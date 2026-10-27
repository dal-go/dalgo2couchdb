package dalgo2couchdb

import (
	"context"
	"errors"
	"sort"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/recordset"
	"github.com/dal-go/record"
)

// ExecuteQueryToRecordsReader returns a lazily paged reader over native
// application documents. Each Mango page is bounded and bookmarked; the
// reader never fetches more rows than the query limit or the configured page,
// byte and page budgets allow.
func (d *database) ExecuteQueryToRecordsReader(ctx context.Context, query dal.Query) (dal.RecordsReader, error) {
	if ctx == nil {
		return nil, errors.New("couchdb: context is required")
	}
	p, err := d.makePlan(query)
	if err != nil {
		return nil, err
	}
	return &recordsReader{ctx: ctx, db: d, plan: p, skip: p.offset}, nil
}

type recordsReader struct {
	ctx          context.Context
	db           *database
	plan         plan
	page         []record.Record
	pos          int
	bookmark     string
	skip         int
	pages        int
	total        int
	networkBytes int64
	rowBytes     int64
	exhausted    bool
	closed       bool
}

var _ dal.RecordsReader = (*recordsReader)(nil)

func (r *recordsReader) Next() (record.Record, error) {
	if r.closed {
		return nil, dal.ErrReaderClosed
	}
	if err := r.ctx.Err(); err != nil {
		r.close()
		return nil, safeContextError(r.ctx, err)
	}
	if r.pos < len(r.page) {
		rec := r.page[r.pos]
		r.pos++
		return rec, nil
	}
	if r.total >= r.plan.limit || r.exhausted {
		return nil, dal.ErrNoMoreRecords
	}
	if r.pages >= r.db.cfg.MaxPages {
		r.close()
		return nil, ErrTooManyPages
	}
	pageSize := r.plan.pageSize
	if remaining := r.plan.limit - r.total; remaining < pageSize {
		pageSize = remaining
	}
	docs, bookmark, exhausted, err := r.db.fetchMangoPage(r.ctx, r.plan, pageSize, r.skip, r.bookmark, &r.networkBytes)
	if err != nil {
		r.close()
		return nil, err
	}
	r.pages++
	r.skip = 0
	r.bookmark = bookmark
	r.exhausted = exhausted || len(docs) == 0
	converted := make([]record.Record, 0, len(docs))
	var pageBytes int64
	for _, doc := range docs {
		rec, err := r.db.recordFromDoc(doc, r.plan)
		if err != nil {
			r.close()
			return nil, err
		}
		size, err := encodedSize(rec.Data())
		if err != nil {
			r.close()
			return nil, malformed("document is not JSON-encodable")
		}
		if pageBytes > r.db.cfg.MaxTotalBytes-int64(size) {
			r.close()
			return nil, ErrResponseTooLarge
		}
		pageBytes += int64(size)
		converted = append(converted, rec)
	}
	if r.total+len(converted) > r.plan.limit {
		r.close()
		return nil, ErrTooManyRows
	}
	if r.rowBytes > r.db.cfg.MaxTotalBytes-pageBytes {
		r.close()
		return nil, ErrResponseTooLarge
	}
	r.rowBytes += pageBytes
	r.total += len(converted)
	r.page = converted
	r.pos = 0
	if len(r.page) == 0 {
		return nil, dal.ErrNoMoreRecords
	}
	rec := r.page[0]
	r.pos = 1
	return rec, nil
}

func (r *recordsReader) Cursor() (string, error) {
	if r.closed {
		return "", dal.ErrReaderClosed
	}
	return "", nil
}

func (r *recordsReader) Close() error {
	r.close()
	return nil
}

func (r *recordsReader) close() {
	r.closed = true
	r.page = nil
	r.pos = 0
}

// ExecuteQueryToRecordsetReader materializes the bounded result and exposes it
// as a DALgo recordset. A projected query keeps its declared output columns;
// an unprojected query uses the sorted union of the returned native fields.
func (d *database) ExecuteQueryToRecordsetReader(ctx context.Context, query dal.Query, options ...recordset.Option) (dal.RecordsetReader, error) {
	recordsReader, err := d.ExecuteQueryToRecordsReader(ctx, query)
	if err != nil {
		return nil, err
	}
	p, err := d.makePlan(query)
	if err != nil {
		_ = recordsReader.Close()
		return nil, err
	}
	records, err := dal.ReadAllToRecords(ctx, recordsReader)
	if err != nil {
		return nil, err
	}
	columns := recordsetColumns(records, p)
	definitions := make([]recordset.Column[any], len(columns))
	for i, name := range columns {
		definitions[i] = recordset.NewTypedColumn[any](name, nil)
	}
	name := d.collection
	if configured := recordset.NewOptions(options...).Name(); configured != "" {
		name = configured
	}
	result := recordset.NewColumnarRecordset(name, definitions...)
	for _, rec := range records {
		rowData := rec.Data().(map[string]any)
		row := result.NewRow()
		for _, column := range columns {
			if err = row.SetValueByName(column, rowData[column], result); err != nil {
				return nil, err
			}
		}
	}
	return &recordsetReader{ctx: ctx, recordset: result}, nil
}

func recordsetColumns(records []record.Record, p plan) []string {
	if p.hasProjection {
		columns := make([]string, 0, len(p.fields))
		for _, field := range p.fields {
			if field != "_id" {
				columns = append(columns, field)
			}
		}
		return columns
	}
	seen := map[string]bool{}
	for _, rec := range records {
		data, ok := rec.Data().(map[string]any)
		if !ok {
			continue
		}
		for name := range data {
			seen[name] = true
		}
	}
	columns := make([]string, 0, len(seen))
	for name := range seen {
		columns = append(columns, name)
	}
	sort.Strings(columns)
	return columns
}

type recordsetReader struct {
	ctx       context.Context
	recordset recordset.Recordset
	position  int
	closed    bool
}

var _ dal.RecordsetReader = (*recordsetReader)(nil)

func (r *recordsetReader) Recordset() recordset.Recordset { return r.recordset }

func (r *recordsetReader) Next() (recordset.Row, recordset.Recordset, error) {
	if r.closed {
		return nil, r.recordset, dal.ErrReaderClosed
	}
	if r.ctx == nil {
		return nil, r.recordset, errors.New("couchdb: context is required")
	}
	if err := r.ctx.Err(); err != nil {
		return nil, r.recordset, safeContextError(r.ctx, err)
	}
	if r.position >= r.recordset.RowsCount() {
		return nil, r.recordset, dal.ErrNoMoreRecords
	}
	row := r.recordset.GetRow(r.position)
	r.position++
	return row, r.recordset, nil
}

func (r *recordsetReader) Cursor() (string, error) {
	if r.closed {
		return "", dal.ErrReaderClosed
	}
	return "", nil
}

func (r *recordsetReader) Close() error {
	r.closed = true
	return nil
}
