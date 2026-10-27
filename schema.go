package dalgo2couchdb

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/dbschema"
	"github.com/dal-go/record"
)

// The adapter implements dbschema.SchemaReader on the backend, so consumers
// reach it with dal.As[dbschema.SchemaReader](db). It reports the configured
// collection and real Mango indexes. It never infers a declared field schema
// from a sampled document.
var _ dbschema.SchemaReader = (*database)(nil)

type indexCatalogEntry struct {
	Name string `json:"name"`
	Type string `json:"type"`
	Def  struct {
		Fields []map[string]string `json:"fields"`
	} `json:"def"`
}

// ListCollections returns the one configured logical collection. CouchDB
// application documents live directly in a database, so there is no broader
// catalog to browse. A non-nil parent is unsupported.
func (d *database) ListCollections(ctx context.Context, parent *record.Key) ([]dal.CollectionRef, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if parent != nil {
		return nil, schemaNotSupported("ListCollections", "CouchDB has no hierarchical collection catalog")
	}
	return []dal.CollectionRef{dal.NewRootCollectionRef(d.collection, "")}, nil
}

// DescribeCollection reports the configured collection and its native _id
// primary key. Fields are deliberately empty: native CouchDB documents do not
// declare a schema and the adapter will not infer one from sample documents.
func (d *database) DescribeCollection(ctx context.Context, ref *dal.CollectionRef) (*dbschema.CollectionDef, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if err := d.validateCollectionRef(ref); err != nil {
		return nil, err
	}
	return &dbschema.CollectionDef{
		Name:       d.collection,
		PrimaryKey: []dal.FieldName{"_id"},
	}, nil
}

// ListIndexes returns the indexes CouchDB reports for the configured
// database. A failed or forbidden metadata request is returned as an error and
// is never presented as an empty index list.
func (d *database) ListIndexes(ctx context.Context, ref *dal.CollectionRef) ([]dbschema.IndexDef, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if err := d.validateCollectionRef(ref); err != nil {
		return nil, err
	}
	var totalBytes int64
	_, payload, err := d.request(ctx, "GET", "/_index", nil, 200)
	if err != nil {
		return nil, err
	}
	if int64(len(payload)) > d.cfg.MaxTotalBytes {
		return nil, ErrResponseTooLarge
	}
	totalBytes = int64(len(payload))
	var wire struct {
		Indexes json.RawMessage `json:"indexes"`
	}
	if err = decodeJSON(payload, &wire); err != nil {
		return nil, err
	}
	// CouchDB must return an explicit non-null indexes array. A missing field,
	// null, an object or a scalar is a malformed catalog, never "no indexes".
	entries, err := decodeRequiredArray[indexCatalogEntry](wire.Indexes, "index catalog")
	if err != nil {
		return nil, err
	}
	if totalBytes > d.cfg.MaxTotalBytes {
		return nil, ErrResponseTooLarge
	}
	indexes := make([]dbschema.IndexDef, 0, len(entries))
	for _, index := range entries {
		if index.Name == "" || len(index.Name) > 256 {
			return nil, malformed("index name is missing or too long")
		}
		fields := make([]dal.FieldName, 0, len(index.Def.Fields))
		for _, entry := range index.Def.Fields {
			if len(entry) != 1 {
				return nil, malformed("index field entry is not a single field")
			}
			for field := range entry {
				if field == "" || len(field) > 256 || strings.ContainsAny(field, "\x00\r\n") {
					return nil, malformed("index field is not supported")
				}
				fields = append(fields, dal.FieldName(field))
			}
		}
		indexes = append(indexes, dbschema.IndexDef{
			Name:       index.Name,
			Collection: d.collection,
			Fields:     fields,
			Unique:     false,
		})
	}
	return indexes, nil
}

// ListConstraints is unsupported: CouchDB has no SQL-style constraint catalog.
func (d *database) ListConstraints(ctx context.Context, ref *dal.CollectionRef) ([]dbschema.ConstraintDef, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if err := d.validateCollectionRef(ref); err != nil {
		return nil, err
	}
	return nil, schemaNotSupported("ListConstraints", "CouchDB has no declared SQL constraints")
}

// ListReferrers is unsupported: CouchDB has no foreign-key catalog.
func (d *database) ListReferrers(ctx context.Context, ref *dal.CollectionRef) ([]dbschema.Referrer, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if err := d.validateCollectionRef(ref); err != nil {
		return nil, err
	}
	return nil, schemaNotSupported("ListReferrers", "CouchDB has no declared foreign keys")
}

func (d *database) validateCollectionRef(ref *dal.CollectionRef) error {
	if ref == nil || ref.Name() != d.collection || ref.Parent() != nil || ref.Schema() != "" || ref.Database() != "" {
		return schemaNotSupported("DescribeCollection", "only the configured collection is described")
	}
	return nil
}

func schemaNotSupported(op, reason string) error {
	return &dbschema.NotSupportedError{Op: op, Backend: adapterName, Reason: reason}
}

func contextError(ctx context.Context) error {
	if ctx == nil {
		return ErrCanceled
	}
	if err := ctx.Err(); err != nil {
		return safeContextError(ctx, err)
	}
	return nil
}
