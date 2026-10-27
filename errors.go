package dalgo2couchdb

import (
	"context"
	"errors"
	"fmt"

	"github.com/dal-go/dalgo/dal"
)

// The adapter returns sentinel errors for conditions callers may need to
// distinguish without parsing text. Every message is deliberately static:
// CouchDB URLs, credentials, headers, request bodies and response bodies are
// never included in an error.
var (
	// ErrRequestTooLarge reports that the Mango request body exceeds the
	// configured byte budget.
	ErrRequestTooLarge = errors.New("couchdb: request exceeds configured byte budget")

	// ErrResponseTooLarge reports that a response body or the retained row data
	// of one operation exceeds a configured byte budget.
	ErrResponseTooLarge = errors.New("couchdb: response exceeds configured byte budget")

	// ErrRowTooLarge reports that one decoded document exceeds MaxRowBytes.
	ErrRowTooLarge = errors.New("couchdb: document exceeds configured row byte budget")

	// ErrTooManyRows reports that a response or operation returned more rows
	// than the configured row budget.
	ErrTooManyRows = errors.New("couchdb: result exceeds configured row budget")

	// ErrTooManyPages reports that completing an operation would need more
	// Mango pages than the configured page budget.
	ErrTooManyPages = errors.New("couchdb: result exceeds configured page budget")

	// ErrMalformedResponse reports a response that is not the expected CouchDB
	// JSON shape. It never includes response text.
	ErrMalformedResponse = errors.New("couchdb: malformed response")

	// ErrUnsafeValue reports an identifier, field, header or constant that the
	// adapter will not send. It never includes the rejected value.
	ErrUnsafeValue = errors.New("couchdb: unsafe value")

	// ErrIndexRequired reports that CouchDB refused a query because no usable
	// Mango index exists for its selector and sort.
	ErrIndexRequired = errors.New("couchdb: query requires a usable Mango index")

	// ErrContinuationMismatch reports a continuation token used with a
	// different endpoint, collection, selector, order or projection.
	ErrContinuationMismatch = errors.New("couchdb: continuation does not belong to this query")

	// ErrHeaderProvider reports that the configured header provider failed.
	// The provider's own error text is not returned because it may contain a
	// credential.
	ErrHeaderProvider = errors.New("couchdb: header provider failed")

	// ErrRequestFailed is the safe, detail-free error returned for a transport
	// failure that is neither a context cancellation nor a deadline.
	ErrRequestFailed = errors.New("couchdb: request failed")

	// ErrDatabaseNotFound reports a missing configured CouchDB database.
	ErrDatabaseNotFound = errors.New("couchdb: database not found")
)

// HTTPError is a safe CouchDB HTTP status error. Only the status code is kept;
// response headers and bodies are discarded.
type HTTPError struct {
	StatusCode int
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("couchdb: request failed with HTTP %d", e.StatusCode)
}

// ErrCanceled and ErrDeadlineExceeded preserve errors.Is semantics for
// context.Canceled and context.DeadlineExceeded without exposing the wrapped
// transport error, whose text may contain the endpoint or credentials.
var (
	ErrCanceled         = fmt.Errorf("couchdb: request canceled: %w", context.Canceled)
	ErrDeadlineExceeded = fmt.Errorf("couchdb: request deadline exceeded: %w", context.DeadlineExceeded)
)

func notSupported(reason string) error {
	return fmt.Errorf("%w: %s", dal.ErrNotSupported, reason)
}

func unsafeValue(reason string) error {
	return fmt.Errorf("%w: %s", ErrUnsafeValue, reason)
}

func malformed(reason string) error {
	return fmt.Errorf("%w: %s", ErrMalformedResponse, reason)
}

// safeContextError maps a transport or reader error to a context sentinel when
// either the error or the context says the operation was canceled or timed out.
// Any other error is replaced by the detail-free ErrRequestFailed.
func safeContextError(ctx context.Context, err error) error {
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return ErrCanceled
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return ErrDeadlineExceeded
		}
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		if errors.Is(ctxErr, context.Canceled) {
			return ErrCanceled
		}
		return ErrDeadlineExceeded
	}
	return ErrRequestFailed
}
