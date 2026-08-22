package objectstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"
)

const (
	ApplicationSHA256Metadata = "application-sha256"
	MaximumListPageObjects    = 1_000
)

var (
	ErrNotFound                     = errors.New("objectstore: object not found")
	ErrPreconditionFailed           = errors.New("objectstore: immutable create precondition failed")
	ErrConditionalCreateUnsupported = errors.New("objectstore: conditional create unsupported")
	ErrProviderDisqualified         = errors.New("objectstore: provider disqualified")
	ErrTransient                    = errors.New("objectstore: transient provider failure")
	ErrPermission                   = errors.New("objectstore: provider permission denied")
	ErrInvalidResponse              = errors.New("objectstore: invalid provider response")
	ErrHashMismatch                 = errors.New("objectstore: application SHA-256 mismatch")
	ErrSizeMismatch                 = errors.New("objectstore: object size mismatch")
	ErrVerificationSourceRequired   = errors.New("objectstore: local verification source required")
	ErrMultipartUnsupported         = errors.New("objectstore: multipart unsupported")
)

type ErrorKind uint8

const (
	ErrorUnknown ErrorKind = iota
	ErrorNotFound
	ErrorPrecondition
	ErrorConditionalUnsupported
	ErrorTransient
	ErrorPermission
	ErrorInvalidResponse
	ErrorMultipartUnsupported
)

type Error struct {
	Op   string
	Key  string
	Kind ErrorKind
	Err  error
}

func (e *Error) Error() string {
	if e.Key == "" {
		return fmt.Sprintf("objectstore: %s: %v", e.Op, e.Err)
	}
	return fmt.Sprintf("objectstore: %s %q: %v", e.Op, e.Key, e.Err)
}

func (e *Error) Unwrap() error { return e.Err }

func (e *Error) Is(target error) bool {
	switch e.Kind {
	case ErrorNotFound:
		return target == ErrNotFound
	case ErrorPrecondition:
		return target == ErrPreconditionFailed
	case ErrorConditionalUnsupported:
		return target == ErrConditionalCreateUnsupported
	case ErrorTransient:
		return target == ErrTransient
	case ErrorPermission:
		return target == ErrPermission
	case ErrorInvalidResponse:
		return target == ErrInvalidResponse
	case ErrorMultipartUnsupported:
		return target == ErrMultipartUnsupported
	default:
		return false
	}
}

func errorWithKind(op, key string, kind ErrorKind, err error) error {
	if err == nil {
		err = errors.New("provider operation failed")
	}
	return &Error{Op: op, Key: key, Kind: kind, Err: err}
}

type ObjectInfo struct {
	Key          string
	Size         int64
	Metadata     map[string]string
	ETag         string
	LastModified time.Time
}

type ListedObject struct {
	Key          string
	Size         int64
	LastModified time.Time
}

type ListPage struct {
	Objects   []ListedObject
	NextToken string
}

type PutObject struct {
	Key    string
	Body   io.ReadSeeker
	Size   int64
	SHA256 [32]byte
}

type UploadedPart struct {
	Number int32
	ETag   string
}

// Client is the provider-neutral immutable object contract. PutIfAbsent and
// CompleteMultipart must apply If-None-Match: * semantics; neither may replace
// an existing key.
type Client interface {
	Head(context.Context, string) (ObjectInfo, error)
	PutIfAbsent(context.Context, PutObject) error
	Get(context.Context, string) (io.ReadCloser, error)
	GetRange(context.Context, string, int64, int64) (io.ReadCloser, error)
	// List returns at most MaximumListPageObjects. NextToken resumes strictly
	// after the returned page; callers may re-fetch a page and resume by offset.
	List(context.Context, string, string) (ListPage, error)
	StartMultipart(context.Context, string, [32]byte) (string, error)
	UploadPart(context.Context, string, string, int32, io.Reader, int64) (UploadedPart, error)
	CompleteMultipart(context.Context, string, string, []UploadedPart) error
	AbortMultipart(context.Context, string, string) error
	ReconcileMultipart(context.Context, string) error
}

// ImmutableCreateReconciler is the explicit provider-specific alternative
// required when native conditional create is unavailable. Implementations must
// atomically bind an immutable key to the exact application hash or reconcile
// an existing exact binding; an ordinary unconditional PUT does not qualify.
type ImmutableCreateReconciler interface {
	CreateImmutable(context.Context, PutObject) error
}
