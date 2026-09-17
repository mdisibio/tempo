package cache

import (
	"context"
	"time"

	instr "github.com/grafana/dskit/instrument"
	"github.com/grafana/dskit/services"
)

type Role string

const (
	// eventKeysRemoved is the span event name emitted when cache keys are deleted.
	eventKeysRemoved = "cache.keys.removed"
)

const (
	// individual roles
	RoleNone             Role = "none"
	RoleBloom            Role = "bloom"
	RoleTraceIDIdx       Role = "trace-id-index"
	RoleParquetFooter    Role = "parquet-footer"
	RoleParquetColumnIdx Role = "parquet-column-idx"
	RoleParquetOffsetIdx Role = "parquet-offset-idx"
	RoleFrontendSearch   Role = "frontend-search"
	RoleParquetPage      Role = "parquet-page"
)

// Provider is an object that can return a cache for a requested role
type Provider interface {
	services.Service

	CacheFor(role Role) Cache
	AddCache(role Role, c Cache) error
}

// Cache byte arrays by key.
//
// NB we intentionally do not return errors in this interface - caching is best
// effort by definition.  We found that when these methods did return errors,
// the caller would just log them - so its easier for implementation to do that.
// Whatsmore, we found partially successful Fetchs were often treated as failed
// when they returned an error.
type Cache interface {
	Store(ctx context.Context, key []string, buf [][]byte)
	Remove(ctx context.Context, key []string)
	MaxItemSize() int
	Fetch(ctx context.Context, keys []string) (found []string, bufs [][]byte, missing []string)
	FetchKey(ctx context.Context, key string) (buf []byte, found bool)
	// FetchKeyWithMeta behaves like FetchKey on a hit (found=true). On a miss,
	// it additionally reports whether the caller should go on to Store the
	// value it's about to fetch from the upstream source. Backends that
	// support a "cache only on repeat sighting" admission policy use
	// vivifyTTL as the window in seconds within which a repeat request for
	// the same key counts as a sighting worth caching; shouldStore is false
	// the first time a key is seen within that window (don't cache what might
	// be a one-hit-wonder) and true on any sighting after that. Backends
	// without such a policy always return shouldStore=true on a miss,
	// matching FetchKey's unconditional-cache-on-miss behavior, and can
	// ignore vivifyTTL entirely.
	FetchKeyWithMeta(ctx context.Context, key string, vivifyTTL int32) (buf []byte, found bool, shouldStore bool)
	// Release allows compliant implementations to reclaim buffers back into a pool for memory efficiency
	Release([]byte)
	Stop()
}

func measureRequest(ctx context.Context, method string, col instr.Collector, toStatusCode func(error) string, f func(context.Context) error) error {
	start := time.Now()
	col.Before(ctx, method, start)
	err := f(ctx)
	col.After(ctx, method, toStatusCode(err), start)
	return err
}
