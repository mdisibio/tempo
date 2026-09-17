package cache_test

import (
	"context"
	"sync"
	"time"

	"github.com/grafana/gomemcache/memcache"
)

type mockMemcache struct {
	sync.RWMutex
	contents map[string][]byte
	stubs    map[string]time.Time // key -> vivify-stub expiry, simulates MetaGet's "N" window
}

func newMockMemcache() *mockMemcache {
	return &mockMemcache{
		contents: map[string][]byte{},
	}
}

func (m *mockMemcache) GetMulti(_ context.Context, keys []string, _ ...memcache.Option) (map[string]*memcache.Item, error) {
	m.RLock()
	defer m.RUnlock()
	result := map[string]*memcache.Item{}
	for _, k := range keys {
		if c, ok := m.contents[k]; ok {
			result[k] = &memcache.Item{
				Value: c,
			}
		}
	}
	return result, nil
}

func (m *mockMemcache) Set(item *memcache.Item) error {
	m.Lock()
	defer m.Unlock()
	m.contents[item.Key] = item.Value
	return nil
}

func (m *mockMemcache) Get(key string, _ ...memcache.Option) (*memcache.Item, error) {
	m.RLock()
	defer m.RUnlock()

	if c, ok := m.contents[key]; ok {
		return &memcache.Item{
			Value: c,
		}, nil
	}

	return nil, memcache.ErrCacheMiss
}

// MetaGet is a simplified simulation of the real memcached meta-get vivify
// behavior: a miss with vivifyTTL > 0 creates a short-lived stub the first
// time (FirstMiss), and any request while that stub is still alive reports
// SubsequentMiss instead, without creating another one.
func (m *mockMemcache) MetaGet(key string, vivifyTTL int32) (*memcache.MetaGetResult, error) {
	m.Lock()
	defer m.Unlock()

	if c, ok := m.contents[key]; ok {
		return &memcache.MetaGetResult{Sighting: memcache.Found, Value: c, TTLRemaining: -1}, nil
	}

	if m.stubs == nil {
		m.stubs = map[string]time.Time{}
	}
	if exp, ok := m.stubs[key]; ok && time.Now().Before(exp) {
		return &memcache.MetaGetResult{Sighting: memcache.SubsequentMiss, TTLRemaining: int32(time.Until(exp).Seconds())}, nil
	}

	if vivifyTTL > 0 {
		m.stubs[key] = time.Now().Add(time.Duration(vivifyTTL) * time.Second)
	}
	return &memcache.MetaGetResult{Sighting: memcache.FirstMiss, TTLRemaining: -1}, nil
}

func (m *mockMemcache) Delete(key string) error {
	m.Lock()
	defer m.Unlock()
	delete(m.contents, key)
	return nil
}

func (m *mockMemcache) Close() {
}
