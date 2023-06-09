package util

import (
	"sync"
)

type LockedEntry[T any] struct {
	release sync.Once

	Value   T
	Release func()
}

type lockableEntry[T any] struct {
	mtx sync.RWMutex
	val T
}

type LockList[K comparable, V any] struct {
	mtx     sync.RWMutex
	entries map[K]*lockableEntry[V]
}

func NewLockList[K comparable, V any]() *LockList[K, V] {
	return &LockList[K, V]{
		entries: make(map[K]*lockableEntry[V]),
	}
}

func (l *LockList[K, V]) Add(key K, val V) {
	l.mtx.Lock()
	defer l.mtx.Unlock()

	if l.entries[key] != nil {
		panic("tried to add locklist entry that already exists")
	}

	l.entries[key] = &lockableEntry[V]{val: val}
}

func (l *LockList[K, V]) GetFirst(selector func(val V) bool) (K, V, func(), bool) {
	l.mtx.RLock()
	defer l.mtx.RUnlock()

	for k, e := range l.entries {
		e.mtx.Lock()

		if selector(e.val) {
			return k, e.val, e.mtx.Unlock, true
		}

		e.mtx.Unlock()
	}

	var defaultK K
	var defaultV V
	return defaultK, defaultV, func() {}, false
}

func (l *LockList[K, V]) RemoveFirst(selector func(val V) bool) (K, V, func(), bool) {
	l.mtx.Lock()
	defer l.mtx.Unlock()

	for k, e := range l.entries {
		e.mtx.Lock()

		if selector(e.val) {
			delete(l.entries, k)
			return k, e.val, e.mtx.Unlock, true
		}

		e.mtx.Unlock()
	}

	var defaultK K
	var defaultV V
	return defaultK, defaultV, func() {}, false
}

func (l *LockList[K, V]) GetAll() ([]*LockedEntry[V], func()) {
	l.mtx.RLock()
	defer l.mtx.RUnlock()

	results := make([]*LockedEntry[V], 0, len(l.entries))

	for _, e := range l.entries {
		e.mtx.RLock()

		entry := &LockedEntry[V]{
			Value: e.val,
		}
		entry.Release = func() {
			entry.release.Do(func() {
				e.mtx.RUnlock()
			})
		}

		results = append(results, entry)
	}

	cancel := func() {
		for _, e := range results {
			e.Release()
		}
	}

	return results, cancel
}

func (l *LockList[K, V]) Len() int {
	l.mtx.RLock()
	defer l.mtx.RUnlock()

	return len(l.entries)
}
