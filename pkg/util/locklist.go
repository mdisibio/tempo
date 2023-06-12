package util

import (
	"sync"
)

type LockedEntry[T any] struct {
	release sync.Once

	Value   T
	Release func() // Release the lock on the element when finished.
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

// Add element to the list in an unlocked state. Panics if the element already exists.
func (l *LockList[K, V]) Add(key K, val V) {
	l.mtx.Lock()
	defer l.mtx.Unlock()

	if l.entries[key] != nil {
		panic("tried to add locklist entry that already exists")
	}

	l.entries[key] = &lockableEntry[V]{val: val}
}

// Get element with the given key and return it under read lock. When finished
// the element must be released by calling the returned unlock func().
func (l *LockList[K, V]) Get(key K) (V, func(), bool) {
	l.mtx.RLock()
	defer l.mtx.RUnlock()

	e := l.entries[key]
	if e == nil {
		var defaultV V
		return defaultV, func() {}, false
	}

	e.mtx.RLock()
	return e.val, e.mtx.RUnlock, true
}

// Remove element with the given key and after acquiring its write lock. Panics
// if the element does not exist.
func (l *LockList[K, V]) Remove(key K) (V, bool) {
	l.mtx.Lock()
	defer l.mtx.Unlock()

	e := l.entries[key]
	if e == nil {
		panic("tried to remove locklist entry that did not exist")
	}

	e.mtx.Lock()
	delete(l.entries, key)

	return e.val, true
}

// GetFirst element that matches the given selector and return it under read lock.
// When finished the element must be released by calling the returned unlock func().
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

// RemoveFirst element that matches the given selector and returns it
// acquiring its write lock.
func (l *LockList[K, V]) RemoveFirst(selector func(val V) bool) (K, V, bool) {
	l.mtx.Lock()
	defer l.mtx.Unlock()

	for k, e := range l.entries {
		e.mtx.Lock()

		if selector(e.val) {
			delete(l.entries, k)
			return k, e.val, true
		}

		e.mtx.Unlock()
	}

	var defaultK K
	var defaultV V
	return defaultK, defaultV, false
}

// GetAll elements in the list with read locks.  Individual elements can be unlocked by
// calling element.Release(). An overall unlock func() is also returned which can be deferred
// to ensure unlocking any remaining elements.
func (l *LockList[K, V]) GetAll() ([]*LockedEntry[V], func()) {
	l.mtx.RLock()
	defer l.mtx.RUnlock()

	results := make([]*LockedEntry[V], 0, len(l.entries))

	for _, e := range l.entries {
		// Uniquify the loop variable
		e := e

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
