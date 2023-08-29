package shared

import (
	"fmt"
	"sync"
)

// The shared package provides generic utility classes for synchronizing access
// to shared objects. When accessing shared objects, users have to define
// whether the access requires shared access (read-only) or exclusive access.
// Furthermore, users have to mark the end of their access to allow potentially
// waiting users to gain access.

// Shared is a wrapper type for a value of type T controlling access to it.
type Shared[T any] struct {
	value T
	mu    sync.RWMutex
}

// MakeShared creates a new shared object and initializes it with the given value.
func MakeShared[T any](value T) *Shared[T] {
	return &Shared[T]{
		value: value,
		mu:    sync.RWMutex{},
	}
}

// TryGetReadHandle tries to get read-only access to the shared value. If
// successful, indicated by the second return value, shared access to the object
// is granted until the provided ReadHandle is released again. Other readers
// may as well gain access concurrently. However, the implementation gurantees
// that no write accesses are granted at the same time.
// NOTE: if the operation is sucessful the provided handle needs to be released.
func (p *Shared[T]) TryGetReadHandle() (ReadHandle[T], bool) {
	succ := p.mu.TryRLock()
	if succ {
		return ReadHandle[T]{p}, true
	}
	return ReadHandle[T]{}, false
}

// GetReadHandle blocks until read-only access to the shared value can be
// granted. Once granted, other threads may have concurrent access to the shared
// value. However, the implementation gurantees that no write accesses are granted
// at the same time.
// NOTE: this operation blocks until access can be granted. The resulting handle
// must be released once access is no longer needed.
func (p *Shared[T]) GetReadHandle() ReadHandle[T] {
	p.mu.RLock()
	return ReadHandle[T]{p}
}

// TryGetWriteHandle tries to get exclusive write access to the shared value. If
// successful, indicated by the second return value, exculsive access to the object
// is granted until the provided WriteHandle is released again. Until then no other
// readers or writers have access to the shared object.
// NOTE: if the operation is sucessful the provided handle needs to be released.
func (p *Shared[T]) TryGetWriteHandle() (WriteHandle[T], bool) {
	succ := p.mu.TryLock()
	if succ {
		return WriteHandle[T]{p}, true
	}
	return WriteHandle[T]{}, false
}

// GetWriteHandle blocks until exclusive write access to the shared value can be
// granted. Once granted, no other read or write access is granted at the same
// time until the granted write access is released.
// NOTE: this operation blocks until access can be granted. The resulting handle
// must be released once access is no longer needed.
func (p *Shared[T]) GetWriteHandle() WriteHandle[T] {
	p.mu.Lock()
	return WriteHandle[T]{p}
}

// ReadHandle represents shared read-access to a shared value of type T. While
// the read handle is valid no exclusive write access to the shared value is
// granted. However, other readers may have access at the same time.
// To gain read access to a shared value call the Shared value's GetReadHandle()
// or TryGetReadHandle() functions.
type ReadHandle[T any] struct {
	shared *Shared[T]
}

// Valid returns true if this handle is representing an active read-only access
// to an underlying shared object, false otherwise. Default initialized handles
// are invalid.
func (h *ReadHandle[T]) Valid() bool {
	return h.shared != nil
}

// Get returns the underlying shared value. Must only be called on valid handles.
func (h *ReadHandle[T]) Get() T {
	return h.shared.value
}

// Release abandons the access permission on the underlying shared object, allowing
// other operations to gain access. It must be called eventually on all valid
// instances to avoid dead-lock situations. After the handle has been released,
// the handle becomes invalid.
func (h *ReadHandle[T]) Release() {
	h.shared.mu.RUnlock()
	h.shared = nil
}

func (h *ReadHandle[T]) String() string {
	return fmt.Sprintf("ReadHandle(%p)", h.shared)
}

// WriteHandle represents exculsive write access to a shared value of type T. While
// the write handle is valid no other access to the shared value is granted.
// To gain write access to a shared value call the Shared value's GetWriteHandle()
// or TryGetWriteHandle() functions.
type WriteHandle[T any] struct {
	shared *Shared[T]
}

// Valid returns true if this handle is representing an active read-only access
// to an underlying shared object, false otherwise. Default initialized handles
// are invalid.
func (h *WriteHandle[T]) Valid() bool {
	return h.shared != nil
}

// Get returns the underlying shared value. Must only be called on valid handles.
func (h *WriteHandle[T]) Get() T {
	return h.shared.value
}

// Ref returns a pointer to the shared value. Must only be called on valid handles.
func (h *WriteHandle[T]) Ref() *T {
	return &h.shared.value
}

// Set updates the shared value. Must only be called on valid handles.
func (h *WriteHandle[T]) Set(value T) {
	h.shared.value = value
}

// Release abandons the access permission on the underlying shared object, allowing
// other operations to gain access. It must be called eventually on all valid
// instances to avoid dead-lock situations. After the handle has been released,
// the handle becomes invalid.
func (h *WriteHandle[T]) Release() {
	h.shared.mu.Unlock()
	h.shared = nil
}

func (h *WriteHandle[T]) String() string {
	return fmt.Sprintf("WriteHandle(%p)", h.shared)
}
