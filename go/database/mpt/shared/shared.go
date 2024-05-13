// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package shared

import (
	"fmt"
	"sync"
)

// The shared package provides generic utility classes for synchronizing access
// to shared hashed objects. Shared hashed objects are expected to contain
// two sets of fields, content fields and hash fields. Content fields contain
// object relevant information while hash fields are derived from content
// fields.
//
// When accessing shared objects, users have to define what level of access is
// required. Four levels are supported:
//  - read access: grants users to read content fields of the object
//  - view access: grants users to read content and hash fields
//  - hash access: grants users to read content and write hash fields
//  - write access: grants write permission to content and hash fields
//
// The following table describes the interaction of the various locking modes:
//
//             want\held  |  None  | Read | View | Hash | Write
//           -------------+--------+------+------+------+-------
//               Read     |    +   |   +  |   +  |   +  |   -
//               View     |    +   |   +  |   +  |   -  |   -
//               Hash     |    +   |   +  |   -  |   -  |   -
//               Write    |    +   |   -  |   -  |   -  |   -
//
// where + means that a request for the targeted permission would be granted
// while - means that the request is blocked or rejected.

// Shared is a wrapper type for a value of type T controlling access to it.
type Shared[T any] struct {
	value        T
	contentMutex sync.RWMutex
	hashMutex    sync.RWMutex
}

// MakeShared creates a new shared object and initializes it with the given value.
func MakeShared[T any](value T) *Shared[T] {
	return &Shared[T]{
		value:        value,
		contentMutex: sync.RWMutex{},
		hashMutex:    sync.RWMutex{},
	}
}

// TryGetReadHandle tries to get read access to the shared value's content. If
// successful, indicated by the second return value, shared access to the object
// is granted until the provided ReadHandle is released again. Other readers,
// viewers, or hashers may as well gain access concurrently. However, the
// implementation guarantees that no write accesses are granted at the same time.
// NOTE: if the operation is successful the provided handle needs to be released.
func (p *Shared[T]) TryGetReadHandle() (ReadHandle[T], bool) {
	succ := p.contentMutex.TryRLock()
	if succ {
		return ReadHandle[T]{handle[T]{p}}, true
	}
	return ReadHandle[T]{}, false
}

// GetReadHandle blocks until read access to the shared value's content can be
// granted. Once granted, other threads may have concurrent access to the shared
// value. However, the implementation guarantees that no write accesses are granted
// at the same time.
// NOTE: this operation blocks until access can be granted. The resulting handle
// must be released once access is no longer needed.
func (p *Shared[T]) GetReadHandle() ReadHandle[T] {
	p.contentMutex.RLock()
	return ReadHandle[T]{handle[T]{p}}
}

// TryGetViewHandle tries to get read access to the shared value's content and
// hash data. If successful, indicated by the second return value, view access
// to the object is granted until the provided ViewHandle is released again.
// Other readers and viewers may as well gain access concurrently. However, the
// implementation guarantees that no hash or write accesses are granted at the
// same time.
// NOTE: if the operation is successful the provided handle needs to be released.
func (p *Shared[T]) TryGetViewHandle() (ViewHandle[T], bool) {
	succ := p.contentMutex.TryRLock()
	if !succ {
		return ViewHandle[T]{}, false
	}
	succ = p.hashMutex.TryRLock()
	if !succ {
		p.contentMutex.RUnlock()
		return ViewHandle[T]{}, false
	}
	return ViewHandle[T]{handle[T]{p}}, true
}

// GetViewHandle blocks until read access to the shared value's content and hash
// data can be granted. Once granted, other threads may have concurrent access to
// the shared value. However, the implementation guarantees that no hash or write
// accesses are granted at the same time.
// NOTE: this operation blocks until access can be granted. The resulting handle
// must be released once access is no longer needed.
func (p *Shared[T]) GetViewHandle() ViewHandle[T] {
	p.contentMutex.RLock()
	p.hashMutex.RLock()
	return ViewHandle[T]{handle[T]{p}}
}

// TryGetHashHandle tries to get read access to the shared value's content and
// write access to the object's hash data. If successful, indicated by the
// second return value, hash access to the object is granted until the provided
// HashHandle is released again. Other readers may as well gain access
// concurrently. However, the implementation guarantees that no view, hash or
// write accesses are granted at the same time.
// NOTE: if the operation is successful the provided handle needs to be released.
func (p *Shared[T]) TryGetHashHandle() (HashHandle[T], bool) {
	succ := p.contentMutex.TryRLock()
	if !succ {
		return HashHandle[T]{}, false
	}
	succ = p.hashMutex.TryLock()
	if !succ {
		p.contentMutex.RUnlock()
		return HashHandle[T]{}, false
	}
	return HashHandle[T]{handle[T]{p}}, true
}

// GetHashHandle blocks until read access to the shared value's content and
// write access to the value's hash data can be granted. Once granted, other
// threads may have concurrent read access to the shared value. However, the
// implementation guarantees that no view, hash or write accesses are granted
// at the same time.
// NOTE: this operation blocks until access can be granted. The resulting handle
// must be released once access is no longer needed.
func (p *Shared[T]) GetHashHandle() HashHandle[T] {
	p.contentMutex.RLock()
	p.hashMutex.Lock()
	return HashHandle[T]{handle[T]{p}}
}

// TryGetWriteHandle tries to get exclusive write access to the shared value. If
// successful, indicated by the second return value, exclusive access to the object
// is granted until the provided WriteHandle is released again. Until then no other
// readers or writers have access to the shared object.
// NOTE: if the operation is successful the provided handle needs to be released.
func (p *Shared[T]) TryGetWriteHandle() (WriteHandle[T], bool) {
	succ := p.contentMutex.TryLock()
	if succ {
		return WriteHandle[T]{handle[T]{p}}, true
	}
	return WriteHandle[T]{}, false
}

// GetWriteHandle blocks until exclusive write access to the shared value can be
// granted. Once granted, no other read or write access is granted at the same
// time until the granted write access is released.
// NOTE: this operation blocks until access can be granted. The resulting handle
// must be released once access is no longer needed.
func (p *Shared[T]) GetWriteHandle() WriteHandle[T] {
	p.contentMutex.Lock()
	return WriteHandle[T]{handle[T]{p}}
}

type handle[T any] struct {
	shared *Shared[T]
}

// Valid returns true if this handle represents an active access permission to
// an underlying shared object, false otherwise. Default initialized handles
// are invalid.
func (h *handle[T]) Valid() bool {
	return h.shared != nil
}

// Get returns the underlying shared value. Must only be called on valid handles.
func (h *handle[T]) Get() T {
	return h.shared.value
}

// ReadHandle represents shared read-access to a shared value of type T. While
// the read handle is valid no exclusive write access to the shared value is
// granted. However, other readers may have access at the same time.
// To gain read access to a shared value call the Shared value's GetReadHandle()
// or TryGetReadHandle() functions.
type ReadHandle[T any] struct {
	handle[T]
}

// Release abandons the access permission on the underlying shared object, allowing
// other operations to gain access. It must be called eventually on all valid
// instances to avoid dead-lock situations. After the handle has been released,
// the handle becomes invalid.
func (h *ReadHandle[T]) Release() {
	h.shared.contentMutex.RUnlock()
	h.shared = nil
}

func (h *ReadHandle[T]) String() string {
	return fmt.Sprintf("ReadHandle(%p)", h.shared)
}

// ViewHandle represents shared read-access to the content and hashes of a shared
// value of type T. While the view handle is valid no exclusive write access to
// the shared value's content is granted. However, other read or view permissions
// may be granted.
// To gain view access to a shared value call the Shared value's GetViewHandle()
// or TryGetViewHandle() functions.
type ViewHandle[T any] struct {
	handle[T]
}

// Release abandons the access permission on the underlying shared object, allowing
// other operations to gain access. It must be called eventually on all valid
// instances to avoid dead-lock situations. After the handle has been released,
// the handle becomes invalid.
func (h *ViewHandle[T]) Release() {
	h.shared.contentMutex.RUnlock()
	h.shared.hashMutex.RUnlock()
	h.shared = nil
}

func (h *ViewHandle[T]) String() string {
	return fmt.Sprintf("ViewHandle(%p)", h.shared)
}

// HashHandle represents shared read-access to the content and exclusive write
// access to hashes of a shared value of type T. While the hash handle is valid
// no read access to the shared values hash data nor exclusive write access to
// the shared value's content is granted. However, read permissions may be
// granted concurrently.
// To gain hash access to a shared value call the Shared value's GetHashHandle()
// or TryGetHashHandle() functions.
type HashHandle[T any] struct {
	handle[T]
}

// Release abandons the access permission on the underlying shared object, allowing
// other operations to gain access. It must be called eventually on all valid
// instances to avoid dead-lock situations. After the handle has been released,
// the handle becomes invalid.
func (h *HashHandle[T]) Release() {
	h.shared.contentMutex.RUnlock()
	h.shared.hashMutex.Unlock()
	h.shared = nil
}

func (h *HashHandle[T]) String() string {
	return fmt.Sprintf("HashHandle(%p)", h.shared)
}

// WriteHandle represents exclusive write access to a shared value of type T. While
// the write handle is valid no other access to the shared value is granted.
// To gain write access to a shared value call the Shared value's GetWriteHandle()
// or TryGetWriteHandle() functions.
type WriteHandle[T any] struct {
	handle[T]
}

// Ref returns a pointer to the shared value. Must only be called on valid handles.
func (h *WriteHandle[T]) Ref() *T {
	return &h.shared.value
}

// Set updates the shared value. Must only be called on valid handles.
func (h *WriteHandle[T]) Set(value T) {
	h.shared.value = value
}

// AsReadHandle obtains a view on this write handle proving read access to the
// shared value. Write access is preserved and must still be released. The
// resulting read access handle must not be released.
// TODO [cleanup]: split access permission proofs and handles
// see: https://github.com/Fantom-foundation/Carmen/issues/719
// See https://github.com/Fantom-foundation/Carmen/issues/719
func (h *WriteHandle[T]) AsReadHandle() ReadHandle[T] {
	return ReadHandle[T]{h.handle}
}

// AsViewHandle obtains a view on this write handle proving view access to the
// shared value. Write access is preserved and must still be released. The
// resulting view access handle must not be released.
// TODO [cleanup]: split access permission proofs and handles
// see: https://github.com/Fantom-foundation/Carmen/issues/719
// See https://github.com/Fantom-foundation/Carmen/issues/719
func (h *WriteHandle[T]) AsViewHandle() ViewHandle[T] {
	return ViewHandle[T]{h.handle}
}

// Release abandons the access permission on the underlying shared object, allowing
// other operations to gain access. It must be called eventually on all valid
// instances to avoid dead-lock situations. After the handle has been released,
// the handle becomes invalid.
func (h *WriteHandle[T]) Release() {
	h.shared.contentMutex.Unlock()
	h.shared = nil
}

func (h *WriteHandle[T]) String() string {
	return fmt.Sprintf("WriteHandle(%p)", h.shared)
}
