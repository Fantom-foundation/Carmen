package stock

import (
	"encoding/binary"
	"unsafe"

	"github.com/Fantom-foundation/Carmen/go/common"
	"golang.org/x/exp/constraints"
)

// Stock is a collection of fixed-sized, serializable values each associated
// to a unique, Stock-controlled index serving as an identifier.
//
// Stocks mirror a persistent version of a memory-management system: indexes
// are pointers referencing memory locations, while values are the objects
// stored in those memory locations. The Stock interface's `New` operation is
// the allocation function and the `Delete` method the free function. The `Get`
// function corresponds to pointer dereferencing.
//
// I ... the type used to address values in the stock (=index space)
// V ... the type of values stored in the stock
type Stock[I Index, V any] interface {
	// New allocates an ID for a new value to be maintained in the Stock. IDs
	// can be used to retrieve, update, and remove values from the Stock. Freed
	// up IDs may be reassigned.
	New() (I, error)

	// Get retrieves a value associated to an index value. If there is no such
	// element, an undefined value is returned. Stock implementations are not
	// required to retain information on valid and deleted indexes to
	// efficiently distinguish valid from invalid accesses. The life-cycle of
	// IDs is expected to be managed by the client code.
	Get(I) (V, error)

	// Updates the value associated ot the given index. The given index must be
	// alive, created through a New call of a Stock based on the same resources
	// and not released.
	Set(I, V) error

	// Delete removes the value assocaited to the given index. The index may be
	// reused as the result of future New() calls.
	// Indexes may only be deleted once. However, implementations are not
	// required to check this. Releasing the same index multiple times leads
	// to undefined behaviour.
	Delete(I) error

	// Stocks must provide information on their memory footprint.
	common.MemoryFootprintProvider

	// Also, stocks need to be flush and closable.
	common.FlushAndCloser
}

// Index defines the type constraints on Stock index types.
type Index interface {
	constraints.Integer
}

// EncodeIndex encodes an index into a binary form to be persisted.
func EncodeIndex[I Index](index I, trg []byte) {
	switch unsafe.Sizeof(index) {
	case 1:
		trg[0] = byte(index)
	case 2:
		binary.BigEndian.PutUint16(trg, uint16(index))
	case 4:
		binary.BigEndian.PutUint32(trg, uint32(index))
	case 8:
		binary.BigEndian.PutUint64(trg, uint64(index))
	default:
		panic("unsupported index type encountered")
	}
}

// DecodeIndex decodes an index value from its persistent binary form.
func DecodeIndex[I Index](src []byte) I {
	var index I
	switch unsafe.Sizeof(index) {
	case 1:
		return I(src[0])
	case 2:
		return I(binary.BigEndian.Uint16(src))
	case 4:
		return I(binary.BigEndian.Uint32(src))
	case 8:
		return I(binary.BigEndian.Uint64(src))
	default:
		panic("unsupported index type encountered")
	}
}

// ValueEncoder is a utility interface for handling the marshaling of values
// within stock instances. Each value is expected to be encoded into a fixed-
// sized byte array.
type ValueEncoder[V any] interface {
	// The number of bytes required for encoding the value.
	GetEncodedSize() int
	// Store encodes the given value into the given byte slice.
	Store([]byte, *V) error
	// Load restores the value encoded in the given byte slice.
	Load([]byte, *V) error
}
