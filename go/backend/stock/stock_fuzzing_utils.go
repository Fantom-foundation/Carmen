package stock

import (
	"encoding/binary"
	"strings"
	"testing"
)

// OpenStockFactory is a factory to open a Stock.
type OpenStockFactory func(directory string) (Stock[int, int], error)

// opType is operation type to be applied to a stock.
type opType byte

const (
	newId opType = iota
	get
	set
	deleteId
	getIds
)

// op is a tuple of opType, index in the stock, and data.
type op struct {
	opType
	index int
	value int
}

// serialise converts the struct to a byte array
// using following format: <opType><index><value>
// The index is exported only for operations get, set and deleteId
// and the data is exported only for operation set.
func (o *op) serialise() []byte {
	b := []byte{byte(o.opType)}
	if o.opType == get || o.opType == set || o.opType == deleteId {
		b = binary.BigEndian.AppendUint16(b, uint16(o.index))
	}
	if o.opType == set {
		b = binary.BigEndian.AppendUint32(b, uint32(o.value))
	}

	return b
}

// parseOperations converts the input byte array
// to the list of operations.
// It is converted from the format:<opType><index><value>
// Value part is parsed only when the opType equals to set.
// Index part is parsed only when the opType equals to get, set, or deleteId.
// This method tries to parse as many those tuples as possible, terminating when no more
// elements are available.
// The index is capped to one 2bytes so there is a chance that
// the fuzzing finds an index matching for combinations of  newId, get, set, deleteId operations.
// Furthermore, this method recognises expensive operations, which is getIds, and it caps
// number of these operations to 20 in total and 3 in row.
// The fuzzing mechanism requires one campaign does not run more than 1s, which is quickly
// broken when an expensive operation is triggered extensively.
func parseOperations(b []byte) []op {
	var ops []op
	var expensiveOpTotal, expensiveOpRow int
	for len(b) >= 1 {
		opType := opType(b[0] % 5)
		b = b[1:]
		var index int
		if opType == getIds {
			expensiveOpRow++
			expensiveOpTotal++
			if expensiveOpRow > 3 || expensiveOpTotal > 20 {
				continue
			}
		} else {
			expensiveOpRow = 0
		}
		if opType == get || opType == set || opType == deleteId {
			if len(b) >= 2 {
				// cap index to 1byte not to allocate huge files
				index = int(binary.BigEndian.Uint16(b[0:2]))
				b = b[2:]
			} else {
				return ops
			}
		}
		var value int
		if opType == set {
			if len(b) >= 4 {
				value = int(binary.BigEndian.Uint32(b[0:4]))
				b = b[4:]
			} else {
				return ops
			}
		}

		ops = append(ops, op{opType, index, value})
	}
	return ops
}

func FuzzStock_RandomOps(f *testing.F, factory OpenStockFactory, shouldClose bool) {

	payload1 := 99
	payload2 := ^99
	empty := 0

	// generate some adhoc sequences of operations
	data := [][]op{
		{op{newId, 0, empty}, op{set, 0, payload1}, op{get, 0, empty}, op{deleteId, 0, empty}},
		{op{newId, 0, empty}, op{newId, 0, empty}, op{set, 0, payload1}, op{set, 1, payload2},
			op{get, 0, empty}, op{get, 1, empty}, op{deleteId, 0, empty}, op{deleteId, 1, empty}},
		{op{newId, 0, empty}, op{newId, 0, empty}, op{deleteId, 0, empty}, op{deleteId, 1, empty}},
		{op{newId, 0, empty}, op{newId, 0, empty}, op{getIds, 0, empty}},
		{op{newId, 0, empty}, op{newId, 0, empty}, op{set, 1, payload2}, op{get, 1, empty}},
		{op{newId, 0, empty}, op{deleteId, 0, empty}, op{newId, 0, empty}, op{set, 0, payload1}, op{get, 0, empty}},
		{op{get, 0, empty}, op{set, 0, payload1}, op{deleteId, 0, empty}},
		{op{deleteId, 0, empty}, op{get, 0, empty}, op{set, 0, payload1}, op{newId, 0, empty}},
	}

	for _, line := range data {
		var raw []byte
		for _, op := range line {
			raw = append(raw, op.serialise()...)
		}
		f.Add(raw)
	}

	f.Fuzz(func(t *testing.T, rawData []byte) {
		path := t.TempDir() + "/test.dat"
		st, err := factory(path)
		if err != nil {
			t.Fatalf("failed to open buffered file: %v", err)
		}
		if shouldClose {
			defer st.Close()
		}

		ids := make(map[int]bool)
		values := make(map[int]int)

		ops := parseOperations(rawData)
		for _, op := range ops {
			switch op.opType {
			case newId:
				id, err := st.New()
				if err != nil {
					t.Errorf("error to generate new ID: %s", err)
				}
				// mark the id was generated, and check it has not been yet used
				_, exists := ids[id]
				if exists {
					t.Errorf("Stock generated ID that was already genereated and not released: %d", id)
				}
				ids[id] = true
			case set:
				if err := st.Set(op.index, op.value); err != nil {
					_, wasGenerated := ids[op.index]
					if !wasGenerated && strings.HasPrefix(err.Error(), "index out of range") {
						// OK state - cannot set at index that was not created before
						continue
					}
					t.Errorf("cannot set: %d -> %d, err: %s", op.index, op.value, err)
				}
				// insert into shadow map only when the ID was generated
				if _, exists := ids[op.index]; exists {
					values[op.index] = op.value
				}
			case get:
				val, err := st.Get(op.index)
				if err != nil {
					t.Errorf("cannot get: %d -> %d, err: %s", op.index, op.value, err)
				}
				if want, exists := values[op.index]; exists && want != val {
					t.Errorf("value set before does not match returned value: %d != %d", val, values[op.index])
				}
			case deleteId:
				// allow for deleting only IDs that were generated
				// - this is on purpose not checked in stock implementation.
				if _, exists := ids[op.index]; exists {
					if err := st.Delete(op.index); err != nil {
						t.Errorf("error to delete index: %s", err)
					}
					delete(ids, op.index)
					delete(values, op.index)
				}
			case getIds:
				set, err := st.GetIds()
				if err != nil {
					t.Errorf("cannot get Ids: %s", err)
				}
				for id := range ids {
					if !set.Contains(id) {
						t.Errorf("set does not contain ID: %d, generated: %v", id, ids)
					}
				}
			default:
				t.Fatalf("unknown op: %v", op.opType)
			}
		}

	})
}
