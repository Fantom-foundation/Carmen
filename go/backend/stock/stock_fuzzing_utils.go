//
// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the GNU Lesser General Public Licence v3 
//

package stock

import (
	"encoding/binary"
	"github.com/Fantom-foundation/Carmen/go/fuzzing"
	"strings"
)

func FuzzStockRandomOps(f fuzzing.TestingF, factory OpenStockFactory, shouldClose bool) {
	fuzzing.Fuzz[stockFuzzContext](f, &stockFuzzCampaign{shouldClose, factory})
}

// OpenStockFactory is a factory to open a Stock.
type OpenStockFactory func(directory string) (Stock[int, int], error)

// opType is operation type to be applied to a stock.
type opType byte

func (o opType) serialize() []byte {
	b := make([]byte, 1, 7) // one byte for type + uint16 for position + uint32 for value
	b[0] = byte(o)
	return b
}

const (
	newId opType = iota
	get
	set
	deleteId
	getIds
)

type opSet struct {
	index int
	value int
}

func (op *opSet) Serialize() []byte {
	b := set.serialize()
	b = binary.BigEndian.AppendUint16(b, uint16(op.index))
	b = binary.BigEndian.AppendUint32(b, uint32(op.value))
	return b
}

func (op *opSet) Apply(t fuzzing.TestingT, c *stockFuzzContext) {
	if err := c.stock.Set(op.index, op.value); err != nil {
		_, wasGenerated := c.ids[op.index]
		if !wasGenerated && strings.HasPrefix(err.Error(), "index out of range") {
			// OK state - cannot set at index that was not created before
			return
		}
		t.Errorf("cannot set: %d -> %d, err: %s", op.index, op.value, err)
	}
	// insert into shadow map only when the ID was generated
	if _, exists := c.ids[op.index]; exists {
		c.values[op.index] = op.value
	}
}

type opGet struct {
	index int
}

func (op *opGet) Serialize() []byte {
	return binary.BigEndian.AppendUint16(get.serialize(), uint16(op.index))
}

func (op *opGet) Apply(t fuzzing.TestingT, c *stockFuzzContext) {
	val, err := c.stock.Get(op.index)
	if err != nil {
		t.Errorf("cannot get: %d -> %d, err: %s", op.index, val, err)
	}
	if want, exists := c.values[op.index]; exists && want != val {
		t.Errorf("value set before does not match returned value: %d != %d", val, c.values[op.index])
	}
}

type opDeleteId struct {
	index int
}

func (op *opDeleteId) Serialize() []byte {
	return binary.BigEndian.AppendUint16(deleteId.serialize(), uint16(op.index))
}

func (op *opDeleteId) Apply(t fuzzing.TestingT, c *stockFuzzContext) {
	// allow for deleting only IDs that were generated
	// - this is on purpose not checked in stock implementation.
	if _, exists := c.ids[op.index]; exists {
		if err := c.stock.Delete(op.index); err != nil {
			t.Errorf("error to delete index: %s", err)
		}
		delete(c.ids, op.index)
		delete(c.values, op.index)
	}
}

type opGetIds struct {
}

func (op *opGetIds) Serialize() []byte {
	return getIds.serialize()
}

func (op *opGetIds) Apply(t fuzzing.TestingT, c *stockFuzzContext) {
	set, err := c.stock.GetIds()
	if err != nil {
		t.Errorf("cannot get Ids: %s", err)
	}
	for id := range c.ids {
		if !set.Contains(id) {
			t.Errorf("set does not contain ID: %d, generated: %v", id, c.ids)
		}
	}
}

type opNewId struct {
}

func (op *opNewId) Serialize() []byte {
	return newId.serialize()
}

func (op *opNewId) Apply(t fuzzing.TestingT, c *stockFuzzContext) {
	id, err := c.stock.New()
	if err != nil {
		t.Errorf("error to generate new ID: %s", err)
	}
	// mark the id was generated, and check it has not been yet used
	_, exists := c.ids[id]
	if exists {
		t.Errorf("Stock generated ID that was already genereated and not released: %d", id)
	}
	c.ids[id] = true
}

// stockFuzzContext carries the stock under test and shadow list of generated IDs
// and values.
type stockFuzzContext struct {
	stock  Stock[int, int]
	ids    map[int]bool
	values map[int]int
}

// stockFuzzCampaign initialises fuzzing campaign of Stock implementations.
type stockFuzzCampaign struct {
	shouldClose bool
	factory     OpenStockFactory
}

func (c *stockFuzzCampaign) Init() []fuzzing.OperationSequence[stockFuzzContext] {
	payload1 := 99
	payload2 := ^99

	// generate some adhoc sequences of operations
	data := []fuzzing.OperationSequence[stockFuzzContext]{
		{&opNewId{}, &opSet{0, payload1}, &opGet{0}, &opDeleteId{0}},
		{&opNewId{}, &opNewId{}, &opSet{0, payload1}, &opSet{1, payload2},
			&opGet{0}, &opGet{1}, &opDeleteId{0}, &opDeleteId{1}},
		{&opNewId{}, &opNewId{}, &opDeleteId{0}, &opDeleteId{1}},
		{&opNewId{}, &opNewId{}, &opGetIds{}},
		{&opNewId{}, &opNewId{}, &opSet{1, payload2}, &opGet{1}},
		{&opNewId{}, &opDeleteId{0}, &opNewId{}, &opSet{0, payload1}, &opGet{0}},
		{&opGet{0}, &opSet{0, payload1}, &opDeleteId{0}},
		{&opDeleteId{0}, &opGet{0}, &opSet{0, payload1}, &opNewId{}},
	}

	return data
}

func (c *stockFuzzCampaign) CreateContext(t fuzzing.TestingT) *stockFuzzContext {
	path := t.TempDir() + "/test.dat"
	st, err := c.factory(path)
	if err != nil {
		t.Fatalf("failed to open buffered file: %v", err)
	}

	ids := make(map[int]bool)
	values := make(map[int]int)

	return &stockFuzzContext{st, ids, values}
}

func (c *stockFuzzCampaign) Deserialize(rawData []byte) []fuzzing.Operation[stockFuzzContext] {
	return parseOperations(rawData)
}

func (c *stockFuzzCampaign) Cleanup(t fuzzing.TestingT, context *stockFuzzContext) {
	if c.shouldClose {
		if err := context.stock.Close(); err != nil {
			t.Fatalf("cannot close file: %s", err)
		}
	}
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
func parseOperations(b []byte) []fuzzing.Operation[stockFuzzContext] {
	var ops []fuzzing.Operation[stockFuzzContext]
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

		var op fuzzing.Operation[stockFuzzContext]
		switch opType {
		case get:
			op = &opGet{index}
		case set:
			op = &opSet{index, value}
		case deleteId:
			op = &opDeleteId{index}
		case newId:
			op = &opNewId{}
		case getIds:
			op = &opGetIds{}
		}

		ops = append(ops, op)
	}
	return ops
}
