package file

import (
	"encoding/binary"
	"github.com/Fantom-foundation/Carmen/go/backend/stock"
	"github.com/Fantom-foundation/Carmen/go/fuzzing"
	"slices"
	"strings"
	"testing"
)

func FuzzStack_RandomOps(f *testing.F) {
	fuzzing.Fuzz[stackFuzzingContext](f, &stackFuzzingCampaign{})
}

// opType is operation type to be applied to a stack.
type opType byte

const (
	push opType = iota
	pop
	getAll
	size
	empty
	close
	flush
)

// serialise converts the struct to a byte array
// using following format: <opType><value>
// The value is exported only for operation push.
func (o opType) serialise() []byte {
	b := make([]byte, 1, 5)
	b[0] = byte(o)
	return b
}

type stackFuzzingCampaign struct {
}

type stackFuzzingContext struct {
	path   string
	stack  *fileBasedStack[int]
	shadow stack[int]
}

func (c *stackFuzzingCampaign) Init() []fuzzing.OperationSequence[stackFuzzingContext] {
	payload1 := 99
	payload2 := ^99

	// generate some adhoc sequences of operations
	data := []fuzzing.OperationSequence[stackFuzzingContext]{
		{&opPush{payload1}, &opPop{}, &opFlush{}, &opClose{}},
		{&opPush{payload1}, &opPop{}, &opSize{}, &opEmpty{},
			&opFlush{}, &opClose{}},
		{&opPush{payload1}, &opPush{payload2}, &opGetAll{}, &opClose{}},
		{&opPop{}, &opPush{payload2}, &opGetAll{}, &opClose{}},
		{&opClose{}, &opPush{payload2}, &opGetAll{}},
	}

	return data
}

func (c *stackFuzzingCampaign) CreateContext(t *testing.T) *stackFuzzingContext {
	path := t.TempDir() + "/test.dat"
	fileStack, err := openFileBasedStack[int](path)
	if err != nil {
		t.Fatalf("failed to open file stack: %v", err)
	}
	shadow := stack[int]{}
	return &stackFuzzingContext{path, fileStack, shadow}
}

func (c *stackFuzzingCampaign) Deserialize(rawData []byte) []fuzzing.Operation[stackFuzzingContext] {
	return parseOperations(rawData)
}

func (c *stackFuzzingCampaign) Cleanup(t *testing.T, context *stackFuzzingContext) {
	if err := context.stack.Close(); err != nil {
		t.Fatalf("cannot close file: %s", err)
	}
}

type opPush struct {
	value int
}

func (op *opPush) Serialize() []byte {
	b := push.serialise()
	b = binary.BigEndian.AppendUint32(b, uint32(op.value))
	return b
}

func (op *opPush) Apply(t *testing.T, c *stackFuzzingContext) {
	err := c.stack.Push(op.value)
	if err != nil {
		t.Errorf("error to push value: %s", err)
	}
	c.shadow.Push(op.value)
}

type opPop struct {
}

func (op *opPop) Serialize() []byte {
	return pop.serialise()
}

func (op *opPop) Apply(t *testing.T, c *stackFuzzingContext) {
	if got, err := c.stack.Pop(); err != nil {
		// error when the shadow is empty is OK state
		if c.shadow.Empty() && strings.HasPrefix(err.Error(), "cannot pop from empty stack") {
			return
		}

		t.Errorf("error to pop value: %s", err)
	} else {
		want := c.shadow.Pop()
		if got != want {
			t.Errorf("stack does not match expected value: %v != %v", got, want)
		}
	}
}

type opGetAll struct {
}

func (op *opGetAll) Serialize() []byte {
	return getAll.serialise()
}

func (op *opGetAll) Apply(t *testing.T, c *stackFuzzingContext) {
	got, err := c.stack.GetAll()
	if err != nil {
		t.Errorf("error to get all values: %s", err)
	}
	want := c.shadow.GetAll()
	if !slices.Equal(got, want) {
		t.Errorf("stack does not match expected value: %v != %v", got, want)
	}
}

type opSize struct {
}

func (op *opSize) Serialize() []byte {
	return size.serialise()
}

func (op *opSize) Apply(t *testing.T, c *stackFuzzingContext) {
	if got, want := c.stack.Size(), c.shadow.Size(); got != want {
		t.Errorf("stack does not match expected value: %v != %v", got, want)
	}
}

type opEmpty struct {
}

func (op *opEmpty) Serialize() []byte {
	return empty.serialise()
}

func (op *opEmpty) Apply(t *testing.T, c *stackFuzzingContext) {
	if got, want := c.stack.Empty(), c.shadow.Empty(); got != want {
		t.Errorf("stack does not match expected value: %v != %v", got, want)
	}
}

type opClose struct {
}

func (op *opClose) Serialize() []byte {
	return close.serialise()
}

func (op *opClose) Apply(t *testing.T, c *stackFuzzingContext) {
	if err := c.stack.Close(); err != nil {
		t.Errorf("error to flush stack: %s", err)
	}
	stack, err := openFileBasedStack[int](c.path)
	if err != nil {
		t.Fatalf("failed to open buffered file: %v", err)
	}
	c.stack = stack
}

type opFlush struct {
}

func (op *opFlush) Serialize() []byte {
	return flush.serialise()
}

func (op *opFlush) Apply(t *testing.T, c *stackFuzzingContext) {
	if err := c.stack.Flush(); err != nil {
		t.Errorf("error to flush stack: %s", err)
	}
}

// parseOperations converts the input byte array
// to the list of operations.
// It is converted from the format:<opType><value>
// Value part is parsed only when the opType equals to push.
// This method tries to parse as many those tuples as possible, terminating when no more
// elements are available.
// This method recognises expensive operations, which is flush, close and getAll, and it caps
// number of these operations to 20 in total and 3 in a row.
// The fuzzing mechanism requires one campaign does not run more than 1s, which is quickly
// broken when an expensive operation is triggered extensively.
func parseOperations(b []byte) []fuzzing.Operation[stackFuzzingContext] {
	var ops []fuzzing.Operation[stackFuzzingContext]
	var expensiveOpTotal, expensiveOpRow int
	for len(b) >= 1 {
		opType := opType(b[0] % 7)
		b = b[1:]
		if opType == flush || opType == close || opType == getAll {
			expensiveOpRow++
			expensiveOpTotal++
			if expensiveOpRow > 3 || expensiveOpTotal > 20 {
				continue
			}
		} else {
			expensiveOpRow = 0
		}
		var value int
		if opType == push {
			if len(b) >= 4 {
				value = int(binary.BigEndian.Uint32(b[0:4]))
				b = b[4:]
			} else {
				return ops
			}
		}

		var op fuzzing.Operation[stackFuzzingContext]
		switch opType {
		case push:
			op = &opPush{value}
		case pop:
			op = &opPop{}
		case getAll:
			op = &opGetAll{}
		case size:
			op = &opSize{}
		case empty:
			op = &opSize{}
		case close:
			op = &opClose{}
		case flush:
			op = &opFlush{}
		}
		ops = append(ops, op)
	}
	return ops
}

// stack used as a shadow implementation for testing.
type stack[I stock.Index] []I

func (s *stack[I]) GetAll() []I {
	return *s
}

func (s *stack[I]) Empty() bool {
	return len(*s) == 0
}

func (s *stack[I]) Size() int {
	return len(*s)
}

func (s *stack[I]) Push(v I) {
	*s = append(*s, v)
}

func (s *stack[I]) Pop() I {
	res := (*s)[len(*s)-1]
	*s = (*s)[:len(*s)-1]
	return res
}
