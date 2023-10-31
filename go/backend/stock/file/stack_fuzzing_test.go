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

type stackFuzzingCampaign struct {
}

type stackFuzzingContext struct {
	path   string
	stack  *fileBasedStack[int]
	shadow stack[int]
}

func (c *stackFuzzingCampaign) Init() []fuzzing.OperationSequence[stackFuzzingContext] {

	push1 := createOp(push, 99)
	push2 := createOp(push, ^99)

	popOp := createOp(pop, 0)
	flushOp := createOp(flush, 0)
	closeOp := createOp(close, 0)
	emptyOp := createOp(empty, 0)
	getAllOp := createOp(getAll, 0)
	sizeOp := createOp(size, 0)

	// generate some adhoc sequences of operations
	data := []fuzzing.OperationSequence[stackFuzzingContext]{
		{push1, popOp, flushOp, closeOp},
		{push1, popOp, sizeOp, emptyOp,
			flushOp, closeOp},
		{push1, push2, getAllOp, closeOp},
		{popOp, push2, getAllOp, closeOp},
		{closeOp, push2, getAllOp},
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

type intValue int

func (v intValue) Serialize() []byte {
	b := make([]byte, 0, 4)
	return binary.BigEndian.AppendUint32(b, uint32(v))
}

// Definitions of fuzzing operation methods.

var opPush = func(value intValue, t *testing.T, c *stackFuzzingContext) {
	err := c.stack.Push(int(value))
	if err != nil {
		t.Errorf("error to push value: %s", err)
	}
	c.shadow.Push(int(value))
}

var opPop = func(_ fuzzing.EmptyPayload, t *testing.T, c *stackFuzzingContext) {
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

var opGetAll = func(_ fuzzing.EmptyPayload, t *testing.T, c *stackFuzzingContext) {
	got, err := c.stack.GetAll()
	if err != nil {
		t.Errorf("error to get all values: %s", err)
	}
	want := c.shadow.GetAll()
	if !slices.Equal(got, want) {
		t.Errorf("stack does not match expected value: %v != %v", got, want)
	}
}

var opSize = func(_ fuzzing.EmptyPayload, t *testing.T, c *stackFuzzingContext) {
	if got, want := c.stack.Size(), c.shadow.Size(); got != want {
		t.Errorf("stack does not match expected value: %v != %v", got, want)
	}
}

var opEmpty = func(_ fuzzing.EmptyPayload, t *testing.T, c *stackFuzzingContext) {
	if got, want := c.stack.Empty(), c.shadow.Empty(); got != want {
		t.Errorf("stack does not match expected value: %v != %v", got, want)
	}
}

var opClose = func(_ fuzzing.EmptyPayload, t *testing.T, c *stackFuzzingContext) {
	if err := c.stack.Close(); err != nil {
		t.Errorf("error to flush stack: %s", err)
	}
	stack, err := openFileBasedStack[int](c.path)
	if err != nil {
		t.Fatalf("failed to open buffered file: %v", err)
	}
	c.stack = stack
}

var opFlush = func(_ fuzzing.EmptyPayload, t *testing.T, c *stackFuzzingContext) {
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
		ops = append(ops, createOp(opType, value))
	}
	return ops
}

func createOp(t opType, value int) fuzzing.Operation[stackFuzzingContext] {
	var op fuzzing.Operation[stackFuzzingContext]
	switch t {
	case push:
		op = fuzzing.NewOp(push, intValue(value), opPush)
	case pop:
		op = fuzzing.NewOp(pop, fuzzing.EmptyPayload{}, opPop)
	case getAll:
		op = fuzzing.NewOp(getAll, fuzzing.EmptyPayload{}, opGetAll)
	case size:
		op = fuzzing.NewOp(size, fuzzing.EmptyPayload{}, opSize)
	case empty:
		op = fuzzing.NewOp(empty, fuzzing.EmptyPayload{}, opEmpty)
	case close:
		op = fuzzing.NewOp(close, fuzzing.EmptyPayload{}, opClose)
	case flush:
		op = fuzzing.NewOp(flush, fuzzing.EmptyPayload{}, opFlush)
	}

	return op
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
