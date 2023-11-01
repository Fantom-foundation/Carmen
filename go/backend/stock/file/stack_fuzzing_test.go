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
	var opPush = func(_ opType, value fuzzing.SerialisedPayload[int], t *testing.T, c *stackFuzzingContext) {
		err := c.stack.Push(value.Val)
		if err != nil {
			t.Errorf("error to push value: %s", err)
		}
		c.shadow.Push(value.Val)
	}

	var opPop = func(_ opType, _ fuzzing.EmptyPayload, t *testing.T, c *stackFuzzingContext) {
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

	var opGetAll = func(_ opType, _ fuzzing.EmptyPayload, t *testing.T, c *stackFuzzingContext) {
		got, err := c.stack.GetAll()
		if err != nil {
			t.Errorf("error to get all values: %s", err)
		}
		want := c.shadow.GetAll()
		if !slices.Equal(got, want) {
			t.Errorf("stack does not match expected value: %v != %v", got, want)
		}
	}

	var opSize = func(_ opType, _ fuzzing.EmptyPayload, t *testing.T, c *stackFuzzingContext) {
		if got, want := c.stack.Size(), c.shadow.Size(); got != want {
			t.Errorf("stack does not match expected value: %v != %v", got, want)
		}
	}

	var opEmpty = func(_ opType, _ fuzzing.EmptyPayload, t *testing.T, c *stackFuzzingContext) {
		if got, want := c.stack.Empty(), c.shadow.Empty(); got != want {
			t.Errorf("stack does not match expected value: %v != %v", got, want)
		}
	}

	var opClose = func(_ opType, _ fuzzing.EmptyPayload, t *testing.T, c *stackFuzzingContext) {
		if err := c.stack.Close(); err != nil {
			t.Errorf("error to flush stack: %s", err)
		}
		stack, err := openFileBasedStack[int](c.path)
		if err != nil {
			t.Fatalf("failed to open buffered file: %v", err)
		}
		c.stack = stack
	}

	var opFlush = func(_ opType, _ fuzzing.EmptyPayload, t *testing.T, c *stackFuzzingContext) {
		if err := c.stack.Flush(); err != nil {
			t.Errorf("error to flush stack: %s", err)
		}
	}

	registry := fuzzing.NewRegistry[opType, stackFuzzingContext]()
	fuzzing.RegisterOp(registry, push, func(payload int) fuzzing.Operation[stackFuzzingContext] {
		b := binary.BigEndian.AppendUint32(make([]byte, 0, 4), uint32(payload))
		return fuzzing.NewOp(push, fuzzing.NewSerialisedPayload(payload, b), opPush)
	})
	fuzzing.RegisterOp(registry, pop, func(_ any) fuzzing.Operation[stackFuzzingContext] {
		return fuzzing.NewOp(pop, fuzzing.EmptyPayload{}, opPop)
	})
	fuzzing.RegisterOp(registry, getAll, func(_ any) fuzzing.Operation[stackFuzzingContext] {
		return fuzzing.NewOp(getAll, fuzzing.EmptyPayload{}, opGetAll)
	})
	fuzzing.RegisterOp(registry, size, func(_ any) fuzzing.Operation[stackFuzzingContext] {
		return fuzzing.NewOp(size, fuzzing.EmptyPayload{}, opSize)
	})
	fuzzing.RegisterOp(registry, empty, func(_ any) fuzzing.Operation[stackFuzzingContext] {
		return fuzzing.NewOp(empty, fuzzing.EmptyPayload{}, opEmpty)
	})
	fuzzing.RegisterOp(registry, close, func(_ any) fuzzing.Operation[stackFuzzingContext] {
		return fuzzing.NewOp(close, fuzzing.EmptyPayload{}, opClose)
	})
	fuzzing.RegisterOp(registry, flush, func(_ any) fuzzing.Operation[stackFuzzingContext] {
		return fuzzing.NewOp(flush, fuzzing.EmptyPayload{}, opFlush)
	})

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
	registry fuzzing.OpsFactoryRegistry[opType, stackFuzzingContext]
}

type stackFuzzingContext struct {
	path   string
	stack  *fileBasedStack[int]
	shadow stack[int]
}

func (c *stackFuzzingCampaign) Init() []fuzzing.OperationSequence[stackFuzzingContext] {

	push1 := c.registry.CreateOp(push, 99)
	push2 := c.registry.CreateOp(push, ^99)

	popOp := c.registry.CreateOp(pop, 0)
	flushOp := c.registry.CreateOp(flush, 0)
	closeOp := c.registry.CreateOp(close, 0)
	emptyOp := c.registry.CreateOp(empty, 0)
	getAllOp := c.registry.CreateOp(getAll, 0)
	sizeOp := c.registry.CreateOp(size, 0)

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
	return parseOperations(c.registry, rawData)
}

func (c *stackFuzzingCampaign) Cleanup(t *testing.T, context *stackFuzzingContext) {
	if err := context.stack.Close(); err != nil {
		t.Fatalf("cannot close file: %s", err)
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
func parseOperations(registry fuzzing.OpsFactoryRegistry[opType, stackFuzzingContext], b []byte) []fuzzing.Operation[stackFuzzingContext] {
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
		op := registry.CreateOp(opType, value)
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
