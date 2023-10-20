package file

import (
	"encoding/binary"
	"fmt"
	"github.com/Fantom-foundation/Carmen/go/backend/stock"
	"golang.org/x/exp/slices"
	"strings"
	"testing"
)

func TestStack_OpenClose(t *testing.T) {
	stack, err := openFileBasedStack[int](t.TempDir() + "/stack.dat")
	if err != nil {
		t.Fatalf("failed to open empty stack: %v", err)
	}
	if err := stack.Close(); err != nil {
		t.Fatalf("failed to close stack: %v", err)
	}
}

func TestStack_PushAndPop(t *testing.T) {
	stack, err := openFileBasedStack[int](t.TempDir() + "/stack.dat")
	if err != nil {
		t.Fatalf("failed to open empty stack: %v", err)
	}
	defer stack.Close()

	if err := stack.Push(12); err != nil {
		t.Fatalf("failed to push element: %v", err)
	}

	if err := stack.Push(14); err != nil {
		t.Fatalf("failed to push element: %v", err)
	}

	if got, err := stack.Pop(); err != nil || got != 14 {
		t.Fatalf("failed to pop correct element, got %v (err: %v)", got, err)
	}

	if got, err := stack.Pop(); err != nil || got != 12 {
		t.Fatalf("failed to pop correct element, got %v (err: %v)", got, err)
	}
}

func TestStack_LargePushAndPop(t *testing.T) {
	stack, err := openFileBasedStack[int](t.TempDir() + "/stack.dat")
	if err != nil {
		t.Fatalf("failed to open empty stack: %v", err)
	}
	defer stack.Close()

	for i := 0; i < 10*stackBufferSize; i++ {
		if got, want := stack.Size(), i; got != want {
			t.Fatalf("invalid size, wanted %d, got %d", want, got)
		}
		if err := stack.Push(i); err != nil {
			t.Fatalf("failed to push element: %v", err)
		}
	}

	for i := 10*stackBufferSize - 1; i >= 0; i-- {
		if got, err := stack.Pop(); err != nil || got != i {
			t.Fatalf("failed to pop correct element, got %v (err: %v)", got, err)
		}
		if got, want := stack.Size(), i; got != want {
			t.Fatalf("invalid size, wanted %d, got %d", want, got)
		}
	}
}

func TestStack_CloseAndReopen(t *testing.T) {
	file := t.TempDir() + "/stack.dat"
	{
		stack, err := openFileBasedStack[int](file)
		if err != nil {
			t.Fatalf("failed to open empty stack: %v", err)
		}

		if err := stack.Push(12); err != nil {
			t.Fatalf("failed to push element: %v", err)
		}

		if err := stack.Push(14); err != nil {
			t.Fatalf("failed to push element: %v", err)
		}

		if err := stack.Close(); err != nil {
			t.Fatalf("failed to close stack: %v", err)
		}
	}

	// Reopen stack and check whether content is preserved.
	{
		stack, err := openFileBasedStack[int](file)
		if err != nil {
			t.Fatalf("failed to re-open stack: %v", err)
		}
		defer stack.Close()

		if got, want := stack.Size(), 2; got != want {
			t.Fatalf("invalid stack size after reopening, wanted %d, got %d", want, got)
		}

		if got, err := stack.Pop(); err != nil || got != 14 {
			t.Fatalf("failed to pop correct element, got %v (err: %v)", got, err)
		}

		if got, err := stack.Pop(); err != nil || got != 12 {
			t.Fatalf("failed to pop correct element, got %v (err: %v)", got, err)
		}
	}
}

func TestStack_CloseAndReopenLarge(t *testing.T) {
	N := 10*stackBufferSize + 123
	file := t.TempDir() + "/stack.dat"
	{
		stack, err := openFileBasedStack[int](file)
		if err != nil {
			t.Fatalf("failed to open empty stack: %v", err)
		}

		for i := 0; i < N; i++ {
			if got, want := stack.Size(), i; got != want {
				t.Fatalf("invalid size, wanted %d, got %d", want, got)
			}
			if err := stack.Push(i); err != nil {
				t.Fatalf("failed to push element: %v", err)
			}
		}

		if err := stack.Close(); err != nil {
			t.Fatalf("failed to close stack: %v", err)
		}
	}

	// Reopen stack and check whether content is preserved.
	{
		stack, err := openFileBasedStack[int](file)
		if err != nil {
			t.Fatalf("failed to re-open stack: %v", err)
		}
		defer stack.Close()

		if got, want := stack.Size(), N; got != want {
			t.Fatalf("invalid stack size after reopening, wanted %d, got %d", want, got)
		}

		for i := N - 1; i >= 0; i-- {
			if got, err := stack.Pop(); err != nil || got != i {
				t.Fatalf("failed to pop correct element, got %v (err: %v)", got, err)
			}
			if got, want := stack.Size(), i; got != want {
				t.Fatalf("invalid size, wanted %d, got %d", want, got)
			}
		}
	}
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

// op is a tuple of opType and value.
type op struct {
	opType
	value int
}

// serialise converts the struct to a byte array
// using following format: <opType><value>
// The value is exported only for operation push.
func (o *op) serialise() []byte {
	b := []byte{byte(o.opType)}
	if o.opType == push {
		b = binary.BigEndian.AppendUint32(b, uint32(o.value))
	}

	return b
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
func parseOperations(b []byte) []op {
	var ops []op
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

		ops = append(ops, op{opType, value})
	}
	return ops
}

func FuzzStack_RandomOps(f *testing.F) {

	payload1 := 99
	payload2 := ^99
	zero := 0

	// generate some adhoc sequences of operations
	data := [][]op{
		{op{push, payload1}, op{pop, zero}, op{flush, zero}, op{close, zero}},
		{op{push, payload1}, op{pop, zero}, op{size, zero}, op{empty, zero},
			op{flush, zero}, op{close, zero}},
		{op{push, payload1}, op{push, payload2}, op{getAll, zero}, op{close, zero}},
		{op{pop, zero}, op{push, payload2}, op{getAll, zero}, op{close, zero}},
		{op{close, zero}, op{push, payload2}, op{getAll, zero}},
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
		st, err := openFileBasedStack[int](path)
		if err != nil {
			t.Fatalf("failed to open buffered file: %v", err)
		}
		defer st.Close()

		shadow := stack[int]{}

		ops := parseOperations(rawData)
		for _, op := range ops {
			fmt.Printf("%d", op.opType)
			switch op.opType {
			case push:
				err := st.Push(op.value)
				if err != nil {
					t.Errorf("error to push value: %s", err)
				}
				shadow.Push(op.value)
			case pop:
				if got, err := st.Pop(); err != nil {
					// error when the shadow is empty is OK state
					if shadow.Empty() && strings.HasPrefix(err.Error(), "cannot pop from empty stack") {
						continue
					}

					t.Errorf("error to pop value: %s", err)
				} else {
					want := shadow.Pop()
					if got != want {
						t.Errorf("stack does not match expected value: %v != %v", got, want)
					}
				}
			case getAll:
				got, err := st.GetAll()
				if err != nil {
					t.Errorf("error to get all values: %s", err)
				}
				want := shadow.GetAll()
				if !slices.Equal(got, want) {
					t.Errorf("stack does not match expected value: %v != %v", got, want)
				}
			case size:
				if got, want := st.Size(), shadow.Size(); got != want {
					t.Errorf("stack does not match expected value: %v != %v", got, want)
				}
			case empty:
				if got, want := st.Empty(), shadow.Empty(); got != want {
					t.Errorf("stack does not match expected value: %v != %v", got, want)
				}
			case close:
				if err := st.Close(); err != nil {
					t.Errorf("error to flush stack: %s", err)
				}
				st, err = openFileBasedStack[int](path)
				if err != nil {
					t.Fatalf("failed to open buffered file: %v", err)
				}
			case flush:
				if err := st.Flush(); err != nil {
					t.Errorf("error to flush stack: %s", err)
				}
			default:
				t.Fatalf("unknown op: %v", op.opType)
			}
		}

	})
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
