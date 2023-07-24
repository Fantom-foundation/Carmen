package file

import "testing"

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
