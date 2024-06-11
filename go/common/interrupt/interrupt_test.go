package interrupt

import (
	"context"
	"syscall"
	"testing"
)

func Test_CatchCancelsContextWhenInterrupted(t *testing.T) {
	ctx := Register(context.Background())
	err := syscall.Kill(syscall.Getpid(), syscall.SIGINT)
	if err != nil {
		t.Fatal("failed to create a SIGINT signal")
	}
	select {
	case <-ctx.Done():
	}
}

func Test_IsContextDone(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	if IsCancelled(ctx) {
		t.Fatal("context was not canceled but func returned true")
	}
	cancel()
	if !IsCancelled(ctx) {
		t.Fatalf("context was canceled but func returned false")
	}
}
