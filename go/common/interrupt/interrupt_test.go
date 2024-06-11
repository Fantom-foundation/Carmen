package interrupt

import (
	"context"
	"syscall"
	"testing"
	"time"
)

func Test_CatchCancelsContextWhenInterrupted(t *testing.T) {
	ctx := Catch(context.Background())
	err := syscall.Kill(syscall.Getpid(), syscall.SIGINT)
	if err != nil {
		t.Fatal("failed to create a SIGINT signal")
	}
	select {
	case <-time.After(1 * time.Second):
		t.Fatal("time out")
	case <-ctx.Done():
	}
}

func Test_IsContextDone(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	if IsContextDone(ctx) {
		t.Fatal("context was not canceled but func returned true")
	}
	cancel()
	if !IsContextDone(ctx) {
		t.Fatalf("context was canceled but func returned false")
	}
}
