// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package interrupt

import (
	"context"
	"syscall"
	"testing"
)

func Test_CatchCancelsContextWhenInterrupted(t *testing.T) {
	ctx := CancelOnInterrupt(context.Background())
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
