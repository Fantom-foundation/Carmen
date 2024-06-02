// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package main

import (
	"context"
	"syscall"
	"testing"
	"time"
)

func Test_CatchInterrupt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	isCanceled := catchInterrupt(ctx, cancel, time.Now())
	time.Sleep(1 * time.Second)
	err := syscall.Kill(syscall.Getpid(), syscall.SIGINT)
	if err != nil {
		t.Fatal("failed to create a SIGINT signal")
	}
	time.Sleep(1 * time.Second)
	if !isCanceled.Load() {
		t.Fatal("context was not canceled")
	}
}
