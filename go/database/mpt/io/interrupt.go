// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package io

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"
)

var errCanceled = errors.New("export was interrupted")

// isContextDone returns true if the given context CancelFunc has been called.
// Otherwise, returns false.
func isContextDone(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

func catchInterrupt() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		defer signal.Stop(c)
		select {
		case <-c:
			log.Println("closing, please wait until proper shutdown to prevent database corruption")
			cancel()
		case <-ctx.Done():
		}
	}()

	return ctx
}