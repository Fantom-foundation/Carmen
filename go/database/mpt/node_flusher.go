// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package mpt

import (
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/Fantom-foundation/Carmen/go/database/mpt/shared"
)

type nodeFlusher struct {
	shutdown chan<- struct{}
	done     <-chan struct{}
	errs     []error
}

type nodeFlusherConfig struct {
	period time.Duration // uses a default period if zero and disables flushing if negative
}

func startNodeFlusher(cache NodeCache, sink NodeSink, config nodeFlusherConfig) *nodeFlusher {

	shutdown := make(chan struct{})
	done := make(chan struct{})

	res := &nodeFlusher{
		shutdown: shutdown,
		done:     done,
	}

	period := config.period
	if period == 0 {
		period = 5 * time.Second
	}
	fmt.Printf("Starting node flusher with a period of %s\n", period)
	if period > 0 {
		go func() {
			defer close(done)
			ticker := time.NewTicker(period)
			defer ticker.Stop()
			for {
				select {
				case <-shutdown:
					return
				case <-ticker.C:
					if err := tryFlushDirtyNodes(cache, sink); err != nil {
						res.errs = append(res.errs, err)
					}
				}
			}
		}()
	} else {
		close(done)
	}

	return res
}

func (f *nodeFlusher) Stop() error {
	close(f.shutdown)
	<-f.done
	return errors.Join(f.errs...)
}

func tryFlushDirtyNodes(cache NodeCache, sink NodeSink) error {
	// Collect a list of dirty nodes to be flushed.
	dirtyIds := make([]NodeId, 0, 1_000_000)
	cache.ForEach(func(id NodeId, node *shared.Shared[Node]) {
		handle, success := node.TryGetViewHandle()
		if !success {
			return
		}
		dirty := handle.Get().IsDirty()
		handle.Release()
		if !dirty {
			return
		}
		dirtyIds = append(dirtyIds, id)
	})

	// The IDs are sorted to increase the chance of sequential
	// writes to the disk.
	slices.Sort(dirtyIds)

	var errs []error
	for _, id := range dirtyIds {
		ref := NewNodeReference(id)
		node, success := cache.Get(&ref)
		if !success {
			continue
		}

		// This service is a best-effort service. If the
		// node is in use right now, we skip the flush and
		// continue with the next node.
		handle, success := node.TryGetWriteHandle()
		if !success {
			continue
		}

		// If the node was cleaned otherwise, we can skip the flush.
		// The cleaning may have been conducted by the write buffer
		// or by releasing the node.
		if !handle.Get().IsDirty() {
			handle.Release()
			continue
		}

		// Nodes with dirty hashes cannot be flushed.
		_, dirtyHash := handle.Get().GetHash()
		if dirtyHash {
			handle.Release()
			continue
		}

		err := sink.Write(id, handle.AsViewHandle())
		if err == nil {
			handle.Get().MarkClean()
		}
		handle.Release()

		if err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
