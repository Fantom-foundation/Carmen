package mpt

import (
	"fmt"
	"slices"
	"time"

	"github.com/Fantom-foundation/Carmen/go/database/mpt/shared"
)

type nodeFlusher struct {
	cache    NodeCache
	sink     NodeSink
	shutdown chan<- struct{}
	done     <-chan struct{}
}

func startNodeFlusher(cache NodeCache, sink NodeSink) *nodeFlusher {

	shutdown := make(chan struct{})
	done := make(chan struct{})

	res := &nodeFlusher{
		cache:    cache,
		sink:     sink,
		shutdown: shutdown,
		done:     done,
	}
	go func() {
		defer close(done)
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-shutdown:
				return
			case <-ticker.C:
				res.flush()
			}
		}
	}()

	return res
}

func (f *nodeFlusher) flush() {
	start := time.Now()

	startCollect := time.Now()
	dirtyIds := make([]NodeId, 0, 10_000)
	count := 0
	// TODO: get a NodeReference instead of only a NodeId
	f.cache.ForEach(func(id NodeId, node *shared.Shared[Node]) {
		count++

		dirty := node.GetUnprotected().IsDirty()
		if !dirty {
			return
		}

		handle, success := node.TryGetViewHandle()
		if !success {
			return
		}
		_, hashDirty := handle.Get().GetHash()
		handle.Release()
		if !hashDirty {
			dirtyIds = append(dirtyIds, id)
		}
	})
	collectionTime := time.Since(startCollect)

	startSort := time.Now()
	slices.Sort(dirtyIds)
	sortTime := time.Since(startSort)

	flushStart := time.Now()
	flushCounter := 0
	for _, id := range dirtyIds {
		ref := NewNodeReference(id)
		node, success := f.cache.Get(&ref)
		if !success {
			continue
		}
		handle, success := node.TryGetWriteHandle()
		if !success {
			continue
		}

		// dirty nodes can not be flushed
		_, dirty := handle.Get().GetHash()
		if dirty {
			handle.Release()
			continue
		}

		err := f.sink.Write(id, handle.AsViewHandle())
		if err == nil {
			flushCounter++
			handle.Get().MarkClean()
		}
		handle.Release()

		// TODO: handle error
		if err != nil {
			fmt.Printf("error flushing node %v: %v\n", id, err)
		}
	}
	flushTime := time.Since(flushStart)

	syncStart := time.Now()
	/*
		if err := f.sink.Flush(); err != nil {
			fmt.Printf("error syncing sink: %v\n", err)
		}
	*/
	syncTime := time.Since(syncStart)

	fmt.Printf(
		"flushing of %d dirty nodes out of %d candidates and %d total nodes took %v, collection %v, sorting %v, flushing %v, syncing %v\n",
		flushCounter,
		len(dirtyIds),
		count,
		time.Since(start),
		collectionTime,
		sortTime,
		flushTime,
		syncTime,
	)
}

func (f *nodeFlusher) Stop() error {
	close(f.shutdown)
	<-f.done
	return nil
}
