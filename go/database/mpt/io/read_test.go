package io

import (
	"fmt"
	"io"
	"math/rand"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Fantom-foundation/Carmen/go/database/mpt"
)

var file = "/tmp/state_1723649973/archive/branches/values.dat"

const B = 1000

func BenchmarkRandomRead(b *testing.B) {
	encoder := mpt.BranchNodeEncoderWithNodeHash{}
	f, err := os.Open(file)
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()

	state, err := f.Stat()
	if err != nil {
		b.Fatal(err)
	}
	entrySize := int64(encoder.GetEncodedSize())
	numEntries := state.Size() / entrySize

	rnd := rand.New(rand.NewSource(int64(time.Now().Nanosecond())))
	buffer := make([]byte, encoder.GetEncodedSize())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < B; j++ {
			pos := rnd.Int63n(numEntries)

			_, err := f.Seek(pos*entrySize, 0)
			if err != nil {
				b.Fatal(err)
			}
			if _, err := io.ReadFull(f, buffer); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkOrderedRead(b *testing.B) {
	encoder := mpt.BranchNodeEncoderWithNodeHash{}
	f, err := os.Open(file)
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()

	state, err := f.Stat()
	if err != nil {
		b.Fatal(err)
	}
	entrySize := int64(encoder.GetEncodedSize())
	numEntries := state.Size() / entrySize

	rnd := rand.New(rand.NewSource(int64(time.Now().Nanosecond())))
	buffer := make([]byte, encoder.GetEncodedSize())

	positions := make([]int64, B)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < B; j++ {
			positions[j] = rnd.Int63n(numEntries)
		}
		slices.Sort(positions)

		for _, pos := range positions {
			_, err := f.Seek(pos*entrySize, 0)
			if err != nil {
				b.Fatal(err)
			}
			if _, err := io.ReadFull(f, buffer); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkParallelRead(b *testing.B) {
	encoder := mpt.BranchNodeEncoderWithNodeHash{}

	for i := 1; i <= 16; i *= 2 {
		b.Run(fmt.Sprintf("workers=%d", i), func(b *testing.B) {

			var S = i

			files := make([]*os.File, 0, S)
			for i := 0; i < S; i++ {
				f, err := os.Open(file)
				if err != nil {
					b.Fatal(err)
				}
				defer f.Close()
				files = append(files, f)
			}

			state, err := files[0].Stat()
			if err != nil {
				b.Fatal(err)
			}
			entrySize := int64(encoder.GetEncodedSize())
			numEntries := state.Size() / entrySize

			rnd := rand.New(rand.NewSource(int64(time.Now().Nanosecond())))
			buffers := make([][]byte, S)
			for i := 0; i < S; i++ {
				buffers[i] = make([]byte, encoder.GetEncodedSize())
			}

			positions := make([]int64, B)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for j := 0; j < B; j++ {
					positions[j] = rnd.Int63n(numEntries)
				}

				pfor(S, B, func(w int, i int) {
					f := files[w]
					pos := positions[i]
					_, err := f.Seek(pos*entrySize, 0)
					if err != nil {
						b.Fatal(err)
					}
					buffer := buffers[w]
					if _, err := io.ReadFull(f, buffer); err != nil {
						b.Fatal(err)
					}
				})
			}
		})
	}
}

func pfor(numWorkers int, upperLimit int, op func(worker int, index int)) {
	var counter atomic.Int64
	var wg sync.WaitGroup
	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go func(worker int) {
			defer wg.Done()
			for {
				index := int(counter.Add(1) - 1)
				if index >= upperLimit {
					break
				}
				op(worker, index)
			}
		}(i)
	}
	wg.Wait()
}
