// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package shared

import (
	"sync"
	"testing"
)

func TestShared_LifeCycle(t *testing.T) {

	shared := MakeShared(10)

	read1 := shared.GetReadHandle()
	if got, want := read1.Get(), 10; got != want {
		t.Errorf("value is not %d, got %d", want, got)
	}
	read1.Release()

	write1 := shared.GetWriteHandle()
	write1.Set(12)
	write1.Release()

	read2 := shared.GetReadHandle()
	if got, want := read2.Get(), 12; got != want {
		t.Errorf("value is not %d, got %d", want, got)
	}
	read2.Release()

	write2 := shared.GetWriteHandle()
	*write2.Ref() = 14
	write2.Release()

	read3 := shared.GetReadHandle()
	if got, want := read3.Get(), 14; got != want {
		t.Errorf("value is not %d, got %d", want, got)
	}
	read3.Release()
}

func TestShared_ReadAccessDoesNotBlocksReadAccess(t *testing.T) {
	shared := MakeShared(10)
	read := shared.GetReadHandle()
	defer read.Release()
	if read2, ok := shared.TryGetReadHandle(); !ok {
		t.Fatalf("read access should not block read access")
	} else {
		read2.Release()
	}
}

func TestShared_ReadAccessBlocksWriteAccess(t *testing.T) {
	shared := MakeShared(10)
	read := shared.GetReadHandle()
	defer read.Release()
	if _, ok := shared.TryGetWriteHandle(); ok {
		t.Fatalf("read access should block write access")
	}
}

func TestShared_WriteAccessBlocksReadAccess(t *testing.T) {
	shared := MakeShared(10)
	write := shared.GetWriteHandle()
	defer write.Release()
	if _, ok := shared.TryGetReadHandle(); ok {
		t.Fatalf("write access should block read access")
	}
}

func TestShared_WriteAccessBlocksWriteAccess(t *testing.T) {
	shared := MakeShared(10)
	write := shared.GetWriteHandle()
	defer write.Release()
	if _, ok := shared.TryGetWriteHandle(); ok {
		t.Fatalf("write access should block write access")
	}
}

type noHandle struct{}

func (noHandle) Release() {}

func TestShared_AccessCombinations(t *testing.T) {
	type releaser interface {
		Release()
	}

	shared := MakeShared(10)

	permission := map[string]func() (releaser, bool){
		"none": func() (releaser, bool) { return noHandle{}, true },
		"read": func() (releaser, bool) {
			handle, success := shared.TryGetReadHandle()
			return &handle, success
		},
		"view": func() (releaser, bool) {
			handle, success := shared.TryGetViewHandle()
			return &handle, success
		},
		"hash": func() (releaser, bool) {
			handle, success := shared.TryGetHashHandle()
			return &handle, success
		},
		"write": func() (releaser, bool) {
			handle, success := shared.TryGetWriteHandle()
			return &handle, success
		},
	}

	// describes all combinations of held and wanted permissions
	rules := map[string]map[string]bool{
		"none": {
			"none":  true,
			"read":  true,
			"view":  true,
			"hash":  true,
			"write": true,
		},
		"read": {
			"none":  true,
			"read":  true,
			"view":  true,
			"hash":  true,
			"write": false,
		},
		"view": {
			"none":  true,
			"read":  true,
			"view":  true,
			"hash":  false,
			"write": false,
		},
		"hash": {
			"none":  true,
			"read":  true,
			"view":  false,
			"hash":  false,
			"write": false,
		},
		"write": {
			"none":  true,
			"read":  false,
			"view":  false,
			"hash":  false,
			"write": false,
		},
	}

	for held, rule := range rules {
		have, success := permission[held]()
		if !success {
			t.Fatalf("failed to get permission %s", held)
		}
		for want, expected := range rule {
			got, success := permission[want]()
			if success != expected {
				t.Errorf("unexpected permission grant, held %s, wanted %s, expected %t, got %t", held, want, expected, success)
			}
			if success {
				got.Release()
			}
		}
		have.Release()
	}
}

func TestShared_ReadIsInvalidatesByRelease(t *testing.T) {
	shared := MakeShared(10)

	read := ReadHandle[int]{}
	if read.Valid() {
		t.Errorf("default read handle should not be valid")
	}

	read = shared.GetReadHandle()
	if !read.Valid() {
		t.Errorf("granted read handle should be valid")
	}

	read.Release()
	if read.Valid() {
		t.Errorf("released read handle should be invalid")
	}
}

func TestShared_ViewIsInvalidatesByRelease(t *testing.T) {
	shared := MakeShared(10)

	view := ViewHandle[int]{}
	if view.Valid() {
		t.Errorf("default view handle should not be valid")
	}

	view = shared.GetViewHandle()
	if !view.Valid() {
		t.Errorf("granted view handle should be valid")
	}

	view.Release()
	if view.Valid() {
		t.Errorf("released view handle should be invalid")
	}
}

func TestShared_HashIsInvalidatesByRelease(t *testing.T) {
	shared := MakeShared(10)

	hash := HashHandle[int]{}
	if hash.Valid() {
		t.Errorf("default hash handle should not be valid")
	}

	hash = shared.GetHashHandle()
	if !hash.Valid() {
		t.Errorf("granted hash handle should be valid")
	}

	hash.Release()
	if hash.Valid() {
		t.Errorf("released hash handle should be invalid")
	}
}

func TestShared_WriteIsInvalidatesByRelease(t *testing.T) {
	shared := MakeShared(10)

	write := WriteHandle[int]{}
	if write.Valid() {
		t.Errorf("default write handle should not be valid")
	}

	write = shared.GetWriteHandle()
	if !write.Valid() {
		t.Errorf("granted write handle should be valid")
	}

	write.Release()
	if write.Valid() {
		t.Errorf("released write handle should be invalid")
	}
}

func TestShared_ConcurrentRead(t *testing.T) {
	shared := MakeShared(10)
	var wg sync.WaitGroup
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			read := shared.GetReadHandle()
			if got, want := read.Get(), 10; got != want {
				t.Errorf("value is not %d, got %d", want, got)
			}
			read.Release()
		}()
	}
	wg.Wait()
}

func TestShared_ConcurrentWrite(t *testing.T) {
	shared := MakeShared(10)
	var wg sync.WaitGroup
	wg.Add(10)
	for i := 0; i < 10; i++ {
		i := i
		go func() {
			defer wg.Done()
			write := shared.GetWriteHandle()
			write.Set(i)
			write.Release()
		}()
	}
	wg.Wait()
}

func TestShared_WriteHandleSynchronizesAccess(t *testing.T) {
	const (
		N = 10
		M = 10
	)
	shared := MakeShared(0)
	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func() {
			defer wg.Done()
			for i := 0; i < M; i++ {
				write := shared.GetWriteHandle()
				cur := write.Get()
				write.Set(cur + 1)
				write.Release()
			}
		}()
	}
	wg.Wait()

	read := shared.GetReadHandle()
	defer read.Release()
	if got, want := read.Get(), N*M; got != want {
		t.Errorf("invalid end result of concurrent writes - wanted %d, got %d", want, got)
	}
}

func Benchmark_ReadSequential(b *testing.B) {
	shared := MakeShared(0)
	for i := 0; i < b.N; i++ {
		read := shared.GetReadHandle()
		read.Get()
		read.Release()
	}
}

func Benchmark_ReadParallel(b *testing.B) {
	shared := MakeShared(0)
	b.RunParallel(func(b *testing.PB) {
		for b.Next() {
			read := shared.GetReadHandle()
			read.Get()
			read.Release()
		}
	})
}

func Benchmark_ViewSequential(b *testing.B) {
	shared := MakeShared(0)
	for i := 0; i < b.N; i++ {
		view := shared.GetViewHandle()
		view.Get()
		view.Release()
	}
}

func Benchmark_ViewParallel(b *testing.B) {
	shared := MakeShared(0)
	b.RunParallel(func(b *testing.PB) {
		for b.Next() {
			view := shared.GetViewHandle()
			view.Get()
			view.Release()
		}
	})
}
