package utils

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"sync"
	"testing"
)

const fileSize = 1 * (1 << 30)
const pageSize = 1 << 12

const directory = ""

func getWorkDirectory(b *testing.B) string {
	if len(directory) == 0 {
		return b.TempDir()
	}
	return directory
}

func BenchmarkWriteFilesInOrder_SingleThreaded(b *testing.B) {
	dir := getWorkDirectory(b)
	for i := 0; i < b.N; i++ {
		err := errors.Join(
			writeFile(dir+string(os.PathSeparator)+"a.dat", fileSize),
			writeFile(dir+string(os.PathSeparator)+"b.dat", fileSize),
		)
		if err != nil {
			b.Errorf("benchmark failed: %v", err)
		}
	}
}

func BenchmarkWriteFilesInOrder_MultiThreaded(b *testing.B) {
	dir := getWorkDirectory(b)
	for i := 0; i < b.N; i++ {
		wg := sync.WaitGroup{}
		wg.Add(2)
		go func() {
			writeFile(dir+string(os.PathSeparator)+"a.dat", fileSize)
			wg.Done()
		}()
		go func() {
			writeFile(dir+string(os.PathSeparator)+"b.dat", fileSize)
			wg.Done()
		}()
		wg.Wait()
	}
}

func writeFile(name string, size int) error {
	f, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer os.Remove(name)
	defer f.Close()

	written := 0
	data := make([]byte, pageSize)
	for written < size {
		if n, err := f.Write(data); err != nil {
			return fmt.Errorf("failed to write data: %w", err)
		} else {
			written += n
		}
	}

	return nil
}

func BenchmarkWriteFilesRandom_SingleThreaded(b *testing.B) {
	positions := make([]int, fileSize/pageSize)
	for i := range positions {
		positions[i] = i
	}
	rand.Shuffle(len(positions), func(i, j int) { positions[i], positions[j] = positions[j], positions[i] })

	dir := getWorkDirectory(b)
	for i := 0; i < b.N; i++ {
		err := errors.Join(
			writePages(dir+string(os.PathSeparator)+"a.dat", positions, nil),
			writePages(dir+string(os.PathSeparator)+"b.dat", positions, nil),
		)
		if err != nil {
			b.Errorf("benchmark failed: %v", err)
		}
	}
}

func BenchmarkWriteFilesRandom_SingleThreadedLocked(b *testing.B) {
	positions := make([]int, fileSize/pageSize)
	for i := range positions {
		positions[i] = i
	}
	rand.Shuffle(len(positions), func(i, j int) { positions[i], positions[j] = positions[j], positions[i] })

	dir := getWorkDirectory(b)
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	for i := 0; i < b.N; i++ {
		err := errors.Join(
			writePages(dir+string(os.PathSeparator)+"a.dat", positions, nil),
			writePages(dir+string(os.PathSeparator)+"b.dat", positions, nil),
		)
		if err != nil {
			b.Errorf("benchmark failed: %v", err)
		}
	}
}
func BenchmarkWriteFilesRandom_MultiThreaded(b *testing.B) {
	positions := make([]int, fileSize/pageSize)
	for i := range positions {
		positions[i] = i
	}
	rand.Shuffle(len(positions), func(i, j int) { positions[i], positions[j] = positions[j], positions[i] })

	dir := getWorkDirectory(b)
	for i := 0; i < b.N; i++ {
		wg := sync.WaitGroup{}
		wg.Add(2)
		go func() {
			writePages(dir+string(os.PathSeparator)+"a.dat", positions, nil)
			wg.Done()
		}()
		go func() {
			writePages(dir+string(os.PathSeparator)+"b.dat", positions, nil)
			wg.Done()
		}()
		wg.Wait()
	}
}

func BenchmarkWriteFilesRandom_MultiThreadedLocked(b *testing.B) {
	positions := make([]int, fileSize/pageSize)
	for i := range positions {
		positions[i] = i
	}
	rand.Shuffle(len(positions), func(i, j int) { positions[i], positions[j] = positions[j], positions[i] })

	dir := getWorkDirectory(b)
	for i := 0; i < b.N; i++ {
		wg := sync.WaitGroup{}
		wg.Add(2)
		go func() {
			runtime.LockOSThread()
			defer runtime.UnlockOSThread()
			writePages(dir+string(os.PathSeparator)+"a.dat", positions, nil)
			wg.Done()
		}()
		go func() {
			runtime.LockOSThread()
			defer runtime.UnlockOSThread()
			writePages(dir+string(os.PathSeparator)+"b.dat", positions, nil)
			wg.Done()
		}()
		wg.Wait()
	}
}

func BenchmarkWriteFilesRandom_MultiThreadedSynchronized(b *testing.B) {
	positions := make([]int, fileSize/pageSize)
	for i := range positions {
		positions[i] = i
	}
	rand.Shuffle(len(positions), func(i, j int) { positions[i], positions[j] = positions[j], positions[i] })

	var lock sync.Mutex
	dir := getWorkDirectory(b)
	for i := 0; i < b.N; i++ {
		wg := sync.WaitGroup{}
		wg.Add(2)
		go func() {
			writePages(dir+string(os.PathSeparator)+"a.dat", positions, &lock)
			wg.Done()
		}()
		go func() {
			writePages(dir+string(os.PathSeparator)+"b.dat", positions, &lock)
			wg.Done()
		}()
		wg.Wait()
	}
}

func BenchmarkWriteFilesRandom_MultiThreadedSynchronizedLocked(b *testing.B) {
	positions := make([]int, fileSize/pageSize)
	for i := range positions {
		positions[i] = i
	}
	rand.Shuffle(len(positions), func(i, j int) { positions[i], positions[j] = positions[j], positions[i] })

	var lock sync.Mutex
	dir := getWorkDirectory(b)
	for i := 0; i < b.N; i++ {
		wg := sync.WaitGroup{}
		wg.Add(2)
		go func() {
			runtime.LockOSThread()
			defer runtime.UnlockOSThread()
			writePages(dir+string(os.PathSeparator)+"a.dat", positions, &lock)
			wg.Done()
		}()
		go func() {
			runtime.LockOSThread()
			defer runtime.UnlockOSThread()
			writePages(dir+string(os.PathSeparator)+"b.dat", positions, &lock)
			wg.Done()
		}()
		wg.Wait()
	}
}

const useWriteAt = false

func writePages(name string, positions []int, lock *sync.Mutex) error {
	f, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer os.Remove(name)
	defer f.Close()

	data := make([]byte, pageSize)
	for _, position := range positions {
		if lock != nil {
			lock.Lock()
		}
		if useWriteAt {
			if _, err := f.WriteAt(data, pageSize*int64(position)); err != nil {
				if lock != nil {
					lock.Unlock()
				}
				return err
			}
		} else {
			if _, err := f.Seek(pageSize*int64(position), 0); err != nil {
				if lock != nil {
					lock.Unlock()
				}
				return fmt.Errorf("failed to seek position: %w", err)
			}
			if _, err := f.Write(data); err != nil {
				if lock != nil {
					lock.Unlock()
				}
				return fmt.Errorf("failed to write data: %w", err)
			}
		}
		if lock != nil {
			lock.Unlock()
		}
	}

	return nil
}
