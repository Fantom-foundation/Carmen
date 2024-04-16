package common

import (
	"fmt"
	"runtime"
	"strings"
	"time"
)

// MemoryUsageCallback is a callback function that will be called with the current memory stats
type MemoryUsageCallback func(*runtime.MemStats)

// GetMemoryUsage returns the memory usage statistics.
// If runGc is true, it will run the garbage collector before getting the stats.
// This will return the memory usage at the time of the call.
func GetMemoryUsage(runGc bool) runtime.MemStats {
	if runGc {
		runtime.GC()
	}
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m
}

// SampleMemoryUsageForCall samples the memory usage statistics while the function is running.
// It will call the callback function with the memory stats at the specified interval in seconds.
// If runGc is true, it will run the garbage collector before getting the stats.
func SampleMemoryUsageForCall(interval float32, runGc bool, f func(), clb MemoryUsageCallback) {
	// start go-routine that will sample memory usage
	// while the function is running
	done := make(chan struct{})
	defer close(done)
	go func() {
		ticker := time.NewTicker(time.Duration(interval) * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				m := GetMemoryUsage(runGc)
				clb(&m)
			}
		}
	}()
	// run the function
	f()
}

// SampleAndPrintMemoryUsageForCall samples the memory usage statistics and prints them while the function is running.
// If runGc is true, it will run the garbage collector before printing the stats.
func SampleAndPrintMemoryUsageForCall(interval float32, runGc bool, f func()) {
	SampleMemoryUsageForCall(interval, runGc, f, printMemoryUsage)
}

// PrintMemoryUsage outputs the current, total and OS memory being used. As well as the number of garbage collection cycles completed.
// If runGc is true, it will run the garbage collector before printing the stats.
// It will output the memory usage at the time of the call.
func PrintMemoryUsage(runGc bool) {
	m := GetMemoryUsage(runGc)
	printMemoryUsage(&m)
}

// printMemoryUsage prints the memory usage statistics.
func printMemoryUsage(stats *runtime.MemStats) {
	sb := strings.Builder{}
	sb.WriteString("Alloc = ")
	memoryAmountToString(&sb, uintptr(stats.Alloc))
	sb.WriteString("\tTotalAlloc = ")
	memoryAmountToString(&sb, uintptr(stats.TotalAlloc))
	sb.WriteString("\tSys = ")
	memoryAmountToString(&sb, uintptr(stats.Sys))
	sb.WriteString(fmt.Sprintf("\tNumGC = %v", stats.NumGC))
	fmt.Println(sb.String())
}
