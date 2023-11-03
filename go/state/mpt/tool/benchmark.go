package main

import (
	"fmt"
	"io/fs"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"time"

	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/state"
	"github.com/urfave/cli/v2"
)

var Benchmark = cli.Command{
	Action: benchmark,
	Name:   "benchmark",
	Usage:  "benchmarks MPT performance by filling data into a fresh instance",
	Flags: []cli.Flag{
		&archiveFlag,
		&diagnosticsFlag,
		&numBlocksFlag,
		&numInsertsPerBlockFlag,
		&reportIntervalFlag,
		&tmpDirFlag,
		&keepStateFlag,
		&cpuProfileFlag,
	},
}

var (
	archiveFlag = cli.BoolFlag{
		Name:  "archive",
		Usage: "enables archive mode",
	}
	diagnosticsFlag = cli.IntFlag{
		Name:  "diagnostic-port",
		Usage: "enable hosting of a realtime diagnostic server by providing a port",
		Value: 0,
	}
	numBlocksFlag = cli.IntFlag{
		Name:  "num-blocks",
		Usage: "the number of blocks to be filled in",
		Value: 10_000,
	}
	numInsertsPerBlockFlag = cli.IntFlag{
		Name:  "inserts-per-block",
		Usage: "the number of inserts per block",
		Value: 1_000,
	}
	reportIntervalFlag = cli.IntFlag{
		Name:  "report-interval",
		Usage: "the size of a reporting interval in number of blocks",
		Value: 1000,
	}
	tmpDirFlag = cli.StringFlag{
		Name:  "tmp-dir",
		Usage: "the directory to place the state for running benchmarks on",
	}
	keepStateFlag = cli.BoolFlag{
		Name:  "keep-state",
		Usage: "disables the deletion of temporary data at the end of the benchmark",
	}
	cpuProfileFlag = cli.StringFlag{
		Name:  "cpuprofile",
		Usage: "sets the target file for storing CPU profiles to",
		Value: "profile.dat",
	}
)

func benchmark(context *cli.Context) error {
	tmpDir := context.String(tmpDirFlag.Name)
	if len(tmpDir) == 0 {
		tmpDir = os.TempDir()
	}

	diagnosticPort := context.Int(diagnosticsFlag.Name)
	if diagnosticPort > 0 && diagnosticPort < (1<<16) {
		fmt.Printf("Starting diagnostic server at port http://localhost:%d (see https://pkg.go.dev/net/http/pprof#hdr-Usage_examples for usage examples)\n", diagnosticPort)
		fmt.Printf("Block and mutex sampling rate is set to 100%% for diagnostics, which may impact overall performance\n")
		go func() {
			addr := fmt.Sprintf("localhost:%d", diagnosticPort)
			log.Println(http.ListenAndServe(addr, nil))
		}()
		runtime.SetBlockProfileRate(1)
		runtime.SetMutexProfileFraction(1)
	}

	start := time.Now()
	results, err := runBenchmark(
		benchmarkParams{
			archive:            context.Bool(archiveFlag.Name),
			numBlocks:          context.Int(numBlocksFlag.Name),
			numInsertsPerBlock: context.Int(numInsertsPerBlockFlag.Name),
			tmpDir:             tmpDir,
			keepState:          context.Bool(keepStateFlag.Name),
			cpuProfilePrefix:   context.String(cpuProfileFlag.Name),
			reportInterval:     context.Int(reportIntervalFlag.Name),
		},
		func(msg string, args ...any) {
			delta := uint64(time.Since(start).Round(time.Second).Seconds())
			fmt.Printf("[t=%3d:%02d:%02d]: ", delta/3600, (delta/60)%60, delta%60)
			fmt.Printf(msg+"\n", args...)
		},
	)
	if err != nil {
		return err
	}

	fmt.Printf("block, memory, disk, throughput\n")
	for _, cur := range results.intervals {
		fmt.Printf("%d, %d, %d, %.2f\n", cur.endOfBlock, cur.memory, cur.disk, cur.throughput)
	}
	fmt.Printf("Overall time: %v (+%v for reporting)\n", results.insertTime, results.reportTime)
	fmt.Printf("Overall throughput: %.2f inserts/second\n", float64(results.numInserts)/results.insertTime.Seconds())
	return nil
}

type benchmarkParams struct {
	archive            bool
	numBlocks          int
	numInsertsPerBlock int
	tmpDir             string
	keepState          bool
	cpuProfilePrefix   string
	reportInterval     int
}

type benchmarkRecord struct {
	endOfBlock int
	memory     int64
	disk       int64
	throughput float64
}

type benchmarkResult struct {
	intervals  []benchmarkRecord
	reportTime time.Duration
	insertTime time.Duration
	numInserts int64
}

func runBenchmark(
	params benchmarkParams,
	observer func(string, ...any),
) (benchmarkResult, error) {
	res := benchmarkResult{}
	// Start profiling ...
	if err := startCpuProfiler(fmt.Sprintf("%s_%06d", params.cpuProfilePrefix, 1)); err != nil {
		return res, err
	}
	defer stopCpuProfiler()

	// Create the target state.
	path := fmt.Sprintf(params.tmpDir+string(os.PathSeparator)+"state_%d", time.Now().Unix())

	if params.archive {
		observer("Creating state with archive in %s ..", path)
	} else {
		observer("Creating state without archive in %s ..", path)
	}
	if err := os.Mkdir(path, 0700); err != nil {
		return res, fmt.Errorf("failed to create temporary state directory: %v", err)
	}
	if params.keepState {
		observer("state in %s will not be removed at the end of the run", path)
	} else {
		observer("state in %s will be removed at the end of the run", path)
		defer func() {
			observer("Cleaning up state in %s ..", path)
			if err := os.RemoveAll(path); err != nil {
				observer("Cleanup failed: %v", err)
			}
		}()
	}

	// Open the state to be tested.
	archive := state.NoArchive
	if params.archive {
		archive = state.S5Archive
	}
	state, err := state.NewState(state.Parameters{
		Directory: path,
		Variant:   state.GoFile,
		Schema:    5,
		Archive:   archive,
	})
	if err != nil {
		return res, err
	}
	success := false
	defer func() {
		if !success {
			return
		}
		start := time.Now()
		if err := state.Close(); err != nil {
			observer("Failed to close state: %v", err)
		}
		observer("Closing state took %v", time.Since(start))
		observer("Final disk usage: %d", getDirectorySize(path))
	}()

	// Progress tracking.
	reportingInterval := params.reportInterval
	lastReportTime := time.Now()

	// Record results.
	res.intervals = make([]benchmarkRecord, 0, params.numBlocks/reportingInterval+1)

	benchmarkStart := time.Now()
	reportingTime := 0 * time.Second

	// Simulate insertions.
	numBlocks := params.numBlocks
	numInsertsPerBlock := params.numInsertsPerBlock
	counter := 0
	for i := 0; i < numBlocks; i++ {
		update := common.Update{}
		update.CreatedAccounts = make([]common.Address, 0, numInsertsPerBlock)
		for j := 0; j < numInsertsPerBlock; j++ {
			addr := common.Address{byte(counter >> 24), byte(counter >> 16), byte(counter >> 8), byte(counter)}
			update.CreatedAccounts = append(update.CreatedAccounts, addr)
			update.Nonces = append(update.Nonces, common.NonceUpdate{addr, common.ToNonce(1)})
			counter++
		}
		if err := state.Apply(uint64(i), update); err != nil {
			return res, fmt.Errorf("error applying block %d: %v", i, err)
		}

		if (i+1)%reportingInterval == 0 {
			stopCpuProfiler()
			startReporting := time.Now()

			throughput := float64(reportingInterval*numInsertsPerBlock) / startReporting.Sub(lastReportTime).Seconds()
			memory := state.GetMemoryFootprint().Total()
			disk := getDirectorySize(path)
			observer(
				"Reached block %d, memory %.2f GB, disk %.2f GB, %.2f inserts/second",
				i+1,
				float64(memory)/float64(1<<30),
				float64(disk)/float64(1<<30),
				throughput,
			)

			res.intervals = append(res.intervals, benchmarkRecord{
				endOfBlock: i + 1,
				memory:     int64(memory),
				disk:       disk,
				throughput: throughput,
			})

			endReporting := time.Now()
			reportingTime += endReporting.Sub(startReporting)
			lastReportTime = endReporting
			startCpuProfiler(fmt.Sprintf("%s_%06d", params.cpuProfilePrefix, ((i+1)/reportingInterval)+1))
		}
	}
	observer("Finished %.2e blocks with %.2e inserts", float64(numBlocks), float64(numBlocks*numInsertsPerBlock))

	benchmarkTime := time.Since(benchmarkStart)
	res.numInserts = int64(counter)
	res.insertTime = benchmarkTime - reportingTime
	res.reportTime = reportingTime

	success = true
	return res, nil
}

func startCpuProfiler(filename string) error {
	f, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("could not create CPU profile: %s", err)
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		return fmt.Errorf("could not start CPU profile: %s", err)
	}
	return nil
}

func stopCpuProfiler() {
	pprof.StopCPUProfile()
}

// GetDirectorySize computes the size of all files in the given directory in bytes.
func getDirectorySize(directory string) int64 {
	var sum int64 = 0
	filepath.Walk(directory, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if !info.IsDir() {
			sum += info.Size()
		}
		return nil
	})
	return sum
}
