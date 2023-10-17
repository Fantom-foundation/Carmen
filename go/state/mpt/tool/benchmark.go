package main

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime/pprof"
	"time"

	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/state/mpt"
	"github.com/urfave/cli/v2"
)

var Benchmark = cli.Command{
	Action: benchmark,
	Name:   "benchmark",
	Usage:  "benchmarks MPT performance by filling data into a fresh instance",
	Flags: []cli.Flag{
		&numBlocksFlag,
		&numInsertsPerBlockFlag,
		&reportIntervalFlag,
		&tmpDirFlag,
		&keepMptFlag,
		&cpuProfileFlag,
	},
}

var (
	numBlocksFlag = cli.IntFlag{
		Name:  "num-blocks",
		Usage: "the number of blocks to be filled in",
		Value: 10_000,
	}
	numInsertsPerBlockFlag = cli.IntFlag{
		Name:  "inserts-per-block",
		Usage: "the number inserts per block",
		Value: 1_000,
	}
	reportIntervalFlag = cli.IntFlag{
		Name:  "report-interval",
		Usage: "the size of a reporting interval in number of blocks",
		Value: 1000,
	}
	tmpDirFlag = cli.StringFlag{
		Name:  "tmp-dir",
		Usage: "the directory to place the MPT for running benchmarks on",
	}
	keepMptFlag = cli.BoolFlag{
		Name:  "keep-mpt",
		Usage: "disables the deletion of temporary data at the end of the benchmark",
	}
	cpuProfileFlag = cli.StringFlag{
		Name:  "cpuprofile",
		Usage: "sets the target file for storing CPU profiles to",
		Value: "profile.dat",
	}
	// TODO
	//  - cpu-profiling
)

func benchmark(context *cli.Context) error {
	tmpDir := context.String(tmpDirFlag.Name)
	if len(tmpDir) == 0 {
		tmpDir = os.TempDir()
	}

	start := time.Now()
	results, err := runBenchmark(
		benchmarkParams{
			numBlocks:          context.Int(numBlocksFlag.Name),
			numInsertsPerBlock: context.Int(numInsertsPerBlockFlag.Name),
			tmpDir:             tmpDir,
			keepMpt:            context.Bool(keepMptFlag.Name),
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
	for _, cur := range results {
		fmt.Printf("%d, %d, %d, %.2f\n", cur.endOfBlock, cur.memory, cur.disk, cur.throughput)
	}
	return nil
}

type benchmarkParams struct {
	numBlocks          int
	numInsertsPerBlock int
	tmpDir             string
	keepMpt            bool
	cpuProfilePrefix   string
	reportInterval     int
}

type benchmarkRecord struct {
	endOfBlock int
	memory     int64
	disk       int64
	throughput float64
}

func runBenchmark(
	params benchmarkParams,
	observer func(string, ...any),
) ([]benchmarkRecord, error) {
	// Start profiling ...
	if err := startCpuProfiler(fmt.Sprintf("%s_%06d", params.cpuProfilePrefix, 1)); err != nil {
		return nil, err
	}
	defer stopCpuProfiler()

	// Create the target MPT.
	path := fmt.Sprintf(params.tmpDir+string(os.PathSeparator)+"mpt_%d", time.Now().Unix())
	observer("Creating MPT in %s ..", path)
	if err := os.Mkdir(path, 0700); err != nil {
		return nil, fmt.Errorf("failed to create temporary MPT directory: %v", err)
	}
	if params.keepMpt {
		observer("MPT in %s will not be removed at end of the run", path)
	} else {
		observer("MPT in %s will be removed at end of the run", path)
		defer func() {
			observer("Cleaning up MPT in in %s ..", path)
			if err := os.RemoveAll(path); err != nil {
				observer("Cleanup failed: %v", err)
			}
		}()
	}

	// Open the MPT to be tested.
	trie, err := mpt.OpenFileLiveTrie(path, mpt.S5Config)
	if err != nil {
		return nil, err
	}
	defer func() {
		start := time.Now()
		if err := trie.Close(); err != nil {
			observer("Failed to close MPT: %v", err)
		}
		observer("Closing MPT took %v", time.Since(start))
		observer("Final disk usage: %d", getDirectorySize(path))
	}()

	// Progress tracking.
	reportingInterval := params.reportInterval
	lastReportTime := time.Now()

	// Record results.
	results := make([]benchmarkRecord, 0, params.numBlocks/reportingInterval+1)

	// Simulate insertions.
	numBlocks := params.numBlocks
	numInsertsPerBlock := params.numInsertsPerBlock
	counter := 0
	for i := 0; i < numBlocks; i++ {
		for j := 0; j < numInsertsPerBlock; j++ {
			addr := common.Address{byte(counter >> 24), byte(counter >> 16), byte(counter >> 8), byte(counter)}
			err := trie.SetAccountInfo(addr, mpt.AccountInfo{Nonce: common.ToNonce(1)})
			if err != nil {
				return results, fmt.Errorf("error inserting new account: %v", err)
			}
			counter++
		}
		if _, err := trie.GetHash(); err != nil {
			return results, fmt.Errorf("error computing hash: %v", err)
		}
		if (i+1)%reportingInterval == 0 {
			stopCpuProfiler()
			now := time.Now()

			throughput := float64(reportingInterval*numInsertsPerBlock) / now.Sub(lastReportTime).Seconds()
			memory := trie.GetMemoryFootprint().Total()
			disk := getDirectorySize(path)
			observer(
				"Reached block %d, memory %.2f GB, disk %.2f GB, %.2f inserts/second",
				i+1,
				float64(memory)/float64(1<<30),
				float64(disk)/float64(1<<30),
				throughput,
			)

			results = append(results, benchmarkRecord{
				endOfBlock: i + 1,
				memory:     int64(memory),
				disk:       disk,
				throughput: throughput,
			})

			lastReportTime = time.Now()
			startCpuProfiler(fmt.Sprintf("%s_%06d", params.cpuProfilePrefix, ((i+1)/reportingInterval)+1))
		}
	}
	observer("Finished %.2e blocks with %.2e inserts", float64(numBlocks), float64(numBlocks*numInsertsPerBlock))
	return results, nil
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
