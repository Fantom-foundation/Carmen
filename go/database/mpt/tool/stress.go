package main

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"github.com/Fantom-foundation/Carmen/go/carmen"
	"github.com/urfave/cli/v2"
)

var Stress = cli.Command{
	Action: stress,
	Name:   "stress",
	Usage:  "stress test an MPT database",
	Flags: []cli.Flag{
		&tmpDirFlag,
		&numBlocksFlag,
		&reportIntervalFlag,
		&seedFlag,
	},
}

var (
	seedFlag = cli.IntFlag{
		Name:  "seed",
		Usage: "the seed for the random number generator, 0 for a random seed",
		Value: 0,
	}
)

func stress(context *cli.Context) error {

	tmpDir := context.String(tmpDirFlag.Name)
	if len(tmpDir) == 0 {
		tmpDir = os.TempDir()
	}

	dir := filepath.Join(tmpDir, fmt.Sprintf("carmen-stress-%d", time.Now().UnixNano()))
	fmt.Printf("Using temporary directory: %s\n", dir)

	properties := carmen.Properties{
		carmen.LiveDBCache: fmt.Sprintf("%d", 64<<20), // 64 MiB
	}

	db, err := carmen.OpenDatabase(dir, carmen.GetCarmenGoS5WithoutArchiveConfiguration(), properties)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}

	numBlocks := context.Int(numBlocksFlag.Name)
	if numBlocks <= 0 {
		numBlocks = 1000
	}
	fmt.Printf("Inserting %d blocks ...\n", numBlocks)

	reportingInterval := context.Int(reportIntervalFlag.Name)
	if reportingInterval <= 0 {
		reportingInterval = 100
	}

	seed := context.Int64(seedFlag.Name)
	if seed <= 0 {
		seed = time.Now().UnixNano()
	}
	fmt.Printf("Using seed: %d\n", seed)
	start := time.Now()
	rand := rand.New(rand.NewSource(seed))

	nextAccount := 0
	nextKey := 0
	state := map[int]map[int]int{}

	getRandomAccountIndex := func() int {
		for i := range state { // iteration order is random, we pick the first one
			return i
		}
		panic("no accounts")
	}

	for i := 0; i < numBlocks; i++ {
		destructed := map[int]struct{}{}
		err := db.AddBlock(uint64(i), func(ctxt carmen.HeadBlockContext) error {
			return ctxt.RunTransaction(func(ctxt carmen.TransactionContext) error {
				// touch 1000 random slots
				for j := 0; j < 1000; j++ {

					// 45% chance to add a slot, 45% chance to update a slot, 10% chance to remove an account.
					switch c := rand.Float32(); {
					case c < 0.65:
						// add a new slot
						// 20:80 of reusing an account or creating a new one
						isNew := false
						addrIndex := 0
						if len(state) > 0 && rand.Float32() < 0.99 {
							addrIndex = getRandomAccountIndex()
						} else {
							addrIndex = nextAccount
							nextAccount++
							isNew = true
						}
						if _, found := destructed[addrIndex]; found {
							continue // skip if the account was destructed
						}
						addr := intToAddress(addrIndex)

						if isNew {
							state[addrIndex] = map[int]int{}
							ctxt.SetNonce(addr, 1) // implicit account creation
						}

						storage := state[addrIndex]
						keyIndex := nextKey
						nextKey++
						key := intToKey(keyIndex)

						current := ctxt.GetState(addr, key)
						if want, got := (carmen.Value{}), current; want != got {
							return fmt.Errorf("unexpected value %d/%d - wanted %x, got %x", addrIndex, keyIndex, want, got)
						}

						value := intToValue(1)
						ctxt.SetState(addr, key, value)
						//fmt.Printf("Setting %d/%d to %d\n", addrIndex, keyIndex, 1)
						storage[keyIndex] = 1

					case c < 0.995:
						if len(state) == 0 {
							continue
						}
						// update an existing slot
						addrIndex := getRandomAccountIndex()
						addr := intToAddress(addrIndex)
						storage := state[addrIndex]

						keyIndex := 0
						for i := range storage {
							keyIndex = i
							break
						}
						key := intToKey(keyIndex)

						current := ctxt.GetState(addr, key)
						if want, got := intToValue(storage[keyIndex]), current; want != got {
							return fmt.Errorf("unexpected value %d/%d before update - wanted %x, got %x", addrIndex, keyIndex, want, got)
						}

						newValue := storage[keyIndex] + 1
						value := intToValue(newValue)
						ctxt.SetState(addr, key, value)
						storage[keyIndex] = newValue

						//fmt.Printf("Updating %d/%d to %d\n", addrIndex, keyIndex, newValue)

					default:
						// remove an account
						if len(state) == 0 {
							continue
						}
						addrIndex := getRandomAccountIndex()
						if false && len(state[addrIndex]) > 50 {
							fmt.Printf("deleting %d with %d keys\n", addrIndex, len(state[addrIndex]))
						}
						addr := intToAddress(addrIndex)
						ctxt.SelfDestruct(addr)
						delete(state, addrIndex)
						destructed[addrIndex] = struct{}{}
					}
				}

				return nil
			})
		})
		if err != nil {
			return fmt.Errorf("failed to add block %d: %w", i, err)
		}

		if (i+1)%reportingInterval == 0 {
			memUsage := getMemoryUsage()
			used := getDirectorySize(dir)
			free, err := getFreeSpace(dir)
			if err != nil {
				return fmt.Errorf("failed to get free space: %w", err)
			}

			numAccounts := len(state)
			numSlots := 0
			for _, storage := range state {
				numSlots += len(storage)
			}

			time := time.Since(start)
			seconds := int(time.Seconds())
			hours := seconds / 3600
			minutes := (seconds / 60) % 60
			seconds = seconds % 60
			const GiB = 1024 * 1024 * 1024
			fmt.Printf(
				"[%d:%02d:%02d] Block %d added, managing %d accounts, %d slots, memory: %.2f GiB, disk used: %.2f GiB, disk free: %.2f GiB\n",
				hours, minutes, seconds,
				i,
				numAccounts,
				numSlots,
				float64(memUsage)/GiB,
				float64(used)/GiB,
				float64(free)/GiB,
			)
		}
	}

	if err := db.Close(); err != nil {
		return fmt.Errorf("failed to close database: %w", err)
	}

	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("failed to remove directory: %w", err)
	}

	return nil
}

func intToAddress(i int) carmen.Address {
	return carmen.Address{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24)}
}

func intToKey(i int) carmen.Key {
	return carmen.Key{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24)}
}

func intToValue(i int) carmen.Value {
	return carmen.Value{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24)}
}

// GetFreeSpace returns the amount of free space in bytes on the filesystem containing the given path.
func getFreeSpace(path string) (int64, error) {
	fs := syscall.Statfs_t{}
	err := syscall.Statfs(path, &fs)
	if err != nil {
		return 0, err
	}
	return int64(fs.Bavail) * fs.Bsize, nil
}

func getMemoryUsage() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.Alloc
}
