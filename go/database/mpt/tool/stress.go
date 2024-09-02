// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/Fantom-foundation/Carmen/go/common"
	"github.com/Fantom-foundation/Carmen/go/common/interrupt"
	"github.com/Fantom-foundation/Carmen/go/database/mpt"
	"github.com/urfave/cli/v2"
)

// StressTestCmd is a command to stress test an MPT database. In particular,
// this command performs random inserts, updates, and account deletions on
// an MPT data base with the aim of stress-testing core components like the
// node cache, the write buffer, and the background flush mechanism.
var StressTestCmd = cli.Command{
	Action: addPerformanceDiagnoses(stressTest),
	Name:   "stress-test",
	Usage:  "stress test an MPT database",
	Flags: []cli.Flag{
		&tmpDirFlag,
		&numBlocksFlag,
		&reportPeriodFlag,
		&flushPeriodFlag,
	},
}

var (
	flushPeriodFlag = cli.DurationFlag{
		Name:  "flush-period",
		Usage: "the time between background node flushes, disabled if negative",
		Value: time.Millisecond,
	}
	reportPeriodFlag = cli.DurationFlag{
		Name:  "report-period",
		Usage: "the time between reports",
		Value: 5 * time.Second,
	}
)

func stressTest(context *cli.Context) error {
	const (
		MiB       = 1024 * 1024
		cacheSize = 64 * MiB
	)

	tmpDir := context.String(tmpDirFlag.Name)
	if len(tmpDir) == 0 {
		tmpDir = os.TempDir()
	}

	dir, err := os.MkdirTemp(tmpDir, "carmen-stress-*")
	if err != nil {
		return fmt.Errorf("failed to create temporary directory: %w", err)
	}
	log.Printf("Using temporary directory: %s\n", dir)

	flushPeriod := context.Duration(flushPeriodFlag.Name)
	log.Printf("Using background flush period: %s\n", flushPeriod)

	reportPeriod := context.Duration(reportPeriodFlag.Name)
	log.Printf("Using report period: %s\n", reportPeriod)

	cacheConfig := mpt.NodeCacheConfig{
		Capacity:              cacheSize / mpt.EstimatePerNodeMemoryUsage(),
		BackgroundFlushPeriod: flushPeriod,
	}

	db, err := mpt.OpenGoFileState(dir, mpt.S5LiveConfig, cacheConfig)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}

	numBlocks := context.Int(numBlocksFlag.Name)
	if numBlocks <= 0 {
		numBlocks = 1000
	}
	log.Printf("Inserting %d blocks ...\n", numBlocks)

	state := createTestState(db, dir)

	var reportWg sync.WaitGroup
	reportWg.Add(1)
	stopReport := make(chan struct{})
	reporterStopped := false
	stopReporter := func() {
		if reporterStopped {
			return
		}
		close(stopReport)
		reportWg.Wait()
		reporterStopped = true
	}
	defer stopReporter()
	go func() {
		defer reportWg.Done()
		ticker := time.NewTicker(reportPeriod)
		for {
			select {
			case <-stopReport:
				return
			case <-ticker.C:
				state.ReportProgress()
			}
		}
	}()

	aborted := false
	ctx := interrupt.CancelOnInterrupt(context.Context)
	rand := rand.New(rand.NewSource(state.start.UnixNano()))
	for i := 0; i < numBlocks; i++ {
		if interrupt.IsCancelled(ctx) {
			aborted = true
			break
		}
		if err := state.AddBlock(rand); err != nil {
			return fmt.Errorf("failed to add block %d: %w", i, err)
		}
	}

	stopReporter()

	if !aborted {
		log.Printf("Processed %d blocks successfully\n", numBlocks)
		log.Printf("Closing and deleting database ...\n")
	}

	if err := db.Close(); err != nil {
		return fmt.Errorf("failed to close database: %w", err)
	}

	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("failed to remove directory: %w", err)
	}

	if !aborted {
		log.Printf("Done\n")
	}

	return nil
}

// --- Stress Test State ---
type stressTestState struct {
	start       time.Time
	directory   string
	db          *mpt.MptState
	lock        sync.Mutex
	state       map[int]map[int]int
	blockHeight int
	nextAccount int
	nextKey     int
}

func createTestState(db *mpt.MptState, directory string) *stressTestState {
	return &stressTestState{
		directory: directory,
		db:        db,
		start:     time.Now(),
		state:     map[int]map[int]int{},
	}
}

func (s *stressTestState) ReportProgress() {
	memUsage := getMemoryUsage()
	used := getDirectorySize(s.directory)
	free, err := getFreeSpace(s.directory)
	if err != nil {
		log.Printf("failed to get free space: %v\n", err)
		return
	}

	s.lock.Lock()
	numAccounts := len(s.state)
	numSlots := 0
	for _, storage := range s.state {
		numSlots += len(storage)
	}
	currentBlock := s.blockHeight
	s.lock.Unlock()

	time := time.Since(s.start)
	seconds := int(time.Seconds())
	hours := seconds / 3600
	minutes := (seconds / 60) % 60
	seconds = seconds % 60
	const GiB = 1024 * 1024 * 1024
	log.Printf(
		"[%d:%02d:%02d] Block %d added, managing %d accounts, %d slots, memory: %.2f GiB, disk used: %.2f GiB, disk free: %.2f GiB\n",
		hours, minutes, seconds,
		currentBlock,
		numAccounts,
		numSlots,
		float64(memUsage)/GiB,
		float64(used)/GiB,
		float64(free)/GiB,
	)
}

func (s *stressTestState) AddBlock(rand *rand.Rand) error {
	const (
		changesPerBlock = 1000
	)
	s.lock.Lock()
	defer s.lock.Unlock()
	for j := 0; j < changesPerBlock; j++ {
		// Select between insert, update, and delete operations.
		// The proportions have been adjusted to produce a slow
		// growth of the database and to have good chances of
		// deleting reasonable large accounts with a few dozen
		// slots.
		var err error
		switch c := rand.Float32(); {
		case c < 0.65:
			err = s.addSlot()
		case c < 0.995:
			err = s.updateSlot()
		default:
			err = s.deleteAccount()
		}
		if err != nil {
			return err
		}
	}

	if _, _, err := s.db.UpdateHashes(); err != nil {
		return fmt.Errorf("failed to update hashes: %w", err)
	}

	s.blockHeight++
	return nil
}

func (s *stressTestState) addSlot() error {
	isNewAccount := false
	addrIndex := 0
	if len(s.state) > 0 && rand.Float32() < 0.98 { // < most of the time an old account is re-used
		addrIndex = s.getRandomAccountIndex()
	} else {
		addrIndex = s.nextAccount
		s.nextAccount++
		isNewAccount = true
	}
	addr := intToAddress(addrIndex)

	if isNewAccount {
		s.state[addrIndex] = map[int]int{}
		if err := s.db.SetNonce(addr, common.ToNonce(1)); err != nil {
			return fmt.Errorf("failed to create account: %w", err)
		}
	}

	storage := s.state[addrIndex]
	keyIndex := s.nextKey
	s.nextKey++
	key := intToKey(keyIndex)

	current, err := s.db.GetStorage(addr, key)
	if err != nil {
		return fmt.Errorf("failed to get value: %w", err)
	}
	if want, got := (common.Value{}), current; want != got {
		return fmt.Errorf("unexpected value %d/%d - wanted %x, got %x", addrIndex, keyIndex, want, got)
	}

	value := intToValue(1)
	if err := s.db.SetStorage(addr, key, value); err != nil {
		return fmt.Errorf("failed to set value: %w", err)
	}
	storage[keyIndex] = 1
	return nil
}

func (s *stressTestState) updateSlot() error {
	if len(s.state) == 0 {
		return nil
	}
	addrIndex := s.getRandomAccountIndex()
	addr := intToAddress(addrIndex)
	storage := s.state[addrIndex]

	keyIndex := 0
	for i := range storage {
		keyIndex = i
		break
	}
	key := intToKey(keyIndex)

	current, err := s.db.GetStorage(addr, key)
	if err != nil {
		return fmt.Errorf("failed to get value: %w", err)
	}
	if want, got := intToValue(storage[keyIndex]), current; want != got {
		return fmt.Errorf("unexpected value %v/%v before update - wanted %x, got %x", addr, key, want, got)
	}

	newValue := storage[keyIndex] + 1
	value := intToValue(newValue)
	if err := s.db.SetStorage(addr, key, value); err != nil {
		return fmt.Errorf("failed to set value: %w", err)
	}
	storage[keyIndex] = newValue
	return nil
}

func (s *stressTestState) deleteAccount() error {
	if len(s.state) == 0 {
		return nil
	}
	addrIndex := s.getRandomAccountIndex()
	addr := intToAddress(addrIndex)
	if err := s.db.DeleteAccount(addr); err != nil {
		return fmt.Errorf("failed to remove account: %w", err)
	}
	delete(s.state, addrIndex)
	return nil
}

func (s *stressTestState) getRandomAccountIndex() int {
	for i := range s.state { // iteration order is random, we pick the first one
		return i
	}
	panic("no accounts")
}

// --- utility functions ---

func intToAddress(i int) common.Address {
	return common.Address{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24)}
}

func intToKey(i int) common.Key {
	return common.Key{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24)}
}

func intToValue(i int) common.Value {
	return common.Value{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24)}
}

// GetFreeSpace returns the amount of free space in bytes on the filesystem containing the given path.
func getFreeSpace(path string) (int64, error) {
	fs := syscall.Statfs_t{}
	err := syscall.Statfs(path, &fs)
	if err != nil {
		return 0, err
	}
	return int64(fs.Bavail) * int64(fs.Bsize), nil
}

func getMemoryUsage() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.Alloc
}
