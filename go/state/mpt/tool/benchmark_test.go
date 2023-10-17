package main

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBenchmark_RunExampleBenchmark(t *testing.T) {
	dir := t.TempDir()
	result, err := runBenchmark(benchmarkParams{
		numBlocks:          300,
		numInsertsPerBlock: 10,
		tmpDir:             dir,
		reportInterval:     100,
		cpuProfilePrefix:   dir + "/profile.dat",
		keepMpt:            false,
	}, func(string, ...any) {})

	if err != nil {
		t.Fatalf("failed to run benchmark: %v", err)
	}

	if got, want := len(result), 3; got != want {
		t.Fatalf("unexpected size of result, wanted %d, got %d", want, got)
	}

	for i, cur := range result {
		if got, want := cur.endOfBlock, (i+1)*100; got != want {
			t.Errorf("invalid block in result line %d, wanted %d, got %d", i, want, got)
		}
		if cur.memory <= 0 {
			t.Errorf("invalid value for memory usage: %d", cur.memory)
		}
		if cur.disk <= 0 {
			t.Errorf("invalid value for dis usage: %d", cur.disk)
		}
		if cur.throughput <= 0 {
			t.Errorf("invalid value for throughput: %f", cur.throughput)
		}
		if !exists(fmt.Sprintf(dir+"/profile.dat_%06d", i+1)) {
			t.Errorf("missing cpu profile for interval %d", i+1)
		}
	}

	filepath.Walk(dir, func(path string, info fs.FileInfo, err error) error {
		if strings.HasPrefix(info.Name(), "mpt_") {
			t.Errorf("temporary DB was not deleted")
		}
		return nil
	})
}

func TestBenchmark_KeepMptRetainsMpt(t *testing.T) {
	dir := t.TempDir()
	_, err := runBenchmark(benchmarkParams{
		numBlocks:          300,
		numInsertsPerBlock: 10,
		tmpDir:             dir,
		reportInterval:     100,
		cpuProfilePrefix:   dir + "/profile.dat",
		keepMpt:            true,
	}, func(string, ...any) {})

	if err != nil {
		t.Fatalf("failed to run benchmark: %v", err)
	}

	found := false
	filepath.Walk(dir, func(path string, info fs.FileInfo, err error) error {
		if strings.HasPrefix(info.Name(), "mpt_") {
			found = true
		}
		return nil
	})

	if !found {
		t.Errorf("temporary MPT was not retained")
	}
}

func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
