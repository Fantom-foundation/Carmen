package state

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestStateConfigs_AllSetupsCreateDataInCorrectDirectories(t *testing.T) {
	for _, config := range initStates() {
		config := config
		for _, archive := range allArchiveTypes {
			archive := archive
			t.Run(fmt.Sprintf("%s-%v", config.name, archive), func(t *testing.T) {
				t.Parallel()
				dir := t.TempDir()
				state, err := config.createStateWithArchive(dir, archive)
				if errors.Is(err, UnsupportedConfiguration) {
					t.Skip()
				}
				if err != nil {
					t.Fatalf("failed to open state: %v", err)
				}
				if err := state.Close(); err != nil {
					t.Errorf("failed to close state: %v", err)
				}

				if !isDirectory(t, filepath.Join(dir, "live")) {
					t.Errorf("missing directory for the LiveDB")
				}
				if err := os.RemoveAll(filepath.Join(dir, "live")); err != nil {
					t.Fatalf("failed to delete 'live' directory: %v", err)
				}

				if archive != NoArchive {
					if !isDirectory(t, filepath.Join(dir, "archive")) {
						t.Errorf("missing directory for the Archive")
					}
					if err := os.RemoveAll(filepath.Join(dir, "archive")); err != nil {
						t.Fatalf("failed to delete 'archive' directory: %v", err)
					}
				}

				content := getFilesIn(t, dir)
				if len(content) > 0 {
					t.Errorf("some data stored outside the live and archive directories, the root directory contains: %v", content)
				}
			})
		}
	}
}

func isDirectory(t *testing.T, path string) bool {
	t.Helper()
	fileInfo, err := os.Stat(path)
	if err != nil {
		t.Fatalf("failed to get file infos for %v: %v", path, err)
	}
	return fileInfo.IsDir()
}

func getFilesIn(t *testing.T, path string) []string {
	t.Helper()
	if !isDirectory(t, path) {
		t.Fatalf("%s is not a directory", path)
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("failed to open directory %s: %v", path, err)
	}
	defer file.Close()
	_, err = file.Stat()
	if err != nil {
		t.Fatalf("failed to open file information for %s: %v", path, err)
	}
	content, err := file.Readdirnames(10)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("failed to list content of directory `%s`: %v", path, err)
	}
	return content
}
