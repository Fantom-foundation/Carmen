package utils

import (
	"bytes"
	"fmt"
	"testing"
)

func TestPagedFile_OpenClose(t *testing.T) {
	path := t.TempDir() + "/test.dat"
	file, err := OpenPagedFile(path)
	if err != nil {
		t.Fatalf("failed to open buffered file: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Errorf("failed to close buffered file: %v", err)
	}
}

func TestPagedFile_WrittenDataCanBeRead(t *testing.T) {
	for _, n := range []int{1, 10, 100, 1000} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			path := t.TempDir() + "/test.dat"
			file, err := OpenPagedFile(path)
			if err != nil {
				t.Fatalf("failed to open buffered file: %v", err)
			}

			for i := 0; i < n; i++ {
				if err := file.Write(int64(i), []byte{byte(i)}); err != nil {
					t.Fatalf("failed to write at position %d: %v", i, err)
				}
			}

			for i := 0; i < n; i++ {
				dst := []byte{0}
				if err := file.Read(int64(i), dst); err != nil {
					t.Fatalf("failed to read at position %d: %v", i, err)
				}
				if dst[0] != byte(i) {
					t.Errorf("invalid data read at postion %d, wanted %d, got %d", i, byte(i), dst[0])
				}
			}

			if err := file.Close(); err != nil {
				t.Errorf("failed to close buffered file: %v", err)
			}
		})
	}
}

func TestPagedFile_DataIsPersistent(t *testing.T) {
	for _, n := range []int{1, 10, 100, 1000} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			path := t.TempDir() + "/test.dat"
			file, err := OpenPagedFile(path)
			if err != nil {
				t.Fatalf("failed to open buffered file: %v", err)
			}

			for i := 0; i < n; i++ {
				if err := file.Write(int64(i), []byte{byte(i + 1)}); err != nil {
					t.Fatalf("failed to write at position %d: %v", i, err)
				}
			}

			if err := file.Close(); err != nil {
				t.Fatalf("failed to close file: %v", err)
			}

			// Reopen the file.
			file, err = OpenPagedFile(path)
			if err != nil {
				t.Fatalf("failed to reopen buffered file: %v", err)
			}

			for i := 0; i < n; i++ {
				dst := []byte{0}
				if err := file.Read(int64(i), dst); err != nil {
					t.Fatalf("failed to read at position %d: %v", i, err)
				}
				if dst[0] != byte(i+1) {
					t.Errorf("invalid data read at position %d, wanted %d, got %d", i, byte(i+1), dst[0])
				}
			}

			if err := file.Close(); err != nil {
				t.Errorf("failed to close buffered file: %v", err)
			}
		})
	}
}

func TestPagedFile_ReadAndWriteCanHandleUnalignedData(t *testing.T) {
	path := t.TempDir() + "/test.dat"
	file, err := OpenPagedFile(path)
	if err != nil {
		t.Fatalf("failed to open buffered file: %v", err)
	}

	// By writing data of length 3 we are sometimes writing data crossing
	// the internal aligned buffer-page boundary.
	for i := 0; i < 1000; i++ {
		if err := file.Write(int64(i)*3, []byte{byte(i), byte(i + 1), byte(i + 2)}); err != nil {
			t.Fatalf("failed to write at position %d: %v", i, err)
		}
	}

	for i := 0; i < 1000; i++ {
		dst := []byte{0, 0, 0}
		if err := file.Read(int64(i)*3, dst); err != nil {
			t.Fatalf("failed to read at position %d: %v", i, err)
		}
		want := []byte{byte(i), byte(i + 1), byte(i + 2)}
		if !bytes.Equal(dst, want) {
			t.Errorf("invalid data read at postion %d, wanted %d, got %d", i, want, dst)
		}
	}

	if err := file.Close(); err != nil {
		t.Errorf("failed to close buffered file: %v", err)
	}
}

func TestPagedFile_WriteAndReadAddBufferBoundary(t *testing.T) {
	path := t.TempDir() + "/test.dat"
	file, err := OpenPagedFile(path)
	if err != nil {
		t.Fatalf("failed to open buffered file: %v", err)
	}

	src := []byte{1, 2, 3, 4, 5}
	file.Write(5*bufferSize-2, src)

	dst := []byte{0, 0, 0, 0, 0}
	file.Read(5*bufferSize-2, dst)

	if !bytes.Equal(src, dst) {
		t.Errorf("failed to read data written across buffer boundary, wanted %v, got %v", src, dst)
	}
}

func TestPagedFile_WriteGrowsFile(t *testing.T) {
	path := t.TempDir() + "/test.dat"
	file, err := OpenPagedFile(path)
	if err != nil {
		t.Fatalf("failed to open buffered file: %v", err)
	}

	src := []byte{1, 2, 3, 4, 5}
	file.Write(20*pageSize, src)

	/*
	dst := []byte{0, 0, 0, 0, 0}
	file.Read(20*pageSize, dst)

	if !bytes.Equal(src, dst) {
		t.Errorf("failed to read data written across buffer boundary, wanted %v, got %v", src, dst)
	}
*/
	file.Close()

	file, _ = OpenPagedFile(path)
	dst := []byte{0, 0, 0, 0, 0}
	file.Read(20*pageSize, dst)

	if !bytes.Equal(src, dst) {
		t.Errorf("failed to read data written across buffer boundary, wanted %v, got %v", src, dst)
	}
}
