package utils

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"
	"testing"
)

func TestBufferedFile_OpenClose(t *testing.T) {
	path := t.TempDir() + "/test.dat"
	file, err := OpenBufferedFile(path)
	if err != nil {
		t.Fatalf("failed to open buffered file: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Errorf("failed to close buffered file: %v", err)
	}
}

func TestBufferedFile_WrittenDataCanBeRead(t *testing.T) {
	for _, n := range []int{1, 10, 100, 1000} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			path := t.TempDir() + "/test.dat"
			file, err := OpenBufferedFile(path)
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

func TestBufferedFile_DataIsPersistent(t *testing.T) {
	for _, n := range []int{1, 10, 100, 1000} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			path := t.TempDir() + "/test.dat"
			file, err := OpenBufferedFile(path)
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
			file, err = OpenBufferedFile(path)
			if err != nil {
				t.Fatalf("failed to reopen buffered file: %v", err)
			}

			for i := 0; i < n; i++ {
				dst := []byte{0}
				if err := file.Read(int64(i), dst); err != nil {
					t.Fatalf("failed to read at position %d: %v", i, err)
				}
				if dst[0] != byte(i+1) {
					t.Errorf("invalid data read at postion %d, wanted %d, got %d", i, byte(i+1), dst[0])
				}
			}

			if err := file.Close(); err != nil {
				t.Errorf("failed to close buffered file: %v", err)
			}
		})
	}
}

func TestBufferedFile_ReadAndWriteCanHandleUnalignedData(t *testing.T) {
	path := t.TempDir() + "/test.dat"
	file, err := OpenBufferedFile(path)
	if err != nil {
		t.Fatalf("failed to open buffered file: %v", err)
	}

	// By writting data of length 3 we are sometimes writing data crossing
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

func TestBufferedFile_WriteAndReadAddBufferBoundary(t *testing.T) {
	path := t.TempDir() + "/test.dat"
	file, err := OpenBufferedFile(path)
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

// updatePair consists of a position and data to use in fuzzing of the buffered file.
// This pair is used either to write and read data at certain position in the file.
// Depending of the use, the data payload either contains data to write to the while
// or data expected to be read from the file.
type updatePair struct {
	pos  int64
	data []byte
}

// serialise converts the updatePair to a byte array.
// The format is simple: <position><len><data>
func (p *updatePair) serialise() []byte {
	res := make([]byte, 0, len(p.data)+8+2)
	res = binary.BigEndian.AppendUint64(res, uint64(p.pos))
	res = binary.BigEndian.AppendUint16(res, uint16(len(p.data)))
	return append(res, p.data...)
}

// parseUpdates convert input bytes in the array of updatePair.
// The format is a set of tuples: <position><len><data>
// This method tries to parse as many those tuples as possible, terminating when no more
// elements are available.
// Certain modifications of input sequence may take place:
// first the position is caped to 2bytes not to seek in extensively huge files
// secondly, the data payload is trimmed to max 2 * bufferSize.
func parseUpdates(b []byte) []updatePair {
	var pairs []updatePair
	for len(b) > 4 {
		// cap position to decent numbers not to allocate huge files
		pos := binary.BigEndian.Uint16(b[0:2])
		// cap the length to max buffer because
		// a) payloads spanning more buffers is not supported
		// b) slices for full address space of Uint16 cannot be created
		l := binary.BigEndian.Uint16(b[2:4]) % (2 * bufferSize)
		if len(b) < int(l)+4 {
			l = uint16(len(b) - 4)
		}
		data := b[4 : l+4]
		pairs = append(pairs, updatePair{int64(pos), data})
		b = b[l+4:]
	}
	return pairs
}

func FuzzBufferedFile_ReadWrite(f *testing.F) {
	length := 5 * bufferSize // length above the buffer size
	long := make([]byte, length)
	for i := 0; i < length; i++ {
		long[i]++
	}

	updates := []updatePair{
		{0, []byte("Hello")},
		{2, []byte("A")},
		{5, []byte("aaaaaaaaaaaaaaaaa")},
		{10, []byte{}},
		{20, long},
		{50, []byte("123456")},
		{int64(length), []byte("Exceeds one buffer")},
	}

	// sample variants of the updates to seed the fuzzing
	for start := 0; start < len(updates); start++ {
		for end := start; end < len(updates); end++ {
			var raw []byte
			for _, update := range updates[start:end] {
				raw = append(raw, update.serialise()...)
			}
			f.Add(raw)
		}
	}

	f.Fuzz(func(t *testing.T, rawData []byte) {
		path := t.TempDir() + "/test.dat"
		file, err := OpenBufferedFile(path)
		if err != nil {
			t.Fatalf("failed to open buffered file: %v", err)
		}
		defer file.Close()

		ops := parseUpdates(rawData)
		for _, op := range ops {
			if err := file.Write(op.pos, op.data); err != nil {
				// expected errors in certain situations
				if len(op.data) >= bufferSize && strings.HasPrefix(err.Error(), "writing data >") {
					continue
				}
				if op.pos < 0 && strings.HasPrefix(err.Error(), "cannot write at negative position") {
					continue
				}

				t.Errorf("error to write to file: %s", err)
			}

			dst := make([]byte, len(op.data))
			if err := file.Read(op.pos, dst); err != nil {
				t.Fatalf("error to read from file: %s", err)
			}

			if !bytes.Equal(op.data, dst) {
				t.Errorf("data read from file does not match written data: %x != %x", op.data, dst)
			}
		}
	})
}
