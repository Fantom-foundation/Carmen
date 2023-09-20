package file

import (
	"fmt"
	"io"
	"os"
	"unsafe"

	"github.com/Fantom-foundation/Carmen/go/backend/stock"
	"github.com/Fantom-foundation/Carmen/go/common"
)

// stackBufferSize a constant defining the batch size of elements buffered in
// memory by the fileBasedStack implementation below.
const stackBufferSize = 1_024

// fileBasedStack is a file-backed stack of integer values.
type fileBasedStack[I stock.Index] struct {
	file         *os.File
	size         int
	buffer       []I
	bufferOffset int
}

func openFileBasedStack[I stock.Index](filename string) (*fileBasedStack[I], error) {
	// Check whether there is an existing stack file.
	size := 0
	valueSize := int(unsafe.Sizeof(I(0)))
	if stats, err := os.Stat(filename); err == nil {
		fileSize := stats.Size()
		if fileSize%int64(valueSize) != 0 {
			return nil, fmt.Errorf("invalid stack file size")
		}
		size = int(fileSize) / valueSize
	}

	file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}

	// Load tailing batch of elements if file is not empty.
	buffer := make([]I, 0, stackBufferSize)
	offset := 0
	if size > 0 {
		toLoad := size % stackBufferSize
		offset = size - toLoad

		if _, err := file.Seek(int64(valueSize*offset), 0); err != nil {
			return nil, err
		}

		valueBuffer := make([]byte, valueSize)
		for i := 0; i < toLoad; i++ {
			if _, err := io.ReadFull(file, valueBuffer); err != nil {
				return nil, err
			}
			buffer = append(buffer, stock.DecodeIndex[I](valueBuffer))
		}
	}

	return &fileBasedStack[I]{
		file:         file,
		size:         size,
		buffer:       buffer,
		bufferOffset: offset,
	}, nil
}

func (s *fileBasedStack[I]) Size() int {
	return s.size
}

func (s *fileBasedStack[I]) Empty() bool {
	return s.size == 0
}

func (s *fileBasedStack[I]) Push(value I) error {
	s.buffer = append(s.buffer, value)
	s.size++
	// Flush buffer if full.
	if len(s.buffer) == cap(s.buffer) {
		if err := s.flushBuffer(); err != nil {
			return err
		}
		s.bufferOffset += len(s.buffer)
		s.buffer = s.buffer[0:0]
	}
	return nil
}

func (s *fileBasedStack[I]) Pop() (I, error) {
	if s.size <= 0 {
		return 0, fmt.Errorf("cannot pop from empty stack")
	}

	// Fetch data from disk if in-memory buffer is empty.
	if len(s.buffer) == 0 {
		s.bufferOffset -= cap(s.buffer)

		valueSize := int(unsafe.Sizeof(I(0)))
		if _, err := s.file.Seek(int64(valueSize*s.bufferOffset), 0); err != nil {
			return 0, err
		}

		buffer := make([]byte, valueSize)
		for i := 0; i < cap(s.buffer); i++ {
			if _, err := io.ReadFull(s.file, buffer); err != nil {
				return 0, err
			}
			s.buffer = append(s.buffer, stock.DecodeIndex[I](buffer))
		}
	}

	bufferSize := len(s.buffer)
	res := s.buffer[bufferSize-1]
	s.buffer = s.buffer[0 : bufferSize-1]
	s.size--
	return res, nil
}

func (s *fileBasedStack[I]) flushBuffer() error {
	valueSize := int(unsafe.Sizeof(I(0)))
	if _, err := s.file.Seek(int64(valueSize*s.bufferOffset), 0); err != nil {
		return err
	}

	buffer := make([]byte, valueSize)
	for _, value := range s.buffer {
		stock.EncodeIndex[I](value, buffer)
		if _, err := s.file.Write(buffer); err != nil {
			return err
		}
	}
	return nil
}

func (s *fileBasedStack[I]) GetAll() ([]I, error) {
	// We flush out temporary data and load all entries from the file.
	if err := s.Flush(); err != nil {
		return nil, err
	}
	res := make([]I, 0, s.size)

	var i I
	entrySize := int(unsafe.Sizeof(i))
	buffer := make([]byte, s.size*entrySize)

	if _, err := s.file.Seek(0, 0); err != nil {
		return nil, err
	}

	if _, err := io.ReadFull(s.file, buffer); err != nil {
		return nil, err
	}

	for i := 0; i < s.size; i++ {
		res = append(res, stock.DecodeIndex[I](buffer[i*entrySize:]))
	}
	return res, nil
}

func (s *fileBasedStack[I]) Flush() error {
	if err := s.flushBuffer(); err != nil {
		return err
	}

	// Truncate file to needed size.
	valueSize := int(unsafe.Sizeof(I(0)))
	if err := s.file.Truncate(int64(s.size * valueSize)); err != nil {
		return err
	}

	return s.file.Sync()
}

func (s *fileBasedStack[I]) Close() error {
	if err := s.Flush(); err != nil {
		return err
	}
	return s.file.Close()
}

func (s *fileBasedStack[I]) GetMemoryFootprint() *common.MemoryFootprint {
	valueSize := int(unsafe.Sizeof(I(0)))
	res := common.NewMemoryFootprint(unsafe.Sizeof(*s))
	res.AddChild("buffer", common.NewMemoryFootprint(uintptr(valueSize*cap(s.buffer))))
	return res
}
