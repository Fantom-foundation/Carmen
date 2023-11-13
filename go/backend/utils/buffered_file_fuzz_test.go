package utils

import (
	"bytes"
	"encoding/binary"
	"github.com/Fantom-foundation/Carmen/go/fuzzing"
	"strings"
	"testing"
)

func FuzzBufferedFile_RandomOps(f *testing.F) {
	fuzzing.Fuzz[buffFileFuzzContext](f, &buffFileFuzzCampaign{})
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

// opType is operation type to be applied to a buffered file.
type opType byte

func (o opType) serialize() []byte {
	b := make([]byte, 1, 3) // one byte for type + uint16 for position
	b[0] = byte(o)
	return b
}

const (
	read opType = iota
	write
	flush
	close
)

type opRead struct {
	pos int64
}

func (op *opRead) Serialize() []byte {
	return binary.BigEndian.AppendUint16(read.serialize(), uint16(op.pos))
}

func (op *opRead) Apply(t fuzzing.TestingT, c *buffFileFuzzContext) {
	// generate some payload from the position, which is randomized by the fuzzer already
	// cap to bufferSize, which is maximal supported size
	size := op.pos / 10 % bufferSize
	payload := make([]byte, size)
	if err := c.file.Read(op.pos, payload); err != nil {
		// expected errors in certain situations
		if op.pos < 0 && strings.HasPrefix(err.Error(), "cannot read at negative index:") {
			return
		}

		t.Fatalf("error to read from file: %s", err)
	}

	// match with the shadow file, if the position was written at all
	shadowPayload := make([]byte, size)
	copy(shadowPayload, c.shadow[op.pos:])
	if !bytes.Equal(payload, shadowPayload) {
		t.Errorf("data read from file does not match written data: %x != %x", payload, shadowPayload)
	}
}

type opWrite struct {
	pos int64
}

func (op *opWrite) Serialize() []byte {
	return binary.BigEndian.AppendUint16(write.serialize(), uint16(op.pos))
}

func (op *opWrite) Apply(t fuzzing.TestingT, c *buffFileFuzzContext) {
	// generate some payload from the position, which is randomized by the fuzzer already
	// cap to bufferSize, which is maximal supported size
	size := op.pos / 10 % bufferSize
	payload := make([]byte, size)
	for i := 0; i < int(size); i++ {
		payload[i] = byte(i*int(size) + 1)
	}

	if err := c.file.Write(op.pos, payload); err != nil {
		// expected errors in certain situations
		if len(payload) >= bufferSize && strings.HasPrefix(err.Error(), "writing data >") {
			return
		}
		if op.pos < 0 && strings.HasPrefix(err.Error(), "cannot write at negative position") {
			return
		}

		t.Errorf("error to write to file: %s", err)
	}

	// write the same to the shadow file
	copy(c.shadow[op.pos:], payload)
}

type opFlush struct {
}

func (op *opFlush) Serialize() []byte {
	return flush.serialize()
}

func (op *opFlush) Apply(t fuzzing.TestingT, c *buffFileFuzzContext) {
	if err := c.file.Flush(); err != nil {
		t.Errorf("error to flush: %s", err)
	}
}

type opClose struct {
}

func (op *opClose) Serialize() []byte {
	return close.serialize()
}

func (op *opClose) Apply(t fuzzing.TestingT, c *buffFileFuzzContext) {
	if err := c.file.Close(); err != nil {
		t.Errorf("error to flush: %s", err)
	}
	file, err := OpenBufferedFile(c.path)
	if err != nil {
		t.Errorf("failed to open buffered file: %v", err)
	}
	c.file = file
}

// buffFileFuzzContext holds an instance of buffered file, which is under test,
// and a shadow file to compare the results with.
type buffFileFuzzContext struct {
	path   string
	file   *BufferedFile
	shadow []byte
}

// buffFileFuzzCampaign implements factory to initialise the fuzzing campaign of the buffered file
// triggering a sequence of random operations.
type buffFileFuzzCampaign struct{}

func (c *buffFileFuzzCampaign) Init() []fuzzing.OperationSequence[buffFileFuzzContext] {
	data := []fuzzing.OperationSequence[buffFileFuzzContext]{
		{&opWrite{0}, &opRead{0}, &opClose{}},
		{&opWrite{100}, &opRead{100}, &opClose{}, &opRead{100}},
		{&opWrite{5}, &opRead{5}, &opFlush{}, &opWrite{10}, &opRead{10}},
		{&opWrite{5}, &opRead{5}, &opFlush{}, &opWrite{10}, &opRead{10}, &opRead{5}},
		{&opClose{}},
		{&opFlush{}},
		{&opRead{50}},
		{&opWrite{50}},
		{&opWrite{50}, &opClose{}},
	}

	return data
}

func (c *buffFileFuzzCampaign) CreateContext(t fuzzing.TestingT) *buffFileFuzzContext {
	path := t.TempDir() + "/test.dat"
	file, err := OpenBufferedFile(path)
	if err != nil {
		t.Fatalf("failed to open buffered file: %v", err)
	}

	return &buffFileFuzzContext{path, file, make([]byte, 0xFFFF+bufferSize)}
}

func (c *buffFileFuzzCampaign) Deserialize(rawData []byte) []fuzzing.Operation[buffFileFuzzContext] {
	return parseOperations(rawData)
}

func (c *buffFileFuzzCampaign) Cleanup(t fuzzing.TestingT, context *buffFileFuzzContext) {
	if err := context.file.Close(); err != nil {
		t.Fatalf("cannot close file: %s", err)
	}
}

// parseOperations converts the input byte array
// to the list of operations.
// It is converted from the format: <opType><position>
// This method tries to parse as many of those tuples as possible, terminating when no more
// elements are available.
// Certain modifications of input sequence may take place,
// first the position is caped to 2bytes not to seek in extensively huge files,
// secondly, the number of file closing or flushing operations executed in a row is capped to 3 each.
// The total number of these operations is capped to 20.
// It is because the fuzzing campaign must not take more than 1s
// and flushing or closing a file repeated many times
// almost always prolong runtime above this time limit.
// Unfortunately this limit cannot be currently configured and
// the fuzzing campaigns fails because of running above this limit.
func parseOperations(b []byte) []fuzzing.Operation[buffFileFuzzContext] {
	var ops []fuzzing.Operation[buffFileFuzzContext]
	var closeOrFlushOpCounter, expensiveOpCounter int
	for len(b) >= 1 {
		opType := opType(b[0] % 4)
		b = b[1:]
		if opType == close || opType == flush {
			closeOrFlushOpCounter++
			expensiveOpCounter++
			// do not allow for more han 20 close or flush ops in total.
			if expensiveOpCounter > 20 {
				continue
			}
			// do not allow for more than 3 close or flush ops in a row
			if closeOrFlushOpCounter > 3 {
				continue
			}
		} else {
			// reset by other ops.
			closeOrFlushOpCounter = 0
		}

		var pos int64
		if opType == write || opType == read {
			if len(b) >= 2 {
				// cap position to 2bytes not to allocate huge files
				pos = int64(binary.BigEndian.Uint16(b[0:2]))
				b = b[2:]
			} else {
				return ops
			}
		}

		var op fuzzing.Operation[buffFileFuzzContext]
		switch opType {
		case read:
			op = &opRead{pos}
		case write:
			op = &opWrite{pos}
		case close:
			op = &opClose{}
		case flush:
			op = &opFlush{}
		}
		ops = append(ops, op)
	}

	return ops
}
